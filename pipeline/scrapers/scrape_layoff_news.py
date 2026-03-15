"""
scrape_layoff_news.py
─────────────────────
Collects layoff signals from multiple free sources:
  1. NewsAPI.org — free tier (100 req/day), keyword search
  2. Google News RSS feeds — completely free, no auth
  3. Economic Times / Mint / MoneyControl RSS — free feeds
  4. Layoffs.fyi (public page) — crowdsourced tech layoff tracker

For each news article, extracts:
  - Company name (regex + NER heuristics)
  - Number of employees laid off (regex pattern matching)
  - Location / city mentioned
  - Date, headline, source URL
  - Sentiment label (positive = hiring, negative = layoff)

Uploads raw signal records to S3 bronze.
"""

from __future__ import annotations

import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

# ── Config ─────────────────────────────────────────────────────────
S3_BUCKET    = os.environ["S3_BUCKET"]
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")          # GitHub Actions secret
BATCH_TS     = datetime.now(timezone.utc).isoformat()
RUN_TS       = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

# ── Keyword sets ───────────────────────────────────────────────────
LAYOFF_KEYWORDS = [
    "layoffs India", "job cuts India", "retrenchment India",
    "workforce reduction India", "employees fired India",
    "pink slip India tech", "mass layoffs Bengaluru",
    "startup layoffs India 2024", "mass layoffs Hyderabad",
    "mass layoffs Chennai", "mass layoffs Pune",
]

HIRING_KEYWORDS = [
    "hiring India technology", "mass hiring India IT",
    "India tech jobs 2024", "freshers hiring India",
    "campus hiring India", "lateral hiring India tech",
    "hiring Bengaluru", "hiring Hyderabad", "hiring Chennai",
]

# ── Indian city / district names for location extraction ──────────
INDIA_CITIES = {
    "bengaluru","bangalore","mumbai","hyderabad","chennai",
    "delhi","new delhi","pune","kolkata","ahmedabad",
    "gurgaon","gurugram","noida","jaipur","kochi","cochin",
    "indore","nagpur","surat","chandigarh","lucknow","bhopal",
    "coimbatore","vadodara","visakhapatnam","vizag","patna",
    "ludhiana","mysore","mysuru","thiruvananthapuram","trivandrum",
    "bhubaneswar","dehradun","nashik","rajkot","madurai","ranchi",
    "guwahati","jodhpur","vijayawada","faridabad","ghaziabad",
    "hubli","hubballi","mangalore","mangaluru","udaipur",
    "aurangabad","thane","kota","kozhikode","warangal","raipur",
    "varanasi","agra","amritsar","jabalpur","meerut","guntur",
}

# ── Google News RSS feeds ──────────────────────────────────────────
GOOGLE_NEWS_RSS = [
    "https://news.google.com/rss/search?q=layoffs+India+tech&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=hiring+India+technology&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=India+startup+funding+jobs&hl=en-IN&gl=IN&ceid=IN:en",
]

# Economic Times / Mint RSS
ET_RSS_FEEDS = [
    "https://economictimes.indiatimes.com/jobs/rssfeeds/1017770.cms",
    "https://www.livemint.com/rss/companies",
]

s3 = boto3.client("s3")


# ── Extraction helpers ─────────────────────────────────────────────

# Patterns to extract headcount from text
HEADCOUNT_PATTERNS = [
    r"(\d[\d,]+)\s+(?:employees?|workers?|staff|jobs?|people)\s+(?:laid off|fired|cut|let go|retrenched)",
    r"(?:lay off|layoff|cut|fire|retrench)\s+(\d[\d,]+)\s+(?:employees?|workers?|staff|people)",
    r"(\d[\d,]+)\s*(?:-|to)\s*\d[\d,]+\s+(?:jobs?|positions?|roles?)",
    r"reduce\s+(?:workforce|headcount|staff)\s+by\s+(\d[\d,]+)",
    r"(\d+)\s*%\s+(?:of its|of the)?\s*(?:workforce|employees?)",
]

# Company name pattern — capitalised words before "lays off / has laid"
COMPANY_PATTERNS = [
    r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+(?:lays? off|has laid off|plans? to lay off)",
    r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+(?:fires?|let go|retrenches?|cuts? \d)",
    r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+(?:announces?|plans?)\s+(?:mass )?hiring",
    r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3})\s+(?:to hire|is hiring|recruiting)",
]

# Known Indian tech companies for boosted matching
KNOWN_COMPANIES = {
    "Infosys", "TCS", "Wipro", "HCL", "Tech Mahindra", "Cognizant",
    "Accenture", "IBM", "Capgemini", "LTIMindtree", "Mphasis",
    "Hexaware", "Persistent", "NIIT Technologies", "Zensar",
    "Byju's", "Paytm", "Swiggy", "Zomato", "PhonePe", "Meesho",
    "Ola", "Uber India", "Nykaa", "Flipkart", "Amazon India",
    "Google India", "Microsoft India", "Meta India", "Apple India",
    "Razorpay", "CRED", "Groww", "Zerodha", "Freshworks",
    "Zoho", "MakeMyTrip", "IndiGo", "Air India", "HDFC Bank",
}


def extract_company(text: str) -> str:
    """Best-effort company name extraction from headline/description."""
    for company in KNOWN_COMPANIES:
        if company.lower() in text.lower():
            return company

    for pattern in COMPANY_PATTERNS:
        m = re.search(pattern, text)
        if m:
            name = m.group(1).strip()
            # Filter out common false positives
            if name not in {"The", "A", "An", "India", "New", "This"}:
                return name
    return ""


def extract_headcount(text: str) -> int | None:
    """Extract number of employees affected."""
    for pattern in HEADCOUNT_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except (ValueError, IndexError):
                continue
    return None


def extract_city(text: str) -> str:
    """Find first mention of an Indian city in the text."""
    text_lower = text.lower()
    for city in INDIA_CITIES:
        if city in text_lower:
            return city.title()
    return ""


def classify_signal(title: str, description: str) -> str:
    """Rule-based signal type: 'layoff', 'hiring', or 'neutral'."""
    text = (title + " " + description).lower()
    layoff_words = {"layoff", "lay off", "fired", "retrench", "cut", "let go",
                    "job loss", "pink slip", "downsizing", "workforce reduction"}
    hiring_words = {"hiring", "recruit", "job opening", "vacancy", "join us",
                    "we're hiring", "campus", "fresher", "lateral hire", "onboard"}
    layoff_score = sum(1 for w in layoff_words if w in text)
    hiring_score = sum(1 for w in hiring_words if w in text)
    if layoff_score > hiring_score:
        return "layoff"
    if hiring_score > layoff_score:
        return "hiring"
    return "neutral"


# ── Source 1: NewsAPI ──────────────────────────────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
def fetch_newsapi(keyword: str) -> list[dict]:
    if not NEWS_API_KEY:
        log.warning("newsapi_key_missing", keyword=keyword)
        return []
    url = "https://newsapi.org/v2/everything"
    params = {
        "q":        keyword,
        "language": "en",
        "sortBy":   "publishedAt",
        "pageSize": 20,
        "apiKey":   NEWS_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("articles", [])


def parse_newsapi_articles(articles: list[dict]) -> list[dict[str, Any]]:
    records: list[dict] = []
    for art in articles:
        title = art.get("title", "") or ""
        desc  = art.get("description", "") or ""
        full  = title + " " + desc
        records.append({
            "title":        title,
            "description":  desc[:500],
            "company":      extract_company(full),
            "city":         extract_city(full),
            "headcount":    extract_headcount(full),
            "signal_type":  classify_signal(title, desc),
            "published_at": art.get("publishedAt", ""),
            "url":          art.get("url", ""),
            "source_name":  art.get("source", {}).get("name", ""),
            "source":       "newsapi",
            "scraped_at":   BATCH_TS,
        })
    return records


# ── Source 2: Google News RSS ─────────────────────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
def fetch_rss(url: str) -> str:
    resp = requests.get(url, timeout=15, headers={
        "User-Agent": "Mozilla/5.0 (compatible; HireWatchBot/1.0)"
    })
    resp.raise_for_status()
    return resp.text


def parse_rss_feed(xml_text: str, source_label: str) -> list[dict[str, Any]]:
    records: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
        ns   = {"content": "http://purl.org/rss/1.0/modules/content/"}
        for item in root.iter("item"):
            title    = (item.findtext("title") or "").strip()
            link     = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            desc     = (item.findtext("description") or "").strip()
            # Strip HTML tags from description
            desc_clean = re.sub(r"<[^>]+>", " ", desc).strip()[:500]
            full = title + " " + desc_clean
            records.append({
                "title":        title,
                "description":  desc_clean,
                "company":      extract_company(full),
                "city":         extract_city(full),
                "headcount":    extract_headcount(full),
                "signal_type":  classify_signal(title, desc_clean),
                "published_at": pub_date,
                "url":          link,
                "source_name":  source_label,
                "source":       "rss",
                "scraped_at":   BATCH_TS,
            })
    except ET.ParseError as exc:
        log.warning("rss_parse_error", source=source_label, exc=str(exc))
    return records


# ── Source 3: Layoffs.fyi public page ─────────────────────────────

def scrape_layoffs_fyi() -> list[dict[str, Any]]:
    """Scrape the Layoffs.fyi tracker page for India entries."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://layoffs.fyi/",
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HireWatchBot/1.0)"},
        )
        if resp.status_code != 200:
            return []
        soup    = BeautifulSoup(resp.text, "lxml")
        rows    = soup.select("table tbody tr")
        records: list[dict] = []
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 5:
                continue
            # Typical columns: Company, #Laid Off, Date, Industry, Location, ...
            company  = cells[0]
            headcount_raw = cells[1] if len(cells) > 1 else ""
            date     = cells[2] if len(cells) > 2 else ""
            industry = cells[3] if len(cells) > 3 else ""
            location = cells[4] if len(cells) > 4 else ""

            # Filter to India-related entries
            if "india" not in location.lower() and not any(
                c in location.lower() for c in INDIA_CITIES
            ):
                continue

            try:
                hc = int(re.sub(r"[^\d]", "", headcount_raw)) if headcount_raw else None
            except ValueError:
                hc = None

            records.append({
                "title":        f"{company} lays off {headcount_raw} employees",
                "description":  f"{company} | {industry} | {location}",
                "company":      company,
                "city":         extract_city(location),
                "headcount":    hc,
                "signal_type":  "layoff",
                "published_at": date,
                "url":          "https://layoffs.fyi/",
                "source_name":  "layoffs.fyi",
                "source":       "layoffs_fyi",
                "scraped_at":   BATCH_TS,
            })
        log.info("layoffs_fyi_scraped", count=len(records))
        return records
    except Exception as exc:  # noqa: BLE001
        log.warning("layoffs_fyi_failed", exc=str(exc))
        return []


# ── Upload ─────────────────────────────────────────────────────────

def upload_to_s3(records: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    key = (
        f"source=news/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"run={RUN_TS}.json"
    )
    payload = {
        "batch_ts":     BATCH_TS,
        "run_id":       RUN_TS,
        "record_count": len(records),
        "records":      records,
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("uploaded", key=key, count=len(records))


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    all_records: list[dict] = []

    # 1) NewsAPI
    for kw in LAYOFF_KEYWORDS + HIRING_KEYWORDS:
        try:
            articles = fetch_newsapi(kw)
            records  = parse_newsapi_articles(articles)
            all_records.extend(records)
            log.info("newsapi_done", keyword=kw, count=len(records))
            time.sleep(1.2)
        except Exception as exc:  # noqa: BLE001
            log.error("newsapi_failed", keyword=kw, exc=str(exc))

    # 2) Google News RSS
    for feed_url in GOOGLE_NEWS_RSS:
        try:
            xml      = fetch_rss(feed_url)
            records  = parse_rss_feed(xml, "google_news")
            all_records.extend(records)
            log.info("rss_done", url=feed_url, count=len(records))
            time.sleep(1)
        except Exception as exc:  # noqa: BLE001
            log.error("rss_failed", url=feed_url, exc=str(exc))

    # 3) ET / Mint RSS
    for feed_url in ET_RSS_FEEDS:
        try:
            xml     = fetch_rss(feed_url)
            records = parse_rss_feed(xml, feed_url.split("/")[2])
            all_records.extend(records)
            log.info("et_rss_done", url=feed_url, count=len(records))
            time.sleep(1)
        except Exception as exc:  # noqa: BLE001
            log.error("et_rss_failed", url=feed_url, exc=str(exc))

    # 4) Layoffs.fyi
    fyi_records = scrape_layoffs_fyi()
    all_records.extend(fyi_records)

    # Deduplicate by URL
    seen: set[str] = set()
    unique = [r for r in all_records
              if r.get("url") not in seen and not seen.add(r.get("url", ""))]  # type: ignore[func-returns-value]

    upload_to_s3(unique)
    log.info("news_scrape_complete", total=len(all_records), unique=len(unique))


if __name__ == "__main__":
    main()
