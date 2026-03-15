"""
scrape_naukri.py
────────────────
Scrapes Naukri.com public job listings for Indian cities.
Uploads raw JSON partitioned by city to S3 bronze bucket.

Environment variables (injected by GitHub Actions):
  S3_BUCKET  — raw S3 bucket name (e.g. hirewatch-raw-india)
  RUN_TS     — GitHub Actions run ID (used as partition key)

Usage:
  python pipeline/scrapers/scrape_naukri.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import cloudscraper
import structlog
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Logging ────────────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

# ── Config ─────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
RUN_TS    = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
BATCH_TS  = datetime.now(timezone.utc).isoformat()

CITIES = [
    # Tier 1 — major metros
    "bangalore", "mumbai", "hyderabad", "chennai", "delhi",
    "pune",      "kolkata", "ahmedabad",
    # Tier 2 — large cities
    "gurgaon",   "noida",   "jaipur",   "kochi",    "indore",
    "nagpur",    "surat",   "chandigarh","lucknow",  "bhopal",
    "coimbatore","vadodara","visakhapatnam","patna", "ludhiana",
    # Tier 3 — emerging tech hubs
    "mysore",    "thiruvananthapuram","bhubaneswar","dehradun","raipur",
    "nashik",    "rajkot",  "madurai",  "varanasi",  "agra",
    "amritsar",  "jabalpur","meerut",   "ranchi",    "guwahati",
    "jodhpur",   "vijayawada","faridabad","ghaziabad","tiruchirappalli",
    "hubli",     "mangalore","udaipur",  "aurangabad","navi-mumbai",
    "thane",     "kota",    "kozhikode","guntur",    "warangal",
]

# Naukri search URL template — public, no auth needed
NAUKRI_URL = (
    "https://www.naukri.com/jobs-in-{city}"
    "?experience=0&jobAge=1&noOfResults=50"
)

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# ── S3 client ──────────────────────────────────────────────────────
s3 = boto3.client("s3")


def s3_key(city: str) -> str:
    """Bronze partition key: source=naukri / date / hour / city.json"""
    now = datetime.now(timezone.utc)
    return (
        f"source=naukri/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"city={city}/run={RUN_TS}.json"
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def fetch_city_page(scraper: cloudscraper.CloudScraper, city: str) -> str:
    url = NAUKRI_URL.format(city=city)
    log.info("fetching", city=city, url=url)
    resp = scraper.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.text


def parse_job_cards(html: str, city: str) -> list[dict[str, Any]]:
    """
    Parse Naukri job-listing cards from HTML.
    Naukri renders article.jobTuple elements for each listing.
    """
    soup = BeautifulSoup(html, "lxml")
    jobs: list[dict[str, Any]] = []

    cards = soup.select("article.jobTuple, div[class*='srp-jobtuple-wrapper']")
    if not cards:
        # Fallback — try JSON-LD embedded data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") == "JobPosting":
                            jobs.append(_ld_to_job(item, city))
                elif data.get("@type") == "JobPosting":
                    jobs.append(_ld_to_job(data, city))
            except (json.JSONDecodeError, TypeError):
                continue
        return jobs

    for card in cards:
        try:
            title_tag   = card.select_one("a.title, [class*='title']")
            company_tag = card.select_one("a.subTitle, [class*='comp-name']")
            loc_tag     = card.select_one("li.location span, [class*='loc-wrap']")
            exp_tag     = card.select_one("li.experience span, [class*='exp-wrap']")
            sal_tag     = card.select_one("li.salary span, [class*='salary']")
            date_tag    = card.select_one("span.date, [class*='job-post-day']")

            jobs.append({
                "title":       _text(title_tag),
                "company":     _text(company_tag),
                "location":    _text(loc_tag) or city.title(),
                "city_key":    city,
                "experience":  _text(exp_tag),
                "salary":      _text(sal_tag),
                "posted_date": _parse_relative_date(_text(date_tag)),
                "job_url":     _href(title_tag),
                "source":      "naukri",
                "scraped_at":  BATCH_TS,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("card_parse_error", exc=str(exc))
            continue

    return jobs


def _ld_to_job(item: dict, city: str) -> dict:
    return {
        "title":       item.get("title", ""),
        "company":     item.get("hiringOrganization", {}).get("name", ""),
        "location":    city.title(),
        "city_key":    city,
        "experience":  "",
        "salary":      item.get("baseSalary", {}).get("value", {}).get("value", ""),
        "posted_date": item.get("datePosted", ""),
        "job_url":     item.get("url", ""),
        "source":      "naukri",
        "scraped_at":  BATCH_TS,
    }


def _text(tag) -> str:
    return tag.get_text(strip=True) if tag else ""


def _href(tag) -> str:
    return tag.get("href", "") if tag else ""


def _parse_relative_date(text: str) -> str:
    """Convert '3 days ago' → ISO date string."""
    if not text:
        return ""
    now = datetime.now(timezone.utc)
    m = re.search(r"(\d+)\s*(day|hour|week|month)", text, re.I)
    if not m:
        return text
    n, unit = int(m.group(1)), m.group(2).lower()
    from datetime import timedelta
    delta_map = {"hour": timedelta(hours=n), "day": timedelta(days=n),
                 "week": timedelta(weeks=n), "month": timedelta(days=n*30)}
    return (now - delta_map.get(unit, timedelta())).date().isoformat()


def upload_to_s3(city: str, jobs: list[dict]) -> None:
    payload = {
        "batch_ts":  BATCH_TS,
        "run_id":    RUN_TS,
        "city":      city,
        "job_count": len(jobs),
        "jobs":      jobs,
    }
    key  = s3_key(city)
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode(),
        ContentType="application/json",
    )
    log.info("uploaded", city=city, key=key, job_count=len(jobs))


def main() -> None:
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    total = 0
    for city in CITIES:
        try:
            html  = fetch_city_page(scraper, city)
            jobs  = parse_job_cards(html, city)
            upload_to_s3(city, jobs)
            total += len(jobs)
            log.info("city_done", city=city, jobs=len(jobs))
            # Polite delay — avoid hammering the server
            time.sleep(2.5)
        except Exception as exc:  # noqa: BLE001
            log.error("city_failed", city=city, exc=str(exc))
            # Continue with next city — don't abort the whole run
            continue

    log.info("scrape_complete", total_jobs=total, cities=len(CITIES))


if __name__ == "__main__":
    main()
