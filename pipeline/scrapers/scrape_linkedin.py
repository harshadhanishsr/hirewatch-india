"""
scrape_linkedin.py
──────────────────
Scrapes LinkedIn public job-search pages for India cities.
Uses LinkedIn's public search endpoint — no login required.
Results are uploaded to S3 bronze as partitioned JSON.

NOTE: LinkedIn may throttle or return CAPTCHAs under heavy load.
      The scraper backs off exponentially and logs failures per city
      without aborting the full run.

Environment variables:
  S3_BUCKET  — raw S3 bucket name
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import boto3
import structlog
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

# ── Config ─────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
BATCH_TS  = datetime.now(timezone.utc).isoformat()
RUN_TS    = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

# Cities mapped to their LinkedIn geo IDs for India
CITY_GEO_MAP = {
    "bangalore":  "105556813",
    "mumbai":     "105742997",
    "hyderabad":  "105556816",
    "chennai":    "105556817",
    "delhi":      "105556775",
    "pune":       "105556814",
    "kolkata":    "105556810",
    "ahmedabad":  "105556805",
    "gurgaon":    "103197467",
    "noida":      "105556821",
    "jaipur":     "106598621",
    "kochi":      "105556799",
    "indore":     "105745065",
    "nagpur":     "105745069",
}

# LinkedIn public jobs search — geoId filters to specific city
LI_URL = (
    "https://www.linkedin.com/jobs/search/"
    "?keywords={keywords}&location={city}&geoId={geo_id}"
    "&f_TPR=r86400&start={start}"    # r86400 = last 24 hours
)

KEYWORDS = ["software engineer", "data engineer", "product manager",
            "marketing", "sales", "operations"]

ua = UserAgent()
s3 = boto3.client("s3")


def _headers() -> dict:
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.linkedin.com/",
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=3, min=5, max=60))
def fetch_page(session: requests.Session, url: str) -> str:
    resp = session.get(url, headers=_headers(), timeout=25)
    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429) — backing off")
    resp.raise_for_status()
    return resp.text


def parse_linkedin_cards(html: str, city: str) -> list[dict[str, Any]]:
    soup  = BeautifulSoup(html, "lxml")
    cards = soup.select("div.base-card, li.result-card")
    jobs: list[dict[str, Any]] = []

    for card in cards:
        try:
            title_tag    = card.select_one("h3.base-search-card__title, h3.result-card__title")
            company_tag  = card.select_one("h4.base-search-card__subtitle, h4.result-card__subtitle")
            location_tag = card.select_one("span.job-search-card__location")
            date_tag     = card.select_one("time")
            link_tag     = card.select_one("a.base-card__full-link, a.result-card__full-card-link")

            jobs.append({
                "title":       _text(title_tag),
                "company":     _text(company_tag),
                "location":    _text(location_tag) or city.title(),
                "city_key":    city,
                "posted_date": date_tag.get("datetime", "") if date_tag else "",
                "job_url":     link_tag.get("href", "") if link_tag else "",
                "source":      "linkedin",
                "scraped_at":  BATCH_TS,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("parse_error", exc=str(exc))

    # Also try JSON-LD embedded in page
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "JobPosting":
                    jobs.append({
                        "title":       item.get("title", ""),
                        "company":     item.get("hiringOrganization", {}).get("name", ""),
                        "location":    city.title(),
                        "city_key":    city,
                        "posted_date": item.get("datePosted", ""),
                        "job_url":     item.get("url", ""),
                        "source":      "linkedin",
                        "scraped_at":  BATCH_TS,
                    })
        except (json.JSONDecodeError, TypeError):
            continue

    return jobs


def _text(tag) -> str:
    return tag.get_text(strip=True) if tag else ""


def s3_key(city: str) -> str:
    now = datetime.now(timezone.utc)
    return (
        f"source=linkedin/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"city={city}/run={RUN_TS}.json"
    )


def upload_to_s3(city: str, jobs: list[dict]) -> None:
    payload = {
        "batch_ts":  BATCH_TS,
        "run_id":    RUN_TS,
        "city":      city,
        "job_count": len(jobs),
        "jobs":      jobs,
    }
    key = s3_key(city)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("uploaded", source="linkedin", city=city, key=key, count=len(jobs))


def main() -> None:
    session = requests.Session()
    total   = 0

    for city, geo_id in CITY_GEO_MAP.items():
        all_jobs: list[dict] = []
        try:
            # Scrape up to 3 pages of results per city (25 results/page)
            for kw in KEYWORDS[:3]:   # top 3 keyword categories only to respect rate limits
                url  = LI_URL.format(
                    keywords=quote_plus(kw),
                    city=quote_plus(city.title() + ", India"),
                    geo_id=geo_id,
                    start=0,
                )
                html = fetch_page(session, url)
                jobs = parse_linkedin_cards(html, city)
                all_jobs.extend(jobs)
                log.info("page_done", city=city, keyword=kw, count=len(jobs))
                time.sleep(3)   # Polite delay between keyword queries

            # Deduplicate by job_url
            seen: set[str] = set()
            unique: list[dict] = []
            for j in all_jobs:
                k = j.get("job_url") or f"{j['title']}|{j['company']}"
                if k not in seen:
                    seen.add(k)
                    unique.append(j)

            upload_to_s3(city, unique)
            total += len(unique)
            log.info("city_done", city=city, unique_jobs=len(unique))
            time.sleep(4)

        except Exception as exc:  # noqa: BLE001
            log.error("city_failed", source="linkedin", city=city, exc=str(exc))
            continue

    log.info("scrape_complete", source="linkedin", total_jobs=total)


if __name__ == "__main__":
    main()
