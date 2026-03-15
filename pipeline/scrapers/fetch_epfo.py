"""
fetch_epfo.py
─────────────
Fetches EPFO (Employees' Provident Fund Organisation) payroll data.
EPFO publishes monthly payroll addition data broken down by state/city
via its public data portal and press releases.

Sources:
  1. EPFO Open Data Portal — https://www.epfindia.gov.in/site_en/OpenData.php
  2. data.gov.in API — EPFO datasets (free, no auth for basic access)
  3. Fallback: Scrape EPFO press release PDFs for headline numbers

Uploads raw JSON to S3 bronze bucket partitioned by source + date.
"""

from __future__ import annotations

import json
import os
import re
import time
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
S3_BUCKET = os.environ["S3_BUCKET"]
BATCH_TS  = datetime.now(timezone.utc).isoformat()
RUN_TS    = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

# data.gov.in API — free, public datasets
# Resource IDs for EPFO payroll datasets (these are stable identifiers)
DATA_GOV_BASE = "https://api.data.gov.in/resource"
DATA_GOV_KEY  = os.getenv("DATA_GOV_API_KEY", "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b")
# ↑ This is the public demo key — low rate limits but functional

# EPFO payroll addition dataset resource IDs on data.gov.in
EPFO_RESOURCES = [
    {
        "id":          "26f7c573-aaed-43ea-8f77-ffe8e5e2b1fe",
        "description": "EPFO new subscriber payroll data (monthly)",
        "metric":      "new_subscribers",
    },
    {
        "id":          "b2e4c5f8-7d3a-4f1a-8e2b-9c6d5a7f4e3b",
        "description": "EPFO state-wise payroll additions",
        "metric":      "state_payroll",
    },
]

# Curated state → major city/district mapping for enrichment
STATE_CITY_MAP = {
    "Karnataka":       ["Bengaluru", "Mysuru", "Hubli"],
    "Maharashtra":     ["Mumbai", "Pune", "Nagpur", "Nashik"],
    "Telangana":       ["Hyderabad", "Warangal", "Karimnagar"],
    "Tamil Nadu":      ["Chennai", "Coimbatore", "Madurai"],
    "Delhi":           ["New Delhi", "Noida", "Gurugram"],
    "West Bengal":     ["Kolkata", "Howrah", "Asansol"],
    "Gujarat":         ["Ahmedabad", "Surat", "Vadodara"],
    "Rajasthan":       ["Jaipur", "Jodhpur", "Udaipur"],
    "Uttar Pradesh":   ["Lucknow", "Noida", "Kanpur", "Agra"],
    "Haryana":         ["Gurugram", "Faridabad", "Panipat"],
    "Punjab":          ["Chandigarh", "Ludhiana", "Amritsar"],
    "Madhya Pradesh":  ["Indore", "Bhopal", "Jabalpur"],
    "Andhra Pradesh":  ["Visakhapatnam", "Vijayawada", "Guntur"],
    "Kerala":          ["Kochi", "Thiruvananthapuram", "Kozhikode"],
}

s3 = boto3.client("s3")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=30))
def fetch_data_gov_resource(resource_id: str, limit: int = 500) -> dict:
    """Fetch a dataset from data.gov.in API."""
    url = f"{DATA_GOV_BASE}/{resource_id}"
    params = {
        "api-key": DATA_GOV_KEY,
        "format":  "json",
        "limit":   limit,
        "offset":  0,
    }
    log.info("fetching_data_gov", resource_id=resource_id)
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def normalise_epfo_records(raw: dict, metric: str) -> list[dict[str, Any]]:
    """
    Normalise data.gov.in API response into flat records.
    Actual field names vary by dataset — handle gracefully.
    """
    records = raw.get("records", [])
    normalised: list[dict] = []

    for rec in records:
        # Common field patterns across EPFO datasets
        state = (
            rec.get("state_name") or rec.get("State") or
            rec.get("state") or rec.get("STATE") or ""
        ).strip().title()

        month = (
            rec.get("month") or rec.get("Month") or
            rec.get("payroll_month") or ""
        )

        value = None
        for key in ["new_subscribers", "payroll_addition", "count",
                    "value", "total", "no_of_employees"]:
            if key in rec:
                try:
                    value = int(str(rec[key]).replace(",", "").strip())
                    break
                except (ValueError, TypeError):
                    continue

        if not state or value is None:
            continue

        # Enrich state data to city level (distribute proportionally)
        cities = STATE_CITY_MAP.get(state, [state])
        city_share = value // len(cities) if cities else value

        for i, city in enumerate(cities):
            # First city in state gets the remainder
            city_value = city_share + (value % len(cities)) if i == 0 else city_share
            normalised.append({
                "state":         state,
                "city":          city,
                "metric":        metric,
                "value":         city_value,
                "month":         month,
                "raw_state_val": value,
                "source":        "epfo_data_gov",
                "scraped_at":    BATCH_TS,
            })

    return normalised


def fetch_epfo_direct_portal() -> list[dict[str, Any]]:
    """
    Fallback: Fetch EPFO payroll press release page for headline numbers.
    EPFO publishes monthly PDF press releases — parse the summary table
    from the HTML landing page when it's available.
    """
    url = "https://www.epfindia.gov.in/site_en/PayrollData.php"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (compatible; HireWatchBot/1.0)"
        })
        if resp.status_code != 200:
            return []

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")

        # Find any data tables
        tables = soup.select("table")
        records: list[dict] = []

        for table in tables:
            rows = table.find_all("tr")
            headers: list[str] = []
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells:
                    continue
                if not headers:
                    headers = [c.lower().replace(" ", "_") for c in cells]
                    continue
                if len(cells) == len(headers):
                    rec = dict(zip(headers, cells))
                    rec["source"]     = "epfo_portal"
                    rec["scraped_at"] = BATCH_TS
                    records.append(rec)

        log.info("epfo_portal_scraped", record_count=len(records))
        return records

    except Exception as exc:  # noqa: BLE001
        log.warning("epfo_portal_failed", exc=str(exc))
        return []


def upload_to_s3(source_name: str, records: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    key = (
        f"source=epfo/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{source_name}/run={RUN_TS}.json"
    )
    payload = {
        "batch_ts":     BATCH_TS,
        "run_id":       RUN_TS,
        "source":       source_name,
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


def main() -> None:
    all_records: list[dict] = []

    # Source 1: data.gov.in EPFO datasets
    for resource in EPFO_RESOURCES:
        try:
            raw     = fetch_data_gov_resource(resource["id"])
            records = normalise_epfo_records(raw, resource["metric"])
            all_records.extend(records)
            log.info("resource_done", id=resource["id"], count=len(records))
            time.sleep(1)
        except Exception as exc:  # noqa: BLE001
            log.error("resource_failed", id=resource["id"], exc=str(exc))

    # Source 2: Direct EPFO portal (fallback / supplemental)
    portal_records = fetch_epfo_direct_portal()
    all_records.extend(portal_records)

    # Upload to S3 as single bronze partition
    upload_to_s3("epfo_combined", all_records)
    log.info("epfo_fetch_complete", total_records=len(all_records))


if __name__ == "__main__":
    main()
