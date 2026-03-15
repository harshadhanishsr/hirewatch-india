"""
hirewatch_etl.py
────────────────
HireWatch India — Bronze → Silver → Gold ETL
Runs inside GitHub Actions — no Databricks, no PySpark, no cluster.
Pure Python + pandas. Reads raw JSON from S3, writes Gold JSON back to S3.

Environment variables (set by GitHub Actions):
  S3_BUCKET        — raw bronze bucket  (hirewatch-raw-india)
  S3_OUTPUT_BUCKET — gold output bucket (hirewatch-gold-india)
  RUN_TS           — GitHub Actions run ID
"""

from __future__ import annotations
import json, os, re, logging
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any

import boto3
import pandas as pd
from rapidfuzz import process as fuzz_process, fuzz
from unidecode import unidecode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
S3_BUCKET    = os.environ["S3_BUCKET"]
OUT_BUCKET   = os.environ["S3_OUTPUT_BUCKET"]
RUN_TS       = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
TODAY        = datetime.now(timezone.utc).strftime("%Y-%m-%d")
NOW          = datetime.now(timezone.utc)
s3           = boto3.client("s3", region_name="ap-south-2")

# ── Canonical company names ──────────────────────────────────────────
CANONICAL_COMPANIES = [
    "Infosys","TCS","Wipro","HCL Technologies","Tech Mahindra",
    "Cognizant","Capgemini","Accenture India","LTIMindtree","Mphasis",
    "Hexaware","Persistent Systems","Zensar Technologies","Flipkart",
    "Swiggy","Zomato","Paytm","Ola","PhonePe","Meesho","Razorpay",
    "CRED","Groww","Zerodha","Freshworks","Zoho","MakeMyTrip","Nykaa",
    "Byju's","Unacademy","upGrad","PolicyBazaar","BigBasket","Delhivery",
    "Amazon India","Google India","Microsoft India","IBM India",
    "SAP India","Oracle India","Salesforce India","Adobe India",
    "Uber India","IndiGo","Air India","HDFC Bank","ICICI Bank",
    "Reliance Industries","Adani Enterprises","Tata Consultancy Services",
]

# ── City normalisation map ───────────────────────────────────────────
CITY_MAP = {
    "bangalore":"Bengaluru","bengaluru":"Bengaluru","bombay":"Mumbai",
    "mumbai":"Mumbai","delhi":"Delhi","new delhi":"Delhi","ncr":"Delhi",
    "gurgaon":"Gurugram","gurugram":"Gurugram","noida":"Noida",
    "hyderabad":"Hyderabad","hyd":"Hyderabad","chennai":"Chennai",
    "madras":"Chennai","kolkata":"Kolkata","calcutta":"Kolkata",
    "pune":"Pune","ahmedabad":"Ahmedabad","amdavad":"Ahmedabad",
    "jaipur":"Jaipur","kochi":"Kochi","cochin":"Kochi",
    "indore":"Indore","nagpur":"Nagpur","surat":"Surat",
    "chandigarh":"Chandigarh","lucknow":"Lucknow","vizag":"Visakhapatnam",
    "visakhapatnam":"Visakhapatnam","coimbatore":"Coimbatore",
}

CITY_COORDS = {
    "Bengaluru":[12.9716,77.5946],"Mumbai":[19.0760,72.8777],
    "Hyderabad":[17.3850,78.4867],"Chennai":[13.0827,80.2707],
    "Delhi":[28.6139,77.2090],"Gurugram":[28.4595,77.0266],
    "Noida":[28.5355,77.3910],"Pune":[18.5204,73.8567],
    "Kolkata":[22.5726,88.3639],"Ahmedabad":[23.0225,72.5714],
    "Jaipur":[26.9124,75.7873],"Kochi":[9.9312,76.2673],
    "Indore":[22.7196,75.8577],"Nagpur":[21.1458,79.0882],
    "Surat":[21.1702,72.8311],"Visakhapatnam":[17.6868,83.2185],
    "Chandigarh":[30.7333,76.7794],"Lucknow":[26.8467,80.9462],
    "Coimbatore":[11.0168,76.9558],
}

# ── Helpers ──────────────────────────────────────────────────────────

def normalise_company(name: str) -> str:
    if not name or len(str(name).strip()) < 2:
        return ""
    name = str(name).strip()
    result = fuzz_process.extractOne(
        name, CANONICAL_COMPANIES,
        scorer=fuzz.token_sort_ratio, score_cutoff=72
    )
    return result[0] if result else name.strip().title()


def normalise_city(raw: str) -> str:
    if not raw:
        return ""
    key = str(raw).lower().strip()
    for k, v in CITY_MAP.items():
        if k in key:
            return v
    return str(raw).strip().title()


def signal_score(signal_type: str, headcount) -> float:
    hc = int(headcount) if headcount and str(headcount).isdigit() else 0
    if signal_type == "layoff":
        if hc > 5000: return -1.0
        if hc > 1000: return -0.8
        if hc > 100:  return -0.6
        return -0.4
    if signal_type == "hiring":
        if hc > 5000: return 1.0
        if hc > 1000: return 0.8
        if hc > 100:  return 0.6
        return 0.4
    return 0.0


# ── S3 helpers ───────────────────────────────────────────────────────

def list_s3_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_s3_json(bucket: str, key: str) -> dict:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception as e:
        log.warning(f"Could not read s3://{bucket}/{key}: {e}")
        return {}


def load_bronze_source(prefix: str) -> list[dict]:
    """Load all JSON files from a bronze prefix, flatten records/jobs arrays."""
    keys = list_s3_keys(S3_BUCKET, f"source={prefix}/")
    records = []
    for key in keys:
        data = read_s3_json(S3_BUCKET, key)
        if not data:
            continue
        # Our envelope has either 'records' or 'jobs' array
        items = data.get("records") or data.get("jobs") or []
        if isinstance(items, list):
            records.extend(items)
    log.info(f"Bronze {prefix}: {len(records)} records from {len(keys)} files")
    return records


def upload_json(key: str, data: Any) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    s3.put_object(
        Bucket=OUT_BUCKET,
        Key=f"data/{key}",
        Body=body.encode(),
        ContentType="application/json",
        CacheControl="public, max-age=3600",
    )
    log.info(f"Uploaded → s3://{OUT_BUCKET}/data/{key}  ({len(body)} bytes)")


# ── Bronze → Silver ──────────────────────────────────────────────────

def build_silver_jobs() -> pd.DataFrame:
    naukri   = load_bronze_source("naukri")
    linkedin = load_bronze_source("linkedin")
    raw      = naukri + linkedin
    if not raw:
        log.warning("No job records found in bronze — returning empty DataFrame")
        return pd.DataFrame()

    df = pd.DataFrame(raw)

    # Normalise company + city
    df["company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    city_col = df.get("city_key") if "city_key" in df.columns else df.get("location", pd.Series(dtype=str))
    df["city"] = city_col.fillna("").apply(normalise_city)

    # Parse date
    df["posted_date"] = pd.to_datetime(df.get("posted_date", pd.Series(dtype=str)), errors="coerce")
    df["is_recent"]   = df["posted_date"] >= (NOW - timedelta(days=7))

    # Deduplicate
    df = df[df["company_norm"].str.len() > 2]
    df = df.drop_duplicates(subset=["company_norm", "city"])
    log.info(f"Silver jobs: {len(df)} rows")
    return df


def build_silver_news() -> pd.DataFrame:
    raw = load_bronze_source("news")
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df["company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    df["city"]         = df.get("city", pd.Series(dtype=str)).fillna("").apply(normalise_city)
    df["signal_score"] = df.apply(
        lambda r: signal_score(r.get("signal_type",""), r.get("headcount")), axis=1
    )
    df["published_at"] = pd.to_datetime(df.get("published_at", pd.Series(dtype=str)), errors="coerce")

    cutoff = NOW - timedelta(days=30)
    df = df[df["published_at"] >= cutoff]
    df = df.drop_duplicates(subset=["url"]) if "url" in df.columns else df
    log.info(f"Silver news: {len(df)} rows")
    return df


def build_silver_mca() -> pd.DataFrame:
    raw = load_bronze_source("mca")
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df["name_norm"] = df.get("name", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    df["city"]      = df.get("city", pd.Series(dtype=str)).fillna("").apply(normalise_city)
    df = df.drop_duplicates(subset=["name_norm"])
    log.info(f"Silver MCA: {len(df)} rows")
    return df


# ── Silver → Gold ────────────────────────────────────────────────────

def build_gold(jobs_df: pd.DataFrame, news_df: pd.DataFrame, mca_df: pd.DataFrame):
    """Aggregate to city level and company level."""

    # ── Company hiring counts from job listings ──
    if not jobs_df.empty and "company_norm" in jobs_df.columns:
        company_hiring = (
            jobs_df.groupby(["company_norm","city"])
            .agg(open_positions=("company_norm","count"),
                 latest_posting=("posted_date","max"))
            .reset_index()
        )
    else:
        company_hiring = pd.DataFrame(columns=["company_norm","city","open_positions","latest_posting"])

    # ── Company news signals ──
    if not news_df.empty and "company_norm" in news_df.columns:
        company_news = (
            news_df.groupby(["company_norm","city"])
            .agg(
                total_layoffs  =("headcount",   lambda x: x[news_df.loc[x.index,"signal_type"]=="layoff"].sum() if "signal_type" in news_df.columns else 0),
                avg_sentiment  =("signal_score","mean"),
                layoff_articles=("signal_type", lambda x: (x=="layoff").sum()),
                hiring_articles=("signal_type", lambda x: (x=="hiring").sum()),
            )
            .reset_index()
        )
    else:
        company_news = pd.DataFrame(columns=["company_norm","city","total_layoffs","avg_sentiment","layoff_articles","hiring_articles"])

    # ── Merge ──
    gold_co = pd.merge(company_hiring, company_news, on=["company_norm","city"], how="outer")

    # Add MCA sector info
    if not mca_df.empty and "name_norm" in mca_df.columns:
        mca_slim = mca_df[["name_norm","sector","status"]].rename(columns={"name_norm":"company_norm"})
        gold_co  = pd.merge(gold_co, mca_slim, on="company_norm", how="left")

    gold_co["open_positions"]  = gold_co.get("open_positions",  pd.Series(dtype=int)).fillna(0).astype(int)
    gold_co["total_layoffs"]   = gold_co.get("total_layoffs",   pd.Series(dtype=int)).fillna(0).astype(int)
    gold_co["avg_sentiment"]   = gold_co.get("avg_sentiment",   pd.Series(dtype=float)).fillna(0.0)
    gold_co["sector"]          = gold_co.get("sector",          pd.Series(dtype=str)).fillna("Technology")
    gold_co["company_status"]  = gold_co["avg_sentiment"].apply(
        lambda s: "layoff" if s < -0.3 else ("hiring" if s > 0.3 else "mixed")
    )
    gold_co["run_ts"]      = RUN_TS
    gold_co["updated_at"]  = TODAY

    # ── District (city) aggregation ──
    gold_city = (
        gold_co.groupby("city")
        .agg(
            total_hiring    =("open_positions",  "sum"),
            total_layoffs   =("total_layoffs",   "sum"),
            total_companies =("company_norm",    "count"),
            avg_sentiment   =("avg_sentiment",   "mean"),
            hiring_companies=("company_status",  lambda x: (x=="hiring").sum()),
            layoff_companies=("company_status",  lambda x: (x=="layoff").sum()),
            it_companies    =("sector",          lambda x: (x=="Technology").sum()),
        )
        .reset_index()
    )
    gold_city["growth_score"] = (
        (gold_city["total_hiring"] - gold_city["total_layoffs"]) /
        gold_city["total_companies"].clip(lower=1)
    ).round(2)
    gold_city["run_ts"]     = RUN_TS
    gold_city["updated_at"] = TODAY

    # ── Sector aggregation ──
    if "sector" in gold_co.columns:
        gold_sector = (
            gold_co.groupby("sector")
            .agg(total_hiring  =("open_positions","sum"),
                 total_layoffs =("total_layoffs",  "sum"),
                 total_companies=("company_norm",  "count"))
            .reset_index()
            .sort_values("total_hiring", ascending=False)
        )
    else:
        gold_sector = pd.DataFrame(columns=["sector","total_hiring","total_layoffs","total_companies"])

    log.info(f"Gold companies: {len(gold_co)} | Gold cities: {len(gold_city)} | Gold sectors: {len(gold_sector)}")
    return gold_co, gold_city, gold_sector


# ── Export Gold JSON ─────────────────────────────────────────────────

def export_gold(gold_co: pd.DataFrame, gold_city: pd.DataFrame, gold_sector: pd.DataFrame):

    # ── india_summary.json ──
    districts = []
    for _, row in gold_city.iterrows():
        city   = row.get("city","")
        coords = CITY_COORDS.get(city, [20.5937, 78.9629])
        districts.append({
            "city":             city,
            "lat":              coords[0],
            "lng":              coords[1],
            "total_hiring":     int(row.get("total_hiring",   0)),
            "total_layoffs":    int(row.get("total_layoffs",  0)),
            "total_companies":  int(row.get("total_companies",0)),
            "it_companies":     int(row.get("it_companies",   0)),
            "hiring_companies": int(row.get("hiring_companies",0)),
            "layoff_companies": int(row.get("layoff_companies",0)),
            "avg_sentiment":    round(float(row.get("avg_sentiment", 0)), 3),
            "growth_score":     round(float(row.get("growth_score",  0)), 2),
            "updated_at":       TODAY,
        })

    upload_json("india_summary.json", {
        "updated_at":     TODAY,
        "run_ts":         RUN_TS,
        "district_count": len(districts),
        "districts":      districts,
    })

    # ── sectors.json ──
    upload_json("sectors.json", {
        "updated_at": TODAY,
        "sectors": gold_sector.to_dict(orient="records"),
    })

    # ── companies_{city}.json ──
    if not gold_co.empty and "city" in gold_co.columns:
        for city, group in gold_co.groupby("city"):
            records = []
            for _, row in group.iterrows():
                rec = {}
                for k, v in row.items():
                    if pd.isna(v) if not isinstance(v, (list, dict)) else False:
                        rec[k] = 0 if k in ("open_positions","total_layoffs","avg_sentiment") else ""
                    elif isinstance(v, float):
                        rec[k] = int(v) if v == int(v) else round(v, 3)
                    else:
                        rec[k] = v
                records.append(rec)
            safe = city.lower().replace(" ","_").replace("/","_")
            upload_json(f"companies_{safe}.json", {
                "city":       city,
                "updated_at": TODAY,
                "companies":  records,
            })

    # ── meta.json ──
    upload_json("meta.json", {
        "updated_at":      TODAY,
        "run_ts":          RUN_TS,
        "total_companies": len(gold_co),
        "total_districts": len(gold_city),
        "pipeline_version":"2.0.0",
        "engine":          "python-pandas",
    })

    log.info(f"Export complete — {len(districts)} cities, {len(gold_co)} companies")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    log.info(f"[HireWatch ETL] Starting — run_ts={RUN_TS}")
    log.info(f"  Bronze bucket : s3://{S3_BUCKET}")
    log.info(f"  Gold bucket   : s3://{OUT_BUCKET}")

    # Bronze → Silver
    jobs_df   = build_silver_jobs()
    news_df   = build_silver_news()
    mca_df    = build_silver_mca()

    # Silver → Gold
    gold_co, gold_city, gold_sector = build_gold(jobs_df, news_df, mca_df)

    # Export to S3
    export_gold(gold_co, gold_city, gold_sector)

    log.info("[HireWatch ETL] ✓ Complete")


if __name__ == "__main__":
    main()
