"""
hirewatch_etl.py
────────────────
HireWatch India — Bronze → Silver → Gold ETL
- Auto-discovers NEW companies from every scraper run
- Tracks 8-week rolling hiring/layoff trends per company
- Classifies sector automatically from company name + news
- Writes companies_all.json so dashboard always shows live data

Environment variables:
  S3_BUCKET        — raw bronze bucket  (hirewatch-raw-india)
  S3_OUTPUT_BUCKET — gold output bucket (hirewatch-gold-india)
  RUN_TS           — GitHub Actions run ID
"""
from __future__ import annotations
import json, os, re, logging
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3
import pandas as pd
from rapidfuzz import process as fuzz_process, fuzz

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

S3_BUCKET  = os.environ["S3_BUCKET"]
OUT_BUCKET = os.environ["S3_OUTPUT_BUCKET"]
RUN_TS     = os.getenv("RUN_TS", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
TODAY      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
NOW        = datetime.now(timezone.utc)
s3         = boto3.client("s3", region_name="ap-south-2")

# ── Canonical company names for fuzzy matching ───────────────────────
CANONICAL = [
    "Infosys","TCS","Wipro","HCL Technologies","Tech Mahindra","Cognizant",
    "Capgemini","Accenture India","LTIMindtree","Mphasis","Hexaware",
    "Persistent Systems","Zensar Technologies","Flipkart","Swiggy","Zomato",
    "Paytm","Ola","PhonePe","Meesho","Razorpay","CRED","Groww","Zerodha",
    "Freshworks","Zoho","MakeMyTrip","Nykaa","Byju's","Unacademy","upGrad",
    "PolicyBazaar","BigBasket","Delhivery","Amazon India","Google India",
    "Microsoft India","IBM India","SAP India","Oracle India","Salesforce India",
    "Adobe India","Uber India","IndiGo","Air India","HDFC Bank","ICICI Bank",
    "Reliance Industries","Adani Enterprises","Tata Consultancy Services",
    "Netflix India","Atlassian","Block","Intel","Cyient","UST Global",
    "Hero MotoCorp","OYO Rooms","ITC Limited","Coal India","Torrent Pharma",
    "HCL Technologies","Bajaj Auto","Ashok Leyland","BHEL","Apollo Hospitals",
    "TVS Motors","Info Edge","Jio","Zepto","Dunzo","Cars24","Lenskart",
    "Urban Company","boAt Lifestyle","Mamaearth","Naukri","Quikr",
]

# ── Sector auto-classification keywords ──────────────────────────────
SECTOR_KEYWORDS = {
    "IT Services":    ["infosys","wipro","tcs","hcl","tech mahindra","cognizant","capgemini",
                       "accenture","ltimindtree","mphasis","hexaware","persistent","zensar",
                       "ust","niit","mastech","kpit"],
    "SaaS/Product":   ["freshworks","zoho","browserstack","chargebee","postman","hasura",
                       "druva","icertis","pubmatic","the math company"],
    "Fintech":        ["paytm","razorpay","phonepe","cred","groww","zerodha","policybazaar",
                       "bankbazaar","slice","jupiter","fi money","navi","freo","lendingkart"],
    "E-Commerce":     ["flipkart","amazon","meesho","nykaa","myntra","snapdeal","shopclues",
                       "limeroad","firstcry","purplle","zivame"],
    "Food Tech":      ["swiggy","zomato","dunzo","bigbasket","zepto","blinkit","juspay"],
    "EdTech":         ["byju","unacademy","upgrad","vedantu","physicswallah","toppr","doubtnut",
                       "simplilearn","skill-lync","testbook"],
    "Travel/Mobility":["makemytrip","ola","uber","indigo","air india","goibibo","yatra",
                       "cleartrip","rapido","bounce","vogo"],
    "Healthcare":     ["apollo","fortis","narayana","manipal","1mg","pharmeasy","practo",
                       "mfine","healthkart","cult.fit"],
    "Media/Gaming":   ["netflix","hotstar","zee","sony","times","jio cinema","dream11",
                       "mpl","nazara","games24x7"],
    "Banking/Finance":["hdfc","icici","sbi","axis","kotak","indusind","yes bank","bajaj finance"],
    "Manufacturing":  ["tata motors","mahindra","hero","bajaj","tvs","ashok leyland","maruti",
                       "bhel","l&t","vedanta","jsw","tata steel"],
    "Infrastructure": ["adani","reliance","gmr","gvk","nhai","irfc","rites"],
    "FMCG":           ["itc","hindustan unilever","dabur","marico","godrej","emami","patanjali"],
    "Real Estate":    ["dlf","sobha","godrej properties","brigade","prestige","oberoi realty"],
    "Logistics":      ["delhivery","ecom express","xpressbees","shadowfax","bluedart","fedex india"],
    "Consulting":     ["mckinsey","bcg","bain","deloitte","pwc","ey","kpmg","roland berger"],
    "Cloud/IT":       ["google","microsoft","amazon web","oracle","sap","salesforce","adobe",
                       "servicenow","workday","vmware"],
}

CITY_MAP = {
    "bangalore":"Bengaluru","bengaluru":"Bengaluru","bombay":"Mumbai","mumbai":"Mumbai",
    "delhi":"Delhi","new delhi":"Delhi","ncr":"Delhi","gurgaon":"Gurugram","gurugram":"Gurugram",
    "noida":"Noida","hyderabad":"Hyderabad","hyd":"Hyderabad","chennai":"Chennai",
    "madras":"Chennai","kolkata":"Kolkata","calcutta":"Kolkata","pune":"Pune",
    "ahmedabad":"Ahmedabad","amdavad":"Ahmedabad","jaipur":"Jaipur","kochi":"Kochi",
    "cochin":"Kochi","indore":"Indore","nagpur":"Nagpur","surat":"Surat",
    "chandigarh":"Chandigarh","lucknow":"Lucknow","bhopal":"Bhopal","coimbatore":"Coimbatore",
    "vadodara":"Vadodara","baroda":"Vadodara","visakhapatnam":"Visakhapatnam","vizag":"Visakhapatnam",
    "patna":"Patna","ludhiana":"Ludhiana","mysore":"Mysuru","mysuru":"Mysuru",
    "thiruvananthapuram":"Thiruvananthapuram","trivandrum":"Thiruvananthapuram",
    "bhubaneswar":"Bhubaneswar","dehradun":"Dehradun","nashik":"Nashik","rajkot":"Rajkot",
    "madurai":"Madurai","ranchi":"Ranchi","guwahati":"Guwahati","jodhpur":"Jodhpur",
    "vijayawada":"Vijayawada","faridabad":"Faridabad","ghaziabad":"Ghaziabad",
    "hubli":"Hubballi","hubballi":"Hubballi","mangalore":"Mangaluru","mangaluru":"Mangaluru",
    "udaipur":"Udaipur","aurangabad":"Aurangabad","thane":"Thane","kota":"Kota",
    "kozhikode":"Kozhikode","calicut":"Kozhikode","warangal":"Warangal","raipur":"Raipur",
    "varanasi":"Varanasi","agra":"Agra","amritsar":"Amritsar","jabalpur":"Jabalpur",
    "meerut":"Meerut","navi-mumbai":"Navi Mumbai","navi mumbai":"Navi Mumbai",
    "tiruchirappalli":"Tiruchirappalli","trichy":"Tiruchirappalli","guntur":"Guntur",
}

CITY_COORDS = {
    "Bengaluru":[12.9716,77.5946],"Mumbai":[19.0760,72.8777],"Hyderabad":[17.3850,78.4867],
    "Chennai":[13.0827,80.2707],"Delhi":[28.6139,77.2090],"Gurugram":[28.4595,77.0266],
    "Noida":[28.5355,77.3910],"Pune":[18.5204,73.8567],"Kolkata":[22.5726,88.3639],
    "Ahmedabad":[23.0225,72.5714],"Jaipur":[26.9124,75.7873],"Kochi":[9.9312,76.2673],
    "Indore":[22.7196,75.8577],"Nagpur":[21.1458,79.0882],"Surat":[21.1702,72.8311],
    "Chandigarh":[30.7333,76.7794],"Lucknow":[26.8467,80.9462],"Bhopal":[23.2599,77.4126],
    "Coimbatore":[11.0168,76.9558],"Vadodara":[22.3072,73.1812],"Visakhapatnam":[17.6868,83.2185],
    "Patna":[25.5941,85.1376],"Ludhiana":[30.9010,75.8573],"Mysuru":[12.2958,76.6394],
    "Thiruvananthapuram":[8.5241,76.9366],"Bhubaneswar":[20.2961,85.8245],
    "Dehradun":[30.3165,78.0322],"Nashik":[19.9975,73.7898],"Rajkot":[22.3039,70.8022],
    "Madurai":[9.9252,78.1198],"Ranchi":[23.3441,85.3096],"Guwahati":[26.1445,91.7362],
    "Jodhpur":[26.2389,73.0243],"Vijayawada":[16.5062,80.6480],"Faridabad":[28.4089,77.3178],
    "Ghaziabad":[28.6692,77.4538],"Hubballi":[15.3647,75.1240],"Mangaluru":[12.9141,74.8560],
    "Udaipur":[24.5854,73.7125],"Aurangabad":[19.8762,75.3433],"Thane":[19.2183,72.9781],
    "Kota":[25.2138,75.8648],"Kozhikode":[11.2588,75.7804],"Warangal":[17.9689,79.5941],
    "Raipur":[21.2514,81.6296],"Varanasi":[25.3176,82.9739],"Agra":[27.1767,78.0081],
    "Amritsar":[31.6340,74.8723],"Jabalpur":[23.1815,79.9864],"Meerut":[28.9845,77.7064],
    "Navi Mumbai":[19.0330,73.0297],"Tiruchirappalli":[10.7905,78.7047],"Guntur":[16.3067,80.4365],
}

# ── Helper functions ──────────────────────────────────────────────────

def normalise_company(name: str) -> str:
    if not name or len(str(name).strip()) < 2:
        return ""
    name = str(name).strip()
    result = fuzz_process.extractOne(name, CANONICAL, scorer=fuzz.token_sort_ratio, score_cutoff=72)
    return result[0] if result else name.strip().title()

def normalise_city(raw: str) -> str:
    if not raw: return ""
    key = str(raw).lower().strip()
    for k, v in CITY_MAP.items():
        if k in key: return v
    return str(raw).strip().title()

def classify_sector(name: str, description: str = "") -> str:
    text = (name + " " + description).lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return sector
    return "Other"

def signal_score(signal_type: str, headcount) -> float:
    hc = 0
    try: hc = int(str(headcount).replace(",","")) if headcount else 0
    except: pass
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

def list_s3_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except Exception as e:
        log.warning(f"list_s3_keys failed for {prefix}: {e}")
    return keys

def read_s3_json(bucket: str, key: str) -> dict:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception as e:
        log.warning(f"read_s3_json failed for {key}: {e}")
        return {}

def upload_json(key: str, data: Any) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    s3.put_object(Bucket=OUT_BUCKET, Key=f"data/{key}", Body=body.encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    log.info(f"Uploaded → s3://{OUT_BUCKET}/data/{key}  ({len(body)} bytes)")

def load_bronze_source(prefix: str) -> list[dict]:
    keys    = list_s3_keys(S3_BUCKET, f"source={prefix}/")
    records = []
    for key in keys:
        data  = read_s3_json(S3_BUCKET, key)
        items = data.get("records") or data.get("jobs") or []
        if isinstance(items, list):
            records.extend(items)
    log.info(f"Bronze {prefix}: {len(records)} records from {len(keys)} files")
    return records

def safe_int(v, default=0) -> int:
    try:
        f = float(v)
        return int(f) if not (f != f) else default  # NaN check
    except: return default

def safe_float(v, default=0.0) -> float:
    try:
        f = float(v)
        return default if (f != f) else round(f, 3)
    except: return default


# ── Trend tracking: load/save historical company snapshots ───────────

HISTORY_KEY = "history/company_history.json"

def load_company_history() -> dict:
    """Load historical weekly snapshots per company from S3."""
    data = read_s3_json(OUT_BUCKET, HISTORY_KEY)
    return data if data else {}

def save_company_history(history: dict) -> None:
    body = json.dumps(history, ensure_ascii=False, indent=2)
    s3.put_object(Bucket=OUT_BUCKET, Key=HISTORY_KEY, Body=body.encode(),
                  ContentType="application/json")
    log.info(f"Saved company history: {len(history)} companies")

def update_history(history: dict, company_name: str, hiring: int, layoffs: int) -> list[int]:
    """
    Append today's hiring score to a company's 8-week rolling history.
    Returns the updated trend list (up to 8 points).
    """
    if company_name not in history:
        history[company_name] = []
    entry = {"date": TODAY, "hiring": hiring, "layoffs": layoffs,
             "score": hiring - layoffs}
    # Keep one entry per week — only append if date differs from last entry
    entries = history[company_name]
    if not entries or entries[-1].get("date") != TODAY:
        entries.append(entry)
    else:
        entries[-1] = entry  # update today's entry
    # Keep last 8 weeks
    history[company_name] = entries[-8:]
    # Return normalised trend (0-100 scale for sparkline)
    scores = [e["score"] for e in history[company_name]]
    if not scores: return [50]
    mn, mx = min(scores), max(scores)
    rng = mx - mn or 1
    return [max(5, min(100, int((s - mn) / rng * 95) + 5)) for s in scores]

# ── Bronze → Silver ───────────────────────────────────────────────────

def build_silver_jobs() -> pd.DataFrame:
    raw = load_bronze_source("naukri") + load_bronze_source("linkedin")
    if not raw: return pd.DataFrame()
    df = pd.DataFrame(raw)
    city_col = df["city_key"] if "city_key" in df.columns else df.get("location", pd.Series(dtype=str))
    df["company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    df["city"]         = city_col.fillna("").apply(normalise_city)
    df["posted_date"]  = pd.to_datetime(df.get("posted_date", pd.Series(dtype=str)), errors="coerce")
    df = df[df["company_norm"].str.len() > 2].drop_duplicates(subset=["company_norm","city"])
    log.info(f"Silver jobs: {len(df)} rows, {df['company_norm'].nunique()} unique companies")
    return df

def build_silver_news() -> pd.DataFrame:
    raw = load_bronze_source("news")
    if not raw: return pd.DataFrame()
    df = pd.DataFrame(raw)
    df["company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    df["city"]         = df.get("city", pd.Series(dtype=str)).fillna("").apply(normalise_city)
    df["signal_score"] = df.apply(lambda r: signal_score(r.get("signal_type",""), r.get("headcount")), axis=1)
    df["published_at"] = pd.to_datetime(df.get("published_at", pd.Series(dtype=str)), errors="coerce")
    cutoff = NOW - timedelta(days=30)
    df = df[df["published_at"] >= cutoff]
    if "url" in df.columns: df = df.drop_duplicates(subset=["url"])
    log.info(f"Silver news: {len(df)} rows, {df['company_norm'].nunique()} companies mentioned")
    return df

def build_silver_mca() -> pd.DataFrame:
    raw = load_bronze_source("mca")
    if not raw: return pd.DataFrame()
    df = pd.DataFrame(raw)
    df["name_norm"] = df.get("name", pd.Series(dtype=str)).fillna("").apply(normalise_company)
    df["city"]      = df.get("city", pd.Series(dtype=str)).fillna("").apply(normalise_city)
    df = df.drop_duplicates(subset=["name_norm"])
    log.info(f"Silver MCA: {len(df)} companies")
    return df


# ── Silver → Gold (with auto-discovery + trend tracking) ─────────────

def build_gold(jobs_df: pd.DataFrame, news_df: pd.DataFrame, mca_df: pd.DataFrame):

    # Step 1: collect all unique company+city combos from ALL sources
    company_sources = []

    if not jobs_df.empty and "company_norm" in jobs_df.columns:
        job_agg = (jobs_df.groupby(["company_norm","city"])
                   .agg(open_positions=("company_norm","count"),
                        latest_posting=("posted_date","max"))
                   .reset_index())
        company_sources.append(job_agg)

    if not news_df.empty and "company_norm" in news_df.columns:
        news_agg = (news_df.groupby(["company_norm","city"])
                    .agg(total_layoffs_raw=("headcount",
                             lambda x: pd.to_numeric(x[news_df.loc[x.index,"signal_type"]=="layoff"], errors="coerce").sum()
                             if "signal_type" in news_df.columns else 0),
                         avg_sentiment   =("signal_score","mean"),
                         layoff_articles =("signal_type", lambda x: (x=="layoff").sum()),
                         hiring_articles =("signal_type", lambda x: (x=="hiring").sum()),
                         news_titles     =("title",       lambda x: list(x.dropna().unique()[:3])))
                    .reset_index()
                    .rename(columns={"total_layoffs_raw":"total_layoffs"}))
        company_sources.append(news_agg)

    if not mca_df.empty and "name_norm" in mca_df.columns:
        mca_agg = (mca_df[["name_norm","city","sector","status"]]
                   .rename(columns={"name_norm":"company_norm"}))
        company_sources.append(mca_agg)

    # Merge all sources into one unified company table
    if not company_sources:
        log.warning("No company data from any source")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    from functools import reduce
    dfs_with_key = [df for df in company_sources if "company_norm" in df.columns]
    if not dfs_with_key:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    gold_co = reduce(lambda a, b: pd.merge(a, b, on=["company_norm","city"], how="outer"), dfs_with_key)

    # Fill missing values safely
    for col, default in [("open_positions",0),("total_layoffs",0),
                         ("avg_sentiment",0.0),("layoff_articles",0),("hiring_articles",0)]:
        if col in gold_co.columns:
            gold_co[col] = gold_co[col].fillna(default)
        else:
            gold_co[col] = default

    # Auto-classify sector if not from MCA
    if "sector" not in gold_co.columns:
        gold_co["sector"] = gold_co["company_norm"].apply(lambda n: classify_sector(n))
    else:
        gold_co["sector"] = gold_co.apply(
            lambda r: r["sector"] if pd.notna(r["sector"]) and r["sector"] not in ("","nan","Other")
            else classify_sector(r["company_norm"]), axis=1)

    # Determine hiring/layoff status from sentiment
    gold_co["company_status"] = gold_co["avg_sentiment"].apply(
        lambda s: "layoff" if s < -0.3 else ("hiring" if s > 0.3 else "mixed"))

    # Override status: if layoff_articles >> hiring_articles, mark as layoff
    gold_co.loc[gold_co["layoff_articles"] > gold_co["hiring_articles"] * 2, "company_status"] = "layoff"
    gold_co.loc[gold_co["hiring_articles"] > gold_co["layoff_articles"] * 2, "company_status"] = "hiring"

    gold_co["run_ts"]     = RUN_TS
    gold_co["updated_at"] = TODAY
    gold_co = gold_co[gold_co["company_norm"].str.len() > 1]

    log.info(f"Gold companies: {len(gold_co)} | unique: {gold_co['company_norm'].nunique()}")

    # City aggregation
    gold_city = (gold_co.groupby("city")
                 .agg(total_hiring    =("open_positions",  "sum"),
                      total_layoffs   =("total_layoffs",   "sum"),
                      total_companies =("company_norm",    "count"),
                      avg_sentiment   =("avg_sentiment",   "mean"),
                      hiring_companies=("company_status",  lambda x: (x=="hiring").sum()),
                      layoff_companies=("company_status",  lambda x: (x=="layoff").sum()),
                      it_companies    =("sector",          lambda x: (x=="IT Services").sum()))
                 .reset_index())
    gold_city["growth_score"] = ((gold_city["total_hiring"] - gold_city["total_layoffs"]) /
                                  gold_city["total_companies"].clip(lower=1)).round(2)
    gold_city["run_ts"]     = RUN_TS
    gold_city["updated_at"] = TODAY

    # Sector aggregation
    gold_sector = (gold_co.groupby("sector")
                   .agg(total_hiring  =("open_positions","sum"),
                        total_layoffs =("total_layoffs",  "sum"),
                        total_companies=("company_norm",  "count"))
                   .reset_index()
                   .sort_values("total_hiring", ascending=False))

    return gold_co, gold_city, gold_sector


# ── Export Gold JSON ──────────────────────────────────────────────────

def export_gold(gold_co: pd.DataFrame, gold_city: pd.DataFrame, gold_sector: pd.DataFrame):

    # Load company history for trend tracking
    history = load_company_history()
    new_companies_count = 0

    # ── companies_all.json — master company list with trends ──
    all_companies = []
    if not gold_co.empty:
        for _, row in gold_co.iterrows():
            name    = str(row.get("company_norm",""))
            city    = str(row.get("city",""))
            hiring  = safe_int(row.get("open_positions", 0))
            layoffs = safe_int(row.get("total_layoffs",  0))
            sector  = str(row.get("sector","Other"))
            status  = str(row.get("company_status","mixed"))
            sentiment = safe_float(row.get("avg_sentiment", 0.0))

            if not name or not city: continue

            # Track trend + detect new companies
            is_new = name not in history
            if is_new: new_companies_count += 1
            trend = update_history(history, name, hiring, layoffs)

            # Get recent news headlines for this company
            titles = []
            if "news_titles" in row and isinstance(row["news_titles"], list):
                titles = row["news_titles"][:3]

            all_companies.append({
                "name":           name,
                "city":           city,
                "sector":         sector,
                "status":         status,
                "hiring":         hiring,
                "layoffs":        layoffs,
                "sentiment":      sentiment,
                "trend":          trend,
                "is_new":         is_new,
                "news_headlines": titles,
                "updated_at":     TODAY,
                "run_ts":         RUN_TS,
            })

    # Sort: new companies first, then by hiring desc
    all_companies.sort(key=lambda c: (-int(c["is_new"]), -c["hiring"]))

    upload_json("companies_all.json", {
        "updated_at":     TODAY,
        "run_ts":         RUN_TS,
        "total":          len(all_companies),
        "new_this_run":   new_companies_count,
        "companies":      all_companies,
    })
    log.info(f"companies_all.json: {len(all_companies)} companies ({new_companies_count} new this run)")

    # Save updated history back to S3
    save_company_history(history)

    # ── india_summary.json ──
    districts = []
    for _, row in gold_city.iterrows():
        city   = row.get("city","")
        coords = CITY_COORDS.get(city, [20.5937, 78.9629])
        districts.append({
            "city":             city,
            "lat":              coords[0],
            "lng":              coords[1],
            "total_hiring":     safe_int(row.get("total_hiring",   0)),
            "total_layoffs":    safe_int(row.get("total_layoffs",  0)),
            "total_companies":  safe_int(row.get("total_companies",0)),
            "it_companies":     safe_int(row.get("it_companies",   0)),
            "hiring_companies": safe_int(row.get("hiring_companies",0)),
            "layoff_companies": safe_int(row.get("layoff_companies",0)),
            "avg_sentiment":    safe_float(row.get("avg_sentiment", 0.0)),
            "growth_score":     safe_float(row.get("growth_score",  0.0)),
            "updated_at":       TODAY,
        })

    upload_json("india_summary.json", {
        "updated_at": TODAY, "run_ts": RUN_TS,
        "district_count": len(districts), "districts": districts,
    })

    # ── sectors.json ──
    upload_json("sectors.json", {
        "updated_at": TODAY,
        "sectors": [{k: safe_int(v) if k!="sector" else v for k,v in row.items()}
                    for row in gold_sector.to_dict(orient="records")],
    })

    # ── meta.json ──
    upload_json("meta.json", {
        "updated_at":       TODAY,
        "run_ts":           RUN_TS,
        "total_companies":  len(all_companies),
        "new_companies":    new_companies_count,
        "total_districts":  len(districts),
        "pipeline_version": "3.0.0",
        "engine":           "python-pandas-autodiscovery",
    })

    log.info(f"Export complete — {len(districts)} cities, {len(all_companies)} companies")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    log.info(f"[HireWatch ETL v3] Starting — run_ts={RUN_TS}")
    log.info(f"  Bronze : s3://{S3_BUCKET}")
    log.info(f"  Gold   : s3://{OUT_BUCKET}")

    jobs_df  = build_silver_jobs()
    news_df  = build_silver_news()
    mca_df   = build_silver_mca()

    gold_co, gold_city, gold_sector = build_gold(jobs_df, news_df, mca_df)
    export_gold(gold_co, gold_city, gold_sector)

    log.info("[HireWatch ETL v3] ✓ Complete")

if __name__ == "__main__":
    main()
