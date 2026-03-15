# Databricks Notebook — HireWatch India ETL
# Run as a scheduled job via REST API (triggered by GitHub Actions)
# Cluster: Community Edition (single-node, 15GB RAM)
#
# Pipeline: Bronze → Silver → Gold → JSON export to S3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Cell 1: Setup & Imports ────────────────────────────────────────
# dbutils.widgets.text("s3_bucket",    "hirewatch-raw-india")
# dbutils.widgets.text("output_bucket","hirewatch-gold-india")
# dbutils.widgets.text("run_ts",       "")

import json
import re
import math
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F, types as T, DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

S3_BUCKET    = dbutils.widgets.get("s3_bucket")     or "hirewatch-raw-india"
OUT_BUCKET   = dbutils.widgets.get("output_bucket") or "hirewatch-gold-india"
RUN_TS       = dbutils.widgets.get("run_ts")        or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Validate required config
if not S3_BUCKET or not OUT_BUCKET:
    raise ValueError("s3_bucket and output_bucket widgets must be set. "
                     "Go to View > Widget panel and set them before running.")
DELTA_BASE   = "/mnt/hirewatch/delta"
NOW          = datetime.now(timezone.utc)
TODAY        = NOW.strftime("%Y-%m-%d")

print(f"[HireWatch ETL] run_ts={RUN_TS}  s3={S3_BUCKET}  out={OUT_BUCKET}")

# ── Cell 2: Mount S3 (if not already mounted) ─────────────────────
def mount_s3(bucket: str, mount_point: str) -> None:
    mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    if mount_point in mounts:
        print(f"Already mounted: {mount_point}")
        return
    # AWS credentials — set as Databricks secrets or Spark config
    aws_key    = dbutils.secrets.get(scope="hirewatch", key="aws_access_key_id")
    aws_secret = dbutils.secrets.get(scope="hirewatch", key="aws_secret_access_key")
    dbutils.fs.mount(
        source       = f"s3a://{bucket}",
        mount_point  = mount_point,
        extra_configs= {
            "fs.s3a.access.key":    aws_key,
            "fs.s3a.secret.key":    aws_secret,
            "fs.s3a.impl":          "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }
    )
    print(f"Mounted s3a://{bucket} → {mount_point}")

mount_s3(S3_BUCKET,  "/mnt/hirewatch/raw")
mount_s3(OUT_BUCKET, "/mnt/hirewatch/gold")

# ── Cell 3: Bronze Layer — Load raw JSON from S3 ──────────────────
def load_bronze_source(source_prefix: str) -> DataFrame:
    """Load all JSON files from a bronze prefix into a DataFrame."""
    path = f"/mnt/hirewatch/raw/source={source_prefix}/"
    try:
        df = spark.read.option("multiLine", True).json(path)
        # Explode records array from our envelope structure
        if "records" in df.columns:
            df = df.select(F.explode("records").alias("r")).select("r.*")
        elif "jobs" in df.columns:
            df = df.select(F.explode("jobs").alias("j")).select("j.*")
        return df
    except Exception as e:
        print(f"[WARN] Could not load {source_prefix}: {e}")
        return spark.createDataFrame([], T.StructType([]))


bronze_naukri   = load_bronze_source("naukri")
bronze_linkedin = load_bronze_source("linkedin")
bronze_news     = load_bronze_source("news")
bronze_epfo     = load_bronze_source("epfo")
bronze_mca      = load_bronze_source("mca")

print(f"Bronze counts — naukri:{bronze_naukri.count()}  linkedin:{bronze_linkedin.count()}  "
      f"news:{bronze_news.count()}  epfo:{bronze_epfo.count()}  mca:{bronze_mca.count()}")

# ── Cell 4: Silver — Company Name Normalisation ───────────────────
# Install fuzzy matching (CE allows pip installs)
# Uncomment the line below when running as a Databricks notebook cell:
# %pip install rapidfuzz==3.6.1 -q
# When running via REST API job, add rapidfuzz to your cluster init script instead.

from rapidfuzz import process as fuzz_process, fuzz

# Known canonical company names (from MCA seed + manual curation)
CANONICAL_COMPANIES = [
    "Infosys", "TCS", "Wipro", "HCL Technologies", "Tech Mahindra",
    "Cognizant", "Capgemini", "Accenture India", "LTIMindtree",
    "Mphasis", "Hexaware", "Persistent Systems", "Zensar Technologies",
    "NIIT Technologies", "Flipkart", "Swiggy", "Zomato", "Paytm",
    "Ola", "PhonePe", "Meesho", "Razorpay", "CRED", "Groww",
    "Zerodha", "Freshworks", "Zoho", "MakeMyTrip", "Nykaa",
    "Byju's", "Unacademy", "upGrad", "PolicyBazaar", "BankBazaar",
    "BigBasket", "Dunzo", "Urban Company", "Lenskart", "Cars24",
    "OYO Rooms", "Delhivery", "Mamaearth", "boAt Lifestyle",
    "Amazon India", "Google India", "Microsoft India", "IBM India",
    "SAP India", "Oracle India", "Salesforce India", "Adobe India",
    "Uber India", "Airbnb India",
]

@F.udf(T.StringType())
def normalise_company_name(name: str) -> str:
    """Fuzzy-match a company name to its canonical form."""
    if not name or len(name) < 3:
        return name or ""
    result = fuzz_process.extractOne(
        name,
        CANONICAL_COMPANIES,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=72,
    )
    if result:
        return result[0]
    return name.strip().title()


@F.udf(T.StringType())
def normalise_city(raw: str) -> str:
    """Map raw location strings to canonical Indian city names."""
    if not raw:
        return ""
    city_map = {
        "bangalore": "Bengaluru", "bengaluru": "Bengaluru",
        "bombay":    "Mumbai",    "mumbai":    "Mumbai",
        "delhi":     "Delhi",     "new delhi": "Delhi",
        "ncr":       "Delhi",     "gurgaon":   "Gurugram",
        "gurugram":  "Gurugram",  "noida":     "Noida",
        "hyderabad": "Hyderabad", "hyd":       "Hyderabad",
        "chennai":   "Chennai",   "madras":    "Chennai",
        "kolkata":   "Kolkata",   "calcutta":  "Kolkata",
        "pune":      "Pune",      "ahmedabad": "Ahmedabad",
        "amdavad":   "Ahmedabad", "jaipur":    "Jaipur",
        "kochi":     "Kochi",     "cochin":    "Kochi",
        "indore":    "Indore",    "nagpur":    "Nagpur",
        "surat":     "Surat",     "vadodara":  "Vadodara",
        "vizag":     "Visakhapatnam", "visakhapatnam": "Visakhapatnam",
        "chandigarh":"Chandigarh","lucknow":   "Lucknow",
    }
    key = raw.lower().strip()
    for k, v in city_map.items():
        if k in key:
            return v
    # Return title-cased fallback
    return raw.strip().title()


# ── Cell 5: Silver — Job Listings ─────────────────────────────────
job_schema = T.StructType([
    T.StructField("company",      T.StringType()),
    T.StructField("title",        T.StringType()),
    T.StructField("city_raw",     T.StringType()),
    T.StructField("posted_date",  T.StringType()),
    T.StructField("source",       T.StringType()),
    T.StructField("scraped_at",   T.StringType()),
    T.StructField("job_url",      T.StringType()),
])

def build_silver_jobs(naukri_df: DataFrame, linkedin_df: DataFrame) -> DataFrame:
    cols = ["company", "title", "city_key", "posted_date", "source", "scraped_at", "job_url"]

    def align(df: DataFrame) -> DataFrame:
        for c in cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast(T.StringType()))
        return df.select(*cols)

    combined = align(naukri_df).unionByName(align(linkedin_df), allowMissingColumns=True)

    silver = (
        combined
        .withColumn("company_norm", normalise_company_name(F.col("company")))
        .withColumn("city",         normalise_city(F.col("city_key")))
        .withColumn("posted_date",  F.coalesce(
            F.to_date("posted_date", "yyyy-MM-dd"),
            F.current_date()
        ))
        .withColumn("is_recent", F.col("posted_date") >= F.lit(TODAY))
        .filter(F.col("company_norm").isNotNull() & (F.length("company_norm") > 2))
        .dropDuplicates(["company_norm", "title", "city"])
    )
    return silver


silver_jobs = build_silver_jobs(bronze_naukri, bronze_linkedin)
print(f"Silver jobs: {silver_jobs.count()}")

# ── Cell 6: Silver — News Signals ─────────────────────────────────
def build_silver_news(news_df: DataFrame) -> DataFrame:
    if news_df.rdd.isEmpty():
        return news_df

    return (
        news_df
        .withColumn("company_norm", normalise_company_name(F.col("company")))
        .withColumn("city",         normalise_city(F.col("city")))
        .filter(F.col("signal_type").isin(["layoff", "hiring", "neutral"]))
        .withColumn("published_at", F.coalesce(
            F.to_date("published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_date("published_at", "EEE, dd MMM yyyy HH:mm:ss zzz"),
            F.current_date()
        ))
        .filter(F.col("published_at") >= F.lit((NOW - timedelta(days=30)).strftime("%Y-%m-%d")))
        .dropDuplicates(["url"])
    )


silver_news = build_silver_news(bronze_news)
print(f"Silver news: {silver_news.count()}")

# ── Cell 7: Silver — MCA Company Master ───────────────────────────
def build_silver_mca(mca_df: DataFrame) -> DataFrame:
    if mca_df.rdd.isEmpty():
        return mca_df
    return (
        mca_df
        .withColumn("name_norm", normalise_company_name(F.col("name")))
        .withColumn("city",      normalise_city(F.col("city")))
        .dropDuplicates(["name_norm"])
        .select("name_norm", "cin", "state", "city", "sector", "status",
                "incorporation_date", "authorized_capital", "paid_up_capital")
    )


silver_mca = build_silver_mca(bronze_mca)
print(f"Silver MCA: {silver_mca.count()}")

# ── Cell 8: AI — Signal Strength Scoring ──────────────────────────
# Simple rule-based scoring — replaces heavyweight HuggingFace
# in Community Edition (limited RAM). Switch to DistilBERT on
# a proper cluster for higher accuracy.

@F.udf(T.FloatType())
def layoff_signal_strength(signal_type: str, headcount) -> float:
    """0.0 = neutral, +1.0 = strong hiring, -1.0 = strong layoff."""
    if signal_type == "layoff":
        hc = headcount or 0
        if   hc > 5000: return -1.0
        elif hc > 1000: return -0.8
        elif hc > 100:  return -0.6
        else:           return -0.4
    elif signal_type == "hiring":
        hc = headcount or 0
        if   hc > 5000: return  1.0
        elif hc > 1000: return  0.8
        elif hc > 100:  return  0.6
        else:           return  0.4
    return 0.0


silver_news_scored = silver_news.withColumn(
    "signal_score",
    layoff_signal_strength(F.col("signal_type"), F.col("headcount"))
)

# ── Cell 9: Gold — Company-Level Aggregation ──────────────────────
# Join job listings with news signals to compute per-company metrics

company_hiring = (
    silver_jobs
    .filter(F.col("is_recent"))
    .groupBy("company_norm", "city")
    .agg(
        F.count("*").alias("open_positions"),
        F.countDistinct("title").alias("distinct_roles"),
        F.max("posted_date").alias("latest_posting"),
    )
)

company_news = (
    silver_news_scored
    .groupBy("company_norm", "city")
    .agg(
        F.sum(F.when(F.col("signal_type") == "layoff", F.coalesce("headcount", F.lit(0))).otherwise(0)).alias("total_layoffs"),
        F.sum(F.when(F.col("signal_type") == "hiring", F.coalesce("headcount", F.lit(0))).otherwise(0)).alias("news_hires"),
        F.avg("signal_score").alias("avg_sentiment"),
        F.count(F.when(F.col("signal_type") == "layoff", 1)).alias("layoff_articles"),
        F.count(F.when(F.col("signal_type") == "hiring", 1)).alias("hiring_articles"),
    )
)

# Merge MCA master data
company_gold = (
    company_hiring
    .join(company_news,  ["company_norm", "city"], "left")
    .join(silver_mca.withColumnRenamed("name_norm", "company_norm"), ["company_norm"], "left")
    .withColumn("company_status",
        F.when(F.col("avg_sentiment") < -0.3, "layoff")
         .when(F.col("avg_sentiment") >  0.3, "hiring")
         .otherwise("mixed")
    )
    .withColumn("total_layoffs",   F.coalesce("total_layoffs",  F.lit(0)))
    .withColumn("open_positions",  F.coalesce("open_positions", F.lit(0)))
    .withColumn("sector",          F.coalesce("sector",         F.lit("Technology")))
    .withColumn("run_ts",          F.lit(RUN_TS))
    .withColumn("updated_at",      F.lit(TODAY))
)

print(f"Gold companies: {company_gold.count()}")

# ── Cell 10: Gold — District-Level Aggregation ────────────────────
district_gold = (
    company_gold
    .groupBy("city")
    .agg(
        F.sum("open_positions").alias("total_hiring"),
        F.sum("total_layoffs").alias("total_layoffs"),
        F.count("company_norm").alias("total_companies"),
        F.avg("avg_sentiment").alias("avg_sentiment"),
        F.sum(F.when(F.col("company_status") == "hiring", 1).otherwise(0)).alias("hiring_companies"),
        F.sum(F.when(F.col("company_status") == "layoff", 1).otherwise(0)).alias("layoff_companies"),
        F.sum(F.when(F.col("sector") == "Technology", 1).otherwise(0)).alias("it_companies"),
        F.collect_set("sector").alias("sectors"),
    )
    .withColumn("growth_score",
        (F.col("total_hiring") - F.col("total_layoffs")) /
        F.greatest(F.col("total_companies"), F.lit(1))
    )
    .withColumn("run_ts",     F.lit(RUN_TS))
    .withColumn("updated_at", F.lit(TODAY))
)

print(f"Gold districts: {district_gold.count()}")

# ── Cell 11: Gold — Sector Breakdown ──────────────────────────────
sector_gold = (
    company_gold
    .groupBy("sector")
    .agg(
        F.sum("open_positions").alias("total_hiring"),
        F.sum("total_layoffs").alias("total_layoffs"),
        F.count("company_norm").alias("total_companies"),
    )
    .orderBy(F.desc("total_hiring"))
)

# ── Cell 12: Write Delta tables ────────────────────────────────────
def write_delta(df: DataFrame, table_name: str) -> None:
    path = f"{DELTA_BASE}/{table_name}"
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "true")
          .save(path)
    )
    print(f"Written delta: {path}  rows={df.count()}")


write_delta(company_gold,  "gold_companies")
write_delta(district_gold, "gold_districts")
write_delta(sector_gold,   "gold_sectors")

# ── Cell 13: Export JSON for GitHub Pages dashboard ───────────────
def df_to_json_list(df: DataFrame) -> list:
    return [row.asDict() for row in df.collect()]


# 1) india_summary.json — district-level metrics for map
districts_list = df_to_json_list(
    district_gold
    .withColumn("city",           F.col("city"))
    .withColumn("total_hiring",   F.col("total_hiring").cast(T.LongType()))
    .withColumn("total_layoffs",  F.col("total_layoffs").cast(T.LongType()))
    .withColumn("total_companies",F.col("total_companies").cast(T.LongType()))
    .withColumn("it_companies",   F.col("it_companies").cast(T.LongType()))
    .withColumn("growth_score",   F.round("growth_score", 2))
)

# Merge with known lat/lng for map markers
CITY_COORDS = {
    "Bengaluru":       [12.9716, 77.5946],
    "Mumbai":          [19.0760, 72.8777],
    "Hyderabad":       [17.3850, 78.4867],
    "Chennai":         [13.0827, 80.2707],
    "Delhi":           [28.6139, 77.2090],
    "Gurugram":        [28.4595, 77.0266],
    "Noida":           [28.5355, 77.3910],
    "Pune":            [18.5204, 73.8567],
    "Kolkata":         [22.5726, 88.3639],
    "Ahmedabad":       [23.0225, 72.5714],
    "Jaipur":          [26.9124, 75.7873],
    "Kochi":           [9.9312,  76.2673],
    "Indore":          [22.7196, 75.8577],
    "Nagpur":          [21.1458, 79.0882],
    "Surat":           [21.1702, 72.8311],
    "Visakhapatnam":   [17.6868, 83.2185],
    "Chandigarh":      [30.7333, 76.7794],
    "Lucknow":         [26.8467, 80.9462],
    "Vadodara":        [22.3072, 73.1812],
    "Coimbatore":      [11.0168, 76.9558],
}

for d in districts_list:
    coords = CITY_COORDS.get(d.get("city", ""), [20.5937, 78.9629])
    d["lat"] = coords[0]
    d["lng"] = coords[1]
    # Convert any non-serialisable types
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif v is None:
            d[k] = 0 if k in ("total_hiring","total_layoffs","total_companies") else ""

india_summary = {
    "updated_at":   TODAY,
    "run_ts":       RUN_TS,
    "district_count": len(districts_list),
    "districts":    districts_list,
}

# 2) companies_{city}.json — per-city company detail
city_company_map: dict = {}
for row in company_gold.collect():
    city = row["city"] or "Unknown"
    if city not in city_company_map:
        city_company_map[city] = []
    rec = row.asDict()
    for k, v in rec.items():
        if hasattr(v, "isoformat"):
            rec[k] = v.isoformat()
        elif v is None:
            rec[k] = 0 if "count" in k or "total" in k else ""
    city_company_map[city].append(rec)

# 3) sectors.json
sectors_list = df_to_json_list(sector_gold)

# 4) meta.json
meta = {
    "updated_at":       TODAY,
    "run_ts":           RUN_TS,
    "total_companies":  company_gold.count(),
    "total_districts":  district_gold.count(),
    "pipeline_version": "1.0.0",
}

# ── Cell 14: Write JSON to S3 (Gold output bucket) ────────────────
import boto3

s3_client = boto3.client(
    "s3",
    aws_access_key_id     = dbutils.secrets.get(scope="hirewatch", key="aws_access_key_id"),
    aws_secret_access_key = dbutils.secrets.get(scope="hirewatch", key="aws_secret_access_key"),
)

def upload_json(key: str, data) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    s3_client.put_object(
        Bucket      = OUT_BUCKET,
        Key         = f"data/{key}",
        Body        = body.encode(),
        ContentType = "application/json",
        CacheControl= "public, max-age=3600",
    )
    print(f"Uploaded: s3://{OUT_BUCKET}/data/{key}  ({len(body)} bytes)")


upload_json("meta.json",          meta)
upload_json("india_summary.json", india_summary)
upload_json("sectors.json",       {"sectors": sectors_list})

for city, companies in city_company_map.items():
    safe_city = city.lower().replace(" ", "_").replace("/", "_")
    upload_json(f"companies_{safe_city}.json", {
        "city":      city,
        "updated_at": TODAY,
        "companies": companies,
    })

print(f"\n[HireWatch ETL] ✓ Complete — {len(city_company_map)} cities exported")
print(f"  Total companies : {company_gold.count()}")
print(f"  Total districts : {district_gold.count()}")
print(f"  S3 output bucket: s3://{OUT_BUCKET}/data/")
