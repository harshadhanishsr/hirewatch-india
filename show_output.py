"""
show_output.py — reads from S3 and prints pipeline results.
Set env vars before running:
  $env:AWS_ACCESS_KEY_ID="your_key"
  $env:AWS_SECRET_ACCESS_KEY="your_secret"
  $env:AWS_DEFAULT_REGION="ap-south-2"
"""
import boto3, json, os

s3 = boto3.client(
    "s3",
    region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-2"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

RAW_BUCKET  = os.environ.get("S3_BUCKET", "hirewatch-raw-india")
GOLD_BUCKET = os.environ.get("S3_OUTPUT_BUCKET", "hirewatch-gold-india")

# ── Gold Summary ──────────────────────────────────────────────────
obj  = s3.get_object(Bucket=GOLD_BUCKET, Key="data/india_summary.json")
data = json.loads(obj["Body"].read())

print("=" * 65)
print("  HIREWATCH INDIA - GOLD OUTPUT")
print("  Updated  : " + str(data["updated_at"]))
print("  Cities   : " + str(data["district_count"]))
print("  Engine   : Python/pandas (no Databricks)")
print("=" * 65)
print("  {:<18} {:>8} {:>8} {:>10} {:>8}".format("City","Hiring","Layoffs","Companies","Growth"))
print("  " + "-"*18 + " " + "-"*8 + " " + "-"*8 + " " + "-"*10 + " " + "-"*8)
for d in sorted(data["districts"], key=lambda x: x.get("avg_sentiment",0), reverse=True):
    print("  {:<18} {:>8} {:>8} {:>10} {:>8.2f}".format(
        d.get("city",""), d.get("total_hiring",0), d.get("total_layoffs",0),
        d.get("total_companies",0), d.get("growth_score",0)
    ))
print("=" * 65)

# ── List all S3 files ─────────────────────────────────────────────
print("\n  S3 GOLD BUCKET FILES")
print("  " + "-"*50)
resp  = s3.list_objects_v2(Bucket=GOLD_BUCKET, Prefix="data/")
for f in resp.get("Contents", []):
    print("  " + f["Key"] + "  (" + str(f["Size"]) + " bytes)")

print("\n  Pipeline is working correctly.")
