"""
run_pipeline.py
───────────────
Full HireWatch India pipeline runner.
Run this in VSCode with the "▶ Full Pipeline" launch config.
Runs all scrapers then ETL, shows results in the terminal.
"""

import subprocess, sys, os, time, json
from datetime import datetime

PYTHON = sys.executable
BASE   = os.path.dirname(os.path.abspath(__file__))
START  = datetime.now()

def banner(text, char="═"):
    line = char * 60
    print(f"\n{line}")
    print(f"  {text}")
    print(f"{line}")

def run_script(name, path, extra_env={}):
    banner(f"Running: {name}")
    t0  = time.time()
    env = {**os.environ, **extra_env}
    result = subprocess.run(
        [PYTHON, path],
        env=env,
        capture_output=False,   # stream output live to terminal
        text=True,
    )
    elapsed = round(time.time() - t0, 1)
    status  = "✅ Done" if result.returncode == 0 else "⚠️  Finished (check output above)"
    print(f"\n{status} — {name} ({elapsed}s)")
    return result.returncode

results = {}

# ── Stage 1: Scrapers ────────────────────────────────────────────
banner("STAGE 1 — INGEST (Scrapers)", "━")

scripts = [
    ("EPFO Payroll Data",  "pipeline/scrapers/fetch_epfo.py"),
    ("MCA21 Company Master", "pipeline/scrapers/sync_mca21.py"),
    ("News & Layoff Signals", "pipeline/scrapers/scrape_layoff_news.py"),
    ("Naukri Job Listings", "pipeline/scrapers/scrape_naukri.py"),
    ("LinkedIn Job Listings", "pipeline/scrapers/scrape_linkedin.py"),
]

for name, rel_path in scripts:
    code = run_script(name, os.path.join(BASE, rel_path))
    results[name] = "✅" if code == 0 else "⚠️"

# ── Stage 2: ETL ─────────────────────────────────────────────────
banner("STAGE 2 — ETL (Bronze → Silver → Gold)", "━")
code = run_script("HireWatch ETL", os.path.join(BASE, "pipeline/etl/hirewatch_etl.py"))
results["ETL"] = "✅" if code == 0 else "❌"

# ── Stage 3: Show what landed in Gold S3 ─────────────────────────
banner("STAGE 3 — VERIFY GOLD OUTPUT", "━")
try:
    import boto3
    s3   = boto3.client("s3", region_name="ap-south-2")
    gold = os.environ.get("S3_OUTPUT_BUCKET", "hirewatch-gold-india")
    resp = s3.list_objects_v2(Bucket=gold, Prefix="data/")
    files = resp.get("Contents", [])
    print(f"\n📦 Files in s3://{gold}/data/\n")
    total_bytes = 0
    for f in sorted(files, key=lambda x: x["Key"]):
        size = f["Size"]
        total_bytes += size
        print(f"  {f['Key']:<45} {size:>8,} bytes")
    print(f"\n  Total: {len(files)} files  |  {total_bytes:,} bytes")

    # Show india_summary.json content
    print("\n" + "─"*60)
    print("  📊 india_summary.json preview:")
    print("─"*60)
    obj  = s3.get_object(Bucket=gold, Key="data/india_summary.json")
    data = json.loads(obj["Body"].read())
    print(f"  Updated at    : {data.get('updated_at')}")
    print(f"  Districts     : {data.get('district_count')}")
    districts = data.get("districts", [])
    if districts:
        print(f"\n  {'City':<20} {'Hiring':>8} {'Layoffs':>8} {'Companies':>10} {'Growth':>8}")
        print(f"  {'─'*20} {'─'*8} {'─'*8} {'─'*10} {'─'*8}")
        for d in sorted(districts, key=lambda x: x.get("total_hiring", 0), reverse=True):
            print(f"  {d.get('city',''):<20} "
                  f"{d.get('total_hiring',0):>8,} "
                  f"{d.get('total_layoffs',0):>8,} "
                  f"{d.get('total_companies',0):>10,} "
                  f"{d.get('growth_score',0):>8.1f}")
    results["S3 Verify"] = "✅"
except Exception as e:
    print(f"  ⚠️  Could not read S3 output: {e}")
    results["S3 Verify"] = "⚠️"

# ── Summary ──────────────────────────────────────────────────────
elapsed_total = round((datetime.now() - START).total_seconds(), 1)
banner(f"PIPELINE COMPLETE — {elapsed_total}s total", "═")
print()
for step, status in results.items():
    print(f"  {status}  {step}")
print(f"\n  🪣 Bronze : s3://{os.environ.get('S3_BUCKET','hirewatch-raw-india')}/")
print(f"  🥇 Gold   : s3://{os.environ.get('S3_OUTPUT_BUCKET','hirewatch-gold-india')}/data/")
print()
