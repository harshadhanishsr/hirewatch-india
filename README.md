# 🇮🇳 HireWatch India — Complete Setup & Deployment Guide

> **Zero-cost** real-time India hiring & layoff intelligence dashboard.  
> Stack: GitHub Actions → AWS S3 → Databricks Community Edition → GitHub Pages  
> **Total monthly cost: $0.00**

---

## How This Works (30-second overview)

Every hour, GitHub Actions automatically:
1. **Scrapes** Naukri, LinkedIn, EPFO, News, MCA — uploads raw JSON to S3
2. **Triggers** a Databricks notebook that transforms raw → clean → aggregated data
3. **Pulls** the processed JSON from S3 and publishes it to your GitHub Pages dashboard

You set it up once. It runs forever.

---

## What You Need Before Starting

| Tool | Why | Download |
|------|-----|----------|
| Git | Version control | https://git-scm.com/downloads |
| GitHub CLI (`gh`) | Create GitHub repo from terminal | https://cli.github.com |
| Python 3.11+ | Install + test scrapers locally | https://python.org/downloads |
| AWS CLI v2 | Create S3 buckets + verify | https://aws.amazon.com/cli/ |

> **Windows users:** Install [Git for Windows](https://git-scm.com/downloads) — this includes **Git Bash**, which you'll need for the shell script in Step 4. Do NOT use PowerShell or CMD for that step.

Verify installs before continuing:
```bash
git --version       # should show 2.x+
gh --version        # should show 2.x+
python --version    # should show 3.11+
aws --version       # should show 2.x+
```

---

## Step 1 — Create AWS Account + S3 Buckets

### 1a. Create a free AWS account
1. Go to https://aws.amazon.com → **Create an AWS Account**
2. You'll need a credit card for identity verification — **you will NOT be charged** within free tier limits
3. Free tier gives you: **5 GB S3 storage + 20,000 GET requests/month** (more than enough)

### 1b. Create two S3 buckets

Go to **AWS Console → S3 → Create bucket** and create these two buckets:

**Bucket 1 — Raw data (Bronze)**
- Bucket name: `hirewatch-raw-india`
- AWS Region: `ap-south-1` ← Mumbai (closest to India)
- Object Ownership: ACLs disabled
- Block all public access: ✅ checked (keep private)
- Versioning: Disable
- Click **Create bucket**

**Bucket 2 — Processed data (Gold)**
- Bucket name: `hirewatch-gold-india`
- Same settings as above
- Click **Create bucket**

> Bucket names must be globally unique. If `hirewatch-raw-india` is taken, add your initials: `hirewatch-raw-india-hd`

### 1c. Create an IAM user for the pipeline

Go to **AWS Console → IAM → Users → Create user**

1. **User name:** ` `
2. **Permissions:** Select "Attach policies directly" → search for `AmazonS3FullAccess` → check it → Next → Create user
3. Click into the new user → **Security credentials** tab → **Create access key**
4. Use case: select **"Application running outside AWS"** → Next → Create access key
5. 🔴 **SAVE BOTH VALUES NOW** — Secret access key is shown only once:
   - `Access key ID`: looks like 
   - `Secret access key`: looks like 


---

## Step 2 — Databricks Community Edition Setup

### 2a. Register (free, no credit card)
1. Go to: https://community.cloud.databricks.com/
2. Click **"Get Started For Free"** → Fill in name/email → Verify email
3. When asked to choose a plan, select **Community Edition**
4. You'll land on your workspace at a URL like: `https://community.cloud.databricks.com/`

> ⚠️ Community Edition clusters **auto-terminate after 2 hours of inactivity**. The GitHub Actions pipeline restarts them automatically via the REST API — this is handled for you.

### 2b. Create a cluster

Go to **Compute** (left sidebar) → **Create compute**

- **Cluster name:** `hirewatch-cluster`
- **Databricks Runtime Version:** `13.3 LTS (Spark 3.4.1, Scala 2.12)` — pick this exact version
- Leave all other settings as default
- Click **Create compute** — takes 3-5 minutes to start

### 2c. Install required libraries on the cluster

Once the cluster is **Running** (green circle):

**Cluster page → Libraries tab → Install new**

Install these one at a time via **PyPI**:
| Library | Version |
|---------|---------|
| `rapidfuzz` | `3.6.1` |
| `structlog` | `24.1.0` |

For each: click Install New → PyPI → type the package name → Install

### 2d. Upload the ETL notebook

1. Go to **Workspace** (left sidebar) → click your username folder
2. Click the **⬇ Import** button (top right of workspace)
3. Select **File** → browse to: `C:\VScode_Projects\hirewatch-india\outputs\pipeline\databricks\hirewatch_etl.py`
4. Click **Import**
5. The notebook will appear in your workspace folder

### 2e. Configure Databricks secrets (stores your AWS keys securely)

**Option A — Using Databricks CLI (recommended):**
```bash
# Install the CLI
pip install databricks-cli

# Configure it (you'll need your workspace URL + token — get token in step 2g first)
databricks configure --token
# Host: https://community.cloud.databricks.com
# Token: (paste your token from step 2g)

# Create the secret scope
databricks secrets create-scope --scope hirewatch --initial-manage-principal users

# Add your AWS keys
databricks secrets put --scope hirewatch --key aws_access_key_id
# (paste your Access Key ID when prompted)

databricks secrets put --scope hirewatch --key aws_secret_access_key
# (paste your Secret Access Key when prompted)
```

**Option B — Directly in a notebook cell:**
```python
# Run this once in any Databricks notebook cell, then delete the cell
dbutils.secrets.put(scope="hirewatch", key="aws_access_key_id",     string="PASTE_YOUR_KEY_ID")
dbutils.secrets.put(scope="hirewatch", key="aws_secret_access_key", string="PASTE_YOUR_SECRET")
```
> ⚠️ Delete the cell immediately after running — never leave credentials in notebook cells

### 2f. Add notebook widgets (run once manually)

Open the `hirewatch_etl` notebook in Databricks. In the **first cell**, add and run:
```python
dbutils.widgets.text("s3_bucket",     "hirewatch-raw-india")
dbutils.widgets.text("output_bucket", "hirewatch-gold-india")
dbutils.widgets.text("run_ts",        "")
```
This creates the parameter panel. The widgets will persist — you only need to do this once.

### 2g. Create a Databricks Job

Go to **Workflows** (left sidebar) → **Create Job**

- **Task name:** `etl`
- **Type:** Notebook
- **Source:** Workspace
- **Path:** Click browse → select your `hirewatch_etl` notebook
- **Cluster:** select `hirewatch-cluster`
- Leave parameters empty (GitHub Actions fills them at runtime)
- Click **Create**

After saving, look at the URL: `https://community.cloud.databricks.com/#job/12345`  
→ Your **Job ID is `12345`** (the number after `/job/`)  
🔴 **Save this number** — you need it for GitHub secrets

### 2h. Generate a personal access token

Go to your **profile icon (top right) → User Settings → Developer → Access Tokens → Generate New Token**

- **Comment:** `github-actions`
- **Lifetime:** 90 days
- Click **Generate** → 🔴 **Copy and save the token** — shown only once


---

## Step 3 — NewsAPI Free Key

1. Go to: https://newsapi.org/register
2. Fill in name + email → verify email
3. You'll land on a dashboard showing your **API key** — copy it
4. Free tier: **100 requests/day** (the pipeline uses ~20/run = 480/day max — fits within limits)

---

## Step 4 — Set Up the GitHub Repository

### 4a. Authenticate GitHub CLI

Open a terminal (Git Bash on Windows, Terminal on Mac/Linux):
```bash
gh auth login
```
Choose:
- **GitHub.com**
- **HTTPS**
- **Login with a web browser** → press Enter → it opens your browser → authorize

### 4b. Create the folder structure

The `outputs/` folder you have now needs to be turned into a proper deployable GitHub repo. Run these commands in **Git Bash** (Windows) or Terminal (Mac/Linux):

```bash
# Navigate to the outputs folder
cd /c/VScode_Projects/hirewatch-india/outputs     # Windows Git Bash
# or
cd ~/VScode_Projects/hirewatch-india/outputs       # Mac/Linux

# Create all required folders
mkdir -p .github/workflows
mkdir -p docs/data
mkdir -p scripts

# Move the workflow file into place
cp hirewatch_pipeline.yml .github/workflows/hirewatch_pipeline.yml

# Create placeholder data files (pipeline will overwrite these)
echo '{"updated_at":"","run_ts":"bootstrap","total_companies":0,"total_districts":0,"pipeline_version":"1.0.0"}' > docs/data/meta.json
echo '{"updated_at":"","run_ts":"bootstrap","district_count":0,"districts":[]}' > docs/data/india_summary.json
echo '{"sectors":[]}' > docs/data/sectors.json

# Copy the dashboard as the Pages index
cp hirewatch_india.html docs/index.html
```

### 4c. Create .gitignore

Still in the same terminal:
```bash
cat > .gitignore << 'EOF'
__pycache__/
*.pyc
*.pyo
.env
.env.*
venv/
.venv/
*.egg-info/
.DS_Store
Thumbs.db
.vscode/
.idea/
docs/data/companies_*.json
EOF
```

### 4d. Initialise git and push to GitHub

```bash
# Still inside outputs/ folder
git init
git add .
git commit -m "feat: initial HireWatch India pipeline scaffold"

# Create the GitHub repo and push (replace YOUR_USERNAME)
gh repo create YOUR_USERNAME/hirewatch-india \
  --public \
  --description "Real-time India hiring & layoff intelligence dashboard" \
  --source=. \
  --push
```

> If `gh repo create` fails, create the repo manually on GitHub.com first, then:
> ```bash
> git remote add origin https://github.com/YOUR_USERNAME/hirewatch-india.git
> git branch -M main
> git push -u origin main
> ```

### 4e. Add GitHub Actions Secrets

Go to: `https://github.com/YOUR_USERNAME/hirewatch-india/settings/secrets/actions`

Click **New repository secret** for each of the following:

| Secret Name | Value | Where you got it |
|-------------|-------|-----------------|
| `AWS_ACCESS_KEY_ID` | `AKIAIOSFODNN7EXAMPLE` | Step 1c |
| `AWS_SECRET_ACCESS_KEY` | `wJalrXUtnFEMI/...` | Step 1c |
| `DATABRICKS_HOST` | `https://community.cloud.databricks.com` | Step 2a |
| `DATABRICKS_TOKEN` | `dapi1234abcd...` | Step 2h |
| `DATABRICKS_JOB_ID` | `12345` | Step 2g (number from URL) |
| `NEWS_API_KEY` | `abc123def456...` | Step 3 |

> ⚠️ The secret names must match **exactly** — they are case-sensitive

### 4f. Enable GitHub Pages

1. Go to your repo on GitHub → **Settings** tab
2. Left sidebar → **Pages**
3. Under **Source**: select **"Deploy from a branch"**
4. **Branch:** `main` — **Folder:** `/docs`
5. Click **Save**

GitHub will show a URL: `https://YOUR_USERNAME.github.io/hirewatch-india/`  
(Takes ~2 minutes to go live the first time)


---

## Step 5 — First Pipeline Run

### 5a. Trigger manually

1. Go to your GitHub repo → **Actions** tab
2. Click **"HireWatch India — Hourly Data Pipeline"** in the left list
3. Click **"Run workflow"** dropdown → **"Run workflow"** button

### 5b. Watch the 3 stages

| Stage | What it does | Expected time |
|-------|-------------|---------------|
| Stage 1 — Ingest | Runs all 5 scrapers, uploads to S3 | ~8–12 min |
| Stage 2 — Process | Databricks ETL: Bronze→Silver→Gold | ~15–20 min |
| Stage 3 — Serve | Pulls JSON from S3, commits to GitHub Pages | ~2 min |

Click each stage to expand and see live logs.

### 5c. Verify S3 data landed

After Stage 1 completes:
```bash
# Check raw data in bronze bucket
aws s3 ls s3://hirewatch-raw-india/ --recursive | head -30

# After Stage 2, check gold output
aws s3 ls s3://hirewatch-gold-india/data/ --recursive
```

You should see files like:
```
source=naukri/year=2024/month=01/day=15/hour=10/city=bangalore/run=12345.json
source=linkedin/year=2024/month=01/day=15/...
```

### 5d. Check the live dashboard

Visit: `https://YOUR_USERNAME.github.io/hirewatch-india/`

If it shows placeholder data, wait for Stage 3 to complete and the GitHub Pages build to finish (~2 min after the commit).

---

## Step 6 — Verify Automatic Scheduling

The pipeline is scheduled to run **every hour** via this cron in `hirewatch_pipeline.yml`:
```yaml
schedule:
  - cron: '0 * * * *'
```

After your first successful manual run, it will run automatically. You can verify by checking the Actions tab the next day — you should see ~24 runs.

---

## Local Testing (Optional)

If you want to test scrapers locally before running the full pipeline:

### Install Python dependencies
```bash
# Windows CMD / PowerShell
cd C:\VScode_Projects\hirewatch-india\outputs
pip install -r requirements_pipeline.txt

# Mac/Linux
cd ~/VScode_Projects/hirewatch-india/outputs
pip install -r requirements_pipeline.txt
```

### Create a local .env file
Create a file called `.env` in the `outputs/` folder (it's in `.gitignore` — safe):
```
S3_BUCKET=hirewatch-raw-india
AWS_ACCESS_KEY_ID=your_actual_key_id
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
NEWS_API_KEY=your_newsapi_key
```

### Run individual scrapers

**Git Bash / Linux / Mac:**
```bash
export $(cat .env | xargs)
python pipeline/scrapers/fetch_epfo.py
python pipeline/scrapers/scrape_layoff_news.py
python pipeline/scrapers/scrape_naukri.py
python pipeline/scrapers/scrape_linkedin.py
python pipeline/scrapers/sync_mca21.py
```

**Windows PowerShell:**
```powershell
Get-Content .env | ForEach-Object {
    $name, $value = $_.split('=', 2)
    [System.Environment]::SetEnvironmentVariable($name, $value)
}
python pipeline/scrapers/fetch_epfo.py
```

---

## Troubleshooting

### ❌ `ModuleNotFoundError: No module named 'pyarrow'`
```bash
pip install pyarrow==15.0.2
```

### ❌ `ModuleNotFoundError: No module named 'cloudscraper'`
```bash
pip install cloudscraper==1.2.71
```

### ❌ `ModuleNotFoundError: No module named 'structlog'`
```bash
pip install structlog==24.1.0
```

### ❌ `botocore.exceptions.NoCredentialsError`
Your AWS credentials aren't set. Run:
```bash
aws configure
# Enter: Access Key ID, Secret Access Key, region=ap-south-1, output=json
```

### ❌ `KeyError: 'S3_BUCKET'` when running a scraper
All scrapers read `S3_BUCKET` from the environment. Set it first:
```bash
export S3_BUCKET=hirewatch-raw-india        # Git Bash / Mac / Linux
$env:S3_BUCKET="hirewatch-raw-india"        # PowerShell
```

### ❌ GitHub Actions: Stage 2 fails with "run_id not found"
Check that `DATABRICKS_JOB_ID` secret is the **number only** (e.g. `12345`), not a URL.

### ❌ GitHub Actions: Stage 2 times out (>20 min)
Databricks CE clusters are slow to start cold. Increase timeout:
```yaml
# In hirewatch_pipeline.yml, under the 'process' job:
timeout-minutes: 35
```

### ❌ Databricks: `AnalysisException: Path does not exist`
Stage 1 (ingest) hasn't run yet, so there's no data in S3. Always run Stage 1 first.  
The ETL handles empty sources gracefully — each `load_bronze_source()` returns an empty DataFrame on error.

### ❌ Databricks: `databricks.sdk.errors.NotFound: Secret not found`
The AWS secrets weren't added to Databricks correctly. Re-run the secrets setup:
```bash
databricks secrets put --scope hirewatch --key aws_access_key_id
databricks secrets put --scope hirewatch --key aws_secret_access_key
```

### ❌ Databricks: Widget error on notebook run
Run this cell manually in the notebook before the job:
```python
dbutils.widgets.text("s3_bucket",     "hirewatch-raw-india")
dbutils.widgets.text("output_bucket", "hirewatch-gold-india")
dbutils.widgets.text("run_ts",        "")
```

### ❌ Dashboard shows no data after pipeline completes
1. Check `aws s3 ls s3://hirewatch-gold-india/data/` — files should exist
2. Check Stage 3 logs — look for the `aws s3 sync` step
3. Check GitHub Pages build: repo → **Actions** → **pages-build-deployment** workflow

### ❌ Naukri / LinkedIn scrapers return 0 jobs
These scrape public HTML which can change. The pipeline will still complete — other sources will populate data. The dashboard will show EPFO and news signals even if job boards fail.

### ❌ `gh repo create` fails with "already exists"
The repo already exists on GitHub. Just add the remote and push:
```bash
git remote add origin https://github.com/YOUR_USERNAME/hirewatch-india.git
git push -u origin main
```


---

## Complete Secrets Reference

Print this out and fill it in as you go through the steps:

```
┌─────────────────────────────────────────────────────────────┐
│             HireWatch India — Secrets Checklist             │
├──────────────────────────┬──────────────────────────────────┤
│ AWS_ACCESS_KEY_ID        │ ____________________________     │
│ AWS_SECRET_ACCESS_KEY    │ ____________________________     │
│ DATABRICKS_HOST          │ https://community.cloud.        │
│                          │ databricks.com                  │
│ DATABRICKS_TOKEN         │ dapi_____________________       │
│ DATABRICKS_JOB_ID        │ ____________ (number only)      │
│ NEWS_API_KEY             │ ____________________________     │
└──────────────────────────┴──────────────────────────────────┘
```

---

## Cost Breakdown

| Service | Free tier | Monthly usage | Cost |
|---------|-----------|---------------|------|
| GitHub Actions | Unlimited on public repos | ~720 min/month | **$0** |
| AWS S3 | 5 GB + 20K GETs (12 months) | ~500 MB, ~14K GETs | **$0** |
| Databricks CE | Free forever | 1 cluster | **$0** |
| GitHub Pages | Free | 1 site | **$0** |
| NewsAPI | 100 req/day free | ~20 req/run | **$0** |
| **Total** | | | **$0.00/month** |

> After AWS 12-month free tier ends: ~$0.12/month for S3 storage.

---

## File Structure Reference

```
hirewatch-india/outputs/          ← your working directory
├── .github/
│   └── workflows/
│       └── hirewatch_pipeline.yml   ← GitHub Actions: all 3 stages
├── pipeline/
│   ├── scrapers/
│   │   ├── scrape_naukri.py         ← Naukri job listings (15 cities)
│   │   ├── scrape_linkedin.py       ← LinkedIn public search (14 cities)
│   │   ├── fetch_epfo.py            ← EPFO govt payroll data
│   │   ├── scrape_layoff_news.py    ← NewsAPI + RSS + Layoffs.fyi
│   │   └── sync_mca21.py            ← MCA21 company master
│   └── databricks/
│       └── hirewatch_etl.py         ← PySpark ETL: Bronze→Silver→Gold
├── docs/
│   ├── index.html                   ← Dashboard UI (GitHub Pages)
│   └── data/                        ← JSON output (auto-populated)
│       ├── meta.json
│       ├── india_summary.json
│       ├── sectors.json
│       └── companies_*.json
├── requirements_pipeline.txt        ← Python dependencies
├── setup_repo.sh                    ← Optional: one-shot bash scaffolder
└── README.md                        ← This file
```

---

## Data Refresh Schedule

| Source | How often | Notes |
|--------|-----------|-------|
| Naukri job listings | Every hour | 15 cities × 5 keywords |
| LinkedIn job listings | Every hour | 14 cities × 3 keywords |
| EPFO payroll data | Every hour | data.gov.in API |
| Layoff news signals | Every hour | NewsAPI + 5 RSS feeds |
| MCA company master | When manually triggered | Large dataset, runs on demand |
| Dashboard (GitHub Pages) | After every pipeline run | Auto-committed by Stage 3 |

---

## Maintenance

### Renewing the Databricks token (every 90 days)
1. Databricks → User Settings → Developer → Access Tokens → Generate New Token
2. GitHub repo → Settings → Secrets → Update `DATABRICKS_TOKEN`

### Checking pipeline health
- Actions tab → look for any red ❌ runs
- A GitHub Issue is auto-created on failure (see Stage 4 in the workflow)

### Monitoring S3 costs
- AWS Console → S3 → your bucket → Metrics tab
- Set a billing alert: AWS Console → Billing → Budgets → Create budget → $1/month threshold

---

## Quick-Start Checklist

- [ ] AWS account created
- [ ] `hirewatch-raw-india` S3 bucket created (ap-south-1)
- [ ] `hirewatch-gold-india` S3 bucket created (ap-south-1)
- [ ] IAM user `hirewatch-pipeline` created with S3FullAccess
- [ ] Access Key ID + Secret saved
- [ ] Databricks CE account registered
- [ ] `hirewatch-cluster` created (Runtime 13.3 LTS)
- [ ] `rapidfuzz==3.6.1` and `structlog==24.1.0` installed on cluster
- [ ] `hirewatch_etl.py` uploaded as notebook
- [ ] AWS secrets added to Databricks secret scope `hirewatch`
- [ ] Notebook widgets added and run once
- [ ] Databricks Job created → Job ID saved
- [ ] Databricks personal access token saved
- [ ] NewsAPI key obtained
- [ ] `gh auth login` done
- [ ] Folder structure created (`.github/workflows/`, `docs/data/`, `scripts/`)
- [ ] `git init` + `git commit` + `gh repo create` + pushed
- [ ] All 6 GitHub Actions secrets added
- [ ] GitHub Pages enabled (`/docs` branch)
- [ ] First manual pipeline run triggered and succeeded
- [ ] Dashboard live at `https://YOUR_USERNAME.github.io/hirewatch-india/`
