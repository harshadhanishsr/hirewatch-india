#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# setup_repo.sh
# Creates the full HireWatch India GitHub repository structure
# on your local machine, then pushes it to a new GitHub repo.
#
# Usage (Linux/macOS/Git Bash on Windows):
#   chmod +x setup_repo.sh
#   ./setup_repo.sh <github-username> <repo-name>
#
# Example:
#   ./setup_repo.sh dhanish hirewatch-india
#
# WINDOWS USERS: Run this inside Git Bash (comes with Git for Windows)
#   or WSL. Do NOT run in plain PowerShell/CMD — it's a bash script.
#
# Prerequisites:
#   - git installed
#   - gh (GitHub CLI) installed and authenticated  →  gh auth login
#
# Run from the parent of the hirewatch-india folder, e.g.:
#   cd C:/VScode_Projects
#   bash hirewatch-india/outputs/setup_repo.sh dhanish hirewatch-india-deploy
# ─────────────────────────────────────────────────────────────────

set -e

GITHUB_USER="${1:-your-github-username}"
REPO_NAME="${2:-hirewatch-india}"
REPO_DIR="./$REPO_NAME"

# Determine the source directory relative to where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUTS_DIR="$SCRIPT_DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " HireWatch India — Repo Scaffolder"
echo " GitHub user : $GITHUB_USER"
echo " Repo name   : $REPO_NAME"
echo " Source dir  : $OUTPUTS_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Create directory structure ─────────────────────────────────
mkdir -p "$REPO_DIR"/{.github/workflows,pipeline/scrapers,pipeline/databricks,docs/data,scripts}

echo "✓ Directory structure created"

# ── 2. Copy pipeline files ────────────────────────────────────────
cp "$OUTPUTS_DIR/hirewatch_pipeline.yml"              "$REPO_DIR/.github/workflows/hirewatch_pipeline.yml"
cp "$OUTPUTS_DIR/requirements_pipeline.txt"           "$REPO_DIR/requirements_pipeline.txt"
cp "$OUTPUTS_DIR/pipeline/scrapers/scrape_naukri.py"      "$REPO_DIR/pipeline/scrapers/"
cp "$OUTPUTS_DIR/pipeline/scrapers/scrape_linkedin.py"    "$REPO_DIR/pipeline/scrapers/"
cp "$OUTPUTS_DIR/pipeline/scrapers/fetch_epfo.py"         "$REPO_DIR/pipeline/scrapers/"
cp "$OUTPUTS_DIR/pipeline/scrapers/scrape_layoff_news.py" "$REPO_DIR/pipeline/scrapers/"
cp "$OUTPUTS_DIR/pipeline/scrapers/sync_mca21.py"         "$REPO_DIR/pipeline/scrapers/"
cp "$OUTPUTS_DIR/pipeline/databricks/hirewatch_etl.py"    "$REPO_DIR/pipeline/databricks/"
cp "$OUTPUTS_DIR/hirewatch_india.html"                "$REPO_DIR/docs/index.html"

echo "✓ Source files copied"

# ── 3. Create placeholder data files (replaced by pipeline) ───────
cat > "$REPO_DIR/docs/data/meta.json" <<'EOF'
{
  "updated_at": "2024-01-01",
  "run_ts": "bootstrap",
  "total_companies": 0,
  "total_districts": 0,
  "pipeline_version": "1.0.0",
  "note": "Placeholder — real data populated by GitHub Actions pipeline"
}
EOF

cat > "$REPO_DIR/docs/data/india_summary.json" <<'EOF'
{"updated_at":"2024-01-01","run_ts":"bootstrap","district_count":0,"districts":[]}
EOF

cat > "$REPO_DIR/docs/data/sectors.json" <<'EOF'
{"sectors":[]}
EOF

echo "✓ Placeholder data files created"

# ── 4. Create .gitignore ───────────────────────────────────────────
cat > "$REPO_DIR/.gitignore" <<'EOF'
# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
.eggs/
dist/
build/
venv/
.env
.env.*

# Secrets (never commit these)
secrets.yml
.secrets/
*_credentials.json

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
*.swp

# Large data files (managed by pipeline, not git)
docs/data/companies_*.json
EOF

echo "✓ .gitignore created"

# ── 5. Create GitHub Actions secrets documentation ────────────────
cat > "$REPO_DIR/scripts/setup_secrets.md" <<'EOF'
# GitHub Actions Secrets Setup

Go to: https://github.com/YOUR_USERNAME/hirewatch-india/settings/secrets/actions

Add these repository secrets (Settings → Secrets and variables → Actions → New repository secret):

| Secret Name            | Description                                    | Where to get it                         |
|------------------------|------------------------------------------------|-----------------------------------------|
| `AWS_ACCESS_KEY_ID`    | AWS IAM access key                             | AWS Console → IAM → Users → Security credentials |
| `AWS_SECRET_ACCESS_KEY`| AWS IAM secret key                             | Same as above (shown once at creation)  |
| `S3_BUCKET`            | Raw bronze bucket name                         | Create in AWS S3 → e.g. `hirewatch-raw-india` |
| `S3_OUTPUT_BUCKET`     | Gold output bucket name                        | Create in AWS S3 → e.g. `hirewatch-gold-india` |
| `DATABRICKS_HOST`      | Databricks CE workspace URL                    | https://community.cloud.databricks.com  |
| `DATABRICKS_TOKEN`     | Databricks personal access token               | Databricks → User Settings → Access Tokens |
| `DATABRICKS_JOB_ID`    | Job ID of the ETL notebook job                 | Databricks → Workflows → Jobs → (your job ID) |
| `NEWS_API_KEY`         | NewsAPI.org free tier key                      | https://newsapi.org/register (free)     |

## AWS S3 — Free Tier Setup

1. Go to AWS Console → S3 → Create bucket
2. Name: `hirewatch-raw-india`  Region: ap-south-1 (Mumbai — lowest latency for India)
3. Block all public access: YES (private — only pipeline reads/writes)
4. Repeat for `hirewatch-gold-india`
5. Create IAM user with AmazonS3FullAccess policy → generate access keys

## Databricks Community Edition — Free Tier Setup

1. Register at: https://community.cloud.databricks.com/
2. Create a cluster: Runtime 13.3 LTS, Single Node, Standard_DS3_v2 (free)
3. Upload `pipeline/databricks/hirewatch_etl.py` as a notebook
4. Create a Job: Workflows → Create Job → select your notebook → Manual trigger
5. Note the Job ID from the URL: /jobs/XXXXX
6. Generate token: User Settings → Developer → Access Tokens → Generate New Token

## GitHub Pages Setup

1. Repo Settings → Pages → Source: Deploy from a branch → Branch: main → Folder: /docs
2. Save → Your dashboard will be at: https://YOUR_USERNAME.github.io/hirewatch-india/

Total cost: $0.00/month ✓
EOF

echo "✓ Secrets documentation created"

# ── 6. Initialise git repo ────────────────────────────────────────
cd "$REPO_DIR"
git init
git add .
git commit -m "feat: initial HireWatch India scaffold

Zero-cost India hiring & layoff intelligence dashboard.
Pipeline: GitHub Actions → S3 (raw) → Databricks CE → GitHub Pages

- Scrapers: Naukri, LinkedIn, EPFO, News, MCA21
- ETL: Bronze → Silver → Gold (Delta Lake)
- Dashboard: Leaflet map with ranked city bubbles
- Cost: \$0/month (GitHub Actions + free tiers)"

echo "✓ Git repo initialised"

# ── 7. Create GitHub repo + push ─────────────────────────────────
if command -v gh &>/dev/null; then
    gh repo create "$GITHUB_USER/$REPO_NAME" \
        --public \
        --description "🇮🇳 Real-time India hiring & layoff intelligence dashboard — zero cost" \
        --source=. \
        --push
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " ✓ Repository created and pushed!"
    echo ""
    echo " Next steps:"
    echo "   1. Add GitHub secrets (see scripts/setup_secrets.md)"
    echo "   2. Enable GitHub Pages (Settings → Pages → /docs branch)"
    echo "   3. Run workflow manually: Actions → hirewatch_pipeline → Run workflow"
    echo ""
    echo " Dashboard URL (after Pages setup):"
    echo "   https://$GITHUB_USER.github.io/$REPO_NAME/"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " ✓ Local repo ready. Push manually:"
    echo ""
    echo "   gh repo create $GITHUB_USER/$REPO_NAME --public --source=. --push"
    echo "   # OR"
    echo "   git remote add origin git@github.com:$GITHUB_USER/$REPO_NAME.git"
    echo "   git push -u origin main"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
fi
