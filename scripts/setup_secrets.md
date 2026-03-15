# GitHub Actions Secrets — Quick Reference

## Where to add them
→ `https://github.com/YOUR_USERNAME/hirewatch-india/settings/secrets/actions`
→ Click **New repository secret** for each row below

## All required secrets

| Secret Name | Example value | How to get it |
|-------------|--------------|---------------|
| `AWS_ACCESS_KEY_ID` | `AKIAIOSFODNN7EXAMPLE` | AWS Console → IAM → Users → hirewatch-pipeline → Security credentials → Create access key |
| `AWS_SECRET_ACCESS_KEY` | `wJalrXUtnFEMI/K7MDENG/...` | Same page — shown once at creation |
| `DATABRICKS_HOST` | `https://community.cloud.databricks.com` | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | `dapi1234abcdef...` | Databricks → User Settings → Developer → Access Tokens → Generate New Token |
| `DATABRICKS_JOB_ID` | `12345` | Databricks → Workflows → your job → URL shows `/jobs/12345` |
| `NEWS_API_KEY` | `abc123def456ghi789` | https://newsapi.org/account → Your API key |

## Notes
- Secret names are **case-sensitive** — copy them exactly
- `DATABRICKS_JOB_ID` is the number only, not a URL
- `DATABRICKS_HOST` has no trailing slash
- Tokens expire — set a calendar reminder to renew `DATABRICKS_TOKEN` every 90 days
