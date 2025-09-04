# GitHub Workflow Setup

This document explains how to set up the GitHub workflow for running LangSmith exports.

## Required GitHub Secrets

Before using the workflow, you need to add the following secrets to your GitHub repository:

### Go to: Repository Settings → Secrets and variables → Actions → New repository secret

### Required Secrets:

#### LangSmith Configuration
- `LANGSMITH_API_KEY` - Your LangSmith API key
- `LS_SESSION_IDS` - Comma-separated list of session IDs to filter

#### AWS/S3 Configuration (required for S3 upload)
- `S3_BUCKET_NAME` - S3 bucket name
- `AWS_ACCESS_KEY_ID` - Your AWS access key
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret key
- `AWS_REGION` - AWS region (e.g., "us-east-1")

## How to Use the Workflow

1. Go to your repository on GitHub
2. Click on the **Actions** tab
3. Find the **LangSmith Export** workflow
4. Click **Run workflow**
5. Configure the parameters:
   - **Hours**: Time window in hours (default: 24)
   - **Output**: Choose json, s3, or json,s3 (default: s3)
   - **Debug limit**: Optional limit for testing (leave empty for no limit)
6. Click **Run workflow**

## Workflow Options

### Hours Parameter
- `24` - Last 24 hours (default)
- `12` - Last 12 hours
- `0.5` - Last 30 minutes
- Any decimal number representing hours

### Output Parameter
- `json` - Save files locally only (available as artifacts)
- `s3` - Upload to S3 only
- `json,s3` - Save locally AND upload to S3

### Debug Limit
- Leave empty for no limit
- Enter a number (e.g., `10`) to limit the export to that many runs for testing

## Output Files

The workflow creates two files:
- `langchain-runs-full-YYYY-MM-DD-HH-MM.txt` - Complete data with conversation messages
- `langchain-runs-summary-YYYY-MM-DD-HH-MM.txt` - Summary data without conversation content

## Artifacts

If you choose `json` output (no S3), the files will be available as GitHub Actions artifacts for 30 days.

## Example Scenarios

### Daily Export to S3
- Hours: `24`
- Output: `s3`
- Debug limit: (empty)

### Quick Test Run
- Hours: `1`
- Output: `json`
- Debug limit: `5`

### Full Export with Local Backup
- Hours: `168` (1 week)
- Output: `json,s3`
- Debug limit: (empty)
