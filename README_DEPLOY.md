# CopeTech SEC API EC2 Deploy

Deploy target:

- Ubuntu 22.04 LTS EC2
- Docker + Docker Compose plugin installed
- HTTP port 80 or 8000 open in the security group
- No AWS keys in the repo
- Later: attach an EC2 IAM role for DynamoDB/S3 access

## 1. Clone the repo

```bash
cd ~
git clone https://github.com/pattty847/CopeTech-Edgar.git
cd CopeTech-Edgar
```

If the repo already exists:

```bash
cd ~/CopeTech-Edgar
git pull
```

## 2. Create environment file

```bash
cp .env.example .env
nano .env
```

Required values:

```bash
PORT=8000
HOST_PORT=8000
AWS_REGION=us-east-1
S3_BUCKET=copeharder-artifacts
DYNAMODB_RATE_LIMITS_TABLE=rate_limits
DYNAMODB_DEMO_JOBS_TABLE=demo_jobs
DYNAMODB_SEC_CACHE_INDEX_TABLE=sec_cache_index
SEC_API_USER_AGENT=CopeHarder your-email@example.com
RATE_LIMIT_PER_DAY=60
SEC_CACHE_DIR=/data/edgar
LOG_LEVEL=INFO
```

To serve directly on public HTTP port 80, change only:

```bash
HOST_PORT=80
```

Keep `PORT=8000`; that is the container port.

## 3. Build the image

```bash
docker compose build
```

## 4. Run the service

```bash
docker compose up -d
```

Check logs:

```bash
docker compose logs -f sec-api
```

## 5. Healthcheck

If `HOST_PORT=8000`:

```bash
curl http://localhost:8000/health
```

If `HOST_PORT=80`:

```bash
curl http://localhost/health
```

Expected shape:

```json
{"ok":true,"service":"copetech-sec-api","region":"us-east-1"}
```

## 6. AAPL insiders test

If `HOST_PORT=8000`:

```bash
curl "http://localhost:8000/api/sec/insiders?symbol=AAPL"
```

If `HOST_PORT=80`:

```bash
curl "http://localhost/api/sec/insiders?symbol=AAPL"
```

Alternate endpoint:

```bash
curl "http://localhost:8000/sec/insiders?symbol=AAPL"
```

Useful bounded test:

```bash
curl "http://localhost:8000/api/sec/insiders?symbol=AAPL&days_back=90&filing_limit=10"
```

## 7. Stop or restart

Restart after code/config changes:

```bash
docker compose up -d --build
```

Stop:

```bash
docker compose down
```

## 8. AWS credentials

Do not put AWS access keys in this repo or `.env`.

For initial EC2 testing, the app can run without DynamoDB writes; it falls back to local in-memory rate limiting if AWS credentials are unavailable.

For the real deployment, attach an EC2 IAM role with scoped access to:

- DynamoDB table `rate_limits`
- DynamoDB table `demo_jobs`
- DynamoDB table `sec_cache_index`
- S3 bucket `copeharder-artifacts`

The app reads resource names from environment variables only.
