# CopeTech SEC API EC2 Deploy

Deploy target:

- Ubuntu 22.04 LTS EC2
- Docker + Docker Compose plugin installed
- HTTP port 80 or 8000 open in the security group
- No AWS keys in the repo
- EC2 IAM role attached for DynamoDB/S3 access, e.g. `copeharder-ec2-backend-role`

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
DYNAMODB_RATE_LIMITS_PK=ip
DYNAMODB_DEMO_JOBS_PK=job_id
DYNAMODB_SEC_CACHE_INDEX_PK=cache_key
SEC_API_USER_AGENT=CopeHarder your-email@example.com
BACKEND_API_SECRET=replace-with-long-random-secret
DEMO_ACCESS_KEYS=friend-demo-key-1,friend-demo-key-2
CORS_ALLOW_ORIGINS=https://lolcopeharder.com,https://www.lolcopeharder.com,http://localhost:5173,http://127.0.0.1:5173
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

When `BACKEND_API_SECRET` is set, include the proxy secret and a friend/demo access key:

```bash
curl \
  -H "x-backend-secret: $BACKEND_API_SECRET" \
  -H "x-demo-key: friend-demo-key-1" \
  "http://localhost/api/sec/insiders?symbol=AAPL"
```

The backend validates `x-demo-key` against `DEMO_ACCESS_KEYS`; invalid keys fail before rate-limit counters are written.

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

The app uses boto3's default AWS credential provider chain. Do not put static AWS access keys in this repo, `.env`, Docker Compose, or Vercel.

For the real deployment, attach an EC2 IAM role with scoped access to:

- DynamoDB table `rate_limits`
- DynamoDB table `demo_jobs`
- DynamoDB table `sec_cache_index`
- S3 bucket `copeharder-artifacts`

The app reads resource names from environment variables only.

Rate limiting writes one daily counter per `demo_key + IP + YYYY-MM-DD` to `DYNAMODB_RATE_LIMITS_TABLE`.
