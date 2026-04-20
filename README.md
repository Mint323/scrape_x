# X Scraper Service

A high-performance async service for scraping X (Twitter) data — profiles, tweets, followers, and following lists. Supports both REST API and MCP (Model Context Protocol) for AI agent integration.

## Features

- **Profile scraping** — batch up to 200 concurrent requests with adaptive rate limiting
- **Tweets / Followers / Following** — via Apify actors
- **Async job engine** — large-scale batch processing (100K+ users), resumable, multi-worker safe
- **Real-time streaming** — SSE for live job progress
- **Profile caching** — 1-hour TTL to avoid duplicate API calls
- **MCP support** — plug into Claude, Cursor, or any MCP-compatible AI agent
- **Prometheus metrics** — built-in observability at `/metrics`

## Prerequisites

- Python 3.10+
- MongoDB (standalone or replica set)
- [Apify](https://console.apify.com/settings/integrations) account (for tweets/followers/following)
- [scrape.do](https://scrape.do/) account (for profile scraping)

## Quick Start

### 1. Clone & install dependencies

```bash
git clone https://github.com/YOUR_USERNAME/scrape_x.git
cd scrape_x
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in the required tokens:

```env
APIFY_API_TOKEN=your_apify_token
SCRAPE_DO_TOKEN=your_scrape_do_token
MONGODB_URI=mongodb://127.0.0.1:27017/
```

### 3. Start the service

```bash
# Development (single worker)
python server.py

# Production (multi-worker)
uvicorn server:app --host 0.0.0.0 --port 22222 --workers 4
```

Service runs at `http://localhost:22222`.

## API Reference

### Authentication

If `X_SCRAPER_API_KEY` is set in `.env`, all API calls (except `/health` and `/metrics`) require:

```
Authorization: Bearer YOUR_API_KEY
```

### REST Endpoints

#### Profiles

```bash
# Single or small batch (sync response)
curl -X POST http://localhost:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"usernames": ["elonmusk", "jack"]}'

# Large batch (auto-creates async job, returns job_id)
curl -X POST http://localhost:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"usernames": ["user1", "user2", "..."], "concurrency": 100}'
```

#### Tweets

```bash
curl -X POST http://localhost:22222/api/tweets \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_results": 20}'
```

#### Followers / Following

```bash
curl -X POST http://localhost:22222/api/followers \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_count": 100}'

curl -X POST http://localhost:22222/api/following \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_count": 100}'
```

#### Async Jobs

```bash
# Force create async job
curl -X POST http://localhost:22222/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"usernames": ["user1", "user2", "..."]}'

# Check job status
curl http://localhost:22222/api/jobs/{job_id}

# Get paginated results
curl "http://localhost:22222/api/jobs/{job_id}/results?skip=0&limit=100"

# Stream results in real-time (SSE)
curl http://localhost:22222/api/jobs/{job_id}/stream

# Cancel a job
curl -X POST http://localhost:22222/api/jobs/{job_id}/cancel

# List all jobs
curl "http://localhost:22222/api/jobs?status=running&limit=50"
```

### MCP Integration

Add to your MCP client config (Claude Desktop, Cursor, etc.):

```json
{
  "mcpServers": {
    "x-scraper": {
      "url": "http://localhost:22222/mcp"
    }
  }
}
```

Available MCP tools:
- `get_x_profiles` — fetch user profiles
- `submit_x_profile_job` — submit async batch job
- `get_x_job_status` — check job progress
- `get_x_tweets` — get user tweets
- `get_x_followers` — get user followers
- `get_x_following` — get user following

## Performance Reference

| Task | Scale | Concurrency | Time |
|------|-------|-------------|------|
| Profiles | 1,000 | 50 | ~3 min |
| Profiles | 10,000 | 100 | ~15 min |
| Profiles | 100,000 | 200 | ~1.5 hours |
| Followers | 1,000 | — | ~10 sec |
| Tweets | 1,000 | — | ~10-30 sec |

## Configuration

All options are in `.env`. See [.env.example](.env.example) for the full list with defaults.

Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `APIFY_API_TOKEN` | — | **Required.** Apify API token |
| `SCRAPE_DO_TOKEN` | — | **Required.** scrape.do proxy token |
| `MONGODB_URI` | `mongodb://127.0.0.1:27017/` | **Required.** MongoDB connection string |
| `X_SCRAPER_API_KEY` | — | Bearer token for auth (empty = no auth) |
| `SCRAPE_CONCURRENCY` | 50 | Default concurrent requests |
| `ASYNC_JOB_THRESHOLD` | 50 | Auto-switch to async job above this count |
| `PROFILE_CACHE_TTL` | 3600 | Profile cache TTL in seconds |

## License

MIT
