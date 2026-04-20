# X Scraper Service — 使用文档

> **一句话简介**：内部 X (Twitter) 数据抓取服务。给用户名，返回 profile / tweets / followers / following。支持单查、小批量同步、大规模异步任务。
>
> **服务地址**：`http://192.168.10.117:22222`
> **维护人**：wx · MongoDB: `x_scraper`
> **协议**：MCP (AI Agent) + REST API (开发者)
> **鉴权**：内网无需鉴权（如需鉴权请联系管理员配 `X_SCRAPER_API_KEY`）

---

## 目录

- [快速开始](#快速开始)
- [端点总览](#端点总览)
- [API 详细说明](#api-详细说明)
  - [1. 用户画像 `/api/profiles`](#1-用户画像-apiprofiles)
  - [2. 异步 Job 系统 `/api/jobs`](#2-异步-job-系统-apijobs)
  - [3. 用户推文 `/api/tweets`](#3-用户推文-apitweets)
  - [4. 粉丝列表 `/api/followers`](#4-粉丝列表-apifollowers)
  - [5. 关注列表 `/api/following`](#5-关注列表-apifollowing)
- [实战脚本](#实战脚本)
- [性能与限制](#性能与限制)
- [常见问题](#常见问题)
- [运维端点](#运维端点)

---

## 快速开始

### 30 秒上手

```bash
# 1. 抓单个用户画像
curl -X POST http://192.168.10.117:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk"}'

# 2. 抓单个用户推文
curl -X POST http://192.168.10.117:22222/api/tweets \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_results": 50}'

# 3. 抓 1000 个粉丝
curl -X POST http://192.168.10.117:22222/api/followers \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_count": 1000}'
```

### Python 调用模板

```python
import requests

BASE = "http://192.168.10.117:22222"

def post(path: str, **body):
    return requests.post(f"{BASE}{path}", json=body, timeout=600).json()

# 单用户 profile
r = post("/api/profiles", username="elonmusk")
print(r["profile"]["name"])           # Elon Musk
print(r["profile"]["followers_count"])
```

---

## 端点总览

| 端点 | 方法 | 用途 |
|---|---|---|
| `/api/profiles` | POST | 单/小批量用户画像（≤50 同步，>50 自动转 job） |
| `/api/jobs` | POST | 强制创建异步大批量 job |
| `/api/jobs` | GET | 列出所有 job |
| `/api/jobs/{id}` | GET | 查询 job 状态/进度 |
| `/api/jobs/{id}/results` | GET | 分页获取 job 结果 |
| `/api/jobs/{id}/cancel` | POST | 取消正在运行的 job |
| `/api/jobs/{id}/stream` | GET | SSE 实时推送 job 结果 |
| `/api/tweets` | POST | 用户推文 |
| `/api/followers` | POST | 粉丝列表 |
| `/api/following` | POST | 关注列表 |
| `/health` | GET | 健康检查 |
| `/metrics` | GET | Prometheus 指标 |
| `/mcp` | POST | MCP 协议（AI Agent） |

**所有 POST 端点都接收 `Content-Type: application/json` 的请求体。**

---

## API 详细说明

### 1. 用户画像 `/api/profiles`

#### 输入

```json
{
  "username": "elonmusk",                    // 单用户 (二选一)
  "usernames": ["user1", "user2", ...],      // 批量 (二选一)
  "concurrency": 50                          // 可选，并发数 (1-200)
}
```

**自动路由策略：**
- ≤ 50 个用户名 → **同步返回结果**
- \> 50 个用户名 → **自动创建 job**，返回 `job_id`

#### 输出（同步模式，单用户）

```json
{
  "username": "elonmusk",
  "profile": {
    "id": "44196397",
    "screen_name": "elonmusk",
    "name": "Elon Musk",
    "description": "...",
    "location": "",
    "url": "...",
    "followers_count": 220000000,
    "friends_count": 800,
    "following_count": 800,
    "statuses_count": 35000,
    "tweet_count": 35000,
    "favourites_count": 80000,
    "media_count": 8000,
    "listed_count": 200000,
    "created_at": "2009-06-02T20:12:29.000Z",
    "verified": true,
    "is_blue_verified": true,
    "protected": false,
    "profile_banner_url": "...",
    "profile_image_url": "https://pbs.twimg.com/profile_images/.../xxx_400x400.jpg",
    "pinned_tweet_ids_str": ["..."],
    "professional_type": null,
    "professional_categories": []
  },
  "success": true,
  "error": null,
  "fetched_at": "2026-04-20T10:00:00+00:00"
}
```

#### 输出（同步模式，多用户）

```json
{
  "total": 10,
  "success_count": 9,
  "fail_count": 1,
  "results": [
    {"username": "user1", "profile": {...}, "success": true, ...},
    {"username": "user2", "profile": null, "success": false, "error": "用户数据未找到", ...},
    ...
  ],
  "fetched_at": "..."
}
```

#### 输出（异步模式，>50 用户）

```json
{
  "job_id": "job_abc123def456",
  "total": 1000,
  "message": "async job submitted, use GET /api/jobs/{id} to check progress"
}
```

#### 示例

```bash
# 单用户
curl -X POST http://192.168.10.117:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk"}'

# 批量 10 个
curl -X POST http://192.168.10.117:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"usernames": ["elonmusk", "jack", "sundarpichai", "tim_cook", ...]}'

# 100 个（自动转 job）
curl -X POST http://192.168.10.117:22222/api/profiles \
  -H "Content-Type: application/json" \
  -d '{"usernames": [...100个...], "concurrency": 100}'
```

---

### 2. 异步 Job 系统 `/api/jobs`

适用于大批量（建议 > 50 个用户）抓取场景。

#### 2.1 创建 Job — `POST /api/jobs`

强制创建异步 job，不管批量大小。

**输入：**
```json
{
  "usernames": ["user1", "user2", ..., "user100000"],
  "concurrency": 100
}
```

**输出：**
```json
{
  "job_id": "job_abc123def456",
  "total": 100000,
  "message": "job submitted"
}
```

**示例：**

```bash
# 用文件读取大量用户名
curl -X POST http://192.168.10.117:22222/api/jobs \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "usernames": $(jq -R -s 'split("\n") | map(select(length > 0))' < usernames.txt),
  "concurrency": 100
}
EOF
```

#### 2.2 查询 Job 状态 — `GET /api/jobs/{job_id}`

```bash
curl http://192.168.10.117:22222/api/jobs/job_abc123def456
```

**输出：**
```json
{
  "_id": "job_abc123def456",
  "status": "running",          // submitted | running | completed | failed
  "total": 100000,
  "done": 35000,                // 已处理（含成功+失败）
  "failed": 200,
  "concurrency": 100,
  "owner": "user-12346-abc123", // 哪个 worker 在跑
  "created_at": "2026-04-20T10:00:00+00:00",
  "started_at": "2026-04-20T10:00:01+00:00",
  "completed_at": null,
  "heartbeat_at": "2026-04-20T10:05:00+00:00",
  "error": null
}
```

#### 2.3 列出所有 Job — `GET /api/jobs`

```bash
# 所有 job（默认 50 条）
curl http://192.168.10.117:22222/api/jobs

# 只看运行中的
curl "http://192.168.10.117:22222/api/jobs?status=running&limit=20"
```

#### 2.4 分页拉取 Job 结果 — `GET /api/jobs/{job_id}/results`

**Query 参数：**
- `skip`：跳过前 N 条（默认 0）
- `limit`：返回多少条（默认 100，建议 100-1000）

**示例：**

```bash
# 前 100 条
curl "http://192.168.10.117:22222/api/jobs/job_abc123def456/results?limit=100"

# 跳 1000 取下一批
curl "http://192.168.10.117:22222/api/jobs/job_abc123def456/results?skip=1000&limit=100"
```

**输出：**
```json
{
  "job_id": "job_abc123def456",
  "skip": 0,
  "limit": 100,
  "total_results": 35000,
  "results": [
    {
      "job_id": "job_abc123def456",
      "username": "elonmusk",
      "status": "success",
      "profile": {...},
      "error": null,
      "fetched_at": "2026-04-20T10:00:05+00:00"
    },
    ...
  ]
}
```

#### 2.5 取消 Job — `POST /api/jobs/{job_id}/cancel`

```bash
curl -X POST http://192.168.10.117:22222/api/jobs/job_abc123def456/cancel
```

**注意**：只能取消运行在**当前 worker** 上的 job。多 worker 部署下若 job 在其他 worker 上跑，会返回 owner 信息。

#### 2.6 SSE 实时推送 — `GET /api/jobs/{job_id}/stream`

```bash
# 边抓边接收（按 Ctrl+C 退出）
curl -N http://192.168.10.117:22222/api/jobs/job_abc123def456/stream
```

**输出（流式 Server-Sent Events）：**

```
event: progress
data: {"type":"progress","status":"running","total":100000,"done":35000,"failed":200}

data: {"username":"user1","status":"success","profile":{...},"fetched_at":"..."}

data: {"username":"user2","status":"failed","error":"用户不存在","fetched_at":"..."}

event: progress
data: {"type":"progress","status":"running","total":100000,"done":35100,"failed":201}

...

event: done
data: {"status":"completed"}
```

**Python 接收示例：**

```python
import httpx
import json

with httpx.stream("GET", "http://192.168.10.117:22222/api/jobs/job_abc123/stream") as r:
    for line in r.iter_lines():
        if line.startswith("data:"):
            payload = json.loads(line[5:].strip())
            print(payload)
```

---

### 3. 用户推文 `/api/tweets`

#### 输入

```json
{
  "username": "elonmusk",
  "max_results": 100      // 可选，0 或不传 = 全部
}
```

#### 输出

```json
{
  "username": "elonmusk",
  "tweets": [
    {
      "id_str": "1234567890",
      "full_text": "推文全文内容...",
      "created_at": "2026-04-20T...",
      "lang": "en",
      "favorite_count": 50000,
      "retweet_count": 10000,
      "reply_count": 5000,
      "quote_count": 2000,
      "media": [
        {"type": "photo", "media_url_https": "...", "width": 1200, "height": 800}
      ],
      "entities": {
        "hashtags": ["AI"],
        "user_mentions": [...],
        "urls": [...]
      },
      "is_retweet": false,
      ...
    },
    ...
  ],
  "tweet_count": 100,
  "success": true,
  "fetched_at": "..."
}
```

#### 示例

```bash
# 全部推文
curl -X POST http://192.168.10.117:22222/api/tweets \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk"}'

# 最近 50 条
curl -X POST http://192.168.10.117:22222/api/tweets \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_results": 50}'
```

---

### 4. 粉丝列表 `/api/followers`

#### 输入

```json
{
  "username": "elonmusk",
  "max_count": 1000     // 可选，0 或不传 = 全部
}
```

#### 输出

```json
{
  "username": "elonmusk",
  "followers": [
    {
      "id_str": "...",
      "screen_name": "...",
      "name": "...",
      "description": "...",
      "followers_count": 1234,
      "is_blue_verified": false,
      "verified": false,
      "location": "..."
    },
    ...
  ],
  "followers_count": 1000,
  "success": true,
  "fetched_at": "..."
}
```

#### 示例

```bash
# 前 500 个粉丝
curl -X POST http://192.168.10.117:22222/api/followers \
  -H "Content-Type: application/json" \
  -d '{"username": "elonmusk", "max_count": 500}'

# 全部粉丝（大账号慎用）
curl -X POST http://192.168.10.117:22222/api/followers \
  -H "Content-Type: application/json" \
  -d '{"username": "indigitalcolor"}'
```

⚠️ **`@elonmusk` 这种 2 亿粉丝的账号建议明确指定 `max_count`，否则会跑很久且消耗大量 Apify 配额（约 $0.0001/条）。**

---

### 5. 关注列表 `/api/following`

#### 输入

```json
{
  "username": "elonmusk",
  "max_count": 0        // 可选，0 = 全部
}
```

#### 输出

```json
{
  "username": "elonmusk",
  "following": [
    {"id_str": "...", "screen_name": "...", "name": "...", "followers_count": ..., ...},
    ...
  ],
  "following_count": 800,
  "success": true,
  "fetched_at": "..."
}
```

---

## 实战脚本

### A. 批量抓 1 万用户 + 实时进度

```python
import httpx
import json
import time

BASE = "http://192.168.10.117:22222"

# 1. 提交 job
with open("usernames.txt") as f:
    usernames = [line.strip() for line in f if line.strip()]

resp = httpx.post(f"{BASE}/api/jobs", json={
    "usernames": usernames,
    "concurrency": 100,
}).json()
job_id = resp["job_id"]
print(f"Job {job_id} submitted, total={resp['total']}")

# 2. 监控进度
while True:
    job = httpx.get(f"{BASE}/api/jobs/{job_id}").json()
    print(f"Status={job['status']} done={job['done']}/{job['total']} failed={job['failed']}")
    if job["status"] in ("completed", "failed"):
        break
    time.sleep(10)

# 3. 拉取所有结果
results = []
skip = 0
while True:
    page = httpx.get(f"{BASE}/api/jobs/{job_id}/results?skip={skip}&limit=500").json()
    results.extend(page["results"])
    if len(page["results"]) < 500:
        break
    skip += 500

print(f"Got {len(results)} results")
with open("output.json", "w") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
```

### B. 实时流式接收（边抓边写入文件）

```python
import httpx
import json

BASE = "http://192.168.10.117:22222"

# 提交 job
job_id = httpx.post(f"{BASE}/api/jobs", json={
    "usernames": ["user1", "user2", "user3", ...],
    "concurrency": 50,
}).json()["job_id"]

# 流式接收
with open("output.jsonl", "w") as f:
    with httpx.stream("GET", f"{BASE}/api/jobs/{job_id}/stream", timeout=None) as r:
        for line in r.iter_lines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            payload = json.loads(line[5:].strip())
            if payload.get("type") == "progress":
                print(f"Progress: {payload['done']}/{payload['total']}")
            elif "username" in payload:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                f.flush()
```

### C. 找某 KOL 的 1000 个高粉丝粉丝

```python
import httpx
BASE = "http://192.168.10.117:22222"

# 拉前 5000 粉丝
followers = httpx.post(f"{BASE}/api/followers", json={
    "username": "naval",
    "max_count": 5000,
}, timeout=600).json()["followers"]

# 筛粉丝数 >10K 的
big_followers = [f for f in followers if (f.get("followers_count") or 0) > 10000]
print(f"{len(big_followers)} 个粉丝数 >10K 的粉丝")
for f in big_followers[:20]:
    print(f"  @{f['screen_name']:<20} {f['followers_count']:>8} {f.get('description', '')[:50]}")
```

### D. 单接口 Curl 速查

```bash
BASE="http://192.168.10.117:22222"

# Profile (single)
curl -X POST $BASE/api/profiles -H 'Content-Type: application/json' -d '{"username":"elonmusk"}'

# Profile (batch ≤50, sync)
curl -X POST $BASE/api/profiles -H 'Content-Type: application/json' \
  -d '{"usernames":["elonmusk","jack","sundarpichai"]}'

# Profile (batch >50, async job)
curl -X POST $BASE/api/jobs -H 'Content-Type: application/json' \
  -d '{"usernames":[...],"concurrency":100}'

# Tweets
curl -X POST $BASE/api/tweets -H 'Content-Type: application/json' \
  -d '{"username":"elonmusk","max_results":50}'

# Followers (前 1000)
curl -X POST $BASE/api/followers -H 'Content-Type: application/json' \
  -d '{"username":"elonmusk","max_count":1000}'

# Following (全部)
curl -X POST $BASE/api/following -H 'Content-Type: application/json' \
  -d '{"username":"elonmusk"}'

# Job 进度
curl $BASE/api/jobs/job_xxx

# Job 结果分页
curl "$BASE/api/jobs/job_xxx/results?skip=0&limit=500"

# Job SSE 实时推送
curl -N $BASE/api/jobs/job_xxx/stream

# 取消 job
curl -X POST $BASE/api/jobs/job_xxx/cancel
```

---

## 性能与限制

### 速度参考

| 任务 | 并发 50 | 并发 100 | 并发 200 |
|---|---|---|---|
| 1,000 profile | ~3 分钟 | ~1.5 分钟 | ~1 分钟 |
| 10,000 profile | ~30 分钟 | ~15 分钟 | ~10 分钟 |
| 100,000 profile | ~5 小时 | ~2.5 小时 | ~1.5 小时 |
| 1,000 followers | ~10 秒 | - | - |
| 100,000 followers | ~10 分钟 | - | - |
| 单用户 1000 tweets | ~10-30 秒 | - | - |

> **注**：实际速度受 scrape.do 限流自适应调整影响。触发 429 时并发会自动降到 5。

### 上游配额（注意控制）

| 数据类型 | 渠道 | 计费 |
|---|---|---|
| Profile | scrape.do | 按请求计费（不重复抓取相同用户 1 小时内） |
| Tweets | Apify ScrapeBadger | 按结果数计费 |
| Followers / Following | Apify KaitoEasyAPI | ~$0.0001/条 |

### 缓存机制

- **Profile 缓存 1 小时**：同一用户 1 小时内重复请求不会再调外部 API，直接读 MongoDB
- Tweets / Followers / Following **不缓存**

### 已知限制

| 限制 | 说明 |
|---|---|
| 私密账号 | 无法抓取受保护账号的内容 |
| 实时订阅 | 只支持抓取，不支持流式订阅新推文 |
| 只读 | 不能发推/点赞/关注 |
| 单接口超时 | 5 分钟（`/api/tweets`、`/api/followers`、`/api/following`），超时建议改用 job |
| 无限流 | 服务无内建速率限制，请合理使用避免烧光 Apify 余额 |

---

## 常见问题

### Q1: job 跑到一半服务重启了怎么办？

**自动续传。** 重启后 worker 通过心跳超时机制接管未完成的 job，已完成的用户跳过，只抓剩下的。

### Q2: 如何查抓取失败的用户？

```bash
# 拉所有 failed 状态的结果
curl "http://192.168.10.117:22222/api/jobs/job_xxx/results?limit=10000" \
  | jq '.results[] | select(.status=="failed") | {username, error}'
```

### Q3: 怎么知道某次抓取消耗了多少钱？

访问 `/metrics` 端点查看 `x_scraper_apify_cost_usd_total` 指标：

```bash
curl http://192.168.10.117:22222/metrics | grep apify_cost_usd_total
```

### Q4: 服务挂了？

```bash
# 健康检查
curl http://192.168.10.117:22222/health

# 检查 MongoDB 连接、worker 状态
```

如返回 `mongodb: error: ...`，联系管理员重启 Mongo 或服务。

### Q5: 我想要 100 万用户的 profile，能搞定吗？

可以，但建议分批：
- 一次最多提交 10 万（避免单 job 太大）
- 设置 `concurrency=100-200`
- 用 SSE 流式接收，边抓边落库
- 预估时间：~15-30 小时

### Q6: 抓到的数据存哪了？

服务端 MongoDB `x_scraper.profile_results` 集合。**调用方需要自己拉走**（用 `/results` 端点）。MongoDB 中的数据可能被后续 job 覆盖（同 username 唯一），重要数据请拉走自存。

### Q7: 多个同事同时使用会冲突吗？

不会。多 worker 部署 + Mongo 分布式锁，每个 job 由一个 worker 独占执行，互不干扰。

---

## 运维端点

### `/health` — 健康检查

```bash
curl http://192.168.10.117:22222/health
```

返回服务状态、MongoDB 连接状态、当前 worker ID、所有可用端点。

### `/metrics` — Prometheus 指标

```bash
curl http://192.168.10.117:22222/metrics
```

主要指标：

| 指标 | 含义 |
|---|---|
| `x_scraper_http_requests_total{endpoint, method, status}` | HTTP 请求计数 |
| `x_scraper_http_request_duration_seconds` | HTTP 延迟分布 |
| `x_scraper_apify_calls_total{actor, status}` | Apify 调用计数 |
| `x_scraper_apify_cost_usd_total{actor}` | Apify 累计花费 (USD) |
| `x_scraper_jobs_total{event}` | Job 事件计数（created/started/completed/failed） |
| `x_scraper_jobs_active` | 当前运行中的 job 数 |
| `x_scraper_profiles_total{status}` | Profile 抓取计数 |
| `x_scraper_scrape_do_calls_total{status}` | scrape.do 调用计数 |

### `/mcp` — MCP 协议（AI Agent）

Cursor / Claude Desktop 配置：

```json
{
  "mcpServers": {
    "x-scraper": {
      "url": "http://192.168.10.117:22222/mcp"
    }
  }
}
```

可用 Tools：
- `get_x_profiles(usernames, concurrency)`
- `submit_x_profile_job(usernames, concurrency)`
- `get_x_job_status(job_id)`
- `get_x_tweets(username, max_results)`
- `get_x_followers(username, max_count)`
- `get_x_following(username, max_count)`

---

## 联系

有问题、需要新接口或服务异常 → 找 wx
