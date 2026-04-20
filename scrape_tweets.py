"""
X (Twitter) Tweets Scraper — 通过 Apify ScrapeBadger API 获取用户推文。

Usage:
    python scrape_tweets.py <username>                     # all tweets
    python scrape_tweets.py <username> --max-results 50    # 最多 50 条
    python scrape_tweets.py <username> --json              # JSON 输出

环境变量:
    APIFY_API_TOKEN   - Apify token（必须）
"""

import argparse
import asyncio
import json
import logging
from typing import Any, Dict, List

from apify_client import run_actor, utcnow_iso

logger = logging.getLogger("x_scraper")

ACTOR_ID = "scrape.badger~twitter-user-scraper"


async def get_latest_tweets(username: str, max_results: int = 0) -> Dict[str, Any]:
    """Get user's latest tweets. max_results=0 means all."""
    effective_max = max_results if max_results > 0 else 999_999_999
    result: Dict[str, Any] = {
        "username": username,
        "tweets": [],
        "tweet_count": 0,
        "success": False,
        "error": None,
        "fetched_at": utcnow_iso(),
    }
    logger.info("get_latest_tweets username=%s max_results=%s", username, max_results or "all")
    try:
        items = await run_actor(
            ACTOR_ID,
            {"mode": "Get User Latest Tweets", "username": username, "max_results": effective_max},
            timeout=180,
        )
        result["tweets"] = items if isinstance(items, list) else []
        result["tweet_count"] = len(result["tweets"])
        result["success"] = True
    except Exception as e:
        logger.error("get_latest_tweets failed username=%s error=%s", username, e)
        result["error"] = str(e)
    return result


# ───────────────── CLI ─────────────────


def print_tweets(tweets: List[Dict]):
    if not tweets:
        print("  No tweets")
        return
    for i, tw in enumerate(tweets[:10], 1):
        text = tw.get("full_text") or tw.get("text", "")
        tid = tw.get("id_str") or tw.get("id", "?")
        date = tw.get("created_at", "?")[:16]
        is_rt = tw.get("is_retweet", False)
        media_count = len(tw.get("media", []))
        tag = " [RT]" if is_rt else ""
        print(f"  {i}. [{date}]{tag} {text[:80]}")
        print(f"     Likes={tw.get('favorite_count', 0)}  RT={tw.get('retweet_count', 0)}  "
              f"Replies={tw.get('reply_count', 0)}  Quotes={tw.get('quote_count', 0)}  "
              f"Media={media_count}  ID={tid}")
    if len(tweets) > 10:
        print(f"  ... and {len(tweets) - 10} more tweets")


async def async_main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape X (Twitter) user tweets via Apify")
    parser.add_argument("username", help="X username (without @)")
    parser.add_argument("--max-results", type=int, default=0, help="Max tweets (0 = all)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    username = args.username.replace("@", "").strip()
    result = await get_latest_tweets(username, args.max_results)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print()
    print(f"{'=' * 60}")
    print(f"  @{username} — Tweets ({result['tweet_count']})")
    print(f"{'=' * 60}")

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        return

    print_tweets(result["tweets"])
    print()


if __name__ == "__main__":
    asyncio.run(async_main())
