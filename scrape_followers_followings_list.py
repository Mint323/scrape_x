"""
X (Twitter) Followers & Following List Scraper via Apify KaitoEasyAPI.

Usage:
    python scrape_followers_followings_list.py <username>                          # both, ALL
    python scrape_followers_followings_list.py <username> --followers-only          # followers only, ALL
    python scrape_followers_followings_list.py <username> --following-only          # following only, ALL
    python scrape_followers_followings_list.py <username> --json                    # raw JSON output
    python scrape_followers_followings_list.py <username> --json -o output.json     # save to file
"""

import argparse
import asyncio
import json
import logging
from typing import Any, Dict, List

from apify_client import run_actor, utcnow_iso

logger = logging.getLogger("x_scraper")

ACTOR_ID = "kaitoeasyapi~premium-x-follower-scraper-following-data"


async def get_followers(username: str, max_count: int = 0) -> Dict[str, Any]:
    """Get followers for a given username. max_count=0 means all."""
    effective_max = max_count if max_count > 0 else 999_999_999
    result: Dict[str, Any] = {
        "username": username,
        "followers": [],
        "followers_count": 0,
        "success": False,
        "error": None,
        "fetched_at": utcnow_iso(),
    }

    payload = {
        "user_names": [username],
        "user_ids": [],
        "getFollowers": True,
        "getFollowing": False,
        "maxFollowers": effective_max,
        "maxFollowings": 0,
    }

    logger.info("get_followers username=%s max=%s", username, max_count or "all")

    try:
        items = await run_actor(ACTOR_ID, payload, timeout=300)
        result["followers"] = items if isinstance(items, list) else []
        result["followers_count"] = len(result["followers"])
        result["success"] = True
    except Exception as e:
        logger.error("get_followers failed username=%s error=%s", username, e)
        result["error"] = str(e)

    return result


async def get_following(username: str, max_count: int = 0) -> Dict[str, Any]:
    """Get following for a given username. max_count=0 means all."""
    effective_max = max_count if max_count > 0 else 999_999_999
    result: Dict[str, Any] = {
        "username": username,
        "following": [],
        "following_count": 0,
        "success": False,
        "error": None,
        "fetched_at": utcnow_iso(),
    }

    payload = {
        "user_names": [username],
        "user_ids": [],
        "getFollowers": False,
        "getFollowing": True,
        "maxFollowers": 0,
        "maxFollowings": effective_max,
    }

    logger.info("get_following username=%s max=%s", username, max_count or "all")

    try:
        items = await run_actor(ACTOR_ID, payload, timeout=300)
        result["following"] = items if isinstance(items, list) else []
        result["following_count"] = len(result["following"])
        result["success"] = True
    except Exception as e:
        logger.error("get_following failed username=%s error=%s", username, e)
        result["error"] = str(e)

    return result


# --------------- CLI helpers ---------------

def print_user_row(i: int, u: Dict):
    name = u.get("name", "?")
    screen = u.get("screen_name") or u.get("username", "?")
    followers = u.get("followers_count", "?")
    verified = u.get("is_blue_verified") or u.get("verified", False)
    badge = " [v]" if verified else ""
    desc = str(u.get("description", ""))[:50]
    print(f"  {i:>4}. @{screen:<20}{badge} {name:<22} followers={str(followers):>10}  {desc}")


async def async_main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape X followers & following list")
    parser.add_argument("username", help="X username (without @)")
    parser.add_argument("--followers-only", action="store_true", help="Only fetch followers")
    parser.add_argument("--following-only", action="store_true", help="Only fetch following")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("-o", "--output", type=str, default=None, help="Save JSON to file")
    args = parser.parse_args()

    username = args.username.replace("@", "").strip()

    want_followers = not args.following_only
    want_following = not args.followers_only

    followers_result = None
    following_result = None

    if want_followers:
        followers_result = await get_followers(username)
    if want_following:
        following_result = await get_following(username)

    combined = {
        "username": username,
        "followers": followers_result.get("followers", []) if followers_result else [],
        "followers_count": followers_result.get("followers_count", 0) if followers_result else 0,
        "following": following_result.get("following", []) if following_result else [],
        "following_count": following_result.get("following_count", 0) if following_result else 0,
        "success": (not followers_result or followers_result["success"]) and
                   (not following_result or following_result["success"]),
        "error": followers_result.get("error") if followers_result and followers_result.get("error")
                 else following_result.get("error") if following_result else None,
    }

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2)
        print(f"Saved to {args.output}")

    if args.json:
        print(json.dumps(combined, ensure_ascii=False, indent=2))
        return

    print()
    print(f"{'=' * 70}")
    print(f"  @{username}  --  Followers & Following")
    print(f"{'=' * 70}")

    if combined["error"]:
        print(f"  ERROR: {combined['error']}")
        return

    if want_followers:
        print(f"\n--- Followers ({combined['followers_count']}) ---")
        if combined["followers"]:
            for i, u in enumerate(combined["followers"][:20], 1):
                print_user_row(i, u)
            if combined["followers_count"] > 20:
                print(f"  ... and {combined['followers_count'] - 20} more")
        else:
            print("  (none)")

    if want_following:
        print(f"\n--- Following ({combined['following_count']}) ---")
        if combined["following"]:
            for i, u in enumerate(combined["following"][:20], 1):
                print_user_row(i, u)
            if combined["following_count"] > 20:
                print(f"  ... and {combined['following_count'] - 20} more")
        else:
            print("  (none)")

    print()


if __name__ == "__main__":
    asyncio.run(async_main())
