"""
Configuration module.
Reads settings from CLI arguments (local dev) or environment variables (TeamCity).
"""

import argparse
import os
import sys

# Load .env file if available (local development only).
# On TeamCity, env vars are injected directly — python-dotenv is not needed.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Running on TeamCity — no .env file needed


def parse_args():
    """Parse command-line arguments for local development."""
    parser = argparse.ArgumentParser(
        description="Create a Datadog log pipeline with custom grok parser processors."
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Datadog log query (e.g. sourcecategory:npe/load/olp/csas/sas2_summary)",
    )
    parser.add_argument(
        "--keys",
        type=str,
        default=None,
        help="Comma-separated list of keys to extract (e.g. resptime,acctid)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Number of days to look back for log verification",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without making any changes to Datadog",
    )
    return parser.parse_args()


def get_config():
    """
    Build configuration from CLI args (priority) or environment variables (fallback).

    Returns a dict with all required config values.
    Exits with an error if required values are missing.
    """
    args = parse_args()

    config = {
        # Datadog credentials (always from env / .env)
        "dd_api_key": os.getenv("DD_API_KEY"),
        "dd_app_key": os.getenv("DD_APP_KEY"),
        "dd_site": os.getenv("DD_SITE", "datadoghq.com"),
        # User inputs: CLI args take priority over env vars
        "query": args.query or os.getenv("QUERY"),
        "keys": _parse_keys(args.keys or os.getenv("KEYS")),
        "days_lookback": args.days or int(os.getenv("DAYS_LOOKBACK", "7")),
        "dry_run": args.dry_run,
    }

    # Validate required fields
    _validate(config)

    return config


def _parse_keys(keys_str):
    """Parse comma-separated keys string into a clean list."""
    if not keys_str:
        return []
    return [k.strip() for k in keys_str.split(",") if k.strip()]


def _validate(config):
    """Validate that all required config values are present."""
    errors = []

    if not config["dd_api_key"]:
        errors.append("DD_API_KEY is missing. Set it in .env or as an environment variable.")
    if not config["dd_app_key"]:
        errors.append("DD_APP_KEY is missing. Set it in .env or as an environment variable.")
    if not config["query"]:
        errors.append(
            "Log query is missing. "
            "Use --query <value> or set QUERY env var."
        )
    elif not config["query"].lower().startswith("sourcecategory:"):
        errors.append(
            "Query must be a sourcecategory query "
            "(e.g. sourcecategory:npe/load/olp/csas/sas2_summary)."
        )
    if not config["keys"]:
        errors.append(
            "Keys are missing. "
            "Use --keys <key1,key2> or set KEYS env var."
        )

    if errors:
        print("❌ Configuration errors:")
        for err in errors:
            print(f"   • {err}")
        sys.exit(1)


# Quick test: run `python config.py` to verify config loading
if __name__ == "__main__":
    cfg = get_config()
    print("✅ Configuration loaded successfully:")
    print(f"   DD Site:      {cfg['dd_site']}")
    print(f"   DD API Key:   {cfg['dd_api_key'][:5]}...{'*' * 10}")
    print(f"   DD App Key:   {cfg['dd_app_key'][:5]}...{'*' * 10}")
    print(f"   Query:        {cfg['query']}")
    print(f"   Keys:         {cfg['keys']}")
    print(f"   Days:         {cfg['days_lookback']}")
    print(f"   Dry Run:      {cfg['dry_run']}")
