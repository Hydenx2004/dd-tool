"""
Datadog API client.
Handles all interactions with the Datadog API:
  - Validate log queries
  - Create log pipelines
  - Search logs
"""

import sys
from datetime import datetime, timedelta, timezone

import requests
import urllib3

# Suppress InsecureRequestWarning when using verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatadogClient:
    """Wrapper around the Datadog REST API for log pipelines and log search."""

    # Environments that use standard (indexed) storage; everything else → flex
    PROD_ENVS = {"prod", "production"}

    def __init__(self, api_key, app_key, site="datadoghq.com"):
        """
        Args:
            api_key: Datadog API key.
            app_key: Datadog Application key.
            site:    Datadog site (e.g. datadoghq.com, datadoghq.eu, us5.datadoghq.com).
        """
        self.base_url = f"https://api.{site}"
        self.headers = {
            "DD-API-KEY": api_key,
            "DD-APPLICATION-KEY": app_key,
            "Content-Type": "application/json",
        }

    # ──────────────────────────────────────────────
    #  Pipeline APIs (v1)
    # ──────────────────────────────────────────────

    def create_pipeline(self, name, query, processors, is_enabled=True):
        """
        POST /api/v1/logs/config/pipelines
        Create a NEW log pipeline.

        Args:
            name:       Pipeline name (e.g. "sourcecategory:npe/load/...-pipeline-catchfields").
            query:      The filter query for matching logs.
            processors: List of processor dicts (grok-parsers, etc.).
            is_enabled: Whether the pipeline starts enabled.

        Returns:
            dict: The newly created pipeline object (with id, name, etc.).
        """
        url = f"{self.base_url}/api/v1/logs/config/pipelines"
        body = {
            "name": name,
            "is_enabled": is_enabled,
            "filter": {
                "query": query,
            },
            "processors": processors,
        }
        return self._request("POST", url, json_body=body)

    # ──────────────────────────────────────────────
    #  Storage-tier detection
    # ──────────────────────────────────────────────

    @staticmethod
    def extract_env_from_query(query):
        """
        Extract the environment segment from a sourcecategory in the query.

        Sourcecategory naming convention:  business_unit/env/...
        Examples:
            sourcecategory:npe/load/olp/csas/sas2_summary  → "load"
            sourcecategory:npe/prod/ncs/mts/nginx          → "prod"

        Args:
            query: The Datadog log query string.

        Returns:
            str or None: The environment segment, or None if not parseable.
        """
        import re
        # Match sourcecategory value (with or without quotes)
        match = re.search(r'sourcecategory:\s*"?([^"\s]+)"?', query, re.IGNORECASE)
        if not match:
            return None
        source_cat = match.group(1)          # e.g. npe/load/olp/csas/sas2_summary
        parts = source_cat.split("/")
        if len(parts) >= 2:
            return parts[1]                  # env is the 2nd segment
        return None

    def detect_storage_tier(self, query):
        """
        Determine the Datadog storage tier based on the environment in the
        query's sourcecategory.

        Non-prod environments store logs in the **flex** tier.
        Prod / production use the default **indexes** tier.

        Args:
            query: The Datadog log query string.

        Returns:
            str: "flex" for non-prod, "indexes" for prod (or when env can't be detected).
        """
        env = self.extract_env_from_query(query)
        if env and env.lower() not in self.PROD_ENVS:
            return "flex"
        return "indexes"

    # ──────────────────────────────────────────────
    #  Query validation
    # ──────────────────────────────────────────────

    def validate_query(self, query, days_lookback=7, storage_tier="indexes"):
        """
        Validate that a log query returns at least one log.

        Args:
            query:         The Datadog log query string.
            days_lookback: Number of days to look back.
            storage_tier:  "indexes" or "flex".

        Returns:
            bool: True if the query returns at least one log.
        """
        result = self.search_logs(query, days_lookback=days_lookback, limit=1,
                                  storage_tier=storage_tier)
        logs = result.get("data", [])
        return len(logs) > 0

    # ──────────────────────────────────────────────
    #  Log Search API (v2)
    # ──────────────────────────────────────────────

    def search_logs(self, query, days_lookback=7, limit=1, storage_tier="indexes"):
        """
        POST /api/v2/logs/events/search
        Search for logs matching a query within the last N days.

        Args:
            query:         Datadog log query string.
            days_lookback: Number of days to look back.
            limit:         Max number of log events to return (we only need 1
                           to check existence).
            storage_tier:  Which Datadog storage tier to search.
                           "indexes" (default) for standard/prod logs,
                           "flex"    for flex-tier (non-prod) logs.

        Returns:
            dict: Response containing 'data' (list of log events) and 'meta'.
        """
        url = f"{self.base_url}/api/v2/logs/events/search"
        now = datetime.now(timezone.utc)
        from_time = now - timedelta(days=days_lookback)

        body = {
            "filter": {
                "query": query,
                "from": from_time.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                "to": now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                "storage_tier": storage_tier,
            },
            "page": {
                "limit": limit,
            },
        }
        return self._request("POST", url, json_body=body)

    def check_key_has_logs(self, query, key, days_lookback=7, storage_tier="indexes"):
        """
        Check if a specific key appears in logs matching the query.

        Args:
            query:          The log query (e.g. "sourcecategory:npe/load/...").
            key:            The key name to check (e.g. "resptime").
            days_lookback:  Number of days to look back.
            storage_tier:   "indexes" or "flex".

        Returns:
            bool: True if at least one log event contains this key.
        """
        combined_query = f"{query} {key}"
        result = self.search_logs(
            combined_query,
            days_lookback=days_lookback,
            limit=1,
            storage_tier=storage_tier,
        )
        logs = result.get("data", [])
        return len(logs) > 0

    def check_keys_for_logs(self, query, keys, days_lookback=7, storage_tier="indexes"):
        """
        Check multiple keys for log presence.

        Args:
            query:          The log query.
            keys:           List of key names.
            days_lookback:  Number of days.
            storage_tier:   "indexes" or "flex".

        Returns:
            tuple: (keys_with_logs: list, keys_without_logs: list)
        """
        keys_with_logs = []
        keys_without_logs = []

        for key in keys:
            print(f"   Checking key '{key}'...", end=" ")
            has_logs = self.check_key_has_logs(query, key, days_lookback, storage_tier)
            if has_logs:
                print("✅ logs found")
                keys_with_logs.append(key)
            else:
                print(f"⚠️  no logs in the last {days_lookback} day(s)")
                keys_without_logs.append(key)

        return keys_with_logs, keys_without_logs

    # ──────────────────────────────────────────────
    #  Internal HTTP helper
    # ──────────────────────────────────────────────

    def _request(self, method, url, json_body=None):
        """
        Make an HTTP request to the Datadog API.
        Raises on non-2xx responses with a helpful error message.
        """
        resp = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            json=json_body,
            verify=False,
        )

        if not resp.ok:
            error_detail = ""
            try:
                error_detail = resp.json().get("errors", resp.text)
            except Exception:
                error_detail = resp.text

            if resp.status_code == 403:
                print(f"❌ 403 Forbidden — check that DD_API_KEY and DD_APP_KEY are correct.")
                print(f"   Detail: {error_detail}")
            elif resp.status_code == 404:
                resp.raise_for_status()
            else:
                print(f"❌ Datadog API error ({resp.status_code} {method} {url})")
                print(f"   Detail: {error_detail}")

            resp.raise_for_status()

        return resp.json()


# ──────────────────────────────────────────────
#  Quick self-test
# ──────────────────────────────────────────────
if __name__ == "__main__":
    from config import get_config

    cfg = get_config()
    client = DatadogClient(
        api_key=cfg["dd_api_key"],
        app_key=cfg["dd_app_key"],
        site=cfg["dd_site"],
    )

    print("=" * 50)
    print("  Datadog Client — Self Test")
    print("=" * 50)

    query = cfg["query"]
    storage_tier = client.detect_storage_tier(query)

    # Test 1: Validate query
    print(f"\n🔍 Validating query: '{query}'")
    print(f"   Storage tier: {storage_tier}")
    has_logs = client.validate_query(query, cfg["days_lookback"], storage_tier)
    print(f"   Has logs: {has_logs}")

    # Test 2: Check keys for logs
    print(f"\n🔎 Checking keys against query: '{query}'")
    with_logs, without_logs = client.check_keys_for_logs(
        query, cfg["keys"], cfg["days_lookback"], storage_tier
    )
    print(f"\n   Keys with logs:    {with_logs}")
    print(f"   Keys without logs: {without_logs}")
