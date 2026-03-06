"""
Main entry point.
Orchestrates the full flow:
  1. Load config (from CLI args or env vars)
  2. Validate the user's log query
  3. Check if keys have logs
  4. Build grok parser rules
  5. Create a NEW pipeline with the processors
"""

import json
import sys

from config import get_config
from datadog_client import DatadogClient
from grok_builder import build_grok_processors, print_rules_summary


def build_pipeline_name(query):
    """
    Build the pipeline name from the query.

    Naming convention: {value}-pipeline-catchfields
    (strips the "sourcecategory:" prefix)

    Args:
        query: The Datadog log query (e.g. "sourcecategory:npe/load/olp/csas/sas2_summary").

    Returns:
        str: The pipeline name (e.g. "npe/load/olp/csas/sas2_summary-pipeline-catchfields").
    """
    # Strip "sourcecategory:" prefix for a cleaner pipeline name
    name = query
    if name.lower().startswith("sourcecategory:"):
        name = name[len("sourcecategory:"):]
    return f"{name}-pipeline-catchfields"


def main():
    # ──────────────────────────────────────────
    #  Step 1: Load configuration
    # ──────────────────────────────────────────
    print("=" * 60)
    print("  Datadog Custom Grok Processor Builder")
    print("=" * 60)

    config = get_config()

    print(f"\n⚙️  Configuration:")
    print(f"   Query:        {config['query']}")
    print(f"   Keys:         {config['keys']}")
    print(f"   Days:         {config['days_lookback']}")
    print(f"   Dry Run:      {config['dry_run']}")

    # Initialize Datadog client
    client = DatadogClient(
        api_key=config["dd_api_key"],
        app_key=config["dd_app_key"],
        site=config["dd_site"],
    )

    query = config["query"]

    # Detect storage tier from environment in sourcecategory
    storage_tier = client.detect_storage_tier(query)
    detected_env = client.extract_env_from_query(query) or "(unknown)"

    print(f"\n   Environment:  {detected_env}")
    print(f"   Storage Tier:{' 🗄️  flex (non-prod)' if storage_tier == 'flex' else ' 📦 indexes (prod)'}")

    # ──────────────────────────────────────────
    #  Step 2: Validate the query
    # ──────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  Step 1: Validate Query")
    print("─" * 60)

    print(f"\n   Query: {query}")
    print(f"   Searching {storage_tier.upper()} tier for the last "
          f"{config['days_lookback']} day(s)...", end=" ")

    has_logs = client.validate_query(query, config["days_lookback"], storage_tier)

    if not has_logs:
        print("❌ no logs found!")
        print(f"\n   ❌ The query returned 0 logs in the last {config['days_lookback']} day(s).")
        print(f"   Please check if the query is correct or increase the lookback days.")
        print(f"\n   Query used:  {query}")
        print(f"   Days:        {config['days_lookback']}")
        print(f"   Tier:        {storage_tier}")
        sys.exit(1)

    print("✅ logs found!")

    # ──────────────────────────────────────────
    #  Step 3: Check if keys have logs
    # ──────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  Step 2: Verify Keys Have Logs")
    print("─" * 60)
    print(f"\n   Checking {len(config['keys'])} key(s) against query...\n")

    keys_with_logs, keys_without_logs = client.check_keys_for_logs(
        query=query,
        keys=config["keys"],
        days_lookback=config["days_lookback"],
        storage_tier=storage_tier,
    )

    # Report keys without logs
    if keys_without_logs:
        print(f"\n   ⚠️  The following key(s) have NO logs in the last "
              f"{config['days_lookback']} day(s):")
        for key in keys_without_logs:
            print(f"      • {key}")
        print("   These keys will be SKIPPED (no grok rules created for them).")

    # Check if we have any valid keys left
    if not keys_with_logs:
        print("\n   ❌ None of the provided keys have logs. Nothing to do.")
        sys.exit(1)

    print(f"\n   ✅ Proceeding with {len(keys_with_logs)} key(s): {keys_with_logs}")

    # ──────────────────────────────────────────
    #  Step 4: Build grok parser rules
    # ──────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  Step 3: Build Grok Parser Rules")
    print("─" * 60)

    processors = build_grok_processors(keys_with_logs)
    print_rules_summary(keys_with_logs, processors)

    # ──────────────────────────────────────────
    #  Step 5: Create a new pipeline
    # ──────────────────────────────────────────
    pipeline_name = build_pipeline_name(query)

    print("─" * 60)
    print("  Step 4: Create New Pipeline")
    print("─" * 60)

    print(f"\n   Pipeline Name: {pipeline_name}")
    print(f"   Filter Query:  {query}")
    print(f"   Processors:    {len(processors)}")

    if config["dry_run"]:
        print("\n   🏁 DRY RUN — No changes will be made to Datadog.\n")
        print("   Pipeline that WOULD be created:\n")
        dry_run_payload = {
            "name": pipeline_name,
            "is_enabled": True,
            "filter": {"query": query},
            "processors": processors,
        }
        print(json.dumps(dry_run_payload, indent=2))
    else:
        print(f"\n   Creating pipeline '{pipeline_name}'...")

        try:
            result = client.create_pipeline(
                name=pipeline_name,
                query=query,
                processors=processors,
            )
            print(f"\n   ✅ Pipeline created successfully!")
            print(f"   Pipeline ID:   {result.get('id', '(unknown)')}")
            print(f"   Pipeline Name: {result.get('name', pipeline_name)}")
            print(f"   Processors:    {len(result.get('processors', []))}")
        except Exception as e:
            print(f"\n   ❌ Failed to create pipeline: {e}")
            sys.exit(1)

    # ──────────────────────────────────────────
    #  Summary
    # ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    print(f"   Pipeline:           {pipeline_name}")
    print(f"   Query:              {query}")
    print(f"   Keys processed:     {keys_with_logs}")
    if keys_without_logs:
        print(f"   Keys skipped:       {keys_without_logs} (no logs found)")
    print(f"   Processors created: {len(processors)} (1 per key)")
    print(f"   Grok rules created: {len(keys_with_logs) * 6} "
          f"(6 per key: 2 quoted + 2 kv + 2 unquoted, "
          f"each with start-of-line & boundary variant)")
    if config["dry_run"]:
        print(f"   Mode:               🏁 DRY RUN (no changes made)")
    else:
        print(f"   Mode:               🚀 LIVE (pipeline created)")
    print("=" * 60)
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
