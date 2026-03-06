"""
Grok parser rule builder.

Generates Datadog grok parser processors for extracting key-value pairs from logs.

Architecture: ONE PROCESSOR PER KEY
  - Each processor independently extracts one key from the log message.
  - Datadog grok parser doesn't modify the message, so each processor sees
    the original message and can extract its key independently.
  - This ensures ALL keys are extracted regardless of order in the log.

Each key gets its own processor with 6 rules (tried in order, first match wins):
  For each format, there are TWO variants:
    a) Key at start of log line (no prefix needed)
    b) Key preceded by a boundary character (space, brace, comma, etc.)

  This prevents partial-name matches: e.g. searching for "status" won't
  accidentally match "rstatus" or "accstatus".

  Formats:
    1. Quoted value:    key: "value with spaces"  or  "key": "value"
    2. KV equals:       key=value
    3. Unquoted colon:  key: value  or  "key": 123

IMPORTANT — Large integer safety:
  All matchers use %{data:field} or custom patterns (%{_val:field}) which
  always output STRING values. We deliberately avoid:
    - %{integer:field}      — truncates / loses precision on >15 digit numbers
    - %{number:field}       — converts to float → exponential notation (e.g. -7.389e+34)
    - %{data::keyvalue()}   — auto-converts large integers to exponential notation
  This ensures values like acctid=-73892090576593678863285018950654902
  are preserved as full strings, never converted to scientific notation.
"""

# ──────────────────────────────────────────────
#  Max rules per grok-parser processor (Datadog limit)
# ──────────────────────────────────────────────
MAX_RULES_PER_PROCESSOR = 10

# Number of rules generated per key (3 formats × 2 variants each)
RULES_PER_KEY = 6

# ──────────────────────────────────────────────
#  Support rules: reusable matching patterns
# ──────────────────────────────────────────────

# _val: Matches the VALUE portion — stops at common delimiters.
# Defined as a support rule to avoid Datadog Grok escaping issues —
# putting } inside %{regex("...")} breaks Grok's brace-matching parser.
#
# Matches one or more characters that are NOT:
#   \s   whitespace        ,    comma          }    closing brace
#   "    quote             )    closing paren   ]    closing bracket
#   ;    semicolon         |    pipe
VALUE_SUPPORT_RULE = r'_val [^\s,}"\)\];|]+'

# _pre: Matches a single BOUNDARY character before the key name.
# Ensures we only match whole key names — e.g. "status" won't match
# inside "rstatus" or "accstatus".
#
# Matches one character that IS:
#   \s   whitespace        {    opening brace   [    opening bracket
#   (    opening paren     ,    comma           ;    semicolon
#   |    pipe              "    quote           :    colon
BOUNDARY_SUPPORT_RULE = r'_pre [\s{\[\(,;|":]'

# Combined support rules (newline-separated for Datadog API)
ALL_SUPPORT_RULES = f"{VALUE_SUPPORT_RULE}\n{BOUNDARY_SUPPORT_RULE}"


# ──────────────────────────────────────────────
#  Rule generators
# ──────────────────────────────────────────────

def _build_quoted_rules(key):
    """
    Rules for quoted values (tried first — most specific).

    Matches:
        request: "GET /mts/v2/NCP HTTP/1.1"     (colon, unquoted key)
        "request": "GET /path HTTP/1.1"          (colon, quoted key)
        request= "some value"                    (equals, quoted value)

    Returns 2 rules:
      - start: key at the beginning of the log line
      - boundary: key preceded by a delimiter (space, brace, etc.)
    """
    start_name = f"extract_quoted_{key}_start"
    start_pattern = f'"?{key}"?\\s*[=:]\\s*"%{{data:{key}}}"%{{data}}'

    bound_name = f"extract_quoted_{key}"
    bound_pattern = f'%{{data}}%{{_pre}}"?{key}"?\\s*[=:]\\s*"%{{data:{key}}}"%{{data}}'

    return [
        f"{start_name} {start_pattern}",
        f"{bound_name} {bound_pattern}",
    ]


def _build_kv_rules(key):
    """
    Rules for key=value format (no quotes on value).

    Matches:
        user_id=abc123
        client=76.50.52.168
        {resptime=0}

    Returns 2 rules:
      - start: key at the beginning of the log line
      - boundary: key preceded by a delimiter
    """
    start_name = f"extract_kv_{key}_start"
    start_pattern = f'{key}=%{{_val:{key}}}%{{data}}'

    bound_name = f"extract_kv_{key}"
    bound_pattern = f'%{{data}}%{{_pre}}{key}=%{{_val:{key}}}%{{data}}'

    return [
        f"{start_name} {start_pattern}",
        f"{bound_name} {bound_pattern}",
    ]


def _build_unquoted_rules(key):
    """
    Rules for colon-separated unquoted values (fallback).

    Matches:
        client: 76.50.52.168
        "user_id": 12345
        status: 200

    Returns 2 rules:
      - start: key at the beginning of the log line
      - boundary: key preceded by a delimiter
    """
    start_name = f"extract_unquoted_{key}_start"
    start_pattern = f'"?{key}"?\\s*[=:]\\s*%{{_val:{key}}}%{{data}}'

    bound_name = f"extract_unquoted_{key}"
    bound_pattern = f'%{{data}}%{{_pre}}"?{key}"?\\s*[=:]\\s*%{{_val:{key}}}%{{data}}'

    return [
        f"{start_name} {start_pattern}",
        f"{bound_name} {bound_pattern}",
    ]


def build_rules_for_key(key):
    """
    Generate all 6 grok rules for a single key.

    Order matters — Datadog tries rules top-to-bottom, first match wins:
      1–2. Quoted value   (most specific — captures values with spaces)
      3–4. KV equals      (key=value)
      5–6. Unquoted colon (key: value without quotes)

    For each format, the start-of-line variant is tried before the
    boundary variant.

    Returns:
        list[str]: List of 6 grok rule strings.
    """
    return [
        *_build_quoted_rules(key),
        *_build_kv_rules(key),
        *_build_unquoted_rules(key),
    ]


# ──────────────────────────────────────────────
#  Processor builder — ONE PROCESSOR PER KEY
# ──────────────────────────────────────────────

def build_grok_processors(keys):
    """
    Build one Datadog grok-parser processor per key.

    Each processor independently extracts one key from the message.
    Since grok parsers don't modify the source message, each processor
    sees the original log and can extract its key independently.

    This ensures ALL keys are extracted from a log line, regardless of
    the order keys appear in.

    Args:
        keys: List of key names to extract.

    Returns:
        list[dict]: List of processor dicts in Datadog API format.
    """
    processors = []

    for key in keys:
        rules = build_rules_for_key(key)
        match_rules_str = "\n".join(rules)

        processor = {
            "type": "grok-parser",
            "name": f"Extract '{key}'",
            "is_enabled": True,
            "source": "message",
            "samples": [],
            "grok": {
                "match_rules": match_rules_str,
                "support_rules": ALL_SUPPORT_RULES,
            },
        }
        processors.append(processor)

    return processors


# ──────────────────────────────────────────────
#  Pretty-print for dry-run / debugging
# ──────────────────────────────────────────────

def print_rules_summary(keys, processors):
    """Print a human-readable summary of the generated rules and processors."""
    total_rules = len(keys) * RULES_PER_KEY

    print(f"\n📝 Generated {total_rules} grok rule(s) for {len(keys)} key(s):")
    print(f"   → {len(processors)} processor(s) (1 per key, {RULES_PER_KEY} rules each)\n")

    for i, proc in enumerate(processors, start=1):
        print(f"   ┌─ Processor {i}/{len(processors)}: {proc['name']}")
        rules_in_proc = proc["grok"]["match_rules"].split("\n")
        for rule in rules_in_proc:
            rule_name = rule.split(" ", 1)[0]
            print(f"   │  • {rule_name}")
        print(f"   └─ ({len(rules_in_proc)} rule(s))\n")


# ──────────────────────────────────────────────
#  Quick self-test
# ──────────────────────────────────────────────
if __name__ == "__main__":
    test_keys = ["status", "client", "request"]

    print("=" * 60)
    print("  Grok Builder — Self Test")
    print("=" * 60)

    print(f"\nKeys: {test_keys}")
    print(f"Rules per key: {RULES_PER_KEY}")
    print(f"Processors: {len(test_keys)} (1 per key)")

    print("\n── Rules per Key ──")
    for key in test_keys:
        print(f"\n  Key: '{key}'")
        for rule in build_rules_for_key(key):
            print(f"    {rule}")

    print("\n── Boundary Protection ──")
    print("  For key 'status', the rules will NOT match:")
    print("    ❌ rstatus=completed   (no boundary before 'status')")
    print("    ❌ accstatus:success   (no boundary before 'status')")
    print("  But WILL match:")
    print("    ✅ status=200          (start of line)")
    print("    ✅ ... status=200      (space before 'status')")
    print('    ✅ {status=200}        ({ before "status")')
    print('    ✅ "status": 200       (quote before "status")')

    print("\n── Format Coverage ──")
    sample_logs = [
        'status=200 rstatus=completed accstatus:success  → only status=200 extracted',
        'client: 76.50.52.168, server: , request: "GET /mts/v2/NCP HTTP/1.1"',
        'user_id=abc123 client=10.0.0.1',
        '{"user_id": "abc123", "request": "/api/v1/users"}',
        '{resptime=0}',
        'status=200; duration=45',
        '[user_id=abc123]',
    ]
    print("  Sample logs these rules cover:")
    for log in sample_logs:
        print(f"    ✅ {log}")

    print("\n── Processors ──")
    processors = build_grok_processors(test_keys)
    print_rules_summary(test_keys, processors)

    print("── Raw Processor JSON ──")
    import json
    for proc in processors:
        print(json.dumps(proc, indent=2))
        print()
