#!/usr/bin/env bash
# Pre-submission gate for the NOAA Weather Intelligence connector.
# Run from the connector directory. Exits non-zero if any check fails.
#
# Reference pattern intended for promotion to the
# fivetran-connector-sdk-submission skill — the skill should invoke this
# (or its equivalent) before any `fivetran deploy`.
set -euo pipefail

cd "$(dirname "$0")/.."

# Line length matches repo's .flake8 (max-line-length=99) and .github/git_hooks/pre-commit (black --line-length=99).
LINE_LEN=99

echo "=== 1/5 flake8 (code quality) ==="
python3 -m flake8 connector.py

echo "=== 2/5 black --check (formatting, line-length=$LINE_LEN) ==="
python3 -m black --check --line-length=$LINE_LEN connector.py

echo "=== 3/5 PII scan (emails in source, embedded tokens) ==="
# Flag any email address in connector.py except role-based addresses
# (developers@, noreply@, hello@, support@, api@, admin@, security@).
FLAGGED_EMAILS=$(grep -En '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' connector.py \
    | grep -vE '(developers|noreply|hello|support|api|admin|security)@' || true)
if [ -n "$FLAGGED_EMAILS" ]; then
    echo "ERROR: possible personal email found in connector.py:"
    echo "$FLAGGED_EMAILS"
    echo "Hint: use a role-based address (e.g., developers@fivetran.com)."
    exit 1
fi
if grep -En 'dapi[a-f0-9]{20,}|Bearer\s+[A-Za-z0-9_-]{30,}' connector.py ; then
    echo "ERROR: embedded secret pattern found in connector.py"
    exit 1
fi
echo "clean"

echo "=== 4/5 pytest (unit + integration) ==="
python3 -m pytest tests/ -v --tb=short

echo "=== 5/5 README/config drift check ==="
if [ -f configuration.json ]; then
    python3 - <<'PY'
import json
import re
from pathlib import Path

cfg = json.loads(Path("configuration.json").read_text())
readme = Path("README.md").read_text()
missing = []
for key in cfg:
    if key not in readme:
        missing.append(key)
if missing:
    print(f"ERROR: config keys missing from README: {missing}")
    raise SystemExit(1)
print(f"All {len(cfg)} configuration keys documented in README.")
PY
fi

echo ""
echo "✓ All pre-submission checks passed."
