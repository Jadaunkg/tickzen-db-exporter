#!/usr/bin/env bash
# Copy the stock-data pipeline from the main TickZen repo into this exporter.
#
# Every file listed in scripts/sync_manifest.txt is copied byte-for-byte, so the exporter
# writes exactly the same rows as tickzen-github's database/cron_update_runner.py.
# Exporter-only files (cron_update_runner.py, app/html_components.py, Dockerfile, deploy/,
# requirements.txt, README.md) are NOT in the manifest and are never overwritten.
#
#   scripts/sync_from_tickzen.sh                     # copy from ~/Desktop/tickzen-github
#   scripts/sync_from_tickzen.sh /path/to/tickzen    # copy from another checkout
#   scripts/sync_from_tickzen.sh --check             # only report files that differ (exit 1 if any)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK=0
SRC="${HOME}/Desktop/tickzen-github"
for arg in "$@"; do
  case "$arg" in
    --check) CHECK=1 ;;
    *) SRC="$arg" ;;
  esac
done

if [[ ! -f "$SRC/database/cron_update_runner.py" ]]; then
  echo "Not a TickZen checkout: $SRC" >&2
  exit 2
fi

changed=0
while IFS= read -r rel; do
  [[ -z "$rel" || "$rel" == \#* ]] && continue
  if [[ ! -f "$SRC/$rel" ]]; then
    echo "MISSING in source: $rel" >&2
    changed=1
    continue
  fi
  if ! cmp -s "$SRC/$rel" "$HERE/$rel" 2>/dev/null; then
    changed=1
    if [[ $CHECK -eq 1 ]]; then
      echo "differs: $rel"
    else
      mkdir -p "$(dirname "$HERE/$rel")"
      cp "$SRC/$rel" "$HERE/$rel"
      echo "updated: $rel"
    fi
  fi
done < "$HERE/scripts/sync_manifest.txt"

if [[ $CHECK -eq 1 ]]; then
  [[ $changed -eq 0 ]] && echo "In sync with $SRC"
  exit $changed
fi
[[ $changed -eq 0 ]] && echo "Already in sync with $SRC"
echo "Source revision: $(git -C "$SRC" rev-parse --short HEAD 2>/dev/null || echo unknown)$(git -C "$SRC" diff --quiet 2>/dev/null || echo ' (+ uncommitted changes)')"
