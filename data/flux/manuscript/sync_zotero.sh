#!/usr/bin/env bash
# Sync Zotero "Tree mortality" collection → sn-bibliography.bib
# Run from the project root: bash manuscript/sync_zotero.sh
# Credentials are NOT stored here: ~/.config/zotero/credentials (line 1: user ID, line 2: API key; chmod 600),
# or the environment variables ZOTERO_USER_ID / ZOTERO_API_KEY.

set -e

CRED=~/.config/zotero/credentials
if [ -z "$ZOTERO_USER_ID" ] || [ -z "$ZOTERO_API_KEY" ]; then
  [ -f "$CRED" ] || { echo "No Zotero credentials: create $CRED (user ID, API key) or set ZOTERO_USER_ID / ZOTERO_API_KEY"; exit 1; }
  ZOTERO_USER_ID=$(sed -n 1p "$CRED" | tr -d '[:space:]'); ZOTERO_API_KEY=$(sed -n 2p "$CRED" | tr -d '[:space:]')
fi
COLLECTION=4B2EEQ27   # "Tree mortality"
BIB=manuscript/sn-bibliography.bib
TMP=/tmp/zotero_sync_$$.bib
trap 'rm -f "$TMP"' EXIT

echo "Fetching Zotero collection..."
: > "$TMP"
START=0
while : ; do   # the API returns at most 100 items per request: page through the whole collection
  PAGE=$(curl -s -H "Zotero-API-Key: ${ZOTERO_API_KEY}" \
    "https://api.zotero.org/users/${ZOTERO_USER_ID}/collections/${COLLECTION}/items/top?format=bibtex&limit=100&start=${START}")
  N=$(printf '%s\n' "$PAGE" | grep -c "^@" || true)
  printf '%s\n\n' "$PAGE" >> "$TMP"
  [ "$N" -lt 100 ] && break
  START=$((START + 100))
done
COUNT=$(grep -c "^@" "$TMP" || true)
echo "  $COUNT entries fetched."

python3 - << PYEOF
import re

existing = open("${BIB}").read()
existing_keys = set(re.findall(r'^@\w+\{(\S+?),', existing, re.MULTILINE))

zotero = open("${TMP}").read()
entries = re.split(r'\n(?=@)', zotero.strip())

added = []
for entry in entries:
    m = re.match(r'@\w+\{(\S+?),', entry)
    if not m:
        continue
    key = m.group(1)
    if key not in existing_keys:
        added.append((key, entry))
        existing_keys.add(key)

if added:
    with open("${BIB}", "a") as f:
        for key, entry in added:
            f.write("\n" + entry.strip() + "\n")
    print(f"Added {len(added)} new entries.")
else:
    print("No new entries — bibliography is up to date.")
PYEOF
