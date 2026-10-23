#!/usr/bin/env bash
# ============================================================================
# Publish paper_code_data/ -> the PRIVATE GitHub repo negin-katal/tree-mortality-EFPs.
#
# paper_code_data/ is rebuilt by build_package.py, so it holds no .git of its own:
# the repo is cloned to a temp folder, synced and pushed from there.
#
# Usage:  bash scripts/push_code_package_to_github.sh ["message"]
# ============================================================================
set -euo pipefail

MSG=${1:-"Update code and data package"}
SRC="/mnt/gsdata/projects/panops/panops-data-registry/data/flux/paper_code_data"
REMOTE="git@github-personal:negin-katal/tree-mortality-EFPs.git"

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
echo "==> cloning $REMOTE"
git clone --quiet "$REMOTE" "$TMP/repo"

echo "==> syncing package"
rsync -a --delete --exclude='.git' --exclude='__pycache__' "$SRC/" "$TMP/repo/"

cd "$TMP/repo"
if [ -z "$(git status --porcelain)" ]; then echo "==> nothing to commit"; exit 0; fi
git add -A
git commit --quiet -m "$MSG"
git push origin HEAD:main
echo
echo "Done. Open:  https://github.com/negin-katal/tree-mortality-EFPs"
