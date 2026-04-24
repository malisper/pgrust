#!/usr/bin/env bash
# Publish pgrust from the private Pager-Free archive to the public repo
# at github.com/malisper/pgrust.
#
# This script lives inside the private repo (scripts/internal/) and is
# stripped from every public commit by the filter-repo --path invert rule
# below. Neither the script nor redactions.txt ever reaches the public repo.
#
# Prereqs:
#   brew install git-filter-repo gitleaks gh
#   gh auth login     # signed in as an account with push access to malisper/pgrust
#   SSH key registered with that GitHub account
#
# Usage:
#   ./scripts/internal/publish-public.sh --republish-dry   # scrubs into ~/tmp, no push
#   ./scripts/internal/publish-public.sh --republish       # scrubs and force-pushes to main
#
# Safe to Ctrl-C at any point; nothing is pushed until the PAUSE gate in
# --republish mode has been passed.
#
# STATUS (2026-04-24): this is the new canonical publish script. An older
# inline-redactions version still lives at
#   pagerfree-shared/docs/hn-launch/ops/publish-pgrust.sh
# and was the one actually used for the 2026-04-24 republish. Both scripts
# currently produce the same scrubbed output (verified via --republish-dry).
# Once this script has been used for one successful real publish (not just
# dry-run), the old file will be replaced with a 3-line pointer here and
# retired. Until that swap happens, keep the redaction rules in sync:
# updates to scripts/internal/redactions.txt need matching edits to the
# heredoc in the old script.

set -euo pipefail

MODE=""
case "${1:-}" in
  --republish-dry) MODE="dry" ;;
  --republish)     MODE="push" ;;
  ""|--help|-h)
    cat <<EOF
Usage: $0 --republish-dry | --republish

Modes:
  --republish-dry    Scrub into ~/tmp and stop. No push, no side effects on origin.
  --republish        Scrub, pause at inspect gate, then force-push to malisper/pgrust:main.
EOF
    exit 0
    ;;
  *)
    echo "Unknown flag: $1 (use --republish-dry or --republish)" >&2
    exit 2
    ;;
esac

# ---------------------------------------------------------------------------
# 0. Self-check: we must be in the private pgrust repo before touching anything.
# ---------------------------------------------------------------------------
REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
  echo "ERROR: couldn't resolve git repo root. Run this script from inside the pgrust clone." >&2
  exit 2
fi

EXPECTED_ORIGIN_PATTERN='git@github\.com:Pager-Free/pgrust(\.git)?$'
ACTUAL_ORIGIN="$(git -C "$REPO_ROOT" config --get remote.origin.url 2>/dev/null || true)"
if [[ ! "$ACTUAL_ORIGIN" =~ $EXPECTED_ORIGIN_PATTERN ]]; then
  echo "ERROR: expected origin matching $EXPECTED_ORIGIN_PATTERN" >&2
  echo "       got: ${ACTUAL_ORIGIN:-<none>}" >&2
  echo "       refusing to run against an unexpected repo." >&2
  exit 2
fi

REDACTIONS_FILE="$REPO_ROOT/scripts/internal/redactions.txt"
if [[ ! -f "$REDACTIONS_FILE" ]]; then
  echo "ERROR: redactions file not found at $REDACTIONS_FILE" >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# 1. Config
# ---------------------------------------------------------------------------
ARCHIVE_URL="${PGRUST_SOURCE_REPO:-git@github.com:Pager-Free/pgrust.git}"
ARCHIVE_BRANCH="${PGRUST_SOURCE_BRANCH:-perf-optimization}"
PUBLIC_OWNER="malisper"
PUBLIC_NAME="pgrust"
PUBLIC_URL="git@github.com:${PUBLIC_OWNER}/${PUBLIC_NAME}.git"

WORKDIR_BASE="${PGRUST_WORKDIR_BASE:-$HOME/tmp}"
WORKDIR="$WORKDIR_BASE/pgrust-public-$(date +%Y%m%d-%H%M%S)"
CLONE="$WORKDIR/pgrust-public"

echo ">>> 1. Creating workspace: $WORKDIR"
mkdir -p "$WORKDIR"

# ---------------------------------------------------------------------------
# 2. Fresh clone of the private archive (always pulls the latest tip)
# ---------------------------------------------------------------------------
echo ">>> 2. Fresh clone of $ARCHIVE_URL ($ARCHIVE_BRANCH)"
git clone --no-local --branch "$ARCHIVE_BRANCH" "$ARCHIVE_URL" "$CLONE"

cd "$CLONE"
git checkout "$ARCHIVE_BRANCH"
git log -1 --oneline

# ---------------------------------------------------------------------------
# 3. filter-repo: drop private paths, strip scripts/internal, replace strings
# ---------------------------------------------------------------------------
echo ">>> 3. filter-repo: drop private paths (including this folder)"
git filter-repo --force \
  --path scripts/internal --invert-paths \
  --path issues.jsonl --invert-paths \
  --path domains --invert-paths \
  --path docs/shipments-query-gaps.md --invert-paths

echo ">>> 4. filter-repo: string replacements (contents AND commit messages)"
# Copy redactions into the scrubbed clone so filter-repo can find the file
# even though the clone no longer has scripts/internal/. Using a sibling
# path in the workdir keeps the scrubbed clone's tree clean.
EXTERNAL_REDACTIONS="$WORKDIR/redactions.txt"
cp "$REDACTIONS_FILE" "$EXTERNAL_REDACTIONS"
git filter-repo --force \
  --replace-text "$EXTERNAL_REDACTIONS" \
  --replace-message "$EXTERNAL_REDACTIONS"

# ---------------------------------------------------------------------------
# 4. Verify scrubs
# ---------------------------------------------------------------------------
echo ">>> 5. Verifying scrubs (should print nothing below this line)"
# Build a single alt-regex from the redactions file so this grows automatically.
LEAK_RE="$(
  awk -F'==>' '/^[^#]/ && NF>=1 && length($1)>0 { print $1 }' "$REDACTIONS_FILE" |
    sed 's/[][\/.^$*+?(){}|]/\\&/g' |
    paste -sd'|' -
)"

echo "--- current-tree grep ---"
# git grep: -E switches to extended regex (good). rg uses -e for pattern input
# because plain -E is the encoding flag.
git grep -n --all -E "$LEAK_RE" 2>/dev/null || true
echo "--- full-history grep (content + commit messages) ---"
if git log --all -p | rg -q -e "$LEAK_RE"; then
  echo "LEAK FOUND — scrub missed something. Aborting." >&2
  exit 1
fi
echo "--- scrubs verified clean ---"
echo "(note: michaelmalis2@gmail.com stays in commit author metadata — intentional.)"

echo ">>> 6. Expire reflog + aggressive gc"
git reflog expire --expire=now --all
git gc --prune=now --aggressive >/dev/null 2>&1

echo ">>> 7. Scan unreachable objects for sensitive strings"
if git fsck --full --unreachable 2>/dev/null | rg -q 'unreachable (blob|commit)'; then
  mapfile -t UNREACHABLE < <(git fsck --full --unreachable 2>/dev/null | awk '/unreachable blob/ {print $3}')
  for sha in "${UNREACHABLE[@]}"; do
    if git cat-file -p "$sha" 2>/dev/null | rg -q -e "$LEAK_RE"; then
      echo "LEAK in unreachable blob $sha — aborting." >&2
      exit 1
    fi
  done
  echo "  unreachable blobs clean"
else
  echo "  no unreachable objects"
fi

echo ">>> 8. Final gate: gitleaks"
if command -v gitleaks >/dev/null 2>&1; then
  gitleaks detect --no-banner --source . && echo "  gitleaks clean"
else
  echo "  gitleaks not installed — skipping. Install via: brew install gitleaks"
fi

NEW_HEAD="$(git rev-parse HEAD)"

# ---------------------------------------------------------------------------
# 5. Dry-mode: print summary and exit
# ---------------------------------------------------------------------------
if [[ "$MODE" == "dry" ]]; then
  echo ""
  echo "REPUBLISH DRY RUN COMPLETE."
  echo "  Scrubbed clone:   $CLONE"
  echo "  Source branch:    $ARCHIVE_BRANCH"
  echo "  Scrubbed HEAD:    $NEW_HEAD"
  echo "  Commit count:     $(git log --oneline | wc -l | tr -d ' ')"
  echo "  Tags local:       $(git tag -l | tr '\n' ' ')"
  echo ""
  echo "Inspect manually:"
  echo "  cd $CLONE"
  echo "  git log --oneline | head"
  echo "  git ls-tree -r HEAD --name-only | rg '^(issues\\.jsonl|domains/|docs/shipments-query-gaps\\.md|scripts/internal/)'   # expect empty"
  echo "  git log --all -p | rg -e \"\$LEAK_RE\"   # expect empty"
  echo ""
  echo "When ready to FORCE-PUSH over github.com/${PUBLIC_OWNER}/${PUBLIC_NAME}:main:"
  echo "  $0 --republish"
  exit 0
fi

# ---------------------------------------------------------------------------
# 6. Republish: pause, then force-push
# ---------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo "PAUSE — inspect before REPUBLISH (force-push)."
echo ""
echo "  Scrubbed clone:   $CLONE"
echo "  Source branch:    $ARCHIVE_BRANCH"
echo "  Scrubbed HEAD:    $NEW_HEAD"
echo "  Target:           github.com/${PUBLIC_OWNER}/${PUBLIC_NAME}:main"
echo ""
echo "THIS WILL FORCE-PUSH. Every pre-existing public commit SHA will change."
echo "Heads up on public repo clones (if any): next 'git pull' will require"
echo "  git fetch origin && git reset --hard origin/main"
echo ""
echo "To abort, press Ctrl-C. Nothing has been pushed yet."
echo "========================================================================"
read -r -p "Press ENTER to force-push, or Ctrl-C to abort: "
echo ""

echo ">>> Adding public remote and fetching main for lease check"
git remote add origin "$PUBLIC_URL"
# Fetch so --force-with-lease has a cached remote-tracking ref to compare
# against. Without this it fails with "stale info".
git fetch origin main

echo ">>> Force-pushing ${ARCHIVE_BRANCH} -> main"
git push --force-with-lease origin "${ARCHIVE_BRANCH}:main"

echo ""
echo "REPUBLISH DONE."
echo "  New public HEAD: $NEW_HEAD"
echo "  https://github.com/${PUBLIC_OWNER}/${PUBLIC_NAME}/commit/$NEW_HEAD"
echo ""
echo "Post-push follow-up:"
echo "  1. Sync your local public clone (if any):"
echo "     git -C ~/dev/2026/pagerfreeglobal/pgrust-public fetch origin && \\"
echo "     git -C ~/dev/2026/pagerfreeglobal/pgrust-public reset --hard origin/main"
echo "  2. If a stale release/nightly tag points at an orphaned commit:"
echo "     git -C ~/dev/2026/pagerfreeglobal/pgrust-public push origin :refs/tags/<tag>"
echo "  3. Sanity check: fresh clone + gitleaks, README renders, commit SHA matches."
