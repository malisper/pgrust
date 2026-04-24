#!/usr/bin/env bash
# Publish a regression run's full output to the regression-history orphan branch.
#
# Inputs (env):
#   RESULTS_DIR          path to scripts/run_regression.sh --results-dir
#   RUN_ID, RUN_ATTEMPT  GitHub Actions identifiers
#   RUN_SHA, RUN_REF     commit and ref under test
#   RUN_URL              link back to the workflow run
#   REGRESS_EXIT_CODE    exit code of run_regression.sh (string)
#
# Outputs (GITHUB_OUTPUT):
#   run_dir_name         e.g. 2026-04-24T1930Z
#   prev_dir_name        previous run dir, or empty on first run
#   prev_tests_passed    integer, or empty
#   prev_queries_matched integer, or empty
#
# Side effects:
#   Pushes a commit to origin/regression-history with:
#     runs/<run_dir_name>/{summary.json,summary.md,meta.json,output/,diff/}
#     runs/latest/  (a copy of the new run)
#     index.tsv     (one new row appended)

set -euo pipefail

if [[ -z "${RESULTS_DIR:-}" || ! -d "$RESULTS_DIR" ]]; then
    echo "publish_regression_history: RESULTS_DIR not set or missing" >&2
    exit 1
fi

if [[ -z "${REGRESS_EXIT_CODE:-}" && -f "$RESULTS_DIR/exit_code.txt" ]]; then
    REGRESS_EXIT_CODE="$(tr -d '\r\n' < "$RESULTS_DIR/exit_code.txt")"
fi

# Use the GitHub Actions auto-token by default; allow override for local testing.
GH_REPO_SLUG="${GITHUB_REPOSITORY:-your-github-org/pgrust}"
HISTORY_REMOTE="${HISTORY_REMOTE:-https://x-access-token:${GITHUB_TOKEN:?GITHUB_TOKEN required}@github.com/${GH_REPO_SLUG}.git}"
HISTORY_BRANCH=regression-history

RUN_DIR_NAME="$(date -u +'%Y-%m-%dT%H%MZ')"
WORKDIR="$(mktemp -d /tmp/pgrust-history.XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "Cloning $HISTORY_BRANCH into $WORKDIR..."
git clone --branch "$HISTORY_BRANCH" --single-branch --depth 50 "$HISTORY_REMOTE" "$WORKDIR" >/dev/null 2>&1 || {
    echo "ERROR: failed to clone $HISTORY_BRANCH. Has it been initialized?" >&2
    exit 1
}

cd "$WORKDIR"
git config user.name "pgrust-regression-bot"
git config user.email "ci@pagerfree.com"

# Capture previous run summary (if any) before we add the new one.
PREV_DIR_NAME=""
PREV_TESTS_PASSED=""
PREV_QUERIES_MATCHED=""
if [[ -f runs/latest/summary.json && -f runs/latest/meta.json ]]; then
    PREV_DIR_NAME="$(python3 -c 'import json,sys; print(json.load(open("runs/latest/meta.json")).get("run_dir_name",""))')"
    PREV_TESTS_PASSED="$(python3 -c 'import json; print(json.load(open("runs/latest/summary.json"))["tests"]["passed"])')"
    PREV_QUERIES_MATCHED="$(python3 -c 'import json; print(json.load(open("runs/latest/summary.json"))["queries"]["matched"])')"
fi

NEW_RUN_DIR="runs/$RUN_DIR_NAME"
mkdir -p "$NEW_RUN_DIR/output" "$NEW_RUN_DIR/diff"

# Copy summary if present.
if [[ -f "$RESULTS_DIR/summary.json" ]]; then
    cp "$RESULTS_DIR/summary.json" "$NEW_RUN_DIR/summary.json"
fi

# Copy per-test output and diffs (may be missing if run died very early).
if [[ -d "$RESULTS_DIR/output" ]]; then
    cp -R "$RESULTS_DIR/output/." "$NEW_RUN_DIR/output/" 2>/dev/null || true
fi
if [[ -d "$RESULTS_DIR/diff" ]]; then
    cp -R "$RESULTS_DIR/diff/." "$NEW_RUN_DIR/diff/" 2>/dev/null || true
fi

# Write meta + human-readable summary.
python3 - "$NEW_RUN_DIR" <<'PY'
import json, os, sys, datetime, pathlib

run_dir = pathlib.Path(sys.argv[1])
summary_path = run_dir / "summary.json"
summary = {}
if summary_path.exists():
    summary = json.loads(summary_path.read_text())

meta = {
    "run_dir_name": run_dir.name,
    "run_id": os.environ.get("RUN_ID", ""),
    "run_attempt": os.environ.get("RUN_ATTEMPT", ""),
    "run_sha": os.environ.get("RUN_SHA", ""),
    "run_ref": os.environ.get("RUN_REF", ""),
    "run_url": os.environ.get("RUN_URL", ""),
    "regress_exit_code": os.environ.get("REGRESS_EXIT_CODE", ""),
    "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
}
(run_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")

tests = summary.get("tests", {})
queries = summary.get("queries", {})

lines = []
lines.append(f"# Regression run {run_dir.name}")
lines.append("")
lines.append(f"- Commit: `{meta['run_sha']}`")
lines.append(f"- Ref: `{meta['run_ref']}`")
lines.append(f"- Workflow run: {meta['run_url']}")
lines.append(f"- Status: `{summary.get('status', 'unknown')}`")
lines.append(f"- Regress script exit code: `{meta['regress_exit_code']}`")
lines.append("")
lines.append("## Tests")
lines.append("")
lines.append(f"- Planned: {tests.get('planned', 0)}")
lines.append(f"- Total:   {tests.get('total', 0)}")
lines.append(f"- Passed:  {tests.get('passed', 0)}")
lines.append(f"- Failed:  {tests.get('failed', 0)}")
lines.append(f"- Errored: {tests.get('errored', 0)}")
lines.append(f"- Pass rate: {tests.get('pass_rate_pct', 0)}%")
lines.append("")
lines.append("## Queries")
lines.append("")
lines.append(f"- Total:      {queries.get('total', 0)}")
lines.append(f"- Matched:    {queries.get('matched', 0)}")
lines.append(f"- Mismatched: {queries.get('mismatched', 0)}")
lines.append(f"- Match rate: {queries.get('match_rate_pct', 0)}%")
lines.append("")
(run_dir / "summary.md").write_text("\n".join(lines))
PY

# Refresh runs/latest as a copy (symlinks render badly on github.com).
rm -rf runs/latest
mkdir -p runs/latest
cp -R "$NEW_RUN_DIR/." runs/latest/

# Append to index.tsv.
python3 - "$NEW_RUN_DIR" >> index.tsv <<'PY'
import json, os, sys, pathlib
run_dir = pathlib.Path(sys.argv[1])
summary_path = run_dir / "summary.json"
summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
tests = summary.get("tests", {})
queries = summary.get("queries", {})
fields = [
    run_dir.name,
    os.environ.get("RUN_SHA", "")[:12],
    str(tests.get("passed", 0)),
    str(tests.get("total", 0)),
    str(tests.get("pass_rate_pct", 0)),
    str(queries.get("matched", 0)),
    str(queries.get("total", 0)),
    str(queries.get("match_rate_pct", 0)),
    os.environ.get("RUN_URL", ""),
]
print("\t".join(fields))
PY

# Make sure index.tsv has a header on first write.
if [[ "$(wc -l < index.tsv)" -eq 1 ]]; then
    HEADER=$'run_dir_name\trun_sha\ttests_passed\ttests_total\ttests_pass_rate_pct\tqueries_matched\tqueries_total\tqueries_match_rate_pct\trun_url'
    {
        echo "$HEADER"
        cat index.tsv
    } > index.tsv.tmp
    mv index.tsv.tmp index.tsv
fi

git add -A
if git diff --cached --quiet; then
    echo "Nothing to commit (results dir was empty?)."
else
    git commit -q -m "regression run $RUN_DIR_NAME ($(echo "$RUN_SHA" | cut -c1-12))"
    # Retry push a couple of times in case the two daily runs happen to race.
    for attempt in 1 2 3; do
        if git push origin "$HISTORY_BRANCH"; then
            break
        fi
        echo "push attempt $attempt failed; pulling --rebase and retrying..." >&2
        git pull --rebase origin "$HISTORY_BRANCH"
    done
fi

{
    echo "run_dir_name=$RUN_DIR_NAME"
    echo "prev_dir_name=$PREV_DIR_NAME"
    echo "prev_tests_passed=$PREV_TESTS_PASSED"
    echo "prev_queries_matched=$PREV_QUERIES_MATCHED"
} >> "${GITHUB_OUTPUT:-/dev/null}"

echo "Published $RUN_DIR_NAME to $HISTORY_BRANCH."
