#!/usr/bin/env bash
# Post a regression-run summary to Slack via incoming webhook.
#
# Inputs (env):
#   SLACK_WEBHOOK_URL    incoming webhook (required; if empty, exit 0 silently)
#   RESULTS_DIR          path containing summary.json
#   RUN_DIR_NAME         e.g. 2026-04-24T1930Z
#   PREV_DIR_NAME        previous run dir, or empty
#   PREV_TESTS_PASSED    previous run's tests.passed (int) or empty
#   PREV_QUERIES_MATCHED previous run's queries.matched (int) or empty
#   RUN_SHA              commit under test
#   RUN_URL              workflow run URL
#   REPO_FULL_NAME       e.g. your-github-org/pgrust
#   RUN_ATTEMPT          GitHub run_attempt (1 = initial, 2 = after auto-rerun)
#   MAX_ATTEMPTS         total attempts allowed under our retry policy (default: 2)

set -euo pipefail

if [[ -z "${SLACK_WEBHOOK_URL:-}" ]]; then
    echo "SLACK_WEBHOOK_URL not set; skipping Slack notification."
    exit 0
fi

SUMMARY_PATH="${RESULTS_DIR:-}/summary.json"
if [[ ! -f "$SUMMARY_PATH" ]]; then
    echo "No summary.json at $SUMMARY_PATH; sending degraded Slack message."
fi

PAYLOAD="$(python3 - <<'PY'
import json, os, pathlib

summary_path = pathlib.Path(os.environ.get("RESULTS_DIR", "")) / "summary.json"
if summary_path.exists():
    summary = json.loads(summary_path.read_text())
else:
    summary = {"status": "missing", "tests": {}, "queries": {}}

tests = summary.get("tests", {})
queries = summary.get("queries", {})

def fmt_delta(curr, prev_str):
    if prev_str == "" or prev_str is None:
        return ""
    try:
        prev = int(prev_str)
    except ValueError:
        return ""
    delta = curr - prev
    if delta == 0:
        return "  (Δ 0)"
    sign = "+" if delta > 0 else ""
    return f"  (Δ {sign}{delta} vs prev)"

passed = tests.get("passed", 0)
total = tests.get("total", 0)
pass_pct = tests.get("pass_rate_pct", 0)

q_matched = queries.get("matched", 0)
q_total = queries.get("total", 0)
q_pct = queries.get("match_rate_pct", 0)

passed_delta = fmt_delta(passed, os.environ.get("PREV_TESTS_PASSED", ""))
matched_delta = fmt_delta(q_matched, os.environ.get("PREV_QUERIES_MATCHED", ""))

run_dir = os.environ.get("RUN_DIR_NAME", "?")
sha = os.environ.get("RUN_SHA", "")[:12]
run_url = os.environ.get("RUN_URL", "")
repo = os.environ.get("REPO_FULL_NAME", "your-github-org/pgrust")
prev_dir = os.environ.get("PREV_DIR_NAME", "")

history_url = f"https://github.com/{repo}/tree/regression-history/runs/{run_dir}"
latest_url = f"https://github.com/{repo}/tree/regression-history/runs/latest"

status = summary.get("status", "")
def _int_env(name, default):
    raw = os.environ.get(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default

run_attempt = _int_env("RUN_ATTEMPT", 1)
max_attempts = _int_env("MAX_ATTEMPTS", 2)
is_first_attempt = run_attempt <= 1
is_final_attempt = run_attempt >= max_attempts
incomplete_statuses = {"partial", "deadline", "aborted", "missing"}
will_retry = (not is_final_attempt) and (status in incomplete_statuses)

if status == "completed":
    if is_first_attempt:
        emoji, header_suffix = ":test_tube:", ""
    else:
        emoji, header_suffix = ":white_check_mark:", " — Complete after retry"
elif will_retry:
    emoji = ":warning:"
    header_suffix = f" — {status.capitalize()}. Auto-rerun queued (attempt {run_attempt} of {max_attempts})."
elif status in incomplete_statuses and is_final_attempt:
    emoji = ":rotating_light:"
    header_suffix = f" — Still {status} after retry. Manual investigation needed."
else:
    emoji, header_suffix = ":test_tube:", ""

header = f"{emoji} pgrust regression — {run_dir}{header_suffix}"

attempt_line = f"*Attempt:* {run_attempt} of {max_attempts}"
if will_retry:
    attempt_line += " — auto-rerun pending; this Slack message will be superseded by the retry result"
elif run_attempt > 1:
    attempt_line += " — final attempt under retry policy"

lines = [
    f"*Status:* `{status or 'unknown'}`  *Commit:* `{sha}`",
    attempt_line,
    f"*Tests:* {passed}/{total} passed ({pass_pct}%){passed_delta}",
    f"*Queries:* {q_matched:,}/{q_total:,} matched ({q_pct}%){matched_delta}",
    f"*Full output:* <{history_url}|runs/{run_dir}>  •  <{latest_url}|runs/latest>",
    f"*Workflow run:* <{run_url}|view in Actions>",
]
if prev_dir:
    lines.append(f"_Previous run: `{prev_dir}`_")

payload = {
    "blocks": [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
    ],
    "text": header,  # fallback for notifications
}
print(json.dumps(payload))
PY
)"

echo "Posting to Slack..."
http_status="$(curl -sS -o /tmp/slack-response -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data "$PAYLOAD" \
    "$SLACK_WEBHOOK_URL")"

if [[ "$http_status" != "200" ]]; then
    echo "Slack webhook returned HTTP $http_status:" >&2
    cat /tmp/slack-response >&2 || true
    exit 1
fi
echo "Slack notification sent."
