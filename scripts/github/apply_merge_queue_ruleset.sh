#!/usr/bin/env bash
set -euo pipefail

OWNER="${OWNER:-your-github-org}"
REPO="${REPO:-pgrust}"
BRANCH="${BRANCH:-perf-optimization}"
RULESET_NAME="${RULESET_NAME:-perf-optimization status checks}"
REQUIRED_CHECK_CONTEXT="${REQUIRED_CHECK_CONTEXT:-cargo-test}"
MERGE_METHOD="${MERGE_METHOD:-MERGE}"
BUILD_CONCURRENCY="${BUILD_CONCURRENCY:-1}"
MIN_ENTRIES_TO_MERGE="${MIN_ENTRIES_TO_MERGE:-1}"
MAX_ENTRIES_TO_MERGE="${MAX_ENTRIES_TO_MERGE:-5}"
MIN_ENTRIES_WAIT_MINUTES="${MIN_ENTRIES_WAIT_MINUTES:-0}"
CHECK_TIMEOUT_MINUTES="${CHECK_TIMEOUT_MINUTES:-60}"
GROUPING_STRATEGY="${GROUPING_STRATEGY:-ALLGREEN}"

API_VERSION="2026-03-10"
REPO_API="repos/${OWNER}/${REPO}"
ALLOWED_PULL_REQUEST_METHOD="${MERGE_METHOD,,}"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh is required" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "gh must be authenticated before running this script" >&2
  exit 1
fi

repo_id="$(
  gh api "${REPO_API}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    --jq '.id'
)"

ruleset_id="$(
  RULESET_NAME="${RULESET_NAME}" BRANCH_REF="refs/heads/${BRANCH}" gh api "${REPO_API}/rulesets" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    --jq '
      (
        .[] | select(.name == env.RULESET_NAME) | .id
      ),
      (
        .[]
        | select(.target == "branch")
        | select(any(.conditions.ref_name.include[]?; . == env.BRANCH_REF))
        | select(any(.rules[]?; .type == "merge_queue"))
        | .id
      )
    ' \
    | head -n1
)"

payload_file="$(mktemp)"
trap 'rm -f "${payload_file}"' EXIT

cat >"${payload_file}" <<JSON
{
  "name": "${RULESET_NAME}",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/${BRANCH}"],
      "exclude": []
    }
  },
  "rules": [
    {
      "type": "pull_request",
      "parameters": {
        "allowed_merge_methods": ["${ALLOWED_PULL_REQUEST_METHOD}"],
        "dismiss_stale_reviews_on_push": false,
        "require_code_owner_review": false,
        "require_last_push_approval": false,
        "required_approving_review_count": 0,
        "required_review_thread_resolution": false
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "do_not_enforce_on_create": true,
        "strict_required_status_checks_policy": false,
        "required_status_checks": [
          {
            "context": "${REQUIRED_CHECK_CONTEXT}"
          }
        ]
      }
    },
    {
      "type": "merge_queue",
      "parameters": {
        "check_response_timeout_minutes": ${CHECK_TIMEOUT_MINUTES},
        "grouping_strategy": "${GROUPING_STRATEGY}",
        "max_entries_to_build": ${BUILD_CONCURRENCY},
        "max_entries_to_merge": ${MAX_ENTRIES_TO_MERGE},
        "merge_method": "${MERGE_METHOD}",
        "min_entries_to_merge": ${MIN_ENTRIES_TO_MERGE},
        "min_entries_to_merge_wait_minutes": ${MIN_ENTRIES_WAIT_MINUTES}
      }
    },
    {
      "type": "non_fast_forward"
    }
  ]
}
JSON

gh api -X PATCH "${REPO_API}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: ${API_VERSION}" \
  -F allow_auto_merge=true \
  -F allow_merge_commit="$([[ "${MERGE_METHOD}" == "MERGE" ]] && echo true || echo false)" \
  -F allow_rebase_merge="$([[ "${MERGE_METHOD}" == "REBASE" ]] && echo true || echo false)" \
  -F allow_squash_merge="$([[ "${MERGE_METHOD}" == "SQUASH" ]] && echo true || echo false)" \
  >/dev/null

if [[ -n "${ruleset_id}" ]]; then
  gh api -X PUT "${REPO_API}/rulesets/${ruleset_id}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    --input "${payload_file}" \
    >/dev/null
  echo "Updated ruleset ${ruleset_id} for ${OWNER}/${REPO}"
else
  gh api -X POST "${REPO_API}/rulesets" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    --input "${payload_file}" \
    >/dev/null
  echo "Created ruleset ${RULESET_NAME} for ${OWNER}/${REPO}"
fi

echo "Repository id: ${repo_id}"
echo "Required check context: ${REQUIRED_CHECK_CONTEXT}"
echo "Target branch: ${BRANCH}"
