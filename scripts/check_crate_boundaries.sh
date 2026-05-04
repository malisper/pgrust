#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failed=0

while IFS= read -r manifest; do
    crate_dir="$(dirname "$manifest")"
    crate_name="$(basename "$crate_dir")"
    if rg -n '(^|[[:space:]])pgrust[[:space:]]*=' "$manifest" >/tmp/pgrust-boundary-match.$$; then
        echo "error: $crate_name depends upward on root pgrust package:" >&2
        sed 's/^/  /' /tmp/pgrust-boundary-match.$$ >&2
        failed=1
    fi
    if rg -n 'package[[:space:]]*=[[:space:]]*"pgrust"' "$manifest" >/tmp/pgrust-boundary-match.$$; then
        echo "error: $crate_name aliases a dependency to root pgrust package:" >&2
        sed 's/^/  /' /tmp/pgrust-boundary-match.$$ >&2
        failed=1
    fi
done < <(find "$repo_root/crates" -mindepth 2 -maxdepth 2 -name Cargo.toml | sort)

rm -f /tmp/pgrust-boundary-match.$$
exit "$failed"
