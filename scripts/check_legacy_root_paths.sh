#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failed=0

external_pattern='(^|[^A-Za-z0-9_])pgrust::(backend|include|pl::|pgrust|executor|parser)::'
crate_pattern='crate::(backend|include|pgrust|pl)::'
root_exports_pattern='^pub mod (backend|include|pgrust|pl);'
expr_public_pattern='(^|[^A-Za-z0-9_])pgrust_expr::(backend|compat)::'
expr_exports_pattern='^pub mod (backend|compat|expr_backend);'

if rg -n "$external_pattern" "$repo_root/src/bin" "$repo_root/src/wasm.rs" >/tmp/pgrust-legacy-paths.$$; then
    echo "error: public binaries/wasm use removed pgrust root compatibility paths:" >&2
    sed 's/^/  /' /tmp/pgrust-legacy-paths.$$ >&2
    failed=1
fi

if rg -n "$crate_pattern" "$repo_root/crates" >/tmp/pgrust-legacy-paths.$$; then
    echo "error: workspace crates use removed internal compatibility paths:" >&2
    sed 's/^/  /' /tmp/pgrust-legacy-paths.$$ >&2
    failed=1
fi

if rg -n "$root_exports_pattern" "$repo_root/src/lib.rs" >/tmp/pgrust-legacy-paths.$$; then
    echo "error: root crate publicly exports removed compatibility modules:" >&2
    sed 's/^/  /' /tmp/pgrust-legacy-paths.$$ >&2
    failed=1
fi

if rg -n "$expr_public_pattern" "$repo_root/src" "$repo_root/crates" >/tmp/pgrust-legacy-paths.$$; then
    echo "error: workspace code uses removed pgrust_expr compatibility paths:" >&2
    sed 's/^/  /' /tmp/pgrust-legacy-paths.$$ >&2
    failed=1
fi

if rg -n "$expr_exports_pattern" "$repo_root/crates/pgrust_expr/src/lib.rs" >/tmp/pgrust-legacy-paths.$$; then
    echo "error: pgrust_expr publicly exports removed compatibility modules:" >&2
    sed 's/^/  /' /tmp/pgrust-legacy-paths.$$ >&2
    failed=1
fi

rm -f /tmp/pgrust-legacy-paths.$$
exit "$failed"
