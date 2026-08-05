# wasm/lib/scratch.sh — shared scratch-datadir convention (source, don't
# exec). Bash only (every scripts/*.sh is bash); set -u / set -e clean.
#
# Why: test/gate/bench scripts used to initdb throwaway clusters at ad-hoc
# /tmp paths and leave them behind whenever a run crashed or was killed
# (~0.3G per datadir; the recurring disk-full on dev boxes AND fleet pods —
# notes/disk-reclamation-2026-07-14.md). This lib gives every script:
#
#   * a common root       SCRATCH_ROOT (default /tmp/pgrust-scratch/$USER —
#                         /tmp is /private/tmp on macOS), so all transient
#                         datadirs live in ONE sweepable place
#   * trap teardown       EXIT/INT/TERM: kill any server still running on a
#                         registered dir (pkill -f on the dir path — every
#                         server has -D/-k args under it), then rm -rf.
#                         KEEP_DATADIR=1 skips the rm (debugging opt-out;
#                         servers are still killed).
#   * a start-time janitor  scratch_janitor sweeps SCRATCH_ROOT entries
#                         older than a day that no live process references —
#                         cheap self-healing for crashed runs whose traps
#                         never fired. Runs automatically on first
#                         scratch_datadir/scratch_adopt.
#   * a cache convention  SCRATCH_CACHE_ROOT for deliberately-persistent
#                         dirs (bench dataset datadirs, initdb templates):
#                         separate root, manifest.tsv, stale versions pruned
#                         on allocation, NEVER trap-deleted or janitor-swept.
#
# API (after `. "$(dirname "$0")/lib/scratch.sh"`):
#   scratch_datadir NAME    echo a fresh unique dir under SCRATCH_ROOT.
#                           Allocation only — command substitution runs in a
#                           subshell, so it cannot self-register; pair with
#                           scratch_adopt in the parent shell.
#   scratch_adopt PATH...   register PATH for teardown and install the
#                           traps. PATHs may be caller-provided dirs outside
#                           SCRATCH_ROOT (the convention is uniform: a
#                           script's work dir is disposable unless
#                           KEEP_DATADIR=1); unexpanded globs are allowed
#                           and expand at cleanup time.
#   scratch_on_exit CMD     eval CMD during teardown, before the rms (extra
#                           server stops, lock releases, ...). LIFO order.
#   scratch_cleanup         the teardown itself (idempotent). IMPORTANT: a
#                           script that sets its own `trap ... EXIT` AFTER
#                           adopting REPLACES this lib's EXIT trap — such
#                           scripts must chain it themselves:
#                           `trap 'my_stuff; scratch_cleanup' EXIT`.
#   scratch_cache_dir NAME VERKEY [KEEPGLOB]
#                           echo SCRATCH_CACHE_ROOT/NAME/VERKEY (created).
#                           Sibling versions of NAME are pruned unless they
#                           match KEEPGLOB (default: prune all others) or a
#                           live process references them; a row is appended
#                           to SCRATCH_CACHE_ROOT/manifest.tsv
#                           (epoch, name, verkey, path). Bump VERKEY when
#                           the cached content's definition changes.
#
# Env:
#   KEEP_DATADIR=1             keep all registered dirs (kills still happen)
#   PGRUST_SCRATCH_ROOT        override SCRATCH_ROOT
#   PGRUST_SCRATCH_CACHE_ROOT  override SCRATCH_CACHE_ROOT
#   SCRATCH_JANITOR_MINDAYS    janitor age threshold in days (default 1)

# Source guard: re-sourcing (e.g. via e2e-scratch-db.sh AND directly) must
# not clobber an earlier registration state.
[ -n "${__SCRATCH_LOADED:-}" ] && return 0
__SCRATCH_LOADED=1

# Keyed by the EFFECTIVE user (id -un), not $USER: fleet pods gosu from root
# to pg with root's env, and a $USER-keyed root would collide across the two
# uids (root-owned 755 dir -> pg's mktemp EACCES).
__SCRATCH_USER="$(id -un 2>/dev/null || echo "${USER:-pod}")"
SCRATCH_ROOT="${PGRUST_SCRATCH_ROOT:-/tmp/pgrust-scratch/$__SCRATCH_USER}"
SCRATCH_CACHE_ROOT="${PGRUST_SCRATCH_CACHE_ROOT:-/tmp/pgrust-scratch-cache/$__SCRATCH_USER}"

__SCRATCH_DIRS=()
__SCRATCH_HOOKS=()
__SCRATCH_TRAPPED=""
__SCRATCH_OWNER=""
__SCRATCH_SWEPT=""
__SCRATCH_DONE=""

# Refuse obviously-catastrophic rm targets (belt and braces: everything we
# register is a /tmp scratch path, but adopt accepts caller-provided dirs).
__scratch_sane_path() {
    case "$1" in
        ''|/|/tmp|/tmp/|/private/tmp|/private/tmp/|/var|/usr|/etc|/home|/Users|"$HOME"|"${HOME:-/nonexistent}"/) return 1 ;;
        /*) return 0 ;;
        *) return 1 ;;
    esac
}

# Kill any process whose command line references $1 (servers are started
# with -D/-k paths under their scratch dir). Best-effort, then a short wait
# so rm -rf doesn't race the dying postmaster's writes.
__scratch_kill_users() {
    local d="$1" i
    command -v pgrep >/dev/null 2>&1 || return 0
    pgrep -f -- "$d" >/dev/null 2>&1 || return 0
    pkill -TERM -f -- "$d" 2>/dev/null
    for i in 1 2 3 4 5 6 7 8 9 10; do
        pgrep -f -- "$d" >/dev/null 2>&1 || return 0
        sleep 0.2
    done
    pkill -9 -f -- "$d" 2>/dev/null
    sleep 0.2
    return 0
}

scratch_cleanup() {
    # Only the shell that registered runs the teardown (subshells inherit
    # variables but must not double-fire).
    [ "${BASHPID:-$$}" = "$__SCRATCH_OWNER" ] || return 0
    [ -n "$__SCRATCH_DONE" ] && return 0
    __SCRATCH_DONE=1
    local i d g
    # LIFO hooks first (server stops, lock releases).
    for (( i=${#__SCRATCH_HOOKS[@]}-1; i>=0; i-- )); do
        eval "${__SCRATCH_HOOKS[$i]}" || true
    done
    for g in ${__SCRATCH_DIRS[@]+"${__SCRATCH_DIRS[@]}"}; do
        # Globs expand at cleanup time (e.g. "$OUT"/seg*/dd).
        for d in $g; do
            [ -e "$d" ] || continue
            __scratch_sane_path "$d" || continue
            __scratch_kill_users "$d"
            if [ -n "${KEEP_DATADIR:-}" ]; then
                echo "scratch: KEEP_DATADIR=1 — keeping $d" >&2
            else
                rm -rf -- "$d" 2>/dev/null || true
            fi
        done
    done
    return 0
}

__scratch_sig() {
    scratch_cleanup
    trap - "$1"
    kill "-$1" "$$" 2>/dev/null
    exit 1
}

__scratch_install_trap() {
    [ -n "$__SCRATCH_TRAPPED" ] && return 0
    __SCRATCH_TRAPPED=1
    __SCRATCH_OWNER="${BASHPID:-$$}"
    trap scratch_cleanup EXIT
    trap '__scratch_sig INT' INT
    trap '__scratch_sig TERM' TERM
}

# Sweep SCRATCH_ROOT entries older than SCRATCH_JANITOR_MINDAYS (default 1
# day) that no live process references — self-healing for runs that died
# without running their traps. Runs at most once per script.
scratch_janitor() {
    [ -n "$__SCRATCH_SWEPT" ] && return 0
    __SCRATCH_SWEPT=1
    [ -d "$SCRATCH_ROOT" ] || return 0
    local d
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        __scratch_sane_path "$d" || continue
        if command -v pgrep >/dev/null 2>&1 && pgrep -f -- "$d" >/dev/null 2>&1; then
            continue    # still in use by a live (long) run
        fi
        rm -rf -- "$d" 2>/dev/null || true
    done < <(find "$SCRATCH_ROOT" -mindepth 1 -maxdepth 1 \
                  -mtime "+${SCRATCH_JANITOR_MINDAYS:-1}" -print 2>/dev/null)
    return 0
}

# The roots are per-user ($SCRATCH_ROOT = <shared>/$USER); the shared parent
# must be sticky-1777 (like /tmp) so root-phase and pg-phase pod runs can
# both create their own subdir. chmod fails harmlessly for non-owners.
__scratch_mkroot() {
    mkdir -p "$1" || return 2
    chmod 1777 "${1%/*}" 2>/dev/null
    return 0
}

scratch_datadir() {
    local name="${1:-scratch}" d
    __scratch_mkroot "$SCRATCH_ROOT" || return 2
    scratch_janitor
    d="$(mktemp -d "$SCRATCH_ROOT/${name}.XXXXXX")" || return 2
    # Root pods hand the dir to an unprivileged server user (gosu pg).
    chmod 755 "$d" 2>/dev/null
    echo "$d"
}

scratch_adopt() {
    local p
    __scratch_install_trap
    scratch_janitor
    for p in "$@"; do
        [ -n "$p" ] || continue
        __SCRATCH_DIRS+=("$p")
    done
    return 0
}

scratch_on_exit() {
    __scratch_install_trap
    __SCRATCH_HOOKS+=("$1")
    return 0
}

scratch_cache_dir() {
    local name="$1" ver="$2" keep="${3:-}" base d old
    base="$SCRATCH_CACHE_ROOT/$name"
    d="$base/$ver"
    __scratch_mkroot "$SCRATCH_CACHE_ROOT" || return 2
    mkdir -p "$base" || return 2
    for old in "$base"/*; do
        [ -e "$old" ] || continue
        [ "$old" = "$d" ] && continue
        # shellcheck disable=SC2254
        if [ -n "$keep" ]; then case "$(basename "$old")" in $keep) continue ;; esac; fi
        if command -v pgrep >/dev/null 2>&1 && pgrep -f -- "$old" >/dev/null 2>&1; then
            continue    # a live run still uses the old version
        fi
        rm -rf -- "$old" 2>/dev/null || true
    done
    mkdir -p "$d" || return 2
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$name" "$ver" "$d" \
        >> "$SCRATCH_CACHE_ROOT/manifest.tsv" 2>/dev/null
    echo "$d"
}
