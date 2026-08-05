#!/bin/bash
# ===========================================================================
# GL-AUDITKIT-1 arm switch.
#
#   /opt/audit/switch-arm.sh {ro-pgrust|ro-cpg|rw-pgrust|rw-cpg|stop|status}
#
# Exactly ONE server may run at a time: all four datadirs bind port 5432 and
# the box is 4 vCPU / 32 GB with shared_buffers=16GB.
#
# The running instance is identified by its DATADIR (the postmaster.pid in
# each /data/<arm>), never by the program name -- both engines present as
# "postgres" in ps output, so a name-based guard would be ambiguous.
#
# Binaries:
#   ro-pgrust, rw-pgrust -> /opt/pgrust/bin/postgres   (release-candidate pgrust)
#   ro-cpg,    rw-cpg    -> /usr/pgsql-18/bin/postgres (C PostgreSQL 18.3, PGDG)
#
# Both arms include the identical /opt/audit/audit.conf.
# ===========================================================================
set -u

ARMS="ro-pgrust ro-cpg rw-pgrust rw-cpg"
DATAROOT=/data
LOGDIR=/data/logs
PGUSER_=postgres
PORT=5432

# max_stack_depth=60000 (kB) requires the process stack rlimit to exceed it.
STACK_KB=204800
NOFILE=1048576          # rig law: pgrust is one process, one thread per connection

die() { echo "switch-arm: $*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || exec sudo -n "$0" "$@"

# max_stack_depth=60000kB needs an rlimit above it.  Raise the HARD limit here,
# while we are still root (CAP_SYS_RESOURCE), so the unprivileged postgres
# process can inherit it.  /etc/security/limits.d/99-audit-stack.conf makes the
# same limit available to interactive logins.
ulimit -H -s $STACK_KB 2>/dev/null || true
ulimit -S -s $STACK_KB 2>/dev/null || true

# BIN_OVERRIDE (2nd CLI arg) lets the auditor boot the pgrust arm from a
# binary they built themselves (see deploy-binary.sh). It applies to the
# pgrust arms ONLY: the C arm must stay the same stock PostgreSQL 18.3 in
# both modes, so a shipped-vs-your-build comparison moves exactly one side.
BIN_OVERRIDE="${BIN_OVERRIDE:-}"
bin_for() {
  case "$1" in
    ro-pgrust|rw-pgrust)
      if [ -n "$BIN_OVERRIDE" ]; then
        [ -x "$BIN_OVERRIDE" ] || { echo "switch-arm: no such binary: $BIN_OVERRIDE" >&2; return 1; }
        echo "$BIN_OVERRIDE"
      else echo /opt/pgrust/bin/postgres; fi ;;
    ro-cpg|rw-cpg)
      [ -n "$BIN_OVERRIDE" ] && echo "switch-arm: ignoring binary override for the C arm (by design)" >&2
      echo /usr/pgsql-18/bin/postgres ;;
    *) return 1 ;;
  esac
}

# Print "<arm> <pid>" for every datadir that has a live postmaster.
running_arms() {
  local a pid
  for a in $ARMS; do
    pid=$(head -1 "$DATAROOT/$a/postmaster.pid" 2>/dev/null) || continue
    [ -n "${pid:-}" ] || continue
    case "$pid" in ''|*[!0-9]*) continue ;; esac
    if kill -0 "$pid" 2>/dev/null; then echo "$a $pid"; fi
  done
}

stop_arm() {
  local arm="$1" pid="$2" i
  echo "switch-arm: stopping $arm (pid $pid) -- fast shutdown"
  kill -INT "$pid" 2>/dev/null
  for i in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || { echo "switch-arm: $arm stopped after ${i}s"; return 0; }
    sleep 1
  done
  echo "switch-arm: $arm did not stop in 120s -- immediate shutdown (SIGQUIT)"
  kill -QUIT "$pid" 2>/dev/null
  for i in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "switch-arm: $arm stopped (immediate)"; return 0; }
    sleep 1
  done
  echo "switch-arm: $arm still alive -- SIGKILL"
  kill -KILL "$pid" 2>/dev/null; sleep 3
  kill -0 "$pid" 2>/dev/null && die "could not kill $arm pid $pid"
  return 0
}

stop_all() {
  local line arm pid stopped=0
  while read -r arm pid; do
    [ -n "${arm:-}" ] || continue
    stop_arm "$arm" "$pid"; stopped=1
  done < <(running_arms)
  # stale pidfile sweep: a datadir whose postmaster is gone
  for arm in $ARMS; do
    if [ -f "$DATAROOT/$arm/postmaster.pid" ]; then
      pid=$(head -1 "$DATAROOT/$arm/postmaster.pid" 2>/dev/null)
      case "${pid:-x}" in ''|*[!0-9]*) rm -f "$DATAROOT/$arm/postmaster.pid" ;; esac
      kill -0 "${pid:-0}" 2>/dev/null || { echo "switch-arm: removing stale pidfile for $arm"; rm -f "$DATAROOT/$arm/postmaster.pid"; }
    fi
  done
  [ "$stopped" = 1 ] || echo "switch-arm: nothing was running"
  # port must now be free
  local i
  for i in $(seq 1 30); do
    ss -ltn "sport = :$PORT" 2>/dev/null | grep -q ":$PORT" || return 0
    sleep 1
  done
  echo "switch-arm: WARNING port $PORT still bound after stop:" >&2
  ss -ltnp "sport = :$PORT" >&2
  return 0
}

status() {
  local any=0 arm pid
  while read -r arm pid; do
    [ -n "${arm:-}" ] || continue
    echo "RUNNING  $arm  pid=$pid  bin=$(readlink -f /proc/$pid/exe)"
    any=1
  done < <(running_arms)
  [ "$any" = 1 ] || echo "RUNNING  (none)"
  ss -ltn "sport = :$PORT" 2>/dev/null | tail -n +2
}

start_arm() {
  local arm="$1" bin dd log i
  bin=$(bin_for "$arm") || die "unknown arm '$arm'"
  dd="$DATAROOT/$arm"
  [ -d "$dd" ]              || die "no such datadir $dd"
  [ -f "$dd/PG_VERSION" ]   || die "$dd is not an initialised datadir"
  [ -x "$bin" ]             || die "missing binary $bin"

  mkdir -p "$LOGDIR"; chown $PGUSER_:$PGUSER_ "$LOGDIR"
  log="$LOGDIR/$arm.log"

  echo "switch-arm: starting $arm"
  echo "  datadir : $dd"
  echo "  binary  : $bin"
  echo "  log     : $log"
  {
    echo
    echo "=========================================================="
    echo "switch-arm start $(date -Is)  arm=$arm  bin=$bin  datadir=$dd"
    echo "=========================================================="
  } >> "$log"
  chown $PGUSER_:$PGUSER_ "$log"

  # </dev/null and full fd detach: otherwise the postmaster inherits the
  # caller's stdin/stdout and an ssh session that started an arm never returns.
  setsid runuser -u $PGUSER_ -- /bin/bash -c \
    "ulimit -s $STACK_KB 2>/dev/null; ulimit -n $NOFILE 2>/dev/null; exec '$bin' -D '$dd' >> '$log' 2>&1" \
    < /dev/null >> "$log" 2>&1 &
  disown 2>/dev/null || true

  for i in $(seq 1 180); do
    if /usr/pgsql-18/bin/pg_isready -h 127.0.0.1 -p $PORT -U postgres -q; then
      echo "switch-arm: $arm accepting connections after ${i}s"
      status
      return 0
    fi
    sleep 1
  done
  echo "switch-arm: $arm FAILED to accept connections in 180s; last log lines:" >&2
  tail -40 "$log" >&2
  return 1
}

case "${1:-}" in
  stop)    stop_all ;;
  status)  status ;;
  ro-pgrust|ro-cpg|rw-pgrust|rw-cpg)
           BIN_OVERRIDE="${2:-$BIN_OVERRIDE}"; export BIN_OVERRIDE
           stop_all; start_arm "$1" ;;
  *) echo "usage: $0 {ro-pgrust|ro-cpg|rw-pgrust|rw-cpg} [BINARY]" >&2
     echo "       $0 {stop|status}" >&2
     echo "  BINARY overrides the executable for the pgrust arms only." >&2
     exit 2 ;;
esac
