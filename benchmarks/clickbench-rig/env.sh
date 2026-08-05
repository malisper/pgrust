# Auditor ClickBench rig environment (GL-AUDITKIT-1).
# Replicates fleet job pgrust-cb-standard-1785123604-1563 (tsv/parload cut).

export CB_ROOT=/data/clickbench
export CB_RIG=$CB_ROOT/rig
export CB_SOCK=$CB_ROOT/sock

# --- the datadir of record -------------------------------------------------
# ONE name is authoritative: PGDATA_CB. run-clickbench.sh reads it, hosts.env
# sets it, and this file derives the rig's CB_DD from it.
#
# This used to be two independent variables -- the rig booted $CB_DD while the
# runner reported on $PGDATA_CB -- so setting either one alone gave you a run
# whose header described a different datadir from the one being measured, with
# nothing complaining. If you set both and they disagree, this file now refuses
# rather than silently picking one.
if [ -n "${PGDATA_CB:-}" ] && [ -n "${CB_DD:-}" ] && [ "$PGDATA_CB" != "$CB_DD" ]; then
  echo "rig env: FATAL PGDATA_CB and CB_DD disagree:" >&2
  echo "  PGDATA_CB=$PGDATA_CB" >&2
  echo "  CB_DD=$CB_DD" >&2
  echo "  Set PGDATA_CB only; CB_DD is derived from it." >&2
  return 1 2>/dev/null || exit 1
fi

# Default: the AS-DELIVERED datadir on the gp3 root volume.
#
# That volume is OFF-REFERENCE. ClickBench's published c8g.4xlarge rows use
# 500 GiB gp2, which delivers roughly twice the sequential bandwidth; measuring
# here costs ~12 points of combined score, all of it on the cold and load axes.
#
# To opt in to reference storage, do NOT hand-edit this line -- run
#   ./use-reference-storage.sh
# which verifies the reference bank's identity before switching and can switch
# back with --revert.
export PGDATA_CB="${PGDATA_CB:-/data/clickbench/pgdata}"
export CB_DD="$PGDATA_CB"

export CB_BIN="${CB_BIN_OVERRIDE:-/opt/pgrust/bin/postgres}"
export PG_BINDIR=/usr/bin
export PSQL=/usr/bin/psql
# PGDG-equivalent share dir on AL2023 (postgresql18-server rpm). The RPM build
# uses --with-system-tzdata, so the tz db is /usr/share/zoneinfo.
export PGSHARE=/usr/share/pgsql
export PGTZDIR=/usr/share/zoneinfo
export HITS=${HITS:-/data/clickbench/hits.tsv}
# Query-phase server env: the official cut ran DISARMED (CBSTD_SERVER_ENV empty).
export CB_SERVER_ENV=""
