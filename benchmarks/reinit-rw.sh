#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# reinit-rw.sh — rebuild the pgbench read-write datasets at a different scale.
#
# RUN THIS ON THE OLTP SERVER (see hosts.env: $OLTP_SERVER_HOST).
#
# WHY YOU MIGHT WANT THIS
#   The read-write datasets shipped resident on this box are scale 6849
#   (~100 GB per arm). Our published raw-instance read-write figure was
#   measured at scale 34247 (~500 GB). If you want to reproduce that exact
#   shape rather than the 100 GB one, you have to rebuild — and, because the
#   872 GB instance store is already ~800 GB full, you have to destroy
#   something first.
#
# WHAT IT COSTS
#   Scale 34247 is ~500 GB per arm. Two arms is ~1 TB, which does not fit at
#   all. So a 500 GB run means ONE arm resident at a time, and switching arms
#   means a full re-init — hours per switch. Plan for that before you start.
#
#   Rough initialisation times on this hardware:
#     scale  6849 (~100 GB) : ~30-60 min per arm
#     scale 20547 (~300 GB) : ~2-3 h per arm
#     scale 34247 (~500 GB) : ~3-5 h per arm
#
# WHAT IT DESTROYS
#   Whatever you tell it to. With --drop-ro it removes the two ~300 GB
#   read-only datadirs, which took several hours to prepare and are NOT backed
#   up. You would have to re-run the sysbench prepare to get them back.
#
# USAGE
#   ./reinit-rw.sh --scale 20547                  # both arms, if it fits
#   ./reinit-rw.sh --scale 34247 --arm pgrust --drop-ro
#   ./reinit-rw.sh --scale 34247 --arm cpg --drop-ro
#   ./reinit-rw.sh --dry-run --scale 34247        # just report the arithmetic
# ---------------------------------------------------------------------------
set -uo pipefail

SCALE=6849
ARMS="pgrust cpg"
DROP_RO=0
DRY=0
DATA=/data

while [ $# -gt 0 ]; do
  case "$1" in
    --scale)   shift; SCALE="$1" ;;
    --arm)     shift; ARMS="$1" ;;
    --drop-ro) DROP_RO=1 ;;
    --dry-run) DRY=1 ;;
    -h|--help) sed -n '2,36p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# pgbench's accounts table is ~100,000 rows per unit of scale, and the whole
# database lands near 15 MB per unit of scale once indexes are built.
EST_GB_PER_ARM=$(( SCALE * 15 / 1000 ))
NARMS=$(echo "$ARMS" | wc -w)
EST_TOTAL=$(( EST_GB_PER_ARM * NARMS ))

AVAIL_GB=$(df -BG --output=avail "$DATA" | tail -1 | tr -dc '0-9')
RO_GB=$(sudo du -sBG "$DATA"/ro-pgrust "$DATA"/ro-cpg 2>/dev/null | awk '{s+=$1} END{print s+0}')
RW_GB=$(sudo du -sBG "$DATA"/rw-pgrust "$DATA"/rw-cpg 2>/dev/null | awk '{s+=$1} END{print s+0}')

echo "=============================================================================="
echo "pgbench read-write re-initialisation"
echo "=============================================================================="
echo "  requested scale     : $SCALE  (~${EST_GB_PER_ARM} GB per arm)"
echo "  arms                : $ARMS  (${NARMS})"
echo "  estimated new total : ~${EST_TOTAL} GB"
echo
echo "  currently on $DATA:"
echo "    read-only datadirs : ~${RO_GB} GB"
echo "    read-write datadirs: ~${RW_GB} GB"
echo "    free               : ~${AVAIL_GB} GB"
echo "  --drop-ro            : $([ $DROP_RO = 1 ] && echo YES && echo "" || echo no)"
echo

RECLAIM=$(( RW_GB + (DROP_RO == 1 ? RO_GB : 0) ))
AFTER=$(( AVAIL_GB + RECLAIM - EST_TOTAL ))
echo "  after reclaiming ~${RECLAIM} GB and writing ~${EST_TOTAL} GB:"
echo "    projected free     : ~${AFTER} GB"
# The rig ships 98% full (19 GiB free). pgbench at c=256 generates WAL faster
# than checkpoints recycle it, and a full filesystem does not fail cleanly --
# it takes the server down mid-run. Keep a real margin.
if [ "$AFTER" -lt 40 ]; then
  echo
  echo "  !! THIS DOES NOT FIT with a safe margin."
  echo "  !! Leave at least ~40 GB free: pgbench at c=256 generates WAL faster"
  echo "  !! than checkpoints recycle it, and a full filesystem mid-run does not"
  echo "  !! fail cleanly — it takes the server down and corrupts the leg."
  [ $DRY = 0 ] && { echo "  !! Refusing. Re-run with --dry-run to explore, or drop more data."; exit 3; }
fi
echo

if [ $DRY = 1 ]; then echo "  Dry run. Nothing changed."; exit 0; fi

read -r -p "Type REINIT to proceed: " ans
[ "$ans" = "REINIT" ] || { echo "Aborted."; exit 1; }

sudo /opt/audit/switch-arm.sh stop

if [ $DROP_RO = 1 ]; then
  echo "  removing read-only datadirs (irreversible) ..."
  sudo rm -rf "$DATA"/ro-pgrust "$DATA"/ro-cpg
fi

PGUSER_OS=$(awk -F= '/^PGUSER_?=/{print $2}' /opt/audit/switch-arm.sh 2>/dev/null | tr -d '"' | head -1)
PGUSER_OS="${PGUSER_OS:-postgres}"

initdb_arm() {
  # Re-create a datadir exactly the way the environment was originally built:
  # the same initdb options on both engines, and the same single shared
  # config file included by both, so the two arms cannot drift apart.
  local arm="$1" dd="$2" bindir
  case "$arm" in
    rw-pgrust) bindir=/opt/pgrust/bin ;;
    rw-cpg)    bindir=/usr/pgsql-18/bin ;;
    *) echo "  !! unknown arm $arm"; return 1 ;;
  esac
  sudo install -d -o "$PGUSER_OS" -g "$PGUSER_OS" -m 700 "$dd"
  sudo runuser -u "$PGUSER_OS" -- "$bindir/initdb" -D "$dd" --no-locale --encoding=UTF8 >/dev/null \
    || return 1
  # Include the one shared config rather than copying settings per arm.
  echo "include = '/opt/audit/audit.conf'" | sudo tee -a "$dd/postgresql.conf" >/dev/null
}

for arm in $ARMS; do
  dd="$DATA/rw-$arm"
  echo "------------------------------------------------------------------------------"
  echo "  re-initialising $dd at scale $SCALE"
  echo "------------------------------------------------------------------------------"
  sudo rm -rf "$dd"
  initdb_arm "rw-$arm" "$dd"                  || { echo "  !! initdb failed"; exit 4; }
  sudo /opt/audit/switch-arm.sh "rw-$arm"     || { echo "  !! start failed"; exit 4; }
  psql -h /tmp -p 5432 -U postgres -d postgres -c 'CREATE DATABASE bench' >/dev/null 2>&1

  t0=$(date +%s)
  pgbench -h /tmp -p 5432 -U postgres -i -s "$SCALE" bench 2>&1 | tail -5
  rc=$?
  t1=$(date +%s)
  echo "  init wall time: $(( t1 - t0 )) s"
  [ $rc -ne 0 ] && { echo "  !! pgbench -i failed"; exit 5; }

  psql -h /tmp -p 5432 -U postgres -d bench -c 'VACUUM ANALYZE' >/dev/null
  echo "  size: $(sudo du -sh "$dd" | cut -f1)   free on $DATA: $(df -h --output=avail "$DATA" | tail -1)"
  sudo /opt/audit/switch-arm.sh stop
done

echo
echo "  Done. Record the new scale before you quote any number from it —"
echo "  read-write results are not comparable across scales, and the figure in"
echo "  the README was measured at a different one."
echo "=============================================================================="
