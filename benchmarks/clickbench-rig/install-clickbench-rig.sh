#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# install-clickbench-rig.sh — rebuild /data/clickbench/rig on the ClickBench box.
#
# RUN THIS ON THE CLICKBENCH BOX, from a checkout of this branch.
#
# WHY THIS EXISTS
#   The ClickBench box drives its server through a small rig of scripts, because
#   the datadir is owned by a service account (`pgrust`) and PostgreSQL refuses
#   to run against a datadir it does not own. Like the oltp-server plumbing,
#   this rig was created in place and so was not auditable from the repository
#   or reproducible after a rebuild. Now it is generated from this directory.
#
# WHAT IT INSTALLS  (into /data/clickbench/rig, owned by the service account)
#   env.sh       paths + CB_BIN, honouring CB_BIN_OVERRIDE so
#                `run-clickbench.sh --binary` can boot a binary you built
#   start/stop/check   server lifecycle, used by the sweep's cold cycle
#   query        runs one query and prints its wall time on stderr
#   data-size    the scored data-size axis (du -bcs over PGDATA)
#   load/run-load      the documented load path
#   create.sql   the cbstore columnar DDL used for the loaded bank
#   queries.sql  symlinked to ../queries.sql — the 43 official queries, so
#                there is exactly ONE copy of them in this kit
#
# WHAT IT DOES NOT DO
#   It does not initdb and it does not load the 100M-row dataset. That is
#   ~18 minutes of load plus a 74.8 GB download, and the recipe is recorded in
#   /opt/pgrust/LOADINFO.txt on the box.
#
# USAGE
#   sudo ./install-clickbench-rig.sh
#   sudo ./install-clickbench-rig.sh --check     # diff tree vs box
# ---------------------------------------------------------------------------
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST=/data/clickbench/rig
SVC="${PGSVC:-pgrust}"

FILES="env.sh start stop check query data-size load run-load create.sql install"

case "${1:-}" in
  -h|--help) sed -n '2,36p' "$0"; exit 0 ;;
  --check)
    echo "Comparing the tree against $DEST (nothing will be changed):"
    rc=0
    for f in $FILES; do
      if sudo diff -q "$HERE/$f" "$DEST/$f" >/dev/null 2>&1; then
        echo "  OK        $f  (byte-identical)"
      else
        echo "  DIFFERS   $f"
        sudo diff -u "$DEST/$f" "$HERE/$f" 2>/dev/null | sed 's/^/            /' | head -20
        rc=1
      fi
    done
    echo
    [ $rc = 0 ] && echo "In sync." || echo "Out of sync — reconcile before trusting either copy."
    exit $rc ;;
esac

[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
id "$SVC" >/dev/null 2>&1 || { echo "service account '$SVC' does not exist" >&2; exit 1; }

install -d -o "$SVC" -g "$SVC" -m 755 "$DEST"
for f in $FILES; do
  case "$f" in *.sql) m=644 ;; *) m=755 ;; esac
  install -o "$SVC" -g "$SVC" -m "$m" "$HERE/$f" "$DEST/$f"
done
# One copy of the 43 official queries in the whole kit, not two that can drift.
ln -sf ../../queries.sql "$DEST/queries.sql" 2>/dev/null \
  || install -o "$SVC" -g "$SVC" -m 644 "$HERE/../queries.sql" "$DEST/queries.sql"

echo "installed:"
ls -la "$DEST" | sed 's/^/  /'
echo
echo "then:  sudo -u $SVC $DEST/start && sudo -u $SVC $DEST/check && echo ready"
