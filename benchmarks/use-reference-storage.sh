#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# use-reference-storage.sh — move the ClickBench datadir onto REFERENCE storage.
#
# RUN THIS ON THE CLICKBENCH BOX. Nothing about your environment changes until
# you run it: as delivered, the datadir sits on off-reference storage.
#
# WHY YOU WANT THIS
#   ClickBench's published `c8g.4xlarge` rows — the ones our numbers are
#   compared against — were produced on **500 GiB gp2** EBS. That is the
#   reference storage for this machine class.
#
#   This box boots from a 500 GB **gp3** volume at default provisioned
#   throughput (125 MB/s). That is *off-reference*: it delivers roughly half
#   the sequential read bandwidth of gp2-500 on this instance type.
#
#   ClickBench scores two I/O-bound axes (cold query time at 20% of the
#   combined metric, load time at 10%). Measured on this box, the difference is:
#
#       Sigma43 cold   218.460 s on gp3    ->  103.450 s on gp2   (2.11x)
#       Sigma43 hot     11.930 s on gp3    ->   11.900 s on gp2   (unchanged)
#       combined         1.0369  on gp3    ->    0.9290  on gp2
#
#   ~12 points of combined score, entirely on the disk-bound axes, with the
#   RAM-resident axis unmoved. **If you benchmark on the as-delivered gp3
#   volume, your numbers will not match our published figures, and the gap is
#   storage, not the engine.**
#
#   Adjudicated by GL-STORARM-1 (branch night/storarm @ 7ea09ba52d), which
#   witnessed the fleet/official arm as gp2-500 and identified this box's
#   gp3-500 as the outlier.
#
# TWO PATHS
#   (A) ADOPT the volume we already attached — $CB_REF_VOLUME in hosts.env, a
#       500 GiB gp2 volume carrying an already-loaded, identity-checked copy of
#       the bank. Fast: no load, no download. This is the default.
#   (B) CLEAN ROOM — provision your own gp2-500, load it yourself from the
#       upstream source, and trust nothing we prepared. Slower (a ~16 GB
#       download and roughly a 10-20 minute load) and strictly better evidence.
#       Use --clean-room to print that procedure.
#
# WHAT IT CHANGES
#   Only which datadir the rig points at, by writing PGDATA_CB into
#   /data/clickbench/rig/env.sh. It does not move, copy, delete or modify any
#   dataset. The as-delivered gp3 datadir stays exactly where it is, so you can
#   switch back with --revert and compare the two directly.
#
# USAGE
#   ./use-reference-storage.sh --check        # report state, change nothing
#   ./use-reference-storage.sh                # adopt the reference volume
#   ./use-reference-storage.sh --revert       # back to the as-delivered gp3 datadir
#   ./use-reference-storage.sh --clean-room   # print the provision-and-load-it-yourself path
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/hosts.env" ] && . "$HERE/hosts.env" 2>/dev/null

REF_VOL="${CB_REF_VOLUME:-<set CB_REF_VOLUME in hosts.env>}"
REF_MOUNT="${REF_MOUNT:-/data2}"
REF_DD="$REF_MOUNT/cb-gp2/pgdata"
ASDELIVERED_DD=/data/clickbench/pgdata
RIG=/data/clickbench/rig
ENVFILE="$RIG/env.sh"
SOCK=/data/clickbench/sock
SVC="${PGSVC:-pgrust}"
BIN="${PGRUST_BIN:-/opt/pgrust/bin/postgres}"

# Bank identity of record. Both numbers come from the official cut and are
# reproduced exactly by the volume we attached; a mismatch means the dataset is
# not the one the published figures describe, and this script will refuse.
WANT_ROWS=99997497
WANT_TABLE_BYTES=17497735168
WANT_Q2=630500

MODE=adopt
case "${1:-}" in
  --check)      MODE=check ;;
  --revert)     MODE=revert ;;
  --clean-room) MODE=cleanroom ;;
  -h|--help)    sed -n '2,56p' "$0"; exit 0 ;;
  "")           MODE=adopt ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

say(){ printf '%s\n' "$*"; }
die(){ say ""; say "!! $*"; exit 1; }

# --- describe the storage under a path -------------------------------------
vol_spec() {  # <path>  -> prints a one-line spec, best effort without AWS
  local p="$1" dev
  dev=$(df --output=source "$p" 2>/dev/null | tail -1)
  local base; base=$(basename "$dev")
  local model rot size
  size=$(lsblk -bdno SIZE "/dev/$base" 2>/dev/null | awk '{printf "%.0f GiB", $1/1073741824}')
  printf '%s (%s)' "$dev" "${size:-size unknown}"
}

measure_bw() {  # <path> -> MB/s from a direct-IO read of a big file under it
  local p="$1" f
  f=$(sudo find "$p" -type f -size +200M 2>/dev/null | head -1)
  [ -n "$f" ] || { echo "n/a (no file >200MB to probe)"; return; }
  sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null
  sudo dd if="$f" of=/dev/null bs=1M count=1000 iflag=direct 2>&1 \
    | awk '/copied/{print $(NF-1), $NF}'
}

current_dd() { sudo -u "$SVC" bash -c ". $ENVFILE >/dev/null 2>&1; echo \$CB_DD" 2>/dev/null; }

# --- clean-room path -------------------------------------------------------
if [ "$MODE" = cleanroom ]; then
cat <<EOF
==============================================================================
CLEAN-ROOM PATH — provision and load reference storage yourself
==============================================================================
Trust nothing we prepared. You will need AWS credentials for your own account
or ours; these instances deliberately have none, so run the AWS steps from your
own machine.

1. Create a 500 GiB gp2 volume in this instance's availability zone and attach
   it. 500 GiB gp2 is the reference spec — do not substitute gp3 with tuned
   throughput, because the point is to match the published rows' storage class.

     aws ec2 create-volume --availability-zone <az> --size 500 --volume-type gp2
     aws ec2 attach-volume --volume-id <vol> --instance-id ${CB_INSTANCE_ID:-<instance>} --device /dev/sdg

2. On this box, make a filesystem and mount it:

     sudo mkfs.xfs /dev/nvme2n1          # confirm the device name with lsblk
     sudo mkdir -p /data3 && sudo mount /dev/nvme2n1 /data3
     sudo install -d -o $SVC -g $SVC -m 750 /data3/cb/pgdata

3. initdb and load, using the recipe recorded in /opt/pgrust/LOADINFO.txt.
   The source is public:

     https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz

   Download it TO THE NEW VOLUME, not to /, or the load re-reads through the
   off-reference disk you are trying to avoid. Expect ~16 GB compressed and
   ~75 GB decompressed.

4. Point the rig at your datadir and verify identity:

     sudo sed -i 's|^export PGDATA_CB=.*|export PGDATA_CB="\\\${PGDATA_CB:-/data3/cb/pgdata}"|' $ENVFILE
     ./use-reference-storage.sh --check

   The identity check must report rows=$WANT_ROWS and
   table_bytes=$WANT_TABLE_BYTES. If your own load produces different numbers,
   that is a finding and we would like to hear it.

5. Time your load and put the number in the result JSON, since load time is a
   scored axis. run-clickbench.sh reads it from
   /opt/pgrust/LOADINFO.txt (key: load_time_seconds=).
==============================================================================
EOF
exit 0
fi

# --- preconditions ---------------------------------------------------------
say "=============================================================================="
say "ClickBench reference storage   [mode: $MODE]"
say "=============================================================================="
[ -f "$ENVFILE" ] || die "no rig at $ENVFILE — is this the clickbench box?"

CUR=$(current_dd)
say "  rig currently points at : ${CUR:-<unresolved>}"
say "  as-delivered datadir    : $ASDELIVERED_DD   $( [ -d "$ASDELIVERED_DD" ] && echo "(present)" || echo "(MISSING)")"
say "  reference datadir       : $REF_DD           $( [ -d "$REF_DD" ] && echo "(present)" || echo "(not mounted)")"
say ""

# --- mount the reference volume if needed ----------------------------------
if [ "$MODE" != revert ] && [ ! -d "$REF_DD" ]; then
  say "  reference datadir not visible — looking for the volume"
  # Find an unmounted ~500 GiB disk that is not the root disk.
  cand=$(lsblk -bdno NAME,SIZE,MOUNTPOINT | awk '$3=="" && $2>4.0e11 {print $1; exit}')
  [ -n "$cand" ] || die "no unmounted ~500 GiB device found. Attach $REF_VOL first, or use --clean-room."
  say "  candidate device: /dev/$cand"
  fstype=$(lsblk -no FSTYPE "/dev/$cand" | head -1)
  [ -n "$fstype" ] || die "/dev/$cand has no filesystem. This script will NOT format a device it did not create — if this is a blank volume, use --clean-room."
  sudo mkdir -p "$REF_MOUNT"
  sudo mount "/dev/$cand" "$REF_MOUNT" || die "could not mount /dev/$cand at $REF_MOUNT"
  say "  mounted /dev/$cand at $REF_MOUNT"
  [ -d "$REF_DD" ] || die "$REF_MOUNT mounted but $REF_DD is not there — wrong volume?"
fi

# --- identity verification -------------------------------------------------
verify_identity() {  # <datadir> -> 0 if the bank is the one of record
  local dd="$1" s=/tmp/urs-sock-$$ rows tbl q2 pid rc=0
  mkdir -p "$s"; sudo chown "$SVC:$SVC" "$s"
  sudo chown -R "$SVC:$SVC" "$(dirname "$dd")" 2>/dev/null
  sudo -u "$SVC" env RUST_MIN_STACK=33554432 \
      PGRUST_TZDIR=/usr/share/zoneinfo PGRUST_PGSHAREDIR=/usr/share/pgsql \
      "$BIN" -D "$dd" -k "$s" -p 5599 -c listen_addresses='' >/tmp/urs-$$.log 2>&1 &
  local i
  for i in $(seq 1 180); do
    psql -h "$s" -p 5599 -U postgres -d test -tAc 'SELECT 1' >/dev/null 2>&1 && break; sleep 1
  done
  if ! psql -h "$s" -p 5599 -U postgres -d test -tAc 'SELECT 1' >/dev/null 2>&1; then
    say "    server would not start against $dd; last lines:"
    tail -8 /tmp/urs-$$.log | sed 's/^/      /'
    rc=1
  else
    rows=$(psql -h "$s" -p 5599 -U postgres -d test -tAc 'SELECT count(*) FROM hits' 2>/dev/null)
    tbl=$(psql -h "$s" -p 5599 -U postgres -d test -tAc "SELECT pg_total_relation_size('hits')" 2>/dev/null)
    q2=$(psql -h "$s" -p 5599 -U postgres -d test -tAc 'SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0' 2>/dev/null)
    printf '    %-26s %s  (want %s)  %s\n' "rows"        "${rows:-?}" "$WANT_ROWS"        "$([ "${rows:-}" = "$WANT_ROWS" ] && echo OK || { echo MISMATCH; rc=1; })"
    printf '    %-26s %s  (want %s)  %s\n' "table bytes" "${tbl:-?}"  "$WANT_TABLE_BYTES" "$([ "${tbl:-}"  = "$WANT_TABLE_BYTES" ] && echo OK || { echo MISMATCH; rc=1; })"
    printf '    %-26s %s  (want %s)  %s\n' "smoke AdvEngineID<>0" "${q2:-?}" "$WANT_Q2" "$([ "${q2:-}" = "$WANT_Q2" ] && echo OK || { echo MISMATCH; rc=1; })"
    pid=$(sudo head -1 "$dd/postmaster.pid" 2>/dev/null)
    [ -n "$pid" ] && sudo kill -TERM "$pid" 2>/dev/null
    for i in $(seq 1 90); do sudo kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  rm -rf "$s" /tmp/urs-$$.log
  return $rc
}

# --- check mode ------------------------------------------------------------
if [ "$MODE" = check ]; then
  say "STORAGE"
  say "  as-delivered  $(vol_spec /)          seq read: $(measure_bw "$ASDELIVERED_DD")"
  if [ -d "$REF_DD" ]; then
    say "  reference     $(vol_spec "$REF_MOUNT")      seq read: $(measure_bw "$REF_DD")"
  else
    say "  reference     not mounted"
  fi
  say ""
  say "VERDICT"
  if [ "$CUR" = "$REF_DD" ]; then
    say "  ON REFERENCE STORAGE. Your ClickBench numbers are comparable to the"
    say "  published c8g.4xlarge rows."
  elif [ "$CUR" = "$ASDELIVERED_DD" ]; then
    say "  ON OFF-REFERENCE STORAGE (gp3-500, the as-delivered state)."
    say "  Expect roughly 12 points of combined score worse than our published"
    say "  figures, concentrated in the cold axis. Run this script with no"
    say "  arguments to move to reference storage."
  else
    say "  UNRECOGNISED datadir: ${CUR:-<unresolved>}"
    say "  It is neither the as-delivered nor the reference datadir. Whatever it"
    say "  is, this script did not put it there, and no claim in the README"
    say "  describes it."
  fi
  say ""
  if [ -d "$REF_DD" ]; then
    say "IDENTITY of the reference bank"
    verify_identity "$REF_DD" && say "    identity OK" || say "    identity FAILED — do not benchmark this dataset"
  fi
  exit 0
fi

# --- revert ----------------------------------------------------------------
if [ "$MODE" = revert ]; then
  [ -d "$ASDELIVERED_DD" ] || die "the as-delivered datadir $ASDELIVERED_DD is gone; cannot revert"
  if [ "$CUR" = "$ASDELIVERED_DD" ]; then say "  already on the as-delivered datadir; nothing to do"; exit 0; fi
  sudo sed -i "s|^export PGDATA_CB=.*|export PGDATA_CB=\"\${PGDATA_CB:-$ASDELIVERED_DD}\"|" "$ENVFILE"
  say "  reverted: rig now points at $(current_dd)"
  say "  (the reference volume stays mounted and untouched)"
  exit 0
fi

# --- adopt -----------------------------------------------------------------
if [ "$CUR" = "$REF_DD" ]; then
  say "  already on reference storage — verifying identity anyway (idempotent)"
else
  say "  verifying the reference bank BEFORE switching to it"
fi
say ""
verify_identity "$REF_DD" || die "identity check FAILED. Refusing to point the rig at a dataset that is not the one of record — a benchmark run against it would not be comparable to anything we published."
say "    identity OK"
say ""

sudo -u "$SVC" bash -c "test -w '$ENVFILE'" 2>/dev/null || true
sudo sed -i "s|^export PGDATA_CB=.*|export PGDATA_CB=\"\${PGDATA_CB:-$REF_DD}\"|" "$ENVFILE"
NEW=$(current_dd)
[ "$NEW" = "$REF_DD" ] || die "tried to set PGDATA_CB but the rig still resolves to '$NEW'. Not proceeding with a half-applied change."

say "  rig now points at: $NEW"
say ""
say "STORAGE, before and after"
say "  before (as-delivered)  $(vol_spec /)"
say "  after  (reference)     $(vol_spec "$REF_MOUNT")"
say ""
say "  Volume specs of record:"
say "    as-delivered  gp3  500 GB, 3000 IOPS, 125 MB/s provisioned   (OS + off-reference datadir)"
say "    reference     gp2  500 GiB, 1500 IOPS                        ($REF_VOL, datadir of record)"
say ""
say "  NOTE: the reference volume has DeleteOnTermination=false. It outlives the"
say "  instance, and it keeps billing (~\$50/mo) after everything else is torn"
say "  down. teardown.sh deletes it explicitly."
say ""
say "NEXT"
say "  ./run-clickbench.sh --smoke      # 2 queries, confirms the path"
say "  ./run-clickbench.sh              # full 43-query sweep, ~6-8 min"
say ""
say "  ONE AXIS IS STILL OFF-REFERENCE: the load."
say ""
say "  The only load time ever measured on this box ran on the gp3 volume:"
say "  1089.902 s. run-clickbench.sh reuses it (load is not re-measured per"
say "  sweep), so even after switching to reference storage your combined score"
say "  carries one off-reference term. Expect roughly 0.9290; the same sweep"
say "  scored with the official cut 593.005 s load would give 0.8741."
say ""
say "  We have NOT measured a load on reference storage. An attempt was made and"
say "  was destroyed by our own concurrent cleanup before it finished, so there"
say "  is no number and we are not going to quote one. For context only, from"
say "  the part that did complete before it died: the source download ran at"
say "  99.8 MB/s (163 s), and the COPY finished 475-546 s into the timed window"
say "  against 1000 s of equivalent phases on gp3 -- which suggests the load"
say "  lands somewhere near the official cut 593 s. That is an inference from"
say "  coarse timestamps, NOT a measurement, and you should not score it."
say ""
say "  If you want the load axis on reference storage, measure it yourself:"
say "  follow --clean-room step 3 into a SCRATCH datadir on the gp2 volume"
say "  (never into the audited bank), time it, and put the result in"
say "  /opt/pgrust/LOADINFO.txt as load_time_seconds=. Your number is then a"
say "  real measurement and better evidence than anything above."
say "=============================================================================="
