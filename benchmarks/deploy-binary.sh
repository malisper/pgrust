#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy-binary.sh — install a binary you built onto the OLTP server.
#
# RUN THIS FROM THE CLICKBENCH BOX (where build-pgo.sh produced the binary).
#
# WHY
#   One build serves both benchmarks. `build-pgo.sh` produces a binary on the
#   clickbench box; the OLTP runners need that same binary on the oltp-server.
#   This copies it over the security group's internal network and installs it
#   ALONGSIDE the shipped artifact, never over it — so the shipped binary stays
#   available for an A/B and nothing you do here is destructive.
#
#   Installed as:  /opt/pgrust/bin/postgres.auditor-build
#   Referred to as: --binary auditor-build   (by the OLTP runners)
#
# INTEGRITY
#   The sha256 is computed before the copy and re-verified after installation
#   on the far side. A mismatch is a hard failure — a silently truncated or
#   corrupted transfer would otherwise produce benchmark numbers attributed to
#   a binary that never ran.
#
# CREDENTIALS
#   None. This uses the ssh key you already use between the audit boxes, over
#   the private network. Nothing here touches AWS.
#
# USAGE
#   ./deploy-binary.sh ~/audit/build/postgres
#   ./deploy-binary.sh ~/audit/build/postgres --name my-experiment
#   ./deploy-binary.sh --verify           # just report what is installed there
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/hosts.env"

NAME="auditor-build"
SRCBIN=""
VERIFY_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --name)   shift; NAME="${1:-}" ;;
    --verify) VERIFY_ONLY=1 ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) SRCBIN="$1" ;;
  esac
  shift
done

DEST="/opt/pgrust/bin/postgres.$NAME"
SSH="ssh -o StrictHostKeyChecking=no ${AUDIT_USER}@${OLTP_SERVER_PRIVATE}"

echo "=============================================================================="
echo "deploy-binary — install a locally built binary on the OLTP server"
echo "=============================================================================="
echo "  target host : $OLTP_SERVER_PRIVATE  (private network, SG-internal only)"
echo "  install as  : $DEST"
echo

if [ "$VERIFY_ONLY" = 1 ]; then
  echo "  Binaries currently installed on the OLTP server:"
  $SSH 'for f in /opt/pgrust/bin/postgres*; do
          case "$f" in *.sha256) continue;; esac
          printf "    %-46s %s\n" "$f" "$(sha256sum "$f" | cut -d" " -f1)"
        done' || { echo "  !! could not reach the server"; exit 1; }
  exit 0
fi

[ -n "$SRCBIN" ]  || { echo "!! need a path to a binary. See --help."; exit 2; }
[ -x "$SRCBIN" ]  || { echo "!! not an executable file: $SRCBIN"; exit 2; }

LOCAL_SHA=$(sha256sum "$SRCBIN" | cut -d' ' -f1)
SIZE=$(stat -c %s "$SRCBIN")
echo "  source      : $SRCBIN"
echo "  size        : $SIZE bytes"
echo "  sha256      : $LOCAL_SHA"

# Carry the build manifest across too, if build-pgo.sh produced one. Provenance
# should travel with the artifact, not live only on the machine that built it.
MANIFEST="$(dirname "$SRCBIN")/build-manifest.txt"
echo

echo "  copying ..."
scp -q -o StrictHostKeyChecking=no "$SRCBIN" "${AUDIT_USER}@${OLTP_SERVER_PRIVATE}:/tmp/postgres.$NAME" \
  || { echo "  !! copy failed"; exit 1; }
if [ -r "$MANIFEST" ]; then
  scp -q -o StrictHostKeyChecking=no "$MANIFEST" \
      "${AUDIT_USER}@${OLTP_SERVER_PRIVATE}:/tmp/build-manifest.$NAME.txt" 2>/dev/null \
    && echo "  build manifest copied alongside"
fi

echo "  installing ..."
REMOTE_SHA=$($SSH "sudo install -m 755 /tmp/postgres.$NAME $DEST \
  && rm -f /tmp/postgres.$NAME \
  && if [ -f /tmp/build-manifest.$NAME.txt ]; then sudo install -m 644 /tmp/build-manifest.$NAME.txt $DEST.manifest.txt; rm -f /tmp/build-manifest.$NAME.txt; fi \
  && sudo sha256sum $DEST | cut -d' ' -f1")

echo "  installed sha256: ${REMOTE_SHA:-<none>}"
echo

if [ "$REMOTE_SHA" != "$LOCAL_SHA" ]; then
  echo "!! SHA MISMATCH AFTER COPY"
  echo "!!   local : $LOCAL_SHA"
  echo "!!   remote: ${REMOTE_SHA:-<none>}"
  echo "!! Refusing to declare this deployed. Do not benchmark it — any number"
  echo "!! it produced would be attributed to a binary that never ran."
  exit 1
fi

$SSH "sudo sh -c 'echo $LOCAL_SHA > $DEST.sha256'"

echo "  VERIFIED — local and installed sha256 match."
echo
echo "  Run the OLTP benchmarks against it:"
echo "    ./run-oltp-ro.sh --binary $NAME"
echo "    ./run-oltp-rw.sh --binary $NAME"
echo "  (run those on the CLIENT box: ${OLTP_CLIENT_HOST})"
echo
echo "  The C PostgreSQL arm is untouched — it is the same 18.3 build in both"
echo "  modes, so a shipped-vs-your-build comparison changes only the pgrust side."
echo "=============================================================================="
