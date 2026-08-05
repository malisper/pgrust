#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# update-binary.sh — install a published pgrust RC binary, sha-verified.
#
# WHY THIS EXISTS
#   The preinstalled binary has an open crash bug (grouped MIN/MAX over text at
#   high repetition — see the README blocker note). When we publish the fixed
#   build, you install it with one command; we do not need to touch your
#   machines, and you do not need to trust that we did the swap correctly.
#
# THE TRUST ANCHOR
#   binaries/MANIFEST.tsv in this branch lists every binary we publish, with its
#   sha256. This script installs ONLY a binary whose hash matches a manifest
#   row. A download that was tampered with, truncated, or is simply the wrong
#   build cannot be installed silently — it is refused.
#
#   That means the manifest, not the download, is what you are trusting. It is
#   in git, so you can see when a row appeared and what changed.
#
# WHAT IT CHANGES
#   Installs to /opt/pgrust/bin/, and by default installs ALONGSIDE the existing
#   binary rather than over it, so you keep the ability to A/B. Use
#   --replace-default to make it the binary the runners pick up with no flags.
#
# USAGE
#   ./update-binary.sh --list                       # what is published, what is installed
#   ./update-binary.sh rc1-sinkfix                  # install by manifest name
#   ./update-binary.sh <sha256>                     # install by hash
#   ./update-binary.sh --from /path/to/postgres     # install a local file (still hash-checked)
#   ./update-binary.sh rc1-sinkfix --replace-default
#   ./update-binary.sh rc1-sinkfix --verify-only     # fetch + hash check, install nothing
#   ./update-binary.sh --url <URL> --expect <sha256>  # explicit, for a URL we gave you
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/hosts.env" ] && . "$HERE/hosts.env" 2>/dev/null
MANIFEST="$HERE/binaries/MANIFEST.tsv"
BINDIR=/opt/pgrust/bin

WANT=""; FROM=""; URL=""; EXPECT=""; REPLACE=0; LIST=0; VERIFY_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --list)             LIST=1 ;;
    --from)             shift; FROM="${1:-}" ;;
    --url)              shift; URL="${1:-}" ;;
    --expect)           shift; EXPECT="${1:-}" ;;
    --replace-default)  REPLACE=1 ;;
    --verify-only)      VERIFY_ONLY=1 ;;
    -h|--help)          sed -n '2,32p' "$0"; exit 0 ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *)  WANT="$1" ;;
  esac
  shift
done

say(){ printf '%s\n' "$*"; }
die(){ say ""; say "!! $*"; exit 1; }
[ -r "$MANIFEST" ] || die "no manifest at $MANIFEST — are you running this from the audit kit?"

rows(){ grep -v '^#' "$MANIFEST" | grep -v '^[[:space:]]*$'; }

say "=============================================================================="
say "pgrust binary update"
say "=============================================================================="

if [ "$LIST" = 1 ]; then
  say ""
  say "PUBLISHED (from binaries/MANIFEST.tsv):"
  printf '  %-14s %-10s %s\n' NAME STATUS SHA256
  rows | while IFS=$'\t' read -r name sha src status url notes; do
    printf '  %-14s %-10s %s\n' "$name" "$status" "$sha"
    [ "${url:-}" != "-" ] && [ -n "${url:-}" ] && printf '  %-14s %-10s url: %s\n' "" "" "${url:0:80}…"
    printf '%s\n' "$notes" | fold -s -w 70 | sed 's/^/                             /'
  done
  say ""
  say "INSTALLED on this box:"
  for f in "$BINDIR"/postgres*; do
    case "$f" in *.sha256|*.manifest.txt|*.profdata) continue ;; esac
    [ -f "$f" ] || continue
    h=$(sudo sha256sum "$f" | cut -d' ' -f1)
    nm=$(rows | awk -F'\t' -v h="$h" '$2==h{print $1; exit}')
    printf '  %-42s %s  %s\n' "$f" "${h:0:16}…" "${nm:+[$nm]}${nm:-[not in manifest]}"
  done
  say ""
  say "The runners default to $BINDIR/postgres. Point them elsewhere with"
  say "  run-clickbench.sh --binary <path>      (clickbench box)"
  say "  run-oltp-{ro,rw}.sh --binary <name>    (oltp client; name = suffix after postgres.)"
  exit 0
fi

# --- resolve what to install ------------------------------------------------
NAME=""; SHA=""; SRC=""; STATUS=""; MURL=""; NOTES=""
if [ -n "$EXPECT" ]; then
  SHA="$EXPECT"; NAME="explicit"; STATUS=current
  # An explicitly supplied hash must still exist in the manifest, otherwise the
  # manifest is not a trust anchor at all.
  rows | awk -F'\t' -v h="$SHA" '$2==h{found=1} END{exit !found}' \
    || die "sha256 $SHA is not in the manifest. Refusing: the manifest is the
     trust anchor, and installing a hash it does not list would make this
     script a download tool rather than a verification tool."
elif [ -n "$WANT" ]; then
  IFS=$'\t' read -r NAME SHA SRC STATUS MURL NOTES < <(rows | awk -F'\t' -v w="$WANT" '$1==w || $2==w {print; exit}')
  [ -n "${NAME:-}" ] || die "'$WANT' is not in the manifest. Run --list to see what is published."
else
  die "nothing specified. Run --list, or give a manifest name / sha256 / --from PATH."
fi

say "  requested : $WANT"
say "  name      : $NAME"
say "  status    : $STATUS"
say "  sha256    : $SHA"
[ -n "${SRC:-}" ] && say "  source sha: $SRC"
[ -n "${NOTES:-}" ] && { say "  notes     :"; printf '%s\n' "$NOTES" | fold -s -w 70 | sed 's/^/              /'; }
say ""

case "$SHA" in
  PENDING*|"")
    die "$NAME is announced but NOT YET PUBLISHED — the manifest has no real
     hash for it. There is nothing to install and nothing to verify against.
     When we publish it, this row gets a real sha256 in git and this command
     starts working. Until then, do not accept a binary for it from any
     source, including us." ;;
esac

# --- obtain the file --------------------------------------------------------
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
CAND="$TMP/postgres"

if [ -n "$FROM" ]; then
  [ -f "$FROM" ] || die "no such file: $FROM"
  cp "$FROM" "$CAND"; say "  source: local file $FROM"
elif [ -n "$URL" ]; then
  say "  downloading $URL"
  curl -fSL --retry 3 --progress-bar -o "$CAND" "$URL" || die "download failed"
elif [ -n "${MURL:-}" ] && [ "${MURL}" != "-" ]; then
  say "  downloading the URL recorded in the manifest"
  curl -fSL --retry 3 --progress-bar -o "$CAND" "$MURL" || die "the manifest URL failed — it is
     a time-limited pre-signed link and has probably expired. Ask us to reissue
     it and pass it with --url. The sha256 check below is unchanged either way,
     so an expired link costs a round trip and nothing else."
else
  die "no source for the bytes. Use --from PATH, or --url URL with the link we
     gave you. This script deliberately has no hardcoded, permanent download
     endpoint: these machines hold no credentials, and an endpoint that could
     be changed without a manifest change would defeat the hash check."
fi

# --- verify -----------------------------------------------------------------
GOT=$(sha256sum "$CAND" | cut -d' ' -f1)
say ""
say "  expected sha256: $SHA"
say "  actual   sha256: $GOT"
if [ "$GOT" != "$SHA" ]; then
  die "HASH MISMATCH — refusing to install.
     These bytes are not the published binary. Do not benchmark them, and do
     not work around this check: a number produced by an unverified binary is
     attributable to nothing."
fi
say "  VERIFIED"
say ""

if [ "$VERIFY_ONLY" = 1 ]; then
  say "  --verify-only: the bytes are correct and nothing was installed."
  say "  size: $(wc -c < "$CAND" | tr -d ' ') bytes"
  say "=============================================================================="
  exit 0
fi

"$CAND" --version >/dev/null 2>&1 || say "  !! warning: the binary would not report --version; installing anyway"

# --- install ----------------------------------------------------------------
OLD_DEFAULT_SHA=$(sudo sha256sum "$BINDIR/postgres" 2>/dev/null | cut -d' ' -f1)
DEST="$BINDIR/postgres.$NAME"
sudo install -d -m 755 "$BINDIR"
sudo install -m 755 "$CAND" "$DEST"
sudo sh -c "echo $SHA > $DEST.sha256"
say "  installed: $DEST"

if [ "$REPLACE" = 1 ]; then
  # Keep the outgoing default recoverable rather than overwriting it blind.
  if [ -n "$OLD_DEFAULT_SHA" ]; then
    KEEP="$BINDIR/postgres.previous-${OLD_DEFAULT_SHA:0:12}"
    sudo cp -n "$BINDIR/postgres" "$KEEP" 2>/dev/null && say "  kept previous default at $KEEP"
  fi
  sudo install -m 755 "$CAND" "$BINDIR/postgres"
  sudo sh -c "echo $SHA > $BINDIR/postgres.sha256"
  say "  replaced the default: $BINDIR/postgres"
fi

say ""
say "  old default sha256: ${OLD_DEFAULT_SHA:-<none>}"
say "  new default sha256: $(sudo sha256sum "$BINDIR/postgres" 2>/dev/null | cut -d' ' -f1)"
say ""
if [ "$REPLACE" = 1 ]; then
  say "  hosts.env still records PGRUST_SHA256=${PGRUST_SHA256:-<unset>}."
  say "  Update it to $SHA so the runners stop warning about a hash mismatch,"
  say "  or leave it and let them warn — the warning is correct either way."
else
  say "  The default binary is UNCHANGED. Use the new one explicitly:"
  say "    ./run-clickbench.sh --binary $DEST"
  say "    ./run-oltp-ro.sh --binary $NAME     # after deploy-binary.sh to the server"
fi
say ""
say "  On the OLTP server, install it there too:"
say "    ./deploy-binary.sh $DEST --name $NAME"
say "=============================================================================="
