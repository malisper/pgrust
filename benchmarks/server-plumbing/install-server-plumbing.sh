#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# install-server-plumbing.sh — rebuild /opt/audit on the OLTP server.
#
# RUN THIS ON THE OLTP SERVER, from a checkout of this branch.
#
# WHY THIS EXISTS
#   The arm-switching plumbing lives at /opt/audit on the oltp-server. It was
#   originally created in place, which meant it was neither auditable from the
#   repository nor reproducible if the box were rebuilt. This script closes
#   that gap: everything under /opt/audit is now generated from files in this
#   directory, and the two must stay byte-identical (verify with --check).
#
# WHAT IT INSTALLS
#   /opt/audit/switch-arm.sh   the one-at-a-time engine switcher
#   /opt/audit/audit.conf      the shared config INCLUDED by all four datadirs
#   /opt/audit/compat-lib/     an openldap 2.6 soname shim (see below)
#
# WHAT IT DOES *NOT* DO
#   It does not create datadirs or load datasets. Those are hours of work and
#   are described in LOADINFO.as-built.txt.
#
# THE compat-lib SHIM, AND WHY AL2023 NEEDS IT
#   The C arm is the official PGDG PostgreSQL 18.3 build for RHEL 9. Its
#   libs link the openldap 2.6 sonames `libldap.so.2` / `liblber.so.2`.
#   Amazon Linux 2023 ships openldap 2.4, whose sonames are
#   `libldap-2.4.so.2` / `liblber-2.4.so.2` — so nothing on the system
#   provides what PGDG asks for, and the PGDG RPMs will not resolve.
#
#   Rather than replace AL2023's openldap (which other things depend on), we
#   place the 2.6 libraries alongside it. The sonames differ, so the two
#   coexist without interfering. AL2023's openldap is untouched.
#
#   The libraries are NOT committed to this repository — they are third-party
#   binaries and a git tree is the wrong place for them. This script fetches
#   them from the public AlmaLinux 9 mirror and extracts them, so the shim is
#   reproducible rather than a mystery blob. A consequence worth knowing:
#   because the PGDG RPMs are then installed with `--nodeps`,
#   `rpm -V postgresql18-server` reports `libldap.so.2` as unsatisfied
#   forever. That is cosmetic residue of a non-RPM-owned library, not a
#   broken install; `ldd` on every binary in /usr/pgsql-18/bin resolves clean.
#
# USAGE
#   sudo ./install-server-plumbing.sh            # install / repair
#   sudo ./install-server-plumbing.sh --check    # diff tree vs box, change nothing
# ---------------------------------------------------------------------------
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST=/opt/audit
OPENLDAP_RPM_URL="${OPENLDAP_RPM_URL:-https://repo.almalinux.org/almalinux/9/BaseOS/aarch64/os/Packages/openldap-2.6.8-4.el9.aarch64.rpm}"

MODE=install
[ "${1:-}" = "--check" ] && MODE=check
[ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] && { sed -n '2,48p' "$0"; exit 0; }

if [ "$MODE" = check ]; then
  echo "Comparing the tree against $DEST (nothing will be changed):"
  rc=0
  for f in switch-arm.sh audit.conf; do
    if sudo diff -q "$HERE/$f" "$DEST/$f" >/dev/null 2>&1; then
      echo "  OK        $f  (byte-identical)"
    else
      echo "  DIFFERS   $f"
      sudo diff -u "$DEST/$f" "$HERE/$f" | sed 's/^/            /' | head -30
      rc=1
    fi
  done
  for l in libldap.so.2 liblber.so.2; do
    [ -e "$DEST/compat-lib/$l" ] && echo "  present   compat-lib/$l" \
      || { echo "  MISSING   compat-lib/$l"; rc=1; }
  done
  echo
  [ $rc = 0 ] && echo "In sync." || echo "Out of sync — reconcile before trusting either copy."
  exit $rc
fi

[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }

echo "installing $DEST from $HERE"
install -d -m 755 "$DEST"
install -m 755 "$HERE/switch-arm.sh" "$DEST/switch-arm.sh"
install -m 644 "$HERE/audit.conf"    "$DEST/audit.conf"

# --- the openldap 2.6 soname shim ------------------------------------------
if [ -e "$DEST/compat-lib/libldap.so.2" ]; then
  echo "  compat-lib already present — leaving it alone"
else
  echo "  building compat-lib from the public AlmaLinux 9 openldap package"
  install -d -m 755 "$DEST/compat-lib" "$DEST/pkgs"
  tmp=$(mktemp -d)
  ( cd "$tmp" \
    && curl -fsSLO "$OPENLDAP_RPM_URL" \
    && cp ./*.rpm "$DEST/pkgs/" \
    && rpm2cpio ./*.rpm | cpio -idm --quiet ) \
    || { echo "  !! could not fetch/extract $OPENLDAP_RPM_URL" >&2; rm -rf "$tmp"; exit 1; }
  # Copy the real objects and recreate the soname symlinks.
  find "$tmp" -name 'libldap.so.2.*' -o -name 'liblber.so.2.*' | while read -r so; do
    install -m 755 "$so" "$DEST/compat-lib/$(basename "$so")"
  done
  ( cd "$DEST/compat-lib"
    for base in libldap liblber; do
      real=$(ls ${base}.so.2.* 2>/dev/null | head -1)
      [ -n "$real" ] && ln -sf "$real" "${base}.so.2"
    done )
  rm -rf "$tmp"
fi

# Publish it to the dynamic linker.
echo "$DEST/compat-lib" > /etc/ld.so.conf.d/audit-compat.conf
ldconfig

# --- the stack rlimit the config needs -------------------------------------
# max_stack_depth=60000 in audit.conf will not start under the default 8MB
# stack rlimit. switch-arm.sh raises it per-launch; this makes it durable.
cat > /etc/security/limits.d/99-audit-stack.conf <<'EOF'
* soft stack 204800
* hard stack 204800
EOF

echo
echo "installed:"
ls -la "$DEST" | sed 's/^/  /'
echo
echo "verify the C arm's libraries resolve:"
echo "  ldd /usr/pgsql-18/bin/postgres | grep -E 'ldap|lber|not found'"
echo
echo "then:  sudo $DEST/switch-arm.sh status"
