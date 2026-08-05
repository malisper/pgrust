#!/bin/sh
# assemble.sh — vendors the PostgreSQL 18.3 node-walker oracle family
# (nodesfam: outfuncs/readfuncs/copyfuncs + read/value/list/bitmapset/
# equalfuncs + datum/stringinfo/stack_depth + common/port support) from the
# vendor tree into fuzz/core/csrc/nodesfam/. Every file lands VERBATIM
# (whole-file copy, never hand-typed); the generated node-support files are
# produced by PostgreSQL's own generators (gen_node_support.pl,
# generate-errcodes.pl, generate-lwlocknames.pl, genbki.pl) run against the
# same pinned tree. Re-running this script must be a no-op diff.
#
# Vendor tree pin: ~/dev/pgrust-fabled/vendor/postgres-src, Stamp-18.3,
# upstream sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0.
#
# NOT copied (shims, hand-written, provenance in their own headers):
#   shim/pg_config.h, shim/pg_config_os.h, ../pg_nodesfam_io.c
set -eu
V=${PGSRC:-"$HOME/dev/pgrust-fabled/vendor/postgres-src"}
D=$(cd "$(dirname "$0")" && pwd)

# --- verbatim C sources -------------------------------------------------
mkdir -p "$D/src"
for f in \
    backend/nodes/outfuncs.c backend/nodes/readfuncs.c \
    backend/nodes/copyfuncs.c backend/nodes/equalfuncs.c \
    backend/nodes/read.c backend/nodes/value.c backend/nodes/list.c \
    backend/nodes/bitmapset.c backend/utils/adt/datum.c \
    backend/utils/misc/stack_depth.c \
    common/stringinfo.c common/d2s.c common/psprintf.c common/string.c \
    common/d2s_full_table.h common/d2s_intrinsics.h common/digit_table.h common/ryu_common.h \
    port/snprintf.c port/pg_bitutils.c port/pg_popcount_aarch64.c \
    port/strerror.c port/strlcpy.c port/strlcat.c; do
  cp "$V/src/$f" "$D/src/$(basename "$f")"
done

# --- verbatim header closure (computed by clang -MM over every TU) ------
mkdir -p "$D/include"
while read -r h; do
  mkdir -p "$D/include/$(dirname "$h")"
  cp "$V/src/include/$h" "$D/include/$h"
done < "$D/hdr_closure.txt"

# --- generated node support (PostgreSQL's own generators) ---------------
GEN="$D/gen"
mkdir -p "$GEN/nodes" "$GEN/utils" "$GEN/storage" "$GEN/catalog"
( cd "$V/src/include" && perl ../backend/nodes/gen_node_support.pl \
    --outdir "$GEN" \
    nodes/nodes.h nodes/primnodes.h nodes/parsenodes.h nodes/pathnodes.h \
    nodes/plannodes.h nodes/execnodes.h access/amapi.h access/cmptype.h \
    access/sdir.h access/tableam.h access/tsmapi.h \
    commands/event_trigger.h commands/trigger.h executor/tuptable.h \
    foreign/fdwapi.h nodes/bitmapset.h nodes/extensible.h \
    nodes/lockoptions.h nodes/miscnodes.h nodes/replnodes.h \
    nodes/supportnodes.h nodes/value.h utils/rel.h )
mv "$GEN/nodetags.h" "$GEN/nodes/nodetags.h"
rm -f "$GEN/queryjumblefuncs.funcs.c" "$GEN/queryjumblefuncs.switch.c"
perl "$V/src/backend/utils/generate-errcodes.pl" \
    --outfile "$GEN/utils/errcodes.h" "$V/src/backend/utils/errcodes.txt"
perl "$V/src/backend/storage/lmgr/generate-lwlocknames.pl" \
    -o "$GEN/storage" "$V/src/include/storage/lwlocklist.h" \
    "$V/src/backend/utils/activity/wait_event_names.txt"
( cd "$V/src/backend/catalog" && perl -I . genbki.pl \
    --include-path "$V/src/include" --set-version 18 \
    --output "$GEN/catalog" "$V"/src/include/catalog/pg_*.h )
# genbki emits .bki/schemapg etc. too; only the _d.h macro headers are used
find "$GEN/catalog" -type f ! -name '*_d.h' -delete
# decls-only stand-in: datum.c includes it, uses no symbol from it
printf '/* SHIM: decls-only stand-in for generated fmgrprotos.h; datum.c\n * includes it but the vendored build references no symbol of it. */\n' \
    > "$GEN/utils/fmgrprotos.h"

# --- enum-domain table for the gate (see gen_enum_domains.py header) ----
python3 "$D/gen_enum_domains.py" "$D/include" "$GEN/enum_domains.tsv"

echo "nodesfam assembled from $V"
