#!/usr/bin/env python3
"""Assemble fuzz/core/csrc/pg_rowtypes_io.c from VERBATIM vendored PG 18.3
sources + hand-written shims (rowtypes_oracle_shims/). Extraction is by
symbol with brace matching so vendored bodies stay byte-for-byte; the only
mechanical transform is an optional `static ` prefix (per-TU symbol hygiene,
pg_strfam.c precedent), noted in the section marker."""
import re, sys
from pathlib import Path

V = Path("/Users/malisper/dev/pgrust-fabled/vendor/postgres-src/src")
OUT = Path("/Users/malisper/dev/pgrust-fast/.wt-p1-laneai/fuzz/core/csrc/pg_rowtypes_io.c")
SHIMS = Path(__file__).parent / "rowtypes_oracle_shims"


def lines(p):
    return (V / p).read_text().split("\n")


def func(path, name, static=False):
    ls = lines(path)
    start = None
    for i, l in enumerate(ls):
        if re.match(rf"^{re.escape(name)}\(", l):
            start = i
            break
    if start is None:
        sys.exit(f"cannot find {name} in {path}")
    b = start
    # single return-type line above (PG style), possibly preceded by qualifiers
    while b > 0 and re.match(r"^(static|inline|extern|const|unsigned|struct|[A-Za-z_][\w]*)[\w\s\*]*$", ls[b - 1]) and ls[b - 1].strip():
        b -= 1
    body, depth, seen = [], 0, False
    for l in ls[b:]:
        body.append(l)
        depth += l.count("{") - l.count("}")
        if "{" in l:
            seen = True
        if seen and depth == 0:
            break
    text = "\n".join(body)
    tag = " [static-prefixed]" if static and not text.startswith("static") else ""
    if static and not text.startswith("static"):
        text = "static " + text
    return f"/* ---- VERBATIM {path}: {name}{tag} ---- */\n" + text + "\n"


def rng(path, a, bz, note):
    ls = lines(path)
    return f"/* ---- VERBATIM {path}:{a}-{bz} ({note}) ---- */\n" + "\n".join(ls[a - 1: bz]) + "\n"


def grep_define(path, name):
    ls = lines(path)
    for i, l in enumerate(ls):
        if l.startswith(f"#define {name}"):
            out = [l]
            j = i
            while ls[j].endswith("\\"):
                j += 1
                out.append(ls[j])
            return f"/* ---- VERBATIM {path}: {name} ---- */\n" + "\n".join(out) + "\n"
    sys.exit(f"cannot find #define {name} in {path}")


def shim(name):
    return (SHIMS / name).read_text()


parts = [shim("00_header.c")]
parts.append(rng("include/varatt.h", 15, 358, "varatt structs + macros, incl guard"))
parts.append(shim("05_postvaratt.c"))
parts.append("/* SHIM: forward typedefs (htup.h carries these) */\n"
             "typedef struct HeapTupleHeaderData HeapTupleHeaderData;\n"
             "typedef HeapTupleHeaderData *HeapTupleHeader;\n")
parts.append(rng("include/access/htup_details.h", 122, 311, "heap tuple header structs + infomask bits"))
for f in ["HeapTupleHeaderGetDatumLength", "HeapTupleHeaderSetDatumLength",
          "HeapTupleHeaderGetTypeId", "HeapTupleHeaderSetTypeId",
          "HeapTupleHeaderGetTypMod", "HeapTupleHeaderSetTypMod"]:
    parts.append(func("include/access/htup_details.h", f))
parts.append(rng("include/access/htup_details.h", 577, 603, "natts macros + BITMAPLEN"))
parts.append(rng("include/access/htup.h", 62, 74, "HeapTupleData + HEAPTUPLESIZE"))
for f in ["HeapTupleHasNulls", "HeapTupleNoNulls", "HeapTupleHasVarWidth"]:
    parts.append(func("include/access/htup_details.h", f))
parts.append(shim("10_pgattr.c"))
parts.append(rng("include/access/tupdesc.h", 68, 88, "CompactAttribute"))
parts.append(shim("12_tupconstr.c"))
parts.append(rng("include/access/tupdesc.h", 135, 145, "TupleDescData"))
parts.append(rng("include/access/tupdesc.h", 149, 167, "TupleDescAttr"))
parts.append(shim("13_compactattr_accessor.c"))
parts.append(grep_define("backend/access/common/heaptuple.c", "COMPACT_ATTR_IS_PACKABLE"))
parts.append(grep_define("include/access/detoast.h", "VARATT_EXTERNAL_GET_POINTER"))
parts.append(rng("include/access/tupmacs.h", 20, 233, "tupmacs"))
parts.append(rng("include/lib/stringinfo.h", 46, 54, "StringInfoData"))
parts.append(func("include/lib/stringinfo.h", "initReadOnlyStringInfo"))
parts.append(shim("20_fmgr_shims.c"))
parts.append("/* SHIM: intra-TU prototypes for the verbatim stringinfo/pq bodies */\n"
             "static void enlargeStringInfo(StringInfo str, int needed);\n"
             "static void resetStringInfo(StringInfo str);\n"
             "static void appendBinaryStringInfo(StringInfo str, const void *data, int datalen);\n")
for f in ["initStringInfoInternal", "initStringInfo", "resetStringInfo",
          "appendStringInfoChar", "appendBinaryStringInfo", "enlargeStringInfo"]:
    parts.append(func("common/stringinfo.c", f, static=True))
parts.append(rng("include/lib/stringinfo.h", 231, 234, "appendStringInfoCharMacro"))
parts.append(func("include/libpq/pqformat.h", "pq_writeint32"))
for f in ["pq_begintypsend", "pq_endtypsend", "pq_sendbytes", "pq_copymsgbytes", "pq_getmsgint"]:
    parts.append(func("backend/libpq/pqformat.c", f, static=True))
parts.append(rng("include/libpq/pqformat.h", 142, 149, "pq_sendint32 inline"))
parts.append(func("backend/access/common/tupdesc.c", "populate_compact_attribute_internal", static=True))
parts.append(func("backend/access/common/tupdesc.c", "populate_compact_attribute", static=True))
for f in ["heap_compute_data_size", "fill_val", "heap_fill_tuple",
          "heap_form_tuple", "heap_deform_tuple", "heap_freetuple"]:
    parts.append(func("backend/access/common/heaptuple.c", f, static=True))
parts.append(func("backend/access/common/detoast.c", "toast_raw_datum_size", static=True))
parts.append(func("backend/utils/adt/datum.c", "datum_image_eq", static=True))
parts.append(shim("30_typcache_codecs.c"))
for fn in ["record_in", "record_out", "record_recv", "record_send",
           "record_cmp", "record_larger", "record_smaller",
           "record_image_cmp", "record_image_eq",
           "hash_record", "hash_record_extended"]:
    parts.append(func("backend/utils/adt/rowtypes.c", fn, static=True))
parts.append(shim("90_drivers.c"))

OUT.write_text("\n".join(parts))
print(f"wrote {OUT}: {len(OUT.read_text().splitlines())} lines")
