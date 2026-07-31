#!/usr/bin/env python3
"""Assemble fuzz/core/csrc/pg_multirangetypes_io.c from verbatim PG 18.3 extracts.

Sibling of assemble.py (which emits pg_rangetypes_io.c). SAME extraction rule:
locate `^name(` at column 0, back up to the return-type line, capture through
the closing `^}`; bodies stay byte-for-byte, the only mechanical transforms are
(a) `static ` on the return-type line (single-TU linkage) and (b) generated
static forward prototypes so paste order never matters.

STRUCTURE: this file #includes pg_rangetypes_io.c, so the two oracles are ONE
translation unit. multirangetypes.c calls ~14 rangetypes.c statics
(make_range, range_deserialize, range_cmp_bounds, range_{contains,overlaps,
before,adjacent,overleft,minus,union,intersect,split}_internal,
bounds_adjacent, make_empty_range) plus the typcache mock, the palloc arena,
the ereport/longjmp shim and the StringInfo/pqformat shims. Duplicating those
into a second TU would either collide at link time or silently drift from the
range oracle; extern-promoting ~40 statics would be a large edit to a
concurrently-owned file. Including it costs one line and keeps
pg_rangetypes_io.c byte-identical apart from the additive `rngtype` field.
build.rs therefore compiles ONLY this file (see its comment).
"""
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else SRC / "pg_multirangetypes_io.c"

protos = []


def load(name):
    return (SRC / name).read_text().split("\n")


def extract_fn(lines, name):
    for i, l in enumerate(lines):
        if re.match(rf"^{re.escape(name)}\(", l):
            j = i - 1
            while j >= 0 and lines[j].strip() and not lines[j].strip().endswith("*/") and not lines[j].startswith("}"):
                j -= 1
            j += 1
            if not lines[j].startswith("static"):
                lines[j] = "static " + lines[j]
            k = i
            while not lines[k].rstrip().endswith(")"):
                k += 1
            protos.append(" ".join(x.strip() if x is not lines[j] else x for x in lines[j:k + 1]) + ";")
            e = k
            while lines[e] != "}":
                e += 1
            return "\n".join(lines[j:e + 1])
    raise SystemExit(f"extract_fn: {name} not found")


def find(lines, pred, start=0):
    for i in range(start, len(lines)):
        if pred(lines[i]):
            return i
    raise SystemExit("find: no match")


mr = load("multirangetypes.c")
mrh = load("multirangetypes.h")
rt = load("rangetypes.c")
af = load("arrayfuncs.c")
au = load("arrayutils.c")

# ---------------------------------------------------------------- verbatim types
region = []

# multirangetypes.h: MultirangeType struct + its accessor macros (VERBATIM)
s = find(mrh, lambda l: l.startswith("typedef struct") and "vl_len_" not in l)
e = find(mrh, lambda l: l.startswith("} MultirangeType;"))
region.append("/* ==== multirangetypes.h MultirangeType — VERBATIM ==== */\n"
              + "\n".join(mrh[s:e + 1]))
s = find(mrh, lambda l: l.startswith("#define MultirangeTypeGetOid"))
e = find(mrh, lambda l: l.startswith("#define MultirangeIsEmpty"))
region.append("\n".join(mrh[s:e + 1]))

MR_TYPES = """
/* ---- multirange fmgr plumbing (multirangetypes.h static inlines, VERBATIM
 * semantics; PG_DETOAST_DATUM is the range oracle's short-header expander) ---- */
#define DatumGetMultirangeTypeP(X)  ((MultirangeType *) PG_DETOAST_DATUM(X))
#define MultirangeTypePGetDatum(X)  PointerGetDatum(X)
#define PG_GETARG_MULTIRANGE_P(n)   DatumGetMultirangeTypeP(PG_GETARG_DATUM(n))
#define PG_RETURN_MULTIRANGE_P(x)   return MultirangeTypePGetDatum(x)

/* pinned multirange type oids (pg_type.dat @ 62d6c7d3df) */
#define INT4MULTIRANGEOID  4451
#define NUMMULTIRANGEOID   4532
#define INT8MULTIRANGEOID  4536

/* errcode classes ADDED by the multirange surface (the range oracle's table
 * is 1..11 + 98/99; the Rust driver mirrors these numbers) */
#define ERRCODE_CARDINALITY_VIOLATION   12  /* 21000 */
#define ERRCODE_NULL_VALUE_NOT_ALLOWED  13  /* 22004 */

#define TYPECACHE_MULTIRANGE_INFO 0x10000
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED  14  /* 54000 */

/* ---- plumbing the multirange surface needs beyond the range oracle's ---- */
/* PG_NARGS: fcinfo->nargs, as in fmgr.h */
#define PG_NARGS() (fcinfo->nargs)

/* pnstrdup (src/backend/utils/mmgr/mcxt.c): NUL-terminated palloc'd copy of
 * at most len bytes. Arena-allocated like the pstrdup shim above it. */
static char *
pnstrdup(const char *in, Size len)
{
	char	   *out;
	Size		n = strnlen(in, len);

	out = palloc(n + 1);
	memcpy(out, in, n);
	out[n] = '\\0';
	return out;
}

/* resetStringInfo (src/common/stringinfo.c): keep the buffer, drop content. */
static void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\\0';
	str->len = 0;
	str->cursor = 0;
}
"""
region.append(MR_TYPES)

# multirangetypes.c: MultirangeParseState + item/part macros + the bsearch
# comparison typedef (VERBATIM region)
s = find(mr, lambda l: l.startswith("typedef struct MultirangeIOData"))
e = find(mr, lambda l: l.startswith("} MultirangeIOData;"))
region.append("/* ==== multirangetypes.c MultirangeIOData — VERBATIM ==== */\n"
              + "\n".join(mr[s:e + 1]))
s = find(mr, lambda l: l.startswith("typedef enum"))
e = find(mr, lambda l: l.startswith("} MultirangeParseState;"))
region.append("/* ==== multirangetypes.c parse state — VERBATIM ==== */\n"
              + "\n".join(mr[s:e + 1]))
s = find(mr, lambda l: l.startswith("#define MultirangeGetItemsPtr"))
e = find(mr, lambda l: l.startswith("#define MULTIRANGE_ITEM_OFFSET_STRIDE"))
region.append("/* ==== multirangetypes.c part/item macros — VERBATIM ==== */\n"
              + "\n".join(mr[s:e + 1]))
s = find(mr, lambda l: l.startswith("typedef int (*multirange_bsearch_comparison)"))
e = find(mr, lambda l: l.rstrip().endswith("bool *match);"), s)
region.append("\n".join(mr[s:e + 1]))

# ---- array plumbing for multirange_constructor2 (verbatim array.h macros) ----
ARRAY_SHIM = """
/* ---- ArrayType + ARR_* (src/include/utils/array.h, VERBATIM) ---- */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

#define ARR_SIZE(a)				VARSIZE(a)
#define ARR_NDIM(a)				((a)->ndim)
#define ARR_HASNULL(a)			((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)			((a)->elemtype)
#define ARR_DIMS(a) \\
		((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \\
		((int *) (((char *) (a)) + sizeof(ArrayType) + \\
				  sizeof(int) * ARR_NDIM(a)))
#define ARR_NULLBITMAP(a) \\
		(ARR_HASNULL(a) ? \\
		 (bits8 *) (((char *) (a)) + sizeof(ArrayType) + \\
					2 * sizeof(int) * ARR_NDIM(a)) \\
		 : (bits8 *) NULL)
#define ARR_OVERHEAD_NONULLS(ndims) \\
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_DATA_OFFSET(a) \\
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) \\
		(((char *) (a)) + ARR_DATA_OFFSET(a))
#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))
#define MaxAllocSize ((Size) 0x3fffffff)
#define PG_GETARG_ARRAYTYPE_P(n) ((ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))

/* ---- qsort_arg: THE verbatim PG sort (src/port/qsort_arg.c over
 * src/include/lib/sort_template.h). Vendored so within-tie ordering is C's,
 * not a hand-rolled stand-in. The two c.h/port.h prerequisites sort_template
 * expects are spelled verbatim first. ---- */
/* src/include/c.h, VERBATIM */
#define CppAsString(identifier) #identifier
#define CppConcat(x, y)			x##y
/* src/include/port.h, VERBATIM */
typedef int (*qsort_arg_comparator) (const void *a, const void *b, void *arg);

#define ST_SORT qsort_arg
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARATOR_TYPE_NAME qsort_arg_comparator
#define ST_COMPARE_RUNTIME_POINTER
#define ST_COMPARE_ARG_TYPE void
#define ST_SCOPE static
#define ST_DEFINE
#include "sort_template.h"
"""
region.append(ARRAY_SHIM)

# ---- multirange typcache mock (mirrors the range oracle's, shim 1) ----
MR_TYPCACHE = """
/* ---- multirange typcache seam (extends the range oracle's mock: the three
 * pinned multirange entries, each pointing rngtype at the already-mocked
 * range entry; the Rust driver pre-seeds the SAME values) ---- */
static TypeCacheEntry pg_mr_int4multirange_typentry = {
	.type_id = INT4MULTIRANGEOID, .typlen = -1, .typbyval = false,
	.typalign = TYPALIGN_INT, .typstorage = TYPSTORAGE_EXTENDED, .typtype = 'm',
	.rngtype = &pg_rt_int4range_typentry,
};

static TypeCacheEntry pg_mr_nummultirange_typentry = {
	.type_id = NUMMULTIRANGEOID, .typlen = -1, .typbyval = false,
	.typalign = TYPALIGN_INT, .typstorage = TYPSTORAGE_EXTENDED, .typtype = 'm',
	.rngtype = &pg_rt_numrange_typentry,
};

static TypeCacheEntry pg_mr_int8multirange_typentry = {
	.type_id = INT8MULTIRANGEOID, .typlen = -1, .typbyval = false,
	.typalign = TYPALIGN_DOUBLE, .typstorage = TYPSTORAGE_EXTENDED, .typtype = 'm',
	.rngtype = &pg_rt_int8range_typentry,
};

/* The verbatim multirange bodies call lookup_type_cache with a MULTIRANGE oid,
 * which the range oracle's mock does not know (it elogs on anything but the
 * three ranges and their elements). Extend it for this half of the TU by the
 * same rename shim used for get_type_io_data below: multirange oids resolve
 * here, everything else delegates to the range oracle's mock unchanged. */
static TypeCacheEntry *
pg_mr_lookup(Oid mltrngtypid)
{
	switch (mltrngtypid)
	{
		case INT4MULTIRANGEOID:
			return &pg_mr_int4multirange_typentry;
		case NUMMULTIRANGEOID:
			return &pg_mr_nummultirange_typentry;
		case INT8MULTIRANGEOID:
			return &pg_mr_int8multirange_typentry;
		default:
			return NULL;
	}
}

static TypeCacheEntry *
pg_mr_lookup_type_cache(Oid type_id, int flags)
{
	TypeCacheEntry *e = pg_mr_lookup(type_id);

	if (e != NULL)
		return e;
	return lookup_type_cache(type_id, flags);
}

/* multirangetypes.c multirange_get_typcache over the stub (fn_extra memo
 * elided exactly as range_get_typcache is: a pure cache, no behavior). */
static TypeCacheEntry *
multirange_get_typcache(FunctionCallInfo fcinfo, Oid mltrngtypid)
{
	TypeCacheEntry *typcache = pg_mr_lookup_type_cache(mltrngtypid,
													   TYPECACHE_MULTIRANGE_INFO);

	if (typcache->rngtype == NULL)
		elog(ERROR, "type %u is not a multirange type", mltrngtypid);
	return typcache;
}

/* A multirange's ELEMENT is its range type, so get_multirange_io_data resolves
 * range_in/out/recv/send where the range oracle's own lsyscache/fmgr shims only
 * know the three scalar element types (they elog on anything else). Extend both
 * shims for this half of the TU by RENAMING the two calls: the verbatim
 * multirange bodies below bind to the extended versions, while the range
 * oracle's already-compiled bodies keep using their originals. Plumbing only —
 * the range-type rows are the real pg_type/pg_proc rows. */
#define F_RANGE_IN   3834
#define F_RANGE_OUT  3835
#define F_RANGE_RECV 3836
#define F_RANGE_SEND 3837

static void
pg_mr_get_type_io_data(Oid typid, IOFuncSelector which_func,
					   int16 *typlen, bool *typbyval, char *typalign,
					   char *typdelim, Oid *typioparam, Oid *func)
{
	if (typid != INT4RANGEOID && typid != INT8RANGEOID && typid != NUMRANGEOID)
	{
		get_type_io_data(typid, which_func, typlen, typbyval, typalign,
						 typdelim, typioparam, func);
		return;
	}
	{
		TypeCacheEntry *e = lookup_type_cache(typid, 0);

		*typlen = e->typlen;
		*typbyval = e->typbyval;
		*typalign = e->typalign;
		*typdelim = ',';
		*typioparam = typid;
		*func = (which_func == IOFunc_input) ? F_RANGE_IN :
			(which_func == IOFunc_output) ? F_RANGE_OUT :
			(which_func == IOFunc_receive) ? F_RANGE_RECV : F_RANGE_SEND;
	}
}

static void
pg_mr_fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, int mcxt)
{
	switch (functionId)
	{
		case F_RANGE_IN:
		case F_RANGE_OUT:
		case F_RANGE_RECV:
		case F_RANGE_SEND:
			(void) mcxt;
			memset(finfo, 0, sizeof(*finfo));
			finfo->fn_oid = functionId;
			finfo->fn_strict = true;
			finfo->fn_addr = (functionId == F_RANGE_IN) ? range_in :
				(functionId == F_RANGE_OUT) ? range_out :
				(functionId == F_RANGE_RECV) ? range_recv : range_send;
			break;
		default:
			fmgr_info_cxt(functionId, finfo, mcxt);
			break;
	}
}

#define lookup_type_cache pg_mr_lookup_type_cache
#define get_type_io_data pg_mr_get_type_io_data
#define fmgr_info_cxt pg_mr_fmgr_info_cxt
"""
region.append(MR_TYPCACHE)

blocks = ["\n\n".join(region)]

# ---------------------------------------------------------------------- functions
fns = []
# ArrayGetNItems + deconstruct_array (constructor2's array path)
fns.append(extract_fn(au, "ArrayGetNItemsSafe"))
fns.append(extract_fn(au, "ArrayGetNItems"))
fns.append(extract_fn(af, "deconstruct_array"))
# rangetypes.c qsort callback used by multirange_canonicalize (NOT in the
# range oracle's extract list, so no duplicate definition)
fns.append(extract_fn(rt, "range_compare"))

MR_FNS = [
    # io + its cache
    "get_multirange_io_data",
    "multirange_in", "multirange_out", "multirange_recv", "multirange_send",
    # normalization kernel + image layout
    "multirange_canonicalize", "multirange_size_estimate",
    "write_multirange_data", "make_multirange",
    "multirange_get_bounds_offset", "multirange_get_range",
    "multirange_get_bounds", "multirange_get_union_range",
    "multirange_deserialize", "make_empty_multirange",
    "range_bounds_overlaps", "range_bounds_contains", "multirange_bsearch_match",
    # constructors
    "multirange_constructor2", "multirange_constructor1", "multirange_constructor0",
    # set ops
    "multirange_union", "multirange_minus", "multirange_minus_internal",
    "multirange_intersect", "multirange_intersect_internal",
    # accessors
    "multirange_lower", "multirange_upper", "multirange_empty",
    "multirange_lower_inc", "multirange_upper_inc",
    "multirange_lower_inf", "multirange_upper_inf",
    # containment
    "multirange_contains_elem", "elem_contained_by_multirange",
    "multirange_elem_bsearch_comparison", "multirange_contains_elem_internal",
    "multirange_contains_range", "range_contains_multirange",
    "range_contained_by_multirange", "multirange_contained_by_range",
    "multirange_range_contains_bsearch_comparison",
    "multirange_contains_range_internal", "range_contains_multirange_internal",
    "multirange_contains_multirange", "multirange_contained_by_multirange",
    "multirange_contains_multirange_internal",
    # equality
    "multirange_eq_internal", "multirange_eq",
    "multirange_ne_internal", "multirange_ne",
    # overlaps
    "range_overlaps_multirange", "multirange_overlaps_range",
    "multirange_overlaps_multirange",
    "multirange_range_overlaps_bsearch_comparison",
    "range_overlaps_multirange_internal",
    "multirange_overlaps_multirange_internal",
    # over-left / over-right
    "range_overleft_multirange_internal", "range_overleft_multirange",
    "multirange_overleft_range", "multirange_overleft_multirange",
    "range_overright_multirange_internal", "range_overright_multirange",
    "multirange_overright_range", "multirange_overright_multirange",
    # before / after / adjacent
    "range_before_multirange", "multirange_before_range",
    "multirange_before_multirange", "range_after_multirange",
    "multirange_after_range", "multirange_after_multirange",
    "range_before_multirange_internal", "multirange_before_multirange_internal",
    "range_after_multirange_internal",
    "range_adjacent_multirange_internal", "range_adjacent_multirange",
    "multirange_adjacent_range", "multirange_adjacent_multirange",
    # ordering
    "multirange_cmp", "multirange_lt", "multirange_le",
    "multirange_ge", "multirange_gt",
    # range_merge(multirange)
    "range_merge_from_multirange",
    # hashing
    "hash_multirange", "hash_multirange_extended",
]
# CARVED OUT (agg-state / SRF; their pure delegates are reached through the
# non-agg entries above): range_agg_transfn, range_agg_finalfn,
# multirange_agg_transfn, multirange_intersect_agg_transfn, multirange_unnest.
for f in MR_FNS:
    fns.append(extract_fn(mr, f))

HEADER = Path(SRC / "mr_header.h.in").read_text()
parts = [HEADER, blocks[0],
         "/* ---- auto-generated static prototypes (paste-order shim) ---- */\n"
         + "\n".join(protos),
         "\n\n".join(fns),
         Path(SRC / "mr_entries.c.in").read_text()]
OUT.write_text("\n\n".join(parts) + "\n")
print(f"wrote {OUT} ({len(OUT.read_text().splitlines())} lines, {len(protos)} fns)")
