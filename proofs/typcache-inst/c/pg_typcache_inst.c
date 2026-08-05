/*
 * pg_typcache_inst.c — verbatim PostgreSQL container-operator bodies for the
 * typcache-instantiation probe: generic array / range operators proven
 * C≡Rust for CONCRETE element types (int4, int8, date, uuid, text), with the
 * typcache seam stubbed IDENTICALLY on both sides.
 *
 * Provenance (fetched 2026-07-28, ref REL_18_STABLE):
 *   - src/backend/utils/adt/arrayfuncs.c: array_eq, array_ne, array_cmp,
 *     array_lt, array_gt, array_le, array_ge, btarraycmp
 *   - src/backend/utils/adt/arrayutils.c: ArrayGetNItems, ArrayGetNItemsSafe
 *   - src/include/utils/arrayaccess.h: array_iter, array_iter_setup,
 *     array_iter_next
 *   - src/backend/utils/adt/rangetypes.c: range_eq/ne/overlaps/contains/
 *     contained_by/before/after/adjacent (fmgr level + *_internal),
 *     range_contains_elem, elem_contained_by_range, range_empty, range_cmp,
 *     range_lt/le/ge/gt, bounds_adjacent, range_cmp_bounds,
 *     range_cmp_bound_values, range_deserialize, range_serialize, make_range,
 *     range_get_flags, int4range_canonical, datum_compute_size, datum_write
 *   - src/backend/access/nbtree/nbtcompare.c: btint4cmp, btint8cmp
 *   - src/backend/utils/adt/int.c: int4eq
 *   - src/backend/utils/adt/int8.c: int8eq
 *   - src/backend/utils/adt/date.c: date_cmp, date_eq
 *   - src/backend/utils/adt/uuid.c: uuid_internal_cmp, uuid_cmp, uuid_eq
 *   - src/backend/utils/adt/varlena.c: varstr_cmp (C-collation arm; locale
 *     arm out of the collation fence, poisoned — same shim as
 *     proofs/text-cmp), text_cmp, texteq, bttextcmp
 *   - src/include/access/tupmacs.h, src/include/varatt.h,
 *     src/include/utils/array.h, src/include/utils/rangetypes.h: the macro /
 *     static-inline plumbing those bodies use (vendored verbatim semantics).
 *
 * Function BODIES are verbatim. Shims (plumbing only, each documented):
 *
 *  1. TYPCACHE SEAM (the probe's subject): lookup_type_cache() /
 *     range_get_typcache() return a STATIC TypeCacheEntry carrying the
 *     requested element type's concrete pg_type attributes and concrete
 *     comparator finfos (all vendored verbatim below):
 *       int4  typlen=4  byval 'i'  eq=int4eq   cmp=btint4cmp
 *       int8  typlen=8  byval 'd'  eq=int8eq   cmp=btint8cmp
 *       date  typlen=4  byval 'i'  eq=date_eq  cmp=date_cmp
 *       uuid  typlen=16 byref 'c'  eq=uuid_eq  cmp=uuid_cmp
 *       text  typlen=-1 byref 'i' storage 'x'  eq=texteq  cmp=bttextcmp
 *     Range entries: int4range(cmp=btint4cmp, canonical=int4range_canonical),
 *     int8range(cmp=btint8cmp), daterange(cmp=date_cmp). The int8range /
 *     daterange canonical finfos are left INVALID on BOTH sides: only
 *     bounds_adjacent/make_range consult them and range_adjacent is out of
 *     scope for those types (Rust-side serialize wall, see ledger).
 *     The Rust harness installs the IDENTICAL attributes + the shipped fc_*
 *     wrappers. Typcache lookup INTERNALS (catalog access, invalidation) are
 *     out of the proof on both sides; everything downstream of the entry is
 *     in.
 *  1b. COLLATION FENCE (text rows only, proofs/text-cmp shim 4/5):
 *     pg_newlocale_from_collation(collid)->collate_is_c / ->deterministic
 *     modeled as (collid == C_COLLATION_OID || POSIX): true for the pinned
 *     built-in C-locale collations; the locale arm of varstr_cmp
 *     (pg_strncoll) and the nondeterministic texteq arm return a poison
 *     sentinel / set the err flag, so a passing proof shows they are never
 *     reached under the fence. check_collation_set(collid==0) -> err flag.
 *     texteq's toast_raw_datum_size(argN) -> VARSIZE_ANY_EXHDR + VARHDRSZ
 *     (identical for the never-toasted proof images).
 *  2. fmgr: minimal FunctionCallInfoBaseData/FmgrInfo structs +
 *     LOCAL_FCINFO / InitFunctionCallInfoData / FunctionCallInvoke /
 *     FunctionCall2Coll with the same call semantics (args by NullableDatum,
 *     isnull flag). PG_GETARG_ANY_ARRAY_P / PG_GETARG_RANGE_P read the datum
 *     as a pre-detoasted flat pointer (bytea-cmp varlena precedent:
 *     detoasting is the caller contract, out of scope; expanded arrays are
 *     never fed — the verbatim VARATT_IS_EXPANDED_HEADER test then always
 *     takes the flat arm). PG_FREE_IF_COPY / check_stack_depth: no-ops.
 *  3. ereport(ERROR)/elog(ERROR) → PROOF_ELOG_ABORT(sentinel): set
 *     pg_proof_err and return a sentinel at the exact abort point (the
 *     PROOF_EREPORT_FLAG convention; message text never crosses).
 *     ereturn(escontext, ret, ...) → same flag + return ret.
 *     SOFT_ERROR_OCCURRED(ctx) → (pg_proof_err != 0) so the verbatim
 *     make_range unwind takes the same early-outs a longjmp would skip.
 *     RangeIsEmpty gets a NULL guard reachable ONLY on the flagged error
 *     path (value is dead there; harness compares err flags instead).
 *  4. palloc0 → two-slot rotating static buffer (range_serialize output,
 *     ≤ 24 bytes for int4 ranges; rotation because make_range's canonical
 *     call re-serializes while the first image's bounds live in locals).
 *
 * Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches.
 */

#include "../../support/c/pg_proof_shim.h"
#include <string.h>

/* ---------------- basic postgres typedefs / macros ---------------- */

typedef uintptr_t Datum;
typedef char *Pointer;
typedef uint8 bits8;
typedef struct Node Node;			/* opaque; only ever NULL here */

#define PG_INT32_MAX INT32_MAX
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define SHORTALIGN(LEN)  TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)    TYPEALIGN(4, (LEN))
#define DOUBLEALIGN(LEN) TYPEALIGN(8, (LEN))
#define MAXALIGN(LEN)    TYPEALIGN(8, (LEN))

#define TYPALIGN_CHAR   'c'
#define TYPALIGN_SHORT  's'
#define TYPALIGN_INT    'i'
#define TYPALIGN_DOUBLE 'd'
#define TYPSTORAGE_PLAIN 'p'

#define DatumGetBool(X)     ((bool) ((X) != 0))
#define BoolGetDatum(X)     ((Datum) ((X) ? 1 : 0))
#define DatumGetInt32(X)    ((int32) (X))
#define Int32GetDatum(X)    ((Datum) (X))
#define Int8GetDatum(X)     ((Datum) (X))
#define Int16GetDatum(X)    ((Datum) (X))
#define Int64GetDatum(X)    ((Datum) (X))
#define DatumGetPointer(X)  ((Pointer) (X))
#define PointerGetDatum(X)  ((Datum) (X))
#define DatumGetCString(X)  ((char *) DatumGetPointer(X))

/* ---- varatt.h (little-endian arm), verbatim semantics ---- */
#define VARHDRSZ_SHORT   1
#define VARATT_SHORT_MAX 0x7F
#define VARATT_IS_4B(PTR)   ((*((const uint8 *) (PTR)) & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) ((*((const uint8 *) (PTR)) & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) ((*((const uint8 *) (PTR)) & 0x03) == 0x02)
#define VARATT_IS_1B(PTR)   ((*((const uint8 *) (PTR)) & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) ((*((const uint8 *) (PTR))) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) (*((const uint8 *) (PTR)) != 0)
#define VARATT_IS_EXTERNAL(PTR) VARATT_IS_1B_E(PTR)
#define VARATT_IS_SHORT(PTR) VARATT_IS_1B(PTR)
#define VARSIZE(PTR)        ((*((const uint32 *) (PTR))) >> 2)
#define VARSIZE_SHORT(PTR)  ((*((const uint8 *) (PTR)) >> 1) & 0x7F)
#define VARDATA(PTR)        (((char *) (PTR)) + VARHDRSZ)
#define VARDATA_SHORT(PTR)  (((char *) (PTR)) + VARHDRSZ_SHORT)
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_SHORT(PTR) - VARHDRSZ_SHORT : \
	 VARSIZE(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) \
	(VARATT_IS_1B(PTR) ? VARDATA_SHORT(PTR) : VARDATA(PTR))
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
#define SET_VARSIZE_SHORT(PTR, len) \
	(*((uint8 *) (PTR)) = (((uint8) (len)) << 1) | 0x01)
/* proofs never feed external toast pointers; guard with the err flag */
#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B(PTR) ? VARSIZE_SHORT(PTR) : VARSIZE(PTR))
#define VARATT_CAN_MAKE_SHORT(PTR) \
	(VARATT_IS_4B_U(PTR) && \
	 (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
#define VARATT_CONVERTED_SHORT_SIZE(PTR) \
	(VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)
#define TYPE_IS_PACKABLE(typlen, typstorage) \
	((typlen) == -1 && (typstorage) != TYPSTORAGE_PLAIN)

/* ---------------- error-flag shim (PROOF_EREPORT_FLAG convention) -------- */

static int pg_proof_err = 0;

void pg_c_reset_err(void) { pg_proof_err = 0; }
int pg_c_get_err(void) { return pg_proof_err; }

/* hard ereport(ERROR)/elog(ERROR): flag + return sentinel at the abort
 * point (C aborts via longjmp there). */
#define PROOF_ELOG_ABORT(ret) do { pg_proof_err = 2; return ret; } while (0)
/* ereturn(escontext, ret, ...): soft-error convention */
#define proof_ereturn(ret) do { pg_proof_err = 1; return ret; } while (0)
#define SOFT_ERROR_OCCURRED(escontext) (pg_proof_err != 0)

/* ---------------- minimal fmgr (plumbing shim) ---------------- */

typedef struct FunctionCallInfoBaseData FunctionCallInfoBaseData;
typedef FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

typedef struct FmgrInfo
{
	PGFunction	fn_addr;
	Oid			fn_oid;
	void	   *fn_extra;
} FmgrInfo;

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	void	   *context;
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[2];
};

#define LOCAL_FCINFO(name, nargs_) \
	FunctionCallInfoBaseData name##_data; \
	FunctionCallInfo name = &name##_data

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	do { \
		(Fcinfo).flinfo = (Flinfo); \
		(Fcinfo).context = (Context); \
		(Fcinfo).resultinfo = (Resultinfo); \
		(Fcinfo).fncollation = (Collation); \
		(Fcinfo).isnull = false; \
		(Fcinfo).nargs = (Nargs); \
	} while (0)

#define FunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->fn_addr) (fcinfo))

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_GETARG_INT32(n)  DatumGetInt32(fcinfo->args[n].value)
#define PG_GETARG_DATUM(n)  (fcinfo->args[n].value)
#define PG_GET_COLLATION()  (fcinfo->fncollation)
#define PG_RETURN_BOOL(x)   return BoolGetDatum(x)
#define PG_RETURN_INT32(x)  return Int32GetDatum(x)
#define PG_FREE_IF_COPY(ptr, n) ((void) 0)	/* shim: inputs never copies */

static inline Datum
FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	LOCAL_FCINFO(fcinfo, 2);
	Datum		result;

	InitFunctionCallInfoData(*fcinfo, flinfo, 2, collation, NULL, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;
	result = FunctionCallInvoke(fcinfo);
	return result;
}

static inline void check_stack_depth(void) {}	/* shim: no-op */

/* ---------------- tupmacs.h static inlines (verbatim) ---------------- */

static inline Datum
fetch_att(const void *T, bool attbyval, int attlen)
{
	if (attbyval)
	{
		switch (attlen)
		{
			case 1:
				return Int8GetDatum(*((const int8 *) T));
			case 2:
				return Int16GetDatum(*((const int16 *) T));
			case 4:
				return Int32GetDatum(*((const int32 *) T));
			case 8:
				return Int64GetDatum(*((const int64 *) T));
			default:
				/* elog(ERROR, "unsupported byval length") */
				PROOF_ELOG_ABORT((Datum) 0);
		}
	}
	else
		return PointerGetDatum(T);
}

#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == TYPALIGN_INT) ? INTALIGN(cur_offset) : \
	 (((attalign) == TYPALIGN_CHAR) ? (uintptr_t) (cur_offset) : \
	  (((attalign) == TYPALIGN_DOUBLE) ? DOUBLEALIGN(cur_offset) : \
	   ( \
			AssertMacro((attalign) == TYPALIGN_SHORT), \
			SHORTALIGN(cur_offset) \
	   ))) \
)

#define att_align_pointer(cur_offset, attalign, attlen, attptr) \
( \
	((attlen) == -1 && VARATT_NOT_PAD_BYTE(attptr)) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)

#define att_align_datum(cur_offset, attalign, attlen, attdatum) \
( \
	((attlen) == -1 && VARATT_IS_SHORT(DatumGetPointer(attdatum))) ? \
	(uintptr_t) (cur_offset) : \
	att_align_nominal(cur_offset, attalign) \
)

#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)

#define att_addlength_datum(cur_offset, attlen, attdatum) \
	att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))

static inline void
store_att_byval(void *T, Datum newdatum, int attlen)
{
	switch (attlen)
	{
		case 1:
			*(int8 *) T = (int8) newdatum;
			break;
		case 2:
			*(int16 *) T = (int16) newdatum;
			break;
		case 4:
			*(int32 *) T = (int32) newdatum;
			break;
		case 8:
			*(int64 *) T = (int64) newdatum;
			break;
		default:
			/* elog(ERROR, "unsupported byval length") */
			pg_proof_err = 2;
	}
}

/* ---------------- typcache seam (THE stub under test) ---------------- */

typedef struct TypeCacheEntry
{
	Oid			type_id;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	FmgrInfo	eq_opr_finfo;
	FmgrInfo	cmp_proc_finfo;
	/* range-type fields */
	struct TypeCacheEntry *rngelemtype;
	Oid			rng_collation;
	FmgrInfo	rng_cmp_proc_finfo;
	FmgrInfo	rng_canonical_finfo;
} TypeCacheEntry;

#define TYPECACHE_EQ_OPR_FINFO    0x0008
#define TYPECACHE_CMP_PROC_FINFO  0x0080

static Datum pg_int4eq_fn(PG_FUNCTION_ARGS);
static Datum pg_btint4cmp_fn(PG_FUNCTION_ARGS);
static Datum pg_int4range_canonical_fn(PG_FUNCTION_ARGS);
static Datum pg_int8eq_fn(PG_FUNCTION_ARGS);
static Datum pg_btint8cmp_fn(PG_FUNCTION_ARGS);
static Datum pg_date_eq_fn(PG_FUNCTION_ARGS);
static Datum pg_date_cmp_fn(PG_FUNCTION_ARGS);
static Datum pg_uuid_eq_fn(PG_FUNCTION_ARGS);
static Datum pg_uuid_cmp_fn(PG_FUNCTION_ARGS);
static Datum pg_texteq_fn(PG_FUNCTION_ARGS);
static Datum pg_bttextcmp_fn(PG_FUNCTION_ARGS);

#define INT4OID 23
#define INT8OID 20
#define TEXTOID 25
#define DATEOID 1082
#define UUIDOID 2950
#define INT4RANGEOID 3904
#define INT8RANGEOID 3926
#define DATERANGEOID 3912
#define F_INT4EQ 65
#define F_BTINT4CMP 351
#define F_INT4RANGE_CANONICAL 3914
#define F_INT8EQ 467
#define F_BTINT8CMP 842
#define F_DATE_EQ 1086
#define F_DATE_CMP 1092
#define F_UUID_EQ 2956
#define F_UUID_CMP 2960
#define F_TEXTEQ 67
#define F_BTTEXTCMP 360

/* Concrete element instantiations — each mirrored EXACTLY by the Rust
 * harness's ElemMeta/ElemInfo (pg_type.dat attributes). */
static TypeCacheEntry pg_int4_typentry = {
	INT4OID, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN,
	{pg_int4eq_fn, F_INT4EQ, NULL},
	{pg_btint4cmp_fn, F_BTINT4CMP, NULL},
	NULL, InvalidOid, {NULL, InvalidOid, NULL}, {NULL, InvalidOid, NULL}
};

static TypeCacheEntry pg_int8_typentry = {
	INT8OID, 8, true, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN,
	{pg_int8eq_fn, F_INT8EQ, NULL},
	{pg_btint8cmp_fn, F_BTINT8CMP, NULL},
	NULL, InvalidOid, {NULL, InvalidOid, NULL}, {NULL, InvalidOid, NULL}
};

static TypeCacheEntry pg_date_typentry = {
	DATEOID, 4, true, TYPALIGN_INT, TYPSTORAGE_PLAIN,
	{pg_date_eq_fn, F_DATE_EQ, NULL},
	{pg_date_cmp_fn, F_DATE_CMP, NULL},
	NULL, InvalidOid, {NULL, InvalidOid, NULL}, {NULL, InvalidOid, NULL}
};

static TypeCacheEntry pg_uuid_typentry = {
	UUIDOID, 16, false, TYPALIGN_CHAR, TYPSTORAGE_PLAIN,
	{pg_uuid_eq_fn, F_UUID_EQ, NULL},
	{pg_uuid_cmp_fn, F_UUID_CMP, NULL},
	NULL, InvalidOid, {NULL, InvalidOid, NULL}, {NULL, InvalidOid, NULL}
};

static TypeCacheEntry pg_text_typentry = {
	TEXTOID, -1, false, TYPALIGN_INT, 'x',
	{pg_texteq_fn, F_TEXTEQ, NULL},
	{pg_bttextcmp_fn, F_BTTEXTCMP, NULL},
	NULL, InvalidOid, {NULL, InvalidOid, NULL}, {NULL, InvalidOid, NULL}
};

/* int4range: subtype cmp = btint4cmp, canonical = int4range_canonical */
static TypeCacheEntry pg_int4range_typentry = {
	INT4RANGEOID, -1, false, TYPALIGN_INT, 'x',
	{NULL, InvalidOid, NULL},
	{NULL, InvalidOid, NULL},
	&pg_int4_typentry, InvalidOid,
	{pg_btint4cmp_fn, F_BTINT4CMP, NULL},
	{pg_int4range_canonical_fn, F_INT4RANGE_CANONICAL, NULL}
};

/* int8range / daterange: canonical finfo INTENTIONALLY invalid on both
 * sides — consulted only by bounds_adjacent/make_range (range_adjacent),
 * which is out of scope for these instantiations (see header note 1). */
static TypeCacheEntry pg_int8range_typentry = {
	INT8RANGEOID, -1, false, TYPALIGN_DOUBLE, 'x',
	{NULL, InvalidOid, NULL},
	{NULL, InvalidOid, NULL},
	&pg_int8_typentry, InvalidOid,
	{pg_btint8cmp_fn, F_BTINT8CMP, NULL},
	{NULL, InvalidOid, NULL}
};

static TypeCacheEntry pg_daterange_typentry = {
	DATERANGEOID, -1, false, TYPALIGN_INT, 'x',
	{NULL, InvalidOid, NULL},
	{NULL, InvalidOid, NULL},
	&pg_date_typentry, InvalidOid,
	{pg_date_cmp_fn, F_DATE_CMP, NULL},
	{NULL, InvalidOid, NULL}
};

static TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
	/* seam stub: dispatch on the concrete instantiations under proof */
	(void) flags;
	switch (type_id)
	{
		case INT4OID:
			return &pg_int4_typentry;
		case INT8OID:
			return &pg_int8_typentry;
		case DATEOID:
			return &pg_date_typentry;
		case UUIDOID:
			return &pg_uuid_typentry;
		case TEXTOID:
			return &pg_text_typentry;
		default:
			pg_proof_err = 3;
			return &pg_int4_typentry;
	}
}

static TypeCacheEntry *
range_get_typcache(FunctionCallInfo fcinfo, Oid rngtypid)
{
	(void) fcinfo;
	switch (rngtypid)
	{
		case INT4RANGEOID:
			return &pg_int4range_typentry;
		case INT8RANGEOID:
			return &pg_int8range_typentry;
		case DATERANGEOID:
			return &pg_daterange_typentry;
		default:
			pg_proof_err = 3;
			return &pg_int4range_typentry;
	}
}

/* ---------------- element functions (verbatim bodies) ---------------- */

/* nbtcompare.c A_LESS_THAN_B / A_GREATER_THAN_B */
#define A_LESS_THAN_B    (-1)
#define A_GREATER_THAN_B 1

/* int.c int4eq, verbatim */
static Datum
pg_int4eq_fn(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

/* nbtcompare.c btint4cmp, verbatim */
static Datum
pg_btint4cmp_fn(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int32		b = PG_GETARG_INT32(1);

	if (a > b)
		PG_RETURN_INT32(A_GREATER_THAN_B);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(A_LESS_THAN_B);
}

/* ---- int8 (fmgr arg macros are plumbing shims, bodies verbatim) ---- */

#define PG_GETARG_INT64(n) ((int64) fcinfo->args[n].value)

/* int8.c int8eq, verbatim */
static Datum
pg_int8eq_fn(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 == val2);
}

/* nbtcompare.c btint8cmp, verbatim */
static Datum
pg_btint8cmp_fn(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int64		b = PG_GETARG_INT64(1);

	if (a > b)
		PG_RETURN_INT32(A_GREATER_THAN_B);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(A_LESS_THAN_B);
}

/* ---- date (date.h DateADT = int32) ---- */

typedef int32 DateADT;

#define DatumGetDateADT(X)  ((DateADT) DatumGetInt32(X))
#define PG_GETARG_DATEADT(n) DatumGetDateADT(PG_GETARG_DATUM(n))

/* date.c date_eq, verbatim */
static Datum
pg_date_eq_fn(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 == dateVal2);
}

/* date.c date_cmp, verbatim */
static Datum
pg_date_cmp_fn(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	if (dateVal1 < dateVal2)
		PG_RETURN_INT32(-1);
	else if (dateVal1 > dateVal2)
		PG_RETURN_INT32(1);
	PG_RETURN_INT32(0);
}

/* ---- uuid (uuid.h pg_uuid_t) ---- */

#define UUID_LEN 16
typedef struct pg_uuid_t
{
	unsigned char data[UUID_LEN];
} pg_uuid_t;

#define PG_GETARG_UUID_P(n) ((pg_uuid_t *) DatumGetPointer(PG_GETARG_DATUM(n)))

/* uuid.c uuid_internal_cmp, verbatim */
static int
pg_uuid_internal_cmp(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return memcmp(arg1->data, arg2->data, UUID_LEN);
}

/* uuid.c uuid_eq, verbatim */
static Datum
pg_uuid_eq_fn(PG_FUNCTION_ARGS)
{
	pg_uuid_t  *arg1 = PG_GETARG_UUID_P(0);
	pg_uuid_t  *arg2 = PG_GETARG_UUID_P(1);

	PG_RETURN_BOOL(pg_uuid_internal_cmp(arg1, arg2) == 0);
}

/* uuid.c uuid_cmp, verbatim */
static Datum
pg_uuid_cmp_fn(PG_FUNCTION_ARGS)
{
	pg_uuid_t  *arg1 = PG_GETARG_UUID_P(0);
	pg_uuid_t  *arg2 = PG_GETARG_UUID_P(1);

	PG_RETURN_INT32(pg_uuid_internal_cmp(arg1, arg2));
}

/* ---- text (varlena.c; collation fence shims per header note 1b) ---- */

typedef struct varlena
{
	int32		vl_len_;
} text;

#define C_COLLATION_OID 950		/* catalog/pg_collation.h */
#define POSIX_COLLATION_OID 951
#define POISON_CMP (-2147483647)	/* out-of-fence sentinel */

/* shim 1b: pg_newlocale_from_collation field models (text-cmp precedent) */
static int
pg_collate_is_c(Oid collid)
{
	return collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID;
}

static int
pg_collate_deterministic(Oid collid)
{
	/* C/POSIX are deterministic; anything else is out of the fence. */
	return pg_collate_is_c(collid);
}

/* shim 1b: check_collation_set -> err-flag guard */
static void
pg_check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
		pg_proof_err = 2;		/* ereport(indeterminate collation) */
}

/* varlena.c varstr_cmp (C-collation arm verbatim; locale arm poisoned) */
static int
pg_varstr_cmp(const char *arg1, int len1, const char *arg2, int len2,
			  Oid collid)
{
	int			result;

	pg_check_collation_set(collid);

	if (pg_collate_is_c(collid))
	{
		result = memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/* pg_strncoll / locale path: out of the collation fence */
		pg_proof_err = 5;
		result = POISON_CMP;
	}

	return result;
}

/* varlena.c text_cmp, verbatim (PG_DETOAST inputs are the caller contract) */
static int
pg_text_cmp(text *arg1, text *arg2, Oid collid)
{
	char	   *a1p,
			   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	return pg_varstr_cmp(a1p, len1, a2p, len2, collid);
}

/* varlena.c texteq, verbatim (toast_raw_datum_size ->
 * VARSIZE_ANY_EXHDR + VARHDRSZ, identical for never-toasted proof images;
 * DatumGetTextPP -> flat pointer: pre-detoast caller contract) */
static Datum
pg_texteq_fn(PG_FUNCTION_ARGS)
{
	Oid			collid = PG_GET_COLLATION();
	bool		result;

	pg_check_collation_set(collid);

	if (pg_collate_deterministic(collid))
	{
		Datum		arg1 = PG_GETARG_DATUM(0);
		Datum		arg2 = PG_GETARG_DATUM(1);
		Size		len1,
					len2;

		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.  In
		 * fact, we don't even have to do a bitwise comparison if we can show
		 * the lengths of the strings are unequal; which might save us from
		 * having to detoast one or both values.
		 */
		len1 = (Size) VARSIZE_ANY_EXHDR(DatumGetPointer(arg1)) + VARHDRSZ;
		len2 = (Size) VARSIZE_ANY_EXHDR(DatumGetPointer(arg2)) + VARHDRSZ;
		if (len1 != len2)
			result = false;
		else
		{
			text	   *targ1 = (text *) DatumGetPointer(arg1);
			text	   *targ2 = (text *) DatumGetPointer(arg2);

			result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
							 len1 - VARHDRSZ) == 0);

			PG_FREE_IF_COPY(targ1, 0);
			PG_FREE_IF_COPY(targ2, 1);
		}
	}
	else
	{
		/* nondeterministic-collation arm: out of the collation fence */
		pg_proof_err = 5;
		result = false;
	}

	PG_RETURN_BOOL(result);
}

/* varlena.c bttextcmp, verbatim */
static Datum
pg_bttextcmp_fn(PG_FUNCTION_ARGS)
{
	text	   *arg1 = (text *) DatumGetPointer(PG_GETARG_DATUM(0));
	text	   *arg2 = (text *) DatumGetPointer(PG_GETARG_DATUM(1));
	int32		result;

	result = pg_text_cmp(arg1, arg2, PG_GET_COLLATION());

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_INT32(result);
}

/* ================= ARRAYS ================= */

/* array.h ArrayType + macros, verbatim semantics */
typedef struct ArrayType
{
	int32		vl_len_;
	int			ndim;
	int32		dataoffset;
	Oid			elemtype;
} ArrayType;

/* minimal ExpandedArrayHeader: fields the verbatim AARR_/array_iter code
 * mentions; the expanded arm is dead (4B-header images are never expanded)
 * but must compile. vl_len_ would be EOH_HEADER_MAGIC (-1) for a real one. */
typedef struct ExpandedArrayHeader
{
	int32		vl_len_;
	int			ndims;
	int		   *dims;
	int		   *lbound;
	Oid			element_type;
	Datum	   *dvalues;
	bool	   *dnulls;
	ArrayType  *fvalue;
} ExpandedArrayHeader;

typedef union AnyArrayType
{
	ArrayType	flt;
	ExpandedArrayHeader xpn;
} AnyArrayType;

#define EOH_HEADER_MAGIC (-1)
#define VARATT_IS_EXPANDED_HEADER(PTR) \
	(((const ExpandedArrayHeader *) (PTR))->vl_len_ == EOH_HEADER_MAGIC)

#define ARR_SIZE(a)     VARSIZE(a)
#define ARR_NDIM(a)     ((a)->ndim)
#define ARR_HASNULL(a)  ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a) ((a)->elemtype)
#define ARR_DIMS(a) \
	((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
	((int *) (((char *) (a)) + sizeof(ArrayType) + \
			  sizeof(int) * ARR_NDIM(a)))
#define ARR_NULLBITMAP(a) \
	(ARR_HASNULL(a) ? \
	 (bits8 *) (((char *) (a)) + sizeof(ArrayType) + \
				2 * sizeof(int) * ARR_NDIM(a)) \
	 : (bits8 *) NULL)
#define ARR_OVERHEAD_NONULLS(ndims) \
	MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_DATA_OFFSET(a) \
	(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
#define ARR_DATA_PTR(a) \
	(((char *) (a)) + ARR_DATA_OFFSET(a))

#define AARR_NDIM(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 (a)->xpn.ndims : ARR_NDIM(&(a)->flt))
#define AARR_HASNULL(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 ((a)->xpn.dvalues != NULL ? ((a)->xpn.dnulls != NULL) : ARR_HASNULL((a)->xpn.fvalue)) : \
	 ARR_HASNULL(&(a)->flt))
#define AARR_ELEMTYPE(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 (a)->xpn.element_type : ARR_ELEMTYPE(&(a)->flt))
#define AARR_DIMS(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 (a)->xpn.dims : ARR_DIMS(&(a)->flt))
#define AARR_LBOUND(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 (a)->xpn.lbound : ARR_LBOUND(&(a)->flt))
/* shim: no-op (inputs never copies, never expanded) */
#define AARR_FREE_IF_COPY(array, n) ((void) 0)

/* shim: pre-detoasted flat pointer straight from the datum */
#define PG_GETARG_ANY_ARRAY_P(n) ((AnyArrayType *) DatumGetPointer(PG_GETARG_DATUM(n)))

/* arrayutils.c MaxArraySize context: MaxAllocSize/sizeof(Datum) */
#define MaxArraySize ((Size) (0x3fffffff / 8))

/* arrayutils.c ArrayGetNItemsSafe, verbatim (ereturn -> proof_ereturn;
 * escontext is always NULL here so the flag means hard error) */
static int
ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
{
	int32		ret;
	int			i;

	if (ndim <= 0)
		return 0;
	ret = 1;
	for (i = 0; i < ndim; i++)
	{
		int64		prod;

		/* A negative dimension implies that UB-LB overflowed ... */
		if (dims[i] < 0)
			proof_ereturn(-1);

		prod = (int64) ret * (int64) dims[i];

		ret = (int32) prod;
		if ((int64) ret != prod)
			proof_ereturn(-1);
	}
	Assert(ret >= 0);
	if ((Size) ret > MaxArraySize)
		proof_ereturn(-1);
	return (int) ret;
}

static int
ArrayGetNItems(int ndim, const int *dims)
{
	return ArrayGetNItemsSafe(ndim, dims, NULL);
}

/* arrayaccess.h array_iter, verbatim */
typedef struct array_iter
{
	Datum	   *datumptr;
	bool	   *isnullptr;
	char	   *dataptr;
	bits8	   *bitmapptr;
	int			bitmask;
} array_iter;

static inline void
array_iter_setup(array_iter *it, AnyArrayType *a)
{
	if (VARATT_IS_EXPANDED_HEADER(a))
	{
		if (a->xpn.dvalues)
		{
			it->datumptr = a->xpn.dvalues;
			it->isnullptr = a->xpn.dnulls;
			it->dataptr = NULL;
			it->bitmapptr = NULL;
		}
		else
		{
			it->datumptr = NULL;
			it->isnullptr = NULL;
			it->dataptr = ARR_DATA_PTR(a->xpn.fvalue);
			it->bitmapptr = ARR_NULLBITMAP(a->xpn.fvalue);
		}
	}
	else
	{
		it->datumptr = NULL;
		it->isnullptr = NULL;
		it->dataptr = ARR_DATA_PTR((ArrayType *) a);
		it->bitmapptr = ARR_NULLBITMAP((ArrayType *) a);
	}
	it->bitmask = 1;
}

static inline Datum
array_iter_next(array_iter *it, bool *isnull, int i,
				int elmlen, bool elmbyval, char elmalign)
{
	Datum		ret;

	if (it->datumptr)
	{
		ret = it->datumptr[i];
		*isnull = it->isnullptr ? it->isnullptr[i] : false;
	}
	else
	{
		if (it->bitmapptr && (*(it->bitmapptr) & it->bitmask) == 0)
		{
			*isnull = true;
			ret = (Datum) 0;
		}
		else
		{
			*isnull = false;
			ret = fetch_att(it->dataptr, elmbyval, elmlen);
			it->dataptr = att_addlength_pointer(it->dataptr, elmlen,
												it->dataptr);
			it->dataptr = (char *) att_align_nominal(it->dataptr, elmalign);
		}
		it->bitmask <<= 1;
		if (it->bitmask == 0x100)
		{
			if (it->bitmapptr)
				it->bitmapptr++;
			it->bitmask = 1;
		}
	}

	return ret;
}

/* arrayfuncs.c array_eq, verbatim (ereport -> PROOF_ELOG_ABORT; typcache /
 * format_type message plumbing removed with it — see header notes) */
static Datum
array_eq(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(locfcinfo, 2);
	AnyArrayType *array1 = PG_GETARG_ANY_ARRAY_P(0);
	AnyArrayType *array2 = PG_GETARG_ANY_ARRAY_P(1);
	Oid			collation = PG_GET_COLLATION();
	int			ndims1 = AARR_NDIM(array1);
	int			ndims2 = AARR_NDIM(array2);
	int		   *dims1 = AARR_DIMS(array1);
	int		   *dims2 = AARR_DIMS(array2);
	int		   *lbs1 = AARR_LBOUND(array1);
	int		   *lbs2 = AARR_LBOUND(array2);
	Oid			element_type = AARR_ELEMTYPE(array1);
	bool		result = true;
	int			nitems;
	TypeCacheEntry *typentry;
	int			typlen;
	bool		typbyval;
	char		typalign;
	array_iter	it1;
	array_iter	it2;
	int			i;

	if (element_type != AARR_ELEMTYPE(array2))
		PROOF_ELOG_ABORT(BoolGetDatum(false));

	/* fast path if the arrays do not have the same dimensionality */
	if (ndims1 != ndims2 ||
		memcmp(dims1, dims2, ndims1 * sizeof(int)) != 0 ||
		memcmp(lbs1, lbs2, ndims1 * sizeof(int)) != 0)
		result = false;
	else
	{
		typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
		if (typentry == NULL ||
			typentry->type_id != element_type)
		{
			typentry = lookup_type_cache(element_type,
										 TYPECACHE_EQ_OPR_FINFO);
			if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
				PROOF_ELOG_ABORT(BoolGetDatum(false));
			fcinfo->flinfo->fn_extra = typentry;
		}
		typlen = typentry->typlen;
		typbyval = typentry->typbyval;
		typalign = typentry->typalign;

		/*
		 * apply the operator to each pair of array elements.
		 */
		InitFunctionCallInfoData(*locfcinfo, &typentry->eq_opr_finfo, 2,
								 collation, NULL, NULL);

		/* Loop over source data */
		nitems = ArrayGetNItems(ndims1, dims1);
		array_iter_setup(&it1, array1);
		array_iter_setup(&it2, array2);

		for (i = 0; i < nitems; i++)
		{
			Datum		elt1;
			Datum		elt2;
			bool		isnull1;
			bool		isnull2;
			bool		oprresult;

			/* Get elements, checking for NULL */
			elt1 = array_iter_next(&it1, &isnull1, i,
								   typlen, typbyval, typalign);
			elt2 = array_iter_next(&it2, &isnull2, i,
								   typlen, typbyval, typalign);

			/*
			 * We consider two NULLs equal; NULL and not-NULL are unequal.
			 */
			if (isnull1 && isnull2)
				continue;
			if (isnull1 || isnull2)
			{
				result = false;
				break;
			}

			/*
			 * Apply the operator to the element pair; treat NULL as false
			 */
			locfcinfo->args[0].value = elt1;
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = elt2;
			locfcinfo->args[1].isnull = false;
			locfcinfo->isnull = false;
			oprresult = DatumGetBool(FunctionCallInvoke(locfcinfo));
			if (locfcinfo->isnull || !oprresult)
			{
				result = false;
				break;
			}
		}
	}

	/* Avoid leaking memory when handed toasted input. */
	AARR_FREE_IF_COPY(array1, 0);
	AARR_FREE_IF_COPY(array2, 1);

	PG_RETURN_BOOL(result);
}

static Datum
array_ne(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(!DatumGetBool(array_eq(fcinfo)));
}

/* arrayfuncs.c array_cmp, verbatim (same shims as array_eq) */
static int
array_cmp(FunctionCallInfo fcinfo)
{
	LOCAL_FCINFO(locfcinfo, 2);
	AnyArrayType *array1 = PG_GETARG_ANY_ARRAY_P(0);
	AnyArrayType *array2 = PG_GETARG_ANY_ARRAY_P(1);
	Oid			collation = PG_GET_COLLATION();
	int			ndims1 = AARR_NDIM(array1);
	int			ndims2 = AARR_NDIM(array2);
	int		   *dims1 = AARR_DIMS(array1);
	int		   *dims2 = AARR_DIMS(array2);
	int			nitems1 = ArrayGetNItems(ndims1, dims1);
	int			nitems2 = ArrayGetNItems(ndims2, dims2);
	Oid			element_type = AARR_ELEMTYPE(array1);
	int			result = 0;
	TypeCacheEntry *typentry;
	int			typlen;
	bool		typbyval;
	char		typalign;
	int			min_nitems;
	array_iter	it1;
	array_iter	it2;
	int			i;

	if (element_type != AARR_ELEMTYPE(array2))
		PROOF_ELOG_ABORT(-2);

	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL ||
		typentry->type_id != element_type)
	{
		typentry = lookup_type_cache(element_type,
									 TYPECACHE_CMP_PROC_FINFO);
		if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
			PROOF_ELOG_ABORT(-2);
		fcinfo->flinfo->fn_extra = typentry;
	}
	typlen = typentry->typlen;
	typbyval = typentry->typbyval;
	typalign = typentry->typalign;

	/*
	 * apply the operator to each pair of array elements.
	 */
	InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
							 collation, NULL, NULL);

	/* Loop over source data */
	min_nitems = Min(nitems1, nitems2);
	array_iter_setup(&it1, array1);
	array_iter_setup(&it2, array2);

	for (i = 0; i < min_nitems; i++)
	{
		Datum		elt1;
		Datum		elt2;
		bool		isnull1;
		bool		isnull2;
		int32		cmpresult;

		/* Get elements, checking for NULL */
		elt1 = array_iter_next(&it1, &isnull1, i, typlen, typbyval, typalign);
		elt2 = array_iter_next(&it2, &isnull2, i, typlen, typbyval, typalign);

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (isnull1 && isnull2)
			continue;
		if (isnull1)
		{
			/* arg1 is greater than arg2 */
			result = 1;
			break;
		}
		if (isnull2)
		{
			/* arg1 is less than arg2 */
			result = -1;
			break;
		}

		/* Compare the pair of elements */
		locfcinfo->args[0].value = elt1;
		locfcinfo->args[0].isnull = false;
		locfcinfo->args[1].value = elt2;
		locfcinfo->args[1].isnull = false;
		locfcinfo->isnull = false;
		cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

		/* We don't expect comparison support functions to return null */
		Assert(!locfcinfo->isnull);

		if (cmpresult == 0)
			continue;			/* equal */

		if (cmpresult < 0)
		{
			/* arg1 is less than arg2 */
			result = -1;
			break;
		}
		else
		{
			/* arg1 is greater than arg2 */
			result = 1;
			break;
		}
	}

	/*
	 * If arrays contain same data (up to end of shorter one), apply
	 * additional rules to sort by dimensionality.
	 */
	if (result == 0)
	{
		if (nitems1 != nitems2)
			result = (nitems1 < nitems2) ? -1 : 1;
		else if (ndims1 != ndims2)
			result = (ndims1 < ndims2) ? -1 : 1;
		else
		{
			for (i = 0; i < ndims1; i++)
			{
				if (dims1[i] != dims2[i])
				{
					result = (dims1[i] < dims2[i]) ? -1 : 1;
					break;
				}
			}
			if (result == 0)
			{
				int		   *lbound1 = AARR_LBOUND(array1);
				int		   *lbound2 = AARR_LBOUND(array2);

				for (i = 0; i < ndims1; i++)
				{
					if (lbound1[i] != lbound2[i])
					{
						result = (lbound1[i] < lbound2[i]) ? -1 : 1;
						break;
					}
				}
			}
		}
	}

	/* Avoid leaking memory when handed toasted input. */
	AARR_FREE_IF_COPY(array1, 0);
	AARR_FREE_IF_COPY(array2, 1);

	return result;
}

/* arrayfuncs.c comparison wrappers, verbatim */
static Datum
array_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(array_cmp(fcinfo) < 0);
}

static Datum
array_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(array_cmp(fcinfo) > 0);
}

static Datum
array_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(array_cmp(fcinfo) <= 0);
}

static Datum
array_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(array_cmp(fcinfo) >= 0);
}

static Datum
btarraycmp(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(array_cmp(fcinfo));
}

/* ---- exported plain-signature entries (fmgr unwrapping shim) ---- */

static Datum
call_array2(Datum (*fn) (FunctionCallInfo), const void *a1, const void *a2,
			Oid collation)
{
	LOCAL_FCINFO(fcinfo, 2);
	FmgrInfo	flinfo;

	flinfo.fn_addr = NULL;
	flinfo.fn_oid = InvalidOid;
	flinfo.fn_extra = NULL;
	InitFunctionCallInfoData(*fcinfo, &flinfo, 2, collation, NULL, NULL);
	fcinfo->args[0].value = PointerGetDatum(a1);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = PointerGetDatum(a2);
	fcinfo->args[1].isnull = false;
	return fn(fcinfo);
}

int pg_c_array_eq(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_eq, a1, a2, collation)); }
int pg_c_array_ne(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_ne, a1, a2, collation)); }
int pg_c_array_lt(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_lt, a1, a2, collation)); }
int pg_c_array_gt(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_gt, a1, a2, collation)); }
int pg_c_array_le(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_le, a1, a2, collation)); }
int pg_c_array_ge(const void *a1, const void *a2, Oid collation)
{ return (int) DatumGetBool(call_array2(array_ge, a1, a2, collation)); }
int32 pg_c_btarraycmp(const void *a1, const void *a2, Oid collation)
{ return DatumGetInt32(call_array2(btarraycmp, a1, a2, collation)); }

/* ================= RANGES ================= */

/* rangetypes.h RangeType + macros, verbatim semantics */
typedef struct RangeType
{
	int32		vl_len_;
	Oid			rangetypid;
} RangeType;

typedef struct RangeBound
{
	Datum		val;
	bool		infinite;
	bool		inclusive;
	bool		lower;
} RangeBound;

#define RANGE_EMPTY		0x01
#define RANGE_LB_INC	0x02
#define RANGE_UB_INC	0x04
#define RANGE_LB_INF	0x08
#define RANGE_UB_INF	0x10
#define RANGE_LB_NULL	0x20
#define RANGE_UB_NULL	0x40
#define RANGE_CONTAIN_EMPTY 0x80

#define RANGE_HAS_LBOUND(flags) (!((flags) & (RANGE_EMPTY | \
											  RANGE_LB_NULL | \
											  RANGE_LB_INF)))
#define RANGE_HAS_UBOUND(flags) (!((flags) & (RANGE_EMPTY | \
											  RANGE_UB_NULL | \
											  RANGE_UB_INF)))

#define RangeTypeGetOid(r) ((r)->rangetypid)
/* NULL guard is a shim: reachable ONLY on the flagged error path, where the
 * C value is dead (harness compares err flags; see header note 3). */
#define RangeIsEmpty(r) ((r) == NULL ? false : \
	((range_get_flags(r) & RANGE_EMPTY) != 0))

#define PG_GETARG_RANGE_P(n) ((RangeType *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_RETURN_RANGE_P(x) return PointerGetDatum(x)
#define RangeTypePGetDatum(x) PointerGetDatum(x)
#define DatumGetRangeTypeP(x) ((RangeType *) DatumGetPointer(x))

/* palloc0 shim: two-slot rotating static buffer (header note 4) */
#define PG_PALLOC_SLOT_SIZE 24
static char pg_palloc_slots[2][PG_PALLOC_SLOT_SIZE];
static int pg_palloc_next = 0;

static void *
palloc0(Size size)
{
	char	   *p;

	if (size > PG_PALLOC_SLOT_SIZE)
		pg_proof_err = 4;
	p = pg_palloc_slots[pg_palloc_next];
	pg_palloc_next = (pg_palloc_next + 1) & 1;
	memset(p, 0, PG_PALLOC_SLOT_SIZE);
	return p;
}

/* rangetypes.c range_get_flags, verbatim */
static char
range_get_flags(const RangeType *range)
{
	/* fetch the flag byte from datum's last byte */
	return *((const char *) range + VARSIZE(range) - 1);
}

/* rangetypes.c range_deserialize, verbatim */
static void
range_deserialize(TypeCacheEntry *typcache, const RangeType *range,
				  RangeBound *lower, RangeBound *upper, bool *empty)
{
	char		flags;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Pointer		ptr;
	Datum		lbound;
	Datum		ubound;

	/* assert caller passed the right typcache entry */
	Assert(RangeTypeGetOid(range) == typcache->type_id);

	/* fetch the flag byte from datum's last byte */
	flags = *((const char *) range + VARSIZE(range) - 1);

	/* fetch information about range's element type */
	typlen = typcache->rngelemtype->typlen;
	typbyval = typcache->rngelemtype->typbyval;
	typalign = typcache->rngelemtype->typalign;

	/* initialize data pointer just after the range OID */
	ptr = (Pointer) (range + 1);

	/* fetch lower bound, if any */
	if (RANGE_HAS_LBOUND(flags))
	{
		/* att_align_pointer cannot be necessary here */
		lbound = fetch_att(ptr, typbyval, typlen);
		ptr = (Pointer) att_addlength_pointer(ptr, typlen, ptr);
	}
	else
		lbound = (Datum) 0;

	/* fetch upper bound, if any */
	if (RANGE_HAS_UBOUND(flags))
	{
		ptr = (Pointer) att_align_pointer(ptr, typalign, typlen, ptr);
		ubound = fetch_att(ptr, typbyval, typlen);
		/* no need for att_addlength_pointer */
	}
	else
		ubound = (Datum) 0;

	/* emit results */

	*empty = (flags & RANGE_EMPTY) != 0;

	lower->val = lbound;
	lower->infinite = (flags & RANGE_LB_INF) != 0;
	lower->inclusive = (flags & RANGE_LB_INC) != 0;
	lower->lower = true;

	upper->val = ubound;
	upper->infinite = (flags & RANGE_UB_INF) != 0;
	upper->inclusive = (flags & RANGE_UB_INC) != 0;
	upper->lower = false;
}

/* rangetypes.c range_cmp_bounds, verbatim */
static int
range_cmp_bounds(TypeCacheEntry *typcache, const RangeBound *b1,
				 const RangeBound *b2)
{
	int32		result;

	if (b1->infinite && b2->infinite)
	{
		if (b1->lower == b2->lower)
			return 0;
		else
			return b1->lower ? -1 : 1;
	}
	else if (b1->infinite)
		return b1->lower ? -1 : 1;
	else if (b2->infinite)
		return b2->lower ? 1 : -1;

	result = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
											 typcache->rng_collation,
											 b1->val, b2->val));

	if (result == 0)
	{
		if (!b1->inclusive && !b2->inclusive)
		{
			/* both are exclusive */
			if (b1->lower == b2->lower)
				return 0;
			else
				return b1->lower ? 1 : -1;
		}
		else if (!b1->inclusive)
			return b1->lower ? 1 : -1;
		else if (!b2->inclusive)
			return b2->lower ? -1 : 1;
		else
			return 0;
	}

	return result;
}

/* rangetypes.c range_cmp_bound_values, verbatim */
static int
range_cmp_bound_values(TypeCacheEntry *typcache, const RangeBound *b1,
					   const RangeBound *b2)
{
	if (b1->infinite && b2->infinite)
	{
		if (b1->lower == b2->lower)
			return 0;
		else
			return b1->lower ? -1 : 1;
	}
	else if (b1->infinite)
		return b1->lower ? -1 : 1;
	else if (b2->infinite)
		return b2->lower ? 1 : -1;

	return DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
										   typcache->rng_collation,
										   b1->val, b2->val));
}

/* rangetypes.c datum_compute_size, verbatim */
static Size
datum_compute_size(Size data_length, Datum val, bool typbyval, char typalign,
				   int16 typlen, char typstorage)
{
	if (TYPE_IS_PACKABLE(typlen, typstorage) &&
		VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
	{
		data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
	}
	else
	{
		data_length = att_align_datum(data_length, typalign, typlen, val);
		data_length = att_addlength_datum(data_length, typlen, val);
	}

	return data_length;
}

/* rangetypes.c datum_write, verbatim (elog -> PROOF_ELOG_ABORT) */
static Pointer
datum_write(Pointer ptr, Datum datum, bool typbyval, char typalign,
			int16 typlen, char typstorage)
{
	Size		data_length;

	if (typbyval)
	{
		/* pass-by-value */
		ptr = (char *) att_align_nominal(ptr, typalign);
		store_att_byval(ptr, datum, typlen);
		data_length = typlen;
	}
	else if (typlen == -1)
	{
		/* varlena */
		Pointer		val = DatumGetPointer(datum);

		if (VARATT_IS_EXTERNAL(val))
		{
			/* elog(ERROR, "cannot store a toast pointer inside a range") */
			PROOF_ELOG_ABORT(ptr);
		}
		else if (VARATT_IS_SHORT(val))
		{
			/* no alignment for short varlenas */
			data_length = VARSIZE_SHORT(val);
			memcpy(ptr, val, data_length);
		}
		else if (TYPE_IS_PACKABLE(typlen, typstorage) &&
				 VARATT_CAN_MAKE_SHORT(val))
		{
			/* convert to short varlena -- no alignment */
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			SET_VARSIZE_SHORT(ptr, data_length);
			memcpy(ptr + 1, VARDATA(val), data_length - 1);
		}
		else
		{
			/* full 4-byte header varlena */
			ptr = (char *) att_align_nominal(ptr, typalign);
			data_length = VARSIZE(val);
			memcpy(ptr, val, data_length);
		}
	}
	else if (typlen == -2)
	{
		/* cstring ... never needs alignment */
		Assert(typalign == TYPALIGN_CHAR);
		data_length = strlen(DatumGetCString(datum)) + 1;
		memcpy(ptr, DatumGetPointer(datum), data_length);
	}
	else
	{
		/* fixed-length pass-by-reference */
		ptr = (char *) att_align_nominal(ptr, typalign);
		Assert(typlen > 0);
		data_length = typlen;
		memcpy(ptr, DatumGetPointer(datum), data_length);
	}

	ptr += data_length;

	return ptr;
}

/* rangetypes.c range_serialize, verbatim (ereturn -> proof_ereturn,
 * palloc0 -> slot shim, PG_DETOAST_DATUM_PACKED unreachable for byval int4
 * and omitted: typlen==-1 arm never runs in this instantiation) */
static RangeType *
range_serialize(TypeCacheEntry *typcache, RangeBound *lower, RangeBound *upper,
				bool empty, struct Node *escontext)
{
	RangeType  *range;
	int			cmp;
	Size		msize;
	Pointer		ptr;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	char		flags = 0;

	Assert(lower->lower);
	Assert(!upper->lower);

	if (empty)
		flags |= RANGE_EMPTY;
	else
	{
		cmp = range_cmp_bound_values(typcache, lower, upper);

		/* error check: if lower bound value is above upper, it's wrong */
		if (cmp > 0)
			proof_ereturn(NULL);

		/* if bounds are equal, and not both inclusive, range is empty */
		if (cmp == 0 && !(lower->inclusive && upper->inclusive))
			flags |= RANGE_EMPTY;
		else
		{
			/* infinite boundaries are never inclusive */
			if (lower->infinite)
				flags |= RANGE_LB_INF;
			else if (lower->inclusive)
				flags |= RANGE_LB_INC;
			if (upper->infinite)
				flags |= RANGE_UB_INF;
			else if (upper->inclusive)
				flags |= RANGE_UB_INC;
		}
	}

	/* Fetch information about range's element type */
	typlen = typcache->rngelemtype->typlen;
	typbyval = typcache->rngelemtype->typbyval;
	typalign = typcache->rngelemtype->typalign;
	typstorage = typcache->rngelemtype->typstorage;

	/* Count space for varlena header and range type's OID */
	msize = sizeof(RangeType);
	Assert(msize == MAXALIGN(msize));

	/* Count space for bounds */
	if (RANGE_HAS_LBOUND(flags))
	{
		/* typlen==-1 detoast arm omitted: int4 instantiation is byval */
		msize = datum_compute_size(msize, lower->val, typbyval, typalign,
								   typlen, typstorage);
	}

	if (RANGE_HAS_UBOUND(flags))
	{
		msize = datum_compute_size(msize, upper->val, typbyval, typalign,
								   typlen, typstorage);
	}

	/* Add space for flag byte */
	msize += sizeof(char);

	/* Note: zero-fill is required here, just as in heap tuples */
	range = (RangeType *) palloc0(msize);
	SET_VARSIZE(range, msize);

	/* Now fill in the datum */
	range->rangetypid = typcache->type_id;

	ptr = (char *) (range + 1);

	if (RANGE_HAS_LBOUND(flags))
	{
		Assert(lower->lower);
		ptr = datum_write(ptr, lower->val, typbyval, typalign, typlen,
						  typstorage);
	}

	if (RANGE_HAS_UBOUND(flags))
	{
		Assert(!upper->lower);
		ptr = datum_write(ptr, upper->val, typbyval, typalign, typlen,
						  typstorage);
	}

	*((char *) ptr) = flags;

	return range;
}

/* rangetypes.c make_range, verbatim (elog -> PROOF_ELOG_ABORT) */
static RangeType *
make_range(TypeCacheEntry *typcache, RangeBound *lower, RangeBound *upper,
		   bool empty, struct Node *escontext)
{
	RangeType  *range;

	range = range_serialize(typcache, lower, upper, empty, escontext);

	if (SOFT_ERROR_OCCURRED(escontext))
		return NULL;

	/* no need to call canonical on empty ranges ... */
	if (OidIsValid(typcache->rng_canonical_finfo.fn_oid) &&
		!RangeIsEmpty(range))
	{
		/* Do this the hard way so that we can pass escontext */
		LOCAL_FCINFO(fcinfo, 1);
		Datum		result;

		InitFunctionCallInfoData(*fcinfo, &typcache->rng_canonical_finfo, 1,
								 InvalidOid, escontext, NULL);

		fcinfo->args[0].value = RangeTypePGetDatum(range);
		fcinfo->args[0].isnull = false;

		result = FunctionCallInvoke(fcinfo);

		if (SOFT_ERROR_OCCURRED(escontext))
			return NULL;

		/* Should not get a null result if there was no error */
		if (fcinfo->isnull)
			PROOF_ELOG_ABORT(NULL);

		range = DatumGetRangeTypeP(result);
	}

	return range;
}

/* rangetypes.c int4range_canonical, verbatim (ereturn -> proof_ereturn) */
static Datum
pg_int4range_canonical_fn(PG_FUNCTION_ARGS)
{
	RangeType  *r = PG_GETARG_RANGE_P(0);
	Node	   *escontext = fcinfo->context;
	TypeCacheEntry *typcache;
	RangeBound	lower;
	RangeBound	upper;
	bool		empty;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

	range_deserialize(typcache, r, &lower, &upper, &empty);

	if (empty)
		PG_RETURN_RANGE_P(r);

	if (!lower.infinite && !lower.inclusive)
	{
		int32		bnd = DatumGetInt32(lower.val);

		/* Handle possible overflow manually */
		if (unlikely(bnd == PG_INT32_MAX))
			proof_ereturn((Datum) 0);
		lower.val = Int32GetDatum(bnd + 1);
		lower.inclusive = true;
	}

	if (!upper.infinite && upper.inclusive)
	{
		int32		bnd = DatumGetInt32(upper.val);

		/* Handle possible overflow manually */
		if (unlikely(bnd == PG_INT32_MAX))
			proof_ereturn((Datum) 0);
		upper.val = Int32GetDatum(bnd + 1);
		upper.inclusive = false;
	}

	PG_RETURN_RANGE_P(range_serialize(typcache, &lower, &upper,
									  false, escontext));
}

/* rangetypes.c internal comparisons, verbatim (elog -> PROOF_ELOG_ABORT) */
static bool
range_eq_internal(TypeCacheEntry *typcache, const RangeType *r1,
				  const RangeType *r2)
{
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	if (empty1 && empty2)
		return true;
	if (empty1 != empty2)
		return false;

	if (range_cmp_bounds(typcache, &lower1, &lower2) != 0)
		return false;

	if (range_cmp_bounds(typcache, &upper1, &upper2) != 0)
		return false;

	return true;
}

static bool
range_ne_internal(TypeCacheEntry *typcache, const RangeType *r1,
				  const RangeType *r2)
{
	return (!range_eq_internal(typcache, r1, r2));
}

static bool
range_contains_elem_internal(TypeCacheEntry *typcache, const RangeType *r,
							 Datum val)
{
	RangeBound	lower;
	RangeBound	upper;
	bool		empty;
	int32		cmp;

	range_deserialize(typcache, r, &lower, &upper, &empty);

	if (empty)
		return false;

	if (!lower.infinite)
	{
		cmp = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
											  typcache->rng_collation,
											  lower.val, val));
		if (cmp > 0)
			return false;
		if (cmp == 0 && !lower.inclusive)
			return false;
	}

	if (!upper.infinite)
	{
		cmp = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
											  typcache->rng_collation,
											  upper.val, val));
		if (cmp < 0)
			return false;
		if (cmp == 0 && !upper.inclusive)
			return false;
	}

	return true;
}

static bool
range_contains_internal(TypeCacheEntry *typcache, const RangeType *r1,
						const RangeType *r2)
{
	RangeBound	lower1;
	RangeBound	upper1;
	bool		empty1;
	RangeBound	lower2;
	RangeBound	upper2;
	bool		empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* If either range is empty, the answer is easy */
	if (empty2)
		return true;
	else if (empty1)
		return false;

	/* Else we must have lower1 <= lower2 and upper1 >= upper2 */
	if (range_cmp_bounds(typcache, &lower1, &lower2) > 0)
		return false;
	if (range_cmp_bounds(typcache, &upper1, &upper2) < 0)
		return false;

	return true;
}

static bool
range_contained_by_internal(TypeCacheEntry *typcache, const RangeType *r1,
							const RangeType *r2)
{
	return range_contains_internal(typcache, r2, r1);
}

static bool
range_before_internal(TypeCacheEntry *typcache, const RangeType *r1,
					  const RangeType *r2)
{
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* An empty range is neither before nor after any other range */
	if (empty1 || empty2)
		return false;

	return (range_cmp_bounds(typcache, &upper1, &lower2) < 0);
}

static bool
range_after_internal(TypeCacheEntry *typcache, const RangeType *r1,
					 const RangeType *r2)
{
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* An empty range is neither before nor after any other range */
	if (empty1 || empty2)
		return false;

	return (range_cmp_bounds(typcache, &lower1, &upper2) > 0);
}

/* rangetypes.c bounds_adjacent, verbatim */
static bool
bounds_adjacent(TypeCacheEntry *typcache, RangeBound boundA, RangeBound boundB)
{
	int			cmp;

	Assert(!boundA.lower && boundB.lower);

	cmp = range_cmp_bound_values(typcache, &boundA, &boundB);
	if (cmp < 0)
	{
		RangeType  *r;

		/* in a continuous subtype, there are assumed to be points between */
		if (!OidIsValid(typcache->rng_canonical_finfo.fn_oid))
			return false;

		/* flip the inclusion flags */
		boundA.inclusive = !boundA.inclusive;
		boundB.inclusive = !boundB.inclusive;
		/* change upper/lower labels to avoid Assert failures */
		boundA.lower = true;
		boundB.lower = false;
		r = make_range(typcache, &boundA, &boundB, false, NULL);
		return RangeIsEmpty(r);
	}
	else if (cmp == 0)
		return boundA.inclusive != boundB.inclusive;
	else
		return false;			/* bounds overlap */
}

static bool
range_adjacent_internal(TypeCacheEntry *typcache, const RangeType *r1,
						const RangeType *r2)
{
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* An empty range is not adjacent to any other range */
	if (empty1 || empty2)
		return false;

	return (bounds_adjacent(typcache, upper1, lower2) ||
			bounds_adjacent(typcache, upper2, lower1));
}

static bool
range_overlaps_internal(TypeCacheEntry *typcache, const RangeType *r1,
						const RangeType *r2)
{
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(false);

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* An empty range does not overlap any other range */
	if (empty1 || empty2)
		return false;

	if (range_cmp_bounds(typcache, &lower1, &lower2) >= 0 &&
		range_cmp_bounds(typcache, &lower1, &upper2) <= 0)
		return true;

	if (range_cmp_bounds(typcache, &lower2, &lower1) >= 0 &&
		range_cmp_bounds(typcache, &lower2, &upper1) <= 0)
		return true;

	return false;
}

/* rangetypes.c range_cmp (fmgr level), verbatim */
static Datum
range_cmp(PG_FUNCTION_ARGS)
{
	RangeType  *r1 = PG_GETARG_RANGE_P(0);
	RangeType  *r2 = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	RangeBound	lower1,
				lower2;
	RangeBound	upper1,
				upper2;
	bool		empty1,
				empty2;
	int			cmp;

	check_stack_depth();		/* recurses when subtype is a range type */

	/* Different types should be prevented by ANYRANGE matching rules */
	if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
		PROOF_ELOG_ABORT(Int32GetDatum(-2));

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	/* For b-tree use, empty ranges sort before all else */
	if (empty1 && empty2)
		cmp = 0;
	else if (empty1)
		cmp = -1;
	else if (empty2)
		cmp = 1;
	else
	{
		cmp = range_cmp_bounds(typcache, &lower1, &lower2);
		if (cmp == 0)
			cmp = range_cmp_bounds(typcache, &upper1, &upper2);
	}

	PG_FREE_IF_COPY(r1, 0);
	PG_FREE_IF_COPY(r2, 1);

	PG_RETURN_INT32(cmp);
}

/* rangetypes.c lt/le/ge/gt wrappers, verbatim */
static Datum
range_lt(PG_FUNCTION_ARGS)
{
	int			cmp = range_cmp(fcinfo);

	PG_RETURN_BOOL(cmp < 0);
}

static Datum
range_le(PG_FUNCTION_ARGS)
{
	int			cmp = range_cmp(fcinfo);

	PG_RETURN_BOOL(cmp <= 0);
}

static Datum
range_ge(PG_FUNCTION_ARGS)
{
	int			cmp = range_cmp(fcinfo);

	PG_RETURN_BOOL(cmp >= 0);
}

static Datum
range_gt(PG_FUNCTION_ARGS)
{
	int			cmp = range_cmp(fcinfo);

	PG_RETURN_BOOL(cmp > 0);
}

/* rangetypes.c range_empty (SQL isempty), verbatim */
static Datum
range_empty(PG_FUNCTION_ARGS)
{
	RangeType  *r1 = PG_GETARG_RANGE_P(0);
	char		flags = range_get_flags(r1);

	PG_RETURN_BOOL(flags & RANGE_EMPTY);
}

/* ---- exported plain-signature range entries ---- */

typedef bool (*range_pair_fn) (TypeCacheEntry *, const RangeType *,
							   const RangeType *);

/* seam dispatch for the plain-signature exports (mirrors what the fmgr
 * bodies do via range_get_typcache on RangeTypeGetOid) */
static TypeCacheEntry *
entry_for_range(const void *r)
{
	return range_get_typcache(NULL, RangeTypeGetOid((const RangeType *) r));
}

static int
call_range_pair(range_pair_fn fn, const void *r1, const void *r2)
{
	return (int) fn(entry_for_range(r1),
					(const RangeType *) r1, (const RangeType *) r2);
}

int pg_c_range_eq(const void *r1, const void *r2)
{ return call_range_pair(range_eq_internal, r1, r2); }
int pg_c_range_ne(const void *r1, const void *r2)
{ return call_range_pair(range_ne_internal, r1, r2); }
int pg_c_range_contains(const void *r1, const void *r2)
{ return call_range_pair(range_contains_internal, r1, r2); }
int pg_c_range_contained_by(const void *r1, const void *r2)
{ return call_range_pair(range_contained_by_internal, r1, r2); }
int pg_c_range_before(const void *r1, const void *r2)
{ return call_range_pair(range_before_internal, r1, r2); }
int pg_c_range_after(const void *r1, const void *r2)
{ return call_range_pair(range_after_internal, r1, r2); }
int pg_c_range_overlaps(const void *r1, const void *r2)
{ return call_range_pair(range_overlaps_internal, r1, r2); }
int pg_c_range_adjacent(const void *r1, const void *r2)
{ return call_range_pair(range_adjacent_internal, r1, r2); }

int pg_c_range_contains_elem(const void *r, int32 val)
{
	return (int) range_contains_elem_internal(entry_for_range(r),
											  (const RangeType *) r,
											  Int32GetDatum(val));
}

int pg_c_range_contains_elem64(const void *r, int64 val)
{
	return (int) range_contains_elem_internal(entry_for_range(r),
											  (const RangeType *) r,
											  Int64GetDatum(val));
}

/* rangetypes.c elem_contained_by_range, verbatim (fmgr arg order: elem is
 * arg0, range arg1) */
static Datum
elem_contained_by_range(PG_FUNCTION_ARGS)
{
	Datum		val = PG_GETARG_DATUM(0);
	RangeType  *r = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

	PG_RETURN_BOOL(range_contains_elem_internal(typcache, r, val));
}

int pg_c_elem_contained_by(uint64 val, const void *r)
{
	LOCAL_FCINFO(fcinfo, 2);
	FmgrInfo	flinfo;

	flinfo.fn_addr = NULL;
	flinfo.fn_oid = InvalidOid;
	flinfo.fn_extra = NULL;
	InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = (Datum) val;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = PointerGetDatum(r);
	fcinfo->args[1].isnull = false;
	return (int) DatumGetBool(elem_contained_by_range(fcinfo));
}

static Datum
call_range2(Datum (*fn) (FunctionCallInfo), const void *r1, const void *r2)
{
	LOCAL_FCINFO(fcinfo, 2);
	FmgrInfo	flinfo;

	flinfo.fn_addr = NULL;
	flinfo.fn_oid = InvalidOid;
	flinfo.fn_extra = NULL;
	InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = PointerGetDatum(r1);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = PointerGetDatum(r2);
	fcinfo->args[1].isnull = false;
	return fn(fcinfo);
}

int32 pg_c_range_cmp(const void *r1, const void *r2)
{ return DatumGetInt32(call_range2(range_cmp, r1, r2)); }
int pg_c_range_lt(const void *r1, const void *r2)
{ return (int) DatumGetBool(call_range2(range_lt, r1, r2)); }
int pg_c_range_le(const void *r1, const void *r2)
{ return (int) DatumGetBool(call_range2(range_le, r1, r2)); }
int pg_c_range_ge(const void *r1, const void *r2)
{ return (int) DatumGetBool(call_range2(range_ge, r1, r2)); }
int pg_c_range_gt(const void *r1, const void *r2)
{ return (int) DatumGetBool(call_range2(range_gt, r1, r2)); }

int pg_c_range_empty(const void *r1)
{
	LOCAL_FCINFO(fcinfo, 2);
	FmgrInfo	flinfo;

	flinfo.fn_addr = NULL;
	flinfo.fn_oid = InvalidOid;
	flinfo.fn_extra = NULL;
	InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = PointerGetDatum(r1);
	fcinfo->args[0].isnull = false;
	return (int) DatumGetBool(range_empty(fcinfo));
}
