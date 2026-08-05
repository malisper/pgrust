/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-28 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *   src/backend/access/brin/brin_minmax.c — brin_minmax_opcinfo,
 *       brin_minmax_add_value, brin_minmax_consistent, brin_minmax_union,
 *       minmax_get_strategy_procinfo, MinmaxOpaque (all bodies verbatim)
 *   src/backend/utils/adt/int.c  — int4{lt,le,eq,ge,gt}
 *   src/backend/utils/adt/date.c — date_{lt,le,eq,ge,gt}
 *   src/backend/utils/adt/uuid.c — uuid_internal_cmp, uuid_{lt,le,eq,ge,gt}
 *
 * SHIMS (everything else is verbatim; nothing here replaces logic under
 * proof):
 *
 *  - fmgr unwrapping: PG_FUNCTION_ARGS entry points become plain C
 *    signatures (bdesc/column/newval/isnull/colloid parameters mirror the
 *    PG_GETARG_* prologue); PG_RETURN_BOOL(x) -> `return x;` (int),
 *    PG_RETURN_DATUM(x) -> `return x;`, PG_RETURN_VOID() -> `return;`.
 *    Comparator PG_GETARG_INT32/DATEADT/UUID_P become typed parameters.
 *  - struct shims: BrinDesc/BrinValues/TupleDescData/FormData_pg_attribute/
 *    RelationData/ScanKeyData/BrinOpcInfo/TypeCacheEntry are redeclared with
 *    ONLY the fields the vendored bodies read (same names/semantics);
 *    TupleDescAttr(desc,i) = &desc->attrs[i] as in tupdesc.h.
 *  - FmgrInfo/FunctionCall2Coll: FmgrInfo carries {fn_addr, fn_oid} and
 *    FunctionCall2Coll dispatches through fn_addr(collation, arg1, arg2) —
 *    the fcinfo frame construction is fmgr plumbing, out of proof; the
 *    dispatched comparator BODY is verbatim and in-theorem.
 *  - THE CATALOG SEAM (the modeled seam of this family): C resolves
 *    strategy -> operator -> proc via SearchSysCache4(AMOPSTRATEGY) +
 *    SysCacheGetAttrNotNull + get_opcode + fmgr_info_cxt. Those four are
 *    shimmed to a concrete model of the real pg_amop/pg_operator catalog
 *    content for the int4/date/uuid btree comparator families:
 *        strategy 1..5 -> operator oid -> pg_proc oid -> comparator fn
 *      int4: ops {97,523,96,525,521}    -> procs {66,149,65,150,147}
 *      date: ops {1095,1096,1093,1098,1097} -> procs {1087,1088,1086,1090,1089}
 *      uuid: ops {2974,2976,2972,2977,2975} -> procs {2954,2955,2956,2957,2958}
 *    (rows copied from pg_amop/pg_operator/pg_proc.dat, REL_18_STABLE).
 *    The Rust harness stubs lsyscache::get_opfamily_member / get_opcode /
 *    fmgr_core::fmgr_info to the SAME tables, so the theorem is: GIVEN this
 *    (identical) resolved comparator family, the support functions are
 *    equivalent. Catalog lookup internals (syscache) are out of proof;
 *    minmax_get_strategy_procinfo's caching/invalidation logic stays
 *    verbatim and in-theorem on both sides.
 *    `pg_seam_skew` (negative control): when 1, the strategy-1 proc (any
 *    type) dispatches to the <= comparator on the C side only — the control
 *    harness must FAIL, witnessing the seam model is load-bearing.
 *  - datumCopy -> static bump arena (byval returns the value verbatim as C
 *    does; byref fixed-length memcpy). Allocation strategy is out of proof
 *    (matches the Rust side's static-buffer allocator model).
 *  - pfree -> no-op (bump arenas reclaim wholesale; the freed value's bytes
 *    are never read again by the verbatim body).
 *  - palloc0 (opcinfo only) -> zeroed static buffer, single allocation per
 *    call.
 *  - lookup_type_cache -> static TypeCacheEntry carrying only type_id (the
 *    only field the harness compares; opcinfo passes flags=0 so no extra
 *    cache fields are requested).
 *  - elog(ERROR,...) -> no-op + fallthrough. Both elog sites are fenced
 *    unreachable by the harnesses (strategy in 1..=5, wired types only);
 *    the Rust side panics at the same program points.
 *  - Assert compiled out (production postgres posture, pg_proof_shim.h).
 */

#include "../../support/c/pg_proof_shim.h"
#include <string.h>

typedef uintptr_t Datum;
typedef int16 AttrNumber;
typedef uint16 StrategyNumber;
typedef void *MemoryContext;
typedef uintptr_t HeapTuple;
typedef Oid RegProcedure;

#define InvalidOid ((Oid) 0)

#define BTLessStrategyNumber 1
#define BTLessEqualStrategyNumber 2
#define BTEqualStrategyNumber 3
#define BTGreaterEqualStrategyNumber 4
#define BTGreaterStrategyNumber 5
#define BTMaxStrategyNumber 5

#define DatumGetBool(X) ((bool) ((X) != 0))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define DatumGetInt32(X) ((int32) (X))
#define DatumGetObjectId(X) ((Oid) (X))
#define ObjectIdGetDatum(X) ((Datum) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((char *) (X))
#define HeapTupleIsValid(tuple) ((tuple) != 0)
#define RegProcedureIsValid(p) ((p) != InvalidOid)

/* ---- minimal struct shims (fields the vendored bodies read) ---- */

typedef struct FormData_pg_attribute
{
	Oid			atttypid;
	int16		attlen;
	bool		attbyval;
} FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

typedef struct TupleDescData
{
	int			natts;
	FormData_pg_attribute attrs[1];
} TupleDescData;
typedef TupleDescData *TupleDesc;
#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

typedef Datum (*PGCmpFn) (Oid collation, Datum arg1, Datum arg2);

typedef struct FmgrInfo
{
	PGCmpFn		fn_addr;
	Oid			fn_oid;
} FmgrInfo;

static Datum
FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	return flinfo->fn_addr(collation, arg1, arg2);
}

typedef struct RelationData
{
	Oid		   *rd_opfamily;
} RelationData;
typedef RelationData *Relation;

typedef struct TypeCacheEntry
{
	Oid			type_id;
} TypeCacheEntry;

typedef struct BrinOpcInfo
{
	uint16		oi_nstored;
	bool		oi_regular_nulls;
	void	   *oi_opaque;
	TypeCacheEntry *oi_typcache[2];
} BrinOpcInfo;

#define SizeofBrinOpcInfo(ncols) \
	(offsetof(BrinOpcInfo, oi_typcache) + sizeof(TypeCacheEntry *) * (ncols))

typedef struct BrinDesc
{
	MemoryContext bd_context;
	Relation	bd_index;
	TupleDesc	bd_tupdesc;
	BrinOpcInfo **bd_info;
} BrinDesc;

typedef struct BrinValues
{
	AttrNumber	bv_attno;
	bool		bv_hasnulls;
	bool		bv_allnulls;
	Datum		bv_values[3];
} BrinValues;

typedef struct ScanKeyData
{
	int			sk_flags;
	AttrNumber	sk_attno;
	StrategyNumber sk_strategy;
	Oid			sk_subtype;
	Oid			sk_collation;
	Datum		sk_argument;
} ScanKeyData;
typedef ScanKeyData *ScanKey;

#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN) \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* elog(ERROR) sites are fenced unreachable by the harnesses */
#define elog(...) ((void) 0)

/* ---- datumCopy / pfree / palloc0 shims ---- */

static char pg_arena[256];
static size_t pg_arena_off;

static Datum
datumCopy(Datum value, bool typbyval, int16 typlen)
{
	if (typbyval)
		return value;
	/* fixed-length by-ref only in this family's harnesses (uuid: 16) */
	{
		char	   *dst = &pg_arena[pg_arena_off];

		pg_arena_off += (size_t) typlen;
		memcpy(dst, (const char *) value, (size_t) typlen);
		return (Datum) dst;
	}
}

#define pfree(p) ((void) (p))

static char pg_opcinfo_buf[sizeof(BrinOpcInfo) + MAXIMUM_ALIGNOF + 64];

static void *
palloc0(Size sz)
{
	(void) sz;					/* single allocation per opcinfo call */
	memset(pg_opcinfo_buf, 0, sizeof(pg_opcinfo_buf));
	return pg_opcinfo_buf;
}

static TypeCacheEntry pg_typcache_entry;

static TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
	(void) flags;
	pg_typcache_entry.type_id = type_id;
	return &pg_typcache_entry;
}

/* ---------------------------------------------------------------------
 * Vendored comparators (bodies verbatim; fmgr unwrapped per file header)
 * --------------------------------------------------------------------- */

/* int.c */
static int pg_int4eq(int32 arg1, int32 arg2) { return arg1 == arg2; }
static int pg_int4lt(int32 arg1, int32 arg2) { return arg1 < arg2; }
static int pg_int4le(int32 arg1, int32 arg2) { return arg1 <= arg2; }
static int pg_int4gt(int32 arg1, int32 arg2) { return arg1 > arg2; }
static int pg_int4ge(int32 arg1, int32 arg2) { return arg1 >= arg2; }

/* date.c (DateADT = int32) */
typedef int32 DateADT;
static int pg_date_eq(DateADT dateVal1, DateADT dateVal2) { return dateVal1 == dateVal2; }
static int pg_date_lt(DateADT dateVal1, DateADT dateVal2) { return dateVal1 < dateVal2; }
static int pg_date_le(DateADT dateVal1, DateADT dateVal2) { return dateVal1 <= dateVal2; }
static int pg_date_gt(DateADT dateVal1, DateADT dateVal2) { return dateVal1 > dateVal2; }
static int pg_date_ge(DateADT dateVal1, DateADT dateVal2) { return dateVal1 >= dateVal2; }

/* uuid.c */
#define UUID_LEN 16
typedef struct pg_uuid_t
{
	unsigned char data[UUID_LEN];
} pg_uuid_t;

static int
uuid_internal_cmp(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return memcmp(arg1->data, arg2->data, UUID_LEN);
}

static int pg_uuid_lt(const pg_uuid_t *arg1, const pg_uuid_t *arg2) { return uuid_internal_cmp(arg1, arg2) < 0; }
static int pg_uuid_le(const pg_uuid_t *arg1, const pg_uuid_t *arg2) { return uuid_internal_cmp(arg1, arg2) <= 0; }
static int pg_uuid_eq(const pg_uuid_t *arg1, const pg_uuid_t *arg2) { return uuid_internal_cmp(arg1, arg2) == 0; }
static int pg_uuid_ge(const pg_uuid_t *arg1, const pg_uuid_t *arg2) { return uuid_internal_cmp(arg1, arg2) >= 0; }
static int pg_uuid_gt(const pg_uuid_t *arg1, const pg_uuid_t *arg2) { return uuid_internal_cmp(arg1, arg2) > 0; }

/* fmgr-shape adapters over the verbatim comparator bodies (plumbing) */
static Datum pg_fc_int4lt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_int4lt(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_int4le(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_int4le(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_int4eq(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_int4eq(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_int4ge(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_int4ge(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_int4gt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_int4gt(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_date_lt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_date_lt(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_date_le(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_date_le(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_date_eq(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_date_eq(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_date_ge(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_date_ge(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_date_gt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_date_gt(DatumGetInt32(a), DatumGetInt32(b))); }
static Datum pg_fc_uuid_lt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_uuid_lt((const pg_uuid_t *) a, (const pg_uuid_t *) b)); }
static Datum pg_fc_uuid_le(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_uuid_le((const pg_uuid_t *) a, (const pg_uuid_t *) b)); }
static Datum pg_fc_uuid_eq(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_uuid_eq((const pg_uuid_t *) a, (const pg_uuid_t *) b)); }
static Datum pg_fc_uuid_ge(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_uuid_ge((const pg_uuid_t *) a, (const pg_uuid_t *) b)); }
static Datum pg_fc_uuid_gt(Oid c, Datum a, Datum b) { (void) c; return BoolGetDatum(pg_uuid_gt((const pg_uuid_t *) a, (const pg_uuid_t *) b)); }

/* ---------------------------------------------------------------------
 * Catalog model (the seam; see file header)
 * --------------------------------------------------------------------- */

int			pg_seam_skew = 0;

static Oid
pg_model_amop(Oid opfamily, Oid lefttype, Oid righttype, uint16 strategynum)
{
	(void) opfamily;			/* opaque token; the model keys on type */
	if (lefttype != righttype || strategynum < 1 || strategynum > 5)
		return InvalidOid;
	switch (lefttype)
	{
		case 23:				/* int4 */
			{
				static const Oid ops[5] = {97, 523, 96, 525, 521};

				return ops[strategynum - 1];
			}
		case 1082:				/* date */
			{
				static const Oid ops[5] = {1095, 1096, 1093, 1098, 1097};

				return ops[strategynum - 1];
			}
		case 2950:				/* uuid */
			{
				static const Oid ops[5] = {2974, 2976, 2972, 2977, 2975};

				return ops[strategynum - 1];
			}
	}
	return InvalidOid;
}

static RegProcedure
pg_model_opcode(Oid oprid)
{
	switch (oprid)
	{
		case 97: return 66;			/* int4lt */
		case 523: return 149;		/* int4le */
		case 96: return 65;			/* int4eq */
		case 525: return 150;		/* int4ge */
		case 521: return 147;		/* int4gt */
		case 1095: return 1087;		/* date_lt */
		case 1096: return 1088;		/* date_le */
		case 1093: return 1086;		/* date_eq */
		case 1098: return 1090;		/* date_ge */
		case 1097: return 1089;		/* date_gt */
		case 2974: return 2954;		/* uuid_lt */
		case 2976: return 2955;		/* uuid_le */
		case 2972: return 2956;		/* uuid_eq */
		case 2977: return 2957;		/* uuid_ge */
		case 2975: return 2958;		/* uuid_gt */
	}
	return InvalidOid;
}

static PGCmpFn
pg_model_fnaddr(RegProcedure procoid)
{
	/* negative control: strategy-1 (lt) procs dispatch to <= when skewed */
	switch (procoid)
	{
		case 66: return pg_seam_skew ? pg_fc_int4le : pg_fc_int4lt;
		case 149: return pg_fc_int4le;
		case 65: return pg_fc_int4eq;
		case 150: return pg_fc_int4ge;
		case 147: return pg_fc_int4gt;
		case 1087: return pg_seam_skew ? pg_fc_date_le : pg_fc_date_lt;
		case 1088: return pg_fc_date_le;
		case 1086: return pg_fc_date_eq;
		case 1090: return pg_fc_date_ge;
		case 1089: return pg_fc_date_gt;
		case 2954: return pg_seam_skew ? pg_fc_uuid_le : pg_fc_uuid_lt;
		case 2955: return pg_fc_uuid_le;
		case 2956: return pg_fc_uuid_eq;
		case 2957: return pg_fc_uuid_ge;
		case 2958: return pg_fc_uuid_gt;
	}
	return 0;
}

/* syscache-shape shims over the model (plumbing around the seam) */
#define AMOPSTRATEGY 0
#define Anum_pg_amop_amopopr 8

static HeapTuple
SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)
{
	(void) cacheId;
	return (HeapTuple) pg_model_amop((Oid) key1, (Oid) key2, (Oid) key3,
									 (uint16) key4);
}

static Datum
SysCacheGetAttrNotNull(int cacheId, HeapTuple tup, int attributeNumber)
{
	(void) cacheId;
	(void) attributeNumber;
	return (Datum) tup;			/* the model tuple IS the amopopr oid */
}

static void
ReleaseSysCache(HeapTuple tuple)
{
	(void) tuple;
}

static RegProcedure
get_opcode(Oid opno)
{
	return pg_model_opcode(opno);
}

static void
fmgr_info_cxt(RegProcedure functionId, FmgrInfo *finfo, MemoryContext mcxt)
{
	(void) mcxt;
	finfo->fn_oid = functionId;
	finfo->fn_addr = pg_model_fnaddr(functionId);
}

/* ---------------------------------------------------------------------
 * brin_minmax.c — verbatim bodies
 * --------------------------------------------------------------------- */

typedef struct MinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} MinmaxOpaque;

static FmgrInfo *minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
											  Oid subtype, uint16 strategynum);

/* brin_minmax_opcinfo, body verbatim (fmgr unwrapped; PG_RETURN_POINTER ->
 * return pointer) */
static BrinOpcInfo *
pg_brin_minmax_opcinfo(Oid typoid)
{
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) +
					 sizeof(MinmaxOpaque));
	result->oi_nstored = 2;
	result->oi_regular_nulls = true;
	result->oi_opaque = (MinmaxOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(2));
	result->oi_typcache[0] = result->oi_typcache[1] =
		lookup_type_cache(typoid, 0);

	return result;
}

/* brin_minmax_add_value, body verbatim from Assert(!isnull) onward */
static int
pg_brin_minmax_add_value(BrinDesc *bdesc, BrinValues *column, Datum newval,
						 bool isnull, Oid colloid)
{
	FmgrInfo   *cmpFn;
	Datum		compar;
	bool		updated = false;
	Form_pg_attribute attr;
	AttrNumber	attno;

	Assert(!isnull);

	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/*
	 * If the recorded value is null, store the new value (which we know to be
	 * not null) as both minimum and maximum, and we're done.
	 */
	if (column->bv_allnulls)
	{
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		column->bv_allnulls = false;
		return true;
	}

	/*
	 * Otherwise, need to compare the new value with the existing boundaries
	 * and update them accordingly.  First check if it's less than the
	 * existing minimum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[0]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[0]));
		column->bv_values[0] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	/*
	 * And now compare it to the existing maximum.
	 */
	cmpFn = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	compar = FunctionCall2Coll(cmpFn, colloid, newval, column->bv_values[1]);
	if (DatumGetBool(compar))
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(column->bv_values[1]));
		column->bv_values[1] = datumCopy(newval, attr->attbyval, attr->attlen);
		updated = true;
	}

	return updated;
}

/* brin_minmax_consistent, body verbatim (PG_RETURN_DATUM -> return) */
static Datum
pg_brin_minmax_consistent(BrinDesc *bdesc, BrinValues *column, ScanKey key,
						  Oid colloid)
{
	Oid			subtype;
	AttrNumber	attno;
	Datum		value;
	Datum		matches;
	FmgrInfo   *finfo;

	Assert(!column->bv_allnulls);

	attno = key->sk_attno;
	subtype = key->sk_subtype;
	value = key->sk_argument;
	switch (key->sk_strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 key->sk_strategy);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
										value);
			break;
		case BTEqualStrategyNumber:

			/*
			 * In the equality case (WHERE col = someval), we want to return
			 * the current page range if the minimum value in the range <=
			 * scan key, and the maximum value >= scan key.
			 */
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 BTLessEqualStrategyNumber);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[0],
										value);
			if (!DatumGetBool(matches))
				break;
			/* max() >= scankey */
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 BTGreaterEqualStrategyNumber);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
										value);
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype,
												 key->sk_strategy);
			matches = FunctionCall2Coll(finfo, colloid, column->bv_values[1],
										value);
			break;
		default:
			/* shouldn't happen */
			elog(ERROR, "invalid strategy number %d", key->sk_strategy);
			matches = 0;
			break;
	}

	return matches;
}

/* brin_minmax_union, body verbatim */
static void
pg_brin_minmax_union(BrinDesc *bdesc, BrinValues *col_a, BrinValues *col_b,
					 Oid colloid)
{
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *finfo;
	bool		needsadj;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTLessStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[0],
								 col_a->bv_values[0]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[0]));
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0],
										attr->attbyval, attr->attlen);
	}

	/* Adjust maximum, if B's max is greater than A's max */
	finfo = minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
										 BTGreaterStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[1],
								 col_a->bv_values[1]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[1]));
		col_a->bv_values[1] = datumCopy(col_b->bv_values[1],
										attr->attbyval, attr->attlen);
	}
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
							 uint16 strategynum)
{
	MinmaxOpaque *opaque;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	opaque = (MinmaxOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * We cache the procedures for the previous subtype in the opaque struct,
	 * to avoid repetitive syscache lookups.  If the subtype changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= BTMaxStrategyNumber; i++)
			opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
		opaque->cached_subtype = subtype;
	}

	if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple	tuple;
		Oid			opfamily,
					oprid;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
														Anum_pg_amop_amopopr));
		ReleaseSysCache(tuple);
		Assert(RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
}

/* ---------------------------------------------------------------------
 * Harness entry points (fmgr/AM plumbing around the verbatim bodies)
 * --------------------------------------------------------------------- */

static BrinDesc *
pg_mk_bdesc(TupleDescData *td, RelationData *rel, BrinOpcInfo **infos,
			BrinDesc *slot, Oid atttypid, int16 attlen, int attbyval,
			Oid opfamily, Oid *fam_slot, MinmaxOpaque *opaque,
			BrinOpcInfo *info)
{
	td->natts = 1;
	td->attrs[0].atttypid = atttypid;
	td->attrs[0].attlen = attlen;
	td->attrs[0].attbyval = (bool) attbyval;
	*fam_slot = opfamily;
	rel->rd_opfamily = fam_slot;
	memset(opaque, 0, sizeof(*opaque));
	info->oi_nstored = 2;
	info->oi_regular_nulls = true;
	info->oi_opaque = opaque;
	infos[0] = info;
	slot->bd_context = 0;
	slot->bd_index = rel;
	slot->bd_tupdesc = td;
	slot->bd_info = infos;
	return slot;
}

int
pg_run_minmax_add_value(Oid atttypid, int16 attlen, int attbyval,
						Oid opfamily, int allnulls, Datum min, Datum max,
						Datum newval, Oid colloid,
						Datum *out_min, Datum *out_max, int *out_allnulls)
{
	TupleDescData td;
	RelationData rel;
	Oid			fam;
	MinmaxOpaque opaque;
	BrinOpcInfo info;
	BrinOpcInfo *infos[1];
	BrinDesc	bdesc;
	BrinValues	col;
	int			r;

	pg_arena_off = 0;
	pg_mk_bdesc(&td, &rel, infos, &bdesc, atttypid, attlen, attbyval,
				opfamily, &fam, &opaque, &info);
	col.bv_attno = 1;
	col.bv_hasnulls = false;
	col.bv_allnulls = (bool) allnulls;
	col.bv_values[0] = min;
	col.bv_values[1] = max;
	col.bv_values[2] = 0;

	r = pg_brin_minmax_add_value(&bdesc, &col, newval, false, colloid);

	*out_min = col.bv_values[0];
	*out_max = col.bv_values[1];
	*out_allnulls = (int) col.bv_allnulls;
	return r;
}

int
pg_run_minmax_consistent(Oid atttypid, int16 attlen, int attbyval,
						 Oid opfamily, Datum min, Datum max,
						 uint16 strategy, Oid subtype, Datum value,
						 Oid colloid)
{
	TupleDescData td;
	RelationData rel;
	Oid			fam;
	MinmaxOpaque opaque;
	BrinOpcInfo info;
	BrinOpcInfo *infos[1];
	BrinDesc	bdesc;
	BrinValues	col;
	ScanKeyData key;
	Datum		matches;

	pg_arena_off = 0;
	pg_mk_bdesc(&td, &rel, infos, &bdesc, atttypid, attlen, attbyval,
				opfamily, &fam, &opaque, &info);
	col.bv_attno = 1;
	col.bv_hasnulls = false;
	col.bv_allnulls = false;
	col.bv_values[0] = min;
	col.bv_values[1] = max;
	col.bv_values[2] = 0;
	key.sk_flags = 0;
	key.sk_attno = 1;
	key.sk_strategy = strategy;
	key.sk_subtype = subtype;
	key.sk_collation = colloid;
	key.sk_argument = value;

	matches = pg_brin_minmax_consistent(&bdesc, &col, &key, colloid);
	return (int) DatumGetBool(matches);
}

int
pg_run_minmax_union(Oid atttypid, int16 attlen, int attbyval,
					Oid opfamily, Datum a_min, Datum a_max,
					Datum b_min, Datum b_max, Oid colloid,
					Datum *out_min, Datum *out_max)
{
	TupleDescData td;
	RelationData rel;
	Oid			fam;
	MinmaxOpaque opaque;
	BrinOpcInfo info;
	BrinOpcInfo *infos[1];
	BrinDesc	bdesc;
	BrinValues	col_a;
	BrinValues	col_b;

	pg_arena_off = 0;
	pg_mk_bdesc(&td, &rel, infos, &bdesc, atttypid, attlen, attbyval,
				opfamily, &fam, &opaque, &info);
	col_a.bv_attno = 1;
	col_a.bv_hasnulls = false;
	col_a.bv_allnulls = false;
	col_a.bv_values[0] = a_min;
	col_a.bv_values[1] = a_max;
	col_a.bv_values[2] = 0;
	col_b.bv_attno = 1;
	col_b.bv_hasnulls = false;
	col_b.bv_allnulls = false;
	col_b.bv_values[0] = b_min;
	col_b.bv_values[1] = b_max;
	col_b.bv_values[2] = 0;

	pg_brin_minmax_union(&bdesc, &col_a, &col_b, colloid);

	*out_min = col_a.bv_values[0];
	*out_max = col_a.bv_values[1];
	return 0;					/* int return: Kani Unit-vs-void FFI trap */
}

/*
 * Harness plumbing: copy a by-ref datum's 16-byte pointee into a caller
 * buffer. Exists so the HARNESS's pointee comparisons of C-returned datums
 * dereference inside C: Kani's Rust codegen gives an integer->pointer cast
 * of an FFI out-param no provenance, and the resulting whole-object-space
 * nondet deref explodes propositional reduction (measured 11.3 GiB on the
 * trivial allnulls arm; 2.1s without it). Not part of any verbatim body.
 */
int
pg_read16(Datum d, unsigned char *out)
{
	memcpy(out, (const char *) d, 16);
	return 0;					/* int return: Kani Unit-vs-void FFI trap */
}

int
pg_run_minmax_opcinfo(Oid typoid, uint16 *nstored, int *regular_nulls,
					  Oid *typid0, Oid *typid1)
{
	BrinOpcInfo *result = pg_brin_minmax_opcinfo(typoid);

	*nstored = result->oi_nstored;
	*regular_nulls = (int) result->oi_regular_nulls;
	*typid0 = result->oi_typcache[0]->type_id;
	*typid1 = result->oi_typcache[1]->type_id;
	return 0;					/* int return: Kani Unit-vs-void FFI trap */
}
