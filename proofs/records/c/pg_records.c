/*
 * pg_records.c — verbatim PostgreSQL record-comparison column loops for the
 * RECORDS tier of the typcache-instantiation pattern: record_cmp / record_eq
 * proven C≡Rust per CONCRETE column descriptor (int4 / date columns), with
 * the typcache seam stubbed IDENTICALLY on both sides.
 *
 * Provenance (fetched 2026-07-28, ref REL_18_STABLE):
 *   - src/backend/utils/adt/rowtypes.c: record_cmp (the column-scan loop,
 *     lines "Scan corresponding columns" .. the column-count check),
 *     record_eq (same slice), ColumnCompareData / RecordCompareData structs.
 *   - src/backend/access/nbtree/nbtcompare.c: btint4cmp (the default
 *     !STRESS_SORT_INT_MIN_MAX arm: A_LESS_THAN_B = -1,
 *     A_GREATER_THAN_B = 1).
 *   - src/backend/utils/adt/int.c: int4eq.
 *   - src/backend/utils/adt/date.c: date_cmp, date_eq.
 *
 * Function BODIES are verbatim.  Shims (plumbing only, each documented):
 *
 *  1. DEFORM TIER SHIMMED OUT (the charter): C record_cmp/record_eq first
 *     detoast + lookup_rowtype_tupdesc + heap_deform_tuple into
 *     (values[], nulls[]) parallel to the tupdesc attrs.  The entry points
 *     here take the SAME deformed-columns interface the shipped Rust cores
 *     take (per-column attisdropped/atttypid/attcollation + Datum values +
 *     null flags) and build a minimal TupleDescData, so the verbatim loop
 *     lines (TupleDescAttr(...)->attisdropped etc.) compile unchanged.
 *     Deform/detoast is out of the theorem on both sides (tested tier).
 *  2. TYPCACHE SEAM (the pattern's subject): lookup_type_cache() returns a
 *     STATIC TypeCacheEntry: int4 -> cmp=btint4cmp/eq=int4eq, date ->
 *     cmp=date_cmp/eq=date_eq (all vendored verbatim below); any other oid
 *     -> an entry with fn_oid = InvalidOid, so the verbatim
 *     "could not identify ..." check fires.  The Rust harness ops implement
 *     the IDENTICAL mapping.  Typcache internals (catalog, invalidation)
 *     are out of the proof on both sides; the lookup PLACEMENT (before the
 *     null checks — the C quirk) is verbatim and in-theorem.
 *  3. fn_extra memoization shim: my_extra is a fresh zeroed local
 *     RecordCompareData per call (the first-call-of-a-series state);
 *     columns[FLEXIBLE_ARRAY_MEMBER] -> columns[PROOF_MAX_ATTS].  The
 *     cross-call memo cache is the Rust side's fn_extra tier too (tested).
 *  4. fmgr: minimal FmgrInfo/FunctionCallInfoBaseData + LOCAL_FCINFO /
 *     InitFunctionCallInfoData with the same call semantics.
 *     FunctionCallInvoke additionally accumulates
 *     pg_coll_acc += fncollation + 1 before dispatching (call-trace
 *     observability: the harness asserts the collation-select line and the
 *     compare-call count against the Rust ops trace).  Comparator bodies
 *     never set isnull, so record_eq's verbatim `locfcinfo->isnull ||`
 *     disjunct is compiled but never true for these column types.
 *  5. ereport(ERROR) -> set pg_proof_err to a distinct code at the exact
 *     abort point and return a sentinel (PROOF_EREPORT_FLAG convention,
 *     per-errcode variant): 1 = dissimilar column types (22023-class
 *     ERRCODE_DATATYPE_MISMATCH), 2 = column count mismatch (same errcode),
 *     3 = no comparison/equality support fn (ERRCODE_UNDEFINED_FUNCTION).
 *     Message text never crosses the seam.
 *  6. check_stack_depth / pfree / ReleaseTupleDesc / PG_FREE_IF_COPY:
 *     dropped with the deform tier (resource plumbing, no logic).
 *
 * Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches.
 */

#include "../../support/c/pg_proof_shim.h"

typedef uintptr_t Datum;

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))

#define DatumGetInt32(X) ((int32) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define DatumGetBool(X)  ((bool) ((X) != 0))
#define BoolGetDatum(X)  ((Datum) ((X) ? 1 : 0))

/* date.c plumbing: DateADT is int32; getters per utils/date.h */
typedef int32 DateADT;
#define DatumGetDateADT(X) ((DateADT) DatumGetInt32(X))

/* ---- error flag (shim 5): per-errcode PROOF_EREPORT_FLAG variant ---- */
static int pg_proof_err;
#define PROOF_ERR_DISSIMILAR 1
#define PROOF_ERR_COLCOUNT   2
#define PROOF_ERR_NOSUPPORT  3

int32
pg_c_get_err(void)
{
	return pg_proof_err;
}

/* ---- minimal attribute / tupdesc structs (shim 1) ---- */
typedef struct FormData_pg_attribute
{
	Oid			atttypid;
	Oid			attcollation;
	bool		attisdropped;
} FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

#define PROOF_MAX_ATTS 4

typedef struct TupleDescData
{
	int			natts;
	FormData_pg_attribute attrs[PROOF_MAX_ATTS];
} TupleDescData;
typedef TupleDescData *TupleDesc;

#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

/* ---- minimal fmgr (shim 4) ---- */
struct FunctionCallInfoBaseData;
typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);

typedef struct FmgrInfo
{
	PGFunction	fn_addr;
	Oid			fn_oid;
} FmgrInfo;

typedef struct NullableDatum
{
	Datum		value;
	bool		isnull;
} NullableDatum;

typedef struct FunctionCallInfoBaseData
{
	FmgrInfo   *flinfo;
	void	   *context;
	void	   *resultinfo;
	Oid			fncollation;
	bool		isnull;
	short		nargs;
	NullableDatum args[2];
} FunctionCallInfoBaseData;

/* shim: fixed 2-arg frame instead of the sized union */
#define LOCAL_FCINFO(name, nargs) \
	FunctionCallInfoBaseData name##_data; \
	FunctionCallInfo name = &name##_data

/* verbatim field semantics of fmgr.h InitFunctionCallInfoData */
#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
	do { \
		(Fcinfo).flinfo = (Flinfo); \
		(Fcinfo).context = (Context); \
		(Fcinfo).resultinfo = (Resultinfo); \
		(Fcinfo).fncollation = (Collation); \
		(Fcinfo).isnull = false; \
		(Fcinfo).nargs = (Nargs); \
	} while (0)

#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
#define PG_GETARG_DATUM(n)   (fcinfo->args[n].value)
#define PG_GETARG_INT32(n)   DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_DATEADT(n) DatumGetDateADT(PG_GETARG_DATUM(n))
#define PG_RETURN_INT32(x)   return Int32GetDatum(x)
#define PG_RETURN_BOOL(x)    return BoolGetDatum(x)

/* call-trace accumulator (shim 4): collation-select line observability */
static uint64 pg_coll_acc;

uint64
pg_c_get_coll_acc(void)
{
	return pg_coll_acc;
}

static Datum
pg_function_call_invoke(FunctionCallInfo fcinfo)
{
	pg_coll_acc += (uint64) fcinfo->fncollation + 1;
	return (*fcinfo->flinfo->fn_addr) (fcinfo);
}

#define FunctionCallInvoke(fcinfo) pg_function_call_invoke(fcinfo)

/* ------------------------------------------------------------------ */
/* nbtcompare.c btint4cmp — verbatim (default !STRESS_SORT_INT_MIN_MAX
 * arm of the A_LESS_THAN_B/A_GREATER_THAN_B macros) */
#define A_LESS_THAN_B		(-1)
#define A_GREATER_THAN_B	1

static Datum
pg_btint4cmp(PG_FUNCTION_ARGS)
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

/* int.c int4eq — verbatim */
static Datum
pg_int4eq(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int32		arg2 = PG_GETARG_INT32(1);

	PG_RETURN_BOOL(arg1 == arg2);
}

/* date.c date_cmp — verbatim */
static Datum
pg_date_cmp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	if (dateVal1 < dateVal2)
		PG_RETURN_INT32(-1);
	else if (dateVal1 > dateVal2)
		PG_RETURN_INT32(1);
	PG_RETURN_INT32(0);
}

/* date.c date_eq — verbatim */
static Datum
pg_date_eq(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 == dateVal2);
}

/* ---- typcache seam (shim 2) ---- */
#define INT4OID 23
#define DATEOID 1082
#define F_BTINT4CMP 351
#define F_INT4EQ 65
#define F_DATE_CMP 1092
#define F_DATE_EQ 1086

#define TYPECACHE_EQ_OPR_FINFO   0x0008
#define TYPECACHE_CMP_PROC_FINFO 0x0080

typedef struct TypeCacheEntry
{
	Oid			type_id;
	FmgrInfo	cmp_proc_finfo;
	FmgrInfo	eq_opr_finfo;
} TypeCacheEntry;

static TypeCacheEntry pg_entry_int4 = {
	INT4OID, {pg_btint4cmp, F_BTINT4CMP}, {pg_int4eq, F_INT4EQ}
};
static TypeCacheEntry pg_entry_date = {
	DATEOID, {pg_date_cmp, F_DATE_CMP}, {pg_date_eq, F_DATE_EQ}
};
static TypeCacheEntry pg_entry_nosupport = {
	InvalidOid, {NULL, InvalidOid}, {NULL, InvalidOid}
};

static TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
	(void) flags;
	if (type_id == INT4OID)
		return &pg_entry_int4;
	if (type_id == DATEOID)
		return &pg_entry_date;
	pg_entry_nosupport.type_id = type_id;
	return &pg_entry_nosupport;
}

/* ---- rowtypes.c comparison-metadata structs — verbatim modulo the
 * FLEXIBLE_ARRAY_MEMBER -> fixed PROOF_MAX_ATTS bound (shim 3) ---- */
typedef struct ColumnCompareData
{
	TypeCacheEntry *typentry;	/* has everything we need, actually */
} ColumnCompareData;

typedef struct RecordCompareData
{
	int			ncolumns;		/* allocated length of columns[] */
	Oid			record1_type;
	int32		record1_typmod;
	Oid			record2_type;
	int32		record2_typmod;
	ColumnCompareData columns[PROOF_MAX_ATTS];
} RecordCompareData;

/*
 * record_cmp — the column-scan slice, rowtypes.c lines from
 * "Scan corresponding columns" through the column-count check, verbatim.
 * Deform/typcache-memo setup above the loop is shimmed per the header.
 * ereport rewires: PROOF_ERR_* + return 0 sentinel at the exact ereport
 * program points.
 */
static int32
pg_record_cmp_loop(TupleDesc tupdesc1, const Datum *values1, const bool *nulls1,
				   TupleDesc tupdesc2, const Datum *values2, const bool *nulls2)
{
	int			result = 0;
	int			ncolumns1 = tupdesc1->natts;
	int			ncolumns2 = tupdesc2->natts;
	RecordCompareData my_extra_data;
	RecordCompareData *my_extra = &my_extra_data;
	int			i1;
	int			i2;
	int			j;

	/* shim 3: fresh first-call memo (fn_extra tier out of theorem) */
	my_extra->ncolumns = Max(ncolumns1, ncolumns2);
	for (j = 0; j < PROOF_MAX_ATTS; j++)
		my_extra->columns[j].typentry = NULL;

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		Form_pg_attribute att1;
		Form_pg_attribute att2;
		TypeCacheEntry *typentry;
		Oid			collation;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
		{
			/* ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
			 * "cannot compare dissimilar column types ..." — shim 5 */
			pg_proof_err = PROOF_ERR_DISSIMILAR;
			return 0;
		}

		/*
		 * If they're not same collation, we don't complain here, but the
		 * comparison function might.
		 */
		collation = att1->attcollation;
		if (collation != att2->attcollation)
			collation = InvalidOid;

		/*
		 * Lookup the comparison function if not done already
		 */
		typentry = my_extra->columns[j].typentry;
		if (typentry == NULL ||
			typentry->type_id != att1->atttypid)
		{
			typentry = lookup_type_cache(att1->atttypid,
										 TYPECACHE_CMP_PROC_FINFO);
			if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
			{
				/* ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
				 * "could not identify a comparison function ..." — shim 5 */
				pg_proof_err = PROOF_ERR_NOSUPPORT;
				return 0;
			}
			my_extra->columns[j].typentry = typentry;
		}

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			LOCAL_FCINFO(locfcinfo, 2);
			int32		cmpresult;

			if (nulls1[i1])
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
			if (nulls2[i2])
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}

			/* Compare the pair of elements */
			InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
									 collation, NULL, NULL);
			locfcinfo->args[0].value = values1[i1];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = values2[i2];
			locfcinfo->args[1].isnull = false;
			cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

			/* We don't expect comparison support functions to return null */
			Assert(!locfcinfo->isnull);

			if (cmpresult < 0)
			{
				/* arg1 is less than arg2 */
				result = -1;
				break;
			}
			else if (cmpresult > 0)
			{
				/* arg1 is greater than arg2 */
				result = 1;
				break;
			}
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result == 0)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
		{
			/* ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
			 * "cannot compare record types with different numbers of
			 * columns" — shim 5 */
			pg_proof_err = PROOF_ERR_COLCOUNT;
			return 0;
		}
	}

	return result;
}

/*
 * record_eq — the column-scan slice, rowtypes.c, verbatim under the same
 * shims as pg_record_cmp_loop.
 */
static bool
pg_record_eq_loop(TupleDesc tupdesc1, const Datum *values1, const bool *nulls1,
				  TupleDesc tupdesc2, const Datum *values2, const bool *nulls2)
{
	bool		result = true;
	int			ncolumns1 = tupdesc1->natts;
	int			ncolumns2 = tupdesc2->natts;
	RecordCompareData my_extra_data;
	RecordCompareData *my_extra = &my_extra_data;
	int			i1;
	int			i2;
	int			j;

	/* shim 3: fresh first-call memo (fn_extra tier out of theorem) */
	my_extra->ncolumns = Max(ncolumns1, ncolumns2);
	for (j = 0; j < PROOF_MAX_ATTS; j++)
		my_extra->columns[j].typentry = NULL;

	/*
	 * Scan corresponding columns, allowing for dropped columns in different
	 * places in the two rows.  i1 and i2 are physical column indexes, j is
	 * the logical column index.
	 */
	i1 = i2 = j = 0;
	while (i1 < ncolumns1 || i2 < ncolumns2)
	{
		LOCAL_FCINFO(locfcinfo, 2);
		Form_pg_attribute att1;
		Form_pg_attribute att2;
		TypeCacheEntry *typentry;
		Oid			collation;
		bool		oprresult;

		/*
		 * Skip dropped columns
		 */
		if (i1 < ncolumns1 && TupleDescAttr(tupdesc1, i1)->attisdropped)
		{
			i1++;
			continue;
		}
		if (i2 < ncolumns2 && TupleDescAttr(tupdesc2, i2)->attisdropped)
		{
			i2++;
			continue;
		}
		if (i1 >= ncolumns1 || i2 >= ncolumns2)
			break;				/* we'll deal with mismatch below loop */

		att1 = TupleDescAttr(tupdesc1, i1);
		att2 = TupleDescAttr(tupdesc2, i2);

		/*
		 * Have two matching columns, they must be same type
		 */
		if (att1->atttypid != att2->atttypid)
		{
			/* ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
			 * "cannot compare dissimilar column types ..." — shim 5 */
			pg_proof_err = PROOF_ERR_DISSIMILAR;
			return false;
		}

		/*
		 * If they're not same collation, we don't complain here, but the
		 * equality function might.
		 */
		collation = att1->attcollation;
		if (collation != att2->attcollation)
			collation = InvalidOid;

		/*
		 * Lookup the equality function if not done already
		 */
		typentry = my_extra->columns[j].typentry;
		if (typentry == NULL ||
			typentry->type_id != att1->atttypid)
		{
			typentry = lookup_type_cache(att1->atttypid,
										 TYPECACHE_EQ_OPR_FINFO);
			if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			{
				/* ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
				 * "could not identify an equality operator ..." — shim 5 */
				pg_proof_err = PROOF_ERR_NOSUPPORT;
				return false;
			}
			my_extra->columns[j].typentry = typentry;
		}

		/*
		 * We consider two NULLs equal; NULL > not-NULL.
		 */
		if (!nulls1[i1] || !nulls2[i2])
		{
			if (nulls1[i1] || nulls2[i2])
			{
				result = false;
				break;
			}

			/* Compare the pair of elements */
			InitFunctionCallInfoData(*locfcinfo, &typentry->eq_opr_finfo, 2,
									 collation, NULL, NULL);
			locfcinfo->args[0].value = values1[i1];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = values2[i2];
			locfcinfo->args[1].isnull = false;
			oprresult = DatumGetBool(FunctionCallInvoke(locfcinfo));
			if (locfcinfo->isnull || !oprresult)
			{
				result = false;
				break;
			}
		}

		/* equal, so continue to next column */
		i1++, i2++, j++;
	}

	/*
	 * If we didn't break out of the loop early, check for column count
	 * mismatch.  (We do not report such mismatch if we found unequal column
	 * values; is that a feature or a bug?)
	 */
	if (result)
	{
		if (i1 != ncolumns1 || i2 != ncolumns2)
		{
			/* ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
			 * "cannot compare record types with different numbers of
			 * columns" — shim 5 */
			pg_proof_err = PROOF_ERR_COLCOUNT;
			return false;
		}
	}

	return result;
}

/* ---- entry points: deformed-columns marshalling (shim 1) ---- */

static void
pg_fill_side(TupleDesc td, Datum *values, bool *nulls,
			 int32 natts, const uint8 *dropped, const uint32 *types,
			 const uint32 *colls, const int32 *ivals, const uint8 *isnull)
{
	int			i;

	td->natts = (int) natts;
	for (i = 0; i < natts; i++)
	{
		td->attrs[i].atttypid = (Oid) types[i];
		td->attrs[i].attcollation = (Oid) colls[i];
		td->attrs[i].attisdropped = (bool) dropped[i];
		values[i] = Int32GetDatum(ivals[i]);
		nulls[i] = (bool) isnull[i];
	}
}

int32
pg_c_record_cmp(int32 natts1, const uint8 *dropped1, const uint32 *types1,
				const uint32 *colls1, const int32 *vals1, const uint8 *nulls1,
				int32 natts2, const uint8 *dropped2, const uint32 *types2,
				const uint32 *colls2, const int32 *vals2, const uint8 *nulls2)
{
	TupleDescData td1;
	TupleDescData td2;
	Datum		values1[PROOF_MAX_ATTS];
	Datum		values2[PROOF_MAX_ATTS];
	bool		isnull1[PROOF_MAX_ATTS];
	bool		isnull2[PROOF_MAX_ATTS];

	pg_proof_err = 0;
	pg_coll_acc = 0;
	pg_fill_side(&td1, values1, isnull1, natts1, dropped1, types1, colls1, vals1, nulls1);
	pg_fill_side(&td2, values2, isnull2, natts2, dropped2, types2, colls2, vals2, nulls2);
	return pg_record_cmp_loop(&td1, values1, isnull1, &td2, values2, isnull2);
}

int32
pg_c_record_eq(int32 natts1, const uint8 *dropped1, const uint32 *types1,
			   const uint32 *colls1, const int32 *vals1, const uint8 *nulls1,
			   int32 natts2, const uint8 *dropped2, const uint32 *types2,
			   const uint32 *colls2, const int32 *vals2, const uint8 *nulls2)
{
	TupleDescData td1;
	TupleDescData td2;
	Datum		values1[PROOF_MAX_ATTS];
	Datum		values2[PROOF_MAX_ATTS];
	bool		isnull1[PROOF_MAX_ATTS];
	bool		isnull2[PROOF_MAX_ATTS];

	pg_proof_err = 0;
	pg_coll_acc = 0;
	pg_fill_side(&td1, values1, isnull1, natts1, dropped1, types1, colls1, vals1, nulls1);
	pg_fill_side(&td2, values2, isnull2, natts2, dropped2, types2, colls2, vals2, nulls2);
	return pg_record_eq_loop(&td1, values1, isnull1, &td2, values2, isnull2) ? 1 : 0;
}
