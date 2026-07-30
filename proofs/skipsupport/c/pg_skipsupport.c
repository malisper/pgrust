/*
 * Vendored PostgreSQL C for the skip-support family proofs
 * (pg_proc oids 6402-6409).
 *
 * Provenance (all fetched 2026-07-28 from postgres/postgres REL_18_STABLE):
 *  - src/backend/access/nbtree/nbtcompare.c
 *      bool_decrement/bool_increment/btboolskipsupport      (lines ~84-126)
 *      int2_decrement/int2_increment/btint2skipsupport      (lines ~156-200)
 *      int4_decrement/int4_increment/btint4skipsupport      (lines ~224-266)
 *      int8_decrement/int8_increment/btint8skipsupport      (lines ~312-354)
 *      oid_decrement/oid_increment/btoidskipsupport         (lines ~478-520)
 *      char_decrement/char_increment/btcharskipsupport      (lines ~560-600)
 *  - src/backend/utils/adt/date.c
 *      date_decrement/date_increment/date_skipsupport       (lines ~466-509)
 *  - src/backend/utils/adt/timestamp.c
 *      timestamp_decrement/timestamp_increment/
 *      timestamp_skipsupport                                (lines ~2313-2357)
 *
 * SHIMS (function bodies verbatim; everything shimmed is listed here):
 *  - names pg_-prefixed; `static` dropped (harness links them via FFI).
 *  - Datum: postgres.h `typedef uintptr_t Datum` -> uint64_t (this proof
 *    suite targets the 64-bit catalog configuration, same as the shipped
 *    Rust Datum, which is 8 bytes on every target).
 *  - postgres.h Datum conversion inlines vendored below verbatim-semantics
 *    (DatumGetBool/BoolGetDatum/DatumGetInt16/Int16GetDatum/...): signed
 *    GetDatum sign-extends into the full word, unsigned zero-extends,
 *    exactly as in REL_18 postgres.h.
 *  - CHAR SIGNEDNESS: postgres.h CharGetDatum takes `char`, whose
 *    signedness is platform-defined (signed on x86, unsigned on ARM64
 *    Linux, unsigned here by pinning to `unsigned char`).  C's own datum
 *    WORD for char_decrement/char_increment therefore differs between
 *    platforms for values > 127 (sign- vs zero-extension of the padding
 *    bits); the only defined surface is the low byte.  The char harnesses
 *    compare AT VALUE LEVEL (uint8) for this reason; all other kernels are
 *    compared at full 64-bit datum-word level.
 *  - Relation: opaque pointer (`typedef void *Relation`); every vendored
 *    body ignores it (C passes it for by-ref types only).  Harness passes
 *    NULL.
 *  - `bool *underflow` / `bool *overflow` out-params -> `int *` (Kani
 *    lowers Rust bool in ways goto-cc can reject against C _Bool; the
 *    verbatim `*underflow = true;` stores 1 either way).
 *  - SkipSupportData: vendored from src/include/utils/skipsupport.h with
 *    the two callback fields typed via the local (int*-shimmed) function
 *    pointer typedef.  low_elem/high_elem/decrement/increment assignment
 *    statements verbatim (modulo the pg_ rename of the kernel symbols).
 *  - PG_FUNCTION_ARGS unwrapping: btXXXskipsupport(PG_FUNCTION_ARGS) with
 *    `SkipSupport sksup = (SkipSupport) PG_GETARG_POINTER(0)` -> plain
 *    `SkipSupport sksup` parameter; PG_RETURN_VOID() -> `return 0;` with
 *    int return type (Rust () lowers as struct Unit, rejected by goto-cc).
 *  - PG_INT16_MIN/MAX etc. (c.h) defined from <stdint.h> constants;
 *    InvalidOid/OID_MAX per postgres_ext.h; UCHAR_MAX per <limits.h>.
 *  - date.c: DateADT typedef + DATEVAL_NOBEGIN/NOEND vendored from
 *    src/include/utils/date.h (REL_18_STABLE): PG_INT32_MIN / PG_INT32_MAX.
 *  - timestamp.c: Timestamp typedef (int64) + the DT_NOBEGIN/DT_NOEND
 *    chain vendored from src/include/datatype/timestamp.h
 *    (REL_18_STABLE): DT_NOBEGIN = TIMESTAMP_MINUS_INFINITY = PG_INT64_MIN,
 *    DT_NOEND = TIMESTAMP_INFINITY = PG_INT64_MAX.  timestamp_skipsupport
 *    spells the sentinels PG_INT64_MIN/MAX directly (verbatim); the
 *    equalities below let the harness state the shipped comment's claim
 *    ("DT_NOBEGIN/DT_NOEND are i64::MIN/MAX so the int8 kernels are
 *    exact") as a theorem.
 */

#include "../../support/c/pg_proof_shim.h"
#include <limits.h>

typedef uint64_t Datum;
typedef void *Relation;

/* c.h fixed-width limits */
#define PG_INT16_MIN	INT16_MIN
#define PG_INT16_MAX	INT16_MAX
#define PG_INT32_MIN	INT32_MIN
#define PG_INT32_MAX	INT32_MAX
#define PG_INT64_MIN	INT64_MIN
#define PG_INT64_MAX	INT64_MAX

/* postgres_ext.h */
#define InvalidOid	((Oid) 0)
#define OID_MAX		UINT32_MAX

/* utils/date.h (REL_18_STABLE) */
typedef int32 DateADT;
#define DATEVAL_NOBEGIN ((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND	((DateADT) PG_INT32_MAX)

/* datatype/timestamp.h (REL_18_STABLE) */
typedef int64 Timestamp;
#define TIMESTAMP_MINUS_INFINITY	PG_INT64_MIN
#define TIMESTAMP_INFINITY			PG_INT64_MAX
#define DT_NOBEGIN		TIMESTAMP_MINUS_INFINITY
#define DT_NOEND		TIMESTAMP_INFINITY

/* postgres.h Datum conversions (REL_18 static inlines, verbatim semantics) */
static inline int
DatumGetBool(Datum X)
{
	return (X != 0);
}

static inline Datum
BoolGetDatum(int X)
{
	return (Datum) (X ? 1 : 0);
}

static inline int16
DatumGetInt16(Datum X)
{
	return (int16) X;
}

static inline Datum
Int16GetDatum(int16 X)
{
	return (Datum) X;			/* sign-extends */
}

static inline int32
DatumGetInt32(Datum X)
{
	return (int32) X;
}

static inline Datum
Int32GetDatum(int32 X)
{
	return (Datum) X;			/* sign-extends */
}

static inline int64
DatumGetInt64(Datum X)
{
	return (int64) X;
}

static inline Datum
Int64GetDatum(int64 X)
{
	return (Datum) X;
}

static inline Oid
DatumGetObjectId(Datum X)
{
	return (Oid) X;
}

static inline Datum
ObjectIdGetDatum(Oid X)
{
	return (Datum) X;			/* zero-extends */
}

static inline uint8
UInt8GetDatum(Datum X)			/* nbtcompare.c calls this ON a Datum; the
								 * verbatim effect is a uint8 truncation */
{
	return (uint8) X;
}

static inline Datum
CharGetDatum(unsigned char X)	/* char pinned unsigned; see header comment */
{
	return (Datum) X;
}

static inline Datum
DateADTGetDatum(DateADT X)
{
	return Int32GetDatum(X);
}

static inline DateADT
DatumGetDateADT(Datum X)
{
	return (DateADT) DatumGetInt32(X);
}

static inline Timestamp
DatumGetTimestamp(Datum X)
{
	return (Timestamp) DatumGetInt64(X);
}

static inline Datum
TimestampGetDatum(Timestamp X)
{
	return Int64GetDatum(X);
}

/* utils/skipsupport.h (REL_18_STABLE), bool* -> int* per shim note */
typedef Datum (*SkipSupportIncDec) (Relation rel, Datum existing,
									int *underflow);

typedef struct SkipSupportData
{
	Datum		low_elem;
	Datum		high_elem;
	SkipSupportIncDec decrement;
	SkipSupportIncDec increment;
} SkipSupportData;

typedef SkipSupportData *SkipSupport;

/* ---------------- nbtcompare.c ---------------- */

Datum
pg_bool_decrement(Relation rel, Datum existing, int *underflow)
{
	int			bexisting = DatumGetBool(existing);

	if (bexisting == 0)			/* shim: `== false` over the int shim */
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return BoolGetDatum(bexisting - 1);
}

Datum
pg_bool_increment(Relation rel, Datum existing, int *overflow)
{
	int			bexisting = DatumGetBool(existing);

	if (bexisting == 1)			/* shim: `== true` over the int shim */
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return BoolGetDatum(bexisting + 1);
}

int
pg_btboolskipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_bool_decrement;
	sksup->increment = pg_bool_increment;
	sksup->low_elem = BoolGetDatum(false);
	sksup->high_elem = BoolGetDatum(true);

	return 0;					/* shim: PG_RETURN_VOID() */
}

Datum
pg_int2_decrement(Relation rel, Datum existing, int *underflow)
{
	int16		iexisting = DatumGetInt16(existing);

	if (iexisting == PG_INT16_MIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return Int16GetDatum(iexisting - 1);
}

Datum
pg_int2_increment(Relation rel, Datum existing, int *overflow)
{
	int16		iexisting = DatumGetInt16(existing);

	if (iexisting == PG_INT16_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return Int16GetDatum(iexisting + 1);
}

int
pg_btint2skipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_int2_decrement;
	sksup->increment = pg_int2_increment;
	sksup->low_elem = Int16GetDatum(PG_INT16_MIN);
	sksup->high_elem = Int16GetDatum(PG_INT16_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

Datum
pg_int4_decrement(Relation rel, Datum existing, int *underflow)
{
	int32		iexisting = DatumGetInt32(existing);

	if (iexisting == PG_INT32_MIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return Int32GetDatum(iexisting - 1);
}

Datum
pg_int4_increment(Relation rel, Datum existing, int *overflow)
{
	int32		iexisting = DatumGetInt32(existing);

	if (iexisting == PG_INT32_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return Int32GetDatum(iexisting + 1);
}

int
pg_btint4skipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_int4_decrement;
	sksup->increment = pg_int4_increment;
	sksup->low_elem = Int32GetDatum(PG_INT32_MIN);
	sksup->high_elem = Int32GetDatum(PG_INT32_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

Datum
pg_int8_decrement(Relation rel, Datum existing, int *underflow)
{
	int64		iexisting = DatumGetInt64(existing);

	if (iexisting == PG_INT64_MIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return Int64GetDatum(iexisting - 1);
}

Datum
pg_int8_increment(Relation rel, Datum existing, int *overflow)
{
	int64		iexisting = DatumGetInt64(existing);

	if (iexisting == PG_INT64_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return Int64GetDatum(iexisting + 1);
}

int
pg_btint8skipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_int8_decrement;
	sksup->increment = pg_int8_increment;
	sksup->low_elem = Int64GetDatum(PG_INT64_MIN);
	sksup->high_elem = Int64GetDatum(PG_INT64_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

Datum
pg_oid_decrement(Relation rel, Datum existing, int *underflow)
{
	Oid			oexisting = DatumGetObjectId(existing);

	if (oexisting == InvalidOid)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return ObjectIdGetDatum(oexisting - 1);
}

Datum
pg_oid_increment(Relation rel, Datum existing, int *overflow)
{
	Oid			oexisting = DatumGetObjectId(existing);

	if (oexisting == OID_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return ObjectIdGetDatum(oexisting + 1);
}

int
pg_btoidskipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_oid_decrement;
	sksup->increment = pg_oid_increment;
	sksup->low_elem = ObjectIdGetDatum(InvalidOid);
	sksup->high_elem = ObjectIdGetDatum(OID_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

Datum
pg_char_decrement(Relation rel, Datum existing, int *underflow)
{
	uint8		cexisting = UInt8GetDatum(existing);

	if (cexisting == 0)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return CharGetDatum((uint8) cexisting - 1);
}

Datum
pg_char_increment(Relation rel, Datum existing, int *overflow)
{
	uint8		cexisting = UInt8GetDatum(existing);

	if (cexisting == UCHAR_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return CharGetDatum((uint8) cexisting + 1);
}

int
pg_btcharskipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_char_decrement;
	sksup->increment = pg_char_increment;

	/* btcharcmp compares chars as unsigned */
	sksup->low_elem = UInt8GetDatum(0);
	sksup->high_elem = UInt8GetDatum(UCHAR_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

/* ---------------- date.c ---------------- */

Datum
pg_date_decrement(Relation rel, Datum existing, int *underflow)
{
	DateADT		dexisting = DatumGetDateADT(existing);

	if (dexisting == DATEVAL_NOBEGIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return DateADTGetDatum(dexisting - 1);
}

Datum
pg_date_increment(Relation rel, Datum existing, int *overflow)
{
	DateADT		dexisting = DatumGetDateADT(existing);

	if (dexisting == DATEVAL_NOEND)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return DateADTGetDatum(dexisting + 1);
}

int
pg_date_skipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_date_decrement;
	sksup->increment = pg_date_increment;
	sksup->low_elem = DateADTGetDatum(DATEVAL_NOBEGIN);
	sksup->high_elem = DateADTGetDatum(DATEVAL_NOEND);

	return 0;					/* shim: PG_RETURN_VOID() */
}

/* ---------------- timestamp.c ---------------- */

/* note: this is used for timestamptz also */
Datum
pg_timestamp_decrement(Relation rel, Datum existing, int *underflow)
{
	Timestamp	texisting = DatumGetTimestamp(existing);

	if (texisting == PG_INT64_MIN)
	{
		/* return value is undefined */
		*underflow = true;
		return (Datum) 0;
	}

	*underflow = false;
	return TimestampGetDatum(texisting - 1);
}

/* note: this is used for timestamptz also */
Datum
pg_timestamp_increment(Relation rel, Datum existing, int *overflow)
{
	Timestamp	texisting = DatumGetTimestamp(existing);

	if (texisting == PG_INT64_MAX)
	{
		/* return value is undefined */
		*overflow = true;
		return (Datum) 0;
	}

	*overflow = false;
	return TimestampGetDatum(texisting + 1);
}

int
pg_timestamp_skipsupport(SkipSupport sksup)
{
	sksup->decrement = pg_timestamp_decrement;
	sksup->increment = pg_timestamp_increment;
	sksup->low_elem = TimestampGetDatum(PG_INT64_MIN);
	sksup->high_elem = TimestampGetDatum(PG_INT64_MAX);

	return 0;					/* shim: PG_RETURN_VOID() */
}

/*
 * Comment-claim exports for the 6409 theorem (see src/lib.rs
 * eq_timestamp_sentinels_are_dt_nobegin_noend): the DT_NOBEGIN/DT_NOEND
 * macro chain, evaluated in C, handed to the harness as values.
 */
int64
pg_dt_nobegin(void)
{
	return DT_NOBEGIN;
}

int64
pg_dt_noend(void)
{
	return DT_NOEND;
}
