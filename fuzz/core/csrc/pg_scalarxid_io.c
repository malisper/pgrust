/*
 * pg_scalarxid_io.c: vendored PostgreSQL C oracle for the scalarxid_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/scalar).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/tid.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3;
 *     verified against ../pgrust-fabled/vendor/postgres-src): tidin, tidout,
 *     tideq, tidne, tidlt, tidle, tidgt, tidge, bttidcmp, tidlarger,
 *     tidsmaller.
 *   - src/backend/storage/page/itemptr.c @ same ref: ItemPointerCompare.
 *   - src/backend/utils/adt/xid.c @ same ref: xidout, xideq, xidneq,
 *     xid8out, xid8eq, xid8ne, xid8lt, xid8gt, xid8le, xid8ge, xid8cmp,
 *     xid8_larger, xid8_smaller.
 *   - src/backend/utils/adt/oid.c @ same ref: oidin, oidout, oideq, oidne,
 *     oidlt, oidle, oidge, oidgt, oidlarger, oidsmaller, oidvectorin,
 *     oidvectorout, check_valid_oidvector.
 *   - src/backend/utils/adt/numutils.c @ same ref: uint32in_subr,
 *     uint64in_subr.
 *   - src/include/access/transam.h @ same ref: FullTransactionId struct +
 *     FullTransactionIdEquals/Precedes/PrecedesOrEquals/Follows/
 *     FollowsOrEquals macros (copied verbatim).
 *   - src/include/storage/itemptr.h + storage/block.h @ same ref:
 *     ItemPointerData layout (BlockIdData bi_hi/bi_lo u16 split preserved)
 *     and the NoCheck get/set accessors as working C macros.
 *
 * Shims (plumbing only, never logic):
 *   - fmgr unwrapping: every Datum fn(PG_FUNCTION_ARGS) body is kept
 *     verbatim between plain-C-signature boundaries (PG_GETARG_* -> named
 *     parameters, PG_RETURN_* -> plain return). escontext is NULL-shaped
 *     (hard error), as in csrc/pg_float_io.c.
 *   - ereturn(escontext, ret, (errcode(X), errmsg(...))) -> record X in
 *     pg_diff_errcode and return ret; errmsg evaluates to 0 unevaluated.
 *   - ereport(ERROR, (...)) (only check_valid_oidvector) -> record the
 *     errcode and return from the void function; callers test
 *     pg_diff_errcode.
 *   - SOFT_ERROR_OCCURRED(escontext) -> (pg_diff_errcode != 0): with the
 *     ereturn shim above this reproduces the "error already raised"
 *     control flow of both the soft- and hard-error C paths.
 *   - errcode symbols -> small ints: 1 = ERRCODE_INVALID_TEXT_REPRESENTATION
 *     (22P02), 2 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003),
 *     3 = ERRCODE_DATATYPE_MISMATCH (42804).
 *   - palloc'd results -> caller/static buffers: tidin's palloc'd
 *     ItemPointerData -> caller struct; tidout/xidout/xid8out/oidout's
 *     palloc'd cstring -> caller char buffer (pstrdup elided, the
 *     snprintf formatting call itself verbatim); oidvectorin's
 *     palloc0/repalloc -> calloc/realloc with free in the driver entry;
 *     oidvectorout's palloc -> malloc, copied to the caller buffer, freed.
 *   - strtou64 (src/include/c.h maps it to strtoull on LP64) -> strtoull.
 *   - UINT64_FORMAT -> "%llu" + unsigned long long cast (c.h's per-ABI
 *     printf format macro).
 *   - SIZEOF_LONG > 4 (pg_config.h) -> ULONG_MAX > 0xFFFFFFFFUL, the same
 *     predicate computed portably; PG_UINT32_MAX -> UINT32_MAX.
 *   - PG_RETURN_NULL() in oidvectorin -> return NULL (the driver reports
 *     the already-recorded errcode).
 *
 * NOTE the numeric parse cores are the platform strtoul/strtoull exactly as
 * in real PostgreSQL (which defers to libc). tidin's empty-field acceptance
 * is therefore PLATFORM-DEPENDENT (glibc accepts "(,5)", BSD libc rejects);
 * the Rust driver bands that input class out — see scalarxid_diff.rs header
 * and ledger oid 48.
 */

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32 Oid;
typedef uint32 TransactionId;
typedef uint32 BlockNumber;
typedef uint16 OffsetNumber;
struct Node;
typedef struct Node Node;

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2
#define ERRCODE_DATATYPE_MISMATCH 3

#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)
#define ereport(elevel, stuff) do { (void) (stuff); return; } while (0)
#define SOFT_ERROR_OCCURRED(escontext) (pg_diff_errcode != 0)

/* ---- storage/block.h + storage/itemptr.h layout & NoCheck accessors ---- */

typedef struct BlockIdData
{
	uint16		bi_hi;
	uint16		bi_lo;
} BlockIdData;

typedef struct ItemPointerData
{
	BlockIdData ip_blkid;
	OffsetNumber ip_posid;
} ItemPointerData;

typedef ItemPointerData *ItemPointer;

#define BlockIdGetBlockNumber(blockId) \
	((((BlockNumber) (blockId)->bi_hi) << 16) | ((BlockNumber) (blockId)->bi_lo))
#define BlockIdSet(blockId, blockNumber) \
	((blockId)->bi_hi = (blockNumber) >> 16, \
	 (blockId)->bi_lo = (blockNumber) & 0xffff)
#define ItemPointerGetBlockNumberNoCheck(pointer) \
	(BlockIdGetBlockNumber(&(pointer)->ip_blkid))
#define ItemPointerGetOffsetNumberNoCheck(pointer) \
	((pointer)->ip_posid)
#define ItemPointerSet(pointer, blockNumber, offNum) \
	(BlockIdSet(&(pointer)->ip_blkid, blockNumber), \
	 (pointer)->ip_posid = (offNum))

/* ---- src/include/access/transam.h: FullTransactionId (VERBATIM) ---- */

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;

#define FullTransactionIdEquals(a, b)	((a).value == (b).value)
#define FullTransactionIdPrecedes(a, b)	((a).value < (b).value)
#define FullTransactionIdPrecedesOrEquals(a, b) ((a).value <= (b).value)
#define FullTransactionIdFollows(a, b) ((a).value > (b).value)
#define FullTransactionIdFollowsOrEquals(a, b) ((a).value >= (b).value)

/* ==================== SECTION 1: numutils.c (VERBATIM) ==================== */

static uint32
uint32in_subr(const char *s, char **endloc,
			  const char *typname, Node *escontext)
{
	uint32		result;
	unsigned long cvt;
	char	   *endptr;

	errno = 0;
	cvt = strtoul(s, &endptr, 0);

	/*
	 * strtoul() normally only sets ERANGE.  On some systems it may also set
	 * EINVAL, which simply means it couldn't parse the input string.  Be sure
	 * to report that the same way as the standard error indication (that
	 * endptr == s).
	 */
	if ((errno && errno != ERANGE) || endptr == s)
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						typname, s)));

	if (errno == ERANGE)
		ereturn(escontext, 0,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s",
						s, typname)));

	if (endloc)
	{
		/* caller wants to deal with rest of string */
		*endloc = endptr;
	}
	else
	{
		/* allow only whitespace after number */
		while (*endptr && isspace((unsigned char) *endptr))
			endptr++;
		if (*endptr)
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							typname, s)));
	}

	result = (uint32) cvt;

	/*
	 * Cope with possibility that unsigned long is wider than uint32, in which
	 * case strtoul will not raise an error for some values that are out of
	 * the range of uint32.
	 *
	 * For backwards compatibility, we want to accept inputs that are given
	 * with a minus sign, so allow the input value if it matches after either
	 * signed or unsigned extension to long.
	 *
	 * To ensure consistent results on 32-bit and 64-bit platforms, make sure
	 * the error message is the same as if strtoul() had returned ERANGE.
	 */
#if UINT32_MAX != ULONG_MAX
	if (cvt != (unsigned long) result &&
		cvt != (unsigned long) ((int) result))
		ereturn(escontext, 0,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s",
						s, typname)));
#endif

	return result;
}

static uint64
uint64in_subr(const char *s, char **endloc,
			  const char *typname, Node *escontext)
{
	uint64		result;
	char	   *endptr;

	errno = 0;
	result = strtoull(s, &endptr, 0);

	/*
	 * strtoul[l] normally only sets ERANGE.  On some systems it may also set
	 * EINVAL, which simply means it couldn't parse the input string.  Be sure
	 * to report that the same way as the standard error indication (that
	 * endptr == s).
	 */
	if ((errno && errno != ERANGE) || endptr == s)
		ereturn(escontext, 0,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						typname, s)));

	if (errno == ERANGE)
		ereturn(escontext, 0,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s",
						s, typname)));

	if (endloc)
	{
		/* caller wants to deal with rest of string */
		*endloc = endptr;
	}
	else
	{
		/* allow only whitespace after number */
		while (*endptr && isspace((unsigned char) *endptr))
			endptr++;
		if (*endptr)
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							typname, s)));
	}

	return result;
}

/* ============ SECTION 2: itemptr.c ItemPointerCompare (VERBATIM) ========== */

static int32
ItemPointerCompare(ItemPointer arg1, ItemPointer arg2)
{
	/*
	 * Use ItemPointerGet{Offset,Block}NumberNoCheck to avoid asserting
	 * ip_posid != 0, which may not be true for a user-supplied TID.
	 */
	BlockNumber b1 = ItemPointerGetBlockNumberNoCheck(arg1);
	BlockNumber b2 = ItemPointerGetBlockNumberNoCheck(arg2);

	if (b1 < b2)
		return -1;
	else if (b1 > b2)
		return 1;
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) <
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		return -1;
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) >
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		return 1;
	else
		return 0;
}

/* ==================== SECTION 3: tid.c (VERBATIM) ==================== */

#define LDELIM			'('
#define RDELIM			')'
#define DELIM			','
#define NTIDARGS		2

/* tidin body verbatim; fmgr unwrapped, escontext NULL-shaped, palloc'd
 * result -> caller struct (see header shim list). Returns 0 ok / 1 error. */
static int
pg_tidin_internal(char *str, ItemPointerData *out)
{
	Node	   *escontext = NULL;
	char	   *p,
			   *coord[NTIDARGS];
	int			i;
	ItemPointer result;
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;
	char	   *badp;
	unsigned long cvt;

	(void) escontext;

	for (i = 0, p = str; *p && i < NTIDARGS && *p != RDELIM; p++)
		if (*p == DELIM || (*p == LDELIM && i == 0))
			coord[i++] = p + 1;

	if (i < NTIDARGS)
		ereturn(escontext, 1,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));

	errno = 0;
	cvt = strtoul(coord[0], &badp, 10);
	if (errno || *badp != DELIM)
		ereturn(escontext, 1,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));
	blockNumber = (BlockNumber) cvt;

	/*
	 * Cope with possibility that unsigned long is wider than BlockNumber, in
	 * which case strtoul will not raise an error for some values that are out
	 * of the range of BlockNumber.  (See similar code in oidin().)
	 */
#if ULONG_MAX > 0xFFFFFFFFUL
	if (cvt != (unsigned long) blockNumber &&
		cvt != (unsigned long) ((int32) blockNumber))
		ereturn(escontext, 1,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));
#endif

	cvt = strtoul(coord[1], &badp, 10);
	if (errno || *badp != RDELIM ||
		cvt > USHRT_MAX)
		ereturn(escontext, 1,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));
	offsetNumber = (OffsetNumber) cvt;

	result = out;

	ItemPointerSet(result, blockNumber, offsetNumber);

	return 0;
}

/* tidout body verbatim; snprintf retargeted at the caller's buffer
 * (pstrdup elided per the header shim list). */
static void
pg_tidout_internal(ItemPointer itemPtr, char *buf, size_t buflen)
{
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;

	blockNumber = ItemPointerGetBlockNumberNoCheck(itemPtr);
	offsetNumber = ItemPointerGetOffsetNumberNoCheck(itemPtr);

	/* Perhaps someday we should output this as a record. */
	snprintf(buf, buflen, "(%u,%u)", blockNumber, offsetNumber);
}

/* ==================== SECTION 4: xid.c (VERBATIM) ==================== */

/* xidout body verbatim; palloc(16) -> caller buffer. */
static void
pg_xidout_internal(TransactionId transactionId, char *result)
{
	snprintf(result, 16, "%lu", (unsigned long) transactionId);
}

#define TransactionIdEquals(id1, id2)	((id1) == (id2))

/* xid8out body verbatim; palloc(21) -> caller buffer; UINT64_FORMAT ->
 * "%llu" per the header shim list. */
static void
pg_xid8out_internal(FullTransactionId fxid, char *result)
{
	snprintf(result, 21, "%llu", (unsigned long long) fxid.value);
}

/* xid8cmp body verbatim (fmgr unwrapped). */
static int32
pg_xid8cmp_internal(FullTransactionId fxid1, FullTransactionId fxid2)
{
	if (FullTransactionIdFollows(fxid1, fxid2))
		return 1;
	else if (FullTransactionIdEquals(fxid1, fxid2))
		return 0;
	else
		return -1;
}

/* ==================== SECTION 5: oid.c (VERBATIM) ==================== */

typedef struct oidvector
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for oidvector */
	int32		dataoffset;		/* always 0 for oidvector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	Oid			values[];
} oidvector;

#define OIDOID 26
#define SET_VARSIZE(v, sz) (((oidvector *) (v))->vl_len_ = ((int32) (sz)) << 2)
#define OidVectorSize(n) (offsetof(oidvector, values) + (n) * sizeof(Oid))

/* oidout body verbatim; palloc(12) -> caller buffer. */
static void
pg_oidout_internal(Oid o, char *result)
{
	snprintf(result, 12, "%u", o);
}

/* check_valid_oidvector body verbatim (ereport shim records + returns). */
static void
check_valid_oidvector(const oidvector *oidArray)
{
	/*
	 * We insist on ndim == 1 and dataoffset == 0 (that is, no nulls) because
	 * otherwise the array's layout will not be what calling code expects.  We
	 * needn't be picky about the index lower bound though.  Checking elemtype
	 * is just paranoia.
	 */
	if (oidArray->ndim != 1 ||
		oidArray->dataoffset != 0 ||
		oidArray->elemtype != OIDOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("array is not a valid oidvector")));
}

/* oidvectorin body verbatim; fmgr unwrapped, escontext NULL-shaped,
 * palloc0/repalloc -> calloc/realloc (freed by the driver entry),
 * PG_RETURN_NULL -> return NULL. */
static oidvector *
pg_oidvectorin_internal(char *oidString)
{
	Node	   *escontext = NULL;
	oidvector  *result;
	int			nalloc;
	int			n;

	nalloc = 32;				/* arbitrary initial size guess */
	result = (oidvector *) calloc(1, OidVectorSize(nalloc));

	for (n = 0;; n++)
	{
		while (*oidString && isspace((unsigned char) *oidString))
			oidString++;
		if (*oidString == '\0')
			break;

		if (n >= nalloc)
		{
			nalloc *= 2;
			result = (oidvector *) realloc(result, OidVectorSize(nalloc));
		}

		result->values[n] = uint32in_subr(oidString, &oidString,
										  "oid", escontext);
		if (SOFT_ERROR_OCCURRED(escontext))
		{
			free(result);
			return NULL;
		}
	}

	SET_VARSIZE(result, OidVectorSize(n));
	result->ndim = 1;
	result->dataoffset = 0;		/* never any nulls */
	result->elemtype = OIDOID;
	result->dim1 = n;
	result->lbound1 = 0;

	return result;
}

/* oidvectorout body verbatim; palloc -> malloc (freed by the driver). */
static char *
pg_oidvectorout_internal(oidvector *oidArray)
{
	int			num,
				nnums;
	char	   *rp;
	char	   *result;

	/* validate input before fetching dim1 */
	check_valid_oidvector(oidArray);
	if (pg_diff_errcode != 0)
		return NULL;
	nnums = oidArray->dim1;

	/* assumes sign, 10 digits, ' ' */
	rp = result = (char *) malloc(nnums * 12 + 1);
	for (num = 0; num < nnums; num++)
	{
		if (num != 0)
			*rp++ = ' ';
		sprintf(rp, "%u", oidArray->values[num]);
		while (*++rp != '\0')
			;
	}
	*rp = '\0';
	return result;
}

/* ========== SECTION 6: fuzz-facing driver entries (NOT Postgres code) ===== */

/* tidin: 0 ok (blk/off written), 1 error (pg_diff_errcode set),
 * 2 driver-refused (over the local copy buffer; Rust length-caps first). */
int
pg_diff_tidin(const char *str, uint32 *blk, uint16 *off)
{
	ItemPointerData tid;
	char		buf[1024];
	size_t		n = strlen(str);

	if (n >= sizeof(buf))
		return 2;
	memcpy(buf, str, n + 1);
	pg_diff_errcode = 0;
	if (pg_tidin_internal(buf, &tid) != 0)
		return 1;
	*blk = ItemPointerGetBlockNumberNoCheck(&tid);
	*off = ItemPointerGetOffsetNumberNoCheck(&tid);
	return 0;
}

void
pg_diff_tidout(uint32 blk, uint16 off, char *buf32)
{
	ItemPointerData tid;

	pg_diff_errcode = 0;
	ItemPointerSet(&tid, blk, off);
	pg_tidout_internal(&tid, buf32, 32);
}

/* All six tid bool comparisons + bttidcmp are `ItemPointerCompare(a,b) OP 0`
 * verbatim; the driver returns the raw comparison, the Rust side applies
 * each OP exactly as the vendored Datum bodies do. */
int32
pg_diff_bttidcmp(uint32 blk1, uint16 off1, uint32 blk2, uint16 off2)
{
	ItemPointerData a, b;

	pg_diff_errcode = 0;
	ItemPointerSet(&a, blk1, off1);
	ItemPointerSet(&b, blk2, off2);
	return ItemPointerCompare(&a, &b);
}

/* tidlarger/tidsmaller verbatim selection semantics (>= / <= pick arg1). */
void
pg_diff_tidlarger(uint32 blk1, uint16 off1, uint32 blk2, uint16 off2,
				  uint32 *blk, uint16 *off)
{
	ItemPointerData a, b;
	ItemPointer r;

	pg_diff_errcode = 0;
	ItemPointerSet(&a, blk1, off1);
	ItemPointerSet(&b, blk2, off2);
	r = ItemPointerCompare(&a, &b) >= 0 ? &a : &b;
	*blk = ItemPointerGetBlockNumberNoCheck(r);
	*off = ItemPointerGetOffsetNumberNoCheck(r);
}

void
pg_diff_tidsmaller(uint32 blk1, uint16 off1, uint32 blk2, uint16 off2,
				   uint32 *blk, uint16 *off)
{
	ItemPointerData a, b;
	ItemPointer r;

	pg_diff_errcode = 0;
	ItemPointerSet(&a, blk1, off1);
	ItemPointerSet(&b, blk2, off2);
	r = ItemPointerCompare(&a, &b) <= 0 ? &a : &b;
	*blk = ItemPointerGetBlockNumberNoCheck(r);
	*off = ItemPointerGetOffsetNumberNoCheck(r);
}

void
pg_diff_xidout(uint32 xid, char *buf16)
{
	pg_diff_errcode = 0;
	pg_xidout_internal(xid, buf16);
}

int
pg_diff_xideq(uint32 x1, uint32 x2)
{
	pg_diff_errcode = 0;
	return TransactionIdEquals(x1, x2) ? 1 : 0;
}

int
pg_diff_xidneq(uint32 x1, uint32 x2)
{
	pg_diff_errcode = 0;
	return !TransactionIdEquals(x1, x2) ? 1 : 0;
}

int32
pg_diff_xid8cmp(uint64 a, uint64 b)
{
	FullTransactionId fa, fb;

	pg_diff_errcode = 0;
	fa.value = a;
	fb.value = b;
	return pg_xid8cmp_internal(fa, fb);
}

/* The six xid8 bool comparisons, verbatim macro semantics:
 * which: 0 eq, 1 ne, 2 lt (Precedes), 3 gt (Follows), 4 le, 5 ge. */
int
pg_diff_xid8rel(int which, uint64 a, uint64 b)
{
	FullTransactionId fa, fb;

	pg_diff_errcode = 0;
	fa.value = a;
	fb.value = b;
	switch (which)
	{
		case 0: return FullTransactionIdEquals(fa, fb) ? 1 : 0;
		case 1: return !FullTransactionIdEquals(fa, fb) ? 1 : 0;
		case 2: return FullTransactionIdPrecedes(fa, fb) ? 1 : 0;
		case 3: return FullTransactionIdFollows(fa, fb) ? 1 : 0;
		case 4: return FullTransactionIdPrecedesOrEquals(fa, fb) ? 1 : 0;
		default: return FullTransactionIdFollowsOrEquals(fa, fb) ? 1 : 0;
	}
}

uint64
pg_diff_xid8_larger(uint64 a, uint64 b)
{
	FullTransactionId fa, fb;

	pg_diff_errcode = 0;
	fa.value = a;
	fb.value = b;
	if (FullTransactionIdFollows(fa, fb))
		return fa.value;
	else
		return fb.value;
}

uint64
pg_diff_xid8_smaller(uint64 a, uint64 b)
{
	FullTransactionId fa, fb;

	pg_diff_errcode = 0;
	fa.value = a;
	fb.value = b;
	if (FullTransactionIdPrecedes(fa, fb))
		return fa.value;
	else
		return fb.value;
}

void
pg_diff_xid8out(uint64 v, char *buf21)
{
	FullTransactionId f;

	pg_diff_errcode = 0;
	f.value = v;
	pg_xid8out_internal(f, buf21);
}

/* The six oid bool comparisons, verbatim one-liner semantics:
 * which: 0 eq, 1 ne, 2 lt, 3 le, 4 ge, 5 gt. */
int
pg_diff_oidrel(int which, uint32 arg1, uint32 arg2)
{
	pg_diff_errcode = 0;
	switch (which)
	{
		case 0: return (arg1 == arg2) ? 1 : 0;
		case 1: return (arg1 != arg2) ? 1 : 0;
		case 2: return (arg1 < arg2) ? 1 : 0;
		case 3: return (arg1 <= arg2) ? 1 : 0;
		case 4: return (arg1 >= arg2) ? 1 : 0;
		default: return (arg1 > arg2) ? 1 : 0;
	}
}

uint32
pg_diff_oidlarger(uint32 arg1, uint32 arg2)
{
	pg_diff_errcode = 0;
	return (arg1 > arg2) ? arg1 : arg2;
}

uint32
pg_diff_oidsmaller(uint32 arg1, uint32 arg2)
{
	pg_diff_errcode = 0;
	return (arg1 < arg2) ? arg1 : arg2;
}

/* oidin (and the xidin/cidin siblings): uint32in_subr with endloc == NULL,
 * exactly the verbatim wrapper body. 0 ok / 1 error. */
int
pg_diff_uint32in(const char *s, uint32 *out)
{
	pg_diff_errcode = 0;
	*out = uint32in_subr(s, NULL, "oid", NULL);
	return pg_diff_errcode != 0;
}

/* xid8in surface: uint64in_subr, endloc == NULL. 0 ok / 1 error. */
int
pg_diff_uint64in(const char *s, uint64 *out)
{
	pg_diff_errcode = 0;
	*out = uint64in_subr(s, NULL, "xid8", NULL);
	return pg_diff_errcode != 0;
}

void
pg_diff_oidout(uint32 o, char *buf12)
{
	pg_diff_errcode = 0;
	pg_oidout_internal(o, buf12);
}

/*
 * oidvectorin: 0 ok (*n = count, values written up to cap), 1 error
 * (pg_diff_errcode set), 2 driver-refused (input over the copy buffer;
 * Rust length-caps first so this cannot fire in practice).
 */
int
pg_diff_oidvectorin(const char *s, uint32 *values, int32 cap, int32 *n)
{
	char		buf[1024];
	size_t		len = strlen(s);
	oidvector  *v;
	int			i;

	if (len >= sizeof(buf))
		return 2;
	memcpy(buf, s, len + 1);
	pg_diff_errcode = 0;
	v = pg_oidvectorin_internal(buf);
	if (v == NULL)
		return 1;
	*n = v->dim1;
	for (i = 0; i < v->dim1 && i < cap; i++)
		values[i] = v->values[i];
	free(v);
	return 0;
}

/*
 * oidvectorout over a well-formed vector (the SQL-boundary shape the Rust
 * wrapper receives): 0 ok (cstring copied into buf, bufcap bytes),
 * 1 errcode recorded, 2 driver-refused (caller buffer too small).
 */
int
pg_diff_oidvectorout(const uint32 *values, int32 n, char *buf, int32 bufcap)
{
	oidvector  *v;
	char	   *r;
	size_t		rl;

	pg_diff_errcode = 0;
	v = (oidvector *) calloc(1, OidVectorSize((size_t) n));
	SET_VARSIZE(v, OidVectorSize((size_t) n));
	v->ndim = 1;
	v->dataoffset = 0;
	v->elemtype = OIDOID;
	v->dim1 = n;
	v->lbound1 = 0;
	memcpy(v->values, values, (size_t) n * sizeof(Oid));
	r = pg_oidvectorout_internal(v);
	free(v);
	if (r == NULL)
		return 1;
	rl = strlen(r);
	if (rl + 1 > (size_t) bufcap)
	{
		free(r);
		return 2;
	}
	memcpy(buf, r, rl + 1);
	free(r);
	return 0;
}

/* ========== SECTION 7 (coverage-extension pass, p1-lanep round 2) ==========
 * Additional verbatim vendored C + driver entries for the builtins arms the
 * first-round driver never dispatched (cid family, xid8in/xid8toxid, send
 * wire images, hash wrappers, oidvector cmp family). Provenance additions:
 *   - src/common/hashfn.c + src/include/port/pg_bitutils.h @ 62d6c7d3df:
 *     hashfn_verbatim.inc (mechanically extracted; see its header). The
 *     four hash entry points are renamed to pg_sx_* via #define so this
 *     translation unit exports no PostgreSQL-named symbols (plumbing shim).
 *   - src/backend/access/hash/hashfunc.c @ same ref: hashint8 body
 *     (backs hashxid8; fmgr unwrapped).
 *   - src/backend/access/nbtree/nbtcompare.c @ same ref: btoidvectorcmp
 *     body + its A_LESS_THAN_B/A_GREATER_THAN_B (-1/1 non-index arm, the
 *     values SQL comparisons see through fmgr).
 *   - src/include/access/transam.h @ same ref: XidFromFullTransactionId.
 *   - src/backend/utils/adt/xid.c @ same ref: cidout body (identical
 *     snprintf %lu shape to xidout), cideq body.
 *   - send drivers: pq_begintypsend/pq_sendintXX/pq_endtypsend are
 *     StringInfo plumbing; their wire effect on these fixed-width sends is
 *     exactly the big-endian byte image of the value(s) in declaration
 *     order (pq_sendint32 block + pq_sendint16 offset for tidsend). The
 *     drivers below emit that image directly — byte-order transform per
 *     pg_hton semantics, environment elided, never value logic.
 */

#define hash_bytes pg_sx_hash_bytes
#define hash_bytes_extended pg_sx_hash_bytes_extended
#define hash_bytes_uint32 pg_sx_hash_bytes_uint32
#define hash_bytes_uint32_extended pg_sx_hash_bytes_uint32_extended
#include "hashfn_verbatim.inc"
#undef hash_bytes
#undef hash_bytes_extended
#undef hash_bytes_uint32
#undef hash_bytes_uint32_extended

/* transam.h VERBATIM */
#define XidFromFullTransactionId(x)		((uint32) (x).value)

/* cidout body verbatim (xid.c); palloc(16) -> caller buffer. */
static void
pg_cidout_internal(uint32 c, char *result)
{
	snprintf(result, 16, "%lu", (unsigned long) c);
}

/* nbtcompare.c btoidvectorcmp body verbatim (fmgr unwrapped; the -1/1
 * non-index constants per its #else arm). */
#define A_LESS_THAN_B		(-1)
#define A_GREATER_THAN_B	1

static int32
pg_btoidvectorcmp_internal(oidvector *a, oidvector *b)
{
	int			i;

	check_valid_oidvector(a);
	check_valid_oidvector(b);

	/* We arbitrarily choose to sort first by vector length */
	if (a->dim1 != b->dim1)
		return a->dim1 - b->dim1;

	for (i = 0; i < a->dim1; i++)
	{
		if (a->values[i] != b->values[i])
		{
			if (a->values[i] > b->values[i])
				return A_GREATER_THAN_B;
			else
				return A_LESS_THAN_B;
		}
	}
	return 0;
}

/* ---- driver entries ---- */

void
pg_diff_cidout(uint32 c, char *buf16)
{
	pg_diff_errcode = 0;
	pg_cidout_internal(c, buf16);
}

/* cideq body verbatim: (arg1 == arg2). */
int
pg_diff_cideq(uint32 a, uint32 b)
{
	pg_diff_errcode = 0;
	return (a == b) ? 1 : 0;
}

uint32
pg_diff_xid8toxid(uint64 v)
{
	FullTransactionId f;

	pg_diff_errcode = 0;
	f.value = v;
	return XidFromFullTransactionId(f);
}

/* Send wire images (see SECTION 7 header note). */
void
pg_diff_send32(uint32 v, unsigned char *out4)
{
	pg_diff_errcode = 0;
	out4[0] = (unsigned char) (v >> 24);
	out4[1] = (unsigned char) (v >> 16);
	out4[2] = (unsigned char) (v >> 8);
	out4[3] = (unsigned char) v;
}

void
pg_diff_send64(uint64 v, unsigned char *out8)
{
	pg_diff_errcode = 0;
	out8[0] = (unsigned char) (v >> 56);
	out8[1] = (unsigned char) (v >> 48);
	out8[2] = (unsigned char) (v >> 40);
	out8[3] = (unsigned char) (v >> 32);
	out8[4] = (unsigned char) (v >> 24);
	out8[5] = (unsigned char) (v >> 16);
	out8[6] = (unsigned char) (v >> 8);
	out8[7] = (unsigned char) v;
}

/* tidsend: pq_sendint32(block) then pq_sendint16(offset) -> 6-byte image. */
void
pg_diff_tidsend(uint32 blk, uint16 off, unsigned char *out6)
{
	pg_diff_errcode = 0;
	pg_diff_send32(blk, out6);
	out6[4] = (unsigned char) (off >> 8);
	out6[5] = (unsigned char) off;
}

/* hash_uint32 / hash_uint32_extended (hashfn.c, via the pg_sx_ renames). */
uint32
pg_diff_hash_uint32(uint32 k)
{
	pg_diff_errcode = 0;
	return pg_sx_hash_bytes_uint32(k);
}

uint64
pg_diff_hash_uint32_extended(uint32 k, uint64 seed)
{
	pg_diff_errcode = 0;
	return pg_sx_hash_bytes_uint32_extended(k, seed);
}

/* hashint8 body verbatim (hashfunc.c; backs hashxid8). hash_uint32(x) in
 * real PG is hash_bytes_uint32 datum-wrapped. */
uint32
pg_diff_hashint8(int64 val)
{
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	pg_diff_errcode = 0;
	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_sx_hash_bytes_uint32(lohalf);
}

uint64
pg_diff_hashint8extended(int64 val, uint64 seed)
{
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	pg_diff_errcode = 0;
	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return pg_sx_hash_bytes_uint32_extended(lohalf, seed);
}

/* hashtid body verbatim semantics (tid.c): hash_any over the 6 raw field
 * bytes (sizeof(BlockIdData) + sizeof(OffsetNumber)); hash_any == hash_bytes
 * (hashfn.h maps the Datum macro). */
uint32
pg_diff_hashtid(uint32 blk, uint16 off)
{
	ItemPointerData key;

	pg_diff_errcode = 0;
	ItemPointerSet(&key, blk, off);
	return pg_sx_hash_bytes((const unsigned char *) &key,
							sizeof(BlockIdData) + sizeof(OffsetNumber));
}

uint64
pg_diff_hashtidextended(uint32 blk, uint16 off, uint64 seed)
{
	ItemPointerData key;

	pg_diff_errcode = 0;
	ItemPointerSet(&key, blk, off);
	return pg_sx_hash_bytes_extended((const unsigned char *) &key,
									 sizeof(BlockIdData) + sizeof(OffsetNumber),
									 seed);
}

/* Build a header-valid oidvector on the C side (SQL-boundary shape). */
static oidvector *
pg_sx_build_oidvector(const uint32 *values, int32 n)
{
	oidvector  *v = (oidvector *) calloc(1, OidVectorSize((size_t) n));

	SET_VARSIZE(v, OidVectorSize((size_t) n));
	v->ndim = 1;
	v->dataoffset = 0;
	v->elemtype = OIDOID;
	v->dim1 = n;
	v->lbound1 = 0;
	memcpy(v->values, values, (size_t) n * sizeof(Oid));
	return v;
}

int32
pg_diff_btoidvectorcmp(const uint32 *va, int32 na, const uint32 *vb, int32 nb)
{
	oidvector  *a = pg_sx_build_oidvector(va, na);
	oidvector  *b = pg_sx_build_oidvector(vb, nb);
	int32		r;

	pg_diff_errcode = 0;
	r = pg_btoidvectorcmp_internal(a, b);
	free(a);
	free(b);
	return r;
}

/* hashoidvector body verbatim semantics (hashfunc.c): check_valid then
 * hash_any over dim1 * sizeof(Oid) value bytes. */
uint32
pg_diff_hashoidvector(const uint32 *values, int32 n)
{
	oidvector  *key = pg_sx_build_oidvector(values, n);
	uint32		r;

	pg_diff_errcode = 0;
	check_valid_oidvector(key);
	r = pg_sx_hash_bytes((const unsigned char *) key->values,
						 key->dim1 * sizeof(Oid));
	free(key);
	return r;
}

uint64
pg_diff_hashoidvectorextended(const uint32 *values, int32 n, uint64 seed)
{
	oidvector  *key = pg_sx_build_oidvector(values, n);
	uint64		r;

	pg_diff_errcode = 0;
	check_valid_oidvector(key);
	r = pg_sx_hash_bytes_extended((const unsigned char *) key->values,
								  key->dim1 * sizeof(Oid), seed);
	free(key);
	return r;
}
