/*
 * Verbatim PostgreSQL C for the scalar-misc comparator batch:
 *   - "char" comparators + btcharcmp
 *   - tid comparators + bttidcmp (ItemPointerCompare)
 *   - xid equality (xideq/xidneq), xid8 comparators + xid8cmp
 *   - oidvector comparators (btoidvectorcmp + the oid.c wrappers)
 *
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * Provenance (fetched 2026-07-28, postgres/postgres master):
 *   src/backend/utils/adt/char.c        (chareq..charge bodies)
 *   src/backend/utils/adt/tid.c         (tideq..tidge, bttidcmp bodies)
 *   src/backend/utils/adt/xid.c         (xideq/xidneq, xid8* bodies)
 *   src/backend/utils/adt/oid.c         (check_valid_oidvector, oidvector*
 *                                        bodies; oidvectoreq..gt all route
 *                                        through btoidvectorcmp(fcinfo))
 *   src/backend/access/nbtree/nbtcompare.c (btcharcmp, btoidvectorcmp bodies)
 *   src/backend/storage/page/itemptr.c  (ItemPointerCompare body)
 *   src/include/storage/block.h         (BlockIdData, BlockIdGetBlockNumber)
 *   src/include/storage/itemptr.h       (ItemPointerData,
 *                                        ItemPointerGet{Block,Offset}NumberNoCheck)
 *   src/include/access/transam.h        (TransactionIdEquals,
 *                                        FullTransactionId + Precedes/Follows/
 *                                        Equals macros)
 *
 * Shims (plumbing only, never logic):
 *   1. PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_* unwrapped to plain C
 *      signatures around the verbatim expression/statement bodies.
 *      bool returns ride as int (Kani lowers Rust () / bool FFI awkwardly).
 *   2. Basic typedefs (int32, uint16, Oid, ...) supplied from <stdint.h>.
 *   3. "char" args are declared `signed char`: for THESE functions the
 *      semantics are signedness-independent (eq/ne are plain equality;
 *      lt/le/gt/ge and btcharcmp cast through (uint8) explicitly, exactly
 *      as the originals do), so pinning the signedness only removes
 *      target-dependent ambiguity (aarch64-linux char is unsigned,
 *      aarch64-darwin signed).
 *   4. oidvector's FLEXIBLE_ARRAY_MEMBER values[] shimmed to values[4]
 *      (harness bound: dim1 <= 4).
 *   5. check_valid_oidvector's ereport(ERROR) shimmed to set
 *      pg_oidvector_error and return; harnesses feed only layout-valid
 *      headers (ndim==1, dataoffset==0, elemtype==OIDOID), so the error
 *      path is unreachable and stays OUT of the proof (value-space only).
 *   6. tid comparators take the six u16 fields (bi_hi, bi_lo, ip_posid x2)
 *      and build ItemPointerData locally — same bytes fmgr would hand the
 *      real function; the block-number composition
 *      (bi_hi << 16) | bi_lo is the verbatim BlockIdGetBlockNumber.
 *   7. oidvector entry points take const void* (cast to oidvector* inside)
 *      so goto-cc's Rust<->C symbol type-check doesn't have to unify two
 *      independently-declared struct types.
 */

#include <stdint.h>

typedef int32_t int32;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32 Oid;
typedef uint32 TransactionId;
typedef uint32 BlockNumber;
typedef uint16 OffsetNumber;

#define OIDOID 26

/* ------------------------------------------------------------------ */
/* char.c: "comparisons are done as though char is unsigned (uint8)"   */
/* ------------------------------------------------------------------ */

int pg_chareq(signed char arg1, signed char arg2)
{
	return arg1 == arg2;
}

int pg_charne(signed char arg1, signed char arg2)
{
	return arg1 != arg2;
}

int pg_charlt(signed char arg1, signed char arg2)
{
	return (uint8) arg1 < (uint8) arg2;
}

int pg_charle(signed char arg1, signed char arg2)
{
	return (uint8) arg1 <= (uint8) arg2;
}

int pg_chargt(signed char arg1, signed char arg2)
{
	return (uint8) arg1 > (uint8) arg2;
}

int pg_charge(signed char arg1, signed char arg2)
{
	return (uint8) arg1 >= (uint8) arg2;
}

/* nbtcompare.c: "Be careful to compare chars as unsigned" */
int32 pg_btcharcmp(signed char a, signed char b)
{
	return (int32) ((uint8) a) - (int32) ((uint8) b);
}

/* ------------------------------------------------------------------ */
/* tid.c / itemptr.c / block.h                                          */
/* ------------------------------------------------------------------ */

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

/* block.h, verbatim */
static BlockNumber
BlockIdGetBlockNumber(const BlockIdData *blockId)
{
	return (((BlockNumber) blockId->bi_hi) << 16) | ((BlockNumber) blockId->bi_lo);
}

/* itemptr.h, verbatim */
static BlockNumber
ItemPointerGetBlockNumberNoCheck(const ItemPointerData *pointer)
{
	return BlockIdGetBlockNumber(&pointer->ip_blkid);
}

static OffsetNumber
ItemPointerGetOffsetNumberNoCheck(const ItemPointerData *pointer)
{
	return pointer->ip_posid;
}

/* storage/page/itemptr.c, verbatim */
static int32
ItemPointerCompare(const ItemPointerData *arg1, const ItemPointerData *arg2)
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

#define PG_TID2(name, expr) \
	int pg_##name(uint16 a_hi, uint16 a_lo, uint16 a_off, \
				  uint16 b_hi, uint16 b_lo, uint16 b_off) \
	{ \
		ItemPointerData arg1 = {{a_hi, a_lo}, a_off}; \
		ItemPointerData arg2 = {{b_hi, b_lo}, b_off}; \
		return expr; \
	}

/* tid.c bodies: PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) OP 0) */
PG_TID2(tideq, ItemPointerCompare(&arg1, &arg2) == 0)
PG_TID2(tidne, ItemPointerCompare(&arg1, &arg2) != 0)
PG_TID2(tidlt, ItemPointerCompare(&arg1, &arg2) < 0)
PG_TID2(tidle, ItemPointerCompare(&arg1, &arg2) <= 0)
PG_TID2(tidgt, ItemPointerCompare(&arg1, &arg2) > 0)
PG_TID2(tidge, ItemPointerCompare(&arg1, &arg2) >= 0)

int32 pg_bttidcmp(uint16 a_hi, uint16 a_lo, uint16 a_off,
				  uint16 b_hi, uint16 b_lo, uint16 b_off)
{
	ItemPointerData arg1 = {{a_hi, a_lo}, a_off};
	ItemPointerData arg2 = {{b_hi, b_lo}, b_off};

	return ItemPointerCompare(&arg1, &arg2);
}

/* ------------------------------------------------------------------ */
/* xid.c + transam.h                                                    */
/* ------------------------------------------------------------------ */

/* transam.h, verbatim */
#define TransactionIdEquals(id1, id2)	((id1) == (id2))

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;

#define FullTransactionIdEquals(a, b)	((a).value == (b).value)
#define FullTransactionIdPrecedes(a, b)	((a).value < (b).value)
#define FullTransactionIdPrecedesOrEquals(a, b) ((a).value <= (b).value)
#define FullTransactionIdFollows(a, b) ((a).value > (b).value)
#define FullTransactionIdFollowsOrEquals(a, b) ((a).value >= (b).value)

int pg_xideq(TransactionId xid1, TransactionId xid2)
{
	return TransactionIdEquals(xid1, xid2);
}

int pg_xidneq(TransactionId xid1, TransactionId xid2)
{
	return !TransactionIdEquals(xid1, xid2);
}

#define PG_XID8_2(name, MACRO) \
	int pg_##name(uint64 v1, uint64 v2) \
	{ \
		FullTransactionId fxid1 = {v1}; \
		FullTransactionId fxid2 = {v2}; \
		return MACRO(fxid1, fxid2); \
	}

PG_XID8_2(xid8eq, FullTransactionIdEquals)
PG_XID8_2(xid8lt, FullTransactionIdPrecedes)
PG_XID8_2(xid8gt, FullTransactionIdFollows)
PG_XID8_2(xid8le, FullTransactionIdPrecedesOrEquals)
PG_XID8_2(xid8ge, FullTransactionIdFollowsOrEquals)

int pg_xid8ne(uint64 v1, uint64 v2)
{
	FullTransactionId fxid1 = {v1};
	FullTransactionId fxid2 = {v2};

	return !FullTransactionIdEquals(fxid1, fxid2);
}

/* xid.c xid8cmp body, verbatim if/else chain */
int32 pg_xid8cmp(uint64 v1, uint64 v2)
{
	FullTransactionId fxid1 = {v1};
	FullTransactionId fxid2 = {v2};

	if (FullTransactionIdFollows(fxid1, fxid2))
		return 1;
	else if (FullTransactionIdEquals(fxid1, fxid2))
		return 0;
	else
		return -1;
}

/*
 * xid8_larger / xid8_smaller / xid8toxid (pg_proc oids 5097/5098/5071),
 * vendored from REL_18_STABLE src/backend/utils/adt/xid.c (fetched
 * 2026-07-28). SHIMS: fmgr wrappers -> plain uint64 signatures
 * (PG_GETARG_FULLTRANSACTIONID -> FullTransactionId built from the u64;
 * PG_RETURN_FULLTRANSACTIONID -> return .value;
 * XidFromFullTransactionId(x) = (TransactionId) ((x).value) per
 * access/transam.h, PG_RETURN_TRANSACTIONID -> return uint32).
 * Selection/conversion expressions verbatim.
 */

#define XidFromFullTransactionId(x) ((TransactionId) ((x).value))

uint64 pg_xid8_larger(uint64 v1, uint64 v2)
{
	FullTransactionId fxid1 = {v1};
	FullTransactionId fxid2 = {v2};

	if (FullTransactionIdFollows(fxid1, fxid2))
		return fxid1.value;
	else
		return fxid2.value;
}

uint64 pg_xid8_smaller(uint64 v1, uint64 v2)
{
	FullTransactionId fxid1 = {v1};
	FullTransactionId fxid2 = {v2};

	if (FullTransactionIdPrecedes(fxid1, fxid2))
		return fxid1.value;
	else
		return fxid2.value;
}

TransactionId pg_xid8toxid(uint64 v)
{
	FullTransactionId fxid = {v};

	return XidFromFullTransactionId(fxid);
}

/* ------------------------------------------------------------------ */
/* oidvector: oid.c + nbtcompare.c                                      */
/* ------------------------------------------------------------------ */

/* c.h oidvector, FLEXIBLE_ARRAY_MEMBER shimmed to [4] (harness cap) */
typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* number of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;
	int			dim1;			/* number of elements */
	int			lbound1;		/* lower bound, usually 1 */
	Oid			values[4];		/* [shim] FLEXIBLE_ARRAY_MEMBER */
} oidvector;

/* nbtcompare.c */
#define A_LESS_THAN_B		(-1)
#define A_GREATER_THAN_B	1

/* [shim] ereport(ERROR) -> flag; unreachable under the harness fence */
int			pg_oidvector_error = 0;

/* oid.c check_valid_oidvector, verbatim condition */
static void
check_valid_oidvector(const oidvector *oidArray)
{
	if (oidArray->ndim != 1 ||
		oidArray->dataoffset != 0 ||
		oidArray->elemtype != OIDOID)
		pg_oidvector_error = 1;		/* [shim] was ereport(ERROR, ...) */
}

/* nbtcompare.c btoidvectorcmp, verbatim body */
static int32
pg_btoidvectorcmp_core(const oidvector *a, const oidvector *b)
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

/* const void* entry points: see shim 7 in the header comment */
int32 pg_btoidvectorcmp(const void *a, const void *b)
{
	return pg_btoidvectorcmp_core((const oidvector *) a, (const oidvector *) b);
}

/* oid.c: every oidvector op is DatumGetInt32(btoidvectorcmp(fcinfo)) OP 0 */
int pg_oidvectoreq(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) == 0;
}

int pg_oidvectorne(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) != 0;
}

int pg_oidvectorlt(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) < 0;
}

int pg_oidvectorle(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) <= 0;
}

int pg_oidvectorge(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) >= 0;
}

int pg_oidvectorgt(const void *a, const void *b)
{
	return pg_btoidvectorcmp(a, b) > 0;
}

/*
 * tidlarger / tidsmaller (pg_proc oids 2795/2796), vendored from
 * REL_18_STABLE src/backend/utils/adt/tid.c (fetched 2026-07-28).
 * SHIMS: fmgr wrappers -> the same u16-triple signatures as the tid
 * comparators above; PG_RETURN_ITEMPOINTER(winner) -> return 1 when the
 * winner is arg1, 2 when arg2 (winning-input identity, bytea_larger
 * pattern). Selection expressions verbatim.
 */

int
pg_tidlarger(uint16 ah, uint16 al, uint16 ao,
			 uint16 bh, uint16 bl, uint16 bo)
{
	ItemPointerData arg1_ = {{ah, al}, ao};
	ItemPointerData arg2_ = {{bh, bl}, bo};
	ItemPointerData *arg1 = &arg1_;
	ItemPointerData *arg2 = &arg2_;

	return (ItemPointerCompare(arg1, arg2) >= 0 ? 1 : 2);
}

int
pg_tidsmaller(uint16 ah, uint16 al, uint16 ao,
			  uint16 bh, uint16 bl, uint16 bo)
{
	ItemPointerData arg1_ = {{ah, al}, ao};
	ItemPointerData arg2_ = {{bh, bl}, bo};
	ItemPointerData *arg1 = &arg1_;
	ItemPointerData *arg2 = &arg2_;

	return (ItemPointerCompare(arg1, arg2) <= 0 ? 1 : 2);
}

/* ==================================================================== */
/* WAVE 5 (2026-07-28): cid rows, xid_age/mxid_age seam cores, and the  */
/* xid/cid/oid/xid8/tid wire (recv/send) rows.                           */
/*                                                                       */
/* Provenance (fetched 2026-07-28, REL_18_STABLE):                       */
/*   src/backend/utils/adt/xid.c      (cideq, xid_age, mxid_age,         */
/*                                     xidrecv/xidsend bodies; cidrecv/  */
/*                                     cidsend are the same bodies over  */
/*                                     CommandId)                        */
/*   src/backend/utils/adt/oid.c     (oidrecv/oidsend bodies)            */
/*   src/backend/utils/adt/tid.c     (tidrecv/tidsend bodies)            */
/*   src/backend/libpq/pqformat.c    (pq_copymsgbytes, pq_getmsgint,     */
/*                                     pq_getmsgint64, pq_begintypsend,  */
/*                                     pq_sendint16/32, pq_endtypsend)   */
/*                                                                       */
/* Shims (plumbing only, never logic) — the proofs/int-arith wire-shim   */
/* conventions, copied verbatim where the bodies coincide:               */
/*   W1. StringInfo -> (const unsigned char *data, int32 len,            */
/*       int32 *cursor) triple on the recv side; the send side's         */
/*       palloc'd StringInfoData -> a caller-provided fixed buffer.      */
/*   W2. ereport(ERRCODE_PROTOCOL_VIOLATION, "insufficient data left in  */
/*       message") -> status 4 (sqlstate 08P01 asserted Rust-side).      */
/*   W3. pg_ntoh16/32/64 / pq_sendintNN byte emission: the little-endian */
/*       byte-swap arm of port/pg_bswap.h written as explicit byte       */
/*       shifts/stores (production targets are little-endian; the        */
/*       theorem compares wire bytes, which are endian-invariant).       */
/*   W4. pq_endtypsend's SET_VARSIZE(result, buf->len): varatt.h 4B      */
/*       little-endian header ((uint32) len << 2), stored byte-wise.     */
/*   W5. xid_age's GetStableLatestTransactionId() / mxid_age's           */
/*       ReadNextMultiXactId() state reads become the `now` PARAMETER    */
/*       (state-seam pattern: the harness feeds ONE shared symbolic      */
/*       value to both sides and proves over ALL seam outputs; a skew    */
/*       control must fail).                                             */
/*   W6. INT_MAX spelled 0x7fffffff (limits.h value, avoids the header). */
/*                                                                       */
/* NOTE on xidout/cidout: the C bodies are snprintf("%lu") — libc printf */
/* has no CBMC model. The C reference used by the eq_xidout/eq_cidout    */
/* harnesses is PostgreSQL's own pg_ultoa_n (numutils.c, vendored in     */
/* ../intout/c/pg_intout.c — pass it as a second --c-lib), whose output  */
/* equals %lu's canonical decimal for every uint32 by the printf spec.   */
/* This is a documented SPEC-LEVEL ANCHOR, not a verbatim-body proof.    */
/* ==================================================================== */

typedef uint32 CommandId;
typedef uint32 MultiXactId;

#define PG_PROOF_INT_MAX 0x7fffffff	/* [shim W6] limits.h INT_MAX */

/* xid.c cideq, verbatim body */
int pg_cideq(CommandId arg1, CommandId arg2)
{
	return arg1 == arg2;
}

/* xid.c xid_age, verbatim body below the seam read ([shim W5]: `now`
 * = GetStableLatestTransactionId() output as a parameter) */
int32 pg_xid_age(TransactionId now, TransactionId xid)
{
	/* Permanent XIDs are always infinitely old */
	if (!(xid >= 3))			/* !TransactionIdIsNormal(xid), transam.h:
								 * FirstNormalTransactionId == 3 */
		return PG_PROOF_INT_MAX;

	return (int32) (now - xid);
}

/* xid.c mxid_age, verbatim body below the seam read ([shim W5]: `now`
 * = ReadNextMultiXactId() output as a parameter) */
int32 pg_mxid_age(MultiXactId now, TransactionId xid)
{
	if (!(xid != 0))			/* !MultiXactIdIsValid(xid):
								 * InvalidMultiXactId == 0 */
		return PG_PROOF_INT_MAX;

	return (int32) (now - xid);
}

/* ---- pqformat.c core, shims W1-W2 (identical to proofs/int-arith) ---- */

#include <string.h>

#define PG_OK 0
#define PG_ERR_PROTOCOL 4		/* ERRCODE_PROTOCOL_VIOLATION (08P01) */

static int
pg_pq_copymsgbytes(const unsigned char *data, int32 len, int32 *cursor,
				   void *buf, int32 datalen)
{
	if (datalen < 0 || datalen > (len - *cursor))
		return PG_ERR_PROTOCOL; /* insufficient data left in message */
	memcpy(buf, &data[*cursor], datalen);
	*cursor += datalen;
	return PG_OK;
}

/* pq_getmsgint(buf, 4) -> uint32: oidrecv / xidrecv / cidrecv body */
int
pg_getmsguint32(const unsigned char *data, int32 len, int32 *cursor,
				uint32 *out)
{
	unsigned char b[4];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 4);

	if (st != PG_OK)
		return st;
	*out = ((uint32) b[0] << 24) | ((uint32) b[1] << 16) |
		((uint32) b[2] << 8) | (uint32) b[3];
	return PG_OK;
}

/* pq_getmsgint(buf, 2) -> uint16 (tidrecv's OffsetNumber read) */
static int
pg_getmsguint16(const unsigned char *data, int32 len, int32 *cursor,
				uint16 *out)
{
	unsigned char b[2];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 2);

	if (st != PG_OK)
		return st;
	*out = (uint16) (((uint16) b[0] << 8) | (uint16) b[1]);
	return PG_OK;
}

/* pq_getmsgint64 -> uint64: xid8recv body */
int
pg_getmsguint64(const unsigned char *data, int32 len, int32 *cursor,
				uint64 *out)
{
	unsigned char b[8];
	int			st = pg_pq_copymsgbytes(data, len, cursor, b, 8);
	uint64		v = 0;
	int			i;

	if (st != PG_OK)
		return st;
	for (i = 0; i < 8; i++)
		v = (v << 8) | (uint64) b[i];
	*out = v;
	return PG_OK;
}

/* tid.c tidrecv: blockNumber = pq_getmsgint(buf, 4);
 *                offsetNumber = pq_getmsgint(buf, 2); */
int
pg_tidrecv(const unsigned char *data, int32 len, int32 *cursor,
		   uint32 *block, uint16 *offset)
{
	int			st = pg_getmsguint32(data, len, cursor, block);

	if (st != PG_OK)
		return st;
	return pg_getmsguint16(data, len, cursor, offset);
}

/* shared endtypsend tail, shim W4 */
static void
pg_set_varsize_4b(unsigned char *out, int32 len)
{
	uint32		hdr = (uint32) len << 2;

	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
}

/* oidsend / xidsend / cidsend: pq_sendint32 of the uint32 */
int32
pg_send_uint32(uint32 arg1, unsigned char *out /* [8] */ )
{
	out[4] = (unsigned char) ((arg1 >> 24) & 0xFF);
	out[5] = (unsigned char) ((arg1 >> 16) & 0xFF);
	out[6] = (unsigned char) ((arg1 >> 8) & 0xFF);
	out[7] = (unsigned char) (arg1 & 0xFF);
	pg_set_varsize_4b(out, 8);
	return 8;
}

/* xid8send: pq_sendint64 of the u64 (xid8 wire value) */
int32
pg_send_uint64(uint64 v, unsigned char *out /* [12] */ )
{
	int			i;

	for (i = 0; i < 8; i++)
		out[4 + i] = (unsigned char) ((v >> (8 * (7 - i))) & 0xFF);
	pg_set_varsize_4b(out, 12);
	return 12;
}

/* tid.c tidsend: pq_sendint32(blockNumber) + pq_sendint16(offsetNumber) */
int32
pg_tidsend(uint32 block, uint16 offset, unsigned char *out /* [10] */ )
{
	out[4] = (unsigned char) ((block >> 24) & 0xFF);
	out[5] = (unsigned char) ((block >> 16) & 0xFF);
	out[6] = (unsigned char) ((block >> 8) & 0xFF);
	out[7] = (unsigned char) (block & 0xFF);
	out[8] = (unsigned char) ((offset >> 8) & 0xFF);
	out[9] = (unsigned char) (offset & 0xFF);
	pg_set_varsize_4b(out, 10);
	return 10;
}
