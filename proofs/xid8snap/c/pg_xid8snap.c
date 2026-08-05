/*
 * pg_xid8snap.c — vendored C for the pg_snapshot / xid8funcs proof family.
 *
 * PROVENANCE: src/backend/utils/adt/xid8funcs.c and
 * src/include/access/transam.h, REL_18_STABLE, fetched 2026-07-30 from
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE. Verbatim
 * upstream copies kept alongside for diffing: xid8funcs_upstream.c,
 * transam_upstream.h (reference only, not compiled).
 *
 * SHIM MANIFEST — every deviation from upstream, exhaustively:
 *  [S1] postgres.h and friends replaced by ../../support/c/pg_proof_shim.h;
 *       TransactionId / FullTransactionId typedefs and the transam.h
 *       macros used are vendored VERBATIM below (line refs given).
 *       FLEXIBLE_ARRAY_MEMBER is defined empty (C99 flexible array).
 *  [S2] Assert is a no-op via the shared shim header (production
 *       non-cassert build). The Asserts inside
 *       FullTransactionIdFromAllowableAt state the CALLER CONTRACT
 *       (xid allowable at nextFullXid; epoch != 0 on the decrement
 *       branch); the harness fences its domain to that contract and the
 *       fence is recorded in the ledger bounds.
 *  [S3] bsearch(): libc call, no CBMC model — replaced by
 *       pg_proof_bsearch, a plain binary search with the libc contract
 *       (array sorted per cmp; returns a pointer to A matching element or
 *       NULL). Plumbing-only shim, same class as the shared header's
 *       C-locale ctype helpers. is_visible_fxid's body is otherwise
 *       verbatim, including the USE_BSEARCH_IF_NXIP_GREATER cutover.
 *  [S4] strtou64(): postgres c.h maps this to libc strtoul/strtoull —
 *       no CBMC model. pg_proof_strtou64 implements the C-standard
 *       strtoull(str, &endp, 10) semantics: skip C-locale isspace
 *       (shared shim's pg_proof_isspace), optional single '+'/'-'
 *       ('-' negates the value modulo 2^64, C17 7.22.1.4), decimal
 *       digits, saturate to UINT64_MAX on overflow (errno is never
 *       consulted by the code under proof), and *endp = str when there
 *       is no digit sequence. This is a MODEL of libc; it is grounded
 *       against the real libc by the native differential in
 *       src/bin/native_diff.rs (see family README section in lib.rs).
 *  [S5] parse_snapshot(): control flow and checks verbatim, except
 *       (a) the StringInfo helpers buf_init/buf_add_txid/buf_finalize
 *       write into a CALLER-PROVIDED fixed pg_snapshot buffer
 *       (palloc/StringInfo -> caller buffer: allowed plumbing shim;
 *       helper names and call sites kept, buf_init gains the buffer
 *       arg), (b) `ereturn(escontext, NULL, ...)` -> PROOF_EREPORT_FLAG
 *       + `return NULL` at the same program point (error KIND parity
 *       only; message text out of proof), (c) the signature gains the
 *       out-buffer and err out-params. The single errcode is
 *       ERRCODE_INVALID_TEXT_REPRESENTATION (22P02), asserted on the
 *       Rust side as sqlstate parity.
 *  [S6] SET_VARSIZE in buf_finalize: kept as a plain store of
 *       PG_SNAPSHOT_SIZE(nxip) into __varsz (varlena headers are fmgr
 *       plumbing; harness compares the nxip/xmin/xmax/xip fields).
 *  [S7] Exported entry points carry a pgc_ prefix; static helpers keep
 *       their upstream names.
 *
 * Everything else — struct pg_snapshot, PG_SNAPSHOT_SIZE, cmp_fxid,
 * is_visible_fxid, FullTransactionIdFromEpochAndXid,
 * FullTransactionIdFromAllowableAt, parse_snapshot's parse loop — is
 * verbatim upstream.
 */

#include "../../support/c/pg_proof_shim.h"

/* ---- [S1] transam.h / c.h vendored types + macros (REL_18_STABLE) ---- */

typedef uint32 TransactionId;

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;

#define FLEXIBLE_ARRAY_MEMBER	/* empty: C99 flexible array member */

/* transam.h:31,34,41,42 */
#define InvalidTransactionId		((TransactionId) 0)
#define FirstNormalTransactionId	((TransactionId) 3)
#define TransactionIdIsValid(xid)		((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid)		((xid) >= FirstNormalTransactionId)

/* transam.h:47-56 */
#define EpochFromFullTransactionId(x)	((uint32) ((x).value >> 32))
#define XidFromFullTransactionId(x)		((uint32) (x).value)
#define U64FromFullTransactionId(x)		((x).value)
#define FullTransactionIdEquals(a, b)	((a).value == (b).value)
#define FullTransactionIdPrecedes(a, b)	((a).value < (b).value)
#define FullTransactionIdPrecedesOrEquals(a, b) ((a).value <= (b).value)
#define FullTransactionIdFollowsOrEquals(a, b) ((a).value >= (b).value)
#define FullTransactionIdIsValid(x)		TransactionIdIsValid(XidFromFullTransactionId(x))
#define InvalidFullTransactionId		FullTransactionIdFromEpochAndXid(0, InvalidTransactionId)

/* transam.h:71 — verbatim */
static inline FullTransactionId
FullTransactionIdFromEpochAndXid(uint32 epoch, TransactionId xid)
{
	FullTransactionId result;

	result.value = ((uint64) epoch) << 32 | xid;

	return result;
}

/* xid8.h FullTransactionIdFromU64 equivalent (value carrier) */
static inline FullTransactionId
FullTransactionIdFromU64(uint64 value)
{
	FullTransactionId result;

	result.value = value;

	return result;
}

/* transam.h:380-414 — verbatim (comments elided; Assert no-op per [S2]) */
static inline FullTransactionId
FullTransactionIdFromAllowableAt(FullTransactionId nextFullXid,
								 TransactionId xid)
{
	uint32		epoch;

	/* Special transaction ID. */
	if (!TransactionIdIsNormal(xid))
		return FullTransactionIdFromEpochAndXid(0, xid);

	Assert(TransactionIdPrecedesOrEquals(xid,
										 XidFromFullTransactionId(nextFullXid)));

	epoch = EpochFromFullTransactionId(nextFullXid);
	if (unlikely(xid > XidFromFullTransactionId(nextFullXid)))
	{
		Assert(epoch != 0);
		epoch--;
	}

	return FullTransactionIdFromEpochAndXid(epoch, xid);
}

/* ---- xid8funcs.c:48 ---- */
#define USE_BSEARCH_IF_NXIP_GREATER 30

/* ---- xid8funcs.c:54-73 — verbatim ---- */
typedef struct
{
	/*
	 * 4-byte length hdr, should not be touched directly.
	 *
	 * Explicit embedding is ok as we want always correct alignment anyway.
	 */
	int32		__varsz;

	uint32		nxip;			/* number of fxids in xip array */
	FullTransactionId xmin;
	FullTransactionId xmax;
	/* in-progress fxids, xmin <= xip[i] < xmax: */
	FullTransactionId xip[FLEXIBLE_ARRAY_MEMBER];
} pg_snapshot;

#define PG_SNAPSHOT_SIZE(nxip) \
	(offsetof(pg_snapshot, xip) + sizeof(FullTransactionId) * (nxip))

/* ---- xid8funcs.c:152-163 — verbatim ---- */
static int
cmp_fxid(const void *aa, const void *bb)
{
	FullTransactionId a = *(const FullTransactionId *) aa;
	FullTransactionId b = *(const FullTransactionId *) bb;

	if (FullTransactionIdPrecedes(a, b))
		return -1;
	if (FullTransactionIdPrecedes(b, a))
		return 1;
	return 0;
}

/*
 * [S3] libc bsearch model: plain binary search honoring the libc
 * contract. Kani/CBMC has no libc model; this is harness plumbing, not
 * logic under proof (the logic under proof calls it exactly where
 * upstream calls libc bsearch).
 */
static const void *
pg_proof_bsearch(const void *key, const void *base, size_t nmemb,
				 size_t size, int (*compar) (const void *, const void *))
{
	size_t		lo = 0;
	size_t		hi = nmemb;

	while (lo < hi)
	{
		size_t		mid = lo + (hi - lo) / 2;
		const char *probe = (const char *) base + mid * size;
		int			c = compar(key, probe);

		if (c == 0)
			return probe;
		if (c > 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return NULL;
}

#define bsearch pg_proof_bsearch	/* [S3] */

/* ---- xid8funcs.c:186-215 — verbatim (bsearch per [S3]) ---- */
static bool
is_visible_fxid(FullTransactionId value, const pg_snapshot *snap)
{
	if (FullTransactionIdPrecedes(value, snap->xmin))
		return true;
	else if (!FullTransactionIdPrecedes(value, snap->xmax))
		return false;
#ifdef USE_BSEARCH_IF_NXIP_GREATER
	else if (snap->nxip > USE_BSEARCH_IF_NXIP_GREATER)
	{
		const void *res;

		res = bsearch(&value, snap->xip, snap->nxip, sizeof(FullTransactionId),
					  cmp_fxid);
		/* if found, transaction is still in progress */
		return (res) ? false : true;
	}
#endif
	else
	{
		uint32		i;

		for (i = 0; i < snap->nxip; i++)
		{
			if (FullTransactionIdEquals(value, snap->xip[i]))
				return false;
		}
		return true;
	}
}

/* ---- [S4] libc strtou64(str, &endp, 10) model ---- */
static uint64
pg_proof_strtou64(const char *str, char **endp)
{
	const char *s = str;
	bool		neg = false;
	bool		overflow = false;
	uint64		value = 0;
	const char *digits_start;

	while (pg_proof_isspace((unsigned char) *s))
		s++;
	if (*s == '+' || *s == '-')
	{
		neg = (*s == '-');
		s++;
	}
	digits_start = s;
	while (pg_proof_isdigit((unsigned char) *s))
	{
		uint64		d = (uint64) (*s - '0');

		if (value > (UINT64_MAX - d) / 10)
			overflow = true;
		else
			value = value * 10 + d;
		s++;
	}
	if (s == digits_start)
	{
		/* no conversion performed: *endp = original str (C17 7.22.1.4) */
		*endp = (char *) str;
		return 0;
	}
	if (overflow)
		value = UINT64_MAX;
	if (neg)
		value = (uint64) 0 - value;
	*endp = (char *) s;
	return value;
}

#define strtou64(str, endp, base) pg_proof_strtou64((str), (endp))	/* [S4] */

/* ---- [S5] StringInfo helpers -> caller-provided fixed buffer ----
 * upstream xid8funcs.c:221-259; helper names + call shape kept. */
static pg_snapshot *
buf_init(pg_snapshot *out, FullTransactionId xmin, FullTransactionId xmax)
{
	out->xmin = xmin;
	out->xmax = xmax;
	out->nxip = 0;
	return out;
}

static void
buf_add_txid(pg_snapshot *buf, FullTransactionId fxid)
{
	buf->xip[buf->nxip] = fxid;
	buf->nxip++;
}

static pg_snapshot *
buf_finalize(pg_snapshot *buf)
{
	/* [S6] SET_VARSIZE equivalent */
	buf->__varsz = (int32) PG_SNAPSHOT_SIZE(buf->nxip);
	return buf;
}

/* ---- xid8funcs.c:264-325 parse_snapshot — body verbatim per [S5] ----
 * err: 0 = OK; 1 = bad_format (ERRCODE_INVALID_TEXT_REPRESENTATION). */
static pg_snapshot *
parse_snapshot(const char *str, pg_snapshot *outbuf, int *err)
{
	FullTransactionId xmin;
	FullTransactionId xmax;
	FullTransactionId last_val = InvalidFullTransactionId;
	FullTransactionId val;
	char	   *endp;
	pg_snapshot *buf;

	xmin = FullTransactionIdFromU64(strtou64(str, &endp, 10));
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	xmax = FullTransactionIdFromU64(strtou64(str, &endp, 10));
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	/* it should look sane */
	if (!FullTransactionIdIsValid(xmin) ||
		!FullTransactionIdIsValid(xmax) ||
		FullTransactionIdPrecedes(xmax, xmin))
		goto bad_format;

	/* allocate buffer */
	buf = buf_init(outbuf, xmin, xmax);

	/* loop over values */
	while (*str != '\0')
	{
		/* read next value */
		val = FullTransactionIdFromU64(strtou64(str, &endp, 10));
		str = endp;

		/* require the input to be in order */
		if (FullTransactionIdPrecedes(val, xmin) ||
			FullTransactionIdFollowsOrEquals(val, xmax) ||
			FullTransactionIdPrecedes(val, last_val))
			goto bad_format;

		/* skip duplicates */
		if (!FullTransactionIdEquals(val, last_val))
			buf_add_txid(buf, val);
		last_val = val;

		if (*str == ',')
			str++;
		else if (*str != '\0')
			goto bad_format;
	}

	return buf_finalize(buf);

bad_format:
	PROOF_EREPORT_FLAG(err);	/* [S5] ereturn(escontext, NULL, 22P02...) */
	return NULL;
}

/* ================= exported entry points ([S7]) ================= */

uint64
pgc_full_xid_from_allowable_at(uint64 next_full_xid, uint32 xid)
{
	return U64FromFullTransactionId(
		FullTransactionIdFromAllowableAt(FullTransactionIdFromU64(next_full_xid),
										 (TransactionId) xid));
}

int
pgc_is_visible_fxid(uint64 value, const void *snap)
{
	return is_visible_fxid(FullTransactionIdFromU64(value),
						   (const pg_snapshot *) snap) ? 1 : 0;
}

/* struct-member reads for the layout theorem (pg_snapshot_xmin/xmax cores
 * xid8funcs.c:569-587 are exactly these member reads) */
uint32
pgc_snap_nxip(const void *snap)
{
	return ((const pg_snapshot *) snap)->nxip;
}

uint64
pgc_snap_xmin(const void *snap)
{
	return U64FromFullTransactionId(((const pg_snapshot *) snap)->xmin);
}

uint64
pgc_snap_xmax(const void *snap)
{
	return U64FromFullTransactionId(((const pg_snapshot *) snap)->xmax);
}

uint64
pgc_snap_xip(const void *snap, uint32 i)
{
	return U64FromFullTransactionId(((const pg_snapshot *) snap)->xip[i]);
}

uint64
pgc_strtou64(const char *str, size_t *endoff)
{
	char	   *endp;
	uint64		v = pg_proof_strtou64(str, &endp);

	*endoff = (size_t) (endp - str);
	return v;
}

/* returns 1 if parse succeeded, 0 on bad format (err also set) */
int
pgc_parse_snapshot(const char *str, void *outbuf, int *err)
{
	return parse_snapshot(str, (pg_snapshot *) outbuf, err) != NULL ? 1 : 0;
}

/* ==================================================================== */
/* WAVE sendrecv (2026-07-30): pg_snapshot_recv (oids 2941/5057) and    */
/* pg_snapshot_send (oids 2942/5058).                                   */
/*                                                                      */
/* PROVENANCE: src/backend/utils/adt/xid8funcs.c, REL_18_STABLE, same   */
/* 2026-07-30 fetch as the rest of this file (verbatim copy alongside   */
/* in xid8funcs_upstream.c: pg_snapshot_recv lines 461-524,             */
/* pg_snapshot_send lines 527-547). pqformat wire conventions copied    */
/* from proofs/pg_lsn/c/pg_pg_lsn.c (its wave-5 section, REL_18_STABLE  */
/* src/backend/libpq/pqformat.c provenance).                            */
/*                                                                      */
/* SHIM MANIFEST (this section only) — every deviation from upstream:   */
/*  [S8]  MaxAllocSize inlined verbatim from utils/memutils.h           */
/*        (#define MaxAllocSize ((Size) 0x3fffffff)).                   */
/*        PG_SNAPSHOT_MAX_NXIP is the verbatim xid8funcs.c:72-73 macro  */
/*        over it (== (0x3fffffff - 24) / 8 = 134217724). The shipped   */
/*        Rust crate computes the same value                            */
/*        (xid8funcs::PG_SNAPSHOT_MAX_NXIP, checked by harness          */
/*        eq_snapshot_max_nxip); the `nxip > PG_SNAPSHOT_MAX_NXIP`      */
/*        comparison below stays verbatim (int vs size_t promotion      */
/*        included — the nxip < 0 arm short-circuits first, as          */
/*        upstream).                                                    */
/*  [S9]  StringInfo -> (data, len, cursor out-param) triple.           */
/*        pq_getmsgint(buf, 4) / pq_getmsgint64(buf) ->                 */
/*        pgrecv_getmsgint32 / pgrecv_getmsgint64: pq_copymsgbytes'     */
/*        bounds check + memcpy + the pg_ntoh big-endian fold written   */
/*        as explicit shifts (no libc/CBMC model for the intrinsics).   */
/*        Insufficient data -> status PGC_ERR_PROTOCOL returned at the  */
/*        exact program point where C's pq_copymsgbytes ereports        */
/*        ERRCODE_PROTOCOL_VIOLATION "insufficient data left in         */
/*        message" (control flow aborts there on both sides; the        */
/*        harness asserts that sqlstate class on the Rust Err arm).     */
/*  [S10] palloc(PG_SNAPSHOT_SIZE(nxip)) -> caller-provided fixed       */
/*        buffer (allowed plumbing shim; allocation strategy leaves     */
/*        the claim). HARNESS CONTRACT: the input frame is <= 36 bytes, */
/*        so at most 2 xip slots are ever written before the message    */
/*        reads run dry; the harness provides a 64-byte buffer (room    */
/*        for 4 slots). A real palloc of a huge in-cap nxip succeeds    */
/*        upstream and then fails the next read — same observable       */
/*        error class; the buffer size itself is out of the claim.      */
/*  [S11] ereport(ERROR, errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),*/
/*        errmsg("invalid external pg_snapshot data")) at bad_format -> */
/*        status PGC_ERR_BADFORMAT returned at the same program point   */
/*        (distinct sentinel per errcode, per the PROOF_EREPORT_FLAG    */
/*        convention note in pg_proof_shim.h; message text out of       */
/*        proof).                                                       */
/*  [S12] SET_VARSIZE(snap, PG_SNAPSHOT_SIZE(nxip)) ->                  */
/*        pgc_set_varsize_le: little-endian 4B-uncompressed varlena     */
/*        header stamp (len << 2), byte-identical to the shipped        */
/*        datum::set_varsize_4b on this little-endian target, so the    */
/*        harness asserts FULL-IMAGE byte equality (header included).   */
/*        *outlen carries C's image size.                               */
/*  [S13] send: StringInfoData + pq_begintypsend / pq_sendint32 /       */
/*        pq_sendint64 / pq_endtypsend -> a fixed caller buffer with a  */
/*        running length (pgsend_int32/pgsend_int64 emit the pg_hton    */
/*        big-endian bytes as explicit shifts; pq_begintypsend's        */
/*        4-byte reservation and pq_endtypsend's SET_VARSIZE(result,    */
/*        buf->len) kept at the same program points, header per [S12]). */
/*        PG_GETARG_VARLENA_P(0)'s detoast is fmgr plumbing outside     */
/*        the claim: the harness passes an inline 4B-U image (same      */
/*        fence as the family's SNAPSHOT MODEL note / the shipped       */
/*        arg_varlena_packed inline arm).                               */
/*  Everything between the shims — nxip/xmin/xmax validation, the xip   */
/*  read loop with its order check and duplicate skip (i--; nxip--;     */
/*  continue), snap->nxip = nxip, and send's field emission order — is  */
/*  verbatim upstream.                                                  */
/* ==================================================================== */

#include <string.h>

/* [S8] utils/memutils.h:40 — verbatim */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

/* xid8funcs.c:72-73 — verbatim */
#define PG_SNAPSHOT_MAX_NXIP \
	((MaxAllocSize - offsetof(pg_snapshot, xip)) / sizeof(FullTransactionId))

/* recv/send status sentinels ([S9]/[S11]) */
#define PGC_ERR_PROTOCOL	4	/* 08P01 insufficient data left in message */
#define PGC_ERR_BADFORMAT	22	/* 22P03 invalid external pg_snapshot data */

/* [S9] pq_getmsgint(buf, 4): pq_copymsgbytes + pg_ntoh32 */
static int
pgrecv_getmsgint32(const unsigned char *data, int32 len, int32 *cursor,
				   uint32 *out)
{
	unsigned char b[4];
	uint32		v = 0;
	int			i;

	if (4 > (len - *cursor))
		return PGC_ERR_PROTOCOL;
	memcpy(b, data + *cursor, 4);
	*cursor += 4;
	for (i = 0; i < 4; i++)
		v = (v << 8) | (uint32) b[i];
	*out = v;
	return 0;
}

/* [S9] pq_getmsgint64(buf): pq_copymsgbytes + pg_ntoh64 */
static int
pgrecv_getmsgint64(const unsigned char *data, int32 len, int32 *cursor,
				   uint64 *out)
{
	unsigned char b[8];
	uint64		v = 0;
	int			i;

	if (8 > (len - *cursor))
		return PGC_ERR_PROTOCOL;
	memcpy(b, data + *cursor, 8);
	*cursor += 8;
	for (i = 0; i < 8; i++)
		v = (v << 8) | (uint64) b[i];
	*out = v;
	return 0;
}

/* [S12] SET_VARSIZE: little-endian 4B-U header, len << 2 */
static void
pgc_set_varsize_le(unsigned char *p, uint32 size)
{
	uint32		hdr = size << 2;

	p[0] = (unsigned char) (hdr & 0xFF);
	p[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	p[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	p[3] = (unsigned char) ((hdr >> 24) & 0xFF);
}

/* [S8] cross-check export for harness eq_snapshot_max_nxip */
uint64
pgc_pg_snapshot_max_nxip(void)
{
	return (uint64) PG_SNAPSHOT_MAX_NXIP;
}

/*
 * xid8funcs.c:461-524 pg_snapshot_recv — body verbatim per [S9]-[S12].
 * Returns 0 = OK (*outlen = image size, outbuf = full varlena image),
 * PGC_ERR_PROTOCOL, or PGC_ERR_BADFORMAT.
 */
int
pgc_pg_snapshot_recv(const unsigned char *data, int32 dlen, int32 *cursor,
					 unsigned char *outbuf, int32 *outlen)
{
	pg_snapshot *snap;
	FullTransactionId last = InvalidFullTransactionId;
	int			nxip;
	int			i;
	FullTransactionId xmin;
	FullTransactionId xmax;
	uint32		u32tmp;
	uint64		u64tmp;

	/* load and validate nxip */
	if (pgrecv_getmsgint32(data, dlen, cursor, &u32tmp) != 0)	/* [S9] */
		return PGC_ERR_PROTOCOL;
	nxip = (int) u32tmp;
	if (nxip < 0 || nxip > PG_SNAPSHOT_MAX_NXIP)
		goto bad_format;

	if (pgrecv_getmsgint64(data, dlen, cursor, &u64tmp) != 0)	/* [S9] */
		return PGC_ERR_PROTOCOL;
	xmin = FullTransactionIdFromU64(u64tmp);
	if (pgrecv_getmsgint64(data, dlen, cursor, &u64tmp) != 0)	/* [S9] */
		return PGC_ERR_PROTOCOL;
	xmax = FullTransactionIdFromU64(u64tmp);
	if (!FullTransactionIdIsValid(xmin) ||
		!FullTransactionIdIsValid(xmax) ||
		FullTransactionIdPrecedes(xmax, xmin))
		goto bad_format;

	snap = (pg_snapshot *) outbuf;	/* [S10] palloc -> caller buffer */
	snap->xmin = xmin;
	snap->xmax = xmax;

	for (i = 0; i < nxip; i++)
	{
		FullTransactionId cur;

		if (pgrecv_getmsgint64(data, dlen, cursor, &u64tmp) != 0)	/* [S9] */
			return PGC_ERR_PROTOCOL;
		cur = FullTransactionIdFromU64(u64tmp);

		if (FullTransactionIdPrecedes(cur, last) ||
			FullTransactionIdPrecedes(cur, xmin) ||
			FullTransactionIdPrecedes(xmax, cur))
			goto bad_format;

		/* skip duplicate xips */
		if (FullTransactionIdEquals(cur, last))
		{
			i--;
			nxip--;
			continue;
		}

		snap->xip[i] = cur;
		last = cur;
	}
	snap->nxip = nxip;
	pgc_set_varsize_le(outbuf, (uint32) PG_SNAPSHOT_SIZE(nxip));	/* [S12] */
	*outlen = (int32) PG_SNAPSHOT_SIZE(nxip);
	return 0;

bad_format:
	return PGC_ERR_BADFORMAT;	/* [S11] ereport(22P03 ...) */
}

/* [S13] pq_sendint32: big-endian emission at the running length */
static void
pgsend_int32(unsigned char *out, int32 *len, uint32 v)
{
	int			i;

	for (i = 0; i < 4; i++)
		out[*len + i] = (unsigned char) ((v >> (8 * (3 - i))) & 0xFF);
	*len += 4;
}

/* [S13] pq_sendint64: big-endian emission at the running length */
static void
pgsend_int64(unsigned char *out, int32 *len, uint64 v)
{
	int			i;

	for (i = 0; i < 8; i++)
		out[*len + i] = (unsigned char) ((v >> (8 * (7 - i))) & 0xFF);
	*len += 8;
}

/*
 * xid8funcs.c:527-547 pg_snapshot_send — body verbatim per [S13].
 * snapimg points at an inline 4B-U pg_snapshot varlena image; out must
 * hold 4 + 4 + 8 + 8 + 8 * snap->nxip bytes. Returns the image length.
 */
int32
pgc_pg_snapshot_send(const void *snapimg, unsigned char *out)
{
	const pg_snapshot *snap = (const pg_snapshot *) snapimg;
	int32		len;
	uint32		i;

	len = (int32) VARHDRSZ;		/* pq_begintypsend: reserve the length word */
	pgsend_int32(out, &len, snap->nxip);
	pgsend_int64(out, &len, U64FromFullTransactionId(snap->xmin));
	pgsend_int64(out, &len, U64FromFullTransactionId(snap->xmax));
	for (i = 0; i < snap->nxip; i++)
		pgsend_int64(out, &len, U64FromFullTransactionId(snap->xip[i]));
	/* pq_endtypsend: SET_VARSIZE(result, buf->len) [S12] */
	pgc_set_varsize_le(out, (uint32) len);
	return len;
}
