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
