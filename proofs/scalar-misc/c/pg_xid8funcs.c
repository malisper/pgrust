/*
 * Verbatim PostgreSQL C for the xid8funcs snapshot family (wave 5):
 *   - FullTransactionIdFromAllowableAt (epoch arithmetic core)
 *   - is_visible_fxid (+ cmp_fxid)      [pg_visible_in_snapshot 2948/5065]
 *   - pg_snapshot_xmin / pg_snapshot_xmax accessors [2945/5062, 2946/5063]
 *   - parse_snapshot                     [pg_snapshot_in 2939/5055]
 *   - pg_snapshot_out digit/format walk  [2940/5056]
 *   - pg_snapshot_recv validating reader [2941/5057]
 *   - pg_snapshot_send wire emission     [2942/5058]
 *
 * Provenance (fetched 2026-07-28, postgres/postgres REL_18_STABLE):
 *   src/backend/utils/adt/xid8funcs.c  (pg_snapshot struct, cmp_fxid,
 *                                       is_visible_fxid, parse_snapshot,
 *                                       pg_snapshot_in/out/recv/send,
 *                                       pg_snapshot_xmin/xmax bodies)
 *   src/include/access/transam.h       (FullTransactionId type + Equals/
 *                                       Precedes/Follows macros,
 *                                       FullTransactionIdFromAllowableAt,
 *                                       Epoch/XidFromFullTransactionId,
 *                                       FullTransactionIdFromEpochAndXid)
 *
 * Shims (plumbing only, never logic):
 *   X1. PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_* unwrapped to plain C
 *       signatures; bool returns ride as int.
 *   X2. pg_snapshot's FLEXIBLE_ARRAY_MEMBER xip[] shimmed to xip[36]
 *       (harness caps: nxip <= 4 on the linear arm, nxip == 32 on the
 *       bsearch arm, nxip <= 2 on the in/out/recv/send rows).
 *   X3. libc bsearch(): CBMC/Kani has no libc model — pg_proof_bsearch
 *       below is a MODEL OF THE LIBC CONTRACT (standard binary search over
 *       a sorted array; returns a pointer to A matching element or NULL).
 *       It is a documented libc-model shim (pg_proof_isspace precedent),
 *       not PostgreSQL code. is_visible_fxid's body is verbatim around it.
 *   X4. libc strtou64 (c.h maps it to strtoul on 64-bit platforms):
 *       pg_proof_strtou64 below is a MODEL OF THE GLIBC CONTRACT for
 *       base-10 strtoul on a 64-bit platform: skip C-locale isspace,
 *       optional +/- sign ('-' negates modulo 2^64), decimal digits,
 *       saturate to UINT64_MAX + ERANGE on overflow, endptr == str when no
 *       digits were consumed.  GROUND-TRUTH note: strtoul edge semantics
 *       are platform-flavored (proofs/TRIAGE.md tidin lesson); glibc is the
 *       production target and this model matches the shipped Rust
 *       xid8funcs::strtou64's documented contract.  Any divergence found
 *       through this seam must be reproduced against real glibc PG before
 *       recording (GROUND-TRUTH law).
 *   X5. StringInfo plumbing:
 *       - parse_snapshot's buf_init/buf_add_txid/buf_finalize grow a
 *         palloc'd buffer; shimmed to caller-provided fixed arrays + out
 *         params (nxip, xmin, xmax, xip[]).  The validation/skip logic is
 *         verbatim.
 *       - pg_snapshot_out's appendStringInfo(UINT64_FORMAT) is libc printf
 *         %llu; the digit emission below uses PostgreSQL's own pg_ulltoa_n
 *         (numutils.c, vendored in ../intout/c/pg_intout.c — pass it as a
 *         second --c-lib), a documented SPEC-LEVEL ANCHOR for %llu's
 *         canonical decimal (same convention as pg_scalar_misc.c's
 *         xidout/cidout note).
 *       - recv reads through the (data,len,cursor) triple and send writes
 *         a caller buffer, per the wire shims W1-W4 in pg_scalar_misc.c
 *         (pg_getmsguint32/pg_getmsguint64 come from that file; link both).
 *   X6. ereport/ereturn(ERROR) -> status returns: 1 = invalid-input
 *       (22P02 text / 22P03 binary, asserted Rust-side), 0 = OK.
 *   X7. qsort/qunique (sort_snapshot) are NOT vendored: they are reached
 *       only from pg_current_snapshot (live-state row, out of wave-5
 *       scope).  parse_snapshot/recv enforce sortedness instead of sorting.
 */

#include <stdint.h>
#include <string.h>

typedef int32_t int32;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int64_t int64;
typedef uint32 TransactionId;

/* ---- transam.h, verbatim ---- */

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;

#define EpochFromFullTransactionId(x)	((uint32) ((x).value >> 32))
#define XidFromFullTransactionId(x)		((uint32) (x).value)
#define U64FromFullTransactionId(x)		((x).value)
#define FullTransactionIdEquals(a, b)	((a).value == (b).value)
#define FullTransactionIdPrecedes(a, b) ((a).value < (b).value)
#define FullTransactionIdPrecedesOrEquals(a, b) ((a).value <= (b).value)
#define FullTransactionIdFollows(a, b) ((a).value > (b).value)
#define FullTransactionIdFollowsOrEquals(a, b) ((a).value >= (b).value)
#define FullTransactionIdIsValid(x)		((uint32) (x).value != 0)	/* TransactionIdIsValid
																	 * (XidFrom..) */
#define InvalidFullTransactionId		FullTransactionIdFromU64(0)
#define FirstNormalTransactionId	((TransactionId) 3)
#define TransactionIdIsNormal(xid)	((xid) >= FirstNormalTransactionId)
#define Assert(x) ((void) 0)
#define unlikely(x) (x)

static FullTransactionId
FullTransactionIdFromU64(uint64 value)
{
	FullTransactionId result;

	result.value = value;
	return result;
}

static FullTransactionId
FullTransactionIdFromEpochAndXid(uint32 epoch, TransactionId xid)
{
	FullTransactionId result;

	result.value = ((uint64) epoch) << 32 | xid;
	return result;
}

/* transam.h FullTransactionIdFromAllowableAt, verbatim (comments elided;
 * Asserts compile out as in a production NDEBUG build — the harness fences
 * the documented precondition TransactionIdPrecedesOrEquals(xid, next32)) */
static FullTransactionId
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

/* plain-signature entry point ([shim X1]) */
uint64
pg_full_xid_from_allowable_at(uint64 next_full_xid, uint32 xid)
{
	return U64FromFullTransactionId(
		FullTransactionIdFromAllowableAt(FullTransactionIdFromU64(next_full_xid), xid));
}

/* ---- xid8funcs.c ---- */

#define USE_BSEARCH_IF_NXIP_GREATER 30

#define PG_PROOF_MAX_NXIP 36	/* [shim X2] FLEXIBLE_ARRAY_MEMBER cap */

typedef struct
{
	int32		__varsz;
	uint32		nxip;			/* number of fxids in xip array */
	FullTransactionId xmin;
	FullTransactionId xmax;
	/* in-progress fxids, xmin <= xip[i] < xmax: */
	FullTransactionId xip[PG_PROOF_MAX_NXIP];	/* [shim X2] */
} pg_snapshot;

/* xid8funcs.c cmp_fxid, verbatim */
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

/* [shim X3] libc bsearch model: standard binary search over a sorted
 * array; the contract (C99 7.20.5.1) — returns a pointer to a matching
 * element, or NULL.  NOT PostgreSQL code. */
static const void *
pg_proof_bsearch(const void *key, const void *base, uint32 nmemb,
				 uint32 size, int (*compar) (const void *, const void *))
{
	uint32		lo = 0;
	uint32		hi = nmemb;

	while (lo < hi)
	{
		uint32		mid = lo + (hi - lo) / 2;
		const unsigned char *p = (const unsigned char *) base + (uint64) mid * size;
		int			c = compar(key, p);

		if (c == 0)
			return p;
		else if (c > 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return 0;
}

/* xid8funcs.c is_visible_fxid, verbatim (bsearch -> the model above) */
static int
is_visible_fxid(FullTransactionId value, const pg_snapshot *snap)
{
	if (FullTransactionIdPrecedes(value, snap->xmin))
		return 1;
	else if (!FullTransactionIdPrecedes(value, snap->xmax))
		return 0;
#ifdef USE_BSEARCH_IF_NXIP_GREATER
	else if (snap->nxip > USE_BSEARCH_IF_NXIP_GREATER)
	{
		const void *res;

		res = pg_proof_bsearch(&value, snap->xip, snap->nxip,
							   sizeof(FullTransactionId), cmp_fxid);
		/* if found, transaction is still in progress */
		return (res) ? 0 : 1;
	}
#endif
	else
	{
		uint32		i;

		for (i = 0; i < snap->nxip; i++)
		{
			if (FullTransactionIdEquals(value, snap->xip[i]))
				return 0;
		}
		return 1;
	}
}

/* entry points over a harness-built snapshot image ([shim X1]/[shim X2]:
 * the harness passes the same nxip/xmin/xmax/xip values it packed into the
 * Rust-side varlena image) */
static pg_snapshot
pg_snap_build(uint32 nxip, uint64 xmin, uint64 xmax, const uint64 *xip)
{
	pg_snapshot snap;
	uint32		i;

	snap.__varsz = 0;			/* never read by the compared code */
	snap.nxip = nxip;
	snap.xmin = FullTransactionIdFromU64(xmin);
	snap.xmax = FullTransactionIdFromU64(xmax);
	for (i = 0; i < nxip && i < PG_PROOF_MAX_NXIP; i++)
		snap.xip[i] = FullTransactionIdFromU64(xip[i]);
	return snap;
}

int
pg_visible_in_snapshot(uint64 value, uint32 nxip, uint64 xmin, uint64 xmax,
					   const uint64 *xip)
{
	pg_snapshot snap = pg_snap_build(nxip, xmin, xmax, xip);

	return is_visible_fxid(FullTransactionIdFromU64(value), &snap);
}

/* pg_snapshot_xmin / pg_snapshot_xmax bodies: plain field reads */
uint64
pg_snapshot_xmin_c(uint32 nxip, uint64 xmin, uint64 xmax, const uint64 *xip)
{
	pg_snapshot snap = pg_snap_build(nxip, xmin, xmax, xip);

	return U64FromFullTransactionId(snap.xmin);
}

uint64
pg_snapshot_xmax_c(uint32 nxip, uint64 xmin, uint64 xmax, const uint64 *xip)
{
	pg_snapshot snap = pg_snap_build(nxip, xmin, xmax, xip);

	return U64FromFullTransactionId(snap.xmax);
}

/* ---- [shim X4] libc strtou64 model (glibc base-10 strtoul contract) ---- */

static int
pg_proof_isspace_x(unsigned char c)
{
	/* C-locale isspace: space, \t, \n, \v, \f, \r */
	return c == ' ' || (c >= 0x09 && c <= 0x0d);
}

uint64
pg_proof_strtou64(const unsigned char *str, const unsigned char **endptr)
{
	const unsigned char *s = str;
	uint64		value = 0;
	int			neg = 0;
	int			overflow = 0;
	const unsigned char *digits_start;

	while (pg_proof_isspace_x(*s))
		s++;
	if (*s == '+' || *s == '-')
	{
		neg = (*s == '-');
		s++;
	}
	digits_start = s;
	while (*s >= '0' && *s <= '9')
	{
		uint64		d = (uint64) (*s - '0');

		if (value > (0xFFFFFFFFFFFFFFFFULL - d) / 10)
			overflow = 1;
		else
			value = value * 10 + d;
		s++;
	}
	if (s == digits_start)
	{
		*endptr = str;			/* no conversion performed */
		return 0;
	}
	*endptr = s;
	if (overflow)
		return 0xFFFFFFFFFFFFFFFFULL;	/* ERANGE: ULONG_MAX, NOT negated
										 * (C99 7.20.1.4; glibc).  NOTE the
										 * shipped Rust strtou64 negates its
										 * saturated value — unreachable
										 * below 20 digits, so outside every
										 * wave-5 harness cap; flagged in
										 * the module doc. */
	if (neg)
		value = (uint64) (-(int64) value);	/* wraps mod 2^64 */
	return value;
}

/* xid8funcs.c parse_snapshot, verbatim control flow ([shim X5]: the
 * growing StringInfo becomes the caller-provided out arrays; [shim X6]:
 * bad_format -> return 1).  Returns 0 = OK, 1 = bad format. */
int
pg_parse_snapshot(const unsigned char *str,
				  uint32 *out_nxip, uint64 *out_xmin, uint64 *out_xmax,
				  uint64 *out_xip /* [PG_PROOF_MAX_NXIP] */ )
{
	FullTransactionId xmin;
	FullTransactionId xmax;
	FullTransactionId last_val = InvalidFullTransactionId;
	FullTransactionId val;
	const unsigned char *endp;
	uint32		nxip = 0;

	xmin = FullTransactionIdFromU64(pg_proof_strtou64(str, &endp));
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	xmax = FullTransactionIdFromU64(pg_proof_strtou64(str, &endp));
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	/* it should look sane */
	if (!FullTransactionIdIsValid(xmin) ||
		!FullTransactionIdIsValid(xmax) ||
		FullTransactionIdPrecedes(xmax, xmin))
		goto bad_format;

	/* loop over values */
	while (*str != '\0')
	{
		/* read next value */
		val = FullTransactionIdFromU64(pg_proof_strtou64(str, &endp));
		str = endp;

		/* require the input to be in order */
		if (FullTransactionIdPrecedes(val, xmin) ||
			FullTransactionIdFollowsOrEquals(val, xmax) ||
			FullTransactionIdPrecedes(val, last_val))
			goto bad_format;

		/* skip duplicates */
		if (!FullTransactionIdEquals(val, last_val))
		{
			if (nxip >= PG_PROOF_MAX_NXIP)
				goto bad_format;	/* [shim X2] cap, unreachable under
									 * harness fences */
			out_xip[nxip++] = U64FromFullTransactionId(val);
		}
		last_val = val;

		if (*str == ',')
			str++;
		else if (*str != '\0')
			goto bad_format;
	}

	*out_nxip = nxip;
	*out_xmin = U64FromFullTransactionId(xmin);
	*out_xmax = U64FromFullTransactionId(xmax);
	return 0;

bad_format:
	return 1;
}

/* xid8funcs.c pg_snapshot_out, verbatim walk ([shim X5]: appendStringInfo
 * UINT64_FORMAT -> pg_ulltoa_n from ../intout/c/pg_intout.c). */
extern int	pg_ulltoa_n(uint64 value, char *a);

int32
pg_snapshot_out_c(uint32 nxip, uint64 xmin, uint64 xmax, const uint64 *xip,
				  unsigned char *out)
{
	int32		n = 0;
	uint32		i;

	n += pg_ulltoa_n(xmin, (char *) out + n);
	out[n++] = ':';
	n += pg_ulltoa_n(xmax, (char *) out + n);
	out[n++] = ':';
	for (i = 0; i < nxip; i++)
	{
		if (i > 0)
			out[n++] = ',';
		n += pg_ulltoa_n(xip[i], (char *) out + n);
	}
	return n;
}

/* xid8funcs.c pg_snapshot_recv, verbatim validation/skip logic over the
 * wire triple ([shim X5]/[shim X6]: 0 = OK, 1 = bad format (22P03),
 * 4 = insufficient data (08P01)).  PG_SNAPSHOT_MAX_NXIP is far above the
 * harness cap; the nxip < 0 check is verbatim. */
extern int	pg_getmsguint32(const unsigned char *data, int32 len,
							int32 *cursor, uint32 *out);
extern int	pg_getmsguint64(const unsigned char *data, int32 len,
							int32 *cursor, uint64 *out);

#define PG_OK_RECV 0			/* pg_scalar_misc.c's PG_OK (macros do not
								 * cross translation units) */

int
pg_snapshot_recv_c(const unsigned char *data, int32 len, int32 *cursor,
				   uint32 *out_nxip, uint64 *out_xmin, uint64 *out_xmax,
				   uint64 *out_xip /* [PG_PROOF_MAX_NXIP] */ )
{
	FullTransactionId last = InvalidFullTransactionId;
	int			nxip;
	int			i;
	FullTransactionId xmin;
	FullTransactionId xmax;
	uint32		u32tmp;
	uint64		u64tmp;
	int			st;

	/* load and validate nxip */
	st = pg_getmsguint32(data, len, cursor, &u32tmp);
	if (st != PG_OK_RECV)
		return st;
	nxip = (int32) u32tmp;
	if (nxip < 0 || nxip > PG_PROOF_MAX_NXIP)	/* [shim X2]: harness stays
												 * far below the real
												 * PG_SNAPSHOT_MAX_NXIP */
		return 1;

	st = pg_getmsguint64(data, len, cursor, &u64tmp);
	if (st != PG_OK_RECV)
		return st;
	xmin = FullTransactionIdFromU64(u64tmp);
	st = pg_getmsguint64(data, len, cursor, &u64tmp);
	if (st != PG_OK_RECV)
		return st;
	xmax = FullTransactionIdFromU64(u64tmp);
	if (!FullTransactionIdIsValid(xmin) ||
		!FullTransactionIdIsValid(xmax) ||
		FullTransactionIdPrecedes(xmax, xmin))
		return 1;

	for (i = 0; i < nxip; i++)
	{
		FullTransactionId cur;

		st = pg_getmsguint64(data, len, cursor, &u64tmp);
		if (st != PG_OK_RECV)
			return st;
		cur = FullTransactionIdFromU64(u64tmp);

		if (FullTransactionIdPrecedes(cur, last) ||
			FullTransactionIdPrecedes(cur, xmin) ||
			FullTransactionIdPrecedes(xmax, cur))
			return 1;

		/* skip duplicate xips */
		if (FullTransactionIdEquals(cur, last))
		{
			i--;
			nxip--;
			continue;
		}

		out_xip[i] = U64FromFullTransactionId(cur);
		last = cur;
	}
	*out_nxip = (uint32) nxip;
	*out_xmin = U64FromFullTransactionId(xmin);
	*out_xmax = U64FromFullTransactionId(xmax);
	return 0;
}

/* xid8funcs.c pg_snapshot_send: int4 nxip, int8 xmin, int8 xmax, int8
 * xip... over the wire shims (big-endian emission + 4B varlena header). */
int32
pg_snapshot_send_c(uint32 nxip, uint64 xmin, uint64 xmax, const uint64 *xip,
				   unsigned char *out)
{
	int32		n = 4;			/* pq_begintypsend header reservation */
	uint32		i;
	int			b;

	out[n++] = (unsigned char) ((nxip >> 24) & 0xFF);
	out[n++] = (unsigned char) ((nxip >> 16) & 0xFF);
	out[n++] = (unsigned char) ((nxip >> 8) & 0xFF);
	out[n++] = (unsigned char) (nxip & 0xFF);
	for (b = 0; b < 8; b++)
		out[n++] = (unsigned char) ((xmin >> (8 * (7 - b))) & 0xFF);
	for (b = 0; b < 8; b++)
		out[n++] = (unsigned char) ((xmax >> (8 * (7 - b))) & 0xFF);
	for (i = 0; i < nxip; i++)
		for (b = 0; b < 8; b++)
			out[n++] = (unsigned char) ((xip[i] >> (8 * (7 - b))) & 0xFF);
	{
		uint32		hdr = (uint32) n << 2;	/* SET_VARSIZE, 4B LE header */

		out[0] = (unsigned char) (hdr & 0xFF);
		out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
		out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
		out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	}
	return n;
}
