/*
 * pg_snapio_io.c: vendored PostgreSQL C oracle for the snapio_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/xid8funcs).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/xid8funcs.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3;
 *     verified against ../pgrust-fabled/vendor/postgres-src):
 *     pg_snapshot struct + PG_SNAPSHOT_SIZE/PG_SNAPSHOT_MAX_NXIP, cmp_fxid,
 *     is_visible_fxid, buf_init, buf_add_txid, buf_finalize, parse_snapshot
 *     — verbatim. The fmgr-wrapped bodies pg_snapshot_in/out/recv/send/
 *     xmin/xmax/pg_visible_in_snapshot are vendored with ONLY the
 *     PG_FUNCTION_ARGS plumbing unwrapped to plain C signatures (the
 *     float4in_internal convention in csrc/pg_float_io.c).
 *   - src/include/access/transam.h @ same ref: TransactionId /
 *     FullTransactionId typedefs, the *TransactionId* predicate macros, and
 *     FullTransactionIdFromEpochAndXid / FullTransactionIdFromU64 — verbatim.
 *
 * NOT vendored (used only by out-of-scope state readers, noted so the diff
 * against upstream is auditable): TransactionIdInRecentPast, sort_snapshot
 * (+ lib/qunique.h), pg_current_xact_id*, pg_current_snapshot,
 * pg_snapshot_xip (SRF), pg_xact_status, and the StaticAssertDecl on
 * MAX_BACKENDS (compile-time only, no runtime behavior).
 *
 * Shims (plumbing only, never logic):
 *   - strtou64(str, &endp, 10) -> strtoull, exactly c.h's definition on this
 *     platform (real PostgreSQL defers to libc here too).
 *   - StringInfo: PG's {data,len,maxlen,cursor} struct with realloc-growth
 *     makeStringInfo/initStringInfo/appendBinaryStringInfo/
 *     appendStringInfoChar/appendStringInfo(fmt); appendStringInfo uses
 *     vsnprintf, which is what PG's implementation bottoms out in.
 *   - pq_getmsgint/pq_getmsgint64: big-endian cursor reads; "insufficient
 *     data left in message" (ERRCODE 08P01 class) modeled as
 *     pg_diff_errcode + longjmp, matching PG's ereport(ERROR) nonlocal exit.
 *   - pq_begintypsend/pq_sendint32/pq_sendint64/pq_endtypsend: big-endian
 *     appends into a StringInfo, header handled by the driver entry.
 *   - ereport/ereturn/errcode/errmsg -> record class in pg_diff_errcode
 *     (shared _Thread_local, defined in csrc/pg_float_io.c); messages
 *     unevaluated. escontext is always NULL (hard-error shape).
 *   - palloc/pfree -> tracked malloc/free through a per-iteration allocation
 *     registry (snapio_alloc/snapio_free/snapio_reset): real PG reclaims
 *     error-path allocations via memory-context reset at transaction abort;
 *     a bare malloc shim leaked them on every bad_format/longjmp exit (LSan
 *     artifact leak-99e971cb, repro "12:13:0" — buf_init's StringInfo leaked
 *     past parse_snapshot's goto bad_format). Every driver entry resets the
 *     registry on entry and before every return, after results are copied to
 *     caller buffers. Plumbing only: allocation lifetime, never computation.
 *   - SET_VARSIZE/VARSIZE -> 4B little-endian
 *     header word (len<<2), exactly postgres.h's va_4byte layout.
 *   - Errcode classes: 1 = ERRCODE_INVALID_TEXT_REPRESENTATION (22P02),
 *     5 = ERRCODE_INVALID_BINARY_REPRESENTATION (22P03),
 *     6 = ERRCODE_PROTOCOL_VIOLATION (08P01, from the pq shims).
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <setjmp.h>
#include <stddef.h>
#include <inttypes.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_ERR_INVALID_TEXT 1		/* 22P02 */
#define PG_DIFF_ERR_INVALID_BINARY 5	/* 22P03 */
#define PG_DIFF_ERR_PROTOCOL 6			/* 08P01 */

/* ---- fixed-width names as in c.h (shim) ---- */
typedef int32_t int32;
typedef uint32_t uint32;
typedef int64_t int64;
typedef uint64_t uint64;
typedef size_t Size;

#define MaxAllocSize ((Size) 0x3fffffff)	/* verbatim, utils/memutils.h */

#define strtou64(str, endptr, base) ((uint64) strtoull(str, endptr, base))

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define unlikely(x) (x)

/* ---- errcode/report shims (see header) ---- */
#define ERRCODE_INVALID_TEXT_REPRESENTATION PG_DIFF_ERR_INVALID_TEXT
#define ERRCODE_INVALID_BINARY_REPRESENTATION PG_DIFF_ERR_INVALID_BINARY
#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define ereturn(escontext, ret, stuff) do { (void) (stuff); return (ret); } while (0)
#define ereport(level, stuff) ((void) (stuff))
#define ERROR 21

typedef struct Node Node;		/* opaque; escontext always NULL here */

/*
 * Per-iteration allocation registry (see header): every shim allocation is
 * tracked so error/longjmp exits cannot leak — real PG reclaims these via
 * memory-context reset at transaction abort. snapio_reset() runs at every
 * driver-entry start AND before every driver-entry return.
 */
#define SNAPIO_TRACK_MAX 4096
static _Thread_local void *snapio_allocs[SNAPIO_TRACK_MAX];
static _Thread_local int snapio_nallocs;

static void *
snapio_alloc(size_t n)
{
	void	   *p = malloc(n);

	if (snapio_nallocs >= SNAPIO_TRACK_MAX)
		abort();				/* registry overflow = driver bug */
	snapio_allocs[snapio_nallocs++] = p;
	return p;
}

static void *
snapio_realloc(void *old, size_t n)
{
	void	   *p = realloc(old, n);
	int			i;

	for (i = snapio_nallocs - 1; i >= 0; i--)
	{
		if (snapio_allocs[i] == old)
		{
			snapio_allocs[i] = p;
			return p;
		}
	}
	abort();					/* realloc of untracked pointer = driver bug */
}

static void
snapio_free(void *p)
{
	int			i;

	if (p == NULL)
		return;
	for (i = snapio_nallocs - 1; i >= 0; i--)
	{
		if (snapio_allocs[i] == p)
		{
			snapio_allocs[i] = snapio_allocs[--snapio_nallocs];
			free(p);
			return;
		}
	}
	abort();					/* free of untracked pointer = driver bug */
}

static void
snapio_reset(void)
{
	int			i;

	for (i = 0; i < snapio_nallocs; i++)
		free(snapio_allocs[i]);
	snapio_nallocs = 0;
}

#define palloc(n) snapio_alloc(n)
#define pfree(p) snapio_free(p)

/* 4B little-endian varlena header (postgres.h va_4byte, LP64 LE) */
#define SET_VARSIZE(PTR, len) (*((uint32 *) (PTR)) = ((uint32) (len)) << 2)
#define VARSIZE(PTR) ((*((const uint32 *) (PTR))) >> 2)

/* ==================== access/transam.h (VERBATIM) ==================== */

typedef uint32 TransactionId;

#define InvalidTransactionId		((TransactionId) 0)
#define FirstNormalTransactionId	((TransactionId) 3)
#define TransactionIdIsValid(xid)		((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid)		((xid) >= FirstNormalTransactionId)

typedef struct FullTransactionId
{
	uint64		value;
} FullTransactionId;

#define XidFromFullTransactionId(x)		((uint32) (x).value)
#define U64FromFullTransactionId(x)		((x).value)
#define FullTransactionIdEquals(a, b)	((a).value == (b).value)
#define FullTransactionIdPrecedes(a, b)	((a).value < (b).value)
#define FullTransactionIdFollowsOrEquals(a, b) ((a).value >= (b).value)
#define FullTransactionIdIsValid(x)		TransactionIdIsValid(XidFromFullTransactionId(x))
#define InvalidFullTransactionId		FullTransactionIdFromEpochAndXid(0, InvalidTransactionId)

static inline FullTransactionId
FullTransactionIdFromEpochAndXid(uint32 epoch, TransactionId xid)
{
	FullTransactionId result;

	result.value = ((uint64) epoch) << 32 | xid;

	return result;
}

static inline FullTransactionId
FullTransactionIdFromU64(uint64 value)
{
	FullTransactionId result;

	result.value = value;

	return result;
}

/* ==================== StringInfo shim (see header) ==================== */

typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

static void
si_ensure(StringInfo str, int more)
{
	if (str->len + more + 1 > str->maxlen)
	{
		while (str->len + more + 1 > str->maxlen)
			str->maxlen *= 2;
		str->data = snapio_realloc(str->data, str->maxlen);
	}
}

static void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = snapio_alloc(str->maxlen);
	str->len = 0;
	str->cursor = 0;
	str->data[0] = '\0';
}

static StringInfo
makeStringInfo(void)
{
	StringInfo	res = snapio_alloc(sizeof(StringInfoData));

	initStringInfo(res);
	return res;
}

static void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	si_ensure(str, datalen);
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	si_ensure(str, 1);
	str->data[str->len++] = ch;
	str->data[str->len] = '\0';
}

static void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	char		tmp[64];
	va_list		ap;
	int			n;

	va_start(ap, fmt);
	n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
	va_end(ap);
	appendBinaryStringInfo(str, tmp, n);
}

/* ---- pqformat shims: big-endian cursor reads, PG-throw = longjmp ---- */

static jmp_buf pg_diff_snapio_jmp;

static void
pq_insufficient(void)
{
	pg_diff_errcode = PG_DIFF_ERR_PROTOCOL;
	longjmp(pg_diff_snapio_jmp, 1);
}

static uint32
pq_getmsgint(StringInfo buf, int n)
{
	uint32		v = 0;
	int			i;

	if (n != 4)
		pq_insufficient();		/* only the 4-byte form is used here */
	if (buf->cursor + n > buf->len)
		pq_insufficient();
	for (i = 0; i < n; i++)
		v = (v << 8) | (unsigned char) buf->data[buf->cursor + i];
	buf->cursor += n;
	return v;
}

static int64
pq_getmsgint64(StringInfo buf)
{
	uint64		v = 0;
	int			i;

	if (buf->cursor + 8 > buf->len)
		pq_insufficient();
	for (i = 0; i < 8; i++)
		v = (v << 8) | (unsigned char) buf->data[buf->cursor + i];
	buf->cursor += 8;
	return (int64) v;
}

static void
pq_sendint32(StringInfo buf, uint32 v)
{
	unsigned char b[4];

	b[0] = v >> 24;
	b[1] = v >> 16;
	b[2] = v >> 8;
	b[3] = v;
	appendBinaryStringInfo(buf, b, 4);
}

static void
pq_sendint64(StringInfo buf, uint64 v)
{
	unsigned char b[8];
	int			i;

	for (i = 0; i < 8; i++)
		b[i] = (unsigned char) (v >> (56 - 8 * i));
	appendBinaryStringInfo(buf, b, 8);
}

/* ============ SECTION 1: xid8funcs.c helpers (VERBATIM) ============ */

/*
 * If defined, use bsearch() function for searching for xid8s in snapshots
 * that have more than the specified number of values.
 */
#define USE_BSEARCH_IF_NXIP_GREATER 30

/*
 * Snapshot containing FullTransactionIds.
 */
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
#define PG_SNAPSHOT_MAX_NXIP \
	((MaxAllocSize - offsetof(pg_snapshot, xip)) / sizeof(FullTransactionId))

/*
 * txid comparator for qsort/bsearch
 */
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
 * check fxid visibility.
 */
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
		void	   *res;

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

/*
 * helper functions to use StringInfo for pg_snapshot creation.
 */

static StringInfo
buf_init(FullTransactionId xmin, FullTransactionId xmax)
{
	pg_snapshot snap;
	StringInfo	buf;

	snap.xmin = xmin;
	snap.xmax = xmax;
	snap.nxip = 0;

	buf = makeStringInfo();
	appendBinaryStringInfo(buf, &snap, PG_SNAPSHOT_SIZE(0));
	return buf;
}

static void
buf_add_txid(StringInfo buf, FullTransactionId fxid)
{
	pg_snapshot *snap = (pg_snapshot *) buf->data;

	/* do this before possible realloc */
	snap->nxip++;

	appendBinaryStringInfo(buf, &fxid, sizeof(fxid));
}

static pg_snapshot *
buf_finalize(StringInfo buf)
{
	pg_snapshot *snap = (pg_snapshot *) buf->data;

	SET_VARSIZE(snap, buf->len);

	/* buf is not needed anymore */
	buf->data = NULL;
	pfree(buf);

	return snap;
}

/*
 * parse snapshot from cstring
 */
static pg_snapshot *
parse_snapshot(const char *str, Node *escontext)
{
	FullTransactionId xmin;
	FullTransactionId xmax;
	FullTransactionId last_val = InvalidFullTransactionId;
	FullTransactionId val;
	const char *str_start = str;
	char	   *endp;
	StringInfo	buf;

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
	buf = buf_init(xmin, xmax);

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
	ereturn(escontext, NULL,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"pg_snapshot", str_start)));
}

/* ===== SECTION 2: unwrapped fmgr bodies (VERBATIM modulo arg plumbing) ===== */

/*
 * pg_snapshot_out body: fmgr unwrapped to (snap in, StringInfo out).
 * UINT64_FORMAT spelled PRIu64 (same object format on this platform).
 */
static void
pg_snapio_out_body(const pg_snapshot *snap, StringInfo str)
{
	uint32		i;

	initStringInfo(str);

	appendStringInfo(str, "%" PRIu64 ":",
					 U64FromFullTransactionId(snap->xmin));
	appendStringInfo(str, "%" PRIu64 ":",
					 U64FromFullTransactionId(snap->xmax));

	for (i = 0; i < snap->nxip; i++)
	{
		if (i > 0)
			appendStringInfoChar(str, ',');
		appendStringInfo(str, "%" PRIu64,
						 U64FromFullTransactionId(snap->xip[i]));
	}
}

/*
 * pg_snapshot_recv body: fmgr unwrapped to (StringInfo in, snap out).
 * Returns NULL on bad_format with pg_diff_errcode set (the ereport(ERROR)
 * shape); pq shims longjmp on insufficient data (see driver entry).
 * One plumbing-only deviation: pfree(snap) before the in-loop bad_format
 * goto (PG's copy leaks into the aborted memory context; here it's malloc).
 */
static pg_snapshot *
pg_snapio_recv_body(StringInfo buf)
{
	pg_snapshot *snap;
	FullTransactionId last = InvalidFullTransactionId;
	int			nxip;
	int			i;
	FullTransactionId xmin;
	FullTransactionId xmax;

	/* load and validate nxip */
	nxip = pq_getmsgint(buf, 4);
	if (nxip < 0 || nxip > PG_SNAPSHOT_MAX_NXIP)
		goto bad_format;

	xmin = FullTransactionIdFromU64((uint64) pq_getmsgint64(buf));
	xmax = FullTransactionIdFromU64((uint64) pq_getmsgint64(buf));
	if (!FullTransactionIdIsValid(xmin) ||
		!FullTransactionIdIsValid(xmax) ||
		FullTransactionIdPrecedes(xmax, xmin))
		goto bad_format;

	snap = palloc(PG_SNAPSHOT_SIZE(nxip));
	snap->xmin = xmin;
	snap->xmax = xmax;

	for (i = 0; i < nxip; i++)
	{
		FullTransactionId cur =
			FullTransactionIdFromU64((uint64) pq_getmsgint64(buf));

		if (FullTransactionIdPrecedes(cur, last) ||
			FullTransactionIdPrecedes(cur, xmin) ||
			FullTransactionIdPrecedes(xmax, cur))
		{
			pfree(snap);
			goto bad_format;
		}

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
	SET_VARSIZE(snap, PG_SNAPSHOT_SIZE(nxip));
	return snap;

bad_format:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
			 errmsg("invalid external pg_snapshot data")));
	return NULL;				/* keep compiler quiet */
}

/*
 * pg_snapshot_send body: fmgr unwrapped to (snap in, StringInfo wire out);
 * pq_begintypsend/pq_endtypsend header handled by the driver entry (the
 * wire payload bytes are what both sides compare).
 */
static void
pg_snapio_send_body(const pg_snapshot *snap, StringInfo buf)
{
	uint32		i;

	initStringInfo(buf);
	pq_sendint32(buf, snap->nxip);
	pq_sendint64(buf, (uint64) U64FromFullTransactionId(snap->xmin));
	pq_sendint64(buf, (uint64) U64FromFullTransactionId(snap->xmax));
	for (i = 0; i < snap->nxip; i++)
		pq_sendint64(buf, (uint64) U64FromFullTransactionId(snap->xip[i]));
}

/* ========== SECTION 3: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * All image buffers are full varlena images (4-byte header + payload),
 * matching the shipped Rust Varlena::as_bytes() layout byte-for-byte
 * (asserted equal offsets: nxip @4, xmin @8, xmax @16, xip @24).
 * Return 0 = ok, 1 = error (class in pg_diff_errcode). -1 = capacity
 * overflow (driver bug, not a verdict).
 */

int
pg_diff_pg_snapshot_in(const char *str, unsigned char *out, int cap, int *outlen)
{
	pg_snapshot *snap;
	int			sz;

	pg_diff_errcode = 0;
	snapio_reset();
	snap = parse_snapshot(str, NULL);
	if (snap == NULL)
	{
		snapio_reset();
		return 1;
	}
	sz = (int) VARSIZE(snap);
	if (sz > cap)
	{
		snapio_reset();
		return -1;
	}
	memcpy(out, snap, sz);
	*outlen = sz;
	snapio_reset();
	return 0;
}

int
pg_diff_pg_snapshot_out(const unsigned char *img, char *out, int cap, int *outlen)
{
	StringInfoData str;

	pg_diff_errcode = 0;
	snapio_reset();
	pg_snapio_out_body((const pg_snapshot *) img, &str);
	if (str.len + 1 > cap)
	{
		snapio_reset();
		return -1;
	}
	memcpy(out, str.data, str.len + 1);
	*outlen = str.len;
	snapio_reset();
	return 0;
}

int
pg_diff_pg_snapshot_recv(const unsigned char *wire, int wirelen,
						 unsigned char *out, int cap, int *outlen)
{
	StringInfoData buf;
	pg_snapshot *snap;
	int			sz;

	pg_diff_errcode = 0;
	snapio_reset();
	if (setjmp(pg_diff_snapio_jmp) != 0)
	{
		snapio_reset();			/* pq shim threw past live allocations */
		return 1;				/* (errcode already set) */
	}
	buf.data = (char *) wire;
	buf.len = wirelen;
	buf.maxlen = wirelen;
	buf.cursor = 0;
	snap = pg_snapio_recv_body(&buf);
	if (snap == NULL)
	{
		snapio_reset();
		return 1;
	}
	sz = (int) VARSIZE(snap);
	if (sz > cap)
	{
		snapio_reset();
		return -1;
	}
	memcpy(out, snap, sz);
	*outlen = sz;
	snapio_reset();
	return 0;
}

int
pg_diff_pg_snapshot_send(const unsigned char *img, unsigned char *out, int cap,
						 int *outlen)
{
	StringInfoData buf;

	pg_diff_errcode = 0;
	snapio_reset();
	pg_snapio_send_body((const pg_snapshot *) img, &buf);
	if (buf.len > cap)
	{
		snapio_reset();
		return -1;
	}
	memcpy(out, buf.data, buf.len);
	*outlen = buf.len;
	snapio_reset();
	return 0;
}

uint64_t
pg_diff_pg_snapshot_xmin(const unsigned char *img)
{
	return U64FromFullTransactionId(((const pg_snapshot *) img)->xmin);
}

uint64_t
pg_diff_pg_snapshot_xmax(const unsigned char *img)
{
	return U64FromFullTransactionId(((const pg_snapshot *) img)->xmax);
}

int
pg_diff_pg_visible_in_snapshot(uint64_t fxid, const unsigned char *img)
{
	return is_visible_fxid(FullTransactionIdFromU64(fxid),
						   (const pg_snapshot *) img) ? 1 : 0;
}
