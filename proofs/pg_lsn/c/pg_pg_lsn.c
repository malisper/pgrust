/*
 * Vendored PostgreSQL C for Kani dual-execution proofs: pg_lsn family.
 *
 * PROVENANCE
 *   Source file: src/backend/utils/adt/pg_lsn.c
 *   Ref:         postgres/postgres master, fetched 2026-07-28
 *                (raw.githubusercontent.com/postgres/postgres/master/...)
 *   Exception:   pg_lsn_out's format string. master (PG19 devel) formats
 *                "%X/%08X"; REL_18_STABLE (fetched same day) formats
 *                "%X/%X". The shipped pgrust pg_lsn_out_into implements the
 *                REL_18 (and all prior releases') "%X/%X". The standing
 *                equivalence harness therefore vendors the REL_18_STABLE
 *                format (pg_pg_lsn_out_rel18); the master variant is also
 *                vendored (pg_pg_lsn_out_master) and used by a
 *                proved-divergence witness harness that characterizes the
 *                upstream drift. Everything else in this file is byte-for-
 *                byte identical between master and REL_18_STABLE except
 *                pg_lsn_in_safe error plumbing (see SHIM 4).
 *
 * SHIMS (plumbing only, never logic) — every departure from upstream:
 *   1. Types: uint32/uint64/XLogRecPtr typedef'd here; InvalidXLogRecPtr,
 *      MAXPG_LSNLEN, MAXPG_LSNCOMPONENT #define'd verbatim from the source
 *      (postgres.h / xlogdefs.h / pg_lsn.c).
 *   2. strspn(s, "0123456789abcdefABCDEF") -> pg_shim_strspn: hand-vendored
 *      loop with exact C-library strspn semantics (count of leading bytes
 *      of s that appear in accept; stops at NUL). libc has no Kani/CBMC
 *      model, so the call cannot be left as an FFI call.
 *   3. strtoul(str, NULL, 16) -> pg_shim_strtoul16: hand-vendored hex
 *      accumulate loop. Call-site-exact narrowing, argued here once:
 *      strtoul additionally (a) skips leading isspace, (b) accepts an
 *      optional +/- sign, (c) accepts an optional "0x"/"0X" prefix in
 *      base 16, and (d) clamps to ULONG_MAX with errno=ERANGE on overflow.
 *      None of these are reachable at the two call sites in
 *      pg_lsn_in_safe: strtoul is only reached after the strspn checks
 *      pass, which guarantee the pointed-at byte is a hex digit (so no
 *      whitespace and no sign), that a "0x" run is impossible (str[len1]
 *      must be '/' and str[len1+1+len2] must be '\0', so the byte after a
 *      leading '0' in either component is never 'x'), and that exactly
 *      len1<=8 / len2<=8 hex digits precede the terminator (so the value
 *      fits in 32 bits and overflow clamping is unreachable). The shim
 *      parses leading hex digits until the first non-hex byte, exactly as
 *      strtoul does past its prefix handling.
 *   4. ereturn(escontext, ...) -> *have_error = 1 + return
 *      InvalidXLogRecPtr. This restores REL_18_STABLE's own signature
 *      (pg_lsn_in_internal(const char *str, bool *have_error)); master's
 *      body is otherwise identical (goto syntax_error structure kept).
 *      bool -> int for the FFI (Kani lowers Rust bool/()/void mismatches
 *      poorly through goto-cc).
 *   5. snprintf(buf, sizeof buf, "%X/%X", LSN_FORMAT_ARGS(lsn)) ->
 *      pg_shim_fmt_hex32 (printf %X semantics: uppercase hex, minimum
 *      width, "0" for zero) and pg_shim_fmt_hex32_pad8 (%08X: zero-padded
 *      to exactly 8). libc snprintf has no Kani model. LSN_FORMAT_ARGS(x)
 *      is ((uint32) ((x) >> 32)), ((uint32) (x)) — inlined verbatim.
 *   6. fmgr wrappers (PG_FUNCTION_ARGS / PG_GETARG_LSN / PG_RETURN_BOOL)
 *      -> plain C signatures over uint64, returning int for bool. The
 *      comparison/cmp bodies between the unwrap and the return are
 *      verbatim.
 *   Functions are renamed with a pg_ prefix (pg_pg_lsn_*) per proofs/
 *   convention.
 */

typedef unsigned int uint32;
typedef unsigned long long uint64;
typedef uint64 XLogRecPtr;

#define InvalidXLogRecPtr ((XLogRecPtr) 0)

#define MAXPG_LSNLEN 17
#define MAXPG_LSNCOMPONENT 8

/*
 * SHIM 2: exact strspn semantics for the fixed accept set used at both
 * call sites, "0123456789abcdefABCDEF". The membership test is hard-coded
 * as range comparisons instead of a scan of the accept string: iterating
 * the 22-char accept set per input byte multiplies CBMC's unwinding by
 * ~23x with zero semantic content (set membership is identical).
 */
static int
pg_shim_strspn_hex(const char *s)
{
	int			n = 0;

	for (; s[n] != '\0'; n++)
	{
		char		c = s[n];

		if (!((c >= '0' && c <= '9') ||
			  (c >= 'a' && c <= 'f') ||
			  (c >= 'A' && c <= 'F')))
			break;
	}
	return n;
}

/* SHIM 3: strtoul(str, NULL, 16), narrowed to these call sites (see top). */
static unsigned long long
pg_shim_strtoul16(const char *s)
{
	unsigned long long v = 0;

	for (;; s++)
	{
		char		c = *s;
		unsigned	d;

		if (c >= '0' && c <= '9')
			d = c - '0';
		else if (c >= 'a' && c <= 'f')
			d = c - 'a' + 10;
		else if (c >= 'A' && c <= 'F')
			d = c - 'A' + 10;
		else
			break;
		v = (v << 4) | d;
	}
	return v;
}

/*
 * Body verbatim from master pg_lsn_in_safe / REL_18 pg_lsn_in_internal
 * (identical logic); SHIM 3 at the strtoul calls, SHIM 4 at syntax_error.
 */
uint64
pg_pg_lsn_in_safe(const char *str, int *have_error)
{
	int			len1,
				len2;
	uint32		id,
				off;
	XLogRecPtr	result;

	*have_error = 0;

	/* Sanity check input format. */
	len1 = pg_shim_strspn_hex(str);
	if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
		goto syntax_error;

	len2 = pg_shim_strspn_hex(str + len1 + 1);
	if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
		goto syntax_error;

	/* Decode result. */
	id = (uint32) pg_shim_strtoul16(str);
	off = (uint32) pg_shim_strtoul16(str + len1 + 1);
	result = ((uint64) id << 32) | off;

	return result;

syntax_error:
	*have_error = 1;			/* SHIM 4: ereturn -> flag */
	return InvalidXLogRecPtr;
}

/* SHIM 5: printf %X — uppercase, minimum width, "0" for zero. */
static int
pg_shim_fmt_hex32(uint32 v, char *dst)
{
	char		tmp[8];
	int			n = 0;
	int			i;

	do
	{
		unsigned	d = v & 0xf;

		tmp[n++] = (char) (d < 10 ? '0' + d : 'A' + d - 10);
		v >>= 4;
	} while (v != 0);
	for (i = 0; i < n; i++)
		dst[i] = tmp[n - 1 - i];
	return n;
}

/* SHIM 5: printf %08X — uppercase, zero-padded to exactly 8. */
static int
pg_shim_fmt_hex32_pad8(uint32 v, char *dst)
{
	int			i;

	for (i = 7; i >= 0; i--)
	{
		unsigned	d = v & 0xf;

		dst[i] = (char) (d < 10 ? '0' + d : 'A' + d - 10);
		v >>= 4;
	}
	return 8;
}

/*
 * pg_lsn_out core, REL_18_STABLE format:
 *   snprintf(buf, sizeof buf, "%X/%X", LSN_FORMAT_ARGS(lsn));
 * Returns the formatted length; buf must hold MAXPG_LSNLEN + 1 = 18 bytes.
 */
int
pg_pg_lsn_out_rel18(uint64 lsn, char *buf)
{
	int			n;

	n = pg_shim_fmt_hex32((uint32) (lsn >> 32), buf);
	buf[n++] = '/';
	n += pg_shim_fmt_hex32((uint32) lsn, buf + n);
	buf[n] = '\0';
	return n;
}

/*
 * pg_lsn_out core, master (PG19 devel) format:
 *   snprintf(buf, sizeof buf, "%X/%08X", LSN_FORMAT_ARGS(lsn));
 * Used only by the upstream-drift witness harness.
 */
int
pg_pg_lsn_out_master(uint64 lsn, char *buf)
{
	int			n;

	n = pg_shim_fmt_hex32((uint32) (lsn >> 32), buf);
	buf[n++] = '/';
	n += pg_shim_fmt_hex32_pad8((uint32) lsn, buf + n);
	buf[n] = '\0';
	return n;
}

/* SHIM 6: fmgr unwrap only; comparison bodies verbatim. */

int
pg_pg_lsn_eq(uint64 lsn1, uint64 lsn2)
{
	return lsn1 == lsn2;
}

int
pg_pg_lsn_ne(uint64 lsn1, uint64 lsn2)
{
	return lsn1 != lsn2;
}

int
pg_pg_lsn_lt(uint64 lsn1, uint64 lsn2)
{
	return lsn1 < lsn2;
}

int
pg_pg_lsn_gt(uint64 lsn1, uint64 lsn2)
{
	return lsn1 > lsn2;
}

int
pg_pg_lsn_le(uint64 lsn1, uint64 lsn2)
{
	return lsn1 <= lsn2;
}

int
pg_pg_lsn_ge(uint64 lsn1, uint64 lsn2)
{
	return lsn1 >= lsn2;
}

/* btree index opclass support — body verbatim (master). */
int
pg_pg_lsn_cmp(uint64 a, uint64 b)
{
	if (a > b)
		return 1;
	else if (a == b)
		return 0;
	else
		return -1;
}

/* pg_lsn_larger / pg_lsn_smaller (oids 4187/4188) — bodies verbatim
 * (identical in master and REL_18_STABLE, fetched 2026-07-28); fmgr
 * unwrapping per SHIM 6 (PG_GETARG_LSN -> uint64 params, PG_RETURN_LSN ->
 * uint64 return). */
uint64
pg_pg_lsn_larger(uint64 lsn1, uint64 lsn2)
{
	return (lsn1 > lsn2) ? lsn1 : lsn2;
}

uint64
pg_pg_lsn_smaller(uint64 lsn1, uint64 lsn2)
{
	return (lsn1 < lsn2) ? lsn1 : lsn2;
}

/* ==================================================================== */
/* WAVE 5 (2026-07-28): pg_lsn_recv (3238) / pg_lsn_send (3239).         */
/*                                                                       */
/* Provenance (fetched 2026-07-28, REL_18_STABLE):                       */
/*   src/backend/utils/adt/pg_lsn.c  (pg_lsn_recv/pg_lsn_send bodies)    */
/*   src/backend/libpq/pqformat.c    (pq_copymsgbytes, pq_getmsgint64,   */
/*                                    pq_begintypsend, pq_sendint64,     */
/*                                    pq_endtypsend)                     */
/* Shims (plumbing only): the proofs/int-arith wire conventions —        */
/* StringInfo -> (data,len,cursor) triple on recv (insufficient data ->  */
/* status 4, sqlstate 08P01 asserted Rust-side); send buffer caller-     */
/* provided; big-endian emission = the little-endian byte-swap arm of    */
/* port/pg_bswap.h written as explicit shifts; SET_VARSIZE = 4B LE       */
/* header (len << 2).                                                    */
/* ==================================================================== */

#include <string.h>
#include <stdint.h>

static int
pg_lsn_copymsgbytes(const unsigned char *data, int32_t len, int32_t *cursor,
					void *buf, int32_t datalen)
{
	if (datalen < 0 || datalen > (len - *cursor))
		return 4;				/* insufficient data left in message */
	memcpy(buf, &data[*cursor], datalen);
	*cursor += datalen;
	return 0;
}

/* pg_lsn_recv body: result = pq_getmsgint64(buf) */
int
pg_pg_lsn_recv(const unsigned char *data, int32_t len, int32_t *cursor,
			   uint64_t *out)
{
	unsigned char b[8];
	int			st = pg_lsn_copymsgbytes(data, len, cursor, b, 8);
	uint64_t	v = 0;
	int			i;

	if (st != 0)
		return st;
	for (i = 0; i < 8; i++)
		v = (v << 8) | (uint64_t) b[i];
	*out = v;
	return 0;
}

/* pg_lsn_send body: pq_begintypsend + pq_sendint64(lsn) + pq_endtypsend */
int32_t
pg_pg_lsn_send(uint64_t lsn, unsigned char *out /* [12] */ )
{
	uint32_t	hdr = (uint32_t) 12 << 2;
	int			i;

	for (i = 0; i < 8; i++)
		out[4 + i] = (unsigned char) ((lsn >> (8 * (7 - i))) & 0xFF);
	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	return 12;
}
