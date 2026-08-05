/*
 * pg_network_io.c: vendored PostgreSQL C oracle for the network_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/network).
 *
 * PROVENANCE (all bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src
 * @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3):
 *   - src/backend/utils/adt/inet_net_pton.c — WHOLE parser cascade verbatim
 *     (pg_inet_net_pton, inet_net_pton_ipv4, inet_cidr_pton_ipv4, getbits,
 *     getv4, inet_net_pton_ipv6, inet_cidr_pton_ipv6).
 *   - src/port/inet_net_ntop.c — pg_inet_net_ntop, inet_net_ntop_ipv4,
 *     inet_net_ntop_ipv6, decoct verbatim.
 *   - src/backend/utils/adt/inet_cidr_ntop.c — pg_inet_cidr_ntop,
 *     inet_cidr_ntop_ipv4, inet_cidr_ntop_ipv6 verbatim.
 *   - src/backend/utils/adt/network.c — network_in, network_out,
 *     cidr_set_masklen_internal, network_cmp_internal, bitncmp, bitncommon,
 *     addressOK, internal_inetpl verbatim as free functions; the fmgr-Datum
 *     functions (inet_set_masklen, cidr_set_masklen, inet_to_cidr,
 *     network_host, network_show, inet_abbrev, cidr_abbrev,
 *     network_broadcast, network_network, network_netmask, network_hostmask,
 *     inet_same_family, inet_merge, inetnot, inetand, inetor, inetpl,
 *     inetmi_int8, inetmi, network_abbrev_convert) are vendored INSIDE the
 *     pg_diff_* driver entries below with their PG_GETARG/PG_RETURN plumbing
 *     unwrapped to plain C parameters; every statement between the GETARG
 *     prologue and the RETURN epilogue is byte-for-byte upstream.
 *
 * SHIMS (plumbing only, never logic):
 *   - inet varlena: modeled as struct { family; bits; ipaddr[16] } without
 *     the 4-byte varlena header (the header is asserted Rust-side against
 *     InetValue::image()); ip_family/ip_bits/ip_addr/ip_addrsize/ip_maxbits
 *     are the exact utils/inet.h field accessors over that struct;
 *     SET_INET_VARSIZE is a no-op (header lives Rust-side).
 *   - palloc0/palloc(sizeof(inet)) -> rotating thread-local static pool
 *     (zeroed per grab, matching palloc0; the one palloc caller,
 *     inet_set_masklen, immediately memcpy-overwrites the whole struct);
 *     VARSIZE_ANY(src) in that memcpy -> sizeof(pgc_inet).
 *   - ereport(ERROR, (errcode(X), ...)) / ereturn(escontext, ...) ->
 *     errcode(X) records X in the thread-local pg_diff_errcode, then
 *     longjmp back to the driver entry (escontext is always NULL here: the
 *     hard-error shape); errmsg/errdetail evaluate to 0, args unevaluated.
 *     elog(ERROR, ...) records PG_DIFF_ERR_INTERNAL (XX000) and longjmps.
 *   - pstrdup(tmp) in network_out -> thread-local buffer sized EXACTLY to
 *     strlen(s)+1 (plus a 0xA5 guard band past the NUL), realloc'd DOWN as
 *     well as up per call, with slack/band probes — the pg_float_io.c
 *     pattern of record (THE SCRIBBLER, task #112 + its exact-sizing
 *     follow-up). See the block comment at pstrdup below for the mcxt.c
 *     contract and why the SIZE is load-bearing.
 *   - cstring_to_text(tmp) + PG_RETURN_TEXT_P -> the driver entry copies
 *     tmp into the caller's buffer and returns its strlen (text varlena
 *     packaging is asserted Rust-side against the fc_* wrapper).
 *   - network_abbrev_convert: SortSupport/hyperLogLog estimation tail
 *     (uss->input_count, addHyperLogLog) dropped — sortsupport bookkeeping
 *     that runs after the returned key value is complete; the key
 *     computation itself is verbatim. Datum -> uint64_t, SIZEOF_DATUM 8,
 *     BITS_PER_BYTE 8, DatumBigEndianToNative/pg_bswap32 ->
 *     __builtin_bswap64/32 (little-endian hosts only, like the shipped
 *     crate); ABBREV_BITS_INET4_NETMASK_SIZE/SUBNET constants inlined as
 *     their network.c values 6/25.
 *   - u_char/u_int spelled via macros; Min/Max/IS_HIGHBIT_SET are the c.h
 *     definitions; Assert -> no-op (release PG); assert() in the pton
 *     cascade kept live via <assert.h> (pure invariants on table lookups).
 *
 * ERRCODE CLASSES recorded in pg_diff_errcode (thread-local, defined in
 * csrc/pg_float_io.c):
 *   1 = ERRCODE_INVALID_TEXT_REPRESENTATION  (22P02)
 *   2 = ERRCODE_INVALID_PARAMETER_VALUE      (22023)
 *   3 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE   (22003)
 *   4 = ERRCODE_INVALID_BINARY_REPRESENTATION (22P03)
 *   5 = internal error (elog ERROR, XX000)
 */

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>				/* realloc/abort for the pstrdup size contract */
#include <string.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define ERRCODE_INVALID_PARAMETER_VALUE 2
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 3
#define ERRCODE_INVALID_BINARY_REPRESENTATION 4
#define PG_DIFF_ERR_INTERNAL 5

/* ---- shims (see header) ---- */

static _Thread_local jmp_buf pg_network_jmp;

#define ereport(elevel, rest) \
	do { (void) (rest); longjmp(pg_network_jmp, 1); } while (0)
#define ereturn(escontext, dummy, rest) \
	do { (void) (rest); longjmp(pg_network_jmp, 1); } while (0)
#define elog(elevel, ...) \
	do { pg_diff_errcode = PG_DIFF_ERR_INTERNAL; longjmp(pg_network_jmp, 1); } while (0)
#define errcode(c) (pg_diff_errcode = (c))
#define errmsg(...) 0
#define errdetail(...) 0

typedef int64_t int64;
typedef uint64_t uint64;
typedef int32_t int32;
typedef uint32_t uint32;

#define u_char unsigned char
#define u_int unsigned int

typedef struct Node Node;		/* opaque; escontext is always NULL here */

#define PGSQL_AF_INET	2
#define PGSQL_AF_INET6	3

typedef struct pgc_inet
{
	unsigned char family;
	unsigned char bits;
	unsigned char ipaddr[16];
} pgc_inet;
typedef pgc_inet inet;

#define ip_family(inetptr)	((inetptr)->family)
#define ip_bits(inetptr)	((inetptr)->bits)
#define ip_addr(inetptr)	((inetptr)->ipaddr)
#define ip_addrsize(inetptr) (ip_family(inetptr) == PGSQL_AF_INET ? 4 : 16)
#define ip_maxbits(inetptr)	(ip_family(inetptr) == PGSQL_AF_INET ? 32 : 128)
#define SET_INET_VARSIZE(dst) ((void) 0)
#define VARSIZE_ANY(p) sizeof(pgc_inet)

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)
#define Assert(condition) ((void) 0)

/* rotating static pool standing in for palloc0(sizeof(inet)) */
static _Thread_local pgc_inet pg_inet_pool[8];
static _Thread_local unsigned pg_inet_pi;

static void *
pgc_inet_alloc0(void)
{
	pgc_inet   *p = &pg_inet_pool[pg_inet_pi++ & 7];

	memset(p, 0, sizeof(*p));
	return p;
}

#define palloc0(sz) pgc_inet_alloc0()
#define palloc(sz) pgc_inet_alloc0()

/*
 * pstrdup — THE SCRIBBLER class, second instance (task #131 rework of the
 * refuted 515fffe6d6a; first instance was pg_float_io.c, task #112).
 *
 * The real contract (vendor/postgres-src, PostgreSQL 18.3):
 *
 *     src/backend/utils/mmgr/mcxt.c:1724-1728
 *         char *pstrdup(const char *in)
 *         { return MemoryContextStrdup(CurrentMemoryContext, in); }
 *     src/backend/utils/mmgr/mcxt.c:1711-1722  MemoryContextStrdup:
 *         Size len = strlen(string) + 1;
 *         nstr = (char *) MemoryContextAlloc(context, len);
 *         memcpy(nstr, string, len);
 *
 * EXACTLY strlen(s)+1 bytes, never truncated. The SIZE is load-bearing even
 * when the contents are out of comparator scope, because verbatim bodies
 * index a pstrdup result by input-derived offsets (float{4,8}in_internal's
 * `errnumber[endptr - num] = '\0'` is the attributed instance: at 256 fixed
 * bytes in pg_float_io.c it wrote one NUL 1346 bytes past the buffer onto
 * another TU's datecache). This TU's previous shim was a fixed 64-byte
 * truncating buffer — the same shape.
 *
 * Sizing is EXACT, not grow-never-shrink (the pg_float_io.c pattern of
 * record): sizing up only would leave slack after a long call, so a later
 * input-derived store past strlen would land in slack and go UNSEEN by the
 * guard band. realloc DOWN as well as up; keep a 64-byte 0xA5 band
 * immediately after the NUL so an over-index is named at the next oracle
 * exit (pg_network_msgbuf_check, wired next to the float H6 check in
 * fuzz/core/src/lib.rs OracleSerial::drop).
 *
 * Deviation kept and bounded: PG hands out a FRESH chunk per call; this shim
 * reuses one thread-local allocation, so two live results would alias. The
 * TU's single call site (network_out, below) consumes the result before any
 * other pstrdup can run, and the band check makes a violated overrun loud.
 */
static _Thread_local char *pg_network_msgbuf;
static _Thread_local size_t pg_network_msgbuf_cap;
static _Thread_local size_t pg_network_msgbuf_len;

#define PG_NETWORK_MSGBUF_GUARD 64
#define PG_NETWORK_MSGBUF_FILL 0xA5

static char *
pstrdup(const char *s)
{
	size_t		n = strlen(s);
	size_t		want = n + 1 + PG_NETWORK_MSGBUF_GUARD;

	if (want != pg_network_msgbuf_cap)
	{
		char	   *p = realloc(pg_network_msgbuf, want);

		if (p == NULL)
			abort();			/* OOM in a shim: loud, never silent */
		pg_network_msgbuf = p;
		pg_network_msgbuf_cap = want;
	}
	memcpy(pg_network_msgbuf, s, n);
	pg_network_msgbuf[n] = '\0';
	pg_network_msgbuf_len = n;
	memset(pg_network_msgbuf + n + 1, PG_NETWORK_MSGBUF_FILL,
		   PG_NETWORK_MSGBUF_GUARD);
	return pg_network_msgbuf;
}

/*
 * Test probes (read-only, never on a comparator path).
 *
 * pg_network_pstrdup_len_probe: strlen(pstrdup(s)), which the mcxt.c
 * contract makes == strlen(s) for EVERY s. Catches any truncating rewrite up
 * to the longest length the pin drives.
 *
 * pg_network_msgbuf_slack: bytes the allocation carries beyond
 * strlen+1+GUARD. Exact sizing keeps this 0 after EVERY call; any fixed-size
 * buffer (of any size — the refuted pin was blind past 4096) and any
 * grow-never-shrink policy reports nonzero slack for some length, so the pin
 * over this probe fails for ANY wrong-size buffer, not just 64. -1 = no call
 * yet on this thread.
 */
size_t
pg_network_pstrdup_len_probe(const char *s)
{
	return strlen(pstrdup(s));
}

int
pg_network_msgbuf_slack(void)
{
	if (pg_network_msgbuf == NULL)
		return -1;
	return (int) (pg_network_msgbuf_cap -
				  (pg_network_msgbuf_len + 1 + PG_NETWORK_MSGBUF_GUARD));
}

/*
 * 0 = intact. 1 = capacity smaller than the string it holds (a truncating
 * shim is back). 2+off = guard byte at offset off clobbered (a body indexed
 * past the string it was handed). Called at oracle exit depth 0 from
 * OracleSerial::drop (fuzz/core/src/lib.rs), release-effective.
 */
int
pg_network_msgbuf_check(void)
{
	if (pg_network_msgbuf == NULL)
		return 0;				/* pstrdup not reached on this thread yet */
	if (pg_network_msgbuf_len + 1 + PG_NETWORK_MSGBUF_GUARD >
		pg_network_msgbuf_cap)
		return 1;
	for (size_t i = 0; i < PG_NETWORK_MSGBUF_GUARD; i++)
	{
		if ((unsigned char) pg_network_msgbuf[pg_network_msgbuf_len + 1 + i]
			!= PG_NETWORK_MSGBUF_FILL)
		{
			/* self-heal: re-arm the band so one hit cannot cascade */
			memset(pg_network_msgbuf + pg_network_msgbuf_len + 1,
				   PG_NETWORK_MSGBUF_FILL, PG_NETWORK_MSGBUF_GUARD);
			return 2 + (int) i;
		}
	}
	return 0;
}

/* ============ SECTION 1: src/backend/utils/adt/inet_net_pton.c ============ */
/* WHOLE parser cascade VERBATIM @ 62d6c7d3df. */

static int	inet_net_pton_ipv4(const char *src, u_char *dst);
static int	inet_cidr_pton_ipv4(const char *src, u_char *dst, size_t size);
static int	inet_net_pton_ipv6(const char *src, u_char *dst);
static int	inet_cidr_pton_ipv6(const char *src, u_char *dst, size_t size);

int
pg_inet_net_pton(int af, const char *src, void *dst, size_t size)
{
	switch (af)
	{
		case PGSQL_AF_INET:
			return size == -1 ?
				inet_net_pton_ipv4(src, dst) :
				inet_cidr_pton_ipv4(src, dst, size);
		case PGSQL_AF_INET6:
			return size == -1 ?
				inet_net_pton_ipv6(src, dst) :
				inet_cidr_pton_ipv6(src, dst, size);
		default:
			errno = EAFNOSUPPORT;
			return -1;
	}
}

static int
inet_cidr_pton_ipv4(const char *src, u_char *dst, size_t size)
{
	static const char xdigits[] = "0123456789abcdef";
	static const char digits[] = "0123456789";
	int			n,
				ch,
				tmp = 0,
				dirty,
				bits;
	const u_char *odst = dst;

	ch = *src++;
	if (ch == '0' && (src[0] == 'x' || src[0] == 'X')
		&& isxdigit((unsigned char) src[1]))
	{
		/* Hexadecimal: Eat nybble string. */
		if (size <= 0U)
			goto emsgsize;
		dirty = 0;
		src++;					/* skip x or X. */
		while ((ch = *src++) != '\0' && isxdigit((unsigned char) ch))
		{
			if (isupper((unsigned char) ch))
				ch = tolower((unsigned char) ch);
			n = strchr(xdigits, ch) - xdigits;
			assert(n >= 0 && n <= 15);
			if (dirty == 0)
				tmp = n;
			else
				tmp = (tmp << 4) | n;
			if (++dirty == 2)
			{
				if (size-- <= 0U)
					goto emsgsize;
				*dst++ = (u_char) tmp;
				dirty = 0;
			}
		}
		if (dirty)
		{						/* Odd trailing nybble? */
			if (size-- <= 0U)
				goto emsgsize;
			*dst++ = (u_char) (tmp << 4);
		}
	}
	else if (isdigit((unsigned char) ch))
	{
		/* Decimal: eat dotted digit string. */
		for (;;)
		{
			tmp = 0;
			do
			{
				n = strchr(digits, ch) - digits;
				assert(n >= 0 && n <= 9);
				tmp *= 10;
				tmp += n;
				if (tmp > 255)
					goto enoent;
			} while ((ch = *src++) != '\0' &&
					 isdigit((unsigned char) ch));
			if (size-- <= 0U)
				goto emsgsize;
			*dst++ = (u_char) tmp;
			if (ch == '\0' || ch == '/')
				break;
			if (ch != '.')
				goto enoent;
			ch = *src++;
			if (!isdigit((unsigned char) ch))
				goto enoent;
		}
	}
	else
		goto enoent;

	bits = -1;
	if (ch == '/' && isdigit((unsigned char) src[0]) && dst > odst)
	{
		/* CIDR width specifier.  Nothing can follow it. */
		ch = *src++;			/* Skip over the /. */
		bits = 0;
		do
		{
			n = strchr(digits, ch) - digits;
			assert(n >= 0 && n <= 9);
			bits *= 10;
			bits += n;
		} while ((ch = *src++) != '\0' && isdigit((unsigned char) ch));
		if (ch != '\0')
			goto enoent;
		if (bits > 32)
			goto emsgsize;
	}

	/* Fiery death and destruction unless we prefetched EOS. */
	if (ch != '\0')
		goto enoent;

	/* If nothing was written to the destination, we found no address. */
	if (dst == odst)
		goto enoent;
	/* If no CIDR spec was given, infer width from net class. */
	if (bits == -1)
	{
		if (*odst >= 240)		/* Class E */
			bits = 32;
		else if (*odst >= 224)	/* Class D */
			bits = 8;
		else if (*odst >= 192)	/* Class C */
			bits = 24;
		else if (*odst >= 128)	/* Class B */
			bits = 16;
		else
			/* Class A */
			bits = 8;
		/* If imputed mask is narrower than specified octets, widen. */
		if (bits < ((dst - odst) * 8))
			bits = (dst - odst) * 8;

		/*
		 * If there are no additional bits specified for a class D address
		 * adjust bits to 4.
		 */
		if (bits == 8 && *odst == 224)
			bits = 4;
	}
	/* Extend network to cover the actual mask. */
	while (bits > ((dst - odst) * 8))
	{
		if (size-- <= 0U)
			goto emsgsize;
		*dst++ = '\0';
	}
	return bits;

enoent:
	errno = ENOENT;
	return -1;

emsgsize:
	errno = EMSGSIZE;
	return -1;
}

static int
inet_net_pton_ipv4(const char *src, u_char *dst)
{
	static const char digits[] = "0123456789";
	const u_char *odst = dst;
	int			n,
				ch,
				tmp,
				bits;
	size_t		size = 4;

	/* Get the mantissa. */
	while (ch = *src++, isdigit((unsigned char) ch))
	{
		tmp = 0;
		do
		{
			n = strchr(digits, ch) - digits;
			assert(n >= 0 && n <= 9);
			tmp *= 10;
			tmp += n;
			if (tmp > 255)
				goto enoent;
		} while ((ch = *src++) != '\0' && isdigit((unsigned char) ch));
		if (size-- == 0)
			goto emsgsize;
		*dst++ = (u_char) tmp;
		if (ch == '\0' || ch == '/')
			break;
		if (ch != '.')
			goto enoent;
	}

	/* Get the prefix length if any. */
	bits = -1;
	if (ch == '/' && isdigit((unsigned char) src[0]) && dst > odst)
	{
		/* CIDR width specifier.  Nothing can follow it. */
		ch = *src++;			/* Skip over the /. */
		bits = 0;
		do
		{
			n = strchr(digits, ch) - digits;
			assert(n >= 0 && n <= 9);
			bits *= 10;
			bits += n;
		} while ((ch = *src++) != '\0' && isdigit((unsigned char) ch));
		if (ch != '\0')
			goto enoent;
		if (bits > 32)
			goto emsgsize;
	}

	/* Fiery death and destruction unless we prefetched EOS. */
	if (ch != '\0')
		goto enoent;

	/* Prefix length can default to /32 only if all four octets spec'd. */
	if (bits == -1)
	{
		if (dst - odst == 4)
			bits = 32;
		else
			goto enoent;
	}

	/* If nothing was written to the destination, we found no address. */
	if (dst == odst)
		goto enoent;

	/* If prefix length overspecifies mantissa, life is bad. */
	if ((bits / 8) > (dst - odst))
		goto enoent;

	/* Extend address to four octets. */
	while (size-- > 0)
		*dst++ = 0;

	return bits;

enoent:
	errno = ENOENT;
	return -1;

emsgsize:
	errno = EMSGSIZE;
	return -1;
}

static int
getbits(const char *src, int *bitsp)
{
	static const char digits[] = "0123456789";
	int			n;
	int			val;
	char		ch;

	val = 0;
	n = 0;
	while ((ch = *src++) != '\0')
	{
		const char *pch;

		pch = strchr(digits, ch);
		if (pch != NULL)
		{
			if (n++ != 0 && val == 0)	/* no leading zeros */
				return 0;
			val *= 10;
			val += (pch - digits);
			if (val > 128)		/* range */
				return 0;
			continue;
		}
		return 0;
	}
	if (n == 0)
		return 0;
	*bitsp = val;
	return 1;
}

static int
getv4(const char *src, u_char *dst, int *bitsp)
{
	static const char digits[] = "0123456789";
	u_char	   *odst = dst;
	int			n;
	u_int		val;
	char		ch;

	val = 0;
	n = 0;
	while ((ch = *src++) != '\0')
	{
		const char *pch;

		pch = strchr(digits, ch);
		if (pch != NULL)
		{
			if (n++ != 0 && val == 0)	/* no leading zeros */
				return 0;
			val *= 10;
			val += (pch - digits);
			if (val > 255)		/* range */
				return 0;
			continue;
		}
		if (ch == '.' || ch == '/')
		{
			if (dst - odst > 3) /* too many octets? */
				return 0;
			*dst++ = val;
			if (ch == '/')
				return getbits(src, bitsp);
			val = 0;
			n = 0;
			continue;
		}
		return 0;
	}
	if (n == 0)
		return 0;
	if (dst - odst > 3)			/* too many octets? */
		return 0;
	*dst++ = val;
	return 1;
}

static int
inet_net_pton_ipv6(const char *src, u_char *dst)
{
	return inet_cidr_pton_ipv6(src, dst, 16);
}

#define NS_IN6ADDRSZ 16
#define NS_INT16SZ 2
#define NS_INADDRSZ 4

static int
inet_cidr_pton_ipv6(const char *src, u_char *dst, size_t size)
{
	static const char xdigits_l[] = "0123456789abcdef",
				xdigits_u[] = "0123456789ABCDEF";
	u_char		tmp[NS_IN6ADDRSZ],
			   *tp,
			   *endp,
			   *colonp;
	const char *xdigits,
			   *curtok;
	int			ch,
				saw_xdigit;
	u_int		val;
	int			digits;
	int			bits;

	if (size < NS_IN6ADDRSZ)
		goto emsgsize;

	memset((tp = tmp), '\0', NS_IN6ADDRSZ);
	endp = tp + NS_IN6ADDRSZ;
	colonp = NULL;
	/* Leading :: requires some special handling. */
	if (*src == ':')
		if (*++src != ':')
			goto enoent;
	curtok = src;
	saw_xdigit = 0;
	val = 0;
	digits = 0;
	bits = -1;
	while ((ch = *src++) != '\0')
	{
		const char *pch;

		if ((pch = strchr((xdigits = xdigits_l), ch)) == NULL)
			pch = strchr((xdigits = xdigits_u), ch);
		if (pch != NULL)
		{
			val <<= 4;
			val |= (pch - xdigits);
			if (++digits > 4)
				goto enoent;
			saw_xdigit = 1;
			continue;
		}
		if (ch == ':')
		{
			curtok = src;
			if (!saw_xdigit)
			{
				if (colonp)
					goto enoent;
				colonp = tp;
				continue;
			}
			else if (*src == '\0')
				goto enoent;
			if (tp + NS_INT16SZ > endp)
				goto enoent;
			*tp++ = (u_char) (val >> 8) & 0xff;
			*tp++ = (u_char) val & 0xff;
			saw_xdigit = 0;
			digits = 0;
			val = 0;
			continue;
		}
		if (ch == '.' && ((tp + NS_INADDRSZ) <= endp) &&
			getv4(curtok, tp, &bits) > 0)
		{
			tp += NS_INADDRSZ;
			saw_xdigit = 0;
			break;				/* '\0' was seen by inet_pton4(). */
		}
		if (ch == '/' && getbits(src, &bits) > 0)
			break;
		goto enoent;
	}
	if (saw_xdigit)
	{
		if (tp + NS_INT16SZ > endp)
			goto enoent;
		*tp++ = (u_char) (val >> 8) & 0xff;
		*tp++ = (u_char) val & 0xff;
	}
	if (bits == -1)
		bits = 128;

	endp = tmp + 16;

	if (colonp != NULL)
	{
		/*
		 * Since some memmove()'s erroneously fail to handle overlapping
		 * regions, we'll do the shift by hand.
		 */
		const int	n = tp - colonp;
		int			i;

		if (tp == endp)
			goto enoent;
		for (i = 1; i <= n; i++)
		{
			endp[-i] = colonp[n - i];
			colonp[n - i] = 0;
		}
		tp = endp;
	}
	if (tp != endp)
		goto enoent;

	/*
	 * Copy out the result.
	 */
	memcpy(dst, tmp, NS_IN6ADDRSZ);

	return bits;

enoent:
	errno = ENOENT;
	return -1;

emsgsize:
	errno = EMSGSIZE;
	return -1;
}

/* ============== SECTION 2: src/port/inet_net_ntop.c (VERBATIM) ============ */

#ifdef SPRINTF_CHAR
#define SPRINTF(x) strlen(sprintf/**/x)
#else
#define SPRINTF(x) ((size_t)sprintf x)
#endif

static char *inet_net_ntop_ipv4(const u_char *src, int bits,
								char *dst, size_t size);
static char *inet_net_ntop_ipv6(const u_char *src, int bits,
								char *dst, size_t size);

char *
pg_inet_net_ntop(int af, const void *src, int bits, char *dst, size_t size)
{
	/*
	 * We need to cover both the address family constants used by the PG inet
	 * type (PGSQL_AF_INET and PGSQL_AF_INET6) and those used by the system
	 * libraries (AF_INET and AF_INET6).  We can safely assume PGSQL_AF_INET
	 * == AF_INET, but the INET6 constants are very likely to be different.
	 */
	switch (af)
	{
		case PGSQL_AF_INET:
			return (inet_net_ntop_ipv4(src, bits, dst, size));
		case PGSQL_AF_INET6:
			return (inet_net_ntop_ipv6(src, bits, dst, size));
		default:
			errno = EAFNOSUPPORT;
			return (NULL);
	}
}

static char *
inet_net_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)
{
	char	   *odst = dst;
	char	   *t;
	int			len = 4;
	int			b;

	if (bits < 0 || bits > 32)
	{
		errno = EINVAL;
		return (NULL);
	}

	/* Always format all four octets, regardless of mask length. */
	for (b = len; b > 0; b--)
	{
		if (size <= sizeof ".255")
			goto emsgsize;
		t = dst;
		if (dst != odst)
			*dst++ = '.';
		dst += SPRINTF((dst, "%u", *src++));
		size -= (size_t) (dst - t);
	}

	/* don't print masklen if 32 bits */
	if (bits != 32)
	{
		if (size <= sizeof "/32")
			goto emsgsize;
		dst += SPRINTF((dst, "/%u", bits));
	}

	return (odst);

emsgsize:
	errno = EMSGSIZE;
	return (NULL);
}

static int
decoct(const u_char *src, int bytes, char *dst, size_t size)
{
	char	   *odst = dst;
	char	   *t;
	int			b;

	for (b = 1; b <= bytes; b++)
	{
		if (size <= sizeof "255.")
			return (0);
		t = dst;
		dst += SPRINTF((dst, "%u", *src++));
		if (b != bytes)
		{
			*dst++ = '.';
			*dst = '\0';
		}
		size -= (size_t) (dst - t);
	}
	return (dst - odst);
}

static char *
inet_net_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
{
	/*
	 * Note that int32_t and int16_t need only be "at least" large enough to
	 * contain a value of the specified size.  On some systems, like Crays,
	 * there is no such thing as an integer variable with 16 bits. Keep this
	 * in mind if you think this function should have been coded to use
	 * pointer overlays.  All the world's not a VAX.
	 */
	char		tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
	char	   *tp;
	struct
	{
		int			base,
					len;
	}			best, cur;
	u_int		words[NS_IN6ADDRSZ / NS_INT16SZ];
	int			i;

	if ((bits < -1) || (bits > 128))
	{
		errno = EINVAL;
		return (NULL);
	}

	/*
	 * Preprocess: Copy the input (bytewise) array into a wordwise array. Find
	 * the longest run of 0x00's in src[] for :: shorthanding.
	 */
	memset(words, '\0', sizeof words);
	for (i = 0; i < NS_IN6ADDRSZ; i++)
		words[i / 2] |= (src[i] << ((1 - (i % 2)) << 3));
	best.base = -1;
	cur.base = -1;
	best.len = 0;
	cur.len = 0;
	for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
	{
		if (words[i] == 0)
		{
			if (cur.base == -1)
				cur.base = i, cur.len = 1;
			else
				cur.len++;
		}
		else
		{
			if (cur.base != -1)
			{
				if (best.base == -1 || cur.len > best.len)
					best = cur;
				cur.base = -1;
			}
		}
	}
	if (cur.base != -1)
	{
		if (best.base == -1 || cur.len > best.len)
			best = cur;
	}
	if (best.base != -1 && best.len < 2)
		best.base = -1;

	/*
	 * Format the result.
	 */
	tp = tmp;
	for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
	{
		/* Are we inside the best run of 0x00's? */
		if (best.base != -1 && i >= best.base &&
			i < (best.base + best.len))
		{
			if (i == best.base)
				*tp++ = ':';
			continue;
		}
		/* Are we following an initial run of 0x00s or any real hex? */
		if (i != 0)
			*tp++ = ':';
		/* Is this address an encapsulated IPv4? */
		if (i == 6 && best.base == 0 && (best.len == 6 ||
										 (best.len == 7 && words[7] != 0x0001) ||
										 (best.len == 5 && words[5] == 0xffff)))
		{
			int			n;

			n = decoct(src + 12, 4, tp, sizeof tmp - (tp - tmp));
			if (n == 0)
			{
				errno = EMSGSIZE;
				return (NULL);
			}
			tp += strlen(tp);
			break;
		}
		tp += SPRINTF((tp, "%x", words[i]));
	}

	/* Was it a trailing run of 0x00's? */
	if (best.base != -1 && (best.base + best.len) ==
		(NS_IN6ADDRSZ / NS_INT16SZ))
		*tp++ = ':';
	*tp = '\0';

	if (bits != -1 && bits != 128)
		tp += SPRINTF((tp, "/%u", bits));

	/*
	 * Check for overflow, copy, and we're done.
	 */
	if ((size_t) (tp - tmp) > size)
	{
		errno = EMSGSIZE;
		return (NULL);
	}
	strcpy(dst, tmp);
	return (dst);
}

/* ========= SECTION 3: src/backend/utils/adt/inet_cidr_ntop.c (VERBATIM) === */

static char *inet_cidr_ntop_ipv4(const u_char *src, int bits,
								 char *dst, size_t size);
static char *inet_cidr_ntop_ipv6(const u_char *src, int bits,
								 char *dst, size_t size);

char *
pg_inet_cidr_ntop(int af, const void *src, int bits, char *dst, size_t size)
{
	switch (af)
	{
		case PGSQL_AF_INET:
			return inet_cidr_ntop_ipv4(src, bits, dst, size);
		case PGSQL_AF_INET6:
			return inet_cidr_ntop_ipv6(src, bits, dst, size);
		default:
			errno = EAFNOSUPPORT;
			return NULL;
	}
}

static char *
inet_cidr_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)
{
	char	   *odst = dst;
	char	   *t;
	u_int		m;
	int			b;

	if (bits < 0 || bits > 32)
	{
		errno = EINVAL;
		return NULL;
	}

	if (bits == 0)
	{
		if (size < sizeof "0")
			goto emsgsize;
		*dst++ = '0';
		size--;
		*dst = '\0';
	}

	/* Format whole octets. */
	for (b = bits / 8; b > 0; b--)
	{
		if (size <= sizeof "255.")
			goto emsgsize;
		t = dst;
		dst += SPRINTF((dst, "%u", *src++));
		if (b > 1)
		{
			*dst++ = '.';
			*dst = '\0';
		}
		size -= (size_t) (dst - t);
	}

	/* Format partial octet. */
	b = bits % 8;
	if (b > 0)
	{
		if (size <= sizeof ".255")
			goto emsgsize;
		t = dst;
		if (dst != odst)
			*dst++ = '.';
		m = ((1 << b) - 1) << (8 - b);
		dst += SPRINTF((dst, "%u", *src & m));
		size -= (size_t) (dst - t);
	}

	/* Format CIDR /width. */
	if (size <= sizeof "/32")
		goto emsgsize;
	dst += SPRINTF((dst, "/%u", bits));
	return odst;

emsgsize:
	errno = EMSGSIZE;
	return NULL;
}

static char *
inet_cidr_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
{
	u_int		m;
	int			b;
	int			p;
	int			zero_s,
				zero_l,
				tmp_zero_s,
				tmp_zero_l;
	int			i;
	int			is_ipv4 = 0;
	unsigned char inbuf[16];
	char		outbuf[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
	char	   *cp;
	int			words;
	u_char	   *s;

	if (bits < 0 || bits > 128)
	{
		errno = EINVAL;
		return NULL;
	}

	cp = outbuf;

	if (bits == 0)
	{
		*cp++ = ':';
		*cp++ = ':';
		*cp = '\0';
	}
	else
	{
		/* Copy src to private buffer.  Zero host part. */
		p = (bits + 7) / 8;
		memcpy(inbuf, src, p);
		memset(inbuf + p, 0, 16 - p);
		b = bits % 8;
		if (b != 0)
		{
			m = ((u_int) ~0) << (8 - b);
			inbuf[p - 1] &= m;
		}

		s = inbuf;

		/* how many words need to be displayed in output */
		words = (bits + 15) / 16;
		if (words == 1)
			words = 2;

		/* Find the longest substring of zero's */
		zero_s = zero_l = tmp_zero_s = tmp_zero_l = 0;
		for (i = 0; i < (words * 2); i += 2)
		{
			if ((s[i] | s[i + 1]) == 0)
			{
				if (tmp_zero_l == 0)
					tmp_zero_s = i / 2;
				tmp_zero_l++;
			}
			else
			{
				if (tmp_zero_l && zero_l < tmp_zero_l)
				{
					zero_s = tmp_zero_s;
					zero_l = tmp_zero_l;
					tmp_zero_l = 0;
				}
			}
		}

		if (tmp_zero_l && zero_l < tmp_zero_l)
		{
			zero_s = tmp_zero_s;
			zero_l = tmp_zero_l;
		}

		if (zero_l != words && zero_s == 0 && ((zero_l == 6) ||
											   ((zero_l == 5 && s[10] == 0xff && s[11] == 0xff) ||
												((zero_l == 7 && s[14] != 0 && s[15] != 1)))))
			is_ipv4 = 1;

		/* Format whole words. */
		for (p = 0; p < words; p++)
		{
			if (zero_l != 0 && p >= zero_s && p < zero_s + zero_l)
			{
				/* Time to skip some zeros */
				if (p == zero_s)
					*cp++ = ':';
				if (p == words - 1)
					*cp++ = ':';
				s++;
				s++;
				continue;
			}

			if (is_ipv4 && p > 5)
			{
				*cp++ = (p == 6) ? ':' : '.';
				cp += SPRINTF((cp, "%u", *s++));
				/* we can potentially drop the last octet */
				if (p != 7 || bits > 120)
				{
					*cp++ = '.';
					cp += SPRINTF((cp, "%u", *s++));
				}
			}
			else
			{
				if (cp != outbuf)
					*cp++ = ':';
				cp += SPRINTF((cp, "%x", *s * 256 + s[1]));
				s += 2;
			}
		}
	}
	/* Format CIDR /width. */
	(void) SPRINTF((cp, "/%u", bits));
	if (strlen(outbuf) + 1 > size)
		goto emsgsize;
	strcpy(dst, outbuf);

	return dst;

emsgsize:
	errno = EMSGSIZE;
	return NULL;
}

/* ====== SECTION 4: src/backend/utils/adt/network.c free functions ======== */

static bool addressOK(unsigned char *a, int bits, int family);
int			bitncmp(const unsigned char *l, const unsigned char *r, int n);
int			bitncommon(const unsigned char *l, const unsigned char *r, int n);
inet	   *cidr_set_masklen_internal(const inet *src, int bits);

/*
 * Common INET/CIDR input routine
 */
static inet *
network_in(char *src, bool is_cidr, Node *escontext)
{
	int			bits;
	inet	   *dst;

	dst = (inet *) palloc0(sizeof(inet));

	/*
	 * First, check to see if this is an IPv6 or IPv4 address.  IPv6 addresses
	 * will have a : somewhere in them (several, in fact) so if there is one
	 * present, assume it's V6, otherwise assume it's V4.
	 */

	if (strchr(src, ':') != NULL)
		ip_family(dst) = PGSQL_AF_INET6;
	else
		ip_family(dst) = PGSQL_AF_INET;

	bits = pg_inet_net_pton(ip_family(dst), src, ip_addr(dst),
							is_cidr ? ip_addrsize(dst) : -1);
	if ((bits < 0) || (bits > ip_maxbits(dst)))
		ereturn(escontext, NULL,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		/* translator: first %s is inet or cidr */
				 errmsg("invalid input syntax for type %s: \"%s\"",
						is_cidr ? "cidr" : "inet", src)));

	/*
	 * Error check: CIDR values must not have any bits set beyond the masklen.
	 */
	if (is_cidr)
	{
		if (!addressOK(ip_addr(dst), bits, ip_family(dst)))
			ereturn(escontext, NULL,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid cidr value: \"%s\"", src),
					 errdetail("Value has bits set to right of mask.")));
	}

	ip_bits(dst) = bits;
	SET_INET_VARSIZE(dst);

	return dst;
}

/*
 * Common INET/CIDR output routine
 */
static char *
network_out(inet *src, bool is_cidr)
{
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
	char	   *dst;
	int			len;

	dst = pg_inet_net_ntop(ip_family(src), ip_addr(src), ip_bits(src),
						   tmp, sizeof(tmp));
	if (dst == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("could not format inet value: %m")));

	/* For CIDR, add /n if not present */
	if (is_cidr && strchr(tmp, '/') == NULL)
	{
		len = strlen(tmp);
		snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(src));
	}

	return pstrdup(tmp);
}

/*
 * Copy src and set mask length to 'bits' (which must be valid for the family)
 */
inet *
cidr_set_masklen_internal(const inet *src, int bits)
{
	inet	   *dst = (inet *) palloc0(sizeof(inet));

	ip_family(dst) = ip_family(src);
	ip_bits(dst) = bits;

	if (bits > 0)
	{
		Assert(bits <= ip_maxbits(dst));

		/* Clone appropriate bytes of the address, leaving the rest 0 */
		memcpy(ip_addr(dst), ip_addr(src), (bits + 7) / 8);

		/* Clear any unwanted bits in the last partial byte */
		if (bits % 8)
			ip_addr(dst)[bits / 8] &= ~(0xFF >> (bits % 8));
	}

	/* Set varlena header correctly */
	SET_INET_VARSIZE(dst);

	return dst;
}

static int32
network_cmp_internal(inet *a1, inet *a2)
{
	if (ip_family(a1) == ip_family(a2))
	{
		int			order;

		order = bitncmp(ip_addr(a1), ip_addr(a2),
						Min(ip_bits(a1), ip_bits(a2)));
		if (order != 0)
			return order;
		order = ((int) ip_bits(a1)) - ((int) ip_bits(a2));
		if (order != 0)
			return order;
		return bitncmp(ip_addr(a1), ip_addr(a2), ip_maxbits(a1));
	}

	return ip_family(a1) - ip_family(a2);
}

int
bitncmp(const unsigned char *l, const unsigned char *r, int n)
{
	unsigned int lb,
				rb;
	int			x,
				b;

	b = n / 8;
	x = memcmp(l, r, b);
	if (x || (n % 8) == 0)
		return x;

	lb = l[b];
	rb = r[b];
	for (b = n % 8; b > 0; b--)
	{
		if (IS_HIGHBIT_SET(lb) != IS_HIGHBIT_SET(rb))
		{
			if (IS_HIGHBIT_SET(lb))
				return 1;
			return -1;
		}
		lb <<= 1;
		rb <<= 1;
	}
	return 0;
}

int
bitncommon(const unsigned char *l, const unsigned char *r, int n)
{
	int			byte,
				nbits;

	/* number of bits to examine in last byte */
	nbits = n % 8;

	/* check whole bytes */
	for (byte = 0; byte < n / 8; byte++)
	{
		if (l[byte] != r[byte])
		{
			/* at least one bit in the last byte is not common */
			nbits = 7;
			break;
		}
	}

	/* check bits in last partial byte */
	if (nbits != 0)
	{
		/* calculate diff of first non-matching bytes */
		unsigned int diff = l[byte] ^ r[byte];

		/* compare the bits from the most to the least */
		while ((diff >> (8 - nbits)) != 0)
			nbits--;
	}

	return (8 * byte) + nbits;
}

/*
 * Verify a CIDR address is OK (doesn't have bits set past the masklen)
 */
static bool
addressOK(unsigned char *a, int bits, int family)
{
	int			byte;
	int			nbits;
	int			maxbits;
	int			maxbytes;
	unsigned char mask;

	if (family == PGSQL_AF_INET)
	{
		maxbits = 32;
		maxbytes = 4;
	}
	else
	{
		maxbits = 128;
		maxbytes = 16;
	}
	Assert(bits <= maxbits);

	if (bits == maxbits)
		return true;

	byte = bits / 8;

	nbits = bits % 8;
	mask = 0xff;
	if (bits != 0)
		mask >>= nbits;

	while (byte < maxbytes)
	{
		if ((a[byte] & mask) != 0)
			return false;
		mask = 0xff;
		byte++;
	}

	return true;
}

static inet *
internal_inetpl(inet *ip, int64 addend)
{
	inet	   *dst;

	dst = (inet *) palloc0(sizeof(inet));

	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pdst = ip_addr(dst);
		int			carry = 0;

		while (--nb >= 0)
		{
			carry = pip[nb] + (int) (addend & 0xFF) + carry;
			pdst[nb] = (unsigned char) (carry & 0xFF);
			carry >>= 8;

			/*
			 * We have to be careful about right-shifting addend because
			 * right-shift isn't portable for negative values, while simply
			 * dividing by 256 doesn't work (the standard rounding is in the
			 * wrong direction, besides which there may be machines out there
			 * that round the wrong way).  So, explicitly clear the low-order
			 * byte to remove any doubt about the correct result of the
			 * division, and then divide rather than shift.
			 */
			addend &= ~((int64) 0xFF);
			addend /= 0x100;
		}

		/*
		 * At this point we should have addend and carry both zero if original
		 * addend was >= 0, or addend -1 and carry 1 if original addend was <
		 * 0.  Anything else means overflow.
		 */
		if (!((addend == 0 && carry == 0) ||
			  (addend == -1 && carry == 1)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("result is out of range")));
	}

	ip_bits(dst) = ip_bits(ip);
	ip_family(dst) = ip_family(ip);
	SET_INET_VARSIZE(dst);

	return dst;
}

/* ===== SECTION 5: fuzz-facing driver entries (NOT Postgres code) ========== */

/*
 * Convention: caller passes inet values flat as (family, bits, addr[16]);
 * results come back the same way. Return 0 = ok, >0 = errcode class (see
 * header). Text-producing entries return the C string length >= 0, or
 * -(errcode class) on error.
 */

#define PG_NETWORK_ENTRY_GUARD() \
	do { \
		pg_diff_errcode = 0; \
		if (setjmp(pg_network_jmp)) \
			return pg_diff_errcode ? pg_diff_errcode : PG_DIFF_ERR_INTERNAL; \
	} while (0)

#define PG_NETWORK_TEXT_GUARD() \
	do { \
		pg_diff_errcode = 0; \
		if (setjmp(pg_network_jmp)) \
			return pg_diff_errcode ? -pg_diff_errcode : -PG_DIFF_ERR_INTERNAL; \
	} while (0)

static void
pgc_inet_load(inet *dst, unsigned char family, unsigned char bits,
			  const unsigned char *addr)
{
	memset(dst, 0, sizeof(*dst));
	dst->family = family;
	dst->bits = bits;
	memcpy(dst->ipaddr, addr, 16);
}

static void
pgc_inet_store(const inet *src, unsigned char *ofam, unsigned char *obits,
			   unsigned char *oaddr)
{
	*ofam = src->family;
	*obits = src->bits;
	memcpy(oaddr, src->ipaddr, 16);
}

int
pg_diff_inet_in(const char *src, unsigned char *ofam, unsigned char *obits,
				unsigned char *oaddr)
{
	inet	   *r;

	PG_NETWORK_ENTRY_GUARD();
	r = network_in((char *) src, false, NULL);
	pgc_inet_store(r, ofam, obits, oaddr);
	return 0;
}

int
pg_diff_cidr_in(const char *src, unsigned char *ofam, unsigned char *obits,
				unsigned char *oaddr)
{
	inet	   *r;

	PG_NETWORK_ENTRY_GUARD();
	r = network_in((char *) src, true, NULL);
	pgc_inet_store(r, ofam, obits, oaddr);
	return 0;
}

int
pg_diff_inet_out(unsigned char fam, unsigned char bits,
				 const unsigned char *addr, char *out)
{
	inet		s;
	char	   *r;

	pgc_inet_load(&s, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();
	r = network_out(&s, false);
	strcpy(out, r);
	return (int) strlen(r);
}

int
pg_diff_cidr_out(unsigned char fam, unsigned char bits,
				 const unsigned char *addr, char *out)
{
	inet		s;
	char	   *r;

	pgc_inet_load(&s, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();
	r = network_out(&s, true);
	strcpy(out, r);
	return (int) strlen(r);
}

/* network.c inet_abbrev body, fmgr-unwrapped */
int
pg_diff_inet_abbrev(unsigned char fam, unsigned char bits,
					const unsigned char *addr, char *out)
{
	inet		ips;
	inet	   *ip = &ips;
	char	   *dst;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	pgc_inet_load(&ips, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();

	dst = pg_inet_net_ntop(ip_family(ip), ip_addr(ip),
						   ip_bits(ip), tmp, sizeof(tmp));

	if (dst == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("could not format inet value: %m")));

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* network.c cidr_abbrev body, fmgr-unwrapped */
int
pg_diff_cidr_abbrev(unsigned char fam, unsigned char bits,
					const unsigned char *addr, char *out)
{
	inet		ips;
	inet	   *ip = &ips;
	char	   *dst;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	pgc_inet_load(&ips, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();

	dst = pg_inet_cidr_ntop(ip_family(ip), ip_addr(ip),
							ip_bits(ip), tmp, sizeof(tmp));

	if (dst == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("could not format cidr value: %m")));

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* network.c network_host body, fmgr-unwrapped */
int
pg_diff_network_host(unsigned char fam, unsigned char bits,
					 const unsigned char *addr, char *out)
{
	inet		ips;
	inet	   *ip = &ips;
	char	   *ptr;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	pgc_inet_load(&ips, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();

	/* force display of max bits, regardless of masklen... */
	if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
						 tmp, sizeof(tmp)) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("could not format inet value: %m")));

	/* Suppress /n if present (shouldn't happen now) */
	if ((ptr = strchr(tmp, '/')) != NULL)
		*ptr = '\0';

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* network.c network_show body, fmgr-unwrapped */
int
pg_diff_network_show(unsigned char fam, unsigned char bits,
					 const unsigned char *addr, char *out)
{
	inet		ips;
	inet	   *ip = &ips;
	int			len;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	pgc_inet_load(&ips, fam, bits, addr);
	PG_NETWORK_TEXT_GUARD();

	if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
						 tmp, sizeof(tmp)) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("could not format inet value: %m")));

	/* Add /n if not present (which it won't be) */
	if (strchr(tmp, '/') == NULL)
	{
		len = strlen(tmp);
		snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(ip));
	}

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

int
pg_diff_network_cmp(unsigned char fam1, unsigned char bits1,
					const unsigned char *addr1,
					unsigned char fam2, unsigned char bits2,
					const unsigned char *addr2)
{
	inet		a1,
				a2;

	pgc_inet_load(&a1, fam1, bits1, addr1);
	pgc_inet_load(&a2, fam2, bits2, addr2);
	return network_cmp_internal(&a1, &a2);
}

/* network.c inet_set_masklen body, fmgr-unwrapped */
int
pg_diff_inet_set_masklen(unsigned char fam, unsigned char abits,
						 const unsigned char *addr, int bits,
						 unsigned char *ofam, unsigned char *obits,
						 unsigned char *oaddr)
{
	inet		srcs;
	inet	   *src = &srcs;
	inet	   *dst;

	pgc_inet_load(&srcs, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	if (bits == -1)
		bits = ip_maxbits(src);

	if ((bits < 0) || (bits > ip_maxbits(src)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid mask length: %d", bits)));

	/* clone the original data */
	dst = (inet *) palloc(VARSIZE_ANY(src));
	memcpy(dst, src, VARSIZE_ANY(src));

	ip_bits(dst) = bits;

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c cidr_set_masklen body, fmgr-unwrapped */
int
pg_diff_cidr_set_masklen(unsigned char fam, unsigned char abits,
						 const unsigned char *addr, int bits,
						 unsigned char *ofam, unsigned char *obits,
						 unsigned char *oaddr)
{
	inet		srcs;
	inet	   *src = &srcs;

	pgc_inet_load(&srcs, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	if (bits == -1)
		bits = ip_maxbits(src);

	if ((bits < 0) || (bits > ip_maxbits(src)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid mask length: %d", bits)));

	pgc_inet_store(cidr_set_masklen_internal(src, bits), ofam, obits, oaddr);
	return 0;
}

/* network.c inet_to_cidr body, fmgr-unwrapped */
int
pg_diff_inet_to_cidr(unsigned char fam, unsigned char abits,
					 const unsigned char *addr,
					 unsigned char *ofam, unsigned char *obits,
					 unsigned char *oaddr)
{
	inet		srcs;
	inet	   *src = &srcs;
	int			bits;

	pgc_inet_load(&srcs, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	bits = ip_bits(src);

	/* safety check */
	if ((bits < 0) || (bits > ip_maxbits(src)))
		elog(ERROR, "invalid inet bit length: %d", bits);

	pgc_inet_store(cidr_set_masklen_internal(src, bits), ofam, obits, oaddr);
	return 0;
}

/* network.c network_network body, fmgr-unwrapped */
int
pg_diff_network_network(unsigned char fam, unsigned char abits,
						const unsigned char *addr,
						unsigned char *ofam, unsigned char *obits,
						unsigned char *oaddr)
{
	inet		ips;
	inet	   *ip = &ips;
	inet	   *dst;
	int			byte;
	int			bits;
	unsigned char mask;
	unsigned char *a,
			   *b;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	/* make sure any unused bits are zeroed */
	dst = (inet *) palloc0(sizeof(inet));

	bits = ip_bits(ip);
	a = ip_addr(ip);
	b = ip_addr(dst);

	byte = 0;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff << (8 - bits);
			bits = 0;
		}

		b[byte] = a[byte] & mask;
		byte++;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_bits(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c network_netmask body, fmgr-unwrapped */
int
pg_diff_network_netmask(unsigned char fam, unsigned char abits,
						const unsigned char *addr,
						unsigned char *ofam, unsigned char *obits,
						unsigned char *oaddr)
{
	inet		ips;
	inet	   *ip = &ips;
	inet	   *dst;
	int			byte;
	int			bits;
	unsigned char mask;
	unsigned char *b;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	/* make sure any unused bits are zeroed */
	dst = (inet *) palloc0(sizeof(inet));

	bits = ip_bits(ip);
	b = ip_addr(dst);

	byte = 0;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff << (8 - bits);
			bits = 0;
		}

		b[byte] = mask;
		byte++;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_maxbits(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c network_broadcast body, fmgr-unwrapped */
int
pg_diff_network_broadcast(unsigned char fam, unsigned char abits,
						  const unsigned char *addr,
						  unsigned char *ofam, unsigned char *obits,
						  unsigned char *oaddr)
{
	inet		ips;
	inet	   *ip = &ips;
	inet	   *dst;
	int			byte;
	int			bits;
	int			maxbytes;
	unsigned char mask;
	unsigned char *a,
			   *b;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	/* make sure any unused bits are zeroed */
	dst = (inet *) palloc0(sizeof(inet));

	maxbytes = ip_addrsize(ip);
	bits = ip_bits(ip);
	a = ip_addr(ip);
	b = ip_addr(dst);

	for (byte = 0; byte < maxbytes; byte++)
	{
		if (bits >= 8)
		{
			mask = 0x00;
			bits -= 8;
		}
		else if (bits == 0)
			mask = 0xff;
		else
		{
			mask = 0xff >> bits;
			bits = 0;
		}

		b[byte] = a[byte] | mask;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_bits(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c network_hostmask body, fmgr-unwrapped */
int
pg_diff_network_hostmask(unsigned char fam, unsigned char abits,
						 const unsigned char *addr,
						 unsigned char *ofam, unsigned char *obits,
						 unsigned char *oaddr)
{
	inet		ips;
	inet	   *ip = &ips;
	inet	   *dst;
	int			byte;
	int			bits;
	int			maxbytes;
	unsigned char mask;
	unsigned char *b;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	/* make sure any unused bits are zeroed */
	dst = (inet *) palloc0(sizeof(inet));

	maxbytes = ip_addrsize(ip);
	bits = ip_maxbits(ip) - ip_bits(ip);
	b = ip_addr(dst);

	byte = maxbytes - 1;

	while (bits)
	{
		if (bits >= 8)
		{
			mask = 0xff;
			bits -= 8;
		}
		else
		{
			mask = 0xff >> (8 - bits);
			bits = 0;
		}

		b[byte] = mask;
		byte--;
	}

	ip_family(dst) = ip_family(ip);
	ip_bits(dst) = ip_maxbits(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

int
pg_diff_inet_same_family(unsigned char fam1, unsigned char bits1,
						 const unsigned char *addr1,
						 unsigned char fam2, unsigned char bits2,
						 const unsigned char *addr2)
{
	inet		a1,
				a2;

	pgc_inet_load(&a1, fam1, bits1, addr1);
	pgc_inet_load(&a2, fam2, bits2, addr2);
	return ip_family(&a1) == ip_family(&a2);
}

/* network.c inet_merge body, fmgr-unwrapped */
int
pg_diff_inet_merge(unsigned char fam1, unsigned char bits1,
				   const unsigned char *addr1,
				   unsigned char fam2, unsigned char bits2,
				   const unsigned char *addr2,
				   unsigned char *ofam, unsigned char *obits,
				   unsigned char *oaddr)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s,
			   *a2 = &a2s;
	int			commonbits;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	PG_NETWORK_ENTRY_GUARD();

	if (ip_family(a1) != ip_family(a2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot merge addresses from different families")));

	commonbits = bitncommon(ip_addr(a1), ip_addr(a2),
							Min(ip_bits(a1), ip_bits(a2)));

	pgc_inet_store(cidr_set_masklen_internal(a1, commonbits),
				   ofam, obits, oaddr);
	return 0;
}

/* network.c inetnot body, fmgr-unwrapped */
int
pg_diff_inetnot(unsigned char fam, unsigned char abits,
				const unsigned char *addr,
				unsigned char *ofam, unsigned char *obits,
				unsigned char *oaddr)
{
	inet		ips;
	inet	   *ip = &ips;
	inet	   *dst;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();

	dst = (inet *) palloc0(sizeof(inet));

	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = ~pip[nb];
	}
	ip_bits(dst) = ip_bits(ip);

	ip_family(dst) = ip_family(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c inetand body, fmgr-unwrapped */
int
pg_diff_inetand(unsigned char fam1, unsigned char bits1,
				const unsigned char *addr1,
				unsigned char fam2, unsigned char bits2,
				const unsigned char *addr2,
				unsigned char *ofam, unsigned char *obits,
				unsigned char *oaddr)
{
	inet		ips,
				ip2s;
	inet	   *ip = &ips,
			   *ip2 = &ip2s;
	inet	   *dst;

	pgc_inet_load(&ips, fam1, bits1, addr1);
	pgc_inet_load(&ip2s, fam2, bits2, addr2);
	PG_NETWORK_ENTRY_GUARD();

	dst = (inet *) palloc0(sizeof(inet));

	if (ip_family(ip) != ip_family(ip2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot AND inet values of different sizes")));
	else
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = pip[nb] & pip2[nb];
	}
	ip_bits(dst) = Max(ip_bits(ip), ip_bits(ip2));

	ip_family(dst) = ip_family(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

/* network.c inetor body, fmgr-unwrapped */
int
pg_diff_inetor(unsigned char fam1, unsigned char bits1,
			   const unsigned char *addr1,
			   unsigned char fam2, unsigned char bits2,
			   const unsigned char *addr2,
			   unsigned char *ofam, unsigned char *obits,
			   unsigned char *oaddr)
{
	inet		ips,
				ip2s;
	inet	   *ip = &ips,
			   *ip2 = &ip2s;
	inet	   *dst;

	pgc_inet_load(&ips, fam1, bits1, addr1);
	pgc_inet_load(&ip2s, fam2, bits2, addr2);
	PG_NETWORK_ENTRY_GUARD();

	dst = (inet *) palloc0(sizeof(inet));

	if (ip_family(ip) != ip_family(ip2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot OR inet values of different sizes")));
	else
	{
		int			nb = ip_addrsize(ip);
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		unsigned char *pdst = ip_addr(dst);

		while (--nb >= 0)
			pdst[nb] = pip[nb] | pip2[nb];
	}
	ip_bits(dst) = Max(ip_bits(ip), ip_bits(ip2));

	ip_family(dst) = ip_family(ip);
	SET_INET_VARSIZE(dst);

	pgc_inet_store(dst, ofam, obits, oaddr);
	return 0;
}

int
pg_diff_inetpl(unsigned char fam, unsigned char abits,
			   const unsigned char *addr, int64 addend,
			   unsigned char *ofam, unsigned char *obits,
			   unsigned char *oaddr)
{
	inet		ips;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();
	pgc_inet_store(internal_inetpl(&ips, addend), ofam, obits, oaddr);
	return 0;
}

/*
 * network.c inetmi_int8 body, fmgr-unwrapped. The -addend negation is
 * upstream's; compiled with -fwrapv (build.rs) it wraps on INT64_MIN,
 * matching the shipped Rust wrapping_neg.
 */
int
pg_diff_inetmi_int8(unsigned char fam, unsigned char abits,
					const unsigned char *addr, int64 addend,
					unsigned char *ofam, unsigned char *obits,
					unsigned char *oaddr)
{
	inet		ips;

	pgc_inet_load(&ips, fam, abits, addr);
	PG_NETWORK_ENTRY_GUARD();
	pgc_inet_store(internal_inetpl(&ips, -addend), ofam, obits, oaddr);
	return 0;
}

/* network.c inetmi body, fmgr-unwrapped */
int
pg_diff_inetmi(unsigned char fam1, unsigned char bits1,
			   const unsigned char *addr1,
			   unsigned char fam2, unsigned char bits2,
			   const unsigned char *addr2,
			   int64 *ores)
{
	inet		ips,
				ip2s;
	inet	   *ip = &ips,
			   *ip2 = &ip2s;
	int64		res = 0;

	pgc_inet_load(&ips, fam1, bits1, addr1);
	pgc_inet_load(&ip2s, fam2, bits2, addr2);
	PG_NETWORK_ENTRY_GUARD();

	if (ip_family(ip) != ip_family(ip2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot subtract inet values of different sizes")));
	else
	{
		/*
		 * We form the difference using the traditional complement, increment,
		 * and add rule, with the increment part being handled by starting the
		 * carry off at 1.  If you don't think integer arithmetic is done in
		 * two's complement, too bad.
		 */
		int			nb = ip_addrsize(ip);
		int			byte = 0;
		unsigned char *pip = ip_addr(ip);
		unsigned char *pip2 = ip_addr(ip2);
		int			carry = 1;

		while (--nb >= 0)
		{
			int			lobyte;

			carry = pip[nb] + (~pip2[nb] & 0xFF) + carry;
			lobyte = carry & 0xFF;
			if (byte < sizeof(int64))
			{
				res |= ((int64) lobyte) << (byte * 8);
			}
			else
			{
				/*
				 * Input wider than int64: check for overflow.  All bytes to
				 * the left of what will fit should be 0 or 0xFF, depending on
				 * sign of the now-complete result.
				 */
				if ((res < 0) ? (lobyte != 0xFF) : (lobyte != 0))
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("result is out of range")));
			}
			carry >>= 8;
			byte++;
		}

		/*
		 * If input is narrower than int64, overflow is not possible, but we
		 * have to do proper sign extension.
		 */
		if (carry == 0 && byte < sizeof(int64))
			res |= ((uint64) (int64) -1) << (byte * 8);
	}

	*ores = res;
	return 0;
}

/*
 * network.c network_abbrev_convert key computation, fmgr/SortSupport
 * unwrapped (see header: estimation tail dropped; LE 8-byte datum arm).
 */
#define SIZEOF_DATUM 8
#define BITS_PER_BYTE 8
typedef uint64 Datum;
#define pg_bswap32(x) __builtin_bswap32(x)
#define DatumBigEndianToNative(x) __builtin_bswap64(x)

uint64
pg_diff_network_abbrev_convert(unsigned char fam, unsigned char abits,
							   const unsigned char *addr)
{
	inet		auths;
	inet	   *authoritative = &auths;
	Datum		res,
				ipaddr_datum,
				subnet_bitmask,
				network;
	int			subnet_size;

	pgc_inet_load(&auths, fam, abits, addr);

	Assert(ip_family(authoritative) == PGSQL_AF_INET ||
		   ip_family(authoritative) == PGSQL_AF_INET6);

	/*
	 * Get an unsigned integer representation of the IP address by taking its
	 * first 4 or 8 bytes. Always take all 4 bytes of an IPv4 address. Take
	 * the first 8 bytes of an IPv6 address with an 8 byte datum and 4 bytes
	 * otherwise.
	 *
	 * We're consuming an array of unsigned char, so byteswap on little endian
	 * systems (an inet's ipaddr field stores the most significant byte
	 * first).
	 */
	if (ip_family(authoritative) == PGSQL_AF_INET)
	{
		uint32		ipaddr_datum32;

		memcpy(&ipaddr_datum32, ip_addr(authoritative), sizeof(uint32));

		/* Must byteswap on little-endian machines */
		ipaddr_datum = pg_bswap32(ipaddr_datum32);

		/* Initialize result without setting ipfamily bit */
		res = (Datum) 0;
	}
	else
	{
		memcpy(&ipaddr_datum, ip_addr(authoritative), sizeof(Datum));

		/* Must byteswap on little-endian machines */
		ipaddr_datum = DatumBigEndianToNative(ipaddr_datum);

		/* Initialize result with ipfamily (most significant) bit set */
		res = ((Datum) 1) << (SIZEOF_DATUM * BITS_PER_BYTE - 1);
	}

	/*
	 * ipaddr_datum must be "split": high order bits go in "network" component
	 * of abbreviated key (often with zeroed bits at the end due to masking),
	 * while low order bits go in "subnet" component when there is space for
	 * one. This is often accomplished by generating a temp datum subnet
	 * bitmask, which we may reuse later when generating the subnet bits
	 * themselves.  (Note that subnet bits are only used with IPv4 datums on
	 * platforms where datum is 8 bytes.)
	 *
	 * The number of bits in subnet is used to generate a datum subnet
	 * bitmask. For example, with a /24 IPv4 datum there are 8 subnet bits
	 * (since 32 - 24 is 8), so the final subnet bitmask is B'1111 1111'. We
	 * need explicit handling for cases where the ipaddr bits cannot all fit
	 * in a datum, though (otherwise we'd incorrectly mask the network
	 * component with IPv6 values).
	 */
	subnet_size = ip_maxbits(authoritative) - ip_bits(authoritative);
	Assert(subnet_size >= 0);
	/* subnet size must work with prefix ipaddr cases */
	subnet_size %= SIZEOF_DATUM * BITS_PER_BYTE;
	if (ip_bits(authoritative) == 0)
	{
		/* Fit as many ipaddr bits as possible into subnet */
		subnet_bitmask = ((Datum) 0) - 1;
		network = 0;
	}
	else if (ip_bits(authoritative) < SIZEOF_DATUM * BITS_PER_BYTE)
	{
		/* Split ipaddr bits between network and subnet */
		subnet_bitmask = (((Datum) 1) << subnet_size) - 1;
		network = ipaddr_datum & ~subnet_bitmask;
	}
	else
	{
		/* Fit as many ipaddr bits as possible into network */
		subnet_bitmask = 0;
		network = ipaddr_datum;
	}

#if SIZEOF_DATUM == 8
	if (ip_family(authoritative) == PGSQL_AF_INET)
	{
		/*
		 * IPv4 with 8 byte datums: keep all 32 netmasked bits, netmask size,
		 * and most significant 25 subnet bits
		 */
		Datum		netmask_size = (Datum) ip_bits(authoritative);
		Datum		subnet;

		/*
		 * Shift left 31 bits: 6 bits netmask size + 25 subnet bits.
		 *
		 * We don't make any distinction between network bits that are zero
		 * due to masking and "true"/non-masked zero bits.  An abbreviated
		 * comparison that is resolved by comparing a non-masked and non-zero
		 * bit to a masked/zeroed bit is effectively resolved based on
		 * ip_bits(), even though the comparison won't reach the netmask_size
		 * bits.
		 */
		network <<= (6 + 25);

		/* Shift size to make room for subnet bits at the end */
		netmask_size <<= 25;

		/* Extract subnet bits without shifting them */
		subnet = ipaddr_datum & subnet_bitmask;

		/*
		 * If we have more than 25 subnet bits, we can't fit everything. Shift
		 * subnet down to avoid clobbering bits that are only supposed to be
		 * used for netmask_size.
		 *
		 * Discarding the least significant subnet bits like this is correct
		 * because abbreviated comparisons that are resolved at the subnet
		 * level must have had equal netmask_size/ip_bits() values in order to
		 * get that far.
		 */
		if (subnet_size > 25)
			subnet >>= subnet_size - 25;

		/*
		 * Assemble the final abbreviated key without clobbering the ipfamily
		 * bit that must remain a zero.
		 */
		res |= network | netmask_size | subnet;
	}
	else
#endif
	{
		/*
		 * 4 byte datums, or IPv6 with 8 byte datums: Use as many of the
		 * netmasked bits as will fit in final abbreviated key. Avoid
		 * clobbering the ipfamily bit that was set earlier.
		 */
		res |= network >> 1;
	}

	return res;
}

/* ====== SECTION 6: network.c recv/send + comparison family + selfuncs ======
 * (p1-lanen round 2, same provenance: src/backend/utils/adt/network.c @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0. Bodies VERBATIM except the
 * documented shims below.)
 *
 * SHIMS (plumbing only):
 *   - StringInfoData/pq_getmsgbyte/pq_begintypsend/pq_sendbyte/pq_endtypsend:
 *     fixed 64-byte in-struct buffer + cursor. pq_getmsgbyte past the end
 *     raises errcode class 6 (ERRCODE_PROTOCOL_VIOLATION 08P01, matching the
 *     shipped pqformat behavior asserted by ledger row 2496).
 *   - bytea return of network_send = the StringInfoData itself; the driver
 *     entry copies payload bytes out (the shipped Rust side carries the 4B
 *     varlena header; the fc plane checks that header against the spec).
 *   - PG_FUNCTION_ARGS wrappers unwrapped to plain C signatures as in
 *     SECTION 5; PG_RETURN_BOOL/INT32 -> return int.
 *   - convert_network_to_scalar: only the INETOID/CIDROID arm is vendored
 *     (the switch's MACADDR arms belong to adt/mac, a different crate; the
 *     inet arm body is verbatim). No Datum header: the value arrives as the
 *     flat triple like every other entry.
 *   - network_scan_first/network_scan_last are DirectFunctionCall
 *     compositions in C; composed here from the SECTION 5 entries
 *     (network_network, network_broadcast, inet_set_masklen(-1)) exactly as
 *     the C calls chain them.
 */

#define PG_DIFF_ERR_PROTOCOL 6

typedef struct StringInfoData
{
	unsigned char data[64];
	int			len;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

static int
pq_getmsgbyte(StringInfo buf)
{
	if (buf->cursor >= buf->len)
	{
		pg_diff_errcode = PG_DIFF_ERR_PROTOCOL;
		longjmp(pg_network_jmp, 1);
	}
	return buf->data[buf->cursor++];
}

static void
pq_begintypsend(StringInfo buf)
{
	buf->len = 0;
	buf->cursor = 0;
}

static void
pq_sendbyte(StringInfo buf, unsigned char b)
{
	assert(buf->len < (int) sizeof(buf->data));
	buf->data[buf->len++] = b;
}

typedef StringInfoData bytea;	/* shim: see header */

/* C's pq_endtypsend returns the palloc'd data buffer, which outlives the
 * caller's stack frame; the verbatim network_send body builds its
 * StringInfoData as a LOCAL and returns pq_endtypsend(&buf), so this shim
 * must copy out to TLS storage to keep the same lifetime contract. */
static _Thread_local StringInfoData pg_send_out;

static bytea *
pq_endtypsend(StringInfo buf)
{
	pg_send_out = *buf;
	return &pg_send_out;
}

/* --- network.c network_recv (VERBATIM body; StringInfo shim above) ------- */

static inet *
network_recv(StringInfo buf, bool is_cidr)
{
	inet	   *addr;
	char	   *addrptr;
	int			bits;
	int			nb,
				i;

	/* make sure any unused bits in a CIDR value are zeroed */
	addr = (inet *) palloc0(sizeof(inet));

	ip_family(addr) = pq_getmsgbyte(buf);
	if (ip_family(addr) != PGSQL_AF_INET &&
		ip_family(addr) != PGSQL_AF_INET6)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
		/* translator: %s is inet or cidr */
				 errmsg("invalid address family in external \"%s\" value",
						is_cidr ? "cidr" : "inet")));
	bits = pq_getmsgbyte(buf);
	if (bits < 0 || bits > ip_maxbits(addr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
		/* translator: %s is inet or cidr */
				 errmsg("invalid bits in external \"%s\" value",
						is_cidr ? "cidr" : "inet")));
	ip_bits(addr) = bits;
	i = pq_getmsgbyte(buf);		/* ignore is_cidr */
	nb = pq_getmsgbyte(buf);
	if (nb != ip_addrsize(addr))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
		/* translator: %s is inet or cidr */
				 errmsg("invalid length in external \"%s\" value",
						is_cidr ? "cidr" : "inet")));

	addrptr = (char *) ip_addr(addr);
	for (i = 0; i < nb; i++)
		addrptr[i] = pq_getmsgbyte(buf);

	/*
	 * Error check: CIDR values must not have any bits set beyond the masklen.
	 */
	if (is_cidr)
	{
		if (!addressOK(ip_addr(addr), bits, ip_family(addr)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("invalid external \"cidr\" value"),
					 errdetail("Value has bits set to right of mask.")));
	}

	SET_INET_VARSIZE(addr);

	return addr;
}

/* --- network.c network_send (VERBATIM body; pq shims above) -------------- */

static bytea *
network_send(inet *addr, bool is_cidr)
{
	StringInfoData buf;
	char	   *addrptr;
	int			nb,
				i;

	pq_begintypsend(&buf);
	pq_sendbyte(&buf, ip_family(addr));
	pq_sendbyte(&buf, ip_bits(addr));
	pq_sendbyte(&buf, is_cidr);
	nb = ip_addrsize(addr);
	pq_sendbyte(&buf, nb);
	addrptr = (char *) ip_addr(addr);
	for (i = 0; i < nb; i++)
		pq_sendbyte(&buf, addrptr[i]);
	return pq_endtypsend(&buf);
}

/* --- driver entries ------------------------------------------------------ */

int
pg_diff_network_recv(const unsigned char *msg, int msglen, int is_cidr,
					 int *consumed, unsigned char *ofam,
					 unsigned char *obits, unsigned char *oaddr)
{
	StringInfoData buf;
	inet	   *r;

	assert(msglen <= (int) sizeof(buf.data));
	memcpy(buf.data, msg, msglen);
	buf.len = msglen;
	buf.cursor = 0;
	*consumed = 0;
	PG_NETWORK_ENTRY_GUARD();
	r = network_recv(&buf, is_cidr != 0);
	*consumed = buf.cursor;
	pgc_inet_store(r, ofam, obits, oaddr);
	return 0;
}

int
pg_diff_network_send(unsigned char fam, unsigned char bits,
					 const unsigned char *addr, int is_cidr,
					 unsigned char *out)
{
	inet		s;
	bytea	   *r;

	pgc_inet_load(&s, fam, bits, addr);
	PG_NETWORK_ENTRY_GUARD();
	r = network_send(&s, is_cidr != 0);
	memcpy(out, r->data, r->len);
	return r->len;
}

/* network.c network_lt/le/eq/ge/gt/ne bodies, fmgr-unwrapped */
int
pg_diff_network_relop(unsigned char fam1, unsigned char bits1,
					  const unsigned char *addr1,
					  unsigned char fam2, unsigned char bits2,
					  const unsigned char *addr2, int op)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	switch (op)
	{
		case 0:
			return network_cmp_internal(a1, a2) < 0;
		case 1:
			return network_cmp_internal(a1, a2) <= 0;
		case 2:
			return network_cmp_internal(a1, a2) == 0;
		case 3:
			return network_cmp_internal(a1, a2) >= 0;
		case 4:
			return network_cmp_internal(a1, a2) > 0;
		default:
			return network_cmp_internal(a1, a2) != 0;
	}
}

/* network.c network_smaller/network_larger bodies, fmgr-unwrapped: returns
 * 0 if a1 is the winning input datum, 1 if a2 (pointer identity in C). */
int
pg_diff_network_smaller(unsigned char fam1, unsigned char bits1,
						const unsigned char *addr1,
						unsigned char fam2, unsigned char bits2,
						const unsigned char *addr2)
{
	inet		a1s,
				a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (network_cmp_internal(&a1s, &a2s) < 0)
		return 0;
	else
		return 1;
}

int
pg_diff_network_larger(unsigned char fam1, unsigned char bits1,
					   const unsigned char *addr1,
					   unsigned char fam2, unsigned char bits2,
					   const unsigned char *addr2)
{
	inet		a1s,
				a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (network_cmp_internal(&a1s, &a2s) > 0)
		return 0;
	else
		return 1;
}

/* network.c network_sub/subeq/sup/supeq/overlap bodies, fmgr-unwrapped */
int
pg_diff_network_sub(unsigned char fam1, unsigned char bits1,
					const unsigned char *addr1,
					unsigned char fam2, unsigned char bits2,
					const unsigned char *addr2)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (ip_family(a1) == ip_family(a2))
	{
		return ip_bits(a1) > ip_bits(a2) &&
			bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2)) == 0;
	}

	return false;
}

int
pg_diff_network_subeq(unsigned char fam1, unsigned char bits1,
					  const unsigned char *addr1,
					  unsigned char fam2, unsigned char bits2,
					  const unsigned char *addr2)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (ip_family(a1) == ip_family(a2))
	{
		return ip_bits(a1) >= ip_bits(a2) &&
			bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a2)) == 0;
	}

	return false;
}

int
pg_diff_network_sup(unsigned char fam1, unsigned char bits1,
					const unsigned char *addr1,
					unsigned char fam2, unsigned char bits2,
					const unsigned char *addr2)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (ip_family(a1) == ip_family(a2))
	{
		return ip_bits(a1) < ip_bits(a2) &&
			bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1)) == 0;
	}

	return false;
}

int
pg_diff_network_supeq(unsigned char fam1, unsigned char bits1,
					  const unsigned char *addr1,
					  unsigned char fam2, unsigned char bits2,
					  const unsigned char *addr2)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (ip_family(a1) == ip_family(a2))
	{
		return ip_bits(a1) <= ip_bits(a2) &&
			bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1)) == 0;
	}

	return false;
}

int
pg_diff_network_overlap(unsigned char fam1, unsigned char bits1,
						const unsigned char *addr1,
						unsigned char fam2, unsigned char bits2,
						const unsigned char *addr2)
{
	inet		a1s,
				a2s;
	inet	   *a1 = &a1s;
	inet	   *a2 = &a2s;

	pgc_inet_load(&a1s, fam1, bits1, addr1);
	pgc_inet_load(&a2s, fam2, bits2, addr2);
	if (ip_family(a1) == ip_family(a2))
	{
		return bitncmp(ip_addr(a1), ip_addr(a2),
					   Min(ip_bits(a1), ip_bits(a2))) == 0;
	}

	return false;
}

/* network.c network_family body, fmgr-unwrapped */
int
pg_diff_network_family(unsigned char fam, unsigned char bits,
					   const unsigned char *addr)
{
	inet		ips;
	inet	   *ip = &ips;

	pgc_inet_load(&ips, fam, bits, addr);
	switch (ip_family(ip))
	{
		case PGSQL_AF_INET:
			return 4;
		case PGSQL_AF_INET6:
			return 6;
		default:
			return 0;
	}
}

/* network.c network_masklen body, fmgr-unwrapped */
int
pg_diff_network_masklen(unsigned char fam, unsigned char bits,
						const unsigned char *addr)
{
	inet		ips;

	pgc_inet_load(&ips, fam, bits, addr);
	return ip_bits(&ips);
}

/* network.c convert_network_to_scalar, INETOID/CIDROID arm (VERBATIM body;
 * see the section header for the mac-arm carve). */
double
pg_diff_convert_network_to_scalar(unsigned char fam, unsigned char bits,
								  const unsigned char *addr)
{
	inet		ips;
	inet	   *ip = &ips;
	int			len;
	double		res;
	int			i;

	pgc_inet_load(&ips, fam, bits, addr);

	/*
	 * Note that we don't use the full address for IPv6.
	 */
	if (ip_family(ip) == PGSQL_AF_INET)
		len = 4;
	else
		len = 5;

	res = ip_family(ip);
	for (i = 0; i < len; i++)
	{
		res *= 256;
		res += ip_addr(ip)[i];
	}
	return res;
}

/* network.c network_scan_first/network_scan_last: DirectFunctionCall
 * compositions, composed from the SECTION 5 entries exactly as C chains
 * them (network_network; inet_set_masklen(network_broadcast(in), -1)). */
int
pg_diff_network_scan_first(unsigned char fam, unsigned char bits,
						   const unsigned char *addr, unsigned char *ofam,
						   unsigned char *obits, unsigned char *oaddr)
{
	return pg_diff_network_network(fam, bits, addr, ofam, obits, oaddr);
}

int
pg_diff_network_scan_last(unsigned char fam, unsigned char bits,
						  const unsigned char *addr, unsigned char *ofam,
						  unsigned char *obits, unsigned char *oaddr)
{
	unsigned char bfam,
				bbits,
				baddr[16];
	int			st;

	st = pg_diff_network_broadcast(fam, bits, addr, &bfam, &bbits, baddr);
	if (st != 0)
		return st;
	return pg_diff_inet_set_masklen(bfam, bbits, baddr, -1, ofam, obits, oaddr);
}
