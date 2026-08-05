/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-28):
 *  - src/port/inet_net_ntop.c: pg_inet_net_ntop, inet_net_ntop_ipv4,
 *    inet_net_ntop_ipv6, decoct — bodies verbatim.
 *  - src/backend/utils/adt/inet_cidr_ntop.c: pg_inet_cidr_ntop,
 *    inet_cidr_ntop_ipv4, inet_cidr_ntop_ipv6 — bodies verbatim.
 *  - src/backend/utils/adt/network.c: network_out (the common inet/cidr
 *    output routine), network_host, network_show, inet_abbrev, cidr_abbrev
 *    — bodies verbatim, fmgr/text plumbing unwrapped (see shims).
 *
 * These are the DERIVED-LENGTH text-formatting rows: per the result-image
 * wall law (proofs/TRIAGE.md) they get concrete SPOT harnesses only, so
 * every loop below runs at concrete trip counts in the proofs.
 *
 * SHIMS (plumbing only, never logic):
 *  - pgc_inet value struct + ip_* macros as in net_ops.c / net_arith.c.
 *  - PG_FUNCTION_ARGS / PG_GETARG_INET_PP unwrapped; PG_RETURN_TEXT_P(
 *    cstring_to_text(tmp)) and pstrdup(tmp) -> copy into a caller buffer,
 *    returning the text LENGTH (the text value payload C builds); -1
 *    encodes the ereport(ERROR, ERRCODE_INVALID_BINARY_REPRESENTATION,
 *    "could not format ...") arm (verdict only; message + %m leave the
 *    proof).
 *  - libc has no Kani/CBMC model (prove-target skill), so:
 *      * sprintf -> pgc_sprintf, a fixed 3-arg (dst, fmt, unsigned value)
 *        emitter for exactly the format strings these files use
 *        ("%u", "%x", "/%u"); C99 sprintf semantics for those formats
 *        (decimal/lower-hex, no padding, NUL-terminated, returns count).
 *        The SPRINTF macro is redefined to route to it; every call site in
 *        the vendored bodies passes exactly one value argument.
 *      * snprintf(dst, size, "/%u", v) -> pgc_sprintf(dst, "/%u", v). The
 *        size clamp is dead at these call sites: tmp is
 *        sizeof("xxxx:...255/128") = 50 and the longest possible output is
 *        49 chars + NUL (documented buffer sizing in network.c itself).
 *      * strlen/strcpy/strchr -> local pgc_ implementations (bounded by the
 *        50-byte tmp buffers; C-standard semantics).
 *      * errno assignments (EINVAL/EMSGSIZE/EAFNOSUPPORT) -> a static
 *        pgc_errno cell; errno VALUES are not part of any claim (the NULL
 *        return is the compared verdict).
 *  - memcpy/memset: extern decls, CBMC's builtin models (net_ops.c
 *    precedent).
 *  - u_char/u_int/size_t typedefs; FRONTEND/includes dropped.
 */

typedef struct {
	unsigned char family;
	unsigned char bits;
	unsigned char addr[16];
} pgc_inet;

#define PGSQL_AF_INET	2
#define PGSQL_AF_INET6	3

#define ip_family(inetptr)	((inetptr)->family)
#define ip_bits(inetptr)	((inetptr)->bits)
#define ip_addr(inetptr)	((inetptr)->addr)
#define ip_maxbits(inetptr) \
	(ip_family(inetptr) == PGSQL_AF_INET ? 32 : 128)

typedef pgc_inet inet;
typedef unsigned char u_char;
typedef unsigned int u_int;
typedef unsigned long size_t;

#define NULL 0
typedef int bool;
#define true 1
#define false 0

void *memcpy(void *dst, const void *src, size_t n);
void *memset(void *s, int c, size_t n);

#define NS_IN6ADDRSZ 16
#define NS_INT16SZ 2

#define EINVAL 22
#define EMSGSIZE 90
#define EAFNOSUPPORT 97
static int pgc_errno;
#define errno pgc_errno

/* ---- libc-model shims (see header) ---- */

static size_t
pgc_strlen(const char *s)
{
	size_t		n = 0;

	while (s[n] != '\0')
		n++;
	return n;
}

static char *
pgc_strcpy(char *dst, const char *src)
{
	size_t		i = 0;

	do
	{
		dst[i] = src[i];
	} while (src[i++] != '\0');
	return dst;
}

static char *
pgc_strchr(const char *s, int c)
{
	for (;; s++)
	{
		if (*s == (char) c)
			return (char *) s;
		if (*s == '\0')
			return 0;
	}
}

#define strlen pgc_strlen
#define strcpy pgc_strcpy
#define strchr pgc_strchr

/*
 * sprintf model for the exact formats used here: "%u", "%x", "/%u" with one
 * unsigned value. Returns the number of characters written (excl. NUL),
 * NUL-terminates, exactly as C99 sprintf does for these formats.
 */
static int
pgc_sprintf(char *dst, const char *fmt, unsigned int v)
{
	int			n = 0;
	int			hex = 0;
	char		tmp[10];
	int			t = 0;

	if (fmt[0] == '/')
	{
		dst[n++] = '/';
		fmt++;
	}
	/* fmt is now "%u" or "%x" */
	hex = (fmt[1] == 'x');
	do
	{
		unsigned int d = hex ? (v & 0xF) : (v % 10u);

		tmp[t++] = (char) (d < 10 ? '0' + d : 'a' + (d - 10));
		v = hex ? (v >> 4) : (v / 10u);
	} while (v != 0);
	while (t > 0)
		dst[n++] = tmp[--t];
	dst[n] = '\0';
	return n;
}

#define SPRINTF(x) ((size_t)pgc_sprintf x)
#define snprintf(d, sz, f, v) pgc_sprintf((d), (f), (v))

/* ================= src/port/inet_net_ntop.c (verbatim bodies) ============ */

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

/* ============ src/backend/utils/adt/inet_cidr_ntop.c (verbatim) ========== */

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

/* ====== network.c output wrappers (verbatim bodies, plumbing shimmed) ==== */

/*
 * Common INET/CIDR output routine [network_out; pstrdup -> copy to caller
 * out buffer, returns text length; ereport -> -1]
 */
int
pg_network_out(inet *src, bool is_cidr, char *out)
{
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
	char	   *dst;
	int			len;

	dst = pg_inet_net_ntop(ip_family(src), ip_addr(src), ip_bits(src),
						   tmp, sizeof(tmp));
	if (dst == NULL)
		return -1;				/* ereport ERRCODE_INVALID_BINARY_REPRESENTATION */

	/* For CIDR, add /n if not present */
	if (is_cidr && strchr(tmp, '/') == NULL)
	{
		len = strlen(tmp);
		snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(src));
	}

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/*
 * network_host [cstring_to_text -> copy to caller out buffer + length;
 * ereport -> -1]
 */
int
pg_network_host(inet *ip, char *out)
{
	char	   *ptr;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	/* force display of max bits, regardless of masklen... */
	if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
						 tmp, sizeof(tmp)) == NULL)
		return -1;				/* ereport ERRCODE_INVALID_BINARY_REPRESENTATION */

	/* Suppress /n if present (shouldn't happen now) */
	if ((ptr = strchr(tmp, '/')) != NULL)
		*ptr = '\0';

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* network_show [same shims as network_host] */
int
pg_network_show(inet *ip, char *out)
{
	int			len;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
						 tmp, sizeof(tmp)) == NULL)
		return -1;				/* ereport ERRCODE_INVALID_BINARY_REPRESENTATION */

	/* Add /n if not present (which it won't be) */
	if (strchr(tmp, '/') == NULL)
	{
		len = strlen(tmp);
		snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(ip));
	}

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* inet_abbrev [same shims] */
int
pg_inet_abbrev(inet *ip, char *out)
{
	char	   *dst;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	dst = pg_inet_net_ntop(ip_family(ip), ip_addr(ip),
						   ip_bits(ip), tmp, sizeof(tmp));

	if (dst == NULL)
		return -1;				/* ereport ERRCODE_INVALID_BINARY_REPRESENTATION */

	strcpy(out, tmp);
	return (int) strlen(tmp);
}

/* cidr_abbrev [same shims] */
int
pg_cidr_abbrev(inet *ip, char *out)
{
	char	   *dst;
	char		tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

	dst = pg_inet_cidr_ntop(ip_family(ip), ip_addr(ip),
							ip_bits(ip), tmp, sizeof(tmp));

	if (dst == NULL)
		return -1;				/* ereport ERRCODE_INVALID_BINARY_REPRESENTATION */

	strcpy(out, tmp);
	return (int) strlen(tmp);
}
