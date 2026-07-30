/*
 * Vendored from postgres master src/backend/utils/adt/mac.c
 * (hibits/lobits macros, macaddr_cmp_internal, macaddr_out; fetched
 * 2026-07-28). REL_18_STABLE conformance: zero code drift vs REL_18_STABLE
 * (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - macaddr struct declared locally (fields a..f, matching inet.h).
 *  - fmgr wrapper of macaddr_out removed; palloc(32) becomes a caller
 *    buffer; returns strlen(result).
 *  - snprintf(result, 32, "%02x:%02x:...") replaced by an explicit
 *    lowercase hex-table expansion. For arguments in 0..=255 (always true:
 *    fields are unsigned char), printf's %02x emits exactly two lowercase
 *    hex digits, which is exactly what the table expansion writes. This is
 *    the one modeled-not-verbatim piece (CBMC has no snprintf format model
 *    we want to depend on).
 *  - names pgc_-prefixed.
 */

typedef struct pgc_macaddr
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
} pgc_macaddr;

#define hibits(addr) \
  ((unsigned long)(((addr)->a<<16)|((addr)->b<<8)|((addr)->c)))

#define lobits(addr) \
  ((unsigned long)(((addr)->d<<16)|((addr)->e<<8)|((addr)->f)))

static int
pgc_macaddr_cmp_internal(pgc_macaddr *a1, pgc_macaddr *a2)
{
	if (hibits(a1) < hibits(a2))
		return -1;
	else if (hibits(a1) > hibits(a2))
		return 1;
	else if (lobits(a1) < lobits(a2))
		return -1;
	else if (lobits(a1) > lobits(a2))
		return 1;
	else
		return 0;
}

int
pgc_macaddr_cmp(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr m1;
	pgc_macaddr m2;

	m1.a = b1[0]; m1.b = b1[1]; m1.c = b1[2];
	m1.d = b1[3]; m1.e = b1[4]; m1.f = b1[5];
	m2.a = b2[0]; m2.b = b2[1]; m2.c = b2[2];
	m2.d = b2[3]; m2.e = b2[4]; m2.f = b2[5];
	return pgc_macaddr_cmp_internal(&m1, &m2);
}

/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/mac.c
 * (macaddr_not, macaddr_and, macaddr_or, macaddr_trunc; fetched 2026-07-28).
 * Note: the functions above were vendored from master; these four are from
 * REL_18_STABLE (bodies are identical byte-op assignments either way).
 *
 * SHIMS (bodies verbatim):
 *  - fmgr wrappers removed -> plain signatures over 6-byte arrays;
 *    PG_GETARG_MACADDR_P -> locally-built pgc_macaddr from the input bytes;
 *    palloc(sizeof(macaddr)) + PG_RETURN_MACADDR_P -> caller-provided
 *    out[6] buffer.
 *  - return type int (constant 0) instead of void: Kani lowers Rust () as
 *    `struct Unit`, which goto-cc rejects against C void.
 *  - names pgc_-prefixed.
 */

static pgc_macaddr
pgc_mac_from_bytes(const unsigned char *b)
{
	pgc_macaddr m;

	m.a = b[0];
	m.b = b[1];
	m.c = b[2];
	m.d = b[3];
	m.e = b[4];
	m.f = b[5];
	return m;
}

static void
pgc_mac_to_bytes(const pgc_macaddr *m, unsigned char *out)
{
	out[0] = m->a;
	out[1] = m->b;
	out[2] = m->c;
	out[3] = m->d;
	out[4] = m->e;
	out[5] = m->f;
}

int
pgc_macaddr_not(const unsigned char *bin, unsigned char *bout)
{
	pgc_macaddr addr_ = pgc_mac_from_bytes(bin);
	pgc_macaddr *addr = &addr_;
	pgc_macaddr result_;
	pgc_macaddr *result = &result_;

	result->a = ~addr->a;
	result->b = ~addr->b;
	result->c = ~addr->c;
	result->d = ~addr->d;
	result->e = ~addr->e;
	result->f = ~addr->f;
	pgc_mac_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr_and(const unsigned char *b1, const unsigned char *b2,
				unsigned char *bout)
{
	pgc_macaddr addr1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr addr2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *addr1 = &addr1_;
	pgc_macaddr *addr2 = &addr2_;
	pgc_macaddr result_;
	pgc_macaddr *result = &result_;

	result->a = addr1->a & addr2->a;
	result->b = addr1->b & addr2->b;
	result->c = addr1->c & addr2->c;
	result->d = addr1->d & addr2->d;
	result->e = addr1->e & addr2->e;
	result->f = addr1->f & addr2->f;
	pgc_mac_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr_or(const unsigned char *b1, const unsigned char *b2,
			   unsigned char *bout)
{
	pgc_macaddr addr1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr addr2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *addr1 = &addr1_;
	pgc_macaddr *addr2 = &addr2_;
	pgc_macaddr result_;
	pgc_macaddr *result = &result_;

	result->a = addr1->a | addr2->a;
	result->b = addr1->b | addr2->b;
	result->c = addr1->c | addr2->c;
	result->d = addr1->d | addr2->d;
	result->e = addr1->e | addr2->e;
	result->f = addr1->f | addr2->f;
	pgc_mac_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr_trunc(const unsigned char *bin, unsigned char *bout)
{
	pgc_macaddr addr_ = pgc_mac_from_bytes(bin);
	pgc_macaddr *addr = &addr_;
	pgc_macaddr result_;
	pgc_macaddr *result = &result_;

	result->a = addr->a;
	result->b = addr->b;
	result->c = addr->c;
	result->d = 0;
	result->e = 0;
	result->f = 0;

	pgc_mac_to_bytes(result, bout);
	return 0;
}

/* result must hold >= 18 bytes; returns strlen(result) (always 17) */
int
pgc_macaddr_out(const unsigned char *b, char *result)
{
	static const char hexdig[] = "0123456789abcdef";
	int			i;

	/* SHIM for: snprintf(result, 32, "%02x:%02x:%02x:%02x:%02x:%02x",
	 *                    addr->a, addr->b, addr->c, addr->d, addr->e, addr->f); */
	for (i = 0; i < 6; i++)
	{
		result[i * 3] = hexdig[b[i] >> 4];
		result[i * 3 + 1] = hexdig[b[i] & 0x0f];
		if (i < 5)
			result[i * 3 + 2] = ':';
	}
	result[17] = '\0';
	return 17;
}

/* =======================================================================
 * macaddr_in (pg_proc oid 436) — REL_18_STABLE mac.c, fetched 2026-07-28.
 *
 * SHIMS for this section (plumbing/libc-seam only):
 *  - fmgr unwrapping: PG_GETARG_CSTRING/escontext/palloc(sizeof(macaddr))
 *    -> pgc_macaddr_in(str, result[6]) returning
 *        0 = accepted (result filled),
 *        1 = ereturn(ERRCODE_INVALID_TEXT_REPRESENTATION,
 *              "invalid input syntax for type macaddr"),
 *        2 = ereturn(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
 *              "invalid octet value in \"macaddr\" value").
 *    The cascade of seven format attempts, the count != 6 tests, the
 *    octet range check and the byte packing are VERBATIM in structure.
 *
 *  - sscanf MODEL (the one modeled-not-verbatim seam, same class as the
 *    strspn/strtoul/snprintf shims in proofs/pg_lsn and the %02x table in
 *    macaddr_out above): libc sscanf has no CBMC model, so the seven
 *    mac.c format strings are executed by pgc_sscanf_mac below, a
 *    directive-by-directive C implementation of C99 fscanf semantics as
 *    glibc implements them, the same semantics the shipped Rust port's
 *    Scanner documents itself against (mac/src/lib.rs:76). Semantics
 *    encoded, per directive:
 *      %x / %2x : skip C-locale whitespace; then, within the field width
 *                 (unlimited for %x, 2 for %2x, sign/prefix chars counting
 *                 toward the width), an optional +/- sign, an optional
 *                 0x/0X prefix, and hex digits, accumulated per strtoul
 *                 base 16; the value is negated for '-' and stored through
 *                 an `unsigned int` conversion into the int target
 *                 (C99 7.19.6.2: %x consumes like strtoul, target type
 *                 unsigned int — the mod-2^32 store is part of the spec'd
 *                 behavior, and is what makes the >8-hex-digit divergence
 *                 below reachable).
 *                 glibc "0x"-pushback quirk kept: "0x" followed by a
 *                 non-hexdigit converts to 0 with the 'x' left unread
 *                 (single-char pushback), matching the shipped Scanner.
 *      literal  : ':' '-' '.' match exactly one identical byte (no
 *                 whitespace skip — C99: a non-whitespace literal
 *                 directive does not skip whitespace).
 *      %1s      : skips whitespace, then assigns iff a non-whitespace
 *                 byte remains ("%1s matches iff there is trailing
 *                 non-whitespace garbage" — mac.c's own comment).
 *    Return value = assigned-directive count, so `count != 6` in the
 *    body reads verbatim (junk assignment makes it 7).
 *    OUT OF MODEL (documented fences): strtoul's ULONG_MAX overflow clamp
 *    (unreachable below the harness length caps: a field would need >=16
 *    significant hex digits, i.e. an input longer than any harness band)
 *    and non-glibc libc variance (the C-side behavior here is
 *    libc-defined; glibc semantics are the ratified reference, the
 *    varbit-rows memcmp-magnitude precedent).
 * ======================================================================= */

static int
pgc_is_space(char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

static int
pgc_is_hexdig(char c)
{
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

static int
pgc_hexval(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	return c - 'A' + 10;
}

/* One %x (width==0: unlimited) / %Nx directive. Returns 1 and stores the
 * conversion into *out (advancing *pp) on a match, 0 on matching failure. */
static int
pgc_scan_hex(const char **pp, int width, int *out)
{
	const char *p = *pp;
	int			max = width ? width : 0x7fffffff;
	int			consumed = 0;
	int			neg = 0;
	int			any = 0;
	unsigned long long acc = 0;

	while (pgc_is_space(*p))
		p++;
	if (consumed < max && (*p == '+' || *p == '-'))
	{
		neg = (*p == '-');
		p++;
		consumed++;
	}
	if (consumed < max && *p == '0')
	{
		const char *save_p = p;
		int			save_consumed = consumed;

		p++;
		consumed++;
		if (consumed < max && (*p == 'x' || *p == 'X'))
		{
			p++;
			consumed++;
			if (consumed >= max || !pgc_is_hexdig(*p))
			{
				/* glibc: '0' stays the conversion result, 'x' unread */
				p--;
				*out = 0;
				*pp = p;
				return 1;
			}
			while (consumed < max && pgc_is_hexdig(*p))
			{
				acc = acc * 16 + pgc_hexval(*p);
				p++;
				consumed++;
			}
			goto convert;
		}
		p = save_p;
		consumed = save_consumed;
	}
	while (consumed < max && pgc_is_hexdig(*p))
	{
		acc = acc * 16 + pgc_hexval(*p);
		p++;
		consumed++;
		any = 1;
	}
	if (!any)
		return 0;
convert:
	{
		/* strtoul value (negated for '-'), stored via unsigned int (%x
		 * target type): both conversions are mod 2^32. */
		unsigned int uv = (unsigned int) acc;

		if (neg)
			uv = 0u - uv;
		*out = (int) uv;
	}
	*pp = p;
	return 1;
}

/* Non-whitespace literal directive: matches exactly one byte. */
static int
pgc_scan_lit(const char **pp, char c)
{
	if (**pp == c)
	{
		(*pp)++;
		return 1;
	}
	return 0;
}

/* %1s: skip whitespace; assigns iff a non-whitespace byte remains. */
static int
pgc_scan_junk(const char **pp)
{
	const char *p = *pp;

	while (pgc_is_space(*p))
		p++;
	if (*p == '\0')
		return 0;
	*pp = p + 1;
	return 1;
}

/* count = sscanf(str, <fmt>, &v[0..5], junk) for the seven mac.c formats:
 *   style 0: "%x<sep>%x<sep>%x<sep>%x<sep>%x<sep>%x%1s"
 *   style 1: "%2x%2x%2x<sep>%2x%2x%2x%1s"
 *   style 2: "%2x%2x<sep>%2x%2x<sep>%2x%2x%1s"   (sep == 0 -> no separator)
 * Returns the assigned count (0..7). */
static int
pgc_sscanf_mac(const char *str, int style, char sep, int v[6])
{
	const char *p = str;
	int			n = 0;
	int			i;

	for (i = 0; i < 6; i++)
	{
		if (style == 0 && i > 0 && !pgc_scan_lit(&p, sep))
			return n;
		if (style == 1 && i == 3 && !pgc_scan_lit(&p, sep))
			return n;
		if (style == 2 && (i == 2 || i == 4) && sep && !pgc_scan_lit(&p, sep))
			return n;
		if (!pgc_scan_hex(&p, style == 0 ? 0 : 2, &v[i]))
			return n;
		n++;
	}
	if (pgc_scan_junk(&p))
		n++;					/* %1s assigned: trailing garbage */
	return n;
}

int
pgc_macaddr_in(const char *str, unsigned char *result)
{
	int			a,
				b,
				c,
				d,
				e,
				f;
	int			v[6];
	int			count;

	/* %1s matches iff there is trailing non-whitespace garbage */

	count = pgc_sscanf_mac(str, 0, ':', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 0, '-', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 1, ':', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 1, '-', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 2, '.', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 2, '-', v);
	if (count != 6)
		count = pgc_sscanf_mac(str, 2, 0, v);
	if (count != 6)
		return 1;				/* ereturn(escontext, ERRCODE_INVALID_TEXT_REPRESENTATION,
								 * "invalid input syntax for type macaddr") */

	a = v[0];
	b = v[1];
	c = v[2];
	d = v[3];
	e = v[4];
	f = v[5];

	if ((a < 0) || (a > 255) || (b < 0) || (b > 255) ||
		(c < 0) || (c > 255) || (d < 0) || (d > 255) ||
		(e < 0) || (e > 255) || (f < 0) || (f > 255))
		return 2;				/* ereturn(escontext, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
								 * "invalid octet value in \"macaddr\" value") */

	result[0] = a;
	result[1] = b;
	result[2] = c;
	result[3] = d;
	result[4] = e;
	result[5] = f;
	return 0;
}

/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/mac.c
 * (macaddr_lt, macaddr_le, macaddr_eq, macaddr_ge, macaddr_gt, macaddr_ne;
 * fetched 2026-07-28).
 *
 * SHIMS (comparison expressions verbatim):
 *  - fmgr wrappers removed -> plain signatures over 6-byte arrays;
 *    PG_GETARG_MACADDR_P -> locally-built pgc_macaddr from the input bytes.
 *  - PG_RETURN_BOOL(x) -> return (int)(x).
 *  - names pgc_-prefixed.
 */

int
pgc_macaddr_lt(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) < 0);
}

int
pgc_macaddr_le(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) <= 0);
}

int
pgc_macaddr_eq(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) == 0);
}

int
pgc_macaddr_ge(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) >= 0);
}

int
pgc_macaddr_gt(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) > 0);
}

int
pgc_macaddr_ne(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr a1_ = pgc_mac_from_bytes(b1);
	pgc_macaddr a2_ = pgc_mac_from_bytes(b2);
	pgc_macaddr *a1 = &a1_;
	pgc_macaddr *a2 = &a2_;

	return (pgc_macaddr_cmp_internal(a1, a2) != 0);
}
