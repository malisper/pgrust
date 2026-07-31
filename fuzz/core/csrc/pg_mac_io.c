/*
 * pg_mac_io.c — vendored PostgreSQL 18.3 oracle for the mac_diff
 * differential fuzz target (adt/mac + adt/mac8, shared target).
 *
 * PROVENANCE: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df),
 * src/backend/utils/adt/mac.c, src/backend/utils/adt/mac8.c,
 * src/common/hashfn.c. Assembled VERBATIM from the already-audited proof
 * shims (zero code drift vs REL_18_STABLE per proofs/PROVENANCE-AUDIT.md):
 *   - proofs/mac/csrc/mac_shim.c        (mac.c bodies + glibc sscanf model)
 *   - proofs/mac8/csrc/mac8_shim.c      (mac8.c bodies)
 *   - proofs/hash-rows/c/pg_hash_rows.c (hashfn.c pg_hash_bytes[_extended]
 *     + hashmacaddr/hashmacaddr8 wrappers)
 * Each section keeps its original shim-inventory header; the only edits made
 * for this assembly are (a) #undef hibits/lobits between the mac and mac8
 * sections (both files define the macros) and (b) the shared typedef/include
 * preamble hoisted here once. Everything else is byte-identical to the proof
 * shims.
 *
 * ORACLE ERROR CONVENTION: no _Thread_local errcode plumbing is needed for
 * this family — every fallible function already returns its ereturn/ereport
 * verdict as an int return code (macaddr_in: 0 ok / 1 = 22P02 / 2 = 22003;
 * macaddr8_in: 1 ok / 0 = 22P02; macaddr8tomacaddr: 0 ok / 1 = 22003).
 * The Rust comparator maps sqlstates onto the same codes.
 */
#include <stdint.h>
#include <stddef.h>
#include <string.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

/* ======================= proofs/mac/csrc/mac_shim.c ====================== */
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

/* both shim files define hibits/lobits; mac8's versions follow */
#undef hibits
#undef lobits

/* ====================== proofs/mac8/csrc/mac8_shim.c ===================== */
/*
 * Vendored from postgres master src/backend/utils/adt/mac8.c
 * (hexlookup table, hex2_to_uchar, macaddr8_in state machine, macaddr8_out,
 * hibits/lobits macros, macaddr8_cmp_internal + the six boolean operator
 * bodies; fetched 2026-07-28). REL_18_STABLE conformance: zero code drift
 * vs REL_18_STABLE (provenance audit, proofs/PROVENANCE-AUDIT.md,
 * 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - macaddr8 struct declared locally (fields a..h, matching inet.h).
 *  - names pgc_-prefixed.
 *  - macaddr8_in: fmgr wrapper removed -> plain C signature
 *    pgc_macaddr8_in(str, out[8]) returning 1 on success; palloc0_object
 *    becomes the caller-provided out buffer; the ereturn(...) at the `fail`
 *    label becomes `return 0` (error VALUE parity only; message text is out
 *    of scope). The a..h locals / count / spacer / goto structure are
 *    verbatim.
 *  - isspace(*ptr) -> pgc_isspace, the C/POSIX-locale space set
 *    {' ','\t','\n','\v','\f','\r'} (avoids depending on CBMC's ctype/locale
 *    model; bytes >= 0x80 are non-space in the C locale). Same judgment call
 *    as proofs/bool-parse. The Rust port (adt_mac::is_c_space) makes the
 *    identical C-locale assumption.
 *  - macaddr8_out: palloc(32) becomes a caller buffer; returns
 *    strlen(result) (always 23). snprintf(result, 32,
 *    "%02x:...x8") replaced by an explicit lowercase hex-table expansion.
 *    For arguments in 0..=255 (always true: fields are unsigned char),
 *    printf's %02x emits exactly two lowercase hex digits, which is exactly
 *    what the table expansion writes. This is the one modeled-not-verbatim
 *    piece (CBMC has no snprintf format model we want to depend on). Same
 *    shim as proofs/mac.
 *  - operator wrappers: PG_GETARG unwrapping -> const unsigned char*
 *    byte-array args; PG_RETURN_BOOL -> int 0/1. The comparison expressions
 *    (< 0, <= 0, == 0, >= 0, > 0, != 0 over macaddr8_cmp_internal) are
 *    verbatim.
 *
 * NOTE on hibits/lobits: kept verbatim, including the C int-promotion
 * subtlety — (addr)->a<<24 promotes to int, so a >= 0x80 shifts into the
 * sign bit and the (unsigned long) cast sign-extends on LP64. The shipped
 * Rust claims plain u32 packing is order-equivalent; these harnesses check
 * that claim against the verbatim macro.
 */

typedef struct pgc_macaddr8
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
	unsigned char g;
	unsigned char h;
} pgc_macaddr8;

#define hibits(addr) \
  ((unsigned long)(((addr)->a<<24) | ((addr)->b<<16) | ((addr)->c<<8) | ((addr)->d)))

#define lobits(addr) \
  ((unsigned long)(((addr)->e<<24) | ((addr)->f<<16) | ((addr)->g<<8) | ((addr)->h)))

static const signed char pgc_hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/* SHIM: C/POSIX-locale isspace (see header comment) */
static int
pgc_isspace(unsigned char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' ||
		c == '\f' || c == '\r';
}

/*
 * hex2_to_uchar - convert 2 hex digits to a byte (unsigned char)
 *
 * Sets *badhex to true if the end of the string is reached ('\0' found), or if
 * either character is not a valid hex digit.
 */
static inline unsigned char
pgc_hex2_to_uchar(const unsigned char *ptr, int *badhex)
{
	unsigned char ret;
	signed char lookup;

	/* Handle the first character */
	if (*ptr > 127)
		goto invalid_input;

	lookup = pgc_hexlookup[*ptr];
	if (lookup < 0)
		goto invalid_input;

	ret = lookup << 4;

	/* Move to the second character */
	ptr++;

	if (*ptr > 127)
		goto invalid_input;

	lookup = pgc_hexlookup[*ptr];
	if (lookup < 0)
		goto invalid_input;

	ret += lookup;

	return ret;

invalid_input:
	*badhex = 1;
	return 0;
}

/*
 * MAC address (EUI-48 and EUI-64) reader. Accepts several common notations.
 * SHIM signature: str must be NUL-terminated; parsed bytes go to out[0..8];
 * returns 1 on success, 0 on the C `fail:` ereturn path.
 */
int
pgc_macaddr8_in(const unsigned char *str, unsigned char *out)
{
	const unsigned char *ptr = str;
	int			badhex = 0;
	unsigned char a = 0,
				b = 0,
				c = 0,
				d = 0,
				e = 0,
				f = 0,
				g = 0,
				h = 0;
	int			count = 0;
	unsigned char spacer = '\0';

	/* skip leading spaces */
	while (*ptr && pgc_isspace(*ptr))
		ptr++;

	/* digits must always come in pairs */
	while (*ptr && *(ptr + 1))
	{
		/*
		 * Attempt to decode each byte, which must be 2 hex digits in a row.
		 * If either digit is not hex, hex2_to_uchar will throw ereport() for
		 * us.  Either 6 or 8 byte MAC addresses are supported.
		 */

		/* Attempt to collect a byte */
		count++;

		switch (count)
		{
			case 1:
				a = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 2:
				b = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 3:
				c = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 4:
				d = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 5:
				e = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 6:
				f = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 7:
				g = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			case 8:
				h = pgc_hex2_to_uchar(ptr, &badhex);
				break;
			default:
				/* must be trailing garbage... */
				goto fail;
		}

		if (badhex)
			goto fail;

		/* Move forward to where the next byte should be */
		ptr += 2;

		/* Check for a spacer, these are valid, anything else is not */
		if (*ptr == ':' || *ptr == '-' || *ptr == '.')
		{
			/* remember the spacer used, if it changes then it isn't valid */
			if (spacer == '\0')
				spacer = *ptr;

			/* Have to use the same spacer throughout */
			else if (spacer != *ptr)
				goto fail;

			/* move past the spacer */
			ptr++;
		}

		/* allow trailing whitespace after if we have 6 or 8 bytes */
		if (count == 6 || count == 8)
		{
			if (pgc_isspace(*ptr))
			{
				while (*++ptr && pgc_isspace(*ptr));

				/* If we found a space and then non-space, it's invalid */
				if (*ptr)
					goto fail;
			}
		}
	}

	/* Convert a 6 byte MAC address to macaddr8 */
	if (count == 6)
	{
		h = f;
		g = e;
		f = d;

		d = 0xFF;
		e = 0xFE;
	}
	else if (count != 8)
		goto fail;

	out[0] = a;
	out[1] = b;
	out[2] = c;
	out[3] = d;
	out[4] = e;
	out[5] = f;
	out[6] = g;
	out[7] = h;

	return 1;

fail:
	return 0;
}

/*
 * MAC8 address (EUI-64) output function. Fixed format.
 * SHIM: result must hold >= 24 bytes; returns strlen(result) (always 23).
 */
int
pgc_macaddr8_out(const unsigned char *b, char *result)
{
	static const char hexdig[] = "0123456789abcdef";
	int			i;

	/* SHIM for: snprintf(result, 32, "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
	 *                    addr->a, addr->b, addr->c, addr->d,
	 *                    addr->e, addr->f, addr->g, addr->h); */
	for (i = 0; i < 8; i++)
	{
		result[i * 3] = hexdig[b[i] >> 4];
		result[i * 3 + 1] = hexdig[b[i] & 0x0f];
		if (i < 7)
			result[i * 3 + 2] = ':';
	}
	result[23] = '\0';
	return 23;
}

/*
 * macaddr8_cmp_internal - comparison function for sorting:
 */
static int
pgc_macaddr8_cmp_internal(pgc_macaddr8 *a1, pgc_macaddr8 *a2)
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

static pgc_macaddr8
pgc_from_bytes(const unsigned char *b)
{
	pgc_macaddr8 m;

	m.a = b[0];
	m.b = b[1];
	m.c = b[2];
	m.d = b[3];
	m.e = b[4];
	m.f = b[5];
	m.g = b[6];
	m.h = b[7];
	return m;
}

int
pgc_macaddr8_cmp(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2);
}

/* Boolean comparison functions (PG_RETURN_BOOL -> int 0/1). */

int
pgc_macaddr8_lt(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) < 0;
}

int
pgc_macaddr8_le(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) <= 0;
}

int
pgc_macaddr8_eq(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) == 0;
}

int
pgc_macaddr8_ge(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) >= 0;
}

int
pgc_macaddr8_gt(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) > 0;
}

int
pgc_macaddr8_ne(const unsigned char *b1, const unsigned char *b2)
{
	pgc_macaddr8 m1 = pgc_from_bytes(b1);
	pgc_macaddr8 m2 = pgc_from_bytes(b2);

	return pgc_macaddr8_cmp_internal(&m1, &m2) != 0;
}

/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/mac8.c
 * (macaddr8_set7bit, macaddrtomacaddr8, macaddr8tomacaddr; fetched
 * 2026-07-28). Note: the functions above were vendored from master; these
 * three are from REL_18_STABLE.
 *
 * SHIMS (bodies verbatim):
 *  - macaddr (EUI-48) struct declared locally (fields a..f, matching
 *    inet.h), as pgc_macaddr6 to avoid clashing with proofs/mac.
 *  - fmgr wrappers removed -> plain signatures over byte arrays;
 *    PG_GETARG_MACADDR8_P / PG_GETARG_MACADDR_P -> locally-built structs
 *    from the input bytes; palloc0(sizeof(..)) + PG_RETURN_* ->
 *    caller-provided out buffer.
 *  - macaddr8tomacaddr's ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_
 *    OUT_OF_RANGE), errmsg(..), errhint(..)) -> `return 1` error flag
 *    (verdict parity; the harness pins the Rust sqlstate to
 *    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, matching this errcode; message and
 *    hint text are out of proof scope). Success -> return 0.
 *  - set7bit/tomacaddr8 return int (constant 0) instead of void: Kani
 *    lowers Rust () as `struct Unit`, which goto-cc rejects against C void.
 *  - names pgc_-prefixed.
 */

typedef struct pgc_macaddr6
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
} pgc_macaddr6;

static void
pgc_mac8_to_bytes(const pgc_macaddr8 *m, unsigned char *out)
{
	out[0] = m->a;
	out[1] = m->b;
	out[2] = m->c;
	out[3] = m->d;
	out[4] = m->e;
	out[5] = m->f;
	out[6] = m->g;
	out[7] = m->h;
}

/*
 * Set 7th bit for modified EUI-64 as used in IPv6.
 */
int
pgc_macaddr8_set7bit(const unsigned char *bin, unsigned char *bout)
{
	pgc_macaddr8 addr_ = pgc_from_bytes(bin);
	pgc_macaddr8 *addr = &addr_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	result->a = addr->a | 0x02;
	result->b = addr->b;
	result->c = addr->c;
	result->d = addr->d;
	result->e = addr->e;
	result->f = addr->f;
	result->g = addr->g;
	result->h = addr->h;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

int
pgc_macaddrtomacaddr8(const unsigned char *b6, unsigned char *bout)
{
	pgc_macaddr6 addr6_;
	pgc_macaddr6 *addr6 = &addr6_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	addr6_.a = b6[0];
	addr6_.b = b6[1];
	addr6_.c = b6[2];
	addr6_.d = b6[3];
	addr6_.e = b6[4];
	addr6_.f = b6[5];

	result->a = addr6->a;
	result->b = addr6->b;
	result->c = addr6->c;
	result->d = 0xFF;
	result->e = 0xFE;
	result->f = addr6->d;
	result->g = addr6->e;
	result->h = addr6->f;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

/* returns 0 on success (6 bytes in bout), 1 on the C ereport(ERROR) path */
int
pgc_macaddr8tomacaddr(const unsigned char *b8, unsigned char *bout)
{
	pgc_macaddr8 addr_ = pgc_from_bytes(b8);
	pgc_macaddr8 *addr = &addr_;
	pgc_macaddr6 result_;
	pgc_macaddr6 *result = &result_;

	if ((addr->d != 0xFF) || (addr->e != 0xFE))
		/* SHIM for: ereport(ERROR,
		 *   (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
		 *    errmsg("macaddr8 data out of range to convert to macaddr"),
		 *    errhint("Only addresses that have FF and FE as values in the "
		 *            "4th and 5th bytes from the left, for example "
		 *            "xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted "
		 *            "from macaddr8 to macaddr."))); */
		return 1;

	result->a = addr->a;
	result->b = addr->b;
	result->c = addr->c;
	result->d = addr->f;
	result->e = addr->g;
	result->f = addr->h;

	bout[0] = result->a;
	bout[1] = result->b;
	bout[2] = result->c;
	bout[3] = result->d;
	bout[4] = result->e;
	bout[5] = result->f;
	return 0;
}

/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/mac8.c
 * (macaddr8_not, macaddr8_and, macaddr8_or, macaddr8_trunc; fetched
 * 2026-07-28).
 *
 * SHIMS (per-field assignment bodies verbatim):
 *  - fmgr wrappers removed -> plain signatures over 8-byte arrays;
 *    PG_GETARG_MACADDR8_P -> locally-built pgc_macaddr8 from input bytes;
 *    palloc0(sizeof(macaddr8)) + PG_RETURN_MACADDR8_P -> caller-provided
 *    out[8] buffer.
 *  - return type int (constant 0) instead of void (Kani Unit-vs-void).
 *  - names pgc_-prefixed.
 */

int
pgc_macaddr8_not(const unsigned char *bin, unsigned char *bout)
{
	pgc_macaddr8 addr_ = pgc_from_bytes(bin);
	pgc_macaddr8 *addr = &addr_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	result->a = ~addr->a;
	result->b = ~addr->b;
	result->c = ~addr->c;
	result->d = ~addr->d;
	result->e = ~addr->e;
	result->f = ~addr->f;
	result->g = ~addr->g;
	result->h = ~addr->h;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr8_and(const unsigned char *b1, const unsigned char *b2,
				 unsigned char *bout)
{
	pgc_macaddr8 addr1_ = pgc_from_bytes(b1);
	pgc_macaddr8 addr2_ = pgc_from_bytes(b2);
	pgc_macaddr8 *addr1 = &addr1_;
	pgc_macaddr8 *addr2 = &addr2_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	result->a = addr1->a & addr2->a;
	result->b = addr1->b & addr2->b;
	result->c = addr1->c & addr2->c;
	result->d = addr1->d & addr2->d;
	result->e = addr1->e & addr2->e;
	result->f = addr1->f & addr2->f;
	result->g = addr1->g & addr2->g;
	result->h = addr1->h & addr2->h;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr8_or(const unsigned char *b1, const unsigned char *b2,
				unsigned char *bout)
{
	pgc_macaddr8 addr1_ = pgc_from_bytes(b1);
	pgc_macaddr8 addr2_ = pgc_from_bytes(b2);
	pgc_macaddr8 *addr1 = &addr1_;
	pgc_macaddr8 *addr2 = &addr2_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	result->a = addr1->a | addr2->a;
	result->b = addr1->b | addr2->b;
	result->c = addr1->c | addr2->c;
	result->d = addr1->d | addr2->d;
	result->e = addr1->e | addr2->e;
	result->f = addr1->f | addr2->f;
	result->g = addr1->g | addr2->g;
	result->h = addr1->h | addr2->h;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

int
pgc_macaddr8_trunc(const unsigned char *bin, unsigned char *bout)
{
	pgc_macaddr8 addr_ = pgc_from_bytes(bin);
	pgc_macaddr8 *addr = &addr_;
	pgc_macaddr8 result_;
	pgc_macaddr8 *result = &result_;

	result->a = addr->a;
	result->b = addr->b;
	result->c = addr->c;
	result->d = 0;
	result->e = 0;
	result->f = 0;
	result->g = 0;
	result->h = 0;

	pgc_mac8_to_bytes(result, bout);
	return 0;
}

#undef hibits
#undef lobits

/* ============ proofs/hash-rows/c/pg_hash_rows.c (hash subset) ============ */
/* port/pg_bitutils.h (verbatim body) */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}
/* ================== src/common/hashfn.c (verbatim, pg_ prefix) ============ */


/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

#define rot(x,k) pg_rotate_left32(x, k)

/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *
 * If four pairs of (a,b,c) inputs are run through mix(), or through
 * mix() in reverse, there are at least 32 bits of the output that
 * are sometimes the same for one pair and different for another pair.
 * This was tested for:
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * This does not achieve avalanche.  There are input bits of (a,b,c)
 * that fail to affect some output bits of (a,b,c), especially of a.  The
 * most thoroughly mixed value is c, but it doesn't really even achieve
 * avalanche in c.
 *
 * This allows some parallelism.  Read-after-writes are good at doubling
 * the number of bits affected, so the goal of mixing pulls in the opposite
 * direction from the goal of parallelism.  I did what I could.  Rotates
 * seem to cost as much as shifts on every machine I could lay my hands on,
 * and rotates are much kinder to the top and bottom bits, so I used rotates.
 *----------
 */
#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

/*----------
 * final -- final mixing of 3 32-bit values (a,b,c) into c
 *
 * Pairs of (a,b,c) values differing in only a few bits will usually
 * produce values of c that look totally different.  This was tested for
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * The use of separate functions for mix() and final() allow for a
 * substantial performance increase since final() does not need to
 * do well in reverse, but is does need to affect all output bits.
 * mix(), on the other hand, does not need to affect all output
 * bits (affecting 32 bits is enough).  The original hash function had
 * a single mixing operation that had to satisfy both sets of requirements
 * and was slower as a result.
 *----------
 */
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}


/*
 * pg_hash_bytes() -- hash a variable-length key into a 32-bit value
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *
 * Returns a uint32 value.  Every bit of the key affects every bit of
 * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
 * About 6*len+35 instructions. The best hash table sizes are powers
 * of 2.  There is no need to do mod a prime (mod is sooo slow!).
 * If you need less than 32 bits, use a bitmask.
 *
 * This procedure must never throw elog(ERROR); the ResourceOwner code
 * relies on this not to fail.
 *
 * Note: we could easily change this function to return a 64-bit hash value
 * by using the final values of both b and c.  b is perhaps a little less
 * well mixed than c, however.
 */
uint32
pg_hash_bytes(const unsigned char *k, int keylen)
{
	uint32		a,
				b,
				c,
				len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const uint32 *ka = (const uint32 *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
			b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
			c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += k[3];
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return c;
}

/*
 * pg_hash_bytes_extended() -- hash into a 64-bit value, using an optional seed
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *		seed	: a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to pg_hash_bytes.
 */
uint64
pg_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
{
	uint32		a,
				b,
				c,
				len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the seed is non-zero, use it to perturb the internal state. */
	if (seed != 0)
	{
		/*
		 * In essence, the seed is treated as part of the data being hashed,
		 * but for simplicity, we pretend that it's padded with four bytes of
		 * zeroes so that the seed constitutes a 12-byte chunk.
		 */
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		mix(a, b, c);
	}

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		const uint32 *ka = (const uint32 *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
			b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
			c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += k[7];
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += k[3];
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
				/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* fall through */
			case 8:
				/* the lowest byte of c is reserved for the length */
				b += ((uint32) k[7] << 24);
				/* fall through */
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ((uint32) k[3] << 24);
				/* fall through */
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
				/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

/*
 * pg_hash_bytes_uint32() -- hash a 32-bit value to a 32-bit value
 *
 * This has the same result as
 *		pg_hash_bytes(&k, sizeof(uint32))
 * but is faster and doesn't force the caller to store k into memory.
 */
uint32
pg_hash_bytes_uint32(uint32 k)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	final(a, b, c);

	/* report the result */
	return c;
}

/*
 * pg_hash_bytes_uint32_extended() -- hash 32-bit value to 64-bit value, with seed
 *
 * Like pg_hash_bytes_uint32, this is a convenience function.
 */
uint64
pg_hash_bytes_uint32_extended(uint32 k, uint64 seed)
{
	uint32		a,
				b,
				c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32) (seed >> 32);
		b += (uint32) seed;
		mix(a, b, c);
	}

	a += k;

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

/* ============ hashfn.h static-inline wrappers (shim 3: raw returns) ======= */

static inline uint32
hash_any(const unsigned char *k, int keylen)
{
	return pg_hash_bytes(k, keylen);
}

static inline uint64
hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	return pg_hash_bytes_extended(k, keylen, seed);
}


/* utils/adt/mac.c (shim 8): key = 6-byte macaddr block */
uint32
pg_hashmacaddr(const uint8 *key)
{
	return hash_any((unsigned char *) key, 6);
}

uint64
pg_hashmacaddrextended(const uint8 *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, 6, seed);
}

/* utils/adt/mac8.c (shim 8): key = 8-byte macaddr8 block */
uint32
pg_hashmacaddr8(const uint8 *key)
{
	return hash_any((unsigned char *) key, 8);
}

uint64
pg_hashmacaddr8extended(const uint8 *key, int64 seed)
{
	return hash_any_extended((unsigned char *) key, 8, seed);
}
