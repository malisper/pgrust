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
