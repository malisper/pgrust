/*
 * pg_uuid_io.c: vendored PostgreSQL C oracle for the uuid_diff differential
 * fuzz target (100%-coverage campaign, lane 0B).
 *
 * Provenance:
 *   - Sections 1-2 below are a byte-for-byte copy of proofs/uuid/c/pg_uuid.c
 *     (this repo), itself vendored VERBATIM from
 *     src/backend/utils/adt/uuid.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (the repo's vendored
 *     ground-truth checkout ../pgrust-fabled/vendor/postgres-src, PG 18.3
 *     stamp; re-verified against that checkout 2026-07-30).  Its shims
 *     (isxdigit/strtoul C-locale shims, ereturn -> sentinel, fmgr
 *     unwrapping, wire triple for recv/send, rand8 RNG seam for
 *     generate_uuidv7) are documented in its own header comment.
 *   - Section 3 (hash_bytes/hash_bytes_extended) is a byte-for-byte copy of
 *     the corresponding section of proofs/hash-rows/c/pg_hash_rows.c, itself
 *     vendored VERBATIM from src/common/hashfn.c @ REL_18_STABLE, with ONE
 *     shim added here: `static` on the two definitions (link-collision
 *     guard: sibling oracles in this csrc/ dir may vendor the same
 *     functions).  uuid.c's uuid_hash/uuid_hash_extended are exactly
 *     hash_any(key->data, UUID_LEN) = hash_bytes / hash_any_extended =
 *     hash_bytes_extended (uuid.c lines 493-506).
 *   - Section 4: fuzz-facing driver entry points (NOT Postgres code) +
 *     the uuid_abbrev_convert pure key kernel (memcpy + byteswap lines
 *     verbatim from uuid_abbrev_convert; DatumBigEndianToNative inlined as
 *     its pg_bswap.h definition on little-endian).
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; uuid.c's only
 * errcode is ERRCODE_INVALID_TEXT_REPRESENTATION (22P02) = class 1.
 */

/* ==================== SECTION 1-2: proofs/uuid/c/pg_uuid.c ============== */

/*
 * Vendored PostgreSQL C for Kani dual-execution proofs: uuid family.
 *
 * Provenance:
 *   file:  src/backend/utils/adt/uuid.c
 *   ref:   postgres/postgres master
 *          (https://raw.githubusercontent.com/postgres/postgres/master/
 *           src/backend/utils/adt/uuid.c)
 *   date fetched: 2026-07-28
 *   REL_18_STABLE conformance: zero code drift vs REL_18_STABLE
 *   (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * Functions copied (bodies verbatim except the shims listed below), renamed
 * with a pg_ prefix:
 *   string_to_uuid     -> pg_string_to_uuid
 *   uuid_out (core)    -> pg_uuid_out
 *   uuid_internal_cmp  -> pg_uuid_internal_cmp
 *   uuid_lt/le/eq/ge/gt/ne/cmp -> pg_uuid_lt/... (fmgr wrappers unwrapped)
 *
 * Shims (plumbing only, never logic):
 *   1. UUID_LEN / pg_uuid_t: inlined from src/include/utils/uuid.h
 *      (#define UUID_LEN 16; struct { unsigned char data[UUID_LEN]; }).
 *   2. ereturn(escontext, ...) -> `return 1` error sentinel.
 *      pg_string_to_uuid returns int: 0 = parsed OK, 1 = syntax error
 *      (C's void + escontext error channel flattened to a return code).
 *   3. palloc'd output buffer in uuid_out -> caller-provided char buf[37]
 *      (2*UUID_LEN + 4 hyphens + NUL), fmgr PG_FUNCTION_ARGS unwrapped to a
 *      plain C signature. Body of the loop verbatim.
 *   4. isxdigit((unsigned char) c) -> shim_isxdigit: C-locale/ASCII
 *      semantics ([0-9a-fA-F]). Kani/CBMC has no libc ctype model. This
 *      PINS the proof to the C locale; a server locale whose isxdigit
 *      accepts more characters is outside the proof domain (Postgres's
 *      intent here is hex parsing, and the subsequent strtoul base-16 only
 *      consumes ASCII hex anyway).
 *   5. strtoul(str_buf, NULL, 16) -> shim_strtoul16: base-16 conversion of
 *      the 2-char NUL-terminated str_buf. Called only on isxdigit-validated
 *      input, exactly as in the original; semantics identical on that
 *      domain (no overflow possible: value <= 0xff).
 *   6. bool -> int returns on the comparators (Kani lowers Rust () / bool
 *      FFI shapes poorly against C bool; int is the documented int-shim).
 *   memcpy/memcmp are left as-is: CBMC models them natively.
 *
 * NOTE on pg_uuid_cmp: it returns memcmp()'s raw value, whose magnitude is
 * implementation-defined (only the sign is specified by C). Harnesses
 * compare signum, matching the btree comparator contract.
 */

#include <string.h>

#define UUID_LEN 16

typedef struct pg_uuid_t
{
	unsigned char data[UUID_LEN];
} pg_uuid_t;

/* shim 4: C-locale / ASCII isxdigit */
static int
shim_isxdigit(unsigned char c)
{
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F');
}

/* shim 5: strtoul(str_buf, NULL, 16) on a short hex string */
static unsigned long
shim_strtoul16(const char *s)
{
	unsigned long v = 0;
	int			i;

	for (i = 0; i < 2 && s[i] != '\0'; i++)
	{
		char		c = s[i];

		if (c >= '0' && c <= '9')
			v = v * 16 + (unsigned long) (c - '0');
		else if (c >= 'a' && c <= 'f')
			v = v * 16 + (unsigned long) (c - 'a' + 10);
		else if (c >= 'A' && c <= 'F')
			v = v * 16 + (unsigned long) (c - 'A' + 10);
		else
			break;
	}
	return v;
}

/*
 * We allow UUIDs as a series of 32 hexadecimal digits with an optional dash
 * after each group of 4 hexadecimal digits, and optionally surrounded by {}.
 * (The canonical format 8x-4x-4x-4x-12x, where "nx" means n hexadecimal
 * digits, is the only one used for output.)
 *
 * Verbatim from string_to_uuid except: ereturn -> return 1 (shim 2),
 * isxdigit/strtoul -> shims 4/5.  Returns 0 on success, 1 on syntax error.
 */
int
pg_string_to_uuid(const char *source, pg_uuid_t *uuid)
{
	const char *src = source;
	int			braces = 0;
	int			i;

	if (src[0] == '{')
	{
		src++;
		braces = 1;
	}

	for (i = 0; i < UUID_LEN; i++)
	{
		char		str_buf[3];

		if (src[0] == '\0' || src[1] == '\0')
			goto syntax_error;
		memcpy(str_buf, src, 2);
		if (!shim_isxdigit((unsigned char) str_buf[0]) ||
			!shim_isxdigit((unsigned char) str_buf[1]))
			goto syntax_error;

		str_buf[2] = '\0';
		uuid->data[i] = (unsigned char) shim_strtoul16(str_buf);
		src += 2;
		if (src[0] == '-' && (i % 2) == 1 && i < UUID_LEN - 1)
			src++;
	}

	if (braces)
	{
		if (*src != '}')
			goto syntax_error;
		src++;
	}

	if (*src != '\0')
		goto syntax_error;

	return 0;

syntax_error:
	return 1;					/* shim 2: ereturn -> sentinel */
}

/*
 * uuid_out core: fmgr wrapper unwrapped, palloc -> caller buffer (shim 3).
 * buf must have room for 2 * UUID_LEN + 5 bytes. Loop body verbatim.
 */
int
pg_uuid_out(const pg_uuid_t *uuid, char *buf)
{
	static const char hex_chars[] = "0123456789abcdef";
	char	   *p;
	int			i;

	p = buf;
	for (i = 0; i < UUID_LEN; i++)
	{
		int			hi;
		int			lo;

		/*
		 * We print uuid values as a string of 8, 4, 4, 4, and then 12
		 * hexadecimal characters, with each group is separated by a hyphen
		 * ("-"). Therefore, add the hyphens at the appropriate places here.
		 */
		if (i == 4 || i == 6 || i == 8 || i == 10)
			*p++ = '-';

		hi = uuid->data[i] >> 4;
		lo = uuid->data[i] & 0x0F;

		*p++ = hex_chars[hi];
		*p++ = hex_chars[lo];
	}
	*p = '\0';

	return 0;
}

/* internal uuid compare function (verbatim) */
static int
pg_uuid_internal_cmp_static(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return memcmp(arg1->data, arg2->data, UUID_LEN);
}

int
pg_uuid_internal_cmp(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2);
}

/* fmgr wrappers unwrapped; PG_RETURN_BOOL -> int (shim 6) */

int
pg_uuid_lt(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) < 0;
}

int
pg_uuid_le(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) <= 0;
}

int
pg_uuid_eq(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) == 0;
}

int
pg_uuid_ge(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) >= 0;
}

int
pg_uuid_gt(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) > 0;
}

int
pg_uuid_ne(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2) != 0;
}

/* handler for btree index operator; PG_RETURN_INT32 -> int */
int
pg_uuid_cmp(const pg_uuid_t *arg1, const pg_uuid_t *arg2)
{
	return pg_uuid_internal_cmp_static(arg1, arg2);
}

/*
 * uuid_extract_version / uuid_extract_timestamp (pg_proc oids 6343/6342),
 * vendored from postgres REL_18_STABLE src/backend/utils/adt/uuid.c
 * (fetched 2026-07-28), plus the epoch/unit macros they use (uuid.c lines
 * ~30-43 + datatype/timestamp.h REL_18_STABLE values, inlined verbatim).
 *
 * SHIMS (extraction/conversion expressions verbatim):
 *   - fmgr wrappers removed -> plain signatures over the 16-byte image;
 *     PG_RETURN_NULL() -> return 0 with *isnull = 1 out-param;
 *     PG_RETURN_UINT16 / PG_RETURN_TIMESTAMPTZ -> plain returns with
 *     *isnull = 0.
 *   - typedefs: TimestampTz = int64 (datatype/timestamp.h); uint16/uint64/
 *     int64 already provided above / via the shim header.
 */

#include <stdint.h>

typedef int64_t TimestampTz;

#define INT64CONST(x) (x##LL)
#define US_PER_MS	INT64CONST(1000)
#define SECS_PER_DAY	86400
#define USECS_PER_SEC	INT64CONST(1000000)
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define UNIX_EPOCH_JDATE		2440588 /* == date2j(1970, 1, 1) */
#define GREGORIAN_EPOCH_JDATE  INT64CONST(2299161)	/* == date2j(1582,10,15) */
#define PG_UNIX_EPOCH_OFFSET_US \
	((int64_t) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC)

uint16_t
pg_uuid_extract_version(const unsigned char *data, int *isnull)
{
	pg_uuid_t	uuid_;
	pg_uuid_t  *uuid = &uuid_;
	uint16_t	version;

	memcpy(uuid->data, data, UUID_LEN);
	*isnull = 0;

	/* check if RFC 9562 variant */
	if ((uuid->data[8] & 0xc0) != 0x80)
	{
		*isnull = 1;
		return 0;
	}

	version = uuid->data[6] >> 4;

	return version;
}

TimestampTz
pg_uuid_extract_timestamp(const unsigned char *data, int *isnull)
{
	pg_uuid_t	uuid_;
	pg_uuid_t  *uuid = &uuid_;
	int			version;
	uint64_t	tms;
	TimestampTz ts;

	memcpy(uuid->data, data, UUID_LEN);
	*isnull = 0;

	/* check if RFC 9562 variant */
	if ((uuid->data[8] & 0xc0) != 0x80)
	{
		*isnull = 1;
		return 0;
	}

	version = uuid->data[6] >> 4;

	if (version == 1)
	{
		tms = ((uint64_t) uuid->data[0] << 24)
			+ ((uint64_t) uuid->data[1] << 16)
			+ ((uint64_t) uuid->data[2] << 8)
			+ ((uint64_t) uuid->data[3])
			+ ((uint64_t) uuid->data[4] << 40)
			+ ((uint64_t) uuid->data[5] << 32)
			+ (((uint64_t) uuid->data[6] & 0xf) << 56)
			+ ((uint64_t) uuid->data[7] << 48);

		/* convert 100-ns intervals to us, then adjust */
		ts = (TimestampTz) (tms / 10) -
			((uint64_t) POSTGRES_EPOCH_JDATE - GREGORIAN_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;
		return ts;
	}

	if (version == 7)
	{
		tms = (uuid->data[5])
			+ (((uint64_t) uuid->data[4]) << 8)
			+ (((uint64_t) uuid->data[3]) << 16)
			+ (((uint64_t) uuid->data[2]) << 24)
			+ (((uint64_t) uuid->data[1]) << 32)
			+ (((uint64_t) uuid->data[0]) << 40);

		/* convert ms to us, then adjust */
		ts = (TimestampTz) (tms * US_PER_MS) - PG_UNIX_EPOCH_OFFSET_US;

		return ts;
	}

	/* not a timestamp-containing UUID version */
	*isnull = 1;
	return 0;
}

/* ==================================================================== */
/* WAVE 5 (2026-07-28): uuid_recv (2961), uuid_send (2962), and the      */
/* generate_uuidv7 core (behind uuidv7/uuidv7_interval 6429/6430).       */
/*                                                                       */
/* Provenance (fetched 2026-07-28, REL_18_STABLE):                       */
/*   src/backend/utils/adt/uuid.c   (uuid_recv, uuid_send,               */
/*                                   generate_uuidv7, uuid_set_version,  */
/*                                   SUBMS_* defines)                    */
/*   src/backend/libpq/pqformat.c   (pq_getmsgbytes -> pq_copymsgbytes;  */
/*                                   pq_begintypsend/pq_sendbytes/       */
/*                                   pq_endtypsend)                      */
/*                                                                       */
/* Shims (plumbing only, never logic):                                   */
/*   U1. Wire plumbing exactly as proofs/scalar-misc's W1-W4 (StringInfo */
/*       -> (data,len,cursor) triple; insufficient data -> status 4;     */
/*       send buffer caller-provided; SET_VARSIZE = 4B LE header).       */
/*   U2. generate_uuidv7's pg_strong_random(&data[8], 8) becomes the     */
/*       rand8 PARAMETER (RNG seam: the harness feeds ONE shared         */
/*       symbolic 8-byte block to both sides and quantifies over ALL RNG */
/*       outputs; a skew control must fail).  The ereport-on-RNG-failure */
/*       arm is unreachable under the seam and leaves the proof.         */
/*   U3. CONFIG CHOICE: SUBMS_MINIMAL_STEP_BITS follows the HOST the     */
/*       harness runs on (__APPLE__ -> 10, matching upstream __darwin__  */
/*       and the shipped Rust cfg(target_os = "macos") arm).  The proof  */
/*       covers the host flavor; the 12-bit (Linux production) arm       */
/*       differs ONLY by dropping the data[7] ^= data[8] >> 6 line and   */
/*       is flagged for a linux-host rerun in the module doc.            */
/* ==================================================================== */

#define PG_UUID_OK 0
#define PG_UUID_ERR_PROTOCOL 4	/* insufficient data (08P01) */

static int
pg_uuid_copymsgbytes(const unsigned char *data, int32_t len, int32_t *cursor,
					 void *buf, int32_t datalen)
{
	if (datalen < 0 || datalen > (len - *cursor))
		return PG_UUID_ERR_PROTOCOL;
	memcpy(buf, &data[*cursor], datalen);
	*cursor += datalen;
	return PG_UUID_OK;
}

/* uuid.c uuid_recv: memcpy(uuid->data, pq_getmsgbytes(buffer, UUID_LEN)) */
int
pg_uuid_recv(const unsigned char *data, int32_t len, int32_t *cursor,
			 pg_uuid_t *out)
{
	return pg_uuid_copymsgbytes(data, len, cursor, out->data, UUID_LEN);
}

/* uuid.c uuid_send: pq_begintypsend + pq_sendbytes(uuid->data, UUID_LEN)
 * + pq_endtypsend -> 20-byte image (4B header + 16 payload bytes) */
int32_t
pg_uuid_send(const pg_uuid_t *uuid, unsigned char *out /* [20] */ )
{
	uint32_t	hdr = (uint32_t) 20 << 2;
	int			i;

	for (i = 0; i < UUID_LEN; i++)
		out[4 + i] = uuid->data[i];
	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	return 20;
}

/* ---- generate_uuidv7 (uuid.c, verbatim body under shims U2/U3) ---- */

#if defined(__APPLE__) || defined(_MSC_VER)
#define PG_SUBMS_MINIMAL_STEP_BITS 10	/* [shim U3] upstream __darwin__ arm */
#else
#define PG_SUBMS_MINIMAL_STEP_BITS 12
#endif
#define PG_SUBMS_BITS	12
#define PG_NS_PER_MS	1000000

/* uuid.c uuid_set_version, verbatim */
static void
pg_uuid_set_version(pg_uuid_t *uuid, unsigned char version)
{
	/* set version field, top four bits */
	uuid->data[6] = (uuid->data[6] & 0x0f) | (version << 4);

	/* set variant field, top two bits are 1, 0 */
	uuid->data[8] = (uuid->data[8] & 0x3f) | 0x80;
}

int
pg_generate_uuidv7(uint64_t unix_ts_ms, uint32_t sub_ms,
				   const unsigned char *rand8 /* [shim U2] */ ,
				   pg_uuid_t *out)
{
	pg_uuid_t  *uuid = out;
	uint32_t	increased_clock_precision;
	int			i;

	/* Fill in time part */
	uuid->data[0] = (unsigned char) (unix_ts_ms >> 40);
	uuid->data[1] = (unsigned char) (unix_ts_ms >> 32);
	uuid->data[2] = (unsigned char) (unix_ts_ms >> 24);
	uuid->data[3] = (unsigned char) (unix_ts_ms >> 16);
	uuid->data[4] = (unsigned char) (unix_ts_ms >> 8);
	uuid->data[5] = (unsigned char) unix_ts_ms;

	/*
	 * sub-millisecond timestamp fraction (SUBMS_BITS bits, not
	 * SUBMS_MINIMAL_STEP_BITS)
	 */
	increased_clock_precision = (sub_ms * (1 << PG_SUBMS_BITS)) / PG_NS_PER_MS;

	/* Fill the increased clock precision to "rand_a" bits */
	uuid->data[6] = (unsigned char) (increased_clock_precision >> 8);
	uuid->data[7] = (unsigned char) (increased_clock_precision);

	/* fill everything after the increased clock precision with random bytes */
	for (i = 0; i < 8; i++)		/* [shim U2] pg_strong_random -> rand8 */
		uuid->data[8 + i] = rand8[i];

#if PG_SUBMS_MINIMAL_STEP_BITS == 10
	uuid->data[7] = uuid->data[7] ^ (uuid->data[8] >> 6);
#endif

	pg_uuid_set_version(uuid, 7);
	return 0;
}

/* ============ SECTION 3: proofs/hash-rows/c/pg_hash_rows.c (hashfn.c) ==== */
/* `static` added to the two definitions (shim, see header). */

#include <string.h>

typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef uint32 Oid;
typedef size_t Size;
typedef float float4;
typedef double float8;

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
static uint32
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
static uint64
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

/* ========== SECTION 4: fuzz-facing driver entries (NOT Postgres code) ===== */

/* Shared errcode channel, defined in pg_float_io.c (same conventions). */
extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_ERR_INVALID_TEXT 1	/* 22P02, the only uuid.c errcode */

/*
 * uuid_in oracle: parse `source` (NUL-terminated). Returns 0 and fills
 * out[16] on success; returns 1 (and records errcode class 1, exactly the
 * ereturn(ERRCODE_INVALID_TEXT_REPRESENTATION) at string_to_uuid's
 * syntax_error label) on failure.
 */
int
pg_diff_uuid_in(const char *source, unsigned char *out /* [16] */ )
{
	pg_uuid_t	u;

	pg_diff_errcode = 0;
	if (pg_string_to_uuid(source, &u) != 0)
	{
		pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
		return 1;
	}
	memcpy(out, u.data, UUID_LEN);
	return 0;
}

/* uuid_out oracle: buf must have room for 37 bytes. */
int
pg_diff_uuid_out(const unsigned char *data, char *buf)
{
	pg_uuid_t	u;

	memcpy(u.data, data, UUID_LEN);
	return pg_uuid_out(&u, buf);
}

/*
 * Comparator family oracle: op selects the sibling
 * (0=cmp,1=lt,2=le,3=eq,4=ge,5=gt,6=ne).
 */
int
pg_diff_uuid_cmpop(int op, const unsigned char *a, const unsigned char *b)
{
	pg_uuid_t	ua,
				ub;

	memcpy(ua.data, a, UUID_LEN);
	memcpy(ub.data, b, UUID_LEN);
	switch (op)
	{
		case 0:
			return pg_uuid_cmp(&ua, &ub);
		case 1:
			return pg_uuid_lt(&ua, &ub);
		case 2:
			return pg_uuid_le(&ua, &ub);
		case 3:
			return pg_uuid_eq(&ua, &ub);
		case 4:
			return pg_uuid_ge(&ua, &ub);
		case 5:
			return pg_uuid_gt(&ua, &ub);
		default:
			return pg_uuid_ne(&ua, &ub);
	}
}

/* uuid.c uuid_hash: return hash_any(key->data, UUID_LEN); */
uint32_t
pg_diff_uuid_hash(const unsigned char *data)
{
	return pg_hash_bytes(data, UUID_LEN);
}

/* uuid.c uuid_hash_extended: hash_any_extended(key->data, UUID_LEN, seed) */
uint64_t
pg_diff_uuid_hash_extended(const unsigned char *data, uint64_t seed)
{
	return pg_hash_bytes_extended(data, UUID_LEN, seed);
}

uint16_t
pg_diff_uuid_extract_version(const unsigned char *data, int *isnull)
{
	return pg_uuid_extract_version(data, isnull);
}

int64_t
pg_diff_uuid_extract_timestamp(const unsigned char *data, int *isnull)
{
	return pg_uuid_extract_timestamp(data, isnull);
}

/* wire recv/send (see pg_uuid.c wave-5 shims: status 4 = insufficient data) */
int
pg_diff_uuid_recv(const unsigned char *data, int32_t len, int32_t *cursor,
				  unsigned char *out /* [16] */ )
{
	pg_uuid_t	u;
	int			st = pg_uuid_recv(data, len, cursor, &u);

	if (st == PG_UUID_OK)
		memcpy(out, u.data, UUID_LEN);
	return st;
}

int32_t
pg_diff_uuid_send(const unsigned char *data, unsigned char *out /* [20] */ )
{
	pg_uuid_t	u;

	memcpy(u.data, data, UUID_LEN);
	return pg_uuid_send(&u, out);
}

int
pg_diff_uuid_generate_v7(uint64_t unix_ts_ms, uint32_t sub_ms,
						 const unsigned char *rand8,
						 unsigned char *out /* [16] */ )
{
	pg_uuid_t	u;
	int			st = pg_generate_uuidv7(unix_ts_ms, sub_ms, rand8, &u);

	memcpy(out, u.data, UUID_LEN);
	return st;
}

/*
 * uuid_abbrev_convert pure key kernel (uuid.c lines 388-421): the memcpy and
 * DatumBigEndianToNative lines verbatim; SIZEOF_DATUM == 8;
 * DatumBigEndianToNative = pg_bswap64 on little-endian hosts, identity on
 * big-endian (port/pg_bswap.h).  The HLL-estimation side effect is state
 * (compared on the Rust side by uuid_abbrev_abort invariant checks).
 */
uint64_t
pg_diff_uuid_abbrev_key(const unsigned char *data)
{
	uint64_t	res;

	memcpy(&res, data, sizeof(res));
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	/* DatumBigEndianToNative is identity */
#else
	res = __builtin_bswap64(res);	/* DatumBigEndianToNative */
#endif
	return res;
}
