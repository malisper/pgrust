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
