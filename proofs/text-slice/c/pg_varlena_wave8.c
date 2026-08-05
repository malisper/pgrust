/*
 * Vendored PostgreSQL C for the WAVE-8 varlena byte-kernel Kani parity
 * proofs (proofs/text-slice, wave8 module).  Ledger rows hosted here:
 *   bytea_int2 (6370), bytea_int4 (6371), bytea_int8 (6372),
 *   int2_bytea (6367), int4_bytea (6368), int8_bytea (6369),
 *   bytea_reverse (6382), bytea_larger (6393), bytea_smaller (6394),
 *   bytea_bit_count (6163),
 *   to_bin32/64 (6330/6331), to_oct32/64 (6332/6333),
 *   to_hex32/64 (2089/2090),
 *   textin (46), textout (47), unknownin (109), unknownout (110),
 *   byteasend (2413), textsend (2415), unknownsend (2417),
 *   byteaoverlay (749), byteaoverlay_no_len (752).
 *   [hashvarlena/hashbytea (456/772/6413/6414) link ../hash/pg_hashfn.c —
 *   pg_hash_bytes / pg_hash_bytes_extended, REL_18-conformant per
 *   proofs/PROVENANCE-AUDIT.md — nothing hash-related is defined here.]
 *
 * Provenance (postgres/postgres REL_18_STABLE, fetched 2026-07-28):
 *   - src/backend/utils/adt/varlena.c: cstring_to_text,
 *     cstring_to_text_with_len, text_to_cstring, textin, textout,
 *     unknownin, unknownout, byteasend, textsend, unknownsend,
 *     bytea_overlay, byteaoverlay, byteaoverlay_no_len, bytea_bit_count,
 *     bytea_reverse, bytea_larger, bytea_smaller, bytea_int2, bytea_int4,
 *     bytea_int8, int2_bytea, int4_bytea, int8_bytea, convert_to_base,
 *     to_bin32, to_bin64, to_oct32, to_oct64, to_hex32, to_hex64.
 *   - src/backend/utils/adt/int.c: int2send, int4send.
 *   - src/backend/utils/adt/int8.c: int8send.
 *   - src/backend/libpq/pqformat.c: pq_sendtext, pq_begintypsend,
 *     pq_endtypsend (StringInfo plumbing shimmed, see shim W4).
 *   - src/port/pg_bitutils.c: pg_popcount_portable (as pg8_pg_popcount),
 *     pg_number_of_ones; src/include/port/pg_bitutils.h:
 *     pg_popcount64_slow arm (as pg8_popcount64, see shim W6).
 *
 * SHIMS (everything else is verbatim; conventions follow c/pg_text_slice.c):
 *  W1. Names pg8_-prefixed; typedefs inlined (int16/uint16 -> short/
 *      unsigned short, int32/uint32 -> int/unsigned int, int64/uint64 ->
 *      long long/unsigned long long, Size -> size_t, bool -> int).
 *      text/bytea arguments ride as (const unsigned char *data, int len)
 *      payload pairs modeling the post-PG_GETARG_*_PP / inline-image
 *      caller contract; DETOASTING is OUT OF SCOPE (established varlena
 *      pattern).
 *  W2. palloc'd results -> caller-provided out buffers; varlena/cstring
 *      returning functions return the payload byte length written
 *      (cstring results additionally write C's trailing NUL).
 *  W3. ereport -> pg8_errflag = <class> + PG_CERR sentinel return
 *      (models C's longjmp unwind), classes:
 *        1 = ERRCODE_SUBSTRING_ERROR        (22011)
 *        5 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003)
 *      Harnesses compare error verdict + class against the Rust PgError
 *      sqlstate (pg8_take_err()).
 *  W4. pq typsend plumbing: StringInfoData -> a fixed-buffer PgSI
 *      {data,len}; appendBinaryStringInfo -> bounded memcpy at len;
 *      pq_begintypsend's four reserved length-word bytes and
 *      pq_endtypsend's SET_VARSIZE are modeled by starting payload at
 *      data+4 and returning len-4 (the harness compares payload bytes,
 *      exactly what VARDATA of C's result holds). pq_sendint16/32/64 ->
 *      the pg_hton16/32/64 big-endian stores those functions perform.
 *  W5. ENCODING-CONVERSION FENCE (textsend/unknownsend):
 *      pg_server_to_client -> identity (returns its input pointer),
 *      modeling ClientEncoding == ServerEncoding — the no-conversion arm
 *      C itself takes then (mbutils.c pg_server_to_client returns src
 *      unchanged). The Rust harness pins the mbutils_seams
 *      pg_server_to_client seam to the same identity. Conversion proper
 *      is OUT OF SCOPE.
 *  W6. pg_popcount config: the direct-function indirection
 *      (pg_popcount_optimized choose/dispatch) is platform plumbing ->
 *      direct call of pg_popcount_portable; pg_popcount64 -> the
 *      pg_popcount64_slow byte-table arm (the C build arm without
 *      HAVE__BUILTIN_POPCOUNT; same config arm proofs/bitutils proved
 *      against). TYPEALIGN from c.h, verbatim.
 *  W7. bytea_larger/bytea_smaller: C's result is POINTER IDENTITY (arg1
 *      or arg2, no copy); the shim returns the selected index (0/1) and
 *      the harness asserts the shipped wrapper returned the SAME arg's
 *      datum pointer.
 *  W8. bytea_overlay: bytea_substring / bytea_catenate -> the verbatim
 *      shimmed pg_bytea_substring / pg_bytea_catenate ALREADY VENDORED in
 *      c/pg_text_slice.c (link both files); intermediate palloc results
 *      -> fixed scratch buffers. Their PG_CERR propagates; their error
 *      class (main file's pg_errflag) is folded into pg8_errflag so one
 *      take-side flag serves the harness. (Both interior substring calls
 *      are structurally error-free — sp-1 >= 0 and length_not_specified —
 *      so the fold is belt.)
 *  W9. Assert -> no-op (compiled out of production builds).
 * W10. memcmp/memcpy/strlen are CBMC's built-in models.
 * W11. pg_add_s32_overflow -> __builtin_add_overflow, exactly how
 *      src/include/common/int.h defines it under gcc/clang.
 */

#include <stddef.h>
#include <string.h>

#define VARHDRSZ 4
#define BITS_PER_BYTE 8
#define Min(x, y) ((x) < (y) ? (x) : (y))

typedef short int16;
typedef unsigned short uint16;
typedef unsigned int uint32;
typedef long long int64;
typedef unsigned long long uint64;
typedef unsigned char uint8;

/* shim W3: error model */
#define PG_CERR (-2100000000)
#define PG8_E_SUBSTRING 1
#define PG8_E_NUM_RANGE 5

static int pg8_errflag = 0;

/* int return: Kani lowers Rust () as `struct Unit`, which goto-cc rejects
 * against C void (prove-target trap) */
int
pg8_take_err(void)
{
	int			e = pg8_errflag;

	pg8_errflag = 0;
	return e;
}

/* shim W11: src/include/common/int.h (gcc/clang arm, verbatim) */
static inline int
pg8_add_s32_overflow(int a, int b, int *result)
{
	return __builtin_add_overflow(a, b, result);
}

/* ---------------- varlena.c: cstring_to_text family (shim W2) --------- */

/*
 * cstring_to_text_with_len
 *
 * C: palloc(len + VARHDRSZ) + SET_VARSIZE + memcpy(VARDATA, s, len).
 * Shim W2: the palloc'd text becomes the caller's out buffer holding the
 * payload; the function returns the payload length (C's VARSIZE - VARHDRSZ).
 */
static int
pg8_cstring_to_text_with_len(const char *s, int len, unsigned char *out)
{
	memcpy(out, s, len);
	return len;
}

/* C: cstring_to_text(s) = cstring_to_text_with_len(s, strlen(s)) */
static int
pg8_cstring_to_text(const char *s, unsigned char *out)
{
	return pg8_cstring_to_text_with_len(s, strlen(s), out);
}

/*
 * text_to_cstring: palloc(len + 1) + memcpy + NUL. Input rides as the
 * (data,len) payload pair (detoast out of scope, shim W1).
 */
static int
pg8_text_to_cstring(const unsigned char *data, int len, unsigned char *out)
{
	memcpy(out, data, len);
	out[len] = '\0';
	return len;
}

/* C: pstrdup(str) — bytes through the terminating NUL, fresh copy. */
static int
pg8_pstrdup(const char *s, unsigned char *out)
{
	int			len = strlen(s);

	memcpy(out, s, len);
	out[len] = '\0';
	return len;
}

/* ---------------- varlena.c: textin / textout / unknown* ---------------- */

/* textin: PG_RETURN_TEXT_P(cstring_to_text(inputText)) */
int
pg8_textin(const char *inputText, unsigned char *out)
{
	return pg8_cstring_to_text(inputText, out);
}

/* textout: PG_RETURN_CSTRING(TextDatumGetCString(txt)) == text_to_cstring */
int
pg8_textout(const unsigned char *data, int len, unsigned char *out)
{
	return pg8_text_to_cstring(data, len, out);
}

/* unknownin: PG_RETURN_CSTRING(pstrdup(str)) */
int
pg8_unknownin(const char *str, unsigned char *out)
{
	return pg8_pstrdup(str, out);
}

/* unknownout: PG_RETURN_CSTRING(pstrdup(str)) */
int
pg8_unknownout(const char *str, unsigned char *out)
{
	return pg8_pstrdup(str, out);
}

/* ---------------- pqformat.c typsend plumbing (shims W4/W5) ------------- */

typedef struct PgSI
{
	unsigned char *data;
	int			len;
}			PgSI;

/* stringinfo.c appendBinaryStringInfo: enlarge is the caller-sized fixed
 * buffer under shim W4; the memcpy + len bump are the verbatim effect. */
static void
pg8_appendBinaryStringInfo(PgSI *buf, const char *data, int datalen)
{
	memcpy(buf->data + buf->len, data, datalen);
	buf->len += datalen;
}

/* shim W5: pg_server_to_client, ClientEncoding == ServerEncoding arm:
 * mbutils.c returns the source pointer unchanged (no conversion). */
static char *
pg8_server_to_client(const char *s, int len)
{
	(void) len;
	return (char *) s;
}

/*
 * pqformat.c pq_sendtext, verbatim body over the shimmed plumbing:
 *
 *	void pq_sendtext(StringInfo buf, const char *str, int slen)
 *	{
 *		char *p;
 *		p = pg_server_to_client(str, slen);
 *		if (p != str) { slen = strlen(p);
 *			appendBinaryStringInfo(buf, p, slen); pfree(p); }
 *		else appendBinaryStringInfo(buf, str, slen);
 *	}
 */
static void
pg8_pq_sendtext(PgSI *buf, const char *str, int slen)
{
	char	   *p;

	p = pg8_server_to_client(str, slen);
	if (p != str)
	{
		slen = strlen(p);
		pg8_appendBinaryStringInfo(buf, p, slen);
	}
	else
		pg8_appendBinaryStringInfo(buf, str, slen);
}

/* pq_begintypsend: initStringInfo + four reserved length-word bytes. */
static void
pg8_pq_begintypsend(PgSI *buf, unsigned char *backing)
{
	buf->data = backing;
	buf->len = 0;
	buf->data[0] = '\0';
	pg8_appendBinaryStringInfo(buf, "\0\0\0\0", 4);
}

/* pq_endtypsend: SET_VARSIZE(buf->data, buf->len) + return as bytea.
 * Shim W4: the harness compares VARDATA == data+4, length len-4. */
static int
pg8_pq_endtypsend(PgSI *buf, unsigned char *out)
{
	memcpy(out, buf->data + VARHDRSZ, buf->len - VARHDRSZ);
	return buf->len - VARHDRSZ;
}

/* pq_sendintN -> the pg_hton big-endian stores those inlines perform. */
static void
pg8_pq_sendint16(PgSI *buf, uint16 i)
{
	unsigned char n[2] = {(unsigned char) (i >> 8), (unsigned char) i};

	pg8_appendBinaryStringInfo(buf, (const char *) n, 2);
}

static void
pg8_pq_sendint32(PgSI *buf, uint32 i)
{
	unsigned char n[4] = {(unsigned char) (i >> 24), (unsigned char) (i >> 16),
	(unsigned char) (i >> 8), (unsigned char) i};

	pg8_appendBinaryStringInfo(buf, (const char *) n, 4);
}

static void
pg8_pq_sendint64(PgSI *buf, uint64 i)
{
	unsigned char n[8] = {(unsigned char) (i >> 56), (unsigned char) (i >> 48),
		(unsigned char) (i >> 40), (unsigned char) (i >> 32),
		(unsigned char) (i >> 24), (unsigned char) (i >> 16),
	(unsigned char) (i >> 8), (unsigned char) i};

	pg8_appendBinaryStringInfo(buf, (const char *) n, 8);
}

/* ---------------- varlena.c: send family ---------------- */

/*
 * byteasend — "just copy the input": PG_GETARG_BYTEA_P_COPY + return.
 * Result payload == input payload.
 */
int
pg8_byteasend(const unsigned char *data, int len, unsigned char *out)
{
	memcpy(out, data, len);
	return len;
}

/*
 * textsend, verbatim body:
 *	pq_begintypsend(&buf);
 *	pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
 *	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
 */
int
pg8_textsend(const unsigned char *data, int len, unsigned char *out)
{
	PgSI		buf;
	unsigned char backing[4 + 64];

	pg8_pq_begintypsend(&buf, backing);
	pg8_pq_sendtext(&buf, (const char *) data, len);
	return pg8_pq_endtypsend(&buf, out);
}

/*
 * unknownsend, verbatim body:
 *	pq_begintypsend(&buf);
 *	pq_sendtext(&buf, str, strlen(str));
 *	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
 */
int
pg8_unknownsend(const char *str, unsigned char *out)
{
	PgSI		buf;
	unsigned char backing[4 + 64];

	pg8_pq_begintypsend(&buf, backing);
	pg8_pq_sendtext(&buf, str, strlen(str));
	return pg8_pq_endtypsend(&buf, out);
}

/* ---------------- int.c/int8.c: intNsend (int2/4/8_bytea) -------------- */

/* int2_bytea: "can just use int2send()". int2send (int.c):
 *	pq_begintypsend(&buf); pq_sendint16(&buf, arg1);
 *	PG_RETURN_BYTEA_P(pq_endtypsend(&buf)); */
int
pg8_int2_bytea(int16 arg1, unsigned char *out)
{
	PgSI		buf;
	unsigned char backing[4 + 8];

	pg8_pq_begintypsend(&buf, backing);
	pg8_pq_sendint16(&buf, (uint16) arg1);
	return pg8_pq_endtypsend(&buf, out);
}

int
pg8_int4_bytea(int arg1, unsigned char *out)
{
	PgSI		buf;
	unsigned char backing[4 + 8];

	pg8_pq_begintypsend(&buf, backing);
	pg8_pq_sendint32(&buf, (uint32) arg1);
	return pg8_pq_endtypsend(&buf, out);
}

int
pg8_int8_bytea(int64 arg1, unsigned char *out)
{
	PgSI		buf;
	unsigned char backing[4 + 16];

	pg8_pq_begintypsend(&buf, backing);
	pg8_pq_sendint64(&buf, (uint64) arg1);
	return pg8_pq_endtypsend(&buf, out);
}

/* ---------------- varlena.c: bytea -> int casts ---------------- */

/* Cast bytea -> int2, verbatim body (input as payload pair, shim W1). */
int16
pg8_bytea_int2(const unsigned char *data, int len)
{
	uint16		result;

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
	{
		pg8_errflag = PG8_E_NUM_RANGE;
		return 0;
	}

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= data[i];
	}

	return (int16) result;
}

/* Cast bytea -> int4, verbatim body. */
int
pg8_bytea_int4(const unsigned char *data, int len)
{
	uint32		result;

	if (len > (int) sizeof(result))
	{
		pg8_errflag = PG8_E_NUM_RANGE;
		return 0;
	}

	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= data[i];
	}

	return (int) result;
}

/* Cast bytea -> int8, verbatim body. */
int64
pg8_bytea_int8(const unsigned char *data, int len)
{
	uint64		result;

	if (len > (int) sizeof(result))
	{
		pg8_errflag = PG8_E_NUM_RANGE;
		return 0;
	}

	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= data[i];
	}

	return (int64) result;
}

/* ---------------- varlena.c: bytea_reverse ---------------- */

/* Return reversed bytea, verbatim body (palloc -> out, shim W2). */
int
pg8_bytea_reverse(const unsigned char *data, int len, unsigned char *out)
{
	const unsigned char *p = data;
	const unsigned char *endp = p + len;
	unsigned char *dst = out + len;

	while (p < endp)
		*(--dst) = *p++;

	return len;
}

/* ---------------- varlena.c: bytea_larger / bytea_smaller (shim W7) ---- */

int
pg8_bytea_larger_pick(const unsigned char *d1, int len1,
					  const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));
	/* result = ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? arg1 : arg2) */
	return ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? 0 : 1);
}

int
pg8_bytea_smaller_pick(const unsigned char *d1, int len1,
					   const unsigned char *d2, int len2)
{
	int			cmp;

	cmp = memcmp(d1, d2, Min(len1, len2));
	/* result = ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? arg1 : arg2) */
	return ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? 0 : 1);
}

/* ---------------- pg_bitutils.c: pg_popcount (shim W6) ---------------- */

/* c.h TYPEALIGN, verbatim */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((unsigned long long) (LEN) + ((ALIGNVAL) - 1)) & ~((unsigned long long) ((ALIGNVAL) - 1)))

/* src/port/pg_bitutils.c pg_number_of_ones, verbatim */
static const uint8 pg8_number_of_ones[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

/* pg_bitutils.h pg_popcount64, the pg_popcount64_slow byte-table arm
 * (build without HAVE__BUILTIN_POPCOUNT; shim W6), verbatim body */
static int
pg8_popcount64(uint64 word)
{
	int			result = 0;

	while (word != 0)
	{
		result += pg8_number_of_ones[word & 255];
		word >>= 8;
	}

	return result;
}

/* src/port/pg_bitutils.c pg_popcount_portable, verbatim body */
static uint64
pg8_pg_popcount(const char *buf, int bytes)
{
	uint64		popcnt = 0;

	/* Process in 64-bit chunks if the buffer is aligned */
	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg8_popcount64(*words++);
			bytes -= 8;
		}

		buf = (const char *) words;
	}

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg8_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/* bytea_bit_count: PG_RETURN_INT64(pg_popcount(VARDATA_ANY, VARSIZE_..)) */
int64
pg8_bytea_bit_count(const unsigned char *data, int len)
{
	return (int64) pg8_pg_popcount((const char *) data, len);
}

/* ---------------- varlena.c: convert_to_base + to_bin/oct/hex ---------- */

/*
 * Workhorse for to_bin, to_oct, and to_hex.  Note that base must be > 1 and
 * <= 16.  Verbatim body; cstring_to_text_with_len -> out (shim W2).
 */
static int
pg8_convert_to_base(uint64 value, int base, unsigned char *out)
{
	const char *digits = "0123456789abcdef";

	/* We size the buffer for to_bin's longest possible return value. */
	char		buf[sizeof(uint64) * BITS_PER_BYTE];
	char	   *const end = buf + sizeof(buf);
	char	   *ptr = end;

	do
	{
		*--ptr = digits[value % base];
		value /= base;
	} while (ptr > buf && value);

	return pg8_cstring_to_text_with_len(ptr, end - ptr, out);
}

int
pg8_to_bin32(int arg, unsigned char *out)
{
	uint64		value = (uint32) arg;

	return pg8_convert_to_base(value, 2, out);
}

int
pg8_to_bin64(int64 arg, unsigned char *out)
{
	uint64		value = (uint64) arg;

	return pg8_convert_to_base(value, 2, out);
}

int
pg8_to_oct32(int arg, unsigned char *out)
{
	uint64		value = (uint32) arg;

	return pg8_convert_to_base(value, 8, out);
}

int
pg8_to_oct64(int64 arg, unsigned char *out)
{
	uint64		value = (uint64) arg;

	return pg8_convert_to_base(value, 8, out);
}

int
pg8_to_hex32(int arg, unsigned char *out)
{
	uint64		value = (uint32) arg;

	return pg8_convert_to_base(value, 16, out);
}

int
pg8_to_hex64(int64 arg, unsigned char *out)
{
	uint64		value = (uint64) arg;

	return pg8_convert_to_base(value, 16, out);
}

/* ---------------- varlena.c: bytea_overlay (shim W8) ---------------- */

/* Vendored in c/pg_text_slice.c (link both files): */
extern int pg_bytea_substring(const unsigned char *d, int len, int s, int l,
							  int length_not_specified, unsigned char *out);
extern int pg_bytea_catenate(const unsigned char *d1, int len1,
							 const unsigned char *d2, int len2,
							 unsigned char *out);
extern int pg_take_err(void);

/*
 * bytea_overlay, verbatim body over the shimmed substring/catenate.
 * no_len models byteaoverlay_no_len's `sl = VARSIZE_ANY_EXHDR(t2)`.
 */
int
pg8_bytea_overlay(const unsigned char *d1, int len1,
				  const unsigned char *d2, int len2,
				  int sp, int sl, unsigned char *out)
{
	int			sp_pl_sl;
	unsigned char s1[16];
	int			s1len;
	unsigned char s2[16];
	int			s2len;
	unsigned char r1[32];
	int			r1len;
	int			rlen;

	/*
	 * Check for possible integer-overflow cases.  For negative sp, throw a
	 * "substring length" error because that's what should be expected
	 * according to the spec's definition of OVERLAY().
	 */
	if (sp <= 0)
	{
		pg8_errflag = PG8_E_SUBSTRING;
		return PG_CERR;
	}
	if (pg8_add_s32_overflow(sp, sl, &sp_pl_sl))
	{
		pg8_errflag = PG8_E_NUM_RANGE;
		return PG_CERR;
	}

	s1len = pg_bytea_substring(d1, len1, 1, sp - 1, 0, s1);
	if (s1len == PG_CERR)
	{
		pg8_errflag = pg_take_err();	/* shim W8 fold (unreachable) */
		return PG_CERR;
	}
	s2len = pg_bytea_substring(d1, len1, sp_pl_sl, -1, 1, s2);
	if (s2len == PG_CERR)
	{
		pg8_errflag = pg_take_err();	/* shim W8 fold (unreachable) */
		return PG_CERR;
	}
	r1len = pg_bytea_catenate(s1, s1len, d2, len2, r1);
	if (r1len == PG_CERR)
	{
		pg8_errflag = pg_take_err();	/* shim W8 fold (unreachable) */
		return PG_CERR;
	}
	rlen = pg_bytea_catenate(r1, r1len, s2, s2len, out);
	if (rlen == PG_CERR)
	{
		pg8_errflag = pg_take_err();	/* shim W8 fold (unreachable) */
		return PG_CERR;
	}

	return rlen;
}
