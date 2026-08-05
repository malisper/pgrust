/*
 * pg_vltext_io.c: vendored PostgreSQL C oracle for the vltext_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/varlena,
 * text family).
 *
 * Provenance (all bodies verbatim modulo the documented shims; source of
 * record: pgrust-fabled/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0, PostgreSQL 18.3 Stamp-18.3):
 *   - src/backend/utils/adt/varlena.c: cstring_to_text,
 *     cstring_to_text_with_len, text_to_cstring, textin, textout, textrecv,
 *     textsend, unknownin, unknownout, unknownrecv, unknownsend, textlen,
 *     text_length, textoctetlen, textcat, text_catenate, text_substr,
 *     text_substr_no_len, text_substring, pg_mbcharcliplen_chars,
 *     textoverlay, textoverlay_no_len, text_overlay, textpos, text_position,
 *     text_position_setup, text_position_next, text_position_next_internal,
 *     text_position_get_match_ptr, text_position_get_match_pos,
 *     text_position_reset, text_position_cleanup, check_collation_set,
 *     varstr_cmp, text_cmp, texteq, textne, text_lt, text_le, text_gt,
 *     text_ge, text_starts_with, bttextcmp, btvarstrequalimage, text_larger,
 *     text_smaller, internal_text_pattern_compare, text_pattern_lt,
 *     text_pattern_le, text_pattern_ge, text_pattern_gt, bttext_pattern_cmp,
 *     appendStringInfoText, replace_text, split_part, TextPositionState.
 *   - src/backend/access/hash/hashfunc.c: hashtext, hashtextextended.
 *   - src/common/hashfn.c: rot/mix/final macros, hash_bytes,
 *     hash_bytes_extended (real hash-value C parity, not an in-harness
 *     identity check).
 *   - src/common/wchar.c: pg_utf_mblen, pg_utf8_islegal, pg_utf8_verifychar.
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_range, pg_mblen_with_len,
 *     pg_mblen_unbounded, pg_mbstrlen_with_len, pg_verify_mbstr multibyte
 *     walk, pg_any_to_server validation branch.
 *   - src/backend/libpq/pqformat.c: pq_getmsgtext, pq_begintypsend,
 *     pq_sendtext, pq_endtypsend.
 *   - src/include/common/int.h: pg_add_s32_overflow, pg_mul_s32_overflow.
 *
 * ENVIRONMENT FENCE (pins the environment, never the computation):
 *   - DATABASE ENCODING FIXED = UTF8 on both sides (PostgreSQL's default;
 *     the Rust driver pins mbutils::SetDatabaseEncoding(PG_UTF8)).
 *     pg_database_encoding_max_length() == 4, GetDatabaseEncoding() ==
 *     PG_UTF8, and every mblen_fn table lookup resolves to pg_utf_mblen /
 *     pg_utf8_verifychar (both vendored verbatim from wchar.c — real UTF8
 *     length/validation logic, never a stub; textlen / text_substr /
 *     textpos semantics depend on it).
 *   - COLLATION FIXED = C collation (C_COLLATION_OID 950) for every
 *     comparison/hash function; locale/ICU arms are OUT of scope per the
 *     campaign carve. pg_newlocale_from_collation() returns a pinned
 *     {collate_is_c = true, deterministic = true} locale for 950 and
 *     abort()s loudly for any other valid OID (the driver never sends one).
 *     InvalidOid (0) IS in scope: check_collation_set's 42P22 arm.
 *   - CLIENT ENCODING stays SQL_ASCII (pgrust's default) on both sides:
 *     pq conversion is the identity, but textrecv/unknownrecv still perform
 *     PG's mandatory validation of wire bytes against the database encoding
 *     (pg_any_to_server: "No conversion is needed, but we must still
 *     validate the data" -> 22021 on invalid UTF8). Outbound
 *     (textsend/unknownsend) pg_server_to_client is the identity with no
 *     validation, as in C.
 *
 * Shims (PLUMBING ONLY, never logic):
 *   1. PG_FUNCTION_ARGS unwrapping -> plain C signatures over (ptr,len)
 *      text payloads (the entries build plain 4B-header varlena images in
 *      the palloc arena, modeling PG_GETARG_TEXT_PP over untoasted args);
 *      PG_RETURN_* -> plain returns / caller out-buffers;
 *      PG_FREE_IF_COPY -> no-op (arguments are never toast copies here).
 *   2. memcmp -> pg_ref_memcmp: classic unsigned-char byte loop returning
 *      the RAW byte difference at the first mismatch (glibc convention; the
 *      raw magnitude is SQL-visible through bttextcmp/bttext_pattern_cmp
 *      and the shipped Rust core implements the same convention). Same shim
 *      precedent as csrc/pg_name_io.c shim 2. Used only where the magnitude
 *      escapes (varstr_cmp, internal_text_pattern_compare); pure-equality
 *      memcmp uses take the libc call.
 *   3. varlena/varatt macros for LITTLE-ENDIAN plain 4B images (VARDATA /
 *      VARSIZE / SET_VARSIZE / VARSIZE_ANY_EXHDR / VARATT_IS_COMPRESSED /
 *      VARATT_IS_EXTERNAL) transcribed from varatt.h; the driver only ever
 *      constructs plain inline images so the compressed/external arms of
 *      text_substring are statically false (as in C for untoasted input).
 *      DatumGetTextPSlice is reached only in the eml==1 arm, which is dead
 *      under the UTF8 pin -> abort()-loud stub.
 *   4. ereport/elog -> errcode class capture in the shared _Thread_local
 *      pg_diff_errcode (defined in csrc/pg_float_io.c) + longjmp back to
 *      the pg_diff_vltext_* entry; errmsg/errhint/errdetail are ignored
 *      (message text out of scope). palloc/palloc0/repalloc/pfree ride the
 *      TLS arena below, so the longjmp cannot strand allocations.
 *   5. StringInfo mini-shim (initStringInfo / appendBinaryStringInfo /
 *      appendStringInfoChar / enlargeStringInfo doubling) over the arena,
 *      for replace_text and the pq send/recv plumbing.
 *   6. pg_locale_t reduced to {collate_is_c, deterministic} (the only
 *      fields the vendored bodies touch); pg_strncoll is the
 *      nondeterministic-only arm -> abort()-loud stub (dead under the C
 *      collation pin).
 *   7. CHECK_FOR_INTERRUPTS() -> no-op (no signal plumbing in the harness).
 *
 * Errcode classes (comparator plane 3; message text out of scope):
 *   1 = 22011 ERRCODE_SUBSTRING_ERROR (negative substring length)
 *   2 = 22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (overlay sp+sl overflow)
 *   3 = 22023 ERRCODE_INVALID_PARAMETER_VALUE (split_part fldnum == 0)
 *   4 = 22021 ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (invalid UTF8 byte seq)
 *   5 = 08P01 ERRCODE_PROTOCOL_VIOLATION (insufficient data in message)
 *   6 = 42P22 ERRCODE_INDETERMINATE_COLLATION (InvalidOid collation)
 *  98 = internal elog (unreachable under the UTF8 pin)
 */

#include <assert.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* Errcode classes (see header). */
#define PG_DIFF_ERR_SUBSTRING 1
#define PG_DIFF_ERR_OUT_OF_RANGE 2
#define PG_DIFF_ERR_INVALID_PARAM 3
#define PG_DIFF_ERR_NOT_IN_REPERTOIRE 4
#define PG_DIFF_ERR_PROTOCOL 5
#define PG_DIFF_ERR_INDET_COLLATION 6
#define PG_DIFF_ERR_INTERNAL 98

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().)
 * PG_DIFF_ARENA_MAX raised 64 -> 512 for this target: replace_text /
 * split_part / text_overlay allocate per text_position_setup + per
 * StringInfo growth step + per substring, and the entries additionally
 * build the argument images here (shim 1). 512 slots bounds the worst
 * capped input comfortably (StringInfo growth is geometric). */
#define PG_DIFF_ARENA_MAX 512
static _Thread_local void *pg_diff_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_arena_n;

static void
pg_diff_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
		free(pg_diff_arena[i]);
	pg_diff_arena_n = 0;
}

static void *
pg_diff_palloc_impl(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void *
pg_diff_repalloc_impl(void *old, size_t n)
{
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == old)
		{
			pg_diff_arena[i] = p;
			return p;
		}
	}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}

static void
pg_diff_pfree_impl(void *p)
{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{
		if (pg_diff_arena[i] == p)
		{
			free(p);
			pg_diff_arena[i] = pg_diff_arena[--pg_diff_arena_n];
			return;
		}
	}
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)

/* ==================== SECTION 0: environment shims ==================== */

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef unsigned int Oid;
typedef uintptr_t Datum;
/* bool/true/false from <stdbool.h> above (1-byte bool, matching real PG's
 * c.h and every other oracle TU; the old `typedef int bool` shim is a C23
 * compile error — 'bool' is a keyword from -std=gnu23, gcc >= 15). */

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define C_COLLATION_OID 950		/* catalog/pg_collation.h */
#define PG_UTF8 6				/* mb/pg_wchar.h */
#define PG_SQL_ASCII 0

#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif
#ifndef Max
#define Max(x, y) ((x) > (y) ? (x) : (y))
#endif
#ifndef Assert
#define Assert(x) ((void) 0)
#endif
#define unlikely(x) (x)
#define CHECK_FOR_INTERRUPTS() ((void) 0)	/* shim 7 */
#define unconstify(underlying_type, expr) ((underlying_type) (expr))

/* shim 3: varlena / varatt over plain little-endian 4B images */
#define VARHDRSZ ((int32) sizeof(int32))
typedef struct varlena
{
	char		vl_len_[4];		/* opaque: use SET_VARSIZE()/VARSIZE() */
	char		vl_dat[];
} varlena;
typedef varlena text;
typedef varlena bytea;

static uint32
pg_vltext_varsize_word(const void *ptr)
{
	uint32		w;

	memcpy(&w, ptr, 4);
	return w;
}

#define VARSIZE(PTR) (pg_vltext_varsize_word(PTR) >> 2)
#define VARDATA(PTR) (((varlena *) (PTR))->vl_dat)
#define SET_VARSIZE(PTR, len) \
	do { uint32 w_ = ((uint32) (len)) << 2; memcpy((PTR), &w_, 4); } while (0)
/* driver images are always plain 4B: _ANY == plain */
#define VARSIZE_ANY(PTR) VARSIZE(PTR)
#define VARSIZE_ANY_EXHDR(PTR) ((int) VARSIZE(PTR) - VARHDRSZ)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARATT_IS_COMPRESSED(PTR) ((*((const uint8 *) (PTR)) & 0x03) == 0x02)
#define VARATT_IS_EXTERNAL(PTR) (*((const uint8 *) (PTR)) == 0x01)

#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetTextPP(X) ((text *) DatumGetPointer(X))	/* untoasted */
#define pg_detoast_datum_packed(p) (p)	/* untoasted */
/* toast_raw_datum_size over plain images = VARSIZE */
#define toast_raw_datum_size(D) ((Size) VARSIZE(DatumGetPointer(D)))

/* Dead under the UTF8 pin (only the eml==1 arm slices datums). */
static text *
DatumGetTextPSlice(Datum d, int32 start, int32 len)
{
	(void) d;
	(void) start;
	(void) len;
	abort();					/* unreachable: eml == 4 */
}

/* shim 4: ereport/elog -> errcode class + longjmp */
static _Thread_local jmp_buf pg_vltext_jb;

#define ERRCODE_SUBSTRING_ERROR PG_DIFF_ERR_SUBSTRING
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE PG_DIFF_ERR_OUT_OF_RANGE
#define ERRCODE_INVALID_PARAMETER_VALUE PG_DIFF_ERR_INVALID_PARAM
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE PG_DIFF_ERR_NOT_IN_REPERTOIRE
#define ERRCODE_PROTOCOL_VIOLATION PG_DIFF_ERR_PROTOCOL
#define ERRCODE_INDETERMINATE_COLLATION PG_DIFF_ERR_INDET_COLLATION
#define ERRCODE_FEATURE_NOT_SUPPORTED PG_DIFF_ERR_INTERNAL	/* dead: C-collation pin */

static int
errcode(int sqlerrcode)
{
	pg_diff_errcode = sqlerrcode;
	return 0;
}

static int
errmsg(const char *fmt, ...)
{
	(void) fmt;					/* message text out of scope */
	return 0;
}

static int
errhint(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

#define ereport(level, rest) \
	do { (void) rest; longjmp(pg_vltext_jb, 1); } while (0)
#define elog(level, ...) \
	do { pg_diff_errcode = PG_DIFF_ERR_INTERNAL; longjmp(pg_vltext_jb, 1); } while (0)

/* shim 6: pg_locale_t pinned to the C collation */
typedef struct pg_locale_struct
{
	bool		collate_is_c;
	bool		deterministic;
} pg_locale_struct;
typedef pg_locale_struct *pg_locale_t;

static pg_locale_struct pg_vltext_c_locale = {true, true};

static pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	if (collid != C_COLLATION_OID)
		abort();				/* fence: driver only sends 950 (or 0, which
								 * check_collation_set rejects first) */
	return &pg_vltext_c_locale;
}

/* nondeterministic-collation-only arm: dead under the C-collation pin */
static int
pg_strncoll(const char *arg1, size_t len1, const char *arg2, size_t len2,
			pg_locale_t locale)
{
	(void) arg1;
	(void) len1;
	(void) arg2;
	(void) len2;
	(void) locale;
	abort();
}

/* shim 2: raw-difference byte-loop memcmp (glibc convention; the raw
 * magnitude is SQL-visible through bttextcmp / bttext_pattern_cmp). */
static int
pg_ref_memcmp(const void *v1, const void *v2, size_t n)
{
	const unsigned char *s1 = v1;
	const unsigned char *s2 = v2;
	size_t		i;

	for (i = 0; i < n; i++)
	{
		if (s1[i] != s2[i])
			return (int) s1[i] - (int) s2[i];
	}
	return 0;
}

/* ---- src/include/common/int.h (verbatim: builtin arm) ---- */

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_mul_overflow(a, b, result);
}

/* ============ SECTION 1: src/common/wchar.c (VERBATIM) ============ */

static int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
	else
		len = 1;
	return len;
}

static bool
pg_utf8_islegal(const unsigned char *source, int length)
{
	unsigned char a;

	switch (length)
	{
		default:
			/* reject lengths 5 and 6 for now */
			return false;
		case 4:
			a = source[3];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 3:
			a = source[2];
			if (a < 0x80 || a > 0xBF)
				return false;
			/* FALL THRU */
		case 2:
			a = source[1];
			switch (*source)
			{
				case 0xE0:
					if (a < 0xA0 || a > 0xBF)
						return false;
					break;
				case 0xED:
					if (a < 0x80 || a > 0x9F)
						return false;
					break;
				case 0xF0:
					if (a < 0x90 || a > 0xBF)
						return false;
					break;
				case 0xF4:
					if (a < 0x80 || a > 0x8F)
						return false;
					break;
				default:
					if (a < 0x80 || a > 0xBF)
						return false;
					break;
			}
			/* FALL THRU */
		case 1:
			a = *source;
			if (a >= 0x80 && a < 0xC2)
				return false;
			if (a > 0xF4)
				return false;
			break;
	}
	return true;
}

static int
pg_utf8_verifychar(const unsigned char *s, int len)
{
	int			l;

	if ((*s & 0x80) == 0)
	{
		if (*s == '\0')
			return -1;
		return 1;
	}
	else if ((*s & 0xe0) == 0xc0)
		l = 2;
	else if ((*s & 0xf0) == 0xe0)
		l = 3;
	else if ((*s & 0xf8) == 0xf0)
		l = 4;
	else
		l = 1;

	if (l > len)
		return -1;

	if (!pg_utf8_islegal(s, l))
		return -1;

	return l;
}

/* ====== SECTION 2: src/backend/utils/mb/mbutils.c (VERBATIM, UTF8) ====== */

/* environment fence: database encoding pinned UTF8 (see header) */
static int
pg_database_encoding_max_length(void)
{
	return 4;					/* pg_wchar_table[PG_UTF8].maxmblen */
}

static int
GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

/* report_invalid_encoding -> errcode class 4 + longjmp (shim 4) */
static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	(void) mbstr;
	(void) mblen;
	(void) len;
	ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence for encoding")));
}

/*
 * pg_mblen_range (mbutils.c, verbatim modulo the mblen_fn table lookup
 * resolved to pg_utf_mblen per the encoding fence).
 */
static int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

	return length;
}

/* pg_mblen_with_len (mbutils.c, verbatim modulo the same resolution) */
static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	return length;
}

/* pg_mblen_unbounded (mbutils.c, verbatim modulo the same resolution) */
static int
pg_mblen_unbounded(const char *mbstr)
{
	return pg_utf_mblen((const unsigned char *) mbstr);
}

/* pg_mbstrlen_with_len (mbutils.c, verbatim; eml fixed > 1) */
static int
pg_mbstrlen_with_len(const char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen_with_len(mbstr, limit);

		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/*
 * pg_verify_mbstr multibyte walk (mbutils.c pg_verify_mbstr_len, verbatim
 * modulo: encoding fixed UTF8, mbverifychar resolved to pg_utf8_verifychar,
 * report_invalid_encoding -> errcode class 4 + longjmp; noError = false
 * posture, so invalid input never returns).
 */
static void
pg_verify_mbstr(int encoding, const char *mbstr, int len, bool noError)
{
	(void) encoding;			/* PG_UTF8 only (fence) */
	(void) noError;				/* false posture */

	while (len > 0)
	{
		int			l;

		/* fast path for ASCII-subset characters */
		if (!(*((const unsigned char *) mbstr) & 0x80))
		{
			if (*mbstr != '\0')
			{
				mbstr++;
				len--;
				continue;
			}
			report_invalid_encoding_db(mbstr, 1, len);
		}

		l = pg_utf8_verifychar((const unsigned char *) mbstr, len);

		if (l < 0)
			report_invalid_encoding_db(mbstr, pg_utf_mblen((const unsigned char *) mbstr), len);

		mbstr += l;
		len -= l;
	}
}

/*
 * pg_any_to_server validation branch (mbutils.c, verbatim; client encoding
 * pinned SQL_ASCII -> "No conversion is needed, but we must still validate
 * the data"). pg_client_to_server resolves here per the fence.
 */
static char *
pg_any_to_server(const char *s, int len, int encoding)
{
	if (len <= 0)
		return unconstify(char *, s);	/* empty string is always valid */

	if (encoding == GetDatabaseEncoding() ||
		encoding == PG_SQL_ASCII)
	{
		/*
		 * No conversion is needed, but we must still validate the data.
		 */
		(void) pg_verify_mbstr(GetDatabaseEncoding(), s, len, false);
		return unconstify(char *, s);
	}
	abort();					/* fence: client encoding is SQL_ASCII */
}

static char *
pg_client_to_server(const char *s, int len)
{
	return pg_any_to_server(s, len, PG_SQL_ASCII);
}

/* pg_server_to_client (client SQL_ASCII): identity, no validation (as C) */
static char *
pg_server_to_client(const char *s, int len)
{
	(void) len;
	return unconstify(char *, s);
}

/* ========= SECTION 3: src/common/hashfn.c (VERBATIM) ========= */

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}

#define rot(x,k) pg_rotate_left32(x, k)

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

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

/* hash_bytes (hashfn.c, verbatim; little-endian arms — the fleet and the
 * laptop are both LE; WORDS_BIGENDIAN never defined here) */
static uint32
hash_bytes(const unsigned char *k, int keylen)
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
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
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
	}

	final(a, b, c);

	/* report the result */
	return c;
}

/* hash_bytes_extended (hashfn.c, verbatim; little-endian arms) */
static uint64
hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
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
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
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
	}

	final(a, b, c);

	/* report the result */
	return ((uint64) b << 32) | c;
}

/* hashfn.h wrappers (Datum boxing dropped per shim 1) */
#define hash_any(k, l) hash_bytes((k), (l))
#define hash_any_extended(k, l, s) hash_bytes_extended((k), (l), (s))

/* ====== SECTION 4: StringInfo mini-shim (shim 5, arena-backed) ====== */

typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
typedef StringInfoData *StringInfo;

static void
resetStringInfo(StringInfo str)
{
	str->data[0] = '\0';
	str->len = 0;
	str->cursor = 0;
}

static void
initStringInfo(StringInfo str)
{
	int			size = 1024;

	str->data = (char *) palloc(size);
	str->maxlen = size;
	resetStringInfo(str);
}

static void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	if (needed <= str->maxlen - str->len - 1)
		return;
	newlen = 2 * str->maxlen;
	while (needed > newlen - str->len - 1)
		newlen = 2 * newlen;
	str->data = (char *) repalloc(str->data, newlen);
	str->maxlen = newlen;
}

static void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	enlargeStringInfo(str, datalen);
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

static void
appendStringInfoChar(StringInfo str, char ch)
{
	enlargeStringInfo(str, 1);
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

#define appendStringInfoCharMacro(str, ch) appendStringInfoChar((str), (ch))

/* ====== SECTION 5: src/backend/libpq/pqformat.c (VERBATIM) ====== */

static char *
pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
{
	char	   *str;
	char	   *p;

	if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	str = &msg->data[msg->cursor];
	msg->cursor += rawbytes;

	p = pg_client_to_server(str, rawbytes);
	if (p != str)				/* actual conversion has been done? */
		*nbytes = strlen(p);
	else
	{
		p = (char *) palloc(rawbytes + 1);
		memcpy(p, str, rawbytes);
		p[rawbytes] = '\0';
		*nbytes = rawbytes;
	}
	return p;
}

static void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
	char	   *p;

	p = pg_server_to_client(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		appendBinaryStringInfo(buf, p, slen);
		pfree(p);
	}
	else
		appendBinaryStringInfo(buf, str, slen);
}

static void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

static bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

/* ====== SECTION 6: src/backend/utils/adt/varlena.c (VERBATIM) ====== */

typedef struct
{
	pg_locale_t locale;			/* collation used for substring matching */
	bool		is_multibyte_char_in_char;	/* need to check char boundaries? */
	bool		greedy;			/* find longest possible substring? */

	char	   *str1;			/* haystack string */
	char	   *str2;			/* needle string */
	int			len1;			/* string lengths in bytes */
	int			len2;

	/* Skip table for Boyer-Moore-Horspool search algorithm: */
	int			skiptablemask;	/* mask for ANDing with skiptable subscripts */
	int			skiptable[256]; /* skip distance for given mismatched char */

	/*
	 * Note that with nondeterministic collations, the length of the last
	 * match is not necessarily equal to the length of the "needle" passed in.
	 */
	char	   *last_match;		/* pointer to last match in 'str1' */
	int			last_match_len; /* length of last match */
	int			last_match_len_tmp; /* same but for internal use */

	/*
	 * Sometimes we need to convert the byte position of a match to a
	 * character position.  These store the last position that was converted,
	 * so that on the next call, we can continue from that point, rather than
	 * count characters from the very beginning.
	 */
	char	   *refpoint;		/* pointer within original haystack string */
	int			refpos;			/* 0-based character offset of the same point */
} TextPositionState;

static text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

static text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

static char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed(unconstify(text *, t));
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);

	return result;
}

/* text_length (verbatim modulo shim 1: PG_RETURN_INT32 -> return) */
static int32
text_length(Datum str)
{
	/* fastpath when max encoding length is one */
	if (pg_database_encoding_max_length() == 1)
		return (toast_raw_datum_size(str) - VARHDRSZ);
	else
	{
		text	   *t = DatumGetTextPP(str);

		return (pg_mbstrlen_with_len(VARDATA_ANY(t),
									 VARSIZE_ANY_EXHDR(t)));
	}
}

/* text_catenate (verbatim) */
static text *
text_catenate(text *t1, text *t2)
{
	text	   *result;
	int			len1,
				len2,
				len;
	char	   *ptr;

	len1 = VARSIZE_ANY_EXHDR(t1);
	len2 = VARSIZE_ANY_EXHDR(t2);

	/* paranoia ... probably should throw error instead? */
	if (len1 < 0)
		len1 = 0;
	if (len2 < 0)
		len2 = 0;

	len = len1 + len2 + VARHDRSZ;
	result = (text *) palloc(len);

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	ptr = VARDATA(result);
	if (len1 > 0)
		memcpy(ptr, VARDATA_ANY(t1), len1);
	if (len2 > 0)
		memcpy(ptr + len1, VARDATA_ANY(t2), len2);

	return result;
}

/* pg_mbcharcliplen_chars (verbatim) */
static int
pg_mbcharcliplen_chars(const char *mbstr, int len, int limit)
{
	int			nch = 0;
	int			l;

	Assert(len > 0);
	Assert(limit > 0);
	Assert(pg_database_encoding_max_length() > 1);

	while (len > 0 && *mbstr)
	{
		l = pg_mblen_with_len(mbstr, len);
		nch++;
		if (nch == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return nch;
}

/* text_substring (verbatim) */
static text *
text_substring(Datum str, int32 start, int32 length, bool length_not_specified)
{
	int32		eml = pg_database_encoding_max_length();
	int32		S = start;		/* start position */
	int32		S1;				/* adjusted start position */
	int32		L1;				/* adjusted substring length */
	int32		E;				/* end position, exclusive */

	/*
	 * SQL99 says S can be zero or negative (which we don't document), but we
	 * still must fetch from the start of the string.
	 * https://www.postgresql.org/message-id/170905442373.643.11536838320909376197%40wrigleys.postgresql.org
	 */
	S1 = Max(S, 1);

	/* life is easy if the encoding max length is 1 */
	if (eml == 1)
	{
		if (length_not_specified)	/* special case - get length to end of
									 * string */
			L1 = -1;
		else if (length < 0)
		{
			/* SQL99 says to throw an error for E < S, i.e., negative length */
			ereport(ERROR,
					(errcode(ERRCODE_SUBSTRING_ERROR),
					 errmsg("negative substring length not allowed")));
			L1 = -1;			/* silence stupider compilers */
		}
		else if (pg_add_s32_overflow(S, length, &E))
		{
			/*
			 * L could be large enough for S + L to overflow, in which case
			 * the substring must run to end of string.
			 */
			L1 = -1;
		}
		else
		{
			/*
			 * A zero or negative value for the end position can happen if the
			 * start was negative or one. SQL99 says to return a zero-length
			 * string.
			 */
			if (E < 1)
				return cstring_to_text("");

			L1 = E - S1;
		}

		/*
		 * If the start position is past the end of the string, SQL99 says to
		 * return a zero-length string -- DatumGetTextPSlice() will do that
		 * for us.  We need only convert S1 to zero-based starting position.
		 */
		return DatumGetTextPSlice(str, S1 - 1, L1);
	}
	else if (eml > 1)
	{
		/*
		 * When encoding max length is > 1, we can't get LC without
		 * detoasting, so we'll grab a conservatively large slice now and go
		 * back later to do the right thing
		 */
		int32		slice_start;
		int32		slice_size;
		int32		slice_strlen;
		int32		slice_len;
		text	   *slice;
		int32		E1;
		int32		i;
		char	   *p;
		char	   *s;
		text	   *ret;

		/*
		 * We need to start at position zero because there is no way to know
		 * in advance which byte offset corresponds to the supplied start
		 * position.
		 */
		slice_start = 0;

		if (length_not_specified)	/* special case - get length to end of
									 * string */
			E = slice_size = L1 = -1;
		else if (length < 0)
		{
			/* SQL99 says to throw an error for E < S, i.e., negative length */
			ereport(ERROR,
					(errcode(ERRCODE_SUBSTRING_ERROR),
					 errmsg("negative substring length not allowed")));
			E = slice_size = L1 = -1;	/* silence stupider compilers */
		}
		else if (pg_add_s32_overflow(S, length, &E))
		{
			/*
			 * L could be large enough for S + L to overflow, in which case
			 * the substring must run to end of string.
			 */
			slice_size = L1 = -1;
		}
		else
		{
			/*
			 * Ending at position 1, exclusive, obviously yields an empty
			 * string.  A zero or negative value can happen if the start was
			 * negative or one. SQL99 says to return a zero-length string.
			 */
			if (E <= 1)
				return cstring_to_text("");

			/*
			 * if E is past the end of the string, the tuple toaster will
			 * truncate the length for us
			 */
			L1 = E - S1;

			/*
			 * Total slice size in bytes can't be any longer than the
			 * inclusive end position times the encoding max length.  If that
			 * overflows, we can just use -1.
			 */
			if (pg_mul_s32_overflow(E - 1, eml, &slice_size))
				slice_size = -1;
		}

		/*
		 * If we're working with an untoasted source, no need to do an extra
		 * copying step.
		 */
		if (VARATT_IS_COMPRESSED(DatumGetPointer(str)) ||
			VARATT_IS_EXTERNAL(DatumGetPointer(str)))
			slice = DatumGetTextPSlice(str, slice_start, slice_size);
		else
			slice = (text *) DatumGetPointer(str);

		/* see if we got back an empty string */
		slice_len = VARSIZE_ANY_EXHDR(slice);
		if (slice_len == 0)
		{
			if (slice != (text *) DatumGetPointer(str))
				pfree(slice);
			return cstring_to_text("");
		}

		/*
		 * Now we can get the actual length of the slice in MB characters,
		 * stopping at the end of the substring.  Continuing beyond the
		 * substring end could find an incomplete character attributable
		 * solely to DatumGetTextPSlice() chopping in the middle of a
		 * character, and it would be superfluous work at best.
		 */
		slice_strlen =
			(slice_size == -1 ?
			 pg_mbstrlen_with_len(VARDATA_ANY(slice), slice_len) :
			 pg_mbcharcliplen_chars(VARDATA_ANY(slice), slice_len, E - 1));

		/*
		 * Check that the start position wasn't > slice_strlen. If so, SQL99
		 * says to return a zero-length string.
		 */
		if (S1 > slice_strlen)
		{
			if (slice != (text *) DatumGetPointer(str))
				pfree(slice);
			return cstring_to_text("");
		}

		/*
		 * Adjust L1 and E1 now that we know the slice string length. Again
		 * remember that S1 is one based, and slice_start is zero based.
		 */
		if (L1 > -1)
			E1 = Min(S1 + L1, slice_start + 1 + slice_strlen);
		else
			E1 = slice_start + 1 + slice_strlen;

		/*
		 * Find the start position in the slice; remember S1 is not zero based
		 */
		p = VARDATA_ANY(slice);
		for (i = 0; i < S1 - 1; i++)
			p += pg_mblen_unbounded(p);

		/* hang onto a pointer to our start position */
		s = p;

		/*
		 * Count the actual bytes used by the substring of the requested
		 * length.
		 */
		for (i = S1; i < E1; i++)
			p += pg_mblen_unbounded(p);

		ret = (text *) palloc(VARHDRSZ + (p - s));
		SET_VARSIZE(ret, VARHDRSZ + (p - s));
		memcpy(VARDATA(ret), s, (p - s));

		if (slice != (text *) DatumGetPointer(str))
			pfree(slice);

		return ret;
	}
	else
		elog(ERROR, "invalid backend encoding: encoding max length < 1");

	/* not reached: suppress compiler warning */
	return NULL;
}

/* text_overlay (verbatim) */
static text *
text_overlay(text *t1, text *t2, int sp, int sl)
{
	text	   *result;
	text	   *s1;
	text	   *s2;
	int			sp_pl_sl;

	/*
	 * Check for possible integer-overflow cases.  For negative sp, throw a
	 * "substring length" error because that's what should be expected
	 * according to the spec's definition of OVERLAY().
	 */
	if (sp <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_SUBSTRING_ERROR),
				 errmsg("negative substring length not allowed")));
	if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	s1 = text_substring(PointerGetDatum(t1), 1, sp - 1, false);
	s2 = text_substring(PointerGetDatum(t1), sp_pl_sl, -1, true);
	result = text_catenate(s1, t2);
	result = text_catenate(result, s2);

	return result;
}

/* check_collation_set (verbatim) */
static void
check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string comparison"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}
}

/* text_position_setup (verbatim) */
static void
text_position_setup(text *t1, text *t2, Oid collid, TextPositionState *state)
{
	int			len1 = VARSIZE_ANY_EXHDR(t1);
	int			len2 = VARSIZE_ANY_EXHDR(t2);

	check_collation_set(collid);

	state->locale = pg_newlocale_from_collation(collid);

	/*
	 * Most callers need greedy mode, but some might want to unset this to
	 * optimize.
	 */
	state->greedy = true;

	Assert(len2 > 0);

	/*
	 * Even with a multi-byte encoding, we perform the search using the raw
	 * byte sequence, ignoring multibyte issues.  For UTF-8, that works fine,
	 * because in UTF-8 the byte sequence of one character cannot contain
	 * another character.  For other multi-byte encodings, we do the search
	 * initially as a simple byte search, ignoring multibyte issues, but
	 * verify afterwards that the match we found is at a character boundary,
	 * and continue the search if it was a false match.
	 */
	if (pg_database_encoding_max_length() == 1)
		state->is_multibyte_char_in_char = false;
	else if (GetDatabaseEncoding() == PG_UTF8)
		state->is_multibyte_char_in_char = false;
	else
		state->is_multibyte_char_in_char = true;

	state->str1 = VARDATA_ANY(t1);
	state->str2 = VARDATA_ANY(t2);
	state->len1 = len1;
	state->len2 = len2;
	state->last_match = NULL;
	state->refpoint = state->str1;
	state->refpos = 0;

	/*
	 * Prepare the skip table for Boyer-Moore-Horspool searching.  In these
	 * notes we use the terminology that the "haystack" is the string to be
	 * searched (t1) and the "needle" is the pattern being sought (t2).
	 *
	 * If the needle is empty or bigger than the haystack then there is no
	 * point in wasting cycles initializing the table.  We also choose not to
	 * use B-M-H for needles of length 1, since the skip table can't possibly
	 * save anything in that case.
	 *
	 * (With nondeterministic collations, the search is already
	 * multibyte-aware, so we don't need this.)
	 */
	if (len1 >= len2 && len2 > 1 && state->locale->deterministic)
	{
		int			searchlength = len1 - len2;
		int			skiptablemask;
		int			last;
		int			i;
		const char *str2 = state->str2;

		/*
		 * First we must determine how much of the skip table to use.  The
		 * declaration of TextPositionState allows up to 256 elements, but for
		 * short search problems we don't really want to have to initialize so
		 * many elements --- it would take too long in comparison to the
		 * actual search time.  So we choose a useful skip table size based on
		 * the haystack length minus the needle length.  The closer the needle
		 * length is to the haystack length the less useful skipping becomes.
		 *
		 * Note: since we use bit-masking to select table elements, the skip
		 * table size MUST be a power of 2, and so the mask must be 2^N-1.
		 */
		if (searchlength < 16)
			skiptablemask = 3;
		else if (searchlength < 64)
			skiptablemask = 7;
		else if (searchlength < 128)
			skiptablemask = 15;
		else if (searchlength < 512)
			skiptablemask = 31;
		else if (searchlength < 2048)
			skiptablemask = 63;
		else if (searchlength < 4096)
			skiptablemask = 127;
		else
			skiptablemask = 255;
		state->skiptablemask = skiptablemask;

		/*
		 * Initialize the skip table.  We set all elements to the needle
		 * length, since this is the correct skip distance for any character
		 * not found in the needle.
		 */
		for (i = 0; i <= skiptablemask; i++)
			state->skiptable[i] = len2;

		/*
		 * Now examine the needle.  For each character except the last one,
		 * set the corresponding table element to the appropriate skip
		 * distance.  Note that when two characters share the same skip table
		 * entry, the one later in the needle must determine the skip
		 * distance.
		 */
		last = len2 - 1;

		for (i = 0; i < last; i++)
			state->skiptable[(unsigned char) str2[i] & skiptablemask] = last - i;
	}
}

/* text_position_next_internal (verbatim) */
static char *
text_position_next_internal(char *start_ptr, TextPositionState *state)
{
	int			haystack_len = state->len1;
	int			needle_len = state->len2;
	int			skiptablemask = state->skiptablemask;
	const char *haystack = state->str1;
	const char *needle = state->str2;
	const char *haystack_end = &haystack[haystack_len];
	const char *hptr;

	Assert(start_ptr >= haystack && start_ptr <= haystack_end);
	Assert(needle_len > 0);

	state->last_match_len_tmp = needle_len;

	if (!state->locale->deterministic)
	{
		/*
		 * With a nondeterministic collation, we have to use an unoptimized
		 * route.  We walk through the haystack and see if at each position
		 * there is a substring of the remaining string that is equal to the
		 * needle under the given collation.
		 *
		 * Note, the found substring could have a different length than the
		 * needle.  Callers that want to skip over the found string need to
		 * read the length of the found substring from last_match_len rather
		 * than just using the length of their needle.
		 *
		 * Most callers will require "greedy" semantics, meaning that we need
		 * to find the longest such substring, not the shortest.  For callers
		 * that don't need greedy semantics, we can finish on the first match.
		 *
		 * This loop depends on the assumption that the needle is nonempty and
		 * any matching substring must also be nonempty.  (Even if the
		 * collation would accept an empty match, returning one would send
		 * callers that search for successive matches into an infinite loop.)
		 */
		const char *result_hptr = NULL;

		hptr = start_ptr;
		while (hptr < haystack_end)
		{
			const char *test_end;

			/*
			 * First check the common case that there is a match in the
			 * haystack of exactly the length of the needle.
			 */
			if (!state->greedy &&
				haystack_end - hptr >= needle_len &&
				pg_strncoll(hptr, needle_len, needle, needle_len, state->locale) == 0)
				return (char *) hptr;

			/*
			 * Else check if any of the non-empty substrings starting at hptr
			 * compare equal to the needle.
			 */
			test_end = hptr;
			do
			{
				test_end += pg_mblen_range(test_end, haystack_end);
				if (pg_strncoll(hptr, (test_end - hptr), needle, needle_len, state->locale) == 0)
				{
					state->last_match_len_tmp = (test_end - hptr);
					result_hptr = hptr;
					if (!state->greedy)
						break;
				}
			} while (test_end < haystack_end);

			if (result_hptr)
				break;

			hptr += pg_mblen_range(hptr, haystack_end);
		}

		return (char *) result_hptr;
	}
	else if (needle_len == 1)
	{
		/* No point in using B-M-H for a one-character needle */
		char		nchar = *needle;

		hptr = start_ptr;
		while (hptr < haystack_end)
		{
			if (*hptr == nchar)
				return (char *) hptr;
			hptr++;
		}
	}
	else
	{
		const char *needle_last = &needle[needle_len - 1];

		/* Start at startpos plus the length of the needle */
		hptr = start_ptr + needle_len - 1;
		while (hptr < haystack_end)
		{
			/* Match the needle scanning *backward* */
			const char *nptr;
			const char *p;

			nptr = needle_last;
			p = hptr;
			while (*nptr == *p)
			{
				/* Matched it all?	If so, return 1-based position */
				if (nptr == needle)
					return (char *) p;
				nptr--, p--;
			}

			/*
			 * No match, so use the haystack char at hptr to decide how far to
			 * advance.  If the needle had any occurrence of that character
			 * (or more precisely, one sharing the same skiptable entry)
			 * before its last character, then we advance far enough to align
			 * the last such needle character with that haystack position.
			 * Otherwise we can advance by the whole needle length.
			 */
			hptr += state->skiptable[(unsigned char) *hptr & skiptablemask];
		}
	}

	return 0;					/* not found */
}

/* text_position_next (verbatim) */
static bool
text_position_next(TextPositionState *state)
{
	int			needle_len = state->len2;
	char	   *start_ptr;
	char	   *matchptr;

	if (needle_len <= 0)
		return false;			/* result for empty pattern */

	/* Start from the point right after the previous match. */
	if (state->last_match)
		start_ptr = state->last_match + state->last_match_len;
	else
		start_ptr = state->str1;

retry:
	matchptr = text_position_next_internal(start_ptr, state);

	if (!matchptr)
		return false;

	/*
	 * Found a match for the byte sequence.  If this is a multibyte encoding,
	 * where one character's byte sequence can appear inside a longer
	 * multi-byte character, we need to verify that the match was at a
	 * character boundary, not in the middle of a multi-byte character.
	 */
	if (state->is_multibyte_char_in_char && state->locale->deterministic)
	{
		const char *haystack_end = state->str1 + state->len1;

		/* Walk one character at a time, until we reach the match. */

		/* the search should never move backwards. */
		Assert(state->refpoint <= matchptr);

		while (state->refpoint < matchptr)
		{
			/* step to next character. */
			state->refpoint += pg_mblen_range(state->refpoint, haystack_end);
			state->refpos++;

			/*
			 * If we stepped over the match's start position, then it was a
			 * false positive, where the byte sequence appeared in the middle
			 * of a multi-byte character.  Skip it, and continue the search at
			 * the next character boundary.
			 */
			if (state->refpoint > matchptr)
			{
				start_ptr = state->refpoint;
				goto retry;
			}
		}
	}

	state->last_match = matchptr;
	state->last_match_len = state->last_match_len_tmp;
	return true;
}

/* text_position_get_match_ptr (verbatim) */
static char *
text_position_get_match_ptr(TextPositionState *state)
{
	return state->last_match;
}

/* text_position_get_match_pos (verbatim) */
static int
text_position_get_match_pos(TextPositionState *state)
{
	/* Convert the byte position to char position. */
	state->refpos += pg_mbstrlen_with_len(state->refpoint,
										  state->last_match - state->refpoint);
	state->refpoint = state->last_match;
	return state->refpos + 1;
}

/* text_position_reset (verbatim) */
static void
text_position_reset(TextPositionState *state)
{
	state->last_match = NULL;
	state->refpoint = state->str1;
	state->refpos = 0;
}

static void
text_position_cleanup(TextPositionState *state)
{
	/* no cleanup needed */
	(void) state;
}

/* text_position (verbatim) */
static int
text_position(text *t1, text *t2, Oid collid)
{
	TextPositionState state;
	int			result;

	check_collation_set(collid);

	/* Empty needle always matches at position 1 */
	if (VARSIZE_ANY_EXHDR(t2) < 1)
		return 1;

	/* Otherwise, can't match if haystack is shorter than needle */
	if (VARSIZE_ANY_EXHDR(t1) < VARSIZE_ANY_EXHDR(t2) &&
		pg_newlocale_from_collation(collid)->deterministic)
		return 0;

	text_position_setup(t1, t2, collid, &state);
	/* don't need greedy mode here */
	state.greedy = false;

	if (!text_position_next(&state))
		result = 0;
	else
		result = text_position_get_match_pos(&state);
	text_position_cleanup(&state);
	return result;
}

/*
 * varstr_cmp (verbatim modulo shim 2: memcmp -> pg_ref_memcmp where the
 * magnitude escapes; the locale arm is dead under the C-collation pin but
 * kept verbatim, pg_strncoll abort-stubbed).
 */
static int
varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid)
{
	int			result;
	pg_locale_t mylocale;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->collate_is_c)
	{
		result = pg_ref_memcmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
		/*
		 * memcmp() can't tell us which of two unequal strings sorts first,
		 * but it's a cheap way to tell if they're equal.  Testing shows that
		 * memcmp() followed by strcoll() is only trivially slower than
		 * strcoll() by itself, so we don't lose much if this doesn't work out
		 * very often, and if it does - for example, because there are many
		 * equal strings in the input - then we win big by avoiding expensive
		 * collation-aware comparisons.
		 */
		if (len1 == len2 && memcmp(arg1, arg2, len1) == 0)
			return 0;

		result = pg_strncoll(arg1, len1, arg2, len2, mylocale);

		/* Break tie if necessary. */
		if (result == 0 && mylocale->deterministic)
		{
			result = pg_ref_memcmp(arg1, arg2, Min(len1, len2));
			if ((result == 0) && (len1 != len2))
				result = (len1 < len2) ? -1 : 1;
		}
	}

	return result;
}

/* text_cmp (verbatim) */
static int
text_cmp(text *arg1, text *arg2, Oid collid)
{
	char	   *a1p,
			   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	return varstr_cmp(a1p, len1, a2p, len2, collid);
}

/* texteq (verbatim modulo shim 1: Datums are plain image pointers) */
static bool
pg_texteq(Datum arg1, Datum arg2, Oid collid)
{
	pg_locale_t mylocale = 0;
	bool		result;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->deterministic)
	{
		Size		len1,
					len2;

		/*
		 * Since we only care about equality or not-equality, we can avoid all
		 * the expense of strcoll() here, and just do bitwise comparison.  In
		 * fact, we don't even have to do a bitwise comparison if we can show
		 * the lengths of the strings are unequal; which might save us from
		 * having to detoast one or both values.
		 */
		len1 = toast_raw_datum_size(arg1);
		len2 = toast_raw_datum_size(arg2);
		if (len1 != len2)
			result = false;
		else
		{
			text	   *targ1 = DatumGetTextPP(arg1);
			text	   *targ2 = DatumGetTextPP(arg2);

			result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
							 len1 - VARHDRSZ) == 0);
		}
	}
	else
	{
		text	   *arg1t = DatumGetTextPP(arg1);
		text	   *arg2t = DatumGetTextPP(arg2);

		result = (text_cmp(arg1t, arg2t, collid) == 0);
	}

	return result;
}

/* textne (verbatim modulo shim 1) */
static bool
pg_textne(Datum arg1, Datum arg2, Oid collid)
{
	pg_locale_t mylocale;
	bool		result;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->deterministic)
	{
		Size		len1,
					len2;

		/* See comment in texteq() */
		len1 = toast_raw_datum_size(arg1);
		len2 = toast_raw_datum_size(arg2);
		if (len1 != len2)
			result = true;
		else
		{
			text	   *targ1 = DatumGetTextPP(arg1);
			text	   *targ2 = DatumGetTextPP(arg2);

			result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
							 len1 - VARHDRSZ) != 0);
		}
	}
	else
	{
		text	   *arg1t = DatumGetTextPP(arg1);
		text	   *arg2t = DatumGetTextPP(arg2);

		result = (text_cmp(arg1t, arg2t, collid) != 0);
	}

	return result;
}

/* text_starts_with (verbatim modulo shim 1) */
static bool
pg_text_starts_with(Datum arg1, Datum arg2, Oid collid)
{
	pg_locale_t mylocale;
	bool		result;
	Size		len1,
				len2;

	check_collation_set(collid);

	mylocale = pg_newlocale_from_collation(collid);

	if (!mylocale->deterministic)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("nondeterministic collations are not supported for substring searches")));

	len1 = toast_raw_datum_size(arg1);
	len2 = toast_raw_datum_size(arg2);
	if (len2 > len1)
		result = false;
	else
	{
		text	   *targ1 = text_substring(arg1, 1, len2, false);
		text	   *targ2 = DatumGetTextPP(arg2);

		result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
						 VARSIZE_ANY_EXHDR(targ2)) == 0);
	}

	return result;
}

/* internal_text_pattern_compare (verbatim modulo shim 2) */
static int
internal_text_pattern_compare(text *arg1, text *arg2)
{
	int			result;
	int			len1,
				len2;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	result = pg_ref_memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	if (result != 0)
		return result;
	else if (len1 < len2)
		return -1;
	else if (len1 > len2)
		return 1;
	else
		return 0;
}

/* appendStringInfoText (varlena.c, verbatim) */
static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
}

/* replace_text (verbatim modulo shim 1) */
static text *
pg_replace_text(text *src_text, text *from_sub_text, text *to_sub_text,
				Oid collid)
{
	int			src_text_len;
	int			from_sub_text_len;
	TextPositionState state;
	text	   *ret_text;
	int			chunk_len;
	char	   *curr_ptr;
	char	   *start_ptr;
	StringInfoData str;
	bool		found;

	src_text_len = VARSIZE_ANY_EXHDR(src_text);
	from_sub_text_len = VARSIZE_ANY_EXHDR(from_sub_text);

	/* Return unmodified source string if empty source or pattern */
	if (src_text_len < 1 || from_sub_text_len < 1)
	{
		return src_text;
	}

	text_position_setup(src_text, from_sub_text, collid, &state);

	found = text_position_next(&state);

	/* When the from_sub_text is not found, there is nothing to do. */
	if (!found)
	{
		text_position_cleanup(&state);
		return src_text;
	}
	curr_ptr = text_position_get_match_ptr(&state);
	start_ptr = VARDATA_ANY(src_text);

	initStringInfo(&str);

	do
	{
		CHECK_FOR_INTERRUPTS();

		/* copy the data skipped over by last text_position_next() */
		chunk_len = curr_ptr - start_ptr;
		appendBinaryStringInfo(&str, start_ptr, chunk_len);

		appendStringInfoText(&str, to_sub_text);

		start_ptr = curr_ptr + state.last_match_len;

		found = text_position_next(&state);
		if (found)
			curr_ptr = text_position_get_match_ptr(&state);
	}
	while (found);

	/* copy trailing data */
	chunk_len = ((char *) src_text + VARSIZE_ANY(src_text)) - start_ptr;
	appendBinaryStringInfo(&str, start_ptr, chunk_len);

	text_position_cleanup(&state);

	ret_text = cstring_to_text_with_len(str.data, str.len);
	pfree(str.data);

	return ret_text;
}

/* split_part (verbatim modulo shim 1) */
static text *
pg_split_part(text *inputstring, text *fldsep, int fldnum, Oid collid)
{
	int			inputstring_len;
	int			fldsep_len;
	TextPositionState state;
	char	   *start_ptr;
	char	   *end_ptr;
	text	   *result_text;
	bool		found;

	/* field number is 1 based */
	if (fldnum == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field position must not be zero")));

	inputstring_len = VARSIZE_ANY_EXHDR(inputstring);
	fldsep_len = VARSIZE_ANY_EXHDR(fldsep);

	/* return empty string for empty input string */
	if (inputstring_len < 1)
		return cstring_to_text("");

	/* handle empty field separator */
	if (fldsep_len < 1)
	{
		/* if first or last field, return input string, else empty string */
		if (fldnum == 1 || fldnum == -1)
			return inputstring;
		else
			return cstring_to_text("");
	}

	/* find the first field separator */
	text_position_setup(inputstring, fldsep, collid, &state);

	found = text_position_next(&state);

	/* special case if fldsep not found at all */
	if (!found)
	{
		text_position_cleanup(&state);
		/* if first or last field, return input string, else empty string */
		if (fldnum == 1 || fldnum == -1)
			return inputstring;
		else
			return cstring_to_text("");
	}

	/*
	 * take care of a negative field number (i.e. count from the right) by
	 * converting to a positive field number; we need total number of fields
	 */
	if (fldnum < 0)
	{
		/* we found a fldsep, so there are at least two fields */
		int			numfields = 2;

		while (text_position_next(&state))
			numfields++;

		/* special case of last field does not require an extra pass */
		if (fldnum == -1)
		{
			start_ptr = text_position_get_match_ptr(&state) + state.last_match_len;
			end_ptr = VARDATA_ANY(inputstring) + inputstring_len;
			text_position_cleanup(&state);
			return cstring_to_text_with_len(start_ptr,
											end_ptr - start_ptr);
		}

		/* else, convert fldnum to positive notation */
		fldnum += numfields + 1;

		/* if nonexistent field, return empty string */
		if (fldnum <= 0)
		{
			text_position_cleanup(&state);
			return cstring_to_text("");
		}

		/* reset to pointing at first match, but now with positive fldnum */
		text_position_reset(&state);
		found = text_position_next(&state);
		Assert(found);
	}

	/* identify bounds of first field */
	start_ptr = VARDATA_ANY(inputstring);
	end_ptr = text_position_get_match_ptr(&state);

	while (found && --fldnum > 0)
	{
		/* identify bounds of next field */
		start_ptr = end_ptr + state.last_match_len;
		found = text_position_next(&state);
		if (found)
			end_ptr = text_position_get_match_ptr(&state);
	}

	text_position_cleanup(&state);

	if (fldnum > 0)
	{
		/* N'th field separator not found */
		/* if last field requested, return it, else empty string */
		if (fldnum == 1)
		{
			int			last_len = start_ptr - VARDATA_ANY(inputstring);

			result_text = cstring_to_text_with_len(start_ptr,
												   inputstring_len - last_len);
		}
		else
			result_text = cstring_to_text("");
	}
	else
	{
		/* non-last field requested */
		result_text = cstring_to_text_with_len(start_ptr, end_ptr - start_ptr);
	}

	return result_text;
}

/* ====== SECTION 7: hashfunc.c hashtext/hashtextextended (VERBATIM) ====== */

/* hashtext (verbatim modulo shim 1; nondeterministic arm dead: C pin) */
static uint32
pg_hashtext(text *key, Oid collid)
{
	pg_locale_t mylocale;
	uint32		result;

	if (!collid)
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string hashing"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->deterministic)
	{
		result = hash_any((unsigned char *) VARDATA_ANY(key),
						  VARSIZE_ANY_EXHDR(key));
	}
	else
	{
		abort();				/* pg_strnxfrm arm: dead under the C pin */
	}

	return result;
}

/* hashtextextended (verbatim modulo shim 1; same fence) */
static uint64
pg_hashtextextended(text *key, Oid collid, uint64 seed)
{
	pg_locale_t mylocale;
	uint64		result;

	if (!collid)
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string hashing"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));

	mylocale = pg_newlocale_from_collation(collid);

	if (mylocale->deterministic)
	{
		result = hash_any_extended((unsigned char *) VARDATA_ANY(key),
								   VARSIZE_ANY_EXHDR(key),
								   seed);
	}
	else
	{
		abort();				/* pg_strnxfrm arm: dead under the C pin */
	}

	return result;
}

/* ========== SECTION 8: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * Entry conventions: every entry FIRST calls pg_diff_arena_reset() (models
 * PG's memory-context reset), clears pg_diff_errcode, and arms the setjmp
 * error trampoline. Return 0 = Ok (outputs written), 1 = error (class in
 * pg_diff_errcode via pg_diff_errcode_get()). Text arguments come in as
 * (ptr,len) and are rebuilt as plain 4B varlena images in the arena
 * (shim 1). Results are copied into caller buffers: the Rust driver sizes
 * them from its own payload caps; outcap overflow aborts loudly (shim bug,
 * not a comparison plane).
 */

#define PG_DIFF_VLTEXT_GUARD() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_vltext_jb)) \
			return 1; \
	} while (0)

static text *
pg_vltext_make_text(const unsigned char *p, int len)
{
	return cstring_to_text_with_len((const char *) p, len);
}

static void
pg_vltext_copy_out(const void *src, int len, unsigned char *out, int outcap,
				   int *outlen)
{
	if (len > outcap)
		abort();				/* driver sizing bug, not a plane */
	memcpy(out, src, len);
	*outlen = len;
}

/* textin: cstring_to_text over a NUL-terminated cstring [oid 46] */
int
pg_diff_vltext_textin(const char *s, unsigned char *out, int outcap,
					  int *outlen)
{
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = cstring_to_text(s);
	pg_vltext_copy_out(VARDATA(r), VARSIZE(r) - VARHDRSZ, out, outcap, outlen);
	return 0;
}

/* textout: text_to_cstring; out gets len bytes + NUL, *outlen = len [oid 47] */
int
pg_diff_vltext_textout(const unsigned char *t, int len, unsigned char *out,
					   int outcap, int *outlen)
{
	char	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = text_to_cstring(pg_vltext_make_text(t, len));
	pg_vltext_copy_out(r, len + 1, out, outcap, outlen);
	*outlen = len;				/* payload length; out[len] is the NUL */
	return 0;
}

/* textlen [oid 1257] */
int
pg_diff_vltext_textlen(const unsigned char *t, int len, int *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = text_length(PointerGetDatum(pg_vltext_make_text(t, len)));
	return 0;
}

/* textoctetlen [oid 1374] */
int
pg_diff_vltext_textoctetlen(const unsigned char *t, int len, int *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = toast_raw_datum_size(PointerGetDatum(pg_vltext_make_text(t, len))) - VARHDRSZ;
	return 0;
}

/* textcat [oid 1258] */
int
pg_diff_vltext_textcat(const unsigned char *t1, int l1,
					   const unsigned char *t2, int l2,
					   unsigned char *out, int outcap, int *outlen)
{
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = text_catenate(pg_vltext_make_text(t1, l1), pg_vltext_make_text(t2, l2));
	pg_vltext_copy_out(VARDATA(r), VARSIZE(r) - VARHDRSZ, out, outcap, outlen);
	return 0;
}

/* text_substr [oid 877] / text_substr_no_len [oid 883] (no_len != 0) */
int
pg_diff_vltext_substr(const unsigned char *t, int len, int start, int length,
					  int no_len, unsigned char *out, int outcap, int *outlen)
{
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = text_substring(PointerGetDatum(pg_vltext_make_text(t, len)),
					   start, no_len ? -1 : length, no_len ? true : false);
	pg_vltext_copy_out(VARDATA(r), VARSIZE(r) - VARHDRSZ, out, outcap, outlen);
	return 0;
}

/* textpos [oid 849] */
int
pg_diff_vltext_textpos(const unsigned char *t1, int l1,
					   const unsigned char *t2, int l2,
					   unsigned int collid, int *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = text_position(pg_vltext_make_text(t1, l1),
							pg_vltext_make_text(t2, l2), collid);
	return 0;
}

/*
 * texteq/textne/text_lt/text_le/text_gt/text_ge/bttextcmp [oids
 * 67/157/740/741/742/743/360]: op = 0 eq, 1 ne, 2 lt, 3 le, 4 gt, 5 ge,
 * 6 cmp. The lt/le/gt/ge/cmp bodies are one-line text_cmp comparisons in C
 * (verbatim above via text_cmp/varstr_cmp); eq/ne go through the vendored
 * deterministic fast arms.
 */
int
pg_diff_vltext_cmpop(int op, const unsigned char *t1, int l1,
					 const unsigned char *t2, int l2,
					 unsigned int collid, int *result)
{
	text	   *a;
	text	   *b;

	PG_DIFF_VLTEXT_GUARD();
	a = pg_vltext_make_text(t1, l1);
	b = pg_vltext_make_text(t2, l2);
	switch (op)
	{
		case 0:
			*result = pg_texteq(PointerGetDatum(a), PointerGetDatum(b), collid);
			break;
		case 1:
			*result = pg_textne(PointerGetDatum(a), PointerGetDatum(b), collid);
			break;
		case 2:
			*result = (text_cmp(a, b, collid) < 0);
			break;
		case 3:
			*result = (text_cmp(a, b, collid) <= 0);
			break;
		case 4:
			*result = (text_cmp(a, b, collid) > 0);
			break;
		case 5:
			*result = (text_cmp(a, b, collid) >= 0);
			break;
		case 6:
			*result = text_cmp(a, b, collid);
			break;
		default:
			abort();
	}
	return 0;
}

/*
 * text_larger [oid 458] / text_smaller [oid 459]: C returns one of the
 * argument pointers; *which = 1 if arg1 was returned, 2 if arg2.
 */
int
pg_diff_vltext_minmax(int larger, const unsigned char *t1, int l1,
					  const unsigned char *t2, int l2,
					  unsigned int collid, int *which)
{
	text	   *a;
	text	   *b;
	text	   *result;

	PG_DIFF_VLTEXT_GUARD();
	a = pg_vltext_make_text(t1, l1);
	b = pg_vltext_make_text(t2, l2);
	if (larger)
		result = ((text_cmp(a, b, collid) > 0) ? a : b);
	else
		result = ((text_cmp(a, b, collid) < 0) ? a : b);
	*which = (result == a) ? 1 : 2;
	return 0;
}

/*
 * text_pattern_lt/le/ge/gt [oids 2160/2161/2163/2164] and bttext_pattern_cmp
 * [oid 2166]: op = 0 lt, 1 le, 2 ge, 3 gt, 4 cmp (raw magnitude).
 */
int
pg_diff_vltext_patcmp(int op, const unsigned char *t1, int l1,
					  const unsigned char *t2, int l2, int *result)
{
	text	   *a;
	text	   *b;
	int			cmp;

	PG_DIFF_VLTEXT_GUARD();
	a = pg_vltext_make_text(t1, l1);
	b = pg_vltext_make_text(t2, l2);
	cmp = internal_text_pattern_compare(a, b);
	switch (op)
	{
		case 0:
			*result = (cmp < 0);
			break;
		case 1:
			*result = (cmp <= 0);
			break;
		case 2:
			*result = (cmp >= 0);
			break;
		case 3:
			*result = (cmp > 0);
			break;
		case 4:
			*result = cmp;
			break;
		default:
			abort();
	}
	return 0;
}

/* btvarstrequalimage [oid 5050] (verbatim body inline: locale->deterministic) */
int
pg_diff_vltext_btvarstrequalimage(unsigned int collid, int *result)
{
	pg_locale_t locale;

	PG_DIFF_VLTEXT_GUARD();
	check_collation_set(collid);
	locale = pg_newlocale_from_collation(collid);
	*result = locale->deterministic;
	return 0;
}

/* text_starts_with [oid 3696] */
int
pg_diff_vltext_starts_with(const unsigned char *t1, int l1,
						   const unsigned char *t2, int l2,
						   unsigned int collid, int *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = pg_text_starts_with(PointerGetDatum(pg_vltext_make_text(t1, l1)),
								  PointerGetDatum(pg_vltext_make_text(t2, l2)),
								  collid);
	return 0;
}

/* replace_text [oid 2087] */
int
pg_diff_vltext_replace_text(const unsigned char *src, int lsrc,
							const unsigned char *from, int lfrom,
							const unsigned char *to, int lto,
							unsigned int collid,
							unsigned char *out, int outcap, int *outlen)
{
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = pg_replace_text(pg_vltext_make_text(src, lsrc),
						pg_vltext_make_text(from, lfrom),
						pg_vltext_make_text(to, lto),
						collid);
	pg_vltext_copy_out(VARDATA_ANY(r), VARSIZE_ANY_EXHDR(r), out, outcap, outlen);
	return 0;
}

/* split_part [oid 2088] */
int
pg_diff_vltext_split_part(const unsigned char *str, int lstr,
						  const unsigned char *sep, int lsep,
						  int fldnum, unsigned int collid,
						  unsigned char *out, int outcap, int *outlen)
{
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	r = pg_split_part(pg_vltext_make_text(str, lstr),
					  pg_vltext_make_text(sep, lsep), fldnum, collid);
	pg_vltext_copy_out(VARDATA_ANY(r), VARSIZE_ANY_EXHDR(r), out, outcap, outlen);
	return 0;
}

/*
 * textoverlay [oid 1404] / textoverlay_no_len [oid 1405] (no_len != 0:
 * sl = text_length(t2) as in the verbatim wrapper).
 */
int
pg_diff_vltext_overlay(const unsigned char *t1, int l1,
					   const unsigned char *t2, int l2,
					   int sp, int sl, int no_len,
					   unsigned char *out, int outcap, int *outlen)
{
	text	   *a;
	text	   *b;
	text	   *r;

	PG_DIFF_VLTEXT_GUARD();
	a = pg_vltext_make_text(t1, l1);
	b = pg_vltext_make_text(t2, l2);
	if (no_len)
		sl = text_length(PointerGetDatum(b));	/* defaults to length(t2) */
	r = text_overlay(a, b, sp, sl);
	pg_vltext_copy_out(VARDATA_ANY(r), VARSIZE_ANY_EXHDR(r), out, outcap, outlen);
	return 0;
}

/* textsend [oid 2415]: out gets the FULL bytea wire image (4B header + data) */
int
pg_diff_vltext_textsend(const unsigned char *t, int len, unsigned char *out,
						int outcap, int *outlen)
{
	text	   *arg;
	StringInfoData buf;
	bytea	   *r;

	PG_DIFF_VLTEXT_GUARD();
	arg = pg_vltext_make_text(t, len);
	pq_begintypsend(&buf);
	pq_sendtext(&buf, VARDATA_ANY(arg), VARSIZE_ANY_EXHDR(arg));
	r = pq_endtypsend(&buf);
	pg_vltext_copy_out(r, VARSIZE(r), out, outcap, outlen);
	return 0;
}

/* textrecv [oid 2414]: data/len = the wire message; out gets the text payload */
int
pg_diff_vltext_textrecv(const unsigned char *data, int len, unsigned char *out,
						int outcap, int *outlen)
{
	StringInfoData buf;
	text	   *result;
	char	   *str;
	int			nbytes;

	PG_DIFF_VLTEXT_GUARD();
	/* model the caller's StringInfo wire buffer ({ptr,len}, cursor 0) */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, data, len);

	str = pq_getmsgtext(&buf, buf.len - buf.cursor, &nbytes);
	result = cstring_to_text_with_len(str, nbytes);
	pfree(str);
	pg_vltext_copy_out(VARDATA(result), VARSIZE(result) - VARHDRSZ, out, outcap, outlen);
	return 0;
}

/* unknownin [oid 109] / unknownout [oid 110]: pstrdup of a cstring */
int
pg_diff_vltext_unknowninout(const char *s, unsigned char *out, int outcap,
							int *outlen)
{
	int			len;
	char	   *r;

	PG_DIFF_VLTEXT_GUARD();
	/* pstrdup (representation is same as cstring) */
	len = (int) strlen(s);
	r = (char *) palloc(len + 1);
	memcpy(r, s, len + 1);
	pg_vltext_copy_out(r, len + 1, out, outcap, outlen);
	*outlen = len;				/* payload length; out[len] is the NUL */
	return 0;
}

/* unknownrecv [oid 2416]: like textrecv but result is the cstring itself */
int
pg_diff_vltext_unknownrecv(const unsigned char *data, int len,
						   unsigned char *out, int outcap, int *outlen)
{
	StringInfoData buf;
	char	   *str;
	int			nbytes;

	PG_DIFF_VLTEXT_GUARD();
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, data, len);

	str = pq_getmsgtext(&buf, buf.len - buf.cursor, &nbytes);
	/* representation is same as cstring */
	pg_vltext_copy_out(str, nbytes + 1, out, outcap, outlen);
	*outlen = nbytes;			/* payload length; out[nbytes] is the NUL */
	return 0;
}

/* unknownsend [oid 2417]: cstring -> bytea wire image (strlen payload) */
int
pg_diff_vltext_unknownsend(const char *s, unsigned char *out, int outcap,
						   int *outlen)
{
	StringInfoData buf;
	bytea	   *r;

	PG_DIFF_VLTEXT_GUARD();
	pq_begintypsend(&buf);
	pq_sendtext(&buf, s, strlen(s));
	r = pq_endtypsend(&buf);
	pg_vltext_copy_out(r, VARSIZE(r), out, outcap, outlen);
	return 0;
}

/* hashtext [oid 400] */
int
pg_diff_vltext_hashtext(const unsigned char *t, int len, unsigned int collid,
						uint32_t *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = pg_hashtext(pg_vltext_make_text(t, len), collid);
	return 0;
}

/* hashtextextended [oid 448] */
int
pg_diff_vltext_hashtextextended(const unsigned char *t, int len,
								unsigned int collid, uint64_t seed,
								uint64_t *result)
{
	PG_DIFF_VLTEXT_GUARD();
	*result = pg_hashtextextended(pg_vltext_make_text(t, len), collid, seed);
	return 0;
}
