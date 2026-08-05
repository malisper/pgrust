/*
 * pg_vlbytea_io.c: vendored PostgreSQL C oracle for the vlbytea_diff differential
 * fuzz target (100%-coverage campaign; crate crates/backend/utils/adt/varlena,
 * bytea family).
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/varlena.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3, Stamp-18.3;
 *     re-verified against the repo's vendored ground-truth checkout
 *     ../pgrust-fabled/vendor/postgres-src): byteain, byteaout, bytearecv,
 *     byteasend, byteaoctetlen, byteacat + bytea_catenate,
 *     bytea_substr / bytea_substr_no_len + bytea_substring,
 *     byteaoverlay / byteaoverlay_no_len + bytea_overlay, byteapos,
 *     bytea_bit_count, byteaGetByte, byteaSetByte, byteaGetBit, byteaSetBit,
 *     bytea_reverse, byteaeq, byteane, bytealt, byteale, byteagt, byteage,
 *     byteacmp, bytea_larger, bytea_smaller, bytea_int2, bytea_int4,
 *     bytea_int8, int2_bytea, int4_bytea, int8_bytea.
 *   - src/backend/utils/adt/encode.c @ same ref: hextbl, hexlookup,
 *     hex_encode, get_hex, hex_decode_safe (the codec byteain/byteaout call).
 *   - src/backend/access/hash/hashfunc.c @ same ref: hashvarlena,
 *     hashvarlenaextended, hashbytea, hashbyteaextended (the README function
 *     table lists these rows under varlena.c; the C bodies live in
 *     hashfunc.c and are one-line wrappers over hash_any/_extended).
 *   - src/common/hashfn.c @ same ref: hash_bytes, hash_bytes_extended
 *     (spliced below VERBATIM, mechanically renamed vlbytea_hash_bytes[_extended]
 *     and made static — SYMBOL ISOLATION per core/build.rs: pg_mac_io.c
 *     vendors its own copy of the same kernels; duplicate globals across
 *     lane oracles have cross-bound before).
 *   - src/backend/access/common/detoast.c @ same ref: detoast_attr_slice,
 *     the slicelimit computation + plain-value slicing arm (the only arm a
 *     plain in-memory 4B varlena can take; external/compressed/short arms
 *     are unreachable in this oracle and are not vendored — see shim 3).
 *   - src/port/pg_bitutils.c @ same ref: pg_number_of_ones[256] and the
 *     byte-table tail of pg_popcount_slow (see shim 8).
 *   - src/include/common/int.h @ same ref: pg_add_s32_overflow
 *     (HAVE__BUILTIN_OP_OVERFLOW arm — the arm every production compiler
 *     takes).
 *   - src/backend/utils/mb/mbutils.c + src/common/wchar.c @ same ref:
 *     pg_mblen_range's length/overrun logic + pg_utf_mblen (see shim 6).
 *
 * Shims (PLUMBING ONLY, never logic):
 *   1. fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures over
 *      (const unsigned char *, int) bytea payloads and scalar args;
 *      PG_RETURN_* -> return / caller out-buffers. PG_GETARG_BYTEA_PP /
 *      PG_GETARG_BYTEA_P_COPY -> a plain 4B varlena image built by
 *      pg_vlbytea_mk() (P_COPY's copy semantics = the fresh image).
 *      PG_FREE_IF_COPY dropped (no toast; arena reset owns lifetime).
 *   2. palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (models
 *      PG's memory-context reset; every pg_diff_* entry resets it first).
 *   3. varatt macros (VARHDRSZ/SET_VARSIZE/VARSIZE/VARDATA/VARDATA_ANY/
 *      VARSIZE_ANY_EXHDR): the 4B-uncompressed little-endian arm only —
 *      this oracle constructs exclusively plain 4B images, never short/
 *      compressed/external ones, so VARDATA_ANY==VARDATA etc. by
 *      construction. DatumGetByteaPSlice -> pg_vlbytea_plain_slice(), the
 *      verbatim plain-value arm of detoast_attr_slice (provenance above).
 *      toast_raw_datum_size(d) - VARHDRSZ (byteaoctetlen/byteaeq/byteane)
 *      -> VARSIZE_ANY_EXHDR of the plain image (identical for non-toasted
 *      datums by that function's own definition).
 *   4. ereport(ERROR)/ereturn/elog(ERROR) -> PG_VLBYTEA_ERROR(class):
 *      records the errcode CLASS in the shared _Thread_local pg_diff_errcode
 *      and longjmps out to the pg_diff_* entry, which returns the class.
 *      Message text never crosses the seam. hex_decode_safe is vendored in
 *      its escontext==NULL (hard-error) posture; the Rust driver compares
 *      the soft-error channel against the same verdict+class. Classes:
 *        1 = ERRCODE_INVALID_TEXT_REPRESENTATION  (22P02) byteain escape form
 *        2 = ERRCODE_INVALID_PARAMETER_VALUE      (22023) hex digit / odd
 *            digits / "new bit must be 0 or 1"
 *        3 = ERRCODE_SUBSTRING_ERROR              (22011) negative substring
 *            length (substr L<0, overlay sp<=0)
 *        4 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE   (22003) overlay sp+sl
 *            overflow, bytea_int2/4/8 too-long input
 *        5 = ERRCODE_ARRAY_SUBSCRIPT_ERROR        (2202E) Get/SetByte,
 *            Get/SetBit index out of range
 *        6 = ERRCODE_PROGRAM_LIMIT_EXCEEDED       (54000) byteaout escape
 *            result > MaxAllocSize (unreachable at the driver's 2 KiB cap;
 *            mapped for completeness)
 *        7 = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE  (22021) pg_mblen_range
 *            overrun while rendering the invalid-hex-digit message (shim 6)
 *       99 = internal elog paths that are unreachable by construction
 *            (unrecognized bytea_output, negative sliceoffset); a 99 in a
 *            comparator is itself a divergence finding.
 *   5. bytea_output GUC: modeled as a settable static (pg_diff_byteaout
 *      takes the mode as an argument and assigns it before the verbatim
 *      body). ENVIRONMENT PINNING, not computation mocking: the Rust driver
 *      sets the same enum value via varlena::set_bytea_output so both arms
 *      run under one pinned GUC per exec. BYTEA_OUTPUT_ESCAPE=0 /
 *      BYTEA_OUTPUT_HEX=1 exactly as in vartypes.h and
 *      guc_tables::consts on the Rust side.
 *   6. DATABASE ENCODING FIXED = UTF8 (PostgreSQL's default; the Rust
 *      driver pins mbutils::SetDatabaseEncoding(PG_UTF8), the name_diff
 *      posture). The only encoding-sensitive point in this family is
 *      hex_decode_safe's invalid-digit errmsg, which calls
 *      pg_mblen_range(s, srcend): C raises 22021 (report_invalid_encoding)
 *      if the leading character's mblen overruns the input end, else the
 *      22023 digit error stands. pg_vlbytea_hexdigit_errclass() vendors
 *      exactly that logic (pg_utf_mblen from wchar.c + the mbstr+length>end
 *      check from mbutils.c pg_mblen_range).
 *   7. memcmp -> pg_vlbytea_memcmp: classic unsigned-char byte loop
 *      returning the RAW byte difference at the first mismatch (glibc
 *      convention; the magnitude is SQL-visible through byteacmp, and the
 *      shipped Rust varstrfastcmp_c implements the same convention). Same
 *      shim precedent as pg_name_io.c shim 2.
 *   8. pg_popcount -> the portable byte-table arm (pg_number_of_ones +
 *      pg_popcount_slow's per-byte tail, verbatim). The word-chunked /
 *      POPCNT / NEON arms are value-identical dispatch alternatives by
 *      pg_bitutils.c's own contract; this is a performance choice, not a
 *      semantic one.
 *   9. int2_bytea/int4_bytea/int8_bytea are "can just use intNsend()" in C;
 *      intNsend is pq_sendintN = the big-endian byte image via pqformat.
 *      The oracle writes that image directly (wire triple shim, the
 *      pg_cash_io.c pq_sendint64 precedent).
 *  10. bytearecv's pq_copymsgbytes over (buf->len - buf->cursor): the entry
 *      receives the remaining wire payload directly (cursor position is
 *      pqformat state owned on the Rust side by a real StringInfo).
 */

#include <assert.h>
#include <setjmp.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;

#ifndef Min
#define Min(x, y) ((x) < (y) ? (x) : (y))
#endif
#ifndef Max
#define Max(x, y) ((x) > (y) ? (x) : (y))
#endif
#define BITS_PER_BYTE 8
#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */
#ifndef unlikely
#define unlikely(x) (x)
#endif

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* Errcode classes (see shim 4 in the header). */
#define PG_DIFF_ERR_INVALID_TEXT 1		/* 22P02 */
#define PG_DIFF_ERR_INVALID_PARAM 2		/* 22023 */
#define PG_DIFF_ERR_SUBSTRING 3			/* 22011 */
#define PG_DIFF_ERR_NUM_OOR 4			/* 22003 */
#define PG_DIFF_ERR_ARRAY_SUBSCRIPT 5	/* 2202E */
#define PG_DIFF_ERR_PROGRAM_LIMIT 6		/* 54000 */
#define PG_DIFF_ERR_NOT_IN_REPERTOIRE 7 /* 22021 */
#define PG_DIFF_ERR_INTERNAL 99			/* unreachable elog arms */

static _Thread_local jmp_buf pg_vlbytea_jmp;

#define PG_VLBYTEA_ERROR(cls) \
	do { pg_diff_errcode = (cls); longjmp(pg_vlbytea_jmp, 1); } while (0)

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
#define PG_DIFF_ARENA_MAX 64
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

/* Kept per the arena contract even though the vendored bytea bodies only
 * palloc (scaffold-emitted; unused-function warnings silenced). */
static void * __attribute__((unused))
pg_diff_palloc0_impl(size_t n)
{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}

static void * __attribute__((unused))
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

static void __attribute__((unused))
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

/* ---- shim 3: 4B-uncompressed little-endian varlena arm only ---- */

typedef struct bytea
{
	uint32		vl_len_;
	char		vl_dat[];
} bytea;

#define VARHDRSZ ((int32) sizeof(uint32))
#define SET_VARSIZE(PTR, len) (((bytea *) (PTR))->vl_len_ = ((uint32) (len)) << 2)
#define VARSIZE(PTR) (((bytea *) (PTR))->vl_len_ >> 2)
#define VARDATA(PTR) (((bytea *) (PTR))->vl_dat)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) ((int) VARSIZE(PTR) - VARHDRSZ)

/* Build a plain 4B bytea image from a (ptr,len) payload (shim 1). */
static bytea *
pg_vlbytea_mk(const unsigned char *data, int len)
{
	bytea	   *v = (bytea *) palloc(len + VARHDRSZ);

	SET_VARSIZE(v, len + VARHDRSZ);
	if (len > 0)
		memcpy(VARDATA(v), data, len);
	return v;
}

/* ---- shim 7: raw-difference byte-loop memcmp ---- */

static int
pg_vlbytea_memcmp(const void *s1v, const void *s2v, size_t n)
{
	const unsigned char *s1 = (const unsigned char *) s1v;
	const unsigned char *s2 = (const unsigned char *) s2v;
	size_t		i;

	for (i = 0; i < n; i++)
	{
		if (s1[i] != s2[i])
			return (int) s1[i] - (int) s2[i];
	}
	return 0;
}

#define memcmp(a, b, n) pg_vlbytea_memcmp((a), (b), (n))

/* ---- src/include/common/int.h: pg_add_s32_overflow (verbatim,
 * HAVE__BUILTIN_OP_OVERFLOW arm) ---- */

static inline _Bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
	return __builtin_add_overflow(a, b, result);
}

/* ---- src/common/wchar.c: pg_utf_mblen (verbatim) ---- */

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

/*
 * shim 6: the errcode class of encode.c's invalid-hexadecimal-digit ereport.
 * C renders errmsg("invalid hexadecimal digit: \"%.*s\"",
 * pg_mblen_range(s, srcend), s); pg_mblen_range (mbutils.c, database
 * encoding pinned UTF8) raises report_invalid_encoding -> 22021 when
 * mbstr + length > end, otherwise the 22023 digit error stands. The
 * length/overrun logic below is the verbatim pg_mblen_range core with the
 * mblen table lookup resolved to pg_utf_mblen.
 */
static int
pg_vlbytea_hexdigit_errclass(const char *s, const char *srcend)
{
	int			length = pg_utf_mblen((const unsigned char *) s);

	if (unlikely(s + length > srcend))
		return PG_DIFF_ERR_NOT_IN_REPERTOIRE;
	return PG_DIFF_ERR_INVALID_PARAM;
}

/* ==================== SECTION 1: encode.c hex codec (VERBATIM) =========== */

/*
 * The hex expansion of each possible byte value (two chars per value).
 */
static const char hextbl[512] =
"000102030405060708090a0b0c0d0e0f"
"101112131415161718191a1b1c1d1e1f"
"202122232425262728292a2b2c2d2e2f"
"303132333435363738393a3b3c3d3e3f"
"404142434445464748494a4b4c4d4e4f"
"505152535455565758595a5b5c5d5e5f"
"606162636465666768696a6b6c6d6e6f"
"707172737475767778797a7b7c7d7e7f"
"808182838485868788898a8b8c8d8e8f"
"909192939495969798999a9b9c9d9e9f"
"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
"b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
"c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
"d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
"e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static uint64
hex_encode(const char *src, size_t len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		unsigned char usrc = *((const unsigned char *) src);

		memcpy(dst, &hextbl[2 * usrc], 2);
		src++;
		dst += 2;
	}
	return (uint64) len * 2;
}

static inline _Bool
get_hex(const char *cp, char *out)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = hexlookup[c];

	*out = (char) res;

	return (res >= 0);
}

/*
 * hex_decode_safe (verbatim modulo shims 4/6: escontext==NULL hard-error
 * posture, each ereturn replaced by PG_VLBYTEA_ERROR at the exact program
 * point; the invalid-digit class runs through pg_vlbytea_hexdigit_errclass
 * because the C errmsg's pg_mblen_range call can itself raise 22021).
 */
static uint64
hex_decode_safe(const char *src, size_t len, char *dst)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		if (!get_hex(s, &v1))
			PG_VLBYTEA_ERROR(pg_vlbytea_hexdigit_errclass(s, srcend));
		s++;
		if (s >= srcend)
			PG_VLBYTEA_ERROR(PG_DIFF_ERR_INVALID_PARAM);	/* odd digits */
		if (!get_hex(s, &v2))
			PG_VLBYTEA_ERROR(pg_vlbytea_hexdigit_errclass(s, srcend));
		s++;
		*p++ = (v1 << 4) | v2;
	}

	return p - dst;
}

/* ============ SECTION 2: detoast.c plain-slice arm (VERBATIM lines) ====== */

/*
 * detoast_attr_slice, restricted to the plain (non-external, non-compressed,
 * non-short) argument arm — the only arm reachable for the plain 4B images
 * this oracle constructs (shim 3). The slicelimit computation and the
 * "slicing of datum for compressed cases and plain value" block are verbatim.
 */
static bytea *
pg_vlbytea_plain_slice(bytea *attr, int32 sliceoffset, int32 slicelength)
{
	bytea	   *result;
	char	   *attrdata;
	int32		slicelimit;
	int32		attrsize;

	if (sliceoffset < 0)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_INTERNAL); /* elog "invalid sliceoffset" */

	/*
	 * Compute slicelimit = offset + length, or -1 if we must fetch all of the
	 * value.  In case of integer overflow, we must fetch all.
	 */
	if (slicelength < 0)
		slicelimit = -1;
	else if (pg_add_s32_overflow(sliceoffset, slicelength, &slicelimit))
		slicelength = slicelimit = -1;

	attrdata = VARDATA(attr);
	attrsize = VARSIZE(attr) - VARHDRSZ;

	/* slicing of datum for compressed cases and plain value */

	if (sliceoffset >= attrsize)
	{
		sliceoffset = 0;
		slicelength = 0;
	}
	else if (slicelength < 0 || slicelimit > attrsize)
		slicelength = attrsize - sliceoffset;

	result = (bytea *) palloc(slicelength + VARHDRSZ);
	SET_VARSIZE(result, slicelength + VARHDRSZ);

	memcpy(VARDATA(result), attrdata + sliceoffset, slicelength);

	return result;
}

/* ====== SECTION 3: pg_bitutils.c popcount (VERBATIM table + byte tail) === */

static const uint8 pg_number_of_ones[256] = {
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

/* shim 8: the portable per-byte tail of pg_popcount_slow (verbatim loop). */
static uint64
pg_popcount(const char *buf, int bytes)
{
	uint64		popcnt = 0;

	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/* ======= SECTION 4: hashfn.c hash kernels (VERBATIM, spliced below) ====== */

/* port/pg_bitutils.h pg_rotate_left32 (verbatim) */
static inline uint32
pg_rotate_left32(uint32 word, int n)
{
	return (word << n) | (word >> (32 - n));
}


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
 * hash_bytes() -- hash a variable-length key into a 32-bit value
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
vlbytea_hash_bytes(const unsigned char *k, int keylen)
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
 * hash_bytes_extended() -- hash into a 64-bit value, using an optional seed
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *		seed	: a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to hash_bytes.
 */
static uint64
vlbytea_hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
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


/* ==================== SECTION 5: varlena.c (VERBATIM) ==================== */

#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

/* shim 5: the bytea_output GUC as a settable static (vartypes.h values). */
#define BYTEA_OUTPUT_ESCAPE 0
#define BYTEA_OUTPUT_HEX 1
static _Thread_local int bytea_output = BYTEA_OUTPUT_HEX;

/*
 * byteain (verbatim modulo shims 1/4: cstring arg direct, escontext==NULL
 * hard posture, ereturn -> PG_VLBYTEA_ERROR(22P02 class) at the exact
 * program points).
 */
static bytea *
pg_byteain(char *inputText)
{
	char	   *tp;
	char	   *rp;
	int			bc;
	bytea	   *result;

	/* Recognize hex input */
	if (inputText[0] == '\\' && inputText[1] == 'x')
	{
		size_t		len = strlen(inputText);

		bc = (len - 2) / 2 + VARHDRSZ;	/* maximum possible length */
		result = palloc(bc);
		bc = hex_decode_safe(inputText + 2, len - 2, VARDATA(result));
		SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

		return result;
	}

	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; *tp != '\0'; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			PG_VLBYTEA_ERROR(PG_DIFF_ERR_INVALID_TEXT);
		}
	}

	bc += VARHDRSZ;

	result = (bytea *) palloc(bc);
	SET_VARSIZE(result, bc);

	tp = inputText;
	rp = VARDATA(result);
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			PG_VLBYTEA_ERROR(PG_DIFF_ERR_INVALID_TEXT);
		}
	}

	return result;
}

/*
 * byteaout (verbatim modulo shims 1/4/5: bytea arg direct, ereport ->
 * PG_VLBYTEA_ERROR, GUC static above). Returns the palloc'd cstring.
 */
static char *
pg_byteaout(bytea *vlena)
{
	char	   *result;
	char	   *rp;

	if (bytea_output == BYTEA_OUTPUT_HEX)
	{
		/* Print hex format */
		rp = result = palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
		*rp++ = '\\';
		*rp++ = 'x';
		rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
	}
	else if (bytea_output == BYTEA_OUTPUT_ESCAPE)
	{
		/* Print traditional escaped format */
		char	   *vp;
		uint64		len;
		int			i;

		len = 1;				/* empty string has 1 char */
		vp = VARDATA_ANY(vlena);
		for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
		{
			if (*vp == '\\')
				len += 2;
			else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
				len += 4;
			else
				len++;
		}

		/*
		 * In principle len can't overflow uint32 if the input fit in 1GB, but
		 * for safety let's check rather than relying on palloc's internal
		 * check.
		 */
		if (len > MaxAllocSize)
			PG_VLBYTEA_ERROR(PG_DIFF_ERR_PROGRAM_LIMIT);
		rp = result = (char *) palloc(len);

		vp = VARDATA_ANY(vlena);
		for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
		{
			if (*vp == '\\')
			{
				*rp++ = '\\';
				*rp++ = '\\';
			}
			else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
			{
				int			val;	/* holds unprintable chars */

				val = *vp;
				rp[0] = '\\';
				rp[3] = DIG(val & 07);
				val >>= 3;
				rp[2] = DIG(val & 07);
				val >>= 3;
				rp[1] = DIG(val & 03);
				rp += 4;
			}
			else
				*rp++ = *vp;
		}
	}
	else
	{
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_INTERNAL); /* elog unrecognized setting */
		rp = result = NULL;		/* keep compiler quiet */
	}
	*rp = '\0';
	return result;
}

/*
 * bytearecv core (verbatim value logic modulo shims 1/10: the remaining
 * wire payload arrives directly; pq_copymsgbytes = the memcpy).
 */
static bytea *
pg_bytearecv(const unsigned char *payload, int nbytes)
{
	bytea	   *result;

	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), payload, nbytes);
	return result;
}

/*
 * bytea_catenate (verbatim modulo shim 7's memcmp — not used here — and
 * shim 1).
 */
static bytea *
bytea_catenate(bytea *t1, bytea *t2)
{
	bytea	   *result;
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
	result = (bytea *) palloc(len);

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

/* PG_STR_GET_BYTEA("") = byteain of the empty cstring = the empty bytea
 * (shim 1: built directly; byteain("") provably yields exactly this). */
#define PG_STR_GET_BYTEA_EMPTY() pg_vlbytea_mk((const unsigned char *) "", 0)

/*
 * bytea_substring (verbatim modulo shims 1/3/4: Datum arg -> plain bytea*,
 * DatumGetByteaPSlice -> pg_vlbytea_plain_slice, ereport -> PG_VLBYTEA_ERROR).
 */
static bytea *
bytea_substring(bytea *str,
				int S,
				int L,
				_Bool length_not_specified)
{
	int32		S1;				/* adjusted start position */
	int32		L1;				/* adjusted substring length */
	int32		E;				/* end position */

	/*
	 * The logic here should generally match text_substring().
	 */
	S1 = Max(S, 1);

	if (length_not_specified)
	{
		/*
		 * Not passed a length - DatumGetByteaPSlice() grabs everything to the
		 * end of the string if we pass it a negative value for length.
		 */
		L1 = -1;
	}
	else if (L < 0)
	{
		/* SQL99 says to throw an error for E < S, i.e., negative length */
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_SUBSTRING);
		L1 = -1;				/* silence stupider compilers */
	}
	else if (pg_add_s32_overflow(S, L, &E))
	{
		/*
		 * L could be large enough for S + L to overflow, in which case the
		 * substring must run to end of string.
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
			return PG_STR_GET_BYTEA_EMPTY();

		L1 = E - S1;
	}

	/*
	 * If the start position is past the end of the string, SQL99 says to
	 * return a zero-length string -- DatumGetByteaPSlice() will do that for
	 * us.  We need only convert S1 to zero-based starting position.
	 */
	return pg_vlbytea_plain_slice(str, S1 - 1, L1);
}

/*
 * bytea_overlay (verbatim modulo shims 1/4).
 */
static bytea *
bytea_overlay(bytea *t1, bytea *t2, int sp, int sl)
{
	bytea	   *result;
	bytea	   *s1;
	bytea	   *s2;
	int			sp_pl_sl;

	/*
	 * Check for possible integer-overflow cases.  For negative sp, throw a
	 * "substring length" error because that's what should be expected
	 * according to the spec's definition of OVERLAY().
	 */
	if (sp <= 0)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_SUBSTRING);
	if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_NUM_OOR);

	s1 = bytea_substring(t1, 1, sp - 1, 0);
	s2 = bytea_substring(t1, sp_pl_sl, -1, 1);
	result = bytea_catenate(s1, t2);
	result = bytea_catenate(result, s2);

	return result;
}

/*
 * byteapos (verbatim modulo shims 1/7).
 */
static int32
pg_byteapos(bytea *t1, bytea *t2)
{
	int			pos;
	int			px,
				p;
	int			len1,
				len2;
	char	   *p1,
			   *p2;

	len1 = VARSIZE_ANY_EXHDR(t1);
	len2 = VARSIZE_ANY_EXHDR(t2);

	if (len2 <= 0)
		return 1;				/* result for empty pattern */

	p1 = VARDATA_ANY(t1);
	p2 = VARDATA_ANY(t2);

	pos = 0;
	px = (len1 - len2);
	for (p = 0; p <= px; p++)
	{
		if ((*p2 == *p1) && (memcmp(p1, p2, len2) == 0))
		{
			pos = p + 1;
			break;
		};
		p1++;
	};

	return pos;
}

/*
 * byteaGetByte (verbatim modulo shims 1/4).
 */
static int32
pg_byteaGetByte(bytea *v, int32 n)
{
	int			len;
	int			byte;

	len = VARSIZE_ANY_EXHDR(v);

	if (n < 0 || n >= len)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_ARRAY_SUBSCRIPT);

	byte = ((unsigned char *) VARDATA_ANY(v))[n];

	return byte;
}

/*
 * byteaGetBit (verbatim modulo shims 1/4).
 */
static int32
pg_byteaGetBit(bytea *v, int64 n)
{
	int			byteNo,
				bitNo;
	int			len;
	int			byte;

	len = VARSIZE_ANY_EXHDR(v);

	if (n < 0 || n >= (int64) len * 8)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_ARRAY_SUBSCRIPT);

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	byte = ((unsigned char *) VARDATA_ANY(v))[byteNo];

	if (byte & (1 << bitNo))
		return 1;
	else
		return 0;
}

/*
 * byteaSetByte (verbatim modulo shims 1/4; PG_GETARG_BYTEA_P_COPY's fresh
 * copy is the pg_vlbytea_mk image `res` the entry passes in).
 */
static bytea *
pg_byteaSetByte(bytea *res, int32 n, int32 newByte)
{
	int			len;

	len = VARSIZE(res) - VARHDRSZ;

	if (n < 0 || n >= len)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_ARRAY_SUBSCRIPT);

	/*
	 * Now set the byte.
	 */
	((unsigned char *) VARDATA(res))[n] = newByte;

	return res;
}

/*
 * byteaSetBit (verbatim modulo shims 1/4).
 */
static bytea *
pg_byteaSetBit(bytea *res, int64 n, int32 newBit)
{
	int			len;
	int			oldByte,
				newByte;
	int			byteNo,
				bitNo;

	len = VARSIZE(res) - VARHDRSZ;

	if (n < 0 || n >= (int64) len * 8)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_ARRAY_SUBSCRIPT);

	/* n/8 is now known < len, so safe to cast to int */
	byteNo = (int) (n / 8);
	bitNo = (int) (n % 8);

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_INVALID_PARAM);

	/*
	 * Update the byte.
	 */
	oldByte = ((unsigned char *) VARDATA(res))[byteNo];

	if (newBit == 0)
		newByte = oldByte & (~(1 << bitNo));
	else
		newByte = oldByte | (1 << bitNo);

	((unsigned char *) VARDATA(res))[byteNo] = newByte;

	return res;
}

/*
 * bytea_reverse (verbatim modulo shim 1).
 */
static bytea *
pg_bytea_reverse(bytea *v)
{
	const char *p = VARDATA_ANY(v);
	int			len = VARSIZE_ANY_EXHDR(v);
	const char *endp = p + len;
	bytea	   *result = palloc(len + VARHDRSZ);
	char	   *dst = (char *) VARDATA(result) + len;

	SET_VARSIZE(result, len + VARHDRSZ);

	while (p < endp)
		*(--dst) = *p++;

	return result;
}

/*
 * The bytea comparison family (verbatim modulo shims 1/3/7: the
 * toast_raw_datum_size length fast path in eq/ne = VARSIZE_ANY_EXHDR of the
 * plain images, memcmp = the raw-difference byte loop, PG_FREE_IF_COPY
 * dropped).
 */
static _Bool
pg_byteaeq(bytea *barg1, bytea *barg2)
{
	_Bool		result;
	Size		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = VARSIZE_ANY_EXHDR(barg1) + VARHDRSZ;
	len2 = VARSIZE_ANY_EXHDR(barg2) + VARHDRSZ;
	if (len1 != len2)
		result = 0;
	else
	{
		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) == 0);
	}

	return result;
}

static _Bool
pg_byteane(bytea *barg1, bytea *barg2)
{
	_Bool		result;
	Size		len1,
				len2;

	len1 = VARSIZE_ANY_EXHDR(barg1) + VARHDRSZ;
	len2 = VARSIZE_ANY_EXHDR(barg2) + VARHDRSZ;
	if (len1 != len2)
		result = 1;
	else
	{
		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) != 0);
	}

	return result;
}

static _Bool
pg_bytealt(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	return ((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

static _Bool
pg_byteale(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	return ((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

static _Bool
pg_byteagt(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	return ((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

static _Bool
pg_byteage(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	return ((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

static int32
pg_byteacmp(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	if ((cmp == 0) && (len1 != len2))
		cmp = (len1 < len2) ? -1 : 1;

	return cmp;
}

/* bytea_larger/bytea_smaller (verbatim comparison; the entry reports WHICH
 * argument C returned — 0 for arg1, 1 for arg2 — the value plane). */
static int
pg_bytea_larger_which(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	return ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? 0 : 1);
}

static int
pg_bytea_smaller_which(bytea *arg1, bytea *arg2)
{
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	return ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? 0 : 1);
}

/*
 * bytea_int2/bytea_int4/bytea_int8 (verbatim modulo shims 1/4).
 */
static int16
pg_bytea_int2(bytea *v)
{
	int			len = VARSIZE_ANY_EXHDR(v);
	uint16		result;

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_NUM_OOR);

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= ((unsigned char *) VARDATA_ANY(v))[i];
	}

	return (int16) result;
}

static int32
pg_bytea_int4(bytea *v)
{
	int			len = VARSIZE_ANY_EXHDR(v);
	uint32		result;

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_NUM_OOR);

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= ((unsigned char *) VARDATA_ANY(v))[i];
	}

	return (int32) result;
}

static int64
pg_bytea_int8(bytea *v)
{
	int			len = VARSIZE_ANY_EXHDR(v);
	uint64		result;

	/* Check that the byte array is not too long */
	if (len > (int) sizeof(result))
		PG_VLBYTEA_ERROR(PG_DIFF_ERR_NUM_OOR);

	/* Convert it to an integer; most significant bytes come first */
	result = 0;
	for (int i = 0; i < len; i++)
	{
		result <<= BITS_PER_BYTE;
		result |= ((unsigned char *) VARDATA_ANY(v))[i];
	}

	return (int64) result;
}

/* ========== SECTION 6: fuzz-facing driver entries (NOT Postgres code) ===== */

#define PG_VLBYTEA_ENTRY() \
	do { \
		pg_diff_arena_reset(); \
		pg_diff_errcode = 0; \
		if (setjmp(pg_vlbytea_jmp)) \
			return pg_diff_errcode; \
	} while (0)

/* Copy a bytea result's payload to the caller buffer. */
static void
pg_vlbytea_out(bytea *r, unsigned char *out, int32 *outlen)
{
	*outlen = VARSIZE_ANY_EXHDR(r);
	if (*outlen > 0)
		memcpy(out, VARDATA_ANY(r), *outlen);
}

/* [oid 1244, varlena.c] out cap >= strlen(input). */
int
pg_diff_byteain(const char *input, unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = pg_byteain((char *) input);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 31, varlena.c] out cap >= 4*len + 4 (escape worst case + NUL). */
int
pg_diff_byteaout(const unsigned char *data, int32 len, int mode,
				 unsigned char *out, int32 *outlen)
{
	bytea	   *v;
	char	   *r;

	PG_VLBYTEA_ENTRY();
	bytea_output = mode;		/* shim 5: environment pinning */
	v = pg_vlbytea_mk(data, len);
	r = pg_byteaout(v);
	*outlen = (int32) strlen(r);
	memcpy(out, r, *outlen + 1);	/* include the NUL */
	return 0;
}

/* [oid 2412, varlena.c] out cap >= nbytes. */
int
pg_diff_bytearecv(const unsigned char *payload, int32 nbytes,
				  unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = pg_bytearecv(payload, nbytes);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 2413, varlena.c] "just copy the input"; out cap >= len. */
int
pg_diff_byteasend(const unsigned char *data, int32 len,
				  unsigned char *out, int32 *outlen)
{
	bytea	   *v;

	PG_VLBYTEA_ENTRY();
	v = pg_vlbytea_mk(data, len);	/* PG_GETARG_BYTEA_P_COPY */
	pg_vlbytea_out(v, out, outlen);
	return 0;
}

/* [oid 720, varlena.c] toast_raw_datum_size(str) - VARHDRSZ (shim 3). */
int
pg_diff_byteaoctetlen(const unsigned char *data, int32 len, int32 *out)
{
	bytea	   *v;

	PG_VLBYTEA_ENTRY();
	v = pg_vlbytea_mk(data, len);
	*out = VARSIZE(v) - VARHDRSZ;
	return 0;
}

/* [oid 2011, varlena.c] out cap >= l1 + l2. */
int
pg_diff_byteacat(const unsigned char *d1, int32 l1,
				 const unsigned char *d2, int32 l2,
				 unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = bytea_catenate(pg_vlbytea_mk(d1, l1), pg_vlbytea_mk(d2, l2));
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* Comparison family [oids 1948/1953/1949/1950/1951/1952/1954]. */
#define PG_VLBYTEA_CMP_ENTRY(name, core) \
	int \
	name(const unsigned char *d1, int32 l1, \
		 const unsigned char *d2, int32 l2, int32 *out) \
	{ \
		PG_VLBYTEA_ENTRY(); \
		*out = (int32) core(pg_vlbytea_mk(d1, l1), pg_vlbytea_mk(d2, l2)); \
		return 0; \
	}

PG_VLBYTEA_CMP_ENTRY(pg_diff_byteaeq, pg_byteaeq)
PG_VLBYTEA_CMP_ENTRY(pg_diff_byteane, pg_byteane)
PG_VLBYTEA_CMP_ENTRY(pg_diff_bytealt, pg_bytealt)
PG_VLBYTEA_CMP_ENTRY(pg_diff_byteale, pg_byteale)
PG_VLBYTEA_CMP_ENTRY(pg_diff_byteagt, pg_byteagt)
PG_VLBYTEA_CMP_ENTRY(pg_diff_byteage, pg_byteage)
PG_VLBYTEA_CMP_ENTRY(pg_diff_byteacmp, pg_byteacmp)

/* [oid 6393/6394, varlena.c] *which = 0 if C returned arg1, 1 for arg2. */
PG_VLBYTEA_CMP_ENTRY(pg_diff_bytea_larger, pg_bytea_larger_which)
PG_VLBYTEA_CMP_ENTRY(pg_diff_bytea_smaller, pg_bytea_smaller_which)

/* [oid 721, varlena.c] */
int
pg_diff_byteaGetByte(const unsigned char *data, int32 len, int32 n, int32 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_byteaGetByte(pg_vlbytea_mk(data, len), n);
	return 0;
}

/* [oid 722, varlena.c] out cap >= len. */
int
pg_diff_byteaSetByte(const unsigned char *data, int32 len, int32 n,
					 int32 newByte, unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = pg_byteaSetByte(pg_vlbytea_mk(data, len), n, newByte);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 723, varlena.c] */
int
pg_diff_byteaGetBit(const unsigned char *data, int32 len, int64 n, int32 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_byteaGetBit(pg_vlbytea_mk(data, len), n);
	return 0;
}

/* [oid 724, varlena.c] out cap >= len. */
int
pg_diff_byteaSetBit(const unsigned char *data, int32 len, int64 n,
					int32 newBit, unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = pg_byteaSetBit(pg_vlbytea_mk(data, len), n, newBit);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 2012, varlena.c] out cap >= len. */
int
pg_diff_bytea_substr(const unsigned char *data, int32 len, int32 S, int32 L,
					 unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = bytea_substring(pg_vlbytea_mk(data, len), S, L, 0);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 2013, varlena.c] out cap >= len. */
int
pg_diff_bytea_substr_no_len(const unsigned char *data, int32 len, int32 S,
							unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = bytea_substring(pg_vlbytea_mk(data, len), S, -1, 1);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 749, varlena.c] out cap >= l1 + l2. */
int
pg_diff_byteaoverlay(const unsigned char *d1, int32 l1,
					 const unsigned char *d2, int32 l2,
					 int32 sp, int32 sl,
					 unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = bytea_overlay(pg_vlbytea_mk(d1, l1), pg_vlbytea_mk(d2, l2), sp, sl);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 752, varlena.c] sl defaults to length(t2). */
int
pg_diff_byteaoverlay_no_len(const unsigned char *d1, int32 l1,
							const unsigned char *d2, int32 l2,
							int32 sp,
							unsigned char *out, int32 *outlen)
{
	bytea	   *t1;
	bytea	   *t2;
	bytea	   *r;
	int			sl;

	PG_VLBYTEA_ENTRY();
	t1 = pg_vlbytea_mk(d1, l1);
	t2 = pg_vlbytea_mk(d2, l2);
	sl = VARSIZE_ANY_EXHDR(t2); /* defaults to length(t2) */
	r = bytea_overlay(t1, t2, sp, sl);
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 2014, varlena.c] */
int
pg_diff_byteapos(const unsigned char *d1, int32 l1,
				 const unsigned char *d2, int32 l2, int32 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_byteapos(pg_vlbytea_mk(d1, l1), pg_vlbytea_mk(d2, l2));
	return 0;
}

/* [oid 6163, varlena.c] */
int
pg_diff_bytea_bit_count(const unsigned char *data, int32 len, int64 *out)
{
	bytea	   *t1;

	PG_VLBYTEA_ENTRY();
	t1 = pg_vlbytea_mk(data, len);
	*out = (int64) pg_popcount(VARDATA_ANY(t1), VARSIZE_ANY_EXHDR(t1));
	return 0;
}

/* [oid 6370/6371/6372, varlena.c] */
int
pg_diff_bytea_int2(const unsigned char *data, int32 len, int16 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_bytea_int2(pg_vlbytea_mk(data, len));
	return 0;
}

int
pg_diff_bytea_int4(const unsigned char *data, int32 len, int32 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_bytea_int4(pg_vlbytea_mk(data, len));
	return 0;
}

int
pg_diff_bytea_int8(const unsigned char *data, int32 len, int64 *out)
{
	PG_VLBYTEA_ENTRY();
	*out = pg_bytea_int8(pg_vlbytea_mk(data, len));
	return 0;
}

/* [oid 6367/6368/6369, varlena.c] intN_bytea = intNsend = the big-endian
 * byte image (shim 9). out caps 2/4/8. */
int
pg_diff_int2_bytea(int16 v, unsigned char *out, int32 *outlen)
{
	PG_VLBYTEA_ENTRY();
	out[0] = (unsigned char) (((uint16) v) >> 8);
	out[1] = (unsigned char) v;
	*outlen = 2;
	return 0;
}

int
pg_diff_int4_bytea(int32 v, unsigned char *out, int32 *outlen)
{
	int			i;

	PG_VLBYTEA_ENTRY();
	for (i = 0; i < 4; i++)
		out[i] = (unsigned char) (((uint32) v) >> (8 * (3 - i)));
	*outlen = 4;
	return 0;
}

int
pg_diff_int8_bytea(int64 v, unsigned char *out, int32 *outlen)
{
	int			i;

	PG_VLBYTEA_ENTRY();
	for (i = 0; i < 8; i++)
		out[i] = (unsigned char) (((uint64) v) >> (8 * (7 - i)));
	*outlen = 8;
	return 0;
}

/* [oid 6382, varlena.c] out cap >= len. */
int
pg_diff_bytea_reverse(const unsigned char *data, int32 len,
					  unsigned char *out, int32 *outlen)
{
	bytea	   *r;

	PG_VLBYTEA_ENTRY();
	r = pg_bytea_reverse(pg_vlbytea_mk(data, len));
	pg_vlbytea_out(r, out, outlen);
	return 0;
}

/* [oid 456/772/6413/6414, hashfunc.c] hash_any(_extended) over the payload
 * (VARDATA_ANY/VARSIZE_ANY_EXHDR of the plain image = the payload itself);
 * hashbytea/hashbyteaextended are verbatim aliases of hashvarlena/
 * hashvarlenaextended. These never error. */
uint32
pg_diff_hashvarlena(const unsigned char *data, int32 len)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	return vlbytea_hash_bytes(data, len);
}

uint64
pg_diff_hashvarlenaextended(const unsigned char *data, int32 len, uint64 seed)
{
	pg_diff_arena_reset();
	pg_diff_errcode = 0;
	return vlbytea_hash_bytes_extended(data, len, seed);
}

uint32
pg_diff_hashbytea(const unsigned char *data, int32 len)
{
	return pg_diff_hashvarlena(data, len);
}

uint64
pg_diff_hashbyteaextended(const unsigned char *data, int32 len, uint64 seed)
{
	return pg_diff_hashvarlenaextended(data, len, seed);
}

/*
 * [no oid, encode.c] hex_encode kernel, exposed directly: the shipped
 * varlena::bytea::hex_encode_into is a public helper other crates call
 * (encode, backup/manifest), so it gets its own differential arm rather
 * than being reachable only through byteaout's "\x" prefix path.
 * out cap >= 2*len.
 */
int
pg_diff_vlbytea_hex_encode(const unsigned char *data, int32 len,
						   unsigned char *out, int32 *outlen)
{
	PG_VLBYTEA_ENTRY();
	*outlen = (int32) hex_encode((const char *) data, (size_t) len, (char *) out);
	return 0;
}
