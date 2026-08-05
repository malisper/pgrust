/*
 * pg_oraclefam_io.c: vendored PostgreSQL C oracle for the oraclefam_diff
 * differential fuzz target (100%-coverage campaign; crate
 * crates/backend/utils/adt/oracle_compat).
 *
 * Provenance (bodies VERBATIM unless a shim is listed below), all from
 * postgres-src 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (PostgreSQL 18.3,
 * "Stamp 18.3"; re-verified against ../pgrust-fabled/vendor/postgres-src):
 *   - src/backend/utils/adt/oracle_compat.c: lpad, rpad, dotrim,
 *     dobyteatrim, translate, ascii, chr, repeat (the 2-arg/1-arg trim SQL
 *     wrappers btrim/ltrim/rtrim/btrim1/... are dotrim flag spellings; the
 *     driver entries pass the same flag/set combinations the wrappers do).
 *   - src/backend/utils/adt/varlena.c: cstring_to_text,
 *     cstring_to_text_with_len, text_substring, pg_mbcharcliplen_chars,
 *     text_left, text_right, text_reverse.
 *   - src/backend/utils/adt/formatting.c: asc_tolower, asc_toupper,
 *     asc_initcap (the C-collation case kernels; the str_* locale dispatch
 *     is the lane's carved-OUT surface).
 *   - src/backend/utils/mb/mbutils.c: pg_mblen_range, pg_mblen_with_len,
 *     pg_mblen_unbounded, pg_mbstrlen_with_len, pg_mbcharcliplen, cliplen.
 *   - src/common/wchar.c: pg_utf_mblen, pg_utf8_islegal.
 *   - src/port/pgstrcasecmp.c: pg_ascii_toupper, pg_ascii_tolower.
 *   - src/backend/utils/mmgr/mcxt.c: pnstrdup.
 *   - src/include/common/int.h: pg_add_s32_overflow, pg_mul_s32_overflow.
 *   - src/include/utils/memutils.h: MaxAllocSize, AllocSizeIsValid.
 *
 * Shims (PLUMBING ONLY, never logic):
 *   - fmgr PG_FUNCTION_ARGS unwrapped to plain C signatures over already
 *     detoasted text / bytea pointers (PG_GETARG_TEXT_PP -> parameter,
 *     PG_RETURN_TEXT_P -> return; text_left's PG_GETARG_DATUM(0) ->
 *     PointerGetDatum(str): the harness only ever builds plain
 *     4B-uncompressed varlenas).
 *   - varlena macros for plain little-endian 4B-uncompressed values only
 *     (the only kind the harness constructs): VARDATA_ANY == VARDATA,
 *     VARSIZE_ANY_EXHDR == VARSIZE - VARHDRSZ, VARATT_IS_COMPRESSED /
 *     VARATT_IS_EXTERNAL == false.
 *   - DatumGetTextPSlice: detoast_attr_slice reduced to its plain-varlena
 *     arm (src/backend/access/common/detoast.c: sliceoffset clamped to the
 *     value length, slicelength < 0 => "to end", result a fresh palloc'd
 *     copy). Toast plumbing, not computation.
 *   - ENCODING ENVIRONMENT (the key seam, mocked identically on the Rust
 *     side via mbutils::SetDatabaseEncoding): a TLS encoding cell set by
 *     every driver entry; GetDatabaseEncoding reads it;
 *     pg_encoding_max_length returns the pg_wchar_table maxmblen for the
 *     three supported encodings (SQL_ASCII=1, LATIN1=1, UTF8=4 — wchar.c
 *     table rows); pg_wchar_table[...].mblen(x) dispatch is spelled
 *     pg_db_mblen(x), returning the verbatim pg_utf_mblen for UTF8 and 1
 *     for the single-byte encodings (pg_ascii_mblen/pg_latin1_mblen are
 *     `return 1` in wchar.c).
 *   - ereport(ERROR, (errcode(E), errmsg(...))) -> record the errcode
 *     class in the shared TLS pg_diff_errcode and longjmp back to the
 *     driver entry (models PG's error longjmp; no garbage value is ever
 *     compared). errmsg / errdetail evaluate to 0 with arguments unused.
 *     report_invalid_encoding_db -> class 3 (22021), same longjmp.
 *   - palloc/palloc0/pfree -> the TLS pointer arena below (models PG's
 *     memory-context reset; error-path longjmps cannot leak).
 *   - Assert -> ((void) 0) (release C compiles Assert out; bars must be
 *     release-effective per the debug-assert masking law), Valgrind client
 *     macros -> no-ops, CHECK_FOR_INTERRUPTS -> no-op (environment).
 *
 * Errcode classes (pg_diff_errcode values):
 *   1 = 54000 ERRCODE_PROGRAM_LIMIT_EXCEEDED
 *   2 = 22023 ERRCODE_INVALID_PARAMETER_VALUE
 *   3 = 22021 ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (invalid byte sequence)
 *   4 = 22011 ERRCODE_SUBSTRING_ERROR (defined for completeness; not
 *       reachable through text_left(n>=0) since length_not_specified=false
 *       and n>=0)
 */

#include <assert.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

#define PG_DIFF_ERR_PROGRAM_LIMIT 1
#define PG_DIFF_ERR_INVALID_PARAM 2
#define PG_DIFF_ERR_BAD_ENCODING 3
#define PG_DIFF_ERR_SUBSTRING 4
#define PG_DIFF_ERR_INTERNAL 99

/* ---------------- fixed-width typedefs (c.h, LP64) ---------------- */

typedef int32_t int32;
typedef uint32_t uint32;
typedef int64_t int64;
typedef size_t Size;
typedef uint32_t Oid;
typedef uintptr_t Datum;

#define PG_INT32_MAX INT32_MAX
#define PG_INT32_MIN INT32_MIN
#define HAVE__BUILTIN_OP_OVERFLOW 1

#define Assert(x) ((void) 0)
#define unlikely(x) (x)
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define VALGRIND_CHECK_MEM_IS_DEFINED(a, b) ((void) 0)
#define CHECK_FOR_INTERRUPTS() ((void) 0)

#define DatumGetPointer(X) ((char *) (X))
#define PointerGetDatum(X) ((Datum) (X))

/* varlena: plain little-endian 4B-uncompressed only (see header shims) */
typedef struct varlena
{
	uint32		vl_len_;
	char		vl_dat[];
} text;
typedef text bytea;

#define VARHDRSZ ((int32) sizeof(uint32))
#define SET_VARSIZE(PTR, len) (((text *) (PTR))->vl_len_ = ((uint32) (len)) << 2)
#define VARSIZE(PTR) ((int32) (((text *) (PTR))->vl_len_ >> 2))
#define VARDATA(PTR) (((text *) (PTR))->vl_dat)
#define VARDATA_ANY(PTR) VARDATA(PTR)
#define VARSIZE_ANY_EXHDR(PTR) (VARSIZE(PTR) - VARHDRSZ)
#define VARATT_IS_COMPRESSED(PTR) 0
#define VARATT_IS_EXTERNAL(PTR) 0

/* ---------------- error shims (see header) ---------------- */

static _Thread_local jmp_buf pg_diff_oc_jmp;
static _Thread_local int pg_diff_oc_pending;

static void
pg_diff_oc_raise(int class_)
{
	pg_diff_errcode = class_;
	longjmp(pg_diff_oc_jmp, 1);
}

#define ERROR 21
#define errcode(e) (pg_diff_oc_pending = (e), 0)
#define errmsg(...) 0
#define ereport(level, rest) \
	do { (void) (rest); pg_diff_oc_raise(pg_diff_oc_pending); } while (0)
#define elog(level, ...) pg_diff_oc_raise(PG_DIFF_ERR_INTERNAL)

#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT
#define ERRCODE_INVALID_PARAMETER_VALUE PG_DIFF_ERR_INVALID_PARAM
#define ERRCODE_SUBSTRING_ERROR PG_DIFF_ERR_SUBSTRING

/* ---------------- palloc arena shim (see header) ---------------- */

#define PG_DIFF_ARENA_MAX 4096
static _Thread_local void *pg_diff_oc_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_oc_arena_n;

static void
pg_diff_oc_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_diff_oc_arena_n; i++)
		free(pg_diff_oc_arena[i]);
	pg_diff_oc_arena_n = 0;
}

static void *
pg_diff_oc_palloc(size_t n)
{
	void	   *p = malloc(n);

	assert(pg_diff_oc_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_oc_arena[pg_diff_oc_arena_n++] = p;
	return p;
}

static void
pg_diff_oc_pfree(void *p)
{
	int			i;

	for (i = 0; i < pg_diff_oc_arena_n; i++)
	{
		if (pg_diff_oc_arena[i] == p)
		{
			free(p);
			pg_diff_oc_arena[i] = pg_diff_oc_arena[--pg_diff_oc_arena_n];
			return;
		}
	}
	assert(!"pfree of a pointer the arena never issued");
	abort();
}

#define palloc(n) pg_diff_oc_palloc(n)
#define pfree(p) pg_diff_oc_pfree(p)

/* ---------------- encoding environment (see header) ---------------- */

#define PG_SQL_ASCII 0
#define PG_EUC_JP 1
#define PG_UTF8 6
#define PG_LATIN1 8

static _Thread_local int pg_diff_oc_enc = PG_SQL_ASCII;

static int
GetDatabaseEncoding(void)
{
	return pg_diff_oc_enc;
}

/* pg_wchar_table maxmblen rows for the supported encodings (wchar.c:
 * SQL_ASCII 1, EUC_JP 3, UTF8 4, LATIN1 1). EUC_JP is pinned ONLY by the
 * non-walking ascii/chr driver entries (they consult max_length and the
 * first byte, never an mblen walk); the pad/trim/translate family is never
 * routed through it — pg_db_mblen below stays a {single-byte, UTF8}
 * dispatch. */
static int
pg_encoding_max_length(int encoding)
{
	return encoding == PG_UTF8 ? 4 : encoding == PG_EUC_JP ? 3 : 1;
}

static int
pg_database_encoding_max_length(void)
{
	return pg_encoding_max_length(pg_diff_oc_enc);
}

/* ======== src/common/wchar.c pg_utf_mblen — VERBATIM ======== */

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
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

/*
 * pg_wchar_table[DatabaseEncoding->encoding].mblen(x) dispatch for the
 * three supported encodings (pg_ascii_mblen / pg_latin1_mblen are
 * `return 1` in wchar.c) — environment shim, see header.
 */
static int
pg_db_mblen(const unsigned char *s)
{
	return pg_diff_oc_enc == PG_UTF8 ? pg_utf_mblen(s) : 1;
}

/* ======== src/common/wchar.c pg_utf8_islegal — VERBATIM ======== */

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

/* ======== src/include/common/int.h — VERBATIM ======== */

static inline bool
pg_add_s32_overflow(int32 a, int32 b, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_add_overflow(a, b, result);
#else
	int64		res = (int64) a + (int64) b;

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
#endif
}

static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_mul_overflow(a, b, result);
#else
	int64		res = (int64) a * (int64) b;

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = (int32) res;
	return false;
#endif
}

/* ======== src/include/utils/memutils.h — VERBATIM ======== */

#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

/* ======== src/port/pgstrcasecmp.c — VERBATIM ======== */

/*
 * Fold a character to upper case, following C/POSIX locale rules.
 */
static unsigned char
pg_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

/*
 * Fold a character to lower case, following C/POSIX locale rules.
 */
static unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

/* ======== src/backend/utils/mmgr/mcxt.c pnstrdup — VERBATIM ======== */

static char *
pnstrdup(const char *in, Size len)
{
	char	   *out;

	len = strnlen(in, len);

	out = palloc(len + 1);
	memcpy(out, in, len);
	out[len] = '\0';

	return out;
}

/* ======== src/backend/utils/mb/mbutils.c — VERBATIM ========
 * (pg_wchar_table[DatabaseEncoding->encoding].mblen -> pg_db_mblen, the
 * documented environment shim; report_invalid_encoding_db -> class-3 raise)
 */

static void
report_invalid_encoding_db(const char *mbstr, int mblen, int len)
{
	(void) mbstr;
	(void) mblen;
	(void) len;
	pg_diff_oc_raise(PG_DIFF_ERR_BAD_ENCODING);
}

static int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_db_mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

static int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_db_mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

static int
pg_mblen_unbounded(const char *mbstr)
{
	int			length = pg_db_mblen((const unsigned char *) mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

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

/* mbcliplen for any single-byte encoding */
static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

static int
pg_mbcharcliplen(const char *mbstr, int len, int limit)
{
	int			clen = 0;
	int			nch = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return cliplen(mbstr, len, limit);

	while (len > 0 && *mbstr)
	{
		l = pg_mblen_with_len(mbstr, len);
		nch++;
		if (nch > limit)
			break;
		clen += l;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/* ======== src/backend/utils/adt/varlena.c helpers — VERBATIM ======== */

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

/*
 * DatumGetTextPSlice: detoast_attr_slice reduced to its plain-varlena arm
 * (see header shims). Toast plumbing, not computation.
 */
static text *
DatumGetTextPSlice(Datum str, int32 first, int32 count)
{
	text	   *t = (text *) DatumGetPointer(str);
	int32		len = VARSIZE_ANY_EXHDR(t);

	if (first >= len)
	{
		first = len;
		count = 0;
	}
	else if (count < 0 || count > len - first)
		count = len - first;
	return cstring_to_text_with_len(VARDATA_ANY(t) + first, count);
}

/* ======== src/backend/utils/adt/varlena.c pg_mbcharcliplen_chars — VERBATIM ======== */

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

/* ======== src/backend/utils/adt/varlena.c text_substring — VERBATIM
 * (declaration is `static text *text_substring(Datum str, ...)` upstream
 * too; only DatumGetTextPSlice behind it is the documented shim) ======== */

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

/* ======== src/backend/utils/adt/formatting.c asc_* — VERBATIM ======== */

static char *
asc_tolower(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
		*p = pg_ascii_tolower((unsigned char) *p);

	return result;
}

static char *
asc_toupper(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
		*p = pg_ascii_toupper((unsigned char) *p);

	return result;
}

static char *
asc_initcap(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;
	int			wasalnum = false;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
	{
		char		c;

		if (wasalnum)
			*p = c = pg_ascii_tolower((unsigned char) *p);
		else
			*p = c = pg_ascii_toupper((unsigned char) *p);
		/* we don't trust isalnum() here */
		wasalnum = ((c >= 'A' && c <= 'Z') ||
					(c >= 'a' && c <= 'z') ||
					(c >= '0' && c <= '9'));
	}

	return result;
}

/* ======== src/backend/utils/adt/oracle_compat.c — VERBATIM
 * (fmgr unwrapped to plain signatures; see header shims) ======== */

static text *
pg_oc_lpad(text *string1, int32 len, text *string2)
{
	text	   *ret;
	char	   *ptr1,
			   *ptr2,
			   *ptr2start,
			   *ptr_ret;
	const char *ptr2end;
	int			m,
				s1len,
				s2len;
	int			bytelen;

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = VARSIZE_ANY_EXHDR(string1);
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = VARSIZE_ANY_EXHDR(string2);
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	/* compute worst-case output length */
	if (unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), len,
									 &bytelen)) ||
		unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen)) ||
		unlikely(!AllocSizeIsValid(bytelen)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	ret = (text *) palloc(bytelen);

	m = len - s1len;

	ptr2 = ptr2start = VARDATA_ANY(string2);
	ptr2end = ptr2 + s2len;
	ptr_ret = VARDATA(ret);

	while (m--)
	{
		int			mlen = pg_mblen_range(ptr2, ptr2end);

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	ptr1 = VARDATA_ANY(string1);

	while (s1len--)
	{
		int			mlen = pg_mblen_unbounded(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	return ret;
}

static text *
pg_oc_rpad(text *string1, int32 len, text *string2)
{
	text	   *ret;
	char	   *ptr1,
			   *ptr2,
			   *ptr2start,
			   *ptr_ret;
	const char *ptr2end;
	int			m,
				s1len,
				s2len;
	int			bytelen;

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = VARSIZE_ANY_EXHDR(string1);
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = VARSIZE_ANY_EXHDR(string2);
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	/* compute worst-case output length */
	if (unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), len,
									 &bytelen)) ||
		unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen)) ||
		unlikely(!AllocSizeIsValid(bytelen)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	ret = (text *) palloc(bytelen);

	m = len - s1len;

	ptr1 = VARDATA_ANY(string1);

	ptr_ret = VARDATA(ret);

	while (s1len--)
	{
		int			mlen = pg_mblen_unbounded(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	ptr2 = ptr2start = VARDATA_ANY(string2);
	ptr2end = ptr2 + s2len;

	while (m--)
	{
		int			mlen = pg_mblen_range(ptr2, ptr2end);

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	return ret;
}

/*
 * Common implementation for btrim, ltrim, rtrim
 */
static text *
dotrim(const char *string, int stringlen,
	   const char *set, int setlen,
	   bool doltrim, bool dortrim)
{
	int			i;

	/* Nothing to do if either string or set is empty */
	if (stringlen > 0 && setlen > 0)
	{
		if (pg_database_encoding_max_length() > 1)
		{
			/*
			 * In the multibyte-encoding case, build arrays of pointers to
			 * character starts, so that we can avoid inefficient checks in
			 * the inner loops.
			 */
			const char **stringchars;
			const char **setchars;
			const char *setend;
			int		   *stringmblen;
			int		   *setmblen;
			int			stringnchars;
			int			setnchars;
			int			resultndx;
			int			resultnchars;
			const char *p;
			const char *pend;
			int			len;
			int			mblen;
			const char *str_pos;
			int			str_len;

			stringchars = (const char **) palloc(stringlen * sizeof(char *));
			stringmblen = (int *) palloc(stringlen * sizeof(int));
			stringnchars = 0;
			p = string;
			len = stringlen;
			pend = p + len;
			while (len > 0)
			{
				stringchars[stringnchars] = p;
				stringmblen[stringnchars] = mblen = pg_mblen_range(p, pend);
				stringnchars++;
				p += mblen;
				len -= mblen;
			}

			setchars = (const char **) palloc(setlen * sizeof(char *));
			setmblen = (int *) palloc(setlen * sizeof(int));
			setnchars = 0;
			p = set;
			len = setlen;
			setend = set + setlen;
			while (len > 0)
			{
				setchars[setnchars] = p;
				setmblen[setnchars] = mblen = pg_mblen_range(p, setend);
				setnchars++;
				p += mblen;
				len -= mblen;
			}

			resultndx = 0;		/* index in stringchars[] */
			resultnchars = stringnchars;

			if (doltrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx];
					str_len = stringmblen[resultndx];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					string += str_len;
					stringlen -= str_len;
					resultndx++;
					resultnchars--;
				}
			}

			if (dortrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx + resultnchars - 1];
					str_len = stringmblen[resultndx + resultnchars - 1];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					stringlen -= str_len;
					resultnchars--;
				}
			}

			pfree(stringchars);
			pfree(stringmblen);
			pfree(setchars);
			pfree(setmblen);
		}
		else
		{
			/*
			 * In the single-byte-encoding case, we don't need such overhead.
			 */
			if (doltrim)
			{
				while (stringlen > 0)
				{
					char		str_ch = *string;

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					string++;
					stringlen--;
				}
			}

			if (dortrim)
			{
				while (stringlen > 0)
				{
					char		str_ch = string[stringlen - 1];

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					stringlen--;
				}
			}
		}
	}

	/* Return selected portion of string */
	return cstring_to_text_with_len(string, stringlen);
}

/*
 * Common implementation for bytea versions of btrim, ltrim, rtrim
 */
static bytea *
dobyteatrim(bytea *string, bytea *set, bool doltrim, bool dortrim)
{
	bytea	   *ret;
	char	   *ptr,
			   *end,
			   *ptr2,
			   *ptr2start,
			   *end2;
	int			m,
				stringlen,
				setlen;

	stringlen = VARSIZE_ANY_EXHDR(string);
	setlen = VARSIZE_ANY_EXHDR(set);

	if (stringlen <= 0 || setlen <= 0)
		return string;

	m = stringlen;
	ptr = VARDATA_ANY(string);
	end = ptr + stringlen - 1;
	ptr2start = VARDATA_ANY(set);
	end2 = ptr2start + setlen - 1;

	if (doltrim)
	{
		while (m > 0)
		{
			ptr2 = ptr2start;
			while (ptr2 <= end2)
			{
				if (*ptr == *ptr2)
					break;
				++ptr2;
			}
			if (ptr2 > end2)
				break;
			ptr++;
			m--;
		}
	}

	if (dortrim)
	{
		while (m > 0)
		{
			ptr2 = ptr2start;
			while (ptr2 <= end2)
			{
				if (*end == *ptr2)
					break;
				++ptr2;
			}
			if (ptr2 > end2)
				break;
			end--;
			m--;
		}
	}

	ret = (bytea *) palloc(VARHDRSZ + m);
	SET_VARSIZE(ret, VARHDRSZ + m);
	memcpy(VARDATA(ret), ptr, m);
	return ret;
}

static text *
pg_oc_translate(text *string, text *from, text *to)
{
	text	   *result;
	char	   *from_ptr,
			   *to_ptr,
			   *to_end;
	char	   *source,
			   *target;
	const char *source_end;
	const char *from_end;
	int			m,
				fromlen,
				tolen,
				retlen,
				i;
	int			bytelen;
	int			len;
	int			source_len;
	int			from_index;

	m = VARSIZE_ANY_EXHDR(string);
	if (m <= 0)
		return string;
	source = VARDATA_ANY(string);
	source_end = source + m;

	fromlen = VARSIZE_ANY_EXHDR(from);
	from_ptr = VARDATA_ANY(from);
	from_end = from_ptr + fromlen;
	tolen = VARSIZE_ANY_EXHDR(to);
	to_ptr = VARDATA_ANY(to);
	to_end = to_ptr + tolen;

	/*
	 * The worst-case expansion is to substitute a max-length character for a
	 * single-byte character at each position of the string.
	 */
	if (unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), m,
									 &bytelen)) ||
		unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen)) ||
		unlikely(!AllocSizeIsValid(bytelen)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	result = (text *) palloc(bytelen);

	target = VARDATA(result);
	retlen = 0;

	while (m > 0)
	{
		source_len = pg_mblen_range(source, source_end);
		from_index = 0;

		for (i = 0; i < fromlen; i += len)
		{
			len = pg_mblen_range(&from_ptr[i], from_end);
			if (len == source_len &&
				memcmp(source, &from_ptr[i], len) == 0)
				break;

			from_index++;
		}
		if (i < fromlen)
		{
			/* substitute, or delete if no corresponding "to" character */
			char	   *p = to_ptr;

			for (i = 0; i < from_index; i++)
			{
				if (p >= to_end)
					break;
				p += pg_mblen_range(p, to_end);
			}
			if (p < to_end)
			{
				len = pg_mblen_range(p, to_end);
				memcpy(target, p, len);
				target += len;
				retlen += len;
			}
		}
		else
		{
			/* no match, so copy */
			memcpy(target, source, source_len);
			target += source_len;
			retlen += source_len;
		}

		source += source_len;
		m -= source_len;
	}

	SET_VARSIZE(result, retlen + VARHDRSZ);

	/*
	 * The function result is probably much bigger than needed, if we're using
	 * a multibyte encoding, but it's not worth reallocating it; the result
	 * probably won't live long anyway.
	 */

	return result;
}

static int32
pg_oc_ascii(text *string)
{
	int			encoding = GetDatabaseEncoding();
	unsigned char *data;

	if (VARSIZE_ANY_EXHDR(string) <= 0)
		return 0;

	data = (unsigned char *) VARDATA_ANY(string);

	if (encoding == PG_UTF8 && *data > 127)
	{
		/* return the code point for Unicode */

		int			result = 0,
					tbytes = 0,
					i;

		if (*data >= 0xF0)
		{
			result = *data & 0x07;
			tbytes = 3;
		}
		else if (*data >= 0xE0)
		{
			result = *data & 0x0F;
			tbytes = 2;
		}
		else
		{
			Assert(*data > 0xC0);
			result = *data & 0x1f;
			tbytes = 1;
		}

		Assert(tbytes > 0);

		for (i = 1; i <= tbytes; i++)
		{
			Assert((data[i] & 0xC0) == 0x80);
			result = (result << 6) + (data[i] & 0x3f);
		}

		return result;
	}
	else
	{
		if (pg_encoding_max_length(encoding) > 1 && *data > 127)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large")));


		return (int32) *data;
	}
}

static text *
pg_oc_chr(int32 arg)
{
	uint32		cvalue;
	text	   *result;
	int			encoding = GetDatabaseEncoding();

	/*
	 * Error out on arguments that make no sense or that we can't validly
	 * represent in the encoding.
	 */
	if (arg < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("character number must be positive")));
	else if (arg == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("null character not permitted")));

	cvalue = arg;

	if (encoding == PG_UTF8 && cvalue > 127)
	{
		/* for Unicode we treat the argument as a code point */
		int			bytes;
		unsigned char *wch;

		/*
		 * We only allow valid Unicode code points; per RFC3629 that stops at
		 * U+10FFFF, even though 4-byte UTF8 sequences can hold values up to
		 * U+1FFFFF.
		 */
		if (cvalue > 0x0010ffff)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large for encoding: %u",
							cvalue)));

		if (cvalue > 0xffff)
			bytes = 4;
		else if (cvalue > 0x07ff)
			bytes = 3;
		else
			bytes = 2;

		result = (text *) palloc(VARHDRSZ + bytes);
		SET_VARSIZE(result, VARHDRSZ + bytes);
		wch = (unsigned char *) VARDATA(result);

		if (bytes == 2)
		{
			wch[0] = 0xC0 | ((cvalue >> 6) & 0x1F);
			wch[1] = 0x80 | (cvalue & 0x3F);
		}
		else if (bytes == 3)
		{
			wch[0] = 0xE0 | ((cvalue >> 12) & 0x0F);
			wch[1] = 0x80 | ((cvalue >> 6) & 0x3F);
			wch[2] = 0x80 | (cvalue & 0x3F);
		}
		else
		{
			wch[0] = 0xF0 | ((cvalue >> 18) & 0x07);
			wch[1] = 0x80 | ((cvalue >> 12) & 0x3F);
			wch[2] = 0x80 | ((cvalue >> 6) & 0x3F);
			wch[3] = 0x80 | (cvalue & 0x3F);
		}

		/*
		 * The preceding range check isn't sufficient, because UTF8 excludes
		 * Unicode "surrogate pair" codes.  Make sure what we created is valid
		 * UTF8.
		 */
		if (!pg_utf8_islegal(wch, bytes))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character not valid for encoding: %u",
							cvalue)));
	}
	else
	{
		bool		is_mb;

		is_mb = pg_encoding_max_length(encoding) > 1;

		if ((is_mb && (cvalue > 127)) || (!is_mb && (cvalue > 255)))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large for encoding: %u",
							cvalue)));

		result = (text *) palloc(VARHDRSZ + 1);
		SET_VARSIZE(result, VARHDRSZ + 1);
		*VARDATA(result) = (char) cvalue;
	}

	return result;
}

static text *
pg_oc_repeat(text *string, int32 count)
{
	text	   *result;
	int			slen,
				tlen;
	int			i;
	char	   *cp,
			   *sp;

	if (count < 0)
		count = 0;

	slen = VARSIZE_ANY_EXHDR(string);

	if (unlikely(pg_mul_s32_overflow(count, slen, &tlen)) ||
		unlikely(pg_add_s32_overflow(tlen, VARHDRSZ, &tlen)) ||
		unlikely(!AllocSizeIsValid(tlen)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	result = (text *) palloc(tlen);

	SET_VARSIZE(result, tlen);
	cp = VARDATA(result);
	sp = VARDATA_ANY(string);
	for (i = 0; i < count; i++)
	{
		memcpy(cp, sp, slen);
		cp += slen;
		CHECK_FOR_INTERRUPTS();
	}

	return result;
}

/* ======== src/backend/utils/adt/varlena.c text_left/right/reverse —
 * VERBATIM (fmgr unwrapped; see header shims) ======== */

static text *
pg_oc_text_left(text *str, int32 n)
{
	if (n < 0)
	{
		const char *p = VARDATA_ANY(str);
		int			len = VARSIZE_ANY_EXHDR(str);
		int			rlen;

		n = pg_mbstrlen_with_len(p, len) + n;
		rlen = pg_mbcharcliplen(p, len, n);
		return cstring_to_text_with_len(p, rlen);
	}
	else
		return text_substring(PointerGetDatum(str), 1, n, false);
}

static text *
pg_oc_text_right(text *str, int32 n)
{
	const char *p = VARDATA_ANY(str);
	int			len = VARSIZE_ANY_EXHDR(str);
	int			off;

	if (n < 0)
		n = -n;
	else
		n = pg_mbstrlen_with_len(p, len) - n;
	off = pg_mbcharcliplen(p, len, n);

	return cstring_to_text_with_len(p + off, len - off);
}

static text *
pg_oc_text_reverse(text *str)
{
	const char *p = VARDATA_ANY(str);
	int			len = VARSIZE_ANY_EXHDR(str);
	const char *endp = p + len;
	text	   *result;
	char	   *dst;

	result = palloc(len + VARHDRSZ);
	dst = (char *) VARDATA(result) + len;
	SET_VARSIZE(result, len + VARHDRSZ);

	if (pg_database_encoding_max_length() > 1)
	{
		/* multibyte version */
		while (p < endp)
		{
			int			sz;

			sz = pg_mblen_range(p, endp);
			dst -= sz;
			memcpy(dst, p, sz);
			p += sz;
		}
	}
	else
	{
		/* single byte version */
		while (p < endp)
			*(--dst) = *p++;
	}

	return result;
}

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

/*
 * Every entry: arena reset, errcode reset, TLS encoding pin, setjmp; on
 * ereport-longjmp return the errcode class. Results are copied into
 * caller-provided buffers (caller sizes them from the same clamped inputs
 * both sides receive; the asserts are harness plumbing, never an oracle).
 */

static text *
pg_diff_oc_make_text(const char *buf, int len)
{
	return cstring_to_text_with_len(buf, len);
}

#define PG_DIFF_OC_ENTRY(enc) \
	do { \
		pg_diff_oc_arena_reset(); \
		pg_diff_errcode = 0; \
		pg_diff_oc_enc = (enc); \
		if (setjmp(pg_diff_oc_jmp) != 0) \
			return pg_diff_errcode; \
	} while (0)

static int
pg_diff_oc_copyout(text *ret, char *out, int32 *outlen)
{
	*outlen = VARSIZE_ANY_EXHDR(ret);
	memcpy(out, VARDATA_ANY(ret), *outlen);
	return 0;
}

/* which: 0 = asc_tolower, 1 = asc_toupper, 2 = asc_initcap.
 * out must hold nbytes + 1 (NUL-terminated cstring result); *outlen gets
 * strlen(result) — the pnstrdup/first-NUL truncation IS the compared
 * contract. Encoding-independent (byte-wise kernels) but the encoding is
 * still pinned for uniformity. */
int
pg_diff_oc_case(int which, int enc, const char *buf, int32 nbytes,
				char *out, int32 *outlen)
{
	char	   *r;

	PG_DIFF_OC_ENTRY(enc);
	r = which == 0 ? asc_tolower(buf, nbytes) :
		which == 1 ? asc_toupper(buf, nbytes) :
		asc_initcap(buf, nbytes);
	*outlen = (int32) strlen(r);
	memcpy(out, r, *outlen + 1);
	return 0;
}

/* left != 0 => lpad, else rpad. out sized by the caller from the clamped
 * len (worst case 4*len + 4 bytes). */
int
pg_diff_oc_pad(int left, int enc, const char *s1, int32 l1, int32 len,
			   const char *s2, int32 l2, char *out, int32 *outlen)
{
	text	   *t1,
			   *t2,
			   *ret;

	PG_DIFF_OC_ENTRY(enc);
	t1 = pg_diff_oc_make_text(s1, l1);
	t2 = pg_diff_oc_make_text(s2, l2);
	ret = left ? pg_oc_lpad(t1, len, t2) : pg_oc_rpad(t1, len, t2);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* doltrim/dortrim exactly as the btrim/ltrim/rtrim(1) SQL wrappers pass
 * them. out sized >= slen. */
int
pg_diff_oc_trim(int enc, const char *s, int32 slen, const char *set,
				int32 setlen, int doltrim, int dortrim,
				char *out, int32 *outlen)
{
	text	   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ret = dotrim(s, slen, set, setlen, doltrim != 0, dortrim != 0);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* bytea trim family (encoding-independent). out sized >= slen. */
int
pg_diff_oc_byteatrim(const char *s, int32 slen, const char *set,
					 int32 setlen, int doltrim, int dortrim,
					 char *out, int32 *outlen)
{
	bytea	   *bs,
			   *bset,
			   *ret;

	PG_DIFF_OC_ENTRY(PG_SQL_ASCII);
	bs = pg_diff_oc_make_text(s, slen);
	bset = pg_diff_oc_make_text(set, setlen);
	ret = dobyteatrim(bs, bset, doltrim != 0, dortrim != 0);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* out sized >= 4*slen + 4 (worst-case expansion). */
int
pg_diff_oc_translate(int enc, const char *s, int32 slen, const char *from,
					 int32 fromlen, const char *to, int32 tolen,
					 char *out, int32 *outlen)
{
	text	   *ts,
			   *tf,
			   *tt,
			   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ts = pg_diff_oc_make_text(s, slen);
	tf = pg_diff_oc_make_text(from, fromlen);
	tt = pg_diff_oc_make_text(to, tolen);
	ret = pg_oc_translate(ts, tf, tt);
	return pg_diff_oc_copyout(ret, out, outlen);
}

int
pg_diff_oc_ascii(int enc, const char *s, int32 slen, int32 *result)
{
	text	   *ts;

	PG_DIFF_OC_ENTRY(enc);
	ts = pg_diff_oc_make_text(s, slen);
	*result = pg_oc_ascii(ts);
	return 0;
}

/* out sized >= 4. */
int
pg_diff_oc_chr(int enc, int32 arg, char *out, int32 *outlen)
{
	text	   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ret = pg_oc_chr(arg);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* out sized by the caller from the clamped count (count * slen bytes). */
int
pg_diff_oc_repeat(int enc, const char *s, int32 slen, int32 count,
				  char *out, int32 *outlen)
{
	text	   *ts,
			   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ts = pg_diff_oc_make_text(s, slen);
	ret = pg_oc_repeat(ts, count);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* which: 0 = text_left, 1 = text_right. out sized >= tlen. */
int
pg_diff_oc_text_leftright(int which, int enc, const char *t, int32 tlen,
						  int32 n, char *out, int32 *outlen)
{
	text	   *ts,
			   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ts = pg_diff_oc_make_text(t, tlen);
	ret = which == 0 ? pg_oc_text_left(ts, n) : pg_oc_text_right(ts, n);
	return pg_diff_oc_copyout(ret, out, outlen);
}

/* out sized >= tlen. */
int
pg_diff_oc_text_reverse(int enc, const char *t, int32 tlen,
						char *out, int32 *outlen)
{
	text	   *ts,
			   *ret;

	PG_DIFF_OC_ENTRY(enc);
	ts = pg_diff_oc_make_text(t, tlen);
	ret = pg_oc_text_reverse(ts);
	return pg_diff_oc_copyout(ret, out, outlen);
}
