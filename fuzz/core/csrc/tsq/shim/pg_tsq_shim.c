/*
 * pg_tsq_shim.c — shim IMPLEMENTATIONS for the tsq oracle family.
 * NOT PostgreSQL code except where marked VERBATIM (with provenance).
 * See shim/postgres.h for the shim inventory and justifications.
 * All from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0 (18.3).
 */

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/miscnodes.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "executor/spi.h"

#include <stdio.h>
#include <wchar.h>

/* ================= error machinery ================= */

_Thread_local jmp_buf pg_tsq_error_jmp;
_Thread_local int pg_tsq_notice_count;

void
pg_tsq_ereport_finish(int elevel)
{
	if (elevel >= ERROR)
		longjmp(pg_tsq_error_jmp, 1);
	if (elevel == NOTICE)
		pg_tsq_notice_count++;
	/* lower levels: swallowed, exactly like a client with min_messages */
}

/*
 * elog.h errsave_start/errsave_finish reduced: a soft-error context
 * records error_occurred and control returns to the caller; otherwise
 * the error is thrown. The errcode class was already recorded by the
 * errcode() macro in the argument list.
 */
void
pg_tsq_errsave_finish(struct Node *escontext)
{
	if (escontext && IsA(escontext, ErrorSaveContext))
	{
		((ErrorSaveContext *) escontext)->error_occurred = true;
		return;
	}
	longjmp(pg_tsq_error_jmp, 1);
}

/* ================= per-entry bump arena (pg_lsn_oracle.c S3 pattern) ===== */

/*
 * Sized for the driver's input caps: the small arms (<= 2 KiB text/wire)
 * stay well under 1 MiB, and the BULK arm (<= ~1.1 MiB text/wire, driving
 * the MAXSTRPOS / operand-too-long program-limit guards) burns arena at a
 * multiple of the input because this is a BUMP arena — upstream's doubling
 * repallocs (pushValue op pool, polstr) leave their old copies behind, so
 * a ~1 MiB pool costs ~4 MiB, plus item arrays and the built image. 64 MiB
 * gives the bulk arm >8x headroom. Overflow aborts loudly, never silently.
 */
#define PG_TSQ_ARENA_CAP (64u << 20)
static _Thread_local unsigned char pg_tsq_arena[PG_TSQ_ARENA_CAP];
static _Thread_local size_t pg_tsq_arena_used;

void
pg_tsq_arena_reset(void)
{
	pg_tsq_arena_used = 0;
	pg_tsq_notice_count = 0;
}

/* 16-byte size header so repalloc can copy the old allocation */
void *
pg_tsq_arena_alloc(Size n, bool zero)
{
	size_t		need = (n + 15u) & ~(size_t) 15;
	unsigned char *p;

	if (need + 16 > PG_TSQ_ARENA_CAP - pg_tsq_arena_used)
		abort();				/* loud overflow */
	p = pg_tsq_arena + pg_tsq_arena_used;
	*(size_t *) p = n;
	p += 16;
	pg_tsq_arena_used += need + 16;
	if (zero)
		memset(p, 0, n);
	return p;
}

void *
pg_tsq_arena_repalloc(void *ptr, Size n)
{
	size_t		oldsz = *(size_t *) ((unsigned char *) ptr - 16);
	void	   *fresh = pg_tsq_arena_alloc(n, false);

	memcpy(fresh, ptr, oldsz < n ? oldsz : n);
	return fresh;
}

char *
pstrdup(const char *s)
{
	size_t		n = strlen(s) + 1;
	char	   *p = pg_tsq_arena_alloc(n, false);

	memcpy(p, s, n);
	return p;
}

struct varlena *
pg_tsq_detoast_copy(struct varlena *v)
{
	uint32		sz = VARSIZE(v);
	struct varlena *copy = pg_tsq_arena_alloc(sz, false);

	memcpy(copy, v, sz);
	return copy;
}


/* ---- BEGIN VERBATIM src/port/pgstrcasecmp.c:68-95 (pg_strncasecmp) ---- */
int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
{
	while (n-- > 0)
	{
		unsigned char ch1 = (unsigned char) *s1++;
		unsigned char ch2 = (unsigned char) *s2++;

		if (ch1 != ch2)
		{
			if (ch1 >= 'A' && ch1 <= 'Z')
				ch1 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
				ch1 = tolower(ch1);

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
				ch2 = tolower(ch2);

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}
/* ---- END VERBATIM ---- */

/* one flat arena context (utils/memutils.h shim) */
MemoryContext CurrentMemoryContext = NULL;

/* ================= nodes/pg_list.h shim impl ================= */

List *
lcons(void *datum, List *list)
{
	if (list == NIL)
	{
		list = pg_tsq_arena_alloc(sizeof(List), true);
		list->max_length = 8;
		list->elements = pg_tsq_arena_alloc(list->max_length * sizeof(ListCell), false);
		list->length = 0;
	}
	else if (list->length == list->max_length)
	{
		list->max_length *= 2;
		ListCell   *fresh = pg_tsq_arena_alloc(list->max_length * sizeof(ListCell), false);

		memcpy(fresh, list->elements, list->length * sizeof(ListCell));
		list->elements = fresh;
	}
	memmove(&list->elements[1], &list->elements[0],
			list->length * sizeof(ListCell));
	list->elements[0].ptr_value = datum;
	list->length++;
	return list;
}

/* ================= utils/builtins.h text helpers ================= */

text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = pg_tsq_arena_alloc(len + VARHDRSZ, false);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);
	return result;
}

text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, (int) strlen(s));
}

char *
text_to_cstring(const text *t)
{
	int			len = (int) VARSIZE_ANY_EXHDR(t);
	char	   *result = pg_tsq_arena_alloc(len + 1, false);

	memcpy(result, VARDATA_ANY(t), len);
	result[len] = '\0';
	return result;
}

/* ================= mb/pg_wchar.h: UTF-8-pinned pg_mblen family ========== */

/* ---- BEGIN VERBATIM src/common/wchar.c:555-577 (pg_utf_mblen) ---- */
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
/* ---- END VERBATIM ---- */

/* ---- BEGIN VERBATIM src/common/wchar.c:2010-2065 (pg_utf8_islegal) ---- */
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
/* ---- END VERBATIM ---- */

/* mbutils.c report_invalid_encoding_db reduced to the errcode plane */
static void
report_invalid_encoding_db(const char *mbstr, int mblen_, int len)
{
	(void) mbstr;
	(void) mblen_;
	(void) len;
	ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence for encoding")));
}

/*
 * The four wrappers below are the VERBATIM control flow of
 * src/backend/utils/mb/mbutils.c pg_mblen_cstr / pg_mblen_with_len /
 * pg_mblen_unbounded / pg_mblen_range with the encoding pinned to UTF-8
 * (pg_wchar_table[PG_UTF8].mblen == pg_utf_mblen) and the Valgrind
 * bookkeeping dropped.
 */
int
pg_mblen_cstr(const char *mbstr)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	for (int i = 1; i < length; ++i)
		if (unlikely(mbstr[i] == 0))
			report_invalid_encoding_db(mbstr, length, i);

	return length;
}

int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

	return length;
}

int
pg_mblen_unbounded(const char *mbstr)
{
	return pg_utf_mblen((const unsigned char *) mbstr);
}

int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_utf_mblen((const unsigned char *) mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, (int) (end - mbstr));

	return length;
}

int
pg_mblen(const char *mbstr)
{
	return pg_mblen_unbounded(mbstr);
}

/* pg_verify_mbstr(PG_UTF8, ...) reduced to a boolean (driver + pqformat) */
bool
pg_tsq_verify_mbstr_utf8(const char *mbstr, int len)
{
	int			i = 0;

	while (i < len)
	{
		if (((const unsigned char *) mbstr)[i] == 0)
			return false;		/* embedded NUL is invalid in PG strings */
		int			l = pg_utf_mblen((const unsigned char *) (mbstr + i));

		if (i + l > len ||
			!pg_utf8_islegal((const unsigned char *) (mbstr + i), l))
			return false;
		i += l;
	}
	return true;
}

/* ================= utils/pg_locale.h ================= */

/*
 * Driver-settable; default false = a real UTF-8 (non-C-ctype) database,
 * the pgrust fuzz environment default. Must mirror the Rust seam
 * ::pg_locale::database_ctype_is_c.
 */
bool		database_ctype_is_c = false;

void
pg_tsq_set_database_ctype_is_c(bool v)
{
	database_ctype_is_c = v;
}

/*
 * pg_locale.c char2wchar's default-locale arm: mbstowcs. The Rust
 * counterpart (ts_locale/src/public.rs classify()) calls the same libc
 * mbstowcs in the same process — parity by construction. Only the first
 * wchar is ever consumed by the t_is* callers.
 */
size_t
char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen,
		   pg_locale_t locale)
{
	char		buf[8];
	size_t		result;

	(void) locale;
	if (tolen == 0)
		return 0;
	if (fromlen >= sizeof(buf))
		fromlen = sizeof(buf) - 1;
	memcpy(buf, from, fromlen);
	buf[fromlen] = '\0';
	result = mbstowcs(to, buf, tolen);
	if (result == (size_t) -1)
	{
		/* pg_locale.c reports invalid multibyte character; same class */
		to[0] = 0;
		if (tolen > 0)
			to[0] = 0;
	}
	return result;
}

/* ================= lib/stringinfo.h shim impl ================= */

void
initStringInfo(StringInfo str)
{
	str->maxlen = 1024;
	str->data = pg_tsq_arena_alloc(str->maxlen, false);
	str->len = 0;
	str->cursor = 0;
	str->data[0] = '\0';
}

void
appendBinaryStringInfo(StringInfo str, const void *data, int datalen)
{
	while (str->len + datalen + 1 > str->maxlen)
	{
		str->maxlen *= 2;
		char	   *fresh = pg_tsq_arena_alloc(str->maxlen, false);

		memcpy(fresh, str->data, str->len);
		str->data = fresh;
	}
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

/* ================= libpq/pqformat.h shim impl ================= */

void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
}

bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = pg_tsq_arena_alloc(buf->len + VARHDRSZ, false);

	SET_VARSIZE(result, buf->len + VARHDRSZ);
	memcpy(VARDATA(result), buf->data, buf->len);
	return result;
}

void
pq_sendint8(StringInfo buf, uint8 i)
{
	appendBinaryStringInfo(buf, &i, 1);
}

void
pq_sendint16(StringInfo buf, uint16 i)
{
	uint8		b[2] = {(uint8) (i >> 8), (uint8) i};

	appendBinaryStringInfo(buf, b, 2);
}

void
pq_sendint32(StringInfo buf, uint32 i)
{
	uint8		b[4] = {(uint8) (i >> 24), (uint8) (i >> 16),
	(uint8) (i >> 8), (uint8) i};

	appendBinaryStringInfo(buf, b, 4);
}

/* pqformat.c pq_sendstring with client==server encoding: bytes + NUL */
void
pq_sendstring(StringInfo buf, const char *str)
{
	appendBinaryStringInfo(buf, str, (int) strlen(str) + 1);
}

static void
pq_insufficient_data(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("insufficient data left in message")));
}

unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result = 0;

	if (msg->cursor + b > msg->len)
		pq_insufficient_data();
	for (int i = 0; i < b; i++)
		result = (result << 8) | (unsigned char) msg->data[msg->cursor + i];
	msg->cursor += b;
	return result;
}

/*
 * pqformat.c pq_getmsgstring + pg_client_to_server on a same-encoding
 * UTF-8 server: extract to NUL (missing terminator -> protocol violation,
 * as pq_getmsgrawstring), then verify UTF-8 (pg_any_to_server calls
 * pg_verify_mbstr when client == server; failure class 22021).
 */
const char *
pq_getmsgstring(StringInfo msg)
{
	int			start = msg->cursor;
	int			i = start;

	while (i < msg->len && msg->data[i] != '\0')
		i++;
	if (i >= msg->len)
		pq_insufficient_data();
	msg->cursor = i + 1;
	if (!pg_tsq_verify_mbstr_utf8(msg->data + start, i - start))
		ereport(ERROR,
				(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
				 errmsg("invalid byte sequence for encoding")));
	return msg->data + start;
}

/* ================= executor/spi.h LINK-ONLY stubs ================= */

/*
 * Reachable only from tsquery_rewrite_query (oid 3685) — the lane's
 * documented SPI carve. No pg_diff_* entry calls it; abort() loudly if
 * anything ever does.
 */
SPITupleTable *SPI_tuptable = NULL;
uint64		SPI_processed = 0;

int
SPI_connect(void)
{
	abort();
}

int
SPI_finish(void)
{
	abort();
}

SPIPlanPtr
SPI_prepare(const char *src, int nargs, Oid *argtypes)
{
	abort();
}

Portal
SPI_cursor_open(const char *name, SPIPlanPtr plan,
				Datum *Values, const char *Nulls, bool read_only)
{
	abort();
}

void
SPI_cursor_fetch(Portal portal, bool forward, long count)
{
	abort();
}

void
SPI_cursor_close(Portal portal)
{
	abort();
}

int
SPI_freeplan(SPIPlanPtr plan)
{
	abort();
}

void
SPI_freetuptable(SPITupleTable *tuptable)
{
	abort();
}

Datum
SPI_getbinval(HeapTuple row, TupleDesc rowdesc, int colnumber, bool *isnull)
{
	abort();
}

Oid
SPI_gettypeid(TupleDesc rowdesc, int colnumber)
{
	abort();
}
