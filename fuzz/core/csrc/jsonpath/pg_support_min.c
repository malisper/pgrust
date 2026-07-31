/*
 * pg_support_min.c — VERBATIM support extracts for the jsonpath_diff oracle,
 * all from postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 * (PostgreSQL 18.3). Every extract carries a "file:A-B VERBATIM" provenance
 * marker (extract_verbatim.py). Sections:
 *   1. src/port/pgstrcasecmp.c — ASCII case helpers.
 *   2. src/backend/libpq/pqformat.c — pq_sendtext/begintypsend/endtypsend/
 *      getmsgint/getmsgbytes/copymsgbytes/getmsgtext.
 *   3. src/backend/utils/adt/json.c — escape_json_char/escape_json/
 *      escape_json_with_len (SIMD path over the verbatim port/simd.h).
 *   4. src/backend/nodes/list.c — new_list/enlarge_list/list_make1_impl/
 *      list_make2_impl/new_tail_cell/lappend (+ pg_nextpower2_32 VERBATIM
 *      from src/include/port/pg_bitutils.h).
 *   5. src/backend/nodes/value.c — makeString.
 *   6. src/common/wchar.c — pg_utf_mblen/pg_utf2wchar_with_len/
 *      pg_utf8_islegal.
 *   7. src/backend/utils/mb/mbutils.c — pg_unicode_to_server(_noerror),
 *      pg_mb2wchar_with_len, pg_mblen_range, pg_mblen_unbounded, pg_mblen.
 *
 * Shims (environment only, never logic — each is listed here):
 *   - ENCODING PIN UTF-8 (mirrors the crate's pin and the Rust harness's
 *     SetDatabaseEncoding(PG_UTF8)): GetDatabaseEncoding() returns PG_UTF8;
 *     DatabaseEncoding/pg_wchar_table are reduced to the UTF-8 row with
 *     exactly the two methods the extracted bodies call (mb2wchar_with_len,
 *     mblen), both bound to the VERBATIM UTF-8 functions of section 6.
 *   - pg_client_to_server/pg_server_to_client: identity (client encoding ==
 *     server encoding UTF-8, the same-encoding fast path of real mbutils).
 *   - report_invalid_encoding_db -> ereport(ERROR, 22021) with the real
 *     message shape (mbutils.c raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
 *     for invalid byte sequences via report_invalid_encoding).
 *   - list.c memory hooks: MemoryContextAlloc(GetMemoryChunkContext(x), sz)
 *     -> palloc(sz) on the TLS arena (single-context model).
 *   - GetDatabaseEncodingName() -> "UTF8" constant (message plumbing only).
 */

#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "port/simd.h"
#include "utils/ascii.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "port/pg_bitutils.h"

/* ================= 1. src/port/pgstrcasecmp.c ================= */

/* ---- pgstrcasecmp.c:32-62 VERBATIM (pg_strcasecmp) ---- */
/*
 * Case-independent comparison of two null-terminated strings.
 */
int
pg_strcasecmp(const char *s1, const char *s2)
{
	for (;;)
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

/* ---- pgstrcasecmp.c:64-95 VERBATIM (pg_strncasecmp) ---- */
/*
 * Case-independent comparison of two not-necessarily-null-terminated strings.
 * At most n bytes will be examined from each string.
 */
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

/* ---- pgstrcasecmp.c:97-112 VERBATIM (pg_toupper) ---- */
/*
 * Fold a character to upper case.
 *
 * Unlike some versions of toupper(), this is safe to apply to characters
 * that aren't lower case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
unsigned char
pg_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	else if (IS_HIGHBIT_SET(ch) && islower(ch))
		ch = toupper(ch);
	return ch;
}

/* ---- pgstrcasecmp.c:114-129 VERBATIM (pg_tolower) ---- */
/*
 * Fold a character to lower case.
 *
 * Unlike some versions of tolower(), this is safe to apply to characters
 * that aren't upper case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
unsigned char
pg_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	else if (IS_HIGHBIT_SET(ch) && isupper(ch))
		ch = tolower(ch);
	return ch;
}

/* ---- pgstrcasecmp.c:131-140 VERBATIM (pg_ascii_toupper) ---- */
/*
 * Fold a character to upper case, following C/POSIX locale rules.
 */
unsigned char
pg_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

/* ---- pgstrcasecmp.c:142-151 VERBATIM (pg_ascii_tolower) ---- */
/*
 * Fold a character to lower case, following C/POSIX locale rules.
 */
unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

/* ================= 2. src/backend/libpq/pqformat.c ================= */

/* forward decls; definitions at the end of this file (they depend on the
 * pinned encoding table + the verbatim verifier chain in section 7) */
static char *pg_client_to_server(const char *s, int len);

/* ---- pqformat.c:161-185 VERBATIM (pq_sendtext) ---- */
/* --------------------------------
 *		pq_sendtext		- append a text string (with conversion)
 *
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.  Note that this is not actually useful
 * for direct frontend transmissions, since there'd be no way for the
 * frontend to determine the string length.  But it is useful for binary
 * format conversions.
 * --------------------------------
 */
void
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

/* ---- pqformat.c:321-334 VERBATIM (pq_begintypsend) ---- */
/* --------------------------------
 *		pq_begintypsend		- initialize for constructing a bytea result
 * --------------------------------
 */
void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* ---- pqformat.c:336-355 VERBATIM (pq_endtypsend) ---- */
/* --------------------------------
 *		pq_endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the palloc'd bytea value.  (We expect
 * that it will be suitably aligned for this because it has been palloc'd.)
 * We assume the StringInfoData is just a local variable in the caller and
 * need not be pfree'd.
 * --------------------------------
 */
bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}

/* ---- pqformat.c:408-442 VERBATIM (pq_getmsgint) ---- */
/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* ---- pqformat.c:500-519 VERBATIM (pq_getmsgbytes) ---- */
/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* ---- pqformat.c:521-536 VERBATIM (pq_copymsgbytes) ---- */
/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
pq_copymsgbytes(StringInfo msg, void *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* ---- pqformat.c:538-569 VERBATIM (pq_getmsgtext) ---- */
/* --------------------------------
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *
 *		Always returns a pointer to a freshly palloc'd result.
 *		The result has a trailing null, *and* we return its strlen in *nbytes.
 * --------------------------------
 */
char *
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

/* ================= 3. src/backend/utils/adt/json.c ================= */

/* ---- json.c:1557-1594 VERBATIM (escape_json_char) ---- */
/*
 * escape_json_char
 *		Inline helper function for escape_json* functions
 */
static pg_attribute_always_inline void
escape_json_char(StringInfo buf, char c)
{
	switch (c)
	{
		case '\b':
			appendStringInfoString(buf, "\\b");
			break;
		case '\f':
			appendStringInfoString(buf, "\\f");
			break;
		case '\n':
			appendStringInfoString(buf, "\\n");
			break;
		case '\r':
			appendStringInfoString(buf, "\\r");
			break;
		case '\t':
			appendStringInfoString(buf, "\\t");
			break;
		case '"':
			appendStringInfoString(buf, "\\\"");
			break;
		case '\\':
			appendStringInfoString(buf, "\\\\");
			break;
		default:
			if ((unsigned char) c < ' ')
				appendStringInfo(buf, "\\u%04x", (int) c);
			else
				appendStringInfoCharMacro(buf, c);
			break;
	}
}

/* ---- json.c:1596-1610 VERBATIM (escape_json) ---- */
/*
 * escape_json
 *		Produce a JSON string literal, properly escaping the NUL-terminated
 *		cstring.
 */
void
escape_json(StringInfo buf, const char *str)
{
	appendStringInfoCharMacro(buf, '"');

	for (; *str != '\0'; str++)
		escape_json_char(buf, *str);

	appendStringInfoCharMacro(buf, '"');
}

/* ---- json.c:1612-1623 VERBATIM ---- */
/*
 * Define the number of bytes that escape_json_with_len will look ahead in the
 * input string before flushing the input string to the destination buffer.
 * Looking ahead too far could result in cachelines being evicted that will
 * need to be reloaded in order to perform the appendBinaryStringInfo call.
 * Smaller values will result in a larger number of calls to
 * appendBinaryStringInfo and introduce additional function call overhead.
 * Values larger than the size of L1d cache will likely result in worse
 * performance.
 */
#define ESCAPE_JSON_FLUSH_AFTER 512

/* ---- json.c:1624-1726 VERBATIM (escape_json_with_len) ---- */
/*
 * escape_json_with_len
 *		Produce a JSON string literal, properly escaping the possibly not
 *		NUL-terminated characters in 'str'.  'len' defines the number of bytes
 *		from 'str' to process.
 */
void
escape_json_with_len(StringInfo buf, const char *str, int len)
{
	int			vlen;

	Assert(len >= 0);

	/*
	 * Since we know the minimum length we'll need to append, let's just
	 * enlarge the buffer now rather than incrementally making more space when
	 * we run out.  Add two extra bytes for the enclosing quotes.
	 */
	enlargeStringInfo(buf, len + 2);

	/*
	 * Figure out how many bytes to process using SIMD.  Round 'len' down to
	 * the previous multiple of sizeof(Vector8), assuming that's a power-of-2.
	 */
	vlen = len & (int) (~(sizeof(Vector8) - 1));

	appendStringInfoCharMacro(buf, '"');

	for (int i = 0, copypos = 0;;)
	{
		/*
		 * To speed this up, try searching sizeof(Vector8) bytes at once for
		 * special characters that we need to escape.  When we find one, we
		 * fall out of the Vector8 loop and copy the portion we've vector
		 * searched and then we process sizeof(Vector8) bytes one byte at a
		 * time.  Once done, come back and try doing vector searching again.
		 * We'll also process any remaining bytes at the tail end of the
		 * string byte-by-byte.  This optimization assumes that most chunks of
		 * sizeof(Vector8) bytes won't contain any special characters.
		 */
		for (; i < vlen; i += sizeof(Vector8))
		{
			Vector8		chunk;

			vector8_load(&chunk, (const uint8 *) &str[i]);

			/*
			 * Break on anything less than ' ' or if we find a '"' or '\\'.
			 * Those need special handling.  That's done in the per-byte loop.
			 */
			if (vector8_has_le(chunk, (unsigned char) 0x1F) ||
				vector8_has(chunk, (unsigned char) '"') ||
				vector8_has(chunk, (unsigned char) '\\'))
				break;

#ifdef ESCAPE_JSON_FLUSH_AFTER

			/*
			 * Flush what's been checked so far out to the destination buffer
			 * every so often to avoid having to re-read cachelines when
			 * escaping large strings.
			 */
			if (i - copypos >= ESCAPE_JSON_FLUSH_AFTER)
			{
				appendBinaryStringInfo(buf, &str[copypos], i - copypos);
				copypos = i;
			}
#endif
		}

		/*
		 * Write to the destination up to the point that we've vector searched
		 * so far.  Do this only when switching into per-byte mode rather than
		 * once every sizeof(Vector8) bytes.
		 */
		if (copypos < i)
		{
			appendBinaryStringInfo(buf, &str[copypos], i - copypos);
			copypos = i;
		}

		/*
		 * Per-byte loop for Vector8s containing special chars and for
		 * processing the tail of the string.
		 */
		for (int b = 0; b < sizeof(Vector8); b++)
		{
			/* check if we've finished */
			if (i == len)
				goto done;

			Assert(i < len);

			escape_json_char(buf, str[i++]);
		}

		copypos = i;
		/* We're not done yet.  Try the vector search again. */
	}

done:
	appendStringInfoCharMacro(buf, '"');
}

/* ================= 4. src/backend/nodes/list.c ================= */

/* shim: single-context model — list cells live on the TLS arena
 * (MemoryContext itself is declared in the shim postgres.h) */
#define GetMemoryChunkContext(pointer) ((MemoryContext) NULL)
#define MemoryContextAlloc(context, sz) palloc(sz)

/* check_list_invariants: assert-only, compiled out (production build) */
#define check_list_invariants(l)  ((void) 0)

/* pg_nextpower2_32 now comes from the shared shim include/port/pg_bitutils.h
 * (verbatim body moved there for the jsonpathexec_diff family) */

/* list.c:47-49 VERBATIM */
#define LIST_HEADER_OVERHEAD  \
	((int) ((offsetof(List, initial_elements) - 1) / sizeof(ListCell) + 1))

/* ---- list.c:83-145 VERBATIM (new_list) ---- */
/*
 * Return a freshly allocated List with room for at least min_size cells.
 *
 * Since empty non-NIL lists are invalid, new_list() sets the initial length
 * to min_size, effectively marking that number of cells as valid; the caller
 * is responsible for filling in their data.
 */
static List *
new_list(NodeTag type, int min_size)
{
	List	   *newlist;
	int			max_size;

	Assert(min_size > 0);

	/*
	 * We allocate all the requested cells, and possibly some more, as part of
	 * the same palloc request as the List header.  This is a big win for the
	 * typical case of short fixed-length lists.  It can lose if we allocate a
	 * moderately long list and then it gets extended; we'll be wasting more
	 * initial_elements[] space than if we'd made the header small.  However,
	 * rounding up the request as we do in the normal code path provides some
	 * defense against small extensions.
	 */

#ifndef DEBUG_LIST_MEMORY_USAGE

	/*
	 * Normally, we set up a list with some extra cells, to allow it to grow
	 * without a repalloc.  Prefer cell counts chosen to make the total
	 * allocation a power-of-2, since palloc would round it up to that anyway.
	 * (That stops being true for very large allocations, but very long lists
	 * are infrequent, so it doesn't seem worth special logic for such cases.)
	 *
	 * The minimum allocation is 8 ListCell units, providing either 4 or 5
	 * available ListCells depending on the machine's word width.  Counting
	 * palloc's overhead, this uses the same amount of space as a one-cell
	 * list did in the old implementation, and less space for any longer list.
	 *
	 * We needn't worry about integer overflow; no caller passes min_size
	 * that's more than twice the size of an existing list, so the size limits
	 * within palloc will ensure that we don't overflow here.
	 */
	max_size = pg_nextpower2_32(Max(8, min_size + LIST_HEADER_OVERHEAD));
	max_size -= LIST_HEADER_OVERHEAD;
#else

	/*
	 * For debugging, don't allow any extra space.  This forces any cell
	 * addition to go through enlarge_list() and thus move the existing data.
	 */
	max_size = min_size;
#endif

	newlist = (List *) palloc(offsetof(List, initial_elements) +
							  max_size * sizeof(ListCell));
	newlist->type = type;
	newlist->length = min_size;
	newlist->max_length = max_size;
	newlist->elements = newlist->initial_elements;

	return newlist;
}

/* ---- list.c:147-229 VERBATIM (enlarge_list) ---- */
/*
 * Enlarge an existing non-NIL List to have room for at least min_size cells.
 *
 * This does *not* update list->length, as some callers would find that
 * inconvenient.  (list->length had better be the correct number of existing
 * valid cells, though.)
 */
static void
enlarge_list(List *list, int min_size)
{
	int			new_max_len;

	Assert(min_size > list->max_length);	/* else we shouldn't be here */

#ifndef DEBUG_LIST_MEMORY_USAGE

	/*
	 * As above, we prefer power-of-two total allocations; but here we need
	 * not account for list header overhead.
	 */

	/* clamp the minimum value to 16, a semi-arbitrary small power of 2 */
	new_max_len = pg_nextpower2_32(Max(16, min_size));

#else
	/* As above, don't allocate anything extra */
	new_max_len = min_size;
#endif

	if (list->elements == list->initial_elements)
	{
		/*
		 * Replace original in-line allocation with a separate palloc block.
		 * Ensure it is in the same memory context as the List header.  (The
		 * previous List implementation did not offer any guarantees about
		 * keeping all list cells in the same context, but it seems reasonable
		 * to create such a guarantee now.)
		 */
		list->elements = (ListCell *)
			MemoryContextAlloc(GetMemoryChunkContext(list),
							   new_max_len * sizeof(ListCell));
		memcpy(list->elements, list->initial_elements,
			   list->length * sizeof(ListCell));

		/*
		 * We must not move the list header, so it's unsafe to try to reclaim
		 * the initial_elements[] space via repalloc.  In debugging builds,
		 * however, we can clear that space and/or mark it inaccessible.
		 * (wipe_mem includes VALGRIND_MAKE_MEM_NOACCESS.)
		 */
#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(list->initial_elements,
				 list->max_length * sizeof(ListCell));
#else
		VALGRIND_MAKE_MEM_NOACCESS(list->initial_elements,
								   list->max_length * sizeof(ListCell));
#endif
	}
	else
	{
#ifndef DEBUG_LIST_MEMORY_USAGE
		/* Normally, let repalloc deal with enlargement */
		list->elements = (ListCell *) repalloc(list->elements,
											   new_max_len * sizeof(ListCell));
#else
		/*
		 * repalloc() might enlarge the space in-place, which we don't want
		 * for debugging purposes, so forcibly move the data somewhere else.
		 */
		ListCell   *newelements;

		newelements = (ListCell *)
			MemoryContextAlloc(GetMemoryChunkContext(list),
							   new_max_len * sizeof(ListCell));
		memcpy(newelements, list->elements,
			   list->length * sizeof(ListCell));
		pfree(list->elements);
		list->elements = newelements;
#endif
	}

	list->max_length = new_max_len;
}

/* ---- list.c:231-243 VERBATIM (list_make1_impl) ---- */
/*
 * Convenience functions to construct short Lists from given values.
 * (These are normally invoked via the list_makeN macros.)
 */
List *
list_make1_impl(NodeTag t, ListCell datum1)
{
	List	   *list = new_list(t, 1);

	list->elements[0] = datum1;
	check_list_invariants(list);
	return list;
}

/* ---- list.c:245-254 VERBATIM (list_make2_impl) ---- */
List *
list_make2_impl(NodeTag t, ListCell datum1, ListCell datum2)
{
	List	   *list = new_list(t, 2);

	list->elements[0] = datum1;
	list->elements[1] = datum2;
	check_list_invariants(list);
	return list;
}

/* ---- list.c:316-329 VERBATIM (new_tail_cell) ---- */
/*
 * Make room for a new tail cell in the given (non-NIL) list.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
	list->length++;
}

/* ---- list.c:331-351 VERBATIM (lappend) ---- */
/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List, 1);
	else
		new_tail_cell(list);

	llast(list) = datum;
	check_list_invariants(list);
	return list;
}

/* ================= 5. src/backend/nodes/value.c ================= */

/* ---- value.c:57-69 VERBATIM (makeString) ---- */
/*
 *	makeString
 *
 * Caller is responsible for passing a palloc'd string.
 */
String *
makeString(char *str)
{
	String	   *v = makeNode(String);

	v->sval = str;
	return v;
}

/* ================= 6. src/common/wchar.c (UTF-8 family) ================= */

/* ---- wchar.c:67 VERBATIM ---- */
#define MB2CHAR_NEED_AT_LEAST(len, need) if ((len) < (need)) break

/* ---- wchar.c:455-515 VERBATIM (pg_utf2wchar_with_len) ---- */
/*
 * convert UTF8 string to pg_wchar (UCS-4)
 * caller must allocate enough space for "to", including a trailing zero!
 * len: length of from.
 * "from" not necessarily null terminated.
 */
static int
pg_utf2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
{
	int			cnt = 0;
	uint32		c1,
				c2,
				c3,
				c4;

	while (len > 0 && *from)
	{
		if ((*from & 0x80) == 0)
		{
			*to = *from++;
			len--;
		}
		else if ((*from & 0xe0) == 0xc0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 2);
			c1 = *from++ & 0x1f;
			c2 = *from++ & 0x3f;
			*to = (c1 << 6) | c2;
			len -= 2;
		}
		else if ((*from & 0xf0) == 0xe0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 3);
			c1 = *from++ & 0x0f;
			c2 = *from++ & 0x3f;
			c3 = *from++ & 0x3f;
			*to = (c1 << 12) | (c2 << 6) | c3;
			len -= 3;
		}
		else if ((*from & 0xf8) == 0xf0)
		{
			MB2CHAR_NEED_AT_LEAST(len, 4);
			c1 = *from++ & 0x07;
			c2 = *from++ & 0x3f;
			c3 = *from++ & 0x3f;
			c4 = *from++ & 0x3f;
			*to = (c1 << 18) | (c2 << 12) | (c3 << 6) | c4;
			len -= 4;
		}
		else
		{
			/* treat a bogus char as length 1; not ours to raise error */
			*to = *from++;
			len--;
		}
		to++;
		cnt++;
	}
	*to = 0;
	return cnt;
}

/* ---- wchar.c:544-577 VERBATIM (pg_utf_mblen) ---- */
/*
 * Return the byte length of a UTF8 character pointed to by s
 *
 * Note: in the current implementation we do not support UTF8 sequences
 * of more than 4 bytes; hence do NOT return a value larger than 4.
 * We return "1" for any leading byte that is either flat-out illegal or
 * indicates a length larger than we support.
 *
 * pg_utf2wchar_with_len(), utf8_to_unicode(), pg_utf8_islegal(), and perhaps
 * other places would need to be fixed to change this.
 */
int
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

/* ---- wchar.c:1996-2065 VERBATIM (pg_utf8_islegal) ---- */
/*
 * Check for validity of a single UTF-8 encoded character
 *
 * This directly implements the rules in RFC3629.  The bizarre-looking
 * restrictions on the second byte are meant to ensure that there isn't
 * more than one encoding of a given Unicode character point; that is,
 * you may not use a longer-than-necessary byte sequence with high order
 * zero bits to represent a character that would fit in fewer bytes.
 * To do otherwise is to create security hazards (eg, create an apparent
 * non-ASCII character that decodes to plain ASCII).
 *
 * length is assumed to have been obtained by pg_utf_mblen(), and the
 * caller must have checked that that many bytes are present in the buffer.
 */
bool
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

/* ================= 7. src/backend/utils/mb/mbutils.c ================= */

/* shim: pinned UTF-8 encoding environment (see header). The struct carries
 * exactly the two methods the extracted bodies dereference. */
typedef struct
{
	int			(*mb2wchar_with_len) (const unsigned char *from, pg_wchar *to, int len);
	int			(*mblen) (const unsigned char *mbstr);
	int			(*mbverifystr) (const unsigned char *mbstr, int len);
} pg_wchar_tbl_shim;

static int	pg_utf8_verifychar(const unsigned char *s, int len);
static int	pg_utf8_verifystr(const unsigned char *s, int len);

static const pg_wchar_tbl_shim pg_wchar_table_shim_rows[] = {
	[PG_UTF8] = {pg_utf2wchar_with_len, pg_utf_mblen, pg_utf8_verifystr},
};
#define pg_wchar_table pg_wchar_table_shim_rows

static const struct
{
	int			encoding;
}			DatabaseEncodingShim = {PG_UTF8};
#define DatabaseEncoding (&DatabaseEncodingShim)

int
GetDatabaseEncoding(void)
{
	return DatabaseEncoding->encoding;
}

const char *
GetDatabaseEncodingName(void)
{
	return "UTF8";
}

/* shim: mbutils.c report_invalid_encoding_db -> report_invalid_encoding
 * (real errcode + message shape; byte dump elided — message plane is out
 * of comparison scope) */
static void
report_invalid_encoding_db(const char *mbstr, int len, int limit)
{
	ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence for encoding \"%s\"", "UTF8")));
}

/* Utf8ToServerConvProc: never installed — server encoding IS UTF-8, so the
 * conversion-proc arms below are unreachable; keep a loud NULL. */
#define Utf8ToServerConvProc ((void *) 0)
#define FunctionCall6(f, a, b, c, d, e, g) \
	(abort(), (Datum) 0)		/* unreachable under the UTF-8 pin */
static const struct
{
	const char *name;
}			pg_enc2name_tbl[] = {[PG_UTF8] = {"UTF8"}};

/* ---- mbutils.c:860-926 VERBATIM (pg_unicode_to_server) ---- */
/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * The code point given by "c" is converted and stored at *s, which must
 * have at least MAX_UNICODE_EQUIVALENT_STRING+1 bytes available.
 * The output will have a trailing '\0'.  Throws error if the conversion
 * cannot be performed.
 *
 * Note that this relies on having previously looked up any required
 * conversion function.  That's partly for speed but mostly because the parser
 * may call this outside any transaction, or in an aborted transaction.
 */
void
pg_unicode_to_server(pg_wchar c, unsigned char *s)
{
	unsigned char c_as_utf8[MAX_MULTIBYTE_CHAR_LEN + 1];
	int			c_as_utf8_len;
	int			server_encoding;

	/*
	 * Complain if invalid Unicode code point.  The choice of errcode here is
	 * debatable, but really our caller should have checked this anyway.
	 */
	if (!is_valid_unicode_codepoint(c))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid Unicode code point")));

	/* Otherwise, if it's in ASCII range, conversion is trivial */
	if (c <= 0x7F)
	{
		s[0] = (unsigned char) c;
		s[1] = '\0';
		return;
	}

	/* If the server encoding is UTF-8, we just need to reformat the code */
	server_encoding = GetDatabaseEncoding();
	if (server_encoding == PG_UTF8)
	{
		unicode_to_utf8(c, s);
		s[pg_utf_mblen(s)] = '\0';
		return;
	}

	/* For all other cases, we must have a conversion function available */
	if (Utf8ToServerConvProc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("conversion between %s and %s is not supported",
						pg_enc2name_tbl[PG_UTF8].name,
						GetDatabaseEncodingName())));

	/* Construct UTF-8 source string */
	unicode_to_utf8(c, c_as_utf8);
	c_as_utf8_len = pg_utf_mblen(c_as_utf8);
	c_as_utf8[c_as_utf8_len] = '\0';

	/* Convert, or throw error if we can't */
	FunctionCall6(Utf8ToServerConvProc,
				  Int32GetDatum(PG_UTF8),
				  Int32GetDatum(server_encoding),
				  CStringGetDatum((char *) c_as_utf8),
				  CStringGetDatum((char *) s),
				  Int32GetDatum(c_as_utf8_len),
				  BoolGetDatum(false));
}

/* ---- mbutils.c:928-983 VERBATIM (pg_unicode_to_server_noerror) ---- */
/*
 * Convert a single Unicode code point into a string in the server encoding.
 *
 * Same as pg_unicode_to_server(), except that we don't throw errors,
 * but simply return false on conversion failure.
 */
bool
pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s)
{
	unsigned char c_as_utf8[MAX_MULTIBYTE_CHAR_LEN + 1];
	int			c_as_utf8_len;
	int			converted_len;
	int			server_encoding;

	/* Fail if invalid Unicode code point */
	if (!is_valid_unicode_codepoint(c))
		return false;

	/* Otherwise, if it's in ASCII range, conversion is trivial */
	if (c <= 0x7F)
	{
		s[0] = (unsigned char) c;
		s[1] = '\0';
		return true;
	}

	/* If the server encoding is UTF-8, we just need to reformat the code */
	server_encoding = GetDatabaseEncoding();
	if (server_encoding == PG_UTF8)
	{
		unicode_to_utf8(c, s);
		s[pg_utf_mblen(s)] = '\0';
		return true;
	}

	/* For all other cases, we must have a conversion function available */
	if (Utf8ToServerConvProc == NULL)
		return false;

	/* Construct UTF-8 source string */
	unicode_to_utf8(c, c_as_utf8);
	c_as_utf8_len = pg_utf_mblen(c_as_utf8);
	c_as_utf8[c_as_utf8_len] = '\0';

	/* Convert, but without throwing error if we can't */
	converted_len = DatumGetInt32(FunctionCall6(Utf8ToServerConvProc,
												Int32GetDatum(PG_UTF8),
												Int32GetDatum(server_encoding),
												CStringGetDatum((char *) c_as_utf8),
												CStringGetDatum((char *) s),
												Int32GetDatum(c_as_utf8_len),
												BoolGetDatum(true)));

	/* Conversion was successful iff it consumed the whole input */
	return (converted_len == c_as_utf8_len);
}

/* ---- mbutils.c:993-998 VERBATIM (pg_mb2wchar_with_len) ---- */
/* convert a multibyte string to a wchar with a limited length */
int
pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
{
	return pg_wchar_table[DatabaseEncoding->encoding].mb2wchar_with_len((const unsigned char *) from, to, len);
}

/* ---- mbutils.c:1076-1098 VERBATIM (pg_mblen_range) ---- */
/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * [mbstr, end) of at least one byte in size.  Raises an illegal byte sequence
 * error if the sequence would exceed the range.
 */
int
pg_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		report_invalid_encoding_db(mbstr, length, end - mbstr);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, end - mbstr);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}

/* ---- mbutils.c:1125-1142 VERBATIM (pg_mblen_unbounded) ---- */
/*
 * Returns the length of a multibyte character sequence, without any
 * validation of bounds.
 *
 * PLEASE NOTE:  This function can only be used safely if the caller has
 * already verified the input string, since otherwise there is a risk of
 * overrunning the buffer if the string is invalid.  A prior call to a
 * pg_mbstrlen* function suffices.
 */
int
pg_mblen_unbounded(const char *mbstr)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);

	return length;
}

/* ---- mbutils.c:1144-1152 VERBATIM (pg_mblen) ---- */
/*
 * Historical name for pg_mblen_unbounded().  Should not be used and will be
 * removed in a later version.
 */
int
pg_mblen(const char *mbstr)
{
	return pg_mblen_unbounded(mbstr);
}

/* ================= 8. src/backend/utils/adt/numutils.c ================= */

/* ---- common/int.h VERBATIM (pg_strtoint32_safe dependency) ---- */
/* ---- int.h:492-508 VERBATIM (pg_neg_u32_overflow) ---- */
static inline bool
pg_neg_u32_overflow(uint32 a, int32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(0, a, result);
#else
	int64		res = -((int64) a);

	if (unlikely(res < PG_INT32_MIN))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = res;
	return false;
#endif
}

/* ---- numutils.c:88-97 VERBATIM ---- */
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

/* ---- numutils.c:360-386 VERBATIM (pg_strtoint32) ---- */
/*
 * Convert input string to a signed 32 bit integer.  Input strings may be
 * expressed in base-10, hexadecimal, octal, or binary format, all of which
 * can be prefixed by an optional sign character, either '+' (the default) or
 * '-' for negative numbers.  Hex strings are recognized by the digits being
 * prefixed by 0x or 0X while octal strings are recognized by the 0o or 0O
 * prefix.  The binary representation is recognized by the 0b or 0B prefix.
 *
 * Allows any number of leading or trailing whitespace characters.  Digits may
 * optionally be separated by a single underscore character.  These can only
 * come between digits and not before or after the digits.  Underscores have
 * no effect on the return value and are supported only to assist in improving
 * the human readability of the input strings.
 *
 * pg_strtoint32() will throw ereport() upon bad input format or overflow;
 * while pg_strtoint32_safe() instead returns such complaints in *escontext,
 * if it's an ErrorSaveContext.
 *
 * NB: Accumulate input as an unsigned number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * signed positive number.
 */
int32
pg_strtoint32(const char *s)
{
	return pg_strtoint32_safe(s, NULL);
}

/* ---- numutils.c:388-619 VERBATIM (pg_strtoint32_safe) ---- */
int32
pg_strtoint32_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint32		tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int32		result;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT32_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (unlikely(tmp > PG_INT32_MAX))
		goto out_of_range;

	return (int32) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT32_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		if (unlikely(pg_neg_u32_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (tmp > PG_INT32_MAX)
		goto out_of_range;

	return (int32) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "integer")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"integer", s)));
}

/* ---- src/backend/utils/mb/wstrncmp.c VERBATIM ---- */
/* ---- wstrncmp.c:54-67 VERBATIM (pg_char_and_wchar_strncmp) ---- */
int
pg_char_and_wchar_strncmp(const char *s1, const pg_wchar *s2, size_t n)
{
	if (n == 0)
		return 0;
	do
	{
		if ((pg_wchar) ((unsigned char) *s1) != *s2++)
			return ((pg_wchar) ((unsigned char) *s1) - *(s2 - 1));
		if (*s1++ == 0)
			break;
	} while (--n != 0);
	return 0;
}

/* ---- mbutils.c:1030-1074 VERBATIM (pg_mblen_cstr) ---- */
/*
 * Returns the byte length of a multibyte character sequence in a
 * null-terminated string.  Raises an illegal byte sequence error if the
 * sequence would hit a null terminator.
 *
 * The caller is expected to have checked for a terminator at *mbstr == 0
 * before calling, but some callers want 1 in that case, so this function
 * continues that tradition.
 *
 * This must only be used for strings that have a null-terminator to enable
 * bounds detection.
 */
int
pg_mblen_cstr(const char *mbstr)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	/*
	 * The .mblen functions return 1 when given a pointer to a terminator.
	 * Some callers depend on that, so we tolerate it for now.  Well-behaved
	 * callers check the leading byte for a terminator *before* calling.
	 */
	for (int i = 1; i < length; ++i)
		if (unlikely(mbstr[i] == 0))
			report_invalid_encoding_db(mbstr, length, i);

	/*
	 * String should be NUL-terminated, but checking that would make typical
	 * callers O(N^2), tripling Valgrind check-world time.  Unless
	 * VALGRIND_EXPENSIVE, check 1 byte after each actual character.  (If we
	 * found a character, not a terminator, the next byte must be a terminator
	 * or the start of the next character.)  If the caller iterates the whole
	 * string, the last call will diagnose a missing terminator.
	 */
	if (mbstr[0] != '\0')
	{
#ifdef VALGRIND_EXPENSIVE
		VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, strlen(mbstr));
#else
		VALGRIND_CHECK_MEM_IS_DEFINED(mbstr + length, 1);
#endif
	}

	return length;
}

/* ---- mbutils.c:1100-1122 VERBATIM (pg_mblen_with_len) ---- */
/*
 * Returns the byte length of a multibyte character sequence bounded by a range
 * extending for 'limit' bytes, which must be at least one.  Raises an illegal
 * byte sequence error if the sequence would exceed the range.
 */
int
pg_mblen_with_len(const char *mbstr, int limit)
{
	int			length = pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);

	Assert(limit >= 1);

	if (unlikely(length > limit))
		report_invalid_encoding_db(mbstr, length, limit);

#ifdef VALGRIND_EXPENSIVE
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, limit);
#else
	VALGRIND_CHECK_MEM_IS_DEFINED(mbstr, length);
#endif

	return length;
}

/* ---- UTF-8 verifier chain VERBATIM (wchar.c DFA + verifychar/verifystr,
 * mbutils.c pg_verify_mbstr). report_invalid_encoding is the shim leaf
 * (real errcode 22021; the byte-dump message is out of comparison
 * scope) — reuses report_invalid_encoding_db above. ---- */
#define report_invalid_encoding(enc, mbstr, len) \
	report_invalid_encoding_db((mbstr), (len), (len))
#define PG_VALID_ENCODING(enc) ((enc) == PG_UTF8)

/* ---- wchar.c:1801-1911 VERBATIM ---- */
#define	ERR  0
/* Begin */
#define	BGN 11
/* Continuation states, expect 1/2/3 continuation bytes */
#define	CS1 16
#define	CS2  1
#define	CS3  5
/* Partial states, where the first continuation byte has a restricted range */
#define	P3A  6					/* Lead was E0, check for 3-byte overlong */
#define	P3B 20					/* Lead was ED, check for surrogate */
#define	P4A 25					/* Lead was F0, check for 4-byte overlong */
#define	P4B 30					/* Lead was F4, check for too-large */
/* Begin and End are the same state */
#define	END BGN

/* the encoded state transitions for the lookup table */

/* ASCII */
#define ASC (END << BGN)
/* 2-byte lead */
#define L2A (CS1 << BGN)
/* 3-byte lead */
#define L3A (P3A << BGN)
#define L3B (CS2 << BGN)
#define L3C (P3B << BGN)
/* 4-byte lead */
#define L4A (P4A << BGN)
#define L4B (CS3 << BGN)
#define L4C (P4B << BGN)
/* continuation byte */
#define CR1 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4B)
#define CR2 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4A)
#define CR3 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3A) | (CS2 << P4A)
/* invalid byte */
#define ILL ERR

static const uint32 Utf8Transition[256] =
{
	/* ASCII */

	ILL, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	/* continuation bytes */

	/* 80..8F */
	CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1,
	CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1,

	/* 90..9F */
	CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2,
	CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2,

	/* A0..BF */
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,

	/* leading bytes */

	/* C0..DF */
	ILL, ILL, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,

	/* E0..EF */
	L3A, L3B, L3B, L3B, L3B, L3B, L3B, L3B,
	L3B, L3B, L3B, L3B, L3B, L3C, L3B, L3B,

	/* F0..FF */
	L4A, L4B, L4B, L4B, L4C, ILL, ILL, ILL,
	ILL, ILL, ILL, ILL, ILL, ILL, ILL, ILL
};

static void
utf8_advance(const unsigned char *s, uint32 *state, int len)
{
	/* Note: We deliberately don't check the state's value here. */
	while (len > 0)
	{
		/*
		 * It's important that the mask value is 31: In most instruction sets,
		 * a shift by a 32-bit operand is understood to be a shift by its mod
		 * 32, so the compiler should elide the mask operation.
		 */
		*state = Utf8Transition[*s++] >> (*state & 31);
		len--;
	}

	*state &= 31;
}

/* ---- wchar.c:1722-1749 VERBATIM (pg_utf8_verifychar) ---- */
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

/* ---- wchar.c:1912-1994 VERBATIM (pg_utf8_verifystr) ---- */
static int
pg_utf8_verifystr(const unsigned char *s, int len)
{
	const unsigned char *start = s;
	const int	orig_len = len;
	uint32		state = BGN;

/*
 * With a stride of two vector widths, gcc will unroll the loop. Even if
 * the compiler can unroll a longer loop, it's not worth it because we
 * must fall back to the byte-wise algorithm if we find any non-ASCII.
 */
#define STRIDE_LENGTH (2 * sizeof(Vector8))

	if (len >= STRIDE_LENGTH)
	{
		while (len >= STRIDE_LENGTH)
		{
			/*
			 * If the chunk is all ASCII, we can skip the full UTF-8 check,
			 * but we must first check for a non-END state, which means the
			 * previous chunk ended in the middle of a multibyte sequence.
			 */
			if (state != END || !is_valid_ascii(s, STRIDE_LENGTH))
				utf8_advance(s, &state, STRIDE_LENGTH);

			s += STRIDE_LENGTH;
			len -= STRIDE_LENGTH;
		}

		/* The error state persists, so we only need to check for it here. */
		if (state == ERR)
		{
			/*
			 * Start over from the beginning with the slow path so we can
			 * count the valid bytes.
			 */
			len = orig_len;
			s = start;
		}
		else if (state != END)
		{
			/*
			 * The fast path exited in the middle of a multibyte sequence.
			 * Walk backwards to find the leading byte so that the slow path
			 * can resume checking from there. We must always backtrack at
			 * least one byte, since the current byte could be e.g. an ASCII
			 * byte after a 2-byte lead, which is invalid.
			 */
			do
			{
				Assert(s > start);
				s--;
				len++;
				Assert(IS_HIGHBIT_SET(*s));
			} while (pg_utf_mblen(s) <= 1);
		}
	}

	/* check remaining bytes */
	while (len > 0)
	{
		int			l;

		/* fast path for ASCII-subset characters */
		if (!IS_HIGHBIT_SET(*s))
		{
			if (*s == '\0')
				break;
			l = 1;
		}
		else
		{
			l = pg_utf8_verifychar(s, len);
			if (l == -1)
				break;
		}
		s += l;
		len -= l;
	}

	return s - start;
}
/* ---- mbutils.c:1687-1706 VERBATIM (pg_verify_mbstr) ---- */
/*
 * Verify mbstr to make sure that it is validly encoded in the specified
 * encoding.
 */
bool
pg_verify_mbstr(int encoding, const char *mbstr, int len, bool noError)
{
	int			oklen;

	Assert(PG_VALID_ENCODING(encoding));

	oklen = pg_wchar_table[encoding].mbverifystr((const unsigned char *) mbstr, len);
	if (oklen != len)
	{
		if (noError)
			return false;
		report_invalid_encoding(encoding, mbstr + oklen, len - oklen);
	}
	return true;
}

/*
 * shim: client encoding == server encoding == UTF-8, so both directions take
 * pg_any_to_server's / pg_server_to_client's same-encoding arm. That arm is
 * NOT the identity: pg_any_to_server still VALIDATES the bytes via
 * pg_verify_mbstr (found by the differential itself, 2026-07-31 — the naive
 * identity shim let invalid UTF-8 through recv and reported 42601 where real
 * PG reports 22021). The verifier chain below is VERBATIM.
 */
static char *
pg_client_to_server(const char *s, int len)
{
	if (len <= 0)
		return (char *) s;
	/* pg_any_to_server, same-encoding arm (mbutils.c) VERBATIM */
	(void) pg_verify_mbstr(DatabaseEncoding->encoding, s, len, false);
	return (char *) s;
}

char *
pg_server_to_client(const char *s, int len)
{
	/* pg_server_to_client -> pg_server_to_any, same-encoding arm: returns the
	 * input unchanged (no validation on the way out, matching mbutils.c) */
	return (char *) s;
}

/* ---- numutils.c:29-61 VERBATIM (DIGIT_TABLE + decimalLength32) ---- */
static const char DIGIT_TABLE[200] =
"00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
"10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
"20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
"30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
"40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
"50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
"60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
"70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
"80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
"90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

/*
 * Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
 */
static inline int
decimalLength32(const uint32 v)
{
	int			t;
	static const uint32 PowersOfTen[] = {
		1, 10, 100,
		1000, 10000, 100000,
		1000000, 10000000, 100000000,
		1000000000
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

/* ---- numutils.c:1047-1109 VERBATIM (pg_ultoa_n) ---- */
/*
 * pg_ultoa_n: converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */
int
pg_ultoa_n(uint32 value, char *a)
{
	int			olength,
				i = 0;

	/* Degenerate case */
	if (value == 0)
	{
		*a = '0';
		return 1;
	}

	olength = decimalLength32(value);

	/* Compute the result string. */
	while (value >= 10000)
	{
		const uint32 c = value - 10000 * (value / 10000);
		const uint32 c0 = (c % 100) << 1;
		const uint32 c1 = (c / 100) << 1;

		char	   *pos = a + olength - i;

		value /= 10000;

		memcpy(pos - 2, DIGIT_TABLE + c0, 2);
		memcpy(pos - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}
	if (value >= 100)
	{
		const uint32 c = (value % 100) << 1;

		char	   *pos = a + olength - i;

		value /= 100;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (value >= 10)
	{
		const uint32 c = value << 1;

		char	   *pos = a + olength - i;

		memcpy(pos - 2, DIGIT_TABLE + c, 2);
	}
	else
	{
		*a = (char) ('0' + value);
	}

	return olength;
}

/* ---- jsonpathexec_diff additions (VERBATIM; shares hexlookup above) ---- */
/* ---- int.h:575-602 VERBATIM (pg_neg_u64_overflow) ---- */
static inline bool
pg_neg_u64_overflow(uint64 a, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(0, a, result);
#elif defined(HAVE_INT128)
	int128		res = -((int128) a);

	if (unlikely(res < PG_INT64_MIN))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	*result = res;
	return false;
#else
	if (unlikely(a > (uint64) PG_INT64_MAX + 1))
	{
		*result = 0x5EED;		/* to avoid spurious warnings */
		return true;
	}
	if (unlikely(a == (uint64) PG_INT64_MAX + 1))
		*result = PG_INT64_MIN;
	else
		*result = -((int64) a);
	return false;
#endif
}

/* ---- numutils.c:621-647 VERBATIM (pg_strtoint64) ---- */
/*
 * Convert input string to a signed 64 bit integer.  Input strings may be
 * expressed in base-10, hexadecimal, octal, or binary format, all of which
 * can be prefixed by an optional sign character, either '+' (the default) or
 * '-' for negative numbers.  Hex strings are recognized by the digits being
 * prefixed by 0x or 0X while octal strings are recognized by the 0o or 0O
 * prefix.  The binary representation is recognized by the 0b or 0B prefix.
 *
 * Allows any number of leading or trailing whitespace characters.  Digits may
 * optionally be separated by a single underscore character.  These can only
 * come between digits and not before or after the digits.  Underscores have
 * no effect on the return value and are supported only to assist in improving
 * the human readability of the input strings.
 *
 * pg_strtoint64() will throw ereport() upon bad input format or overflow;
 * while pg_strtoint64_safe() instead returns such complaints in *escontext,
 * if it's an ErrorSaveContext.
 *
 * NB: Accumulate input as an unsigned number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * signed positive number.
 */
int64
pg_strtoint64(const char *s)
{
	return pg_strtoint64_safe(s, NULL);
}

/* ---- numutils.c:649-880 VERBATIM (pg_strtoint64_safe) ---- */
int64
pg_strtoint64_safe(const char *s, Node *escontext)
{
	const char *ptr = s;
	const char *firstdigit;
	uint64		tmp = 0;
	bool		neg = false;
	unsigned char digit;
	int64		result;

	/*
	 * The majority of cases are likely to be base-10 digits without any
	 * underscore separator characters.  We'll first try to parse the string
	 * with the assumption that's the case and only fallback on a slower
	 * implementation which handles hex, octal and binary strings and
	 * underscores if the fastpath version cannot parse the string.
	 */

	/* leave it up to the slow path to look for leading spaces */

	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}

	/* a leading '+' is uncommon so leave that for the slow path */

	/* process the first digit */
	digit = (*ptr - '0');

	/*
	 * Exploit unsigned arithmetic to save having to check both the upper and
	 * lower bounds of the digit.
	 */
	if (likely(digit < 10))
	{
		ptr++;
		tmp = digit;
	}
	else
	{
		/* we need at least one digit */
		goto slow;
	}

	/* process remaining digits */
	for (;;)
	{
		digit = (*ptr - '0');

		if (digit >= 10)
			break;

		ptr++;

		if (unlikely(tmp > -(PG_INT64_MIN / 10)))
			goto out_of_range;

		tmp = tmp * 10 + digit;
	}

	/* when the string does not end in a digit, let the slow path handle it */
	if (unlikely(*ptr != '\0'))
		goto slow;

	if (neg)
	{
		if (unlikely(pg_neg_u64_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (unlikely(tmp > PG_INT64_MAX))
		goto out_of_range;

	return (int64) tmp;

slow:
	tmp = 0;
	ptr = s;
	/* no need to reset neg */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* process digits */
	if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (isxdigit((unsigned char) *ptr))
			{
				if (unlikely(tmp > -(PG_INT64_MIN / 16)))
					goto out_of_range;

				tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '7')
			{
				if (unlikely(tmp > -(PG_INT64_MIN / 8)))
					goto out_of_range;

				tmp = tmp * 8 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
	{
		firstdigit = ptr += 2;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '1')
			{
				if (unlikely(tmp > -(PG_INT64_MIN / 2)))
					goto out_of_range;

				tmp = tmp * 2 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
	{
		firstdigit = ptr;

		for (;;)
		{
			if (*ptr >= '0' && *ptr <= '9')
			{
				if (unlikely(tmp > -(PG_INT64_MIN / 10)))
					goto out_of_range;

				tmp = tmp * 10 + (*ptr++ - '0');
			}
			else if (*ptr == '_')
			{
				/* underscore may not be first */
				if (unlikely(ptr == firstdigit))
					goto invalid_syntax;
				/* and it must be followed by more digits */
				ptr++;
				if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
					goto invalid_syntax;
			}
			else
				break;
		}
	}

	/* require at least one digit */
	if (unlikely(ptr == firstdigit))
		goto invalid_syntax;

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (neg)
	{
		if (unlikely(pg_neg_u64_overflow(tmp, &result)))
			goto out_of_range;
		return result;
	}

	if (tmp > PG_INT64_MAX)
		goto out_of_range;

	return (int64) tmp;

out_of_range:
	ereturn(escontext, 0,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "bigint")));

invalid_syntax:
	ereturn(escontext, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"bigint", s)));
}

/* ---- numutils.c:1111-1133 VERBATIM (pg_ltoa) ---- */
/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation and
 * returns strlen(a).
 *
 * It is the caller's responsibility to ensure that a is at least 12 bytes long,
 * which is enough room to hold a minus sign, a maximally long int32, and the
 * above terminating NUL.
 */
int
pg_ltoa(int32 value, char *a)
{
	uint32		uvalue = (uint32) value;
	int			len = 0;

	if (value < 0)
	{
		uvalue = (uint32) 0 - uvalue;
		a[len++] = '-';
	}
	len += pg_ultoa_n(uvalue, a + len);
	a[len] = '\0';
	return len;
}

