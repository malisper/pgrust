/*
 * Vendored VERBATIM from postgres master src/backend/utils/adt/json.c
 * (escape_json_char, escape_json, escape_json_with_len), fetched 2026-07-28.
 *
 * SHIMS (everything below "SHIM LAYER" is ours; the three pg_escape_json*
 * function bodies are byte-for-byte the C logic, only renamed with pg_ prefix
 * and with these substitutions):
 *   - StringInfo           -> PG_StringInfo: fixed 128-byte buffer + int len.
 *   - appendStringInfoCharMacro(b,c)   -> pg_append_char(b,c)
 *   - appendStringInfoString(b,s)      -> pg_append_string(b,s)
 *   - appendBinaryStringInfo(b,p,n)    -> pg_append_binary(b,p,n)
 *   - enlargeStringInfo(b,n)           -> pg_enlarge(b,n)  (bounds assert only)
 *   - appendStringInfo(buf, "\\u%04x", (int) c)
 *        -> pg_append_u04x(buf, (int) c): appends '\\','u' and 4 LOWERCASE hex
 *           digits of the low 16 bits — exactly what %04x prints for the
 *           guarded range (0..0x1f), avoiding sprintf inside CBMC.
 *   - Vector8 -> struct of 16 bytes; vector8_load/has/has_le implemented as
 *     scalar loops with identical semantics. (For len <= 16 the vector loop is
 *     dead anyway: vlen rounds down to 0.)
 *   - Assert -> assert (from <assert.h>)
 *   - pg_always_inline -> static inline
 *   - escape_json_with_len's inner per-chunk loop index: upstream declares
 *     `for (int b = 0; b < sizeof(Vector8); b++)`; vendored uses size_t b.
 *     b ranges 0..16 and the upstream int is promoted to size_t in the
 *     comparison anyway — no behavior change. (Shim was previously
 *     undocumented; recorded per proofs/PROVENANCE-AUDIT.md 2026-07-28.)
 * Escaping DECISION LOGIC (which bytes escape, to what) is untouched.
 *
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE json.c
 * (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 */
#include <assert.h>
#include <stddef.h>
#include <string.h>

/*
 * Shim-layer buffer size (not part of the vendored logic): worst-case
 * output for the len<=4 harnesses is 4*6 + 2 quotes = 26 bytes. Keeping
 * the fixed buffer small matters under CBMC: every append creates a new
 * SSA version of the whole array in the SAT formula.
 */
#ifndef PG_BUFSZ
#define PG_BUFSZ 32
#endif

typedef struct PG_StringInfo
{
	char		data[PG_BUFSZ];
	int			len;
}			PG_StringInfo;

typedef unsigned char uint8;

typedef struct Vector8
{
	uint8		b[16];
}			Vector8;

/* ---- SHIM LAYER ---- */

static void
pg_append_char(PG_StringInfo * buf, char c)
{
	assert(buf->len < PG_BUFSZ);
	buf->data[buf->len++] = c;
}

static void
pg_append_string(PG_StringInfo * buf, const char *s)
{
	while (*s != '\0')
		pg_append_char(buf, *s++);
}

static void
pg_append_binary(PG_StringInfo * buf, const char *p, int n)
{
	int			i;

	assert(buf->len + n <= PG_BUFSZ);
	for (i = 0; i < n; i++)
		buf->data[buf->len++] = p[i];
}

static void
pg_enlarge(PG_StringInfo * buf, int needed)
{
	assert(buf->len + needed <= PG_BUFSZ);
}

static void
pg_append_u04x(PG_StringInfo * buf, int v)
{
	static const char hex[] = "0123456789abcdef";

	pg_append_char(buf, '\\');
	pg_append_char(buf, 'u');
	pg_append_char(buf, hex[(v >> 12) & 0xf]);
	pg_append_char(buf, hex[(v >> 8) & 0xf]);
	pg_append_char(buf, hex[(v >> 4) & 0xf]);
	pg_append_char(buf, hex[v & 0xf]);
}

static void
vector8_load(Vector8 * v, const uint8 *p)
{
	memcpy(v->b, p, sizeof(Vector8));
}

static int
vector8_has(Vector8 v, unsigned char c)
{
	for (size_t i = 0; i < sizeof(Vector8); i++)
		if (v.b[i] == c)
			return 1;
	return 0;
}

static int
vector8_has_le(Vector8 v, unsigned char c)
{
	for (size_t i = 0; i < sizeof(Vector8); i++)
		if (v.b[i] <= c)
			return 1;
	return 0;
}

/* ---- VERBATIM POSTGRES LOGIC (renames + shims per header comment) ---- */

/*
 * escape_json_char
 *		Inline helper function for escape_json* functions
 */
static inline void
pg_escape_json_char(PG_StringInfo * buf, char c)
{
	switch (c)
	{
		case '\b':
			pg_append_string(buf, "\\b");
			break;
		case '\f':
			pg_append_string(buf, "\\f");
			break;
		case '\n':
			pg_append_string(buf, "\\n");
			break;
		case '\r':
			pg_append_string(buf, "\\r");
			break;
		case '\t':
			pg_append_string(buf, "\\t");
			break;
		case '"':
			pg_append_string(buf, "\\\"");
			break;
		case '\\':
			pg_append_string(buf, "\\\\");
			break;
		default:
			if ((unsigned char) c < ' ')
				pg_append_u04x(buf, (int) c);
			else
				pg_append_char(buf, c);
			break;
	}
}

/*
 * escape_json
 *		Produce a JSON string literal, properly escaping the NUL-terminated
 *		cstring.
 */
static void
pg_escape_json(PG_StringInfo * buf, const char *str)
{
	pg_append_char(buf, '"');

	for (; *str != '\0'; str++)
		pg_escape_json_char(buf, *str);

	pg_append_char(buf, '"');
}

#define ESCAPE_JSON_FLUSH_AFTER 512

/*
 * escape_json_with_len
 *		Produce a JSON string literal, properly escaping the possibly not
 *		NUL-terminated characters in 'str'.  'len' defines the number of bytes
 *		from 'str' to process.
 */
static void
pg_escape_json_with_len(PG_StringInfo * buf, const char *str, int len)
{
	int			vlen;

	assert(len >= 0);

	pg_enlarge(buf, len + 2);

	vlen = len & (int) (~(sizeof(Vector8) - 1));

	pg_append_char(buf, '"');

	for (int i = 0, copypos = 0;;)
	{
		for (; i < vlen; i += sizeof(Vector8))
		{
			Vector8		chunk;

			vector8_load(&chunk, (const uint8 *) &str[i]);

			if (vector8_has_le(chunk, (unsigned char) 0x1F) ||
				vector8_has(chunk, (unsigned char) '"') ||
				vector8_has(chunk, (unsigned char) '\\'))
				break;

#ifdef ESCAPE_JSON_FLUSH_AFTER
			if (i - copypos >= ESCAPE_JSON_FLUSH_AFTER)
			{
				pg_append_binary(buf, &str[copypos], i - copypos);
				copypos = i;
			}
#endif
		}

		if (copypos < i)
		{
			pg_append_binary(buf, &str[copypos], i - copypos);
			copypos = i;
		}

		for (size_t b = 0; b < sizeof(Vector8); b++)	/* shim: upstream `int b` */
		{
			if (i == len)
				goto done;

			assert(i < len);

			pg_escape_json_char(buf, str[i++]);
		}

		copypos = i;
	}

done:
	pg_append_char(buf, '"');
}

/* ---- int-returning entry shims (void-return C rejected by goto-cc/Unit wart) ---- */

/* Run escape_json_with_len on (str,len); write result bytes to out; return out len. */
int
pg_run_escape_json_with_len(const unsigned char *str, int len, unsigned char *out)
{
	PG_StringInfo si;

	si.len = 0;
	pg_escape_json_with_len(&si, (const char *) str, len);
	for (int i = 0; i < si.len; i++)
		out[i] = (unsigned char) si.data[i];
	return si.len;
}

/* Run the cstring variant escape_json on a NUL-terminated str. */
int
pg_run_escape_json_cstr(const unsigned char *str, unsigned char *out)
{
	PG_StringInfo si;

	si.len = 0;
	pg_escape_json(&si, (const char *) str);
	for (int i = 0; i < si.len; i++)
		out[i] = (unsigned char) si.data[i];
	return si.len;
}
