/*
 * Vendored PostgreSQL C: "char" (1-byte) type — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/char.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp 18.3, the repo's
 *     vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
 *     charin, charout, chareq, charne, charlt, charle, chargt, charge,
 *     chartoi4, i4tochar, text_char, char_text — bodies verbatim.
 *     ISOCTAL/TOOCTAL/FROMOCTAL macros verbatim.
 *
 * Shims (numbered; plumbing only, never logic):
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS, PG_GETARG_x, PG_RETURN_x ->
 *      plain C signatures. The bodies between those macros are verbatim.
 *   2. palloc(5)/palloc(VARHDRSZ + 4) -> caller-provided buffers. Store
 *      statements verbatim; char_text's SET_VARSIZE bookkeeping is returned
 *      as the payload length instead of a varlena header (the Rust
 *      comparator checks the payload image; the 4-byte header is pqformat/
 *      varlena framing, not char.c logic).
 *   3. ereport(ERROR, errcode(X), errmsg(...)) in i4tochar -> record X in
 *      the shared thread-local pg_diff_errcode (defined in pg_float_io.c)
 *      and return an error flag. Message text unevaluated (comparator
 *      checks errcode class only). Same small-int mapping as pg_float_io.c:
 *      2 = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003).
 *   4. text_char's PG_GETARG_TEXT_PP + VARDATA_ANY/VARSIZE_ANY_EXHDR ->
 *      (const char *data, int len) parameters: the detoast/packed-header
 *      decode is varlena framing done by the caller on both sides; the
 *      body's decision logic on (data, len) is verbatim.
 *   5. charrecv/charsend are NOT vendored here: their bodies are single
 *      pq_getmsgbyte/pq_sendbyte calls (pqformat framing, no char.c
 *      logic). The Rust driver compares the shipped wire images against
 *      the fixed expected layout documented at the call site.
 *   6. SCHAR_MIN/SCHAR_MAX from <limits.h> as in the original; int8/int32
 *      etc. from the shim postgres.h.
 */

#include "postgres.h"

#include <limits.h>

/* shared with pg_float_io.c (thread-local errcode capture) */
extern _Thread_local int pg_diff_errcode;

#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE 2

#define ISOCTAL(c)   (((c) >= '0') && ((c) <= '7'))
#define TOOCTAL(c)   ((c) + '0')
#define FROMOCTAL(c) ((unsigned char) (c) - '0')

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

/*
 * charin — verbatim body (SHIM 1: cstring arg, char return).
 */
char
pg_diff_charin(const char *ch)
{
	pg_diff_errcode = 0;
	if (strlen(ch) == 4 && ch[0] == '\\' &&
		ISOCTAL(ch[1]) && ISOCTAL(ch[2]) && ISOCTAL(ch[3]))
		return (char) ((FROMOCTAL(ch[1]) << 6) +
					   (FROMOCTAL(ch[2]) << 3) +
					   FROMOCTAL(ch[3]));
	/* This will do the right thing for a zero-length input string */
	return ch[0];
}

/*
 * charout — verbatim body (SHIM 2: palloc(5) -> caller buffer out5).
 * Returns strlen of the produced cstring image.
 */
int
pg_diff_charout(char ch, char *out5)
{
	char	   *result = out5;

	pg_diff_errcode = 0;
	if (IS_HIGHBIT_SET(ch))
	{
		result[0] = '\\';
		result[1] = TOOCTAL(((unsigned char) ch) >> 6);
		result[2] = TOOCTAL((((unsigned char) ch) >> 3) & 07);
		result[3] = TOOCTAL(((unsigned char) ch) & 07);
		result[4] = '\0';
		return 4;
	}
	else
	{
		/* This produces acceptable results for 0x00 as well */
		result[0] = ch;
		result[1] = '\0';
		return result[0] ? 1 : 0;
	}
}

/* comparison bodies — verbatim expressions (SHIM 1) */
int
pg_diff_chareq(char arg1, char arg2)
{
	return arg1 == arg2;
}

int
pg_diff_charne(char arg1, char arg2)
{
	return arg1 != arg2;
}

int
pg_diff_charlt(char arg1, char arg2)
{
	return (uint8) arg1 < (uint8) arg2;
}

int
pg_diff_charle(char arg1, char arg2)
{
	return (uint8) arg1 <= (uint8) arg2;
}

int
pg_diff_chargt(char arg1, char arg2)
{
	return (uint8) arg1 > (uint8) arg2;
}

int
pg_diff_charge(char arg1, char arg2)
{
	return (uint8) arg1 >= (uint8) arg2;
}

int32
pg_diff_chartoi4(char arg1)
{
	return (int32) ((int8) arg1);
}

/*
 * i4tochar — verbatim body (SHIM 3: ereport -> errcode record + error
 * return). Returns 0 and stores the char on success; returns 1 with
 * pg_diff_errcode set on the out-of-range error.
 */
int
pg_diff_i4tochar(int32 arg1, char *out)
{
	pg_diff_errcode = 0;
	if (arg1 < SCHAR_MIN || arg1 > SCHAR_MAX)
	{
		pg_diff_errcode = ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
		return 1;
	}

	*out = (char) (int8) arg1;
	return 0;
}

/*
 * text_char — verbatim body (SHIM 4: (data, len) in place of the text
 * varlena).
 */
char
pg_diff_text_char(const char *ch, int len)
{
	char		result;

	pg_diff_errcode = 0;

	/*
	 * Conversion rules are the same as in charin(), but here we need to
	 * handle the empty-string case honestly.
	 */
	if (len == 4 && ch[0] == '\\' &&
		ISOCTAL(ch[1]) && ISOCTAL(ch[2]) && ISOCTAL(ch[3]))
		result = (FROMOCTAL(ch[1]) << 6) +
			(FROMOCTAL(ch[2]) << 3) +
			FROMOCTAL(ch[3]);
	else if (len > 0)
		result = ch[0];
	else
		result = '\0';

	return result;
}

/*
 * char_text — verbatim stores (SHIM 2: palloc'd varlena -> caller 4-byte
 * payload buffer; SET_VARSIZE -> returned payload length).
 */
int
pg_diff_char_text(char arg1, char *out4)
{
	pg_diff_errcode = 0;
	if (IS_HIGHBIT_SET(arg1))
	{
		out4[0] = '\\';
		out4[1] = TOOCTAL(((unsigned char) arg1) >> 6);
		out4[2] = TOOCTAL((((unsigned char) arg1) >> 3) & 07);
		out4[3] = TOOCTAL(((unsigned char) arg1) & 07);
		return 4;
	}
	else if (arg1 != '\0')
	{
		*out4 = arg1;
		return 1;
	}
	else
		return 0;
}
