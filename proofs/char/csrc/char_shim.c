/*
 * Vendored from postgres src/backend/utils/adt/char.c: charin / charout
 * core logic. Ref: REL_18_STABLE — fetched from master 2026-07-28 and
 * verified byte-identical to REL_18_STABLE (provenance audit,
 * proofs/PROVENANCE-AUDIT.md); matches the REL_18_STABLE citation used
 * for chartoi4/i4tochar/text_char/char_text further down this file.
 *
 * SHIMS (everything else is verbatim):
 *  - fmgr wrappers (PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_*) removed;
 *    plain args in, int out. PG_RETURN_CHAR truncates int->char: modeled by
 *    the explicit (char) casts below, exactly where C's fmgr macro does it.
 *  - charout's palloc(5) replaced by a caller-provided buffer; the function
 *    returns the produced cstring length instead (0, 1, or 4). The trailing
 *    NUL store is kept verbatim (result[4]/result[1]) so the buffer must be
 *    5 bytes.
 *  - charin's strlen() replaced by an explicit caller-passed len over a
 *    NUL-terminated buffer, with a manual strlen loop kept for fidelity:
 *    we recompute strlen ourselves so C sees exactly what strlen sees.
 */

#define ISOCTAL(c)   (((c) >= '0') && ((c) <= '7'))
#define TOOCTAL(c)   ((c) + '0')
#define FROMOCTAL(c) ((unsigned char) (c) - '0')
#define IS_HIGHBIT_SET(ch)	((unsigned char)(ch) & 0x80)

/* returns the char result as a signed 8-bit value widened to int
 * (PG_RETURN_CHAR semantics: sum computed in int, truncated to char) */
int
pgc_charin(const char *ch)
{
	unsigned long len = 0;			/* SHIM: manual strlen */
	while (ch[len] != '\0')
		len++;

	if (len == 4 && ch[0] == '\\' &&
		ISOCTAL(ch[1]) && ISOCTAL(ch[2]) && ISOCTAL(ch[3]))
		return (int) (char) ((FROMOCTAL(ch[1]) << 6) +
							 (FROMOCTAL(ch[2]) << 3) +
							 FROMOCTAL(ch[3]));
	/* This will do the right thing for a zero-length input string */
	return (int) (char) ch[0];
}

/* result must have room for 5 bytes; returns strlen(result) */
int
pgc_charout(int ch_i, char *result)
{
	char		ch = (char) ch_i;	/* SHIM: PG_GETARG_CHAR */

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
		return ch == 0 ? 0 : 1;
	}
}

/*
 * Vendored from postgres REL_18_STABLE src/backend/utils/adt/char.c
 * (fetched 2026-07-28): chartoi4, i4tochar, text_char, char_text.
 *
 * SHIMS (conversion expressions verbatim):
 *  - fmgr wrappers removed -> plain signatures.
 *  - i4tochar: ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ...)
 *    -> *err = 1 out-flag + return 0 (message text out of proof);
 *    SCHAR_MIN/SCHAR_MAX from limits.h semantics inlined (-128/127).
 *  - text_char: PG_GETARG_TEXT_PP + VARDATA_ANY/VARSIZE_ANY_EXHDR ->
 *    (data, len) pair (pre-detoasted caller contract, bytea-cmp pattern).
 *  - char_text: palloc'd text -> caller buffer for the payload bytes;
 *    returns payload length (SET_VARSIZE(VARHDRSZ+n) -> return n).
 */

#define SCHAR_MIN_ (-128)
#define SCHAR_MAX_ 127

int
pgc_chartoi4(int ch_i)
{
	char		arg1 = (char) ch_i;	/* SHIM: PG_GETARG_CHAR */

	return (int) ((signed char) arg1);
}

int
pgc_i4tochar(int arg1, int *err)
{
	*err = 0;
	if (arg1 < SCHAR_MIN_ || arg1 > SCHAR_MAX_)
	{
		*err = 1;				/* ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
								 * "\"char\" out of range") */
		return 0;
	}

	return (int) (char) ((signed char) arg1);	/* PG_RETURN_CHAR */
}

int
pgc_text_char(const char *ch, unsigned long len)
{
	char		result;

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

	return (int) (char) result;	/* PG_RETURN_CHAR */
}

int
pgc_char_text(int ch_i, char *out)
{
	char		arg1 = (char) ch_i;	/* SHIM: PG_GETARG_CHAR */

	/*
	 * Conversion rules are the same as in charout(), but here we need to be
	 * honest about converting 0x00 to an empty string.
	 */
	if (IS_HIGHBIT_SET(arg1))
	{
		out[0] = '\\';
		out[1] = TOOCTAL(((unsigned char) arg1) >> 6);
		out[2] = TOOCTAL((((unsigned char) arg1) >> 3) & 07);
		out[3] = TOOCTAL(((unsigned char) arg1) & 07);
		return 4;
	}
	else if (arg1 != '\0')
	{
		out[0] = arg1;
		return 1;
	}
	else
		return 0;
}
