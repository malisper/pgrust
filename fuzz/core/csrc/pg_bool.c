/*
 * Vendored PostgreSQL C: bool type — differential-fuzz oracle.
 *
 * Provenance (all bodies VERBATIM unless a shim is listed below):
 *   - src/backend/utils/adt/bool.c @ postgres-src
 *     62d6c7d3df6287f1bd83199c1a746e50d31571a0 (Stamp 18.3, the repo's
 *     vendored ground-truth checkout ../pgrust-fabled/vendor/postgres-src):
 *     parse_bool, parse_bool_with_len, boolin, boolout, boolrecv-decision,
 *     booltext-selection, booleq, boolne, boollt, boolgt, boolle, boolge,
 *     booland_statefunc, boolor_statefunc, BoolAggState, bool_accum,
 *     bool_accum_inv, bool_alltrue, bool_anytrue — bodies verbatim.
 *   - src/port/pgstrcasecmp.c @ same ref: pg_strncasecmp — verbatim
 *     (static copy, same as csrc/pg_float_io.c; the IS_HIGHBIT_SET branch
 *     keeps the real libc isupper()/tolower() calls — this host runs the
 *     C locale, where they are no-ops for high-bit bytes, matching both
 *     glibc-PG's C-locale behavior and Rust's to_ascii_lowercase).
 *
 * Shims (numbered; plumbing only, never logic):
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS, PG_GETARG_x, PG_RETURN_x ->
 *      plain C signatures; bool crosses the FFI as int (0/1). Bodies
 *      verbatim.
 *   2. boolin's ereturn(fcinfo->context, ...) -> record the errcode in the
 *      shared thread-local pg_diff_errcode (defined in pg_float_io.c) and
 *      return an error flag; message text unevaluated. Small-int mapping
 *      as in pg_float_io.c: 1 = ERRCODE_INVALID_TEXT_REPRESENTATION
 *      (22P02). The hard-vs-soft error split is caller policy (fcinfo
 *      context), not bool.c logic — the oracle reports "error", the Rust
 *      driver checks both its hard and soft paths against that verdict.
 *   3. boolout: palloc(2) -> caller 2-byte buffer; store statements
 *      verbatim. booltext: cstring_to_text(str) -> the selected literal is
 *      copied to a caller buffer and its length returned (the text varlena
 *      framing is not bool.c logic).
 *   4. boolrecv: pq_getmsgbyte(buf) -> the byte is the parameter (wire
 *      framing is pqformat's); the ext != 0 decision is verbatim.
 *      boolsend is a single pq_sendbyte(arg1 ? 1 : 0) — the ?: selection
 *      is inlined at the Rust comparison site against the fixed wire
 *      image; nothing to vendor.
 *   5. bool_accum/bool_accum_inv: fcinfo NULL-flag plumbing and the
 *      makeBoolAggState aggcontext allocation -> caller-owned
 *      (state_isnull, aggcount, aggtrue) parameters. The count/true
 *      mutations and the NULL-state handling are verbatim. elog(ERROR,
 *      "bool_accum_inv called with NULL state") -> errcode 5 (internal
 *      error class) + error flag. AggCheckCallContext is executor
 *      environment, not bool.c logic — the Rust fc_-wrapper's
 *      non-aggregate-context arm is checked against a pinned message in
 *      the driver, not against this oracle.
 *   6. bool_alltrue/bool_anytrue: PG_RETURN_NULL() -> returned -1
 *      (0 = false, 1 = true, -1 = SQL NULL).
 */

#include "postgres.h"

#include <ctype.h>

/* shared with pg_float_io.c (thread-local errcode capture) */
extern _Thread_local int pg_diff_errcode;

#define ERRCODE_INVALID_TEXT_REPRESENTATION 1
#define PG_DIFF_ERR_INTERNAL 5

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

/* ---- src/port/pgstrcasecmp.c: pg_strncasecmp — VERBATIM ---- */

static int
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

/* ---- bool.c: parse_bool_with_len — VERBATIM (SHIM 1: bool -> int) ---- */

int
pg_diff_parse_bool_with_len(const char *value, size_t len, int *result)
{
	/* Check the most-used possibilities first. */
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = 1;
				return 1;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = 1;
				return 1;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = 0;
				return 1;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = 0;			/* suppress compiler warning */
	return 0;
}

int
pg_diff_parse_bool(const char *value, int *result)
{
	return pg_diff_parse_bool_with_len(value, strlen(value), result);
}

/*
 * boolin — verbatim body (SHIM 1: cstring arg; SHIM 2: ereturn -> errcode
 * + error flag). Returns 0 with *result set on success, 1 on parse error.
 */
int
pg_diff_boolin(const char *in_str, int *result)
{
	const char *str;
	size_t		len;

	pg_diff_errcode = 0;

	/*
	 * Skip leading and trailing whitespace
	 */
	str = in_str;
	while (isspace((unsigned char) *str))
		str++;

	len = strlen(str);
	while (len > 0 && isspace((unsigned char) str[len - 1]))
		len--;

	if (pg_diff_parse_bool_with_len(str, len, result))
		return 0;

	pg_diff_errcode = ERRCODE_INVALID_TEXT_REPRESENTATION;
	return 1;
}

/* boolout — verbatim stores (SHIM 3: palloc(2) -> caller buffer) */
void
pg_diff_boolout(int b, char *out2)
{
	char	   *result = out2;

	result[0] = (b) ? 't' : 'f';
	result[1] = '\0';
}

/* boolrecv — verbatim decision (SHIM 4: byte is the parameter) */
int
pg_diff_boolrecv(int ext)
{
	return ext != 0;
}

/* booltext — verbatim selection (SHIM 3: literal copied to caller buffer) */
int
pg_diff_booltext(int arg1, char *out8)
{
	const char *str;

	if (arg1)
		str = "true";
	else
		str = "false";

	strcpy(out8, str);
	return (int) strlen(str);
}

/* comparison bodies — verbatim expressions (SHIM 1: bool as int 0/1) */
int
pg_diff_booleq(int arg1, int arg2)
{
	return arg1 == arg2;
}

int
pg_diff_boolne(int arg1, int arg2)
{
	return arg1 != arg2;
}

int
pg_diff_boollt(int arg1, int arg2)
{
	return arg1 < arg2;
}

int
pg_diff_boolgt(int arg1, int arg2)
{
	return arg1 > arg2;
}

int
pg_diff_boolle(int arg1, int arg2)
{
	return arg1 <= arg2;
}

int
pg_diff_boolge(int arg1, int arg2)
{
	return arg1 >= arg2;
}

int
pg_diff_booland_statefunc(int a, int b)
{
	return a && b;
}

int
pg_diff_boolor_statefunc(int a, int b)
{
	return a || b;
}

/* ---- bool.c aggregate support — verbatim mutations (SHIM 5/6) ---- */

typedef struct BoolAggState
{
	int64		aggcount;		/* number of non-null values aggregated */
	int64		aggtrue;		/* number of values aggregated that are true */
} BoolAggState;

/*
 * bool_accum. state_isnull models PG_ARGISNULL(0); on first call the
 * caller's storage plays the makeBoolAggState aggcontext allocation
 * (zero-initialized here exactly as makeBoolAggState does).
 */
void
pg_diff_bool_accum(int state_isnull, BoolAggState *state,
				   int val_isnull, int val)
{
	pg_diff_errcode = 0;

	/* Create the state data on first call */
	if (state_isnull)
	{
		state->aggcount = 0;
		state->aggtrue = 0;
	}

	if (!val_isnull)
	{
		state->aggcount++;
		if (val)
			state->aggtrue++;
	}
}

/* bool_accum_inv; returns 1 with errcode 5 on the NULL-state elog arm */
int
pg_diff_bool_accum_inv(int state_isnull, BoolAggState *state,
					   int val_isnull, int val)
{
	pg_diff_errcode = 0;

	/* bool_accum should have created the state data */
	if (state_isnull)
	{
		pg_diff_errcode = PG_DIFF_ERR_INTERNAL;
		return 1;
	}

	if (!val_isnull)
	{
		state->aggcount--;
		if (val)
			state->aggtrue--;
	}
	return 0;
}

/* bool_alltrue — verbatim decisions (SHIM 6: NULL -> -1) */
int
pg_diff_bool_alltrue(int state_isnull, const BoolAggState *state)
{
	/* if there were no non-null values, return NULL */
	if (state_isnull || state->aggcount == 0)
		return -1;

	/* true if all non-null values are true */
	return state->aggtrue == state->aggcount;
}

int
pg_diff_bool_anytrue(int state_isnull, const BoolAggState *state)
{
	/* if there were no non-null values, return NULL */
	if (state_isnull || state->aggcount == 0)
		return -1;

	/* true if any non-null value is true */
	return state->aggtrue > 0;
}
