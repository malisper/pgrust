/*
 * Vendored PostgreSQL C for Kani C≡Rust equivalence proofs (proof-bool).
 *
 * Provenance:
 *   - parse_bool_with_len, boolout core, booleq, boolne, boollt, boolgt,
 *     boolle, boolge: src/backend/utils/adt/bool.c
 *     fetched from https://raw.githubusercontent.com/postgres/postgres/master/
 *     src/backend/utils/adt/bool.c, master, 2026-07-28.
 *   - pg_strncasecmp: src/port/pgstrcasecmp.c, same repo/ref/date.
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 * Bodies are verbatim except the shims documented below. Functions renamed
 * with a pg_ prefix (pg_strncasecmp keeps its upstream name).
 *
 * Shims (plumbing only, never logic):
 *   1. fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures. The Datum
 *      wrappers' bodies (the comparison expressions, the boolout store) are
 *      kept verbatim inside the plain-signature functions.
 *   2. bool return / bool args cross the FFI boundary as int (Kani lowers
 *      Rust unit/bool poorly against C _Bool at the goto-cc link; harnesses
 *      pass 0/1 only, matching the Datum bool domain).
 *   3. boolout: palloc(2) -> caller-provided 2-byte buffer. Store statements
 *      verbatim.
 *   4. pg_strncasecmp: the IS_HIGHBIT_SET branch calls locale-dependent
 *      isupper()/tolower() (libc — no Kani model). Shimmed to the C-locale
 *      definition, where isupper(ch) is false for every high-bit-set byte,
 *      i.e. the branch is a no-op and ch passes through unchanged. This
 *      matches Rust's u8::to_ascii_lowercase. Recorded as a fence: proofs
 *      hold for C-locale semantics of high-bit bytes.
 *   5. size_t len parameters passed as unsigned long (== size_t on this
 *      LP64 host).
 *
 * Postgres compiles with -fwrapv; CBMC's two's-complement default matches.
 */

#include <stddef.h>
#include <stdint.h>

#ifndef __cplusplus
typedef _Bool bool;
#define true 1
#define false 0
#endif

#define IS_HIGHBIT_SET(ch) ((unsigned char) (ch) & 0x80)

/* Shim 4: C-locale isupper() is false for all bytes >= 0x80. */
#define pg_shim_isupper_highbit(ch) (0)

/*
 * src/port/pgstrcasecmp.c pg_strncasecmp — verbatim except shim 4
 * (the isupper/tolower high-bit branch reduced to its C-locale value).
 */
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
			else if (IS_HIGHBIT_SET(ch1) && pg_shim_isupper_highbit(ch1))
				ch1 = ch1;		/* shim 4: C-locale tolower is identity here */

			if (ch2 >= 'A' && ch2 <= 'Z')
				ch2 += 'a' - 'A';
			else if (IS_HIGHBIT_SET(ch2) && pg_shim_isupper_highbit(ch2))
				ch2 = ch2;		/* shim 4 */

			if (ch1 != ch2)
				return (int) ch1 - (int) ch2;
		}
		if (ch1 == 0)
			break;
	}
	return 0;
}

/* bool.c parse_bool_with_len — body verbatim. */
static bool
parse_bool_with_len_core(const char *value, size_t len, bool *result)
{
	/* Check the most-used possibilities first. */
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}

/* Shim 1/2: int-boundary wrapper for the harness. */
int
pg_parse_bool_with_len(const char *value, unsigned long len, int *result)
{
	bool		res = false;
	bool		ok = parse_bool_with_len_core(value, (size_t) len, &res);

	*result = res ? 1 : 0;
	return ok ? 1 : 0;
}

/* bool.c boolout — stores verbatim; shims 1 (fmgr) and 3 (palloc->buffer). */
int
pg_boolout(int b_arg, char *result)
{
	bool		b = (bool) b_arg;

	result[0] = (b) ? 't' : 'f';
	result[1] = '\0';
	return 0;
}

/* bool.c comparison family — expressions verbatim; shims 1 and 2. */
int
pg_booleq(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 == arg2);
}

int
pg_boolne(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 != arg2);
}

int
pg_boollt(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 < arg2);
}

int
pg_boolgt(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 > arg2);
}

int
pg_boolle(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 <= arg2);
}

int
pg_boolge(int a1, int a2)
{
	bool		arg1 = (bool) a1;
	bool		arg2 = (bool) a2;

	return (arg1 >= arg2);
}

/*
 * bool_accum / bool_accum_inv / bool_alltrue / bool_anytrue (pg_proc oids
 * 3496-3499), vendored from REL_18_STABLE src/backend/utils/adt/bool.c,
 * and int4_bool / bool_int4 (oids 2557/2558) from REL_18_STABLE
 * src/backend/utils/adt/int.c (fetched 2026-07-28).
 *
 * SHIMS (transition/finalizer/cast expressions verbatim):
 *   - BoolAggState declared locally (bool.c layout: two int64 counters).
 *   - fmgr frames flattened: PG_ARGISNULL(0) -> has_state flag +
 *     (in_count,in_true) value pair; makeBoolAggState (agg-context palloc,
 *     plus the C "called in aggregate context" check) -> local zero-init
 *     state. The AGGREGATE CONTEXT is out of scope: these shims model the
 *     value transition only.
 *   - bool_accum_inv's elog(ERROR, "bool_accum_inv called with NULL
 *     state") -> *err = 1 sentinel.
 *   - PG_RETURN_POINTER(state) -> counters written to out params;
 *     PG_RETURN_NULL() in the finalizers -> *isnull = 1.
 */

typedef struct PgProofBoolAggState
{
	int64_t		aggcount;		/* number of non-null values aggregated */
	int64_t		aggtrue;		/* number of values aggregated that are true */
} PgProofBoolAggState;

int
pg_bool_accum(int has_state, int64_t in_count, int64_t in_true,
			  int has_val, int val,
			  int64_t *out_count, int64_t *out_true)
{
	PgProofBoolAggState state_ = {0, 0};
	PgProofBoolAggState *state = &state_;

	if (has_state)
	{
		state->aggcount = in_count;
		state->aggtrue = in_true;
	}

	if (has_val)
	{
		state->aggcount++;
		if (val)
			state->aggtrue++;
	}

	*out_count = state->aggcount;
	*out_true = state->aggtrue;
	return 0;
}

int
pg_bool_accum_inv(int has_state, int64_t in_count, int64_t in_true,
				  int has_val, int val,
				  int64_t *out_count, int64_t *out_true, int *err)
{
	PgProofBoolAggState state_ = {0, 0};
	PgProofBoolAggState *state = &state_;

	*err = 0;
	if (!has_state)
	{
		*err = 1;				/* elog(ERROR, "bool_accum_inv called with
								 * NULL state") */
		return 0;
	}
	state->aggcount = in_count;
	state->aggtrue = in_true;

	if (has_val)
	{
		state->aggcount--;
		if (val)
			state->aggtrue--;
	}

	*out_count = state->aggcount;
	*out_true = state->aggtrue;
	return 0;
}

int
pg_bool_alltrue(int has_state, int64_t aggcount, int64_t aggtrue, int *isnull)
{
	*isnull = 0;
	/* if there were no non-null values, return NULL */
	if (!has_state || aggcount == 0)
	{
		*isnull = 1;
		return 0;
	}

	/* true if all non-null values are true */
	return aggtrue == aggcount;
}

int
pg_bool_anytrue(int has_state, int64_t aggcount, int64_t aggtrue, int *isnull)
{
	*isnull = 0;
	/* if there were no non-null values, return NULL */
	if (!has_state || aggcount == 0)
	{
		*isnull = 1;
		return 0;
	}

	/* true if any non-null value is true */
	return aggtrue > 0;
}

int
pg_int4_bool(int32_t arg)
{
	if (arg == 0)
		return 0;
	else
		return 1;
}

int32_t
pg_bool_int4(int arg)
{
	if (arg == 0)
		return 0;
	else
		return 1;
}

/* ==================================================================== */
/* WAVE (2026-07-30): boolsend (pg_proc oid 2437) / booltext (2971).    */
/*                                                                      */
/* Provenance (REL_18_STABLE, fetched 2026-07-30):                      */
/*   src/backend/utils/adt/bool.c    (boolsend, booltext bodies)        */
/*   src/backend/libpq/pqformat.c    (pq_begintypsend, pq_sendbyte,     */
/*                                    pq_endtypsend semantics)          */
/*                                                                      */
/* SHIMS (plumbing only, never logic; the proofs/uuid pg_uuid_send      */
/* wire conventions):                                                   */
/*   W1. fmgr PG_FUNCTION_ARGS unwrapped -> plain int arg (shim 2:      */
/*       bool crosses the FFI boundary as int).                         */
/*   W2. pq_begintypsend + pq_sendbyte + pq_endtypsend -> caller-       */
/*       provided out buffer; SET_VARSIZE = 4-byte little-endian        */
/*       varlena header (total_len << 2), payload after it. Returns     */
/*       the total image length.                                        */
/*   W3. booltext's cstring_to_text(str) -> the same caller-buffer      */
/*       varlena image ((VARHDRSZ + strlen) << 2 LE header + bytes);    */
/*       the str selection ("true"/"false") is verbatim.                */
/* ==================================================================== */

/* bool.c boolsend: pq_begintypsend + pq_sendbyte(arg1 ? 1 : 0)
 * + pq_endtypsend -> 5-byte image (4B header + 1 payload byte) */
int32_t
pg_boolsend(int b_arg, unsigned char *out /* [5] */ )
{
	bool		arg1 = (bool) b_arg;	/* shim W1 */
	uint32_t	hdr = (uint32_t) 5 << 2;	/* shim W2 */

	out[4] = (unsigned char) (arg1 ? 1 : 0);	/* pq_sendbyte, verbatim value */
	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	return 5;
}

/* bool.c booltext: cstring_to_text("true"/"false"); selection verbatim,
 * text packing per shim W3. Returns the total image length (8 or 9). */
int32_t
pg_booltext(int b_arg, unsigned char *out /* [9] */ )
{
	bool		arg1 = (bool) b_arg;	/* shim W1 */
	const char *str;
	int			len = 0;
	uint32_t	hdr;
	int			i;

	if (arg1)
		str = "true";
	else
		str = "false";

	/* shim W3: cstring_to_text -> caller-buffer varlena image */
	while (str[len] != '\0')
		len++;
	hdr = (uint32_t) (4 + len) << 2;
	out[0] = (unsigned char) (hdr & 0xFF);
	out[1] = (unsigned char) ((hdr >> 8) & 0xFF);
	out[2] = (unsigned char) ((hdr >> 16) & 0xFF);
	out[3] = (unsigned char) ((hdr >> 24) & 0xFF);
	for (i = 0; i < len; i++)
		out[4 + i] = (unsigned char) str[i];
	return 4 + len;
}

/*
 * booland_statefunc / boolor_statefunc — bool.c:300-303 / :309-312 verbatim
 * (Stamp 18.3, 62d6c7d3df). SHIM: PG_FUNCTION_ARGS unwrapping -> plain int
 * (0/1) args/return, same convention as pg_booleq above (shim 2 in the
 * provenance header).
 */
int
pg_booland_statefunc(int a1, int a2)
{
	return (a1 != 0) && (a2 != 0);
}

int
pg_boolor_statefunc(int a1, int a2)
{
	return (a1 != 0) || (a2 != 0);
}
