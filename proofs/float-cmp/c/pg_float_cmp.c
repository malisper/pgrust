/*
 * Vendored PostgreSQL C: the float comparator family.
 *
 * Provenance:
 *   - src/backend/utils/adt/float.c   @ postgres/postgres master
 *     (239eabda41e39de73c376000ba74bbeb8fe32a5c, fetched 2026-07-28)
 *   - src/include/utils/float.h      @ same ref (the NaN-aware inline
 *     comparison helpers float4_eq .. float8_ge, verbatim)
 *   REL_18_STABLE conformance: zero code drift vs REL_18_STABLE
 *   (provenance audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * Functions copied, bodies verbatim, renamed with pg_ prefix:
 *   float4{eq,ne,lt,le,gt,ge}, float8{...}, float48{...}, float84{...},
 *   btfloat4cmp, btfloat8cmp, btfloat48cmp, btfloat84cmp,
 *   float4_cmp_internal, float8_cmp_internal,
 *   float4larger, float4smaller, float8larger, float8smaller.
 *
 * Shims (plumbing only, never logic):
 *   - PG_FUNCTION_ARGS unwrapping -> plain C signatures (float4/float8 args
 *     by value, exactly what PG_GETARG_FLOAT4/8 deliver).
 *   - PG_RETURN_BOOL -> int return (0/1); Kani lowers Rust bool against
 *     C _Bool inconsistently, int is the established shim.
 *   - float4 -> float, float8 -> double (their c.h typedefs).
 *   - isnan comes from <math.h>; CBMC models it bit-exactly on IEEE-754.
 *
 * pg_float4eq_ieee at the bottom is NOT Postgres code: it is a deliberately
 * WRONG comparator (plain IEEE ==) used only by the negative-control
 * harness, which must fail at NaN==NaN. Never cite it as vendored C.
 */

#include <math.h>

typedef float float4;
typedef double float8;

/* ---- src/include/utils/float.h: NaN-aware comparisons, verbatim ----
 *
 * "We consider all NaNs to be equal and larger than any non-NaN. This is
 *  somewhat arbitrary; the important thing is to have a consistent sort
 *  order."
 */

static inline int
float4_eq(const float4 val1, const float4 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static inline int
float8_eq(const float8 val1, const float8 val2)
{
	return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static inline int
float4_ne(const float4 val1, const float4 val2)
{
	return isnan(val1) ? !isnan(val2) : isnan(val2) || val1 != val2;
}

static inline int
float8_ne(const float8 val1, const float8 val2)
{
	return isnan(val1) ? !isnan(val2) : isnan(val2) || val1 != val2;
}

static inline int
float4_lt(const float4 val1, const float4 val2)
{
	return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static inline int
float8_lt(const float8 val1, const float8 val2)
{
	return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static inline int
float4_le(const float4 val1, const float4 val2)
{
	return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static inline int
float8_le(const float8 val1, const float8 val2)
{
	return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static inline int
float4_gt(const float4 val1, const float4 val2)
{
	return !isnan(val2) && (isnan(val1) || val1 > val2);
}

static inline int
float8_gt(const float8 val1, const float8 val2)
{
	return !isnan(val2) && (isnan(val1) || val1 > val2);
}

static inline int
float4_ge(const float4 val1, const float4 val2)
{
	return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

static inline int
float8_ge(const float8 val1, const float8 val2)
{
	return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

/* ---- float.c: float4/float4 comparison operations ---- */

int
pg_float4_cmp_internal(float4 a, float4 b)
{
	if (float4_gt(a, b))
		return 1;
	if (float4_lt(a, b))
		return -1;
	return 0;
}

int pg_float4eq(float4 arg1, float4 arg2) { return float4_eq(arg1, arg2); }
int pg_float4ne(float4 arg1, float4 arg2) { return float4_ne(arg1, arg2); }
int pg_float4lt(float4 arg1, float4 arg2) { return float4_lt(arg1, arg2); }
int pg_float4le(float4 arg1, float4 arg2) { return float4_le(arg1, arg2); }
int pg_float4gt(float4 arg1, float4 arg2) { return float4_gt(arg1, arg2); }
int pg_float4ge(float4 arg1, float4 arg2) { return float4_ge(arg1, arg2); }

int pg_btfloat4cmp(float4 arg1, float4 arg2)
{
	return pg_float4_cmp_internal(arg1, arg2);
}

/* ---- float.c: float8/float8 comparison operations ---- */

int
pg_float8_cmp_internal(float8 a, float8 b)
{
	if (float8_gt(a, b))
		return 1;
	if (float8_lt(a, b))
		return -1;
	return 0;
}

int pg_float8eq(float8 arg1, float8 arg2) { return float8_eq(arg1, arg2); }
int pg_float8ne(float8 arg1, float8 arg2) { return float8_ne(arg1, arg2); }
int pg_float8lt(float8 arg1, float8 arg2) { return float8_lt(arg1, arg2); }
int pg_float8le(float8 arg1, float8 arg2) { return float8_le(arg1, arg2); }
int pg_float8gt(float8 arg1, float8 arg2) { return float8_gt(arg1, arg2); }
int pg_float8ge(float8 arg1, float8 arg2) { return float8_ge(arg1, arg2); }

int pg_btfloat8cmp(float8 arg1, float8 arg2)
{
	return pg_float8_cmp_internal(arg1, arg2);
}

/* ---- float.c: float48/float84 mixed-width operations ----
 * C widens float4 -> float8 (exact) and compares at float8. Casts verbatim.
 */

int pg_float48eq(float4 arg1, float8 arg2) { return float8_eq((float8) arg1, arg2); }
int pg_float48ne(float4 arg1, float8 arg2) { return float8_ne((float8) arg1, arg2); }
int pg_float48lt(float4 arg1, float8 arg2) { return float8_lt((float8) arg1, arg2); }
int pg_float48le(float4 arg1, float8 arg2) { return float8_le((float8) arg1, arg2); }
int pg_float48gt(float4 arg1, float8 arg2) { return float8_gt((float8) arg1, arg2); }
int pg_float48ge(float4 arg1, float8 arg2) { return float8_ge((float8) arg1, arg2); }

int pg_float84eq(float8 arg1, float4 arg2) { return float8_eq(arg1, (float8) arg2); }
int pg_float84ne(float8 arg1, float4 arg2) { return float8_ne(arg1, (float8) arg2); }
int pg_float84lt(float8 arg1, float4 arg2) { return float8_lt(arg1, (float8) arg2); }
int pg_float84le(float8 arg1, float4 arg2) { return float8_le(arg1, (float8) arg2); }
int pg_float84gt(float8 arg1, float4 arg2) { return float8_gt(arg1, (float8) arg2); }
int pg_float84ge(float8 arg1, float4 arg2) { return float8_ge(arg1, (float8) arg2); }

int pg_btfloat48cmp(float4 arg1, float8 arg2)
{
	/* widen float4 to float8 and then compare */
	return pg_float8_cmp_internal(arg1, arg2);
}

int pg_btfloat84cmp(float8 arg1, float4 arg2)
{
	/* widen float4 to float8 and then compare */
	return pg_float8_cmp_internal(arg1, arg2);
}

/* ---- float.c: larger/smaller (MAX()/MIN() aggregate support) ---- */

float4
pg_float4larger(float4 arg1, float4 arg2)
{
	float4		result;

	if (float4_gt(arg1, arg2))
		result = arg1;
	else
		result = arg2;
	return result;
}

float4
pg_float4smaller(float4 arg1, float4 arg2)
{
	float4		result;

	if (float4_lt(arg1, arg2))
		result = arg1;
	else
		result = arg2;
	return result;
}

float8
pg_float8larger(float8 arg1, float8 arg2)
{
	float8		result;

	if (float8_gt(arg1, arg2))
		result = arg1;
	else
		result = arg2;
	return result;
}

float8
pg_float8smaller(float8 arg1, float8 arg2)
{
	float8		result;

	if (float8_lt(arg1, arg2))
		result = arg1;
	else
		result = arg2;
	return result;
}

/* ---- NEGATIVE-CONTROL ONLY: not Postgres code ----
 * Plain IEEE ==: differs from float4_eq exactly on the NaN row/column.
 * The control harness pits fc_float4eq against this and MUST fail,
 * witnessing that the rig explores the NaN subspace.
 */
int pg_float4eq_ieee(float4 arg1, float4 arg2) { return arg1 == arg2; }
