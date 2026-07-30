/*
 * Vendored from postgres master (fetched 2026-07-28 via
 * raw.githubusercontent.com/postgres/postgres/master):
 *   src/backend/utils/adt/int.c   — int2/int4/int24/int42 {eq,ne,lt,le,gt,ge}
 *   src/backend/utils/adt/int8.c  — int8/int84/int48/int82/int28 {eq,ne,lt,le,gt,ge}
 *   src/backend/utils/adt/oid.c   — oid {eq,ne,lt,le,gt,ge}
 *   src/backend/access/nbtree/nbtcompare.c — btint2cmp, btint4cmp, btint8cmp,
 *                                            btoidcmp (A_LESS_THAN_B /
 *                                            A_GREATER_THAN_B kept verbatim)
 * REL_18_STABLE conformance: zero code drift vs REL_18_STABLE (provenance
 * audit, proofs/PROVENANCE-AUDIT.md, 2026-07-28).
 *
 * SHIMS (everything else is verbatim):
 *  - fmgr wrappers removed: PG_GETARG_INT16/INT32/INT64/OID become plain
 *    typed parameters (parameter names kept from the C: arg1/arg2 in int.c
 *    and oid.c, val1/val2 in int8.c, a/b in nbtcompare.c).
 *  - PG_RETURN_BOOL(expr) -> `return expr;` with an int return type (C bool
 *    and Datum-bool are both 0/1; int avoids the Kani Unit/void FFI trap).
 *  - PG_RETURN_INT32(x) -> `return x;` (int32_t return).
 *  - C typedefs mapped: int16 -> int16_t, int32 -> int32_t, int64 -> int64_t,
 *    Oid -> uint32_t (postgres: typedef unsigned int Oid).
 *  - all functions renamed with a pg_ prefix.
 * No logic is shimmed: every comparison expression, cast, and branch is
 * byte-for-byte the postgres body.
 */

#include <stdint.h>

/* nbtcompare.c, verbatim */
#define A_LESS_THAN_B		(-1)
#define A_GREATER_THAN_B	1

/* ---------- int.c ---------- */

int pg_int4eq(int32_t arg1, int32_t arg2) { return arg1 == arg2; }
int pg_int4ne(int32_t arg1, int32_t arg2) { return arg1 != arg2; }
int pg_int4lt(int32_t arg1, int32_t arg2) { return arg1 < arg2; }
int pg_int4le(int32_t arg1, int32_t arg2) { return arg1 <= arg2; }
int pg_int4gt(int32_t arg1, int32_t arg2) { return arg1 > arg2; }
int pg_int4ge(int32_t arg1, int32_t arg2) { return arg1 >= arg2; }

int pg_int2eq(int16_t arg1, int16_t arg2) { return arg1 == arg2; }
int pg_int2ne(int16_t arg1, int16_t arg2) { return arg1 != arg2; }
int pg_int2lt(int16_t arg1, int16_t arg2) { return arg1 < arg2; }
int pg_int2le(int16_t arg1, int16_t arg2) { return arg1 <= arg2; }
int pg_int2gt(int16_t arg1, int16_t arg2) { return arg1 > arg2; }
int pg_int2ge(int16_t arg1, int16_t arg2) { return arg1 >= arg2; }

int pg_int24eq(int16_t arg1, int32_t arg2) { return arg1 == arg2; }
int pg_int24ne(int16_t arg1, int32_t arg2) { return arg1 != arg2; }
int pg_int24lt(int16_t arg1, int32_t arg2) { return arg1 < arg2; }
int pg_int24le(int16_t arg1, int32_t arg2) { return arg1 <= arg2; }
int pg_int24gt(int16_t arg1, int32_t arg2) { return arg1 > arg2; }
int pg_int24ge(int16_t arg1, int32_t arg2) { return arg1 >= arg2; }

int pg_int42eq(int32_t arg1, int16_t arg2) { return arg1 == arg2; }
int pg_int42ne(int32_t arg1, int16_t arg2) { return arg1 != arg2; }
int pg_int42lt(int32_t arg1, int16_t arg2) { return arg1 < arg2; }
int pg_int42le(int32_t arg1, int16_t arg2) { return arg1 <= arg2; }
int pg_int42gt(int32_t arg1, int16_t arg2) { return arg1 > arg2; }
int pg_int42ge(int32_t arg1, int16_t arg2) { return arg1 >= arg2; }

/* ---------- int8.c ---------- */

int pg_int8eq(int64_t val1, int64_t val2) { return val1 == val2; }
int pg_int8ne(int64_t val1, int64_t val2) { return val1 != val2; }
int pg_int8lt(int64_t val1, int64_t val2) { return val1 < val2; }
int pg_int8gt(int64_t val1, int64_t val2) { return val1 > val2; }
int pg_int8le(int64_t val1, int64_t val2) { return val1 <= val2; }
int pg_int8ge(int64_t val1, int64_t val2) { return val1 >= val2; }

int pg_int84eq(int64_t val1, int32_t val2) { return val1 == val2; }
int pg_int84ne(int64_t val1, int32_t val2) { return val1 != val2; }
int pg_int84lt(int64_t val1, int32_t val2) { return val1 < val2; }
int pg_int84gt(int64_t val1, int32_t val2) { return val1 > val2; }
int pg_int84le(int64_t val1, int32_t val2) { return val1 <= val2; }
int pg_int84ge(int64_t val1, int32_t val2) { return val1 >= val2; }

int pg_int48eq(int32_t val1, int64_t val2) { return val1 == val2; }
int pg_int48ne(int32_t val1, int64_t val2) { return val1 != val2; }
int pg_int48lt(int32_t val1, int64_t val2) { return val1 < val2; }
int pg_int48gt(int32_t val1, int64_t val2) { return val1 > val2; }
int pg_int48le(int32_t val1, int64_t val2) { return val1 <= val2; }
int pg_int48ge(int32_t val1, int64_t val2) { return val1 >= val2; }

int pg_int82eq(int64_t val1, int16_t val2) { return val1 == val2; }
int pg_int82ne(int64_t val1, int16_t val2) { return val1 != val2; }
int pg_int82lt(int64_t val1, int16_t val2) { return val1 < val2; }
int pg_int82gt(int64_t val1, int16_t val2) { return val1 > val2; }
int pg_int82le(int64_t val1, int16_t val2) { return val1 <= val2; }
int pg_int82ge(int64_t val1, int16_t val2) { return val1 >= val2; }

int pg_int28eq(int16_t val1, int64_t val2) { return val1 == val2; }
int pg_int28ne(int16_t val1, int64_t val2) { return val1 != val2; }
int pg_int28lt(int16_t val1, int64_t val2) { return val1 < val2; }
int pg_int28gt(int16_t val1, int64_t val2) { return val1 > val2; }
int pg_int28le(int16_t val1, int64_t val2) { return val1 <= val2; }
int pg_int28ge(int16_t val1, int64_t val2) { return val1 >= val2; }

/* ---------- oid.c ---------- */

int pg_oideq(uint32_t arg1, uint32_t arg2) { return arg1 == arg2; }
int pg_oidne(uint32_t arg1, uint32_t arg2) { return arg1 != arg2; }
int pg_oidlt(uint32_t arg1, uint32_t arg2) { return arg1 < arg2; }
int pg_oidle(uint32_t arg1, uint32_t arg2) { return arg1 <= arg2; }
int pg_oidge(uint32_t arg1, uint32_t arg2) { return arg1 >= arg2; }
int pg_oidgt(uint32_t arg1, uint32_t arg2) { return arg1 > arg2; }

/* ---------- nbtcompare.c ---------- */

int32_t pg_btint2cmp(int16_t a, int16_t b)
{
	return (int32_t) a - (int32_t) b;
}

int32_t pg_btint4cmp(int32_t a, int32_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint8cmp(int64_t a, int64_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btoidcmp(uint32_t a, uint32_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

/* ----------------------------------------------------------------------
 * WAVE (tail-triage warm-ups, added 2026-07-29): nbtcompare.c btboolcmp +
 * the six mixed-width btint cmps, and datum.c btequalimage — vendored from
 * postgres REL_18_STABLE (raw.githubusercontent.com, fetched 2026-07-29).
 * Same shims as the header documents (fmgr unwrap -> typed params,
 * PG_RETURN_INT32/BOOL -> int32_t/int returns; C `bool` args ride as
 * unsigned char, the C ABI representation). Bodies verbatim.
 * ---------------------------------------------------------------------- */

/* nbtcompare.c btboolcmp: PG_RETURN_INT32((int32) a - (int32) b) */
int32_t pg_btboolcmp(unsigned char a, unsigned char b)
{
	return (int32_t) a - (int32_t) b;
}

int32_t pg_btint48cmp(int32_t a, int64_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint84cmp(int64_t a, int32_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint24cmp(int16_t a, int32_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint42cmp(int32_t a, int16_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint28cmp(int16_t a, int64_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

int32_t pg_btint82cmp(int64_t a, int16_t b)
{
	if (a > b)
		return A_GREATER_THAN_B;
	else if (a == b)
		return 0;
	else
		return A_LESS_THAN_B;
}

/* datum.c btequalimage: PG_RETURN_BOOL(true); opcintype arg unused, kept
 * for signature fidelity */
int pg_btequalimage(uint32_t opcintype)
{
	/* Oid		opcintype = PG_GETARG_OID(0); */
	(void) opcintype;
	return 1;
}
