/*
 * Vendored PostgreSQL C for Kani dual-execution proofs: small remaining
 * pg_proc families batch (enum_eq/enum_ne, oidlarger/oidsmaller,
 * int8inc/int8dec, bit_bit_count, bytea_bit_count).
 *
 * PROVENANCE (all REL_18_STABLE, raw.githubusercontent.com, fetched
 * 2026-07-28):
 *   src/backend/utils/adt/enum.c     enum_eq (l.324), enum_ne (l.333)
 *   src/backend/utils/adt/oid.c      oidlarger (l.355), oidsmaller (l.364)
 *   src/backend/utils/adt/int8.c     int8inc (l.719), int8dec (l.757)
 *   src/backend/utils/adt/varbit.c   bit_bit_count (l.1211)
 *   src/backend/utils/adt/varlena.c  bytea_bit_count (l.3254)
 *   src/include/common/int.h         pg_add_s64_overflow, pg_sub_s64_overflow
 *   src/port/pg_bitutils.c           pg_number_of_ones, pg_popcount64,
 *                                    pg_popcount (portable configuration;
 *                                    byte-identical to the ALREADY PROVED
 *                                    vendoring in proofs/bitutils/
 *                                    c_bitutils.c — bit_count composes on
 *                                    that proved kernel)
 *
 * SHIMS (plumbing only, never logic) — every departure from upstream:
 *   1. pg_ prefix on every function name.
 *   2. fmgr PG_FUNCTION_ARGS unwrapping -> plain C signatures:
 *      PG_GETARG_OID -> uint32 params; PG_GETARG_INT64 -> int64 param;
 *      PG_RETURN_BOOL -> int return; PG_RETURN_OID -> uint32 return;
 *      PG_RETURN_INT64 -> int64 return. Bodies between the arg fetch and
 *      the return are verbatim.
 *   3. int8inc/int8dec: USE_FLOAT8_BYVAL is defined on every 64-bit build
 *      (all pgrust targets), so the `#ifndef USE_FLOAT8_BYVAL` aggregate
 *      modify-in-place branch is compiled OUT, exactly as in a production
 *      64-bit postgres; what remains is the "dumb way" branch, verbatim.
 *      ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of
 *      range") -> return 1 (0 on success with *result set) — the
 *      proofs/cash ereport-flag pattern.
 *   4. pg_add_s64_overflow / pg_sub_s64_overflow: int.h's
 *      HAVE__BUILTIN_OP_OVERFLOW arm (__builtin_add/sub_overflow), the arm
 *      every production compiler takes; CBMC models these builtins.
 *   5. bit_bit_count: PG_GETARG_VARBIT_P + VARBITS/VARBITBYTES macro
 *      unwrapping -> (payload pointer, bitlen) params. VARBITS(arg) is the
 *      bit_dat payload past the 8-byte VarBit header (passed directly);
 *      VARBITBYTES(arg) = (bitlen + 7) / 8 is inlined verbatim from the
 *      utils/varbit.h macro. Detoast is out of scope (caller contract, the
 *      bytea-cmp precedent).
 *   6. bytea_bit_count: PG_GETARG_BYTEA_PP + VARDATA_ANY/VARSIZE_ANY_EXHDR
 *      -> (data pointer, byte length) params; same caller contract.
 *   7. pg_popcount: the static-inline dispatcher from pg_bitutils.h with
 *      pg_popcount_optimized = the portable version (the
 *      !HAVE_X86_64_POPCNTQ && !USE_NEON configuration) — the identical
 *      configuration choice, argued and PROVED, in proofs/bitutils.
 *      SIZEOF_VOID_P is 8 (all pgrust targets), so the aligned-word loop
 *      is compiled in.
 *   Postgres compiles with -fwrapv; CBMC's two's-complement wrap matches
 *   (no signed overflow is reachable here anyway: inc/dec go through the
 *   overflow builtins).
 */

/* shared suite shim boilerplate: typedefs (int32/int64/Oid/...), unlikely() */
#include "../../support/c/pg_proof_shim.h"

#define UINT64CONST(x) (x##ULL)
/* verbatim shape from c.h */
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

/* ---------- common/int.h: HAVE__BUILTIN_OP_OVERFLOW arm ---------- */

static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_add_overflow(a, b, result);
}

static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
	return __builtin_sub_overflow(a, b, result);
}

/* ---------- enum.c: enum_eq / enum_ne ---------- */

int
pg_enum_eq(Oid a, Oid b)
{
	return a == b;
}

int
pg_enum_ne(Oid a, Oid b)
{
	return a != b;
}

/* ---------- oid.c: oidlarger / oidsmaller ---------- */

Oid
pg_oidlarger(Oid arg1, Oid arg2)
{
	return (arg1 > arg2) ? arg1 : arg2;
}

Oid
pg_oidsmaller(Oid arg1, Oid arg2)
{
	return (arg1 < arg2) ? arg1 : arg2;
}

/* ---------- int8.c: int8inc / int8dec (ereport-flag shim) ---------- */

int
pg_int8inc(int64 arg, int64 *result)
{
	/* Not called as an aggregate, so just do it the dumb way */
	if (unlikely(pg_add_s64_overflow(arg, 1, result)))
		return 1;				/* ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
								 * "bigint out of range") */
	return 0;
}

int
pg_int8dec(int64 arg, int64 *result)
{
	if (unlikely(pg_sub_s64_overflow(arg, 1, result)))
		return 1;				/* ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
								 * "bigint out of range") */
	return 0;
}

/* ---------- src/port/pg_bitutils.c: popcount kernel (verbatim; identical
 * vendoring to proofs/bitutils/c_bitutils.c where it is PROVED) ---------- */

const uint8 pg_number_of_ones[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

static int
pg_popcount64_k(uint64 word)
{
	word -= (word >> 1) & UINT64CONST(0x5555555555555555);
	word = (word & UINT64CONST(0x3333333333333333)) +
		((word >> 2) & UINT64CONST(0x3333333333333333));
	word = (word + (word >> 4)) & UINT64CONST(0xf0f0f0f0f0f0f0f);
	return (word * UINT64CONST(0x101010101010101)) >> 56;
}

static uint64
pg_popcount_portable(const char *buf, int bytes)
{
	uint64		popcnt = 0;

	/* Process in 64-bit chunks if the buffer is aligned. */
	if (buf == (const char *) TYPEALIGN(8, buf))
	{
		const uint64 *words = (const uint64 *) buf;

		while (bytes >= 8)
		{
			popcnt += pg_popcount64_k(*words++);
			bytes -= 8;
		}

		buf = (const char *) words;
	}

	/* Process any remaining bytes */
	while (bytes--)
		popcnt += pg_number_of_ones[(unsigned char) *buf++];

	return popcnt;
}

/* pg_bitutils.h static-inline dispatcher, portable configuration */
static uint64
pg_popcount_shim(const char *buf, int bytes)
{
	if (bytes < 8)
	{
		uint64		popcnt = 0;

		while (bytes--)
			popcnt += pg_number_of_ones[(unsigned char) *buf++];
		return popcnt;
	}

	return pg_popcount_portable(buf, bytes);
}

/* ---------- varbit.c: bit_bit_count ---------- */

int64
pg_bit_bit_count(const unsigned char *varbits, int32 bitlen)
{
	/* VARBITBYTES(arg) = (arg->bit_len + 7) / 8 (utils/varbit.h) */
	return pg_popcount_shim((const char *) varbits, (bitlen + 7) / 8);
}

/* ---------- varlena.c: bytea_bit_count ---------- */

int64
pg_bytea_bit_count(const char *data, int len)
{
	return pg_popcount_shim(data, len);
}
