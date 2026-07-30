/*
 * pg_enum_cmp.c — vendored PostgreSQL enum comparison engine + wrappers.
 *
 * Provenance: src/backend/utils/adt/enum.c, REL_18_STABLE, fetched
 * 2026-07-30. Functions: enum_cmp_internal, enum_lt, enum_le, enum_ge,
 * enum_gt, enum_smaller, enum_larger, enum_cmp. Bodies verbatim except the
 * shims below.
 *
 * Shims (plumbing/seams only, never logic):
 *  - fmgr unwrap: PG_FUNCTION_ARGS -> plain (Oid a, Oid b, int has_memo,
 *    Oid memo_typeoid, <out>, int *err). `has_memo`/`memo_typeoid` model
 *    fcinfo->flinfo->fn_extra (C caches a TypeCacheEntry*; the model
 *    carries the enum type OID the entry is keyed by, matching the Rust
 *    side's Oid fn_extra carrier). Assert(fcinfo->flinfo != NULL) is
 *    compiled out by the shim header, production parity.
 *  - ENUMOID syscache seam (SearchSysCache1/GETSTRUCT/ReleaseSysCache):
 *    replaced by the shared lookup oracle statics pg_enum_oracle_lookup_
 *    found / _typeoid. The Rust harness stubs
 *    syscache_seams::lookup_pg_enum_by_oid::call to read the SAME statics,
 *    so the proof quantifies over every possible catalog answer; syscache
 *    internals leave the proof (state-seam pattern).
 *  - compare_values_of_enum seam (typcache.c): replaced by the shared cmp
 *    oracle static pg_enum_oracle_cmp; the call's (typeoid, arg1, arg2)
 *    inputs are RECORDED in pg_enum_trace_* so the harness can assert
 *    input/trace parity against the Rust seam stub's recording — the
 *    typcache sort-order engine itself is catalog state, out of proof.
 *  - lookup_type_cache(typeoid, 0): the C caches a TypeCacheEntry* keyed
 *    by typeoid; the model carries typeoid itself (see has_memo above).
 *  - ereport(ERROR, ...) -> PROOF_EREPORT_FLAG convention: *err = 1 +
 *    sentinel return; error class asserted Rust-side (22P03).
 *  - fn_extra write-back: fcinfo->flinfo->fn_extra = tcache -> records
 *    pg_enum_memo_written(_flag) for memo-write parity.
 */

#include "../../support/c/pg_proof_shim.h"

/* ---- shared oracle statics (written by the Rust harness) ---- */
Oid pg_enum_oracle_lookup_typeoid; /* ENUMOID answer: enumtypid for arg1 */
int pg_enum_oracle_lookup_found;   /* 0 => syscache miss (error arm) */
int pg_enum_oracle_cmp;            /* compare_values_of_enum answer */

/* ---- trace recording (read back by the harness for parity asserts) ---- */
int pg_enum_trace_cmp_called;
Oid pg_enum_trace_cmp_typeoid;
Oid pg_enum_trace_cmp_arg1;
Oid pg_enum_trace_cmp_arg2;
int pg_enum_memo_written_flag;
Oid pg_enum_memo_written;

int
pg_enum_trace_reset(void)
{
	pg_enum_trace_cmp_called = 0;
	pg_enum_trace_cmp_typeoid = 0;
	pg_enum_trace_cmp_arg1 = 0;
	pg_enum_trace_cmp_arg2 = 0;
	pg_enum_memo_written_flag = 0;
	pg_enum_memo_written = 0;
	return 0;
}

/* compare_values_of_enum seam: record inputs, answer from the oracle. */
static int
compare_values_of_enum(Oid tcache_typeoid, Oid arg1, Oid arg2)
{
	pg_enum_trace_cmp_called = 1;
	pg_enum_trace_cmp_typeoid = tcache_typeoid;
	pg_enum_trace_cmp_arg1 = arg1;
	pg_enum_trace_cmp_arg2 = arg2;
	return pg_enum_oracle_cmp;
}

/*
 * enum_cmp_internal is the common engine for all the visible comparison
 * functions, except for enum_eq and enum_ne which can just check for OID
 * equality directly.
 *
 * Body verbatim modulo the documented shims; the syscache block keeps the
 * original control flow (miss => ereport ERROR 22P03).
 */
static int
enum_cmp_internal(Oid arg1, Oid arg2, int has_memo, Oid memo_typeoid, int *err)
{
	/* Assert(fcinfo->flinfo != NULL); -- compiled out (shim header) */

	/* Equal OIDs are equal no matter what */
	if (arg1 == arg2)
		return 0;

	/* Fast path: even-numbered Oids are known to compare correctly */
	if ((arg1 & 1) == 0 && (arg2 & 1) == 0)
	{
		if (arg1 < arg2)
			return -1;
		else
			return 1;
	}

	/* Locate the typcache entry for the enum type */
	if (!has_memo)
	{
		Oid			typeoid;

		/* Get the OID of the enum type containing arg1 (ENUMOID seam) */
		if (!pg_enum_oracle_lookup_found)
		{
			/* ereport(ERROR, errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
			 *         errmsg("invalid internal value for enum: %u", arg1)); */
			*err = 1;
			return 0;
		}
		typeoid = pg_enum_oracle_lookup_typeoid;
		/* Now locate and remember the typcache entry
		 * (lookup_type_cache + fn_extra write, modeled as the typeoid) */
		memo_typeoid = typeoid;
		pg_enum_memo_written_flag = 1;
		pg_enum_memo_written = typeoid;
	}

	/* The remaining comparison logic is in typcache.c (cmp seam) */
	return compare_values_of_enum(memo_typeoid, arg1, arg2);
}

int
pg_enum_lt(Oid a, Oid b, int has_memo, Oid memo_typeoid, int *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) < 0;
	return 0;
}

int
pg_enum_le(Oid a, Oid b, int has_memo, Oid memo_typeoid, int *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) <= 0;
	return 0;
}

int
pg_enum_ge(Oid a, Oid b, int has_memo, Oid memo_typeoid, int *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) >= 0;
	return 0;
}

int
pg_enum_gt(Oid a, Oid b, int has_memo, Oid memo_typeoid, int *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) > 0;
	return 0;
}

int
pg_enum_smaller(Oid a, Oid b, int has_memo, Oid memo_typeoid, Oid *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) < 0 ? a : b;
	return 0;
}

int
pg_enum_larger(Oid a, Oid b, int has_memo, Oid memo_typeoid, Oid *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err) > 0 ? a : b;
	return 0;
}

int
pg_enum_cmp(Oid a, Oid b, int has_memo, Oid memo_typeoid, int *out, int *err)
{
	*out = enum_cmp_internal(a, b, has_memo, memo_typeoid, err);
	return 0;
}
