/*
 * Vendored PostgreSQL C for the brin-opcinfo proof family (w2-fmgr lane,
 * 2026-07-30): the three non-minmax BRIN opclass opcinfo support procs.
 *
 * Provenance:
 *   - src/backend/access/brin/brin_bloom.c         (brin_bloom_opcinfo)
 *   - src/backend/access/brin/brin_minmax_multi.c  (brin_minmax_multi_opcinfo)
 *   - src/backend/access/brin/brin_inclusion.c     (brin_inclusion_opcinfo)
 *   - src/include/catalog/pg_type.dat: PG_BRIN_BLOOM_SUMMARYOID = 4600,
 *     PG_BRIN_MINMAX_MULTI_SUMMARYOID = 4601, BOOLOID = 16
 *   ref: postgres/postgres REL_18_STABLE
 *        (raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/...)
 *   fetched: 2026-07-30
 *
 * Shims (plumbing only, never logic — brin-minmax family precedent):
 *   - `pg_` prefix; fmgr PG_FUNCTION_ARGS unwrapping -> plain C signature;
 *     PG_RETURN_POINTER -> returned struct pointer.
 *   - BrinOpcInfo/TypeCacheEntry redeclared with ONLY the fields the proof
 *     reads (type_id); opaque structs -> char (the opaque pointer arithmetic
 *     is allocation layout, not logic; the Rust side's opaque lives behind
 *     Option<Box<..>> and is not part of the metadata claim).
 *   - palloc0 -> individually-NAMED static result structs (pooled slots
 *     kill CBMC field sensitivity — TRIAGE C-shim hygiene law).
 *   - lookup_type_cache -> per-call-site NAMED static TypeCacheEntry
 *     carrying only type_id (catalog seam, same as the PROVED 3383 family;
 *     the typcache machinery is out of scope, the REQUESTED oid is in).
 *   - pg_run_* out-param runners with int return (Kani Unit-vs-void trap).
 *
 * Function bodies between arg-fetch and return are otherwise verbatim.
 */

#include "../../support/c/pg_proof_shim.h"

#define BOOLOID							16
#define PG_BRIN_BLOOM_SUMMARYOID		4600
#define PG_BRIN_MINMAX_MULTI_SUMMARYOID 4601

/* brin_internal.h INCLUSION_* stored-column indexes (brin_inclusion.c) */
#define INCLUSION_UNION				0
#define INCLUSION_UNMERGEABLE		1
#define INCLUSION_CONTAINS_EMPTY	2

typedef struct TypeCacheEntry
{
	Oid			type_id;
} TypeCacheEntry;

typedef struct BrinOpcInfo
{
	uint16		oi_nstored;
	int			oi_regular_nulls;	/* bool */
	void	   *oi_opaque;
	TypeCacheEntry *oi_typcache[3];
} BrinOpcInfo;

/* per-call-site named statics (field-sensitivity law) */
static TypeCacheEntry pg_tc_bloom_summary;
static TypeCacheEntry pg_tc_mmm_summary;
static TypeCacheEntry pg_tc_incl_union;
static TypeCacheEntry pg_tc_incl_bool;

static BrinOpcInfo pg_opcinfo_bloom_result;
static BrinOpcInfo pg_opcinfo_mmm_result;
static BrinOpcInfo pg_opcinfo_incl_result;

/*
 * brin_bloom_opcinfo (brin_bloom.c):
 *   result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) + sizeof(BloomOpaque));
 *   result->oi_nstored = 1;
 *   result->oi_regular_nulls = true;
 *   result->oi_opaque = (BloomOpaque *) MAXALIGN(...);
 *   result->oi_typcache[0] = lookup_type_cache(PG_BRIN_BLOOM_SUMMARYOID, 0);
 *   PG_RETURN_POINTER(result);
 */
static BrinOpcInfo *
pg_brin_bloom_opcinfo(Oid typoid)
{
	BrinOpcInfo *result = &pg_opcinfo_bloom_result;

	result->oi_nstored = 1;
	result->oi_regular_nulls = 1;
	result->oi_opaque = 0;		/* allocation-layout shim; not compared */
	pg_tc_bloom_summary.type_id = PG_BRIN_BLOOM_SUMMARYOID;
	result->oi_typcache[0] = &pg_tc_bloom_summary;
	result->oi_typcache[1] = 0;
	result->oi_typcache[2] = 0;
	return result;
}

/*
 * brin_minmax_multi_opcinfo (brin_minmax_multi.c): identical shape,
 * PG_BRIN_MINMAX_MULTI_SUMMARYOID summary column.
 */
static BrinOpcInfo *
pg_brin_minmax_multi_opcinfo(Oid typoid)
{
	BrinOpcInfo *result = &pg_opcinfo_mmm_result;

	result->oi_nstored = 1;
	result->oi_regular_nulls = 1;
	result->oi_opaque = 0;
	pg_tc_mmm_summary.type_id = PG_BRIN_MINMAX_MULTI_SUMMARYOID;
	result->oi_typcache[0] = &pg_tc_mmm_summary;
	result->oi_typcache[1] = 0;
	result->oi_typcache[2] = 0;
	return result;
}

/*
 * brin_inclusion_opcinfo (brin_inclusion.c):
 *   Oid typoid = PG_GETARG_OID(0);
 *   TypeCacheEntry *bool_typcache = lookup_type_cache(BOOLOID, 0);
 *   result = palloc0(MAXALIGN(SizeofBrinOpcInfo(3)) + sizeof(InclusionOpaque));
 *   result->oi_nstored = 3;
 *   result->oi_regular_nulls = true;
 *   result->oi_opaque = (InclusionOpaque *) MAXALIGN(...);
 *   result->oi_typcache[INCLUSION_UNION] = lookup_type_cache(typoid, 0);
 *   result->oi_typcache[INCLUSION_UNMERGEABLE] = bool_typcache;
 *   result->oi_typcache[INCLUSION_CONTAINS_EMPTY] = bool_typcache;
 *   PG_RETURN_POINTER(result);
 */
static BrinOpcInfo *
pg_brin_inclusion_opcinfo(Oid typoid)
{
	BrinOpcInfo *result = &pg_opcinfo_incl_result;

	pg_tc_incl_bool.type_id = BOOLOID;
	result->oi_nstored = 3;
	result->oi_regular_nulls = 1;
	result->oi_opaque = 0;
	pg_tc_incl_union.type_id = typoid;
	result->oi_typcache[INCLUSION_UNION] = &pg_tc_incl_union;
	result->oi_typcache[INCLUSION_UNMERGEABLE] = &pg_tc_incl_bool;
	result->oi_typcache[INCLUSION_CONTAINS_EMPTY] = &pg_tc_incl_bool;
	return result;
}

/* ---- out-param runners (int return: Kani Unit-vs-void FFI trap) ---- */

int
pg_run_bloom_opcinfo(Oid typoid, uint16 *nstored, int *regular_nulls,
					 Oid *typid0)
{
	BrinOpcInfo *r = pg_brin_bloom_opcinfo(typoid);

	*nstored = r->oi_nstored;
	*regular_nulls = r->oi_regular_nulls;
	*typid0 = r->oi_typcache[0]->type_id;
	return 0;
}

int
pg_run_mmm_opcinfo(Oid typoid, uint16 *nstored, int *regular_nulls,
				   Oid *typid0)
{
	BrinOpcInfo *r = pg_brin_minmax_multi_opcinfo(typoid);

	*nstored = r->oi_nstored;
	*regular_nulls = r->oi_regular_nulls;
	*typid0 = r->oi_typcache[0]->type_id;
	return 0;
}

int
pg_run_inclusion_opcinfo(Oid typoid, uint16 *nstored, int *regular_nulls,
						 Oid *typid0, Oid *typid1, Oid *typid2)
{
	BrinOpcInfo *r = pg_brin_inclusion_opcinfo(typoid);

	*nstored = r->oi_nstored;
	*regular_nulls = r->oi_regular_nulls;
	*typid0 = r->oi_typcache[INCLUSION_UNION]->type_id;
	*typid1 = r->oi_typcache[INCLUSION_UNMERGEABLE]->type_id;
	*typid2 = r->oi_typcache[INCLUSION_CONTAINS_EMPTY]->type_id;
	return 0;
}
