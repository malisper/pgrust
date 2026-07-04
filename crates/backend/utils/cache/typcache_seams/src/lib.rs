use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::{NameData, TupleDescData};

seam_core::seam!(
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    pub fn at_eosubxact_type_cache()
);

seam_core::seam!(
    // lookup_rowtype_tupdesc_copy(type_id, typmod) (typcache.c) — a
    // freestanding copy the caller owns and may re-stamp.
    pub fn lookup_rowtype_tupdesc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        type_id: Oid,
        typmod: i32,
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    // assign_record_type_typmod(tupDesc) (typcache.c) — registers the rowtype
    // and stamps tdtypmod.
    pub fn assign_record_type_typmod(tupdesc: &mut TupleDescData<'_>) -> PgResult<()>
);

// One pg_constraint CHECK row of a domain's ConstraintTypidIndexId scan
// (load_domaintype_info, typcache.c).
#[derive(Debug)]
pub struct DomainCheckRow<'mcx> {
    pub conname: NameData,
    pub conbin: &'mcx str,
}

seam_core::seam!(
    pub fn scan_domain_check_constraints<'mcx>(
        mcx: Mcx<'mcx>,
        contypid: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, DomainCheckRow<'mcx>>>
);

seam_core::seam!(
    // DomainHasConstraints (typcache.c); installed by typcache for consumers
    // below it in the dep graph (clauses' eval_const_expressions arm).
    pub fn domain_has_constraints(type_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // domains.c domain_check_input engine (compiled-check evaluation lives
    // with execexpr; adt_domains sits under fmgr_core and calls through here).
    // Violations errsave into `escontext` when armed (C failure surface).
    pub fn domain_check_input(
        value: datum::Datum,
        isnull: bool,
        domain_type: Oid,
        escontext: Option<&mut types_error::SoftErrorContext>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // compare_values_of_enum (typcache.c) keyed by the enum type OID — the
    // enum.c odd-OID comparison fallback consumer.
    pub fn compare_values_of_enum(type_id: Oid, arg1: Oid, arg2: Oid) -> PgResult<i32>
);

seam_core::seam!(
    // lookup_type_cache(.., TYPECACHE_CMP_PROC).cmp_proc — the
    // op_mergejoinable ARRAY_EQ/RECORD_EQ arms (lsyscache sits below typcache).
    pub fn type_cache_cmp_proc(type_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn type_cache_hash_proc(type_id: Oid) -> PgResult<Oid>
);
