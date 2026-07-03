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
    pub fn domain_check_input(value: datum::Datum, isnull: bool, domain_type: Oid) -> PgResult<()>
);
