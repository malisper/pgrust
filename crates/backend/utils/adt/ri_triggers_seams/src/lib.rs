use types_error::PgResult;
use types_rel::Relation;
use types_trigger::Trigger;
use types_tuple::HeapTupleData;

/// C TriggerData reduced to what the RI trigger functions read; tg_trigtuple
/// is the fired row (INSERT new / DELETE old / UPDATE old), tg_newtuple the
/// UPDATE new row.
pub struct RiTriggerData<'a, 'mcx> {
    pub tg_event: u32,
    pub tg_relation: &'a Relation<'mcx>,
    pub tg_trigtuple: &'a HeapTupleData<'a>,
    pub tg_newtuple: Option<&'a HeapTupleData<'a>>,
    pub tg_trigger: &'a Trigger<'mcx>,
}

seam_core::seam!(
    pub fn ri_fkey_trigger<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tgfoid: types_core::Oid,
        tgdata: &RiTriggerData<'_, 'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn ri_initial_check<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: &Trigger<'mcx>,
        fk_rel: &Relation<'mcx>,
        pk_rel: &Relation<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn ri_fkey_fk_upd_check_required<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: &Trigger<'mcx>,
        rel: &Relation<'mcx>,
        old_slot: &HeapTupleData<'_>,
        new_slot: &HeapTupleData<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn ri_fkey_pk_upd_check_required<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: &Trigger<'mcx>,
        rel: &Relation<'mcx>,
        old_slot: &HeapTupleData<'_>,
        new_slot: &HeapTupleData<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn ri_partition_remove_check<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        trigger: &Trigger<'mcx>,
        fk_rel: &Relation<'mcx>,
        pk_rel: &Relation<'mcx>,
    ) -> PgResult<()>
);
