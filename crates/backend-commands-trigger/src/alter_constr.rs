//! `AlterConstrTriggerDeferrability` (commands/tablecmds.c:12694): the
//! pg_trigger leg of `ALTER TABLE ... ALTER CONSTRAINT ... [NOT] DEFERRABLE`.

use mcx::{Mcx, MemoryContext};
use types_catalog::pg_trigger as pt;
use types_core::fmgr::F_OIDEQ;
use types_core::Oid;
use types_error::PgResult;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_catalog_indexing_seams as indexing;

// Built-in function OIDs of the RI trigger procs (fmgroids.h). Only the four
// triggers whose deferrability `AlterConstrTriggerDeferrability` updates; see
// createForeignKeyActionTriggers and CreateFKCheckTrigger.
const F_RI_FKEY_CHECK_INS: Oid = 1644;
const F_RI_FKEY_CHECK_UPD: Oid = 1645;
const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

/// `AlterConstrTriggerDeferrability(conoid, tgrel, rel, deferrable,
/// initdeferred, otherrelids)` (tablecmds.c:12694).
///
/// Scans `pg_trigger` for the constraint's triggers and updates `tgdeferrable`
/// / `tginitdeferred` on the RI check/no-action triggers. `relid` is the
/// relation directly altered (the FK relation); any trigger row whose
/// `tgrelid` differs is recorded in `otherrelids` (deduplicated) so the caller
/// can invalidate those relcaches too.
pub fn AlterConstrTriggerDeferrability(
    conoid: Oid,
    relid: Oid,
    deferrable: bool,
    initdeferred: bool,
    otherrelids: &mut Vec<Oid>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterConstrTriggerDeferrability");
    let mcx: Mcx<'_> = ctx.mcx();

    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    let mut tgkey = ScanKeyData::empty();
    ScanKeyInit(
        &mut tgkey,
        pt::Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(conoid),
    )?;
    let keys = [tgkey];

    let mut tgscan = genam_seams::systable_beginscan::call(
        &tgrel,
        pt::TriggerConstraintIndexId,
        true,
        None,
        &keys,
    )?;

    while let Some(tgtuple) = genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())? {
        let cols = heap_deform_tuple(mcx, &tgtuple.tuple, &tgrel.rd_att, &tgtuple.data)?;
        let col = |attno: i16| cols[attno as usize - 1].0.clone();
        let tgrelid = col(pt::Anum_pg_trigger_tgrelid).as_oid();
        let tgfoid = col(pt::Anum_pg_trigger_tgfoid).as_oid();
        let tgoid = col(pt::Anum_pg_trigger_oid).as_oid();

        // Remember OIDs of other relation(s) involved in the FK constraint.
        if tgrelid != relid && !otherrelids.contains(&tgrelid) {
            otherrelids.push(tgrelid);
        }

        // Update only the RI_FKey_noaction_{del,upd} and RI_FKey_check_{ins,upd}
        // triggers, not others.
        if tgfoid != F_RI_FKEY_NOACTION_DEL
            && tgfoid != F_RI_FKEY_NOACTION_UPD
            && tgfoid != F_RI_FKEY_CHECK_INS
            && tgfoid != F_RI_FKEY_CHECK_UPD
        {
            continue;
        }

        let fields = pt::TriggerFieldUpdate {
            tgname: None,
            tgparentid: None,
            tgdeferrable: Some(deferrable),
            tginitdeferred: Some(initdeferred),
            tgenabled: None,
        };
        indexing::catalog_tuple_update_pg_trigger::call(&tgrel, tgtuple.tuple.t_self, &fields)?;

        // InvokeObjectPostAlterHook(TriggerRelationId, tgform->oid, 0); -- no-op.
        let _ = tgoid;
    }

    let _ = tgscan;
    tgrel.close(RowExclusiveLock)?;

    Ok(())
}
