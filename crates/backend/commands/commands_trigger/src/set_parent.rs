//! `TriggerSetParentTrigger` (commands/trigger.c:1220): mark a partition's
//! trigger as a child of its parent trigger (or remove the linkage).

use mcx::{Mcx, MemoryContext};
use ::types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC,
};
use ::types_catalog::pg_trigger as pt;
use ::types_core::fmgr::F_OIDEQ;
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERROR};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::Datum;

use ::heaptuple::heap_deform_tuple;
use ::scankey::ScanKeyInit;
use genam_seams as genam_seams;
use indexing_seams as indexing;
use ::objectaddress::consts::{RelationRelationId, TriggerRelationId};

/// `TriggerSetParentTrigger(trigRel, childTrigId, parentTrigId, childTableId)`
/// (trigger.c:1220).
///
/// Sets a partition's trigger as child of its parent trigger when
/// `parentTrigId` is valid, or removes the linkage when it is `InvalidOid`.
/// Updates the child's `pg_trigger.tgparentid` and adds/removes the PARTITION
/// dependency records that keep the child trigger from being dropped on its
/// own. `trigrel` is an already-open `pg_trigger` relation.
pub fn TriggerSetParentTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    trigrel: &rel::Relation<'mcx>,
    child_trig_id: Oid,
    parent_trig_id: Oid,
    child_table_id: Oid,
) -> PgResult<()> {
    let scratch = MemoryContext::new("TriggerSetParentTrigger");
    let smcx = scratch.mcx();

    // Find the trigger to modify (scan pg_trigger by oid).
    let mut skey = ScanKeyData::empty();
    ScanKeyInit(
        &mut skey,
        pt::Anum_pg_trigger_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(child_trig_id),
    )?;
    let keys = [skey];

    let mut tgscan =
        genam_seams::systable_beginscan::call(trigrel, pt::TriggerOidIndexId, true, None, &keys)?;

    let tuple = genam_seams::systable_getnext::call(smcx, tgscan.desc_mut())?.ok_or_else(|| {
        utils_error::ereport(ERROR)
            .errmsg_internal(format!("could not find tuple for trigger {child_trig_id}"))
            .into_error()
    })?;

    if OidIsValid(parent_trig_id) {
        // Don't allow setting a parent for a trigger that already has one.
        let cols = heap_deform_tuple(smcx, &tuple.tuple, &trigrel.rd_att, &tuple.data)?;
        let cur_parent = cols[pt::Anum_pg_trigger_tgparentid as usize - 1].0.as_oid();
        if OidIsValid(cur_parent) {
            return Err(utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "trigger {child_trig_id} already has a parent trigger"
                ))
                .into_error());
        }

        let fields = pt::TriggerFieldUpdate {
            tgparentid: Some(parent_trig_id),
            ..Default::default()
        };
        indexing::catalog_tuple_update_pg_trigger::call(trigrel, tuple.tuple.t_self, &fields)?;

        let depender = ObjectAddress {
            classId: TriggerRelationId,
            objectId: child_trig_id,
            objectSubId: 0,
        };
        let referenced_parent = ObjectAddress {
            classId: TriggerRelationId,
            objectId: parent_trig_id,
            objectSubId: 0,
        };
        pg_depend::recordDependencyOn(
            mcx,
            &depender,
            &referenced_parent,
            DEPENDENCY_PARTITION_PRI,
        )?;
        let referenced_tbl = ObjectAddress {
            classId: RelationRelationId,
            objectId: child_table_id,
            objectSubId: 0,
        };
        pg_depend::recordDependencyOn(
            mcx,
            &depender,
            &referenced_tbl,
            DEPENDENCY_PARTITION_SEC,
        )?;
    } else {
        let fields = pt::TriggerFieldUpdate {
            tgparentid: Some(InvalidOid),
            ..Default::default()
        };
        indexing::catalog_tuple_update_pg_trigger::call(trigrel, tuple.tuple.t_self, &fields)?;

        pg_depend::deleteDependencyRecordsForClass(
            TriggerRelationId,
            child_trig_id,
            TriggerRelationId,
            DEPENDENCY_PARTITION_PRI.0,
        )?;
        pg_depend::deleteDependencyRecordsForClass(
            TriggerRelationId,
            child_trig_id,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC.0,
        )?;
    }

    let _ = tgscan;
    Ok(())
}
