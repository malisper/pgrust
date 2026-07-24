// ATTACH/DETACH PARTITION exec slice of tablecmds.c: ATExecAttachPartition,
// AttachPartitionEnsureIndexes, QueuePartitionConstraintValidation +
// implication proof, CreateInheritance/RemoveInheritance (partition arm),
// ATExecDetachPartition (plain + CONCURRENTLY) + MarkInheritDetached +
// DetachAddConstraintIfNeeded + DetachPartitionFinalize + FINALIZE verb.
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, DEBUG1, ERROR, ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nodes::list::NodeList;
use types_nodes::primnodes::{NullTest, NullTestType, Var};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionCmd};
use types_nodes::Node;
use types_core::catalog::RELPERSISTENCE_TEMP;
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use crate::alter::{attname_lookup, oid_scankey, AlteredTableInfo};

fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

const InheritsRelationId: Oid = 2611;
const InheritsRelidSeqnoIndexId: Oid = 2680;
const InheritsParentIndexId: Oid = 2187;
const TriggerRelationId: Oid = 2620;
const TriggerRelidNameIndexId: Oid = 2701;
const DependRelationId: Oid = 2608;
const DependDependerIndexId: Oid = 2673;
const CONSTRAINT_FOREIGN: u8 = pg_constraint::CONSTRAINT_FOREIGN;
const CONSTRAINT_CHECK: u8 = pg_constraint::CONSTRAINT_CHECK;
const CONSTRAINT_NOTNULL: u8 = pg_constraint::CONSTRAINT_NOTNULL;
const Anum_pg_attribute_attnum: usize = 5;
const Anum_pg_attribute_attidentity: usize = 15;
const Anum_pg_attribute_attisdropped: usize = 17;
const Anum_pg_attribute_attislocal: usize = 18;
const Anum_pg_attribute_attinhcount: usize = 19;
const Anum_pg_class_relpartbound: usize = 34;
const Anum_pg_class_relispartition: usize = 28;
const AttributeRelidNumIndexId: Oid = 2659;
const F_PG_GET_EXPR: Oid = 1716;

fn name_at<'a>(tup: &'a types_tuple::HeapTupleData<'_>, desc: &types_tuple::TupleDescData<'_>, attno: i32) -> &'a str {
    let mut isnull = false;
    // SAFETY: caller passes a NOT NULL name column of the scanned catalog.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, desc, &mut isnull) };
    // SAFETY: NameData is a 64-byte NUL-padded buffer.
    let bytes = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    core::str::from_utf8(&bytes[..len]).expect("catalog name UTF-8")
}

fn getattr(
    tup: &types_tuple::HeapTupleData<'_>,
    desc: &types_tuple::TupleDescData<'_>,
    attno: usize,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: fixed catalog column under the relation's own descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno as i32, desc, &mut isnull) };
    (d, isnull)
}


fn text_datum_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<mcx::PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: d comes off a not-null text column: a live varlena image
    // readable through its varsize_any extent.
    let image = unsafe { std::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let st = core::str::from_utf8(payload.as_bytes())
        .unwrap_or_else(|_| panic!("non-UTF-8 text datum"));
    mcx::PgString::from_str_in(st, mcx)
}

fn open_by_rangevar<'mcx>(
    mcx: Mcx<'mcx>,
    prv: &types_nodes::primnodes::RangeVar<'_>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let relid = catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, 0, None)?;
    table::table_open(mcx, relid, NoLock)
}

pub(crate) fn ATExecAttachPartition<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let pdesc = partdesc::RelationGetPartitionDesc(rel, true)?;
    let default_part_oid = pdesc
        .boundinfo
        .as_ref()
        .filter(|b| b.has_default())
        .map(|b| pdesc.oids[b.default_index as usize])
        .unwrap_or(InvalidOid);
    if default_part_oid != InvalidOid {
        lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
    }

    let attachrel = open_by_rangevar(mcx, cmd.name.expect("PartitionCmd.name"), AccessExclusiveLock)?;

    match attachrel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE | types_rel::RELKIND_FOREIGN_TABLE => {}
        // unported: ATTACH PARTITION of remaining relkinds
        _ => {
            return Err(err(
                "ATTACH PARTITION for this type of relation is not supported yet".to_string(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ))
        }
    }
    if !aclchk::object_ownercheck(
        types_core::RELATION_RELATION_ID,
        attachrel.rd_id,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(attachrel.rd_rel.relkind),
            attachrel.name(),
        )?;
    }

    if attachrel.rd_rel.relispartition {
        return Err(err(
            format!("\"{}\" is already a partition", attachrel.name()),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if crate::alter::rel_reloftype(attachrel.rd_id)? != InvalidOid {
        return Err(err(
            "cannot attach a typed table as partition".into(),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let catalog = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    {
        let keys = [oid_scankey(1, attachrel.rd_id)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &catalog,
            InheritsRelidSeqnoIndexId,
            true,
            None,
            &keys,
        )?;
        let child_row = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if child_row {
            return Err(err(
                "cannot attach inheritance child as partition".into(),
                ERRCODE_WRONG_OBJECT_TYPE,
            ));
        }
    }
    {
        let keys = [oid_scankey(2, attachrel.rd_id)];
        let mut scan =
            genam::systable_beginscan(mcx, &catalog, InheritsParentIndexId, true, None, &keys)?;
        let parent_row = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if parent_row && attachrel.rd_rel.relkind == RELKIND_RELATION {
            return Err(err(
                "cannot attach inheritance parent as partition".into(),
                ERRCODE_WRONG_OBJECT_TYPE,
            ));
        }
    }
    catalog.close(AccessShareLock)?;

    let attachrel_children =
        pg_inherits::find_all_inheritors(mcx, attachrel.rd_id, AccessExclusiveLock)?;
    if attachrel_children.iter().any(|&c| c == rel.rd_id) {
        return Err(Box::new(
            PgError::new(ERROR, "circular inheritance not allowed".to_string())
                .with_sqlstate(ERRCODE_DUPLICATE_TABLE)
                .with_detail(format!(
                    "\"{}\" is already a child of \"{}\".",
                    rel.name(),
                    attachrel.name()
                )),
        ));
    }

    if rel.rd_rel.relpersistence != RELPERSISTENCE_TEMP
        && attachrel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
    {
        return Err(err(
            format!(
                "cannot attach a temporary relation as partition of permanent relation \"{}\"",
                rel.name()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        && attachrel.rd_rel.relpersistence != RELPERSISTENCE_TEMP
    {
        return Err(err(
            format!(
                "cannot attach a permanent relation as partition of temporary relation \"{}\"",
                rel.name()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if rel.is_other_temp() {
        return Err(err(
            "cannot attach as partition of temporary relation of another session".into(),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if attachrel.is_other_temp() {
        return Err(err(
            "cannot attach temporary relation of another session as partition".into(),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let tuple_desc = attachrel.descr();
    for i in 0..tuple_desc.natts as usize {
        let att = tuple_desc.attr(i);
        if att.attisdropped {
            continue;
        }
        let attname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
        if att.attidentity != 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "table \"{}\" being attached contains an identity column \"{attname}\"",
                        attachrel.name()
                    ),
                )
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .with_detail("The new partition may not contain an identity column.".to_string()),
            ));
        }
        if attname_lookup(mcx, rel.rd_id, attname, true)?.is_none() {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "table \"{}\" contains column \"{attname}\" not found in parent \"{}\"",
                        attachrel.name(),
                        rel.name()
                    ),
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_detail(
                    "The new partition may contain only the columns present in parent."
                        .to_string(),
                ),
            ));
        }
    }

    if attachrel.rd_hastriggers {
        check_no_transition_table_triggers(mcx, &attachrel)?;
    }

    let mut pstate = parser_small1::make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(query_string.as_bytes());
    let bound = crate::partition::transformPartitionBound(
        mcx,
        &mut pstate,
        rel,
        cmd.bound.expect("ATTACH PARTITION bound"),
    )?;
    let spec = bound
        .as_variant::<PartitionBoundSpec>()
        .expect("PartitionBoundSpec");

    let key = partcache::RelationGetPartitionKey(rel)?;
    partbounds::check_new_partition_bound(
        mcx,
        attachrel.name(),
        &key,
        pdesc.boundinfo.as_ref(),
        &pdesc.oids,
        spec,
        Some(query_string.as_bytes()),
    )?;

    CreateInheritance(mcx, &attachrel, rel)?;
    catalog_heap::StorePartitionBound(mcx, &attachrel, rel, bound)?;

    AttachPartitionEnsureIndexes(mcx, rel, &attachrel)?;

    crate::partition::CloneRowTriggersToPartition(mcx, rel, &attachrel)?;

    crate::fk::CloneForeignKeyConstraints(mcx, Some(wqueue), rel, &attachrel)?;

    let part_bound_constraint =
        partbounds::get_qual_from_partbound(mcx, &key, rel.rd_id, pdesc.boundinfo.as_ref(), &pdesc.oids, spec)?;

    let mut part_constraint = NodeList::nil();
    for q in part_bound_constraint.iter() {
        part_constraint.lappend(mcx, q)?;
    }
    for q in partdesc::RelationGetPartitionQual(mcx, rel)?.iter() {
        part_constraint.lappend(mcx, q)?;
    }

    if !part_constraint.is_nil() {
        let explicit = partbounds::make_ands_explicit(mcx, part_constraint)?;
        let simplified = clauses::eval_const_expressions(mcx, explicit)?;
        let one = NodeList::make1(mcx, simplified)?;
        let one = partbounds::map_partition_varattnos(mcx, one, 1, &attachrel, rel)?;
        QueuePartitionConstraintValidation(mcx, wqueue, &attachrel, &one, false)?;
    }

    if default_part_oid != InvalidOid {
        assert!(!spec.is_default);
        let defaultrel = table::table_open(mcx, default_part_oid, NoLock)?;
        let def_constraint =
            partbounds::get_proposed_default_constraint(mcx, part_bound_constraint)?;
        let def_constraint =
            partbounds::map_partition_varattnos(mcx, def_constraint, 1, &defaultrel, rel)?;
        QueuePartitionConstraintValidation(mcx, wqueue, &defaultrel, &def_constraint, true)?;
        defaultrel.close(NoLock)?;
    }

    if attachrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        for &c in attachrel_children.iter() {
            inval::invalidate::CacheInvalidateRelcacheByRelid(c)?;
        }
    }

    attachrel.close(NoLock)
}

// FindTriggerIncompatibleWithInheritance over pg_trigger: the first row
// trigger with transition tables, if any (trigger.c:2280).
pub(crate) fn find_transition_table_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<Option<String>> {
    const Anum_pg_trigger_tgname: usize = 4;
    const Anum_pg_trigger_tgtype: usize = 6;
    const Anum_pg_trigger_tgoldtable: usize = 18;
    const Anum_pg_trigger_tgnewtable: usize = 19;
    const TRIGGER_TYPE_ROW: i16 = 1;
    let tgrel = table::table_open(mcx, TriggerRelationId, AccessShareLock)?;
    let keys = [oid_scankey(2, rel.rd_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &tgrel, TriggerRelidNameIndexId, true, None, &keys)?;
    let desc = tgrel.descr();
    let mut bad: Option<String> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (tgtype, _) = getattr(tup, desc, Anum_pg_trigger_tgtype);
        if tgtype.as_i16() & TRIGGER_TYPE_ROW == 0 {
            continue;
        }
        let (_, old_null) = getattr(tup, desc, Anum_pg_trigger_tgoldtable);
        let (_, new_null) = getattr(tup, desc, Anum_pg_trigger_tgnewtable);
        if !old_null || !new_null {
            bad = Some(name_at(tup, desc, Anum_pg_trigger_tgname as i32).to_string());
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    tgrel.close(AccessShareLock)?;
    Ok(bad)
}

// Row triggers with transition tables block ATTACH (tablecmds.c:20430).
fn check_no_transition_table_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<()> {
    if let Some(trigger_name) = find_transition_table_trigger(mcx, rel)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "trigger \"{trigger_name}\" prevents table \"{}\" from becoming a partition",
                    rel.name()
                ),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "ROW triggers with transition tables are not supported on partitions.".to_string(),
            ),
        ));
    }
    Ok(())
}


// CreateInheritance (tablecmds.c:17374), ATTACH form: the caller has already
// proven attachrel has no pg_inherits rows, so inhseqno is always 1.
fn CreateInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    MergeAttributesIntoExisting(mcx, child_rel, parent_rel)?;
    MergeConstraintsIntoExisting(mcx, child_rel, parent_rel)?;
    crate::partition::store_catalog_inheritance1(mcx, child_rel.rd_id, parent_rel.rd_id)
}

fn MergeAttributesIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let attrrel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let parent_desc = parent_rel.descr();
    for i in 0..parent_desc.natts as usize {
        let parent_att = parent_desc.attr(i);
        if parent_att.attisdropped {
            continue;
        }
        let parent_attname =
            core::str::from_utf8(parent_att.attname.name_str()).expect("attname UTF-8");

        let keys = [oid_scankey(1, child_rel.rd_id)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &attrrel,
            AttributeRelidNumIndexId,
            true,
            None,
            &keys,
        )?;
        let desc = attrrel.descr();
        let mut matched = false;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let (dropped, _) = getattr(tup, desc, Anum_pg_attribute_attisdropped);
            if dropped.as_bool() {
                continue;
            }
            if name_at(tup, desc, 2) != parent_attname {
                continue;
            }
            matched = true;
            let (attnum, _) = getattr(tup, desc, Anum_pg_attribute_attnum);
            let child_att = child_rel.descr().attr(attnum.as_i16() as usize - 1);
            if parent_att.atttypid != child_att.atttypid
                || parent_att.atttypmod != child_att.atttypmod
            {
                return Err(err(
                    format!(
                        "child table \"{}\" has different type for column \"{parent_attname}\"",
                        child_rel.name()
                    ),
                    ERRCODE_DATATYPE_MISMATCH,
                ));
            }
            if parent_att.attcollation != child_att.attcollation {
                return Err(err(
                    format!(
                        "child table \"{}\" has different collation for column \"{parent_attname}\"",
                        child_rel.name()
                    ),
                    ERRCODE_COLLATION_MISMATCH,
                ));
            }
            if parent_att.attnotnull && !child_att.attnotnull {
                let contup = pg_constraint::findNotNullConstraintAttnum(
                    mcx,
                    parent_rel.rd_id,
                    parent_att.attnum,
                )?;
                if let Some(c) = contup {
                    if !c.connoinherit {
                        return Err(err(
                            format!(
                                "column \"{parent_attname}\" in child table \"{}\" must be marked NOT NULL",
                                child_rel.name()
                            ),
                            ERRCODE_DATATYPE_MISMATCH,
                        ));
                    }
                }
            }
            if parent_att.attgenerated != 0 && child_att.attgenerated == 0 {
                return Err(err(
                    format!(
                        "column \"{parent_attname}\" in child table must be a generated column"
                    ),
                    ERRCODE_DATATYPE_MISMATCH,
                ));
            }
            if child_att.attgenerated != 0 && parent_att.attgenerated == 0 {
                return Err(err(
                    format!(
                        "column \"{parent_attname}\" in child table must not be a generated column"
                    ),
                    ERRCODE_DATATYPE_MISMATCH,
                ));
            }
            if parent_att.attgenerated != 0
                && child_att.attgenerated != 0
                && child_att.attgenerated != parent_att.attgenerated
            {
                let kind = |g: i8| if g == b's' as i8 { "STORED" } else { "VIRTUAL" };
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "column \"{parent_attname}\" inherits from generated column of different kind"
                        ),
                    )
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                    .with_detail(format!(
                        "Parent column is {}, child column is {}.",
                        kind(parent_att.attgenerated),
                        kind(child_att.attgenerated)
                    )),
                ));
            }
            let (inhcount, _) = getattr(tup, desc, Anum_pg_attribute_attinhcount);
            let inhcount = inhcount.as_i16();
            if inhcount == i16::MAX {
                return Err(err(
                    "too many inheritance parents".into(),
                    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                ));
            }
            debug_assert!(inhcount + 1 == 1, "partition attinhcount must become 1");
            let natts = desc.natts as usize;
            let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_attribute_attinhcount - 1] = Datum::from_i16(inhcount + 1);
            replace[Anum_pg_attribute_attinhcount - 1] = true;
            values[Anum_pg_attribute_attislocal - 1] = Datum::from_bool(false);
            replace[Anum_pg_attribute_attislocal - 1] = true;
            // tablecmds.c:17579-17583: partitions inherit the parent's
            // identity property.
            values[Anum_pg_attribute_attidentity - 1] =
                Datum::from_i8(parent_att.attidentity);
            replace[Anum_pg_attribute_attidentity - 1] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            let otid = tup.t_self;
            catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
            break;
        }
        genam::systable_endscan(mcx, scan)?;
        if !matched {
            return Err(err(
                format!("child table is missing column \"{parent_attname}\""),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
    }
    attrrel.close(RowExclusiveLock)
}

struct ConRow<'mcx> {
    oid: Oid,
    name: mcx::PgString<'mcx>,
    contype: u8,
    connoinherit: bool,
    convalidated: bool,
    conenforced: bool,
    condeferrable: bool,
    condeferred: bool,
    coninhcount: i16,
    nn_attno: AttrNumber,
    conbin: Option<mcx::PgString<'mcx>>,
    tid: types_tuple::ItemPointerData,
}

fn scan_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    conrel: &Relation<'mcx>,
    relid: Oid,
) -> PgResult<PgVec<'mcx, ConRow<'mcx>>> {
    use pg_constraint::*;
    let keys = [oid_scankey(Anum_pg_constraint_conrelid as usize, relid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        conrel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = conrel.descr();
    let mut rows: PgVec<'mcx, ConRow<'mcx>> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (oid, _) = getattr(tup, desc, Anum_pg_constraint_oid as usize);
        let (contype, _) = getattr(tup, desc, Anum_pg_constraint_contype as usize);
        let contype = contype.as_i8() as u8;
        let (connoinherit, _) = getattr(tup, desc, Anum_pg_constraint_connoinherit as usize);
        let (convalidated, _) = getattr(tup, desc, Anum_pg_constraint_convalidated as usize);
        let (conenforced, _) = getattr(tup, desc, Anum_pg_constraint_conenforced as usize);
        let (condeferrable, _) = getattr(tup, desc, Anum_pg_constraint_condeferrable as usize);
        let (condeferred, _) = getattr(tup, desc, Anum_pg_constraint_condeferred as usize);
        let (coninhcount, _) = getattr(tup, desc, Anum_pg_constraint_coninhcount as usize);
        let nn_attno = if contype == CONSTRAINT_NOTNULL {
            pg_constraint::extract_notnull_column(mcx, tup, desc)?
        } else {
            0
        };
        let (conbin_d, conbin_null) = getattr(tup, desc, Anum_pg_constraint_conbin as usize);
        let conbin = if conbin_null { None } else { Some(text_datum_str(mcx, conbin_d)?) };
        rows.push(ConRow {
            oid: oid.as_oid(),
            name: mcx::PgString::from_str_in(name_at(tup, desc, 2), mcx)?,
            contype,
            connoinherit: connoinherit.as_bool(),
            convalidated: convalidated.as_bool(),
            conenforced: conenforced.as_bool(),
            condeferrable: condeferrable.as_bool(),
            condeferred: condeferred.as_bool(),
            coninhcount: coninhcount.as_i16(),
            nn_attno,
            conbin,
            tid: tup.t_self,
        });
    }
    genam::systable_endscan(mcx, scan)?;
    Ok(rows)
}

fn decompile_conbin(mcx: Mcx<'_>, conbin: &str, relid: Oid) -> PgResult<String> {
    let t = varlena::cstring_to_text(mcx, conbin.as_bytes())?;
    // pg_get_expr's result datum aliases the resolved FmgrInfo's retained
    // scratch; the FmgrInfo must outlive the copy-out (oid_function_call*
    // drops it before returning).
    let mut flinfo = fmgr_core::fmgr_info(F_PG_GET_EXPR)?;
    let d = fmgr_core::function_call2_coll(
        &mut flinfo,
        InvalidOid,
        Datum::from_usize(t.as_bytes().as_ptr() as usize),
        Datum::from_oid(relid),
    )?;
    Ok(text_datum_str(mcx, d)?.as_str().to_string())
}

fn MergeConstraintsIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let conrel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let parent_cons = scan_constraints(mcx, &conrel, parent_rel.rd_id)?;
    let desc = conrel.descr();
    let attmap =
        tupdesc::build_attrmap_by_name_missing_ok(mcx, parent_rel.descr(), child_rel.descr())?;
    for pcon in parent_cons.iter() {
        if pcon.contype != CONSTRAINT_CHECK && pcon.contype != CONSTRAINT_NOTNULL {
            continue;
        }
        if pcon.connoinherit {
            continue;
        }
        // Fresh child scan per parent constraint, as in C; the previous
        // iteration's CatalogTupleUpdate stays invisible until CCI, and
        // distinct parent constraints match distinct child rows.
        let child_cons = scan_constraints(mcx, &conrel, child_rel.rd_id)?;
        let mut found = false;
        for ccon in child_cons.iter() {
            if ccon.contype != pcon.contype {
                continue;
            }
            if ccon.contype == CONSTRAINT_CHECK {
                if ccon.name.as_str() != pcon.name.as_str() {
                    continue;
                }
            } else if ccon.contype == CONSTRAINT_NOTNULL {
                if pcon.nn_attno != attmap[ccon.nn_attno as usize - 1] {
                    continue;
                }
                let parent_att = parent_rel.descr().attr(pcon.nn_attno as usize - 1);
                let child_att = child_rel.descr().attr(ccon.nn_attno as usize - 1);
                if parent_att.attisdropped || child_att.attisdropped {
                    return Err(Box::new(PgError::error(
                        "found not-null constraint on dropped columns",
                    )));
                }
            }
            if ccon.contype == CONSTRAINT_CHECK {
                let peq = pcon.condeferrable == ccon.condeferrable
                    && pcon.condeferred == ccon.condeferred
                    && decompile_conbin(
                        mcx,
                        pcon.conbin.as_ref().expect("check conbin").as_str(),
                        parent_rel.rd_id,
                    )? == decompile_conbin(
                        mcx,
                        ccon.conbin.as_ref().expect("check conbin").as_str(),
                        child_rel.rd_id,
                    )?;
                if !peq {
                    return Err(err(
                        format!(
                            "child table \"{}\" has different definition for check constraint \"{}\"",
                            child_rel.name(),
                            pcon.name.as_str()
                        ),
                        ERRCODE_DATATYPE_MISMATCH,
                    ));
                }
            }
            if ccon.connoinherit {
                return Err(err(
                    format!(
                        "constraint \"{}\" conflicts with non-inherited constraint on child table \"{}\"",
                        ccon.name.as_str(),
                        child_rel.name()
                    ),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            if pcon.convalidated && ccon.conenforced && !ccon.convalidated {
                return Err(err(
                    format!(
                        "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{}\"",
                        ccon.name.as_str(),
                        child_rel.name()
                    ),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            if pcon.conenforced && !ccon.conenforced {
                return Err(err(
                    format!(
                        "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{}\"",
                        ccon.name.as_str(),
                        child_rel.name()
                    ),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            if ccon.coninhcount == i16::MAX {
                return Err(err(
                    "too many inheritance parents".into(),
                    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                ));
            }
            debug_assert!(ccon.coninhcount + 1 == 1, "partition coninhcount must become 1");
            let natts = desc.natts as usize;
            let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[pg_constraint::Anum_pg_constraint_coninhcount as usize - 1] =
                Datum::from_i16(ccon.coninhcount + 1);
            replace[pg_constraint::Anum_pg_constraint_coninhcount as usize - 1] = true;
            values[pg_constraint::Anum_pg_constraint_conislocal as usize - 1] =
                Datum::from_bool(false);
            replace[pg_constraint::Anum_pg_constraint_conislocal as usize - 1] = true;
            // Re-fetch the row image by tid to modify: reuse the scan tuple's
            // tid captured above.
            let keys = [oid_scankey(pg_constraint::Anum_pg_constraint_conrelid as usize, child_rel.rd_id)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &conrel,
                pg_constraint::ConstraintRelidTypidNameIndexId,
                true,
                None,
                &keys,
            )?;
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                let (oid, _) = getattr(tup, desc, pg_constraint::Anum_pg_constraint_oid as usize);
                if oid.as_oid() != ccon.oid {
                    continue;
                }
                let mut newtup =
                    heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
                let otid = tup.t_self;
                catalog_indexing::CatalogTupleUpdate(mcx, &conrel, &otid, &mut newtup)?;
                break;
            }
            genam::systable_endscan(mcx, scan)?;
            found = true;
            break;
        }
        if !found {
            if pcon.contype == CONSTRAINT_NOTNULL {
                let colname = lsyscache::attribute::get_attname(mcx, parent_rel.rd_id, pcon.nn_attno, false)?
                    .expect("attname");
                return Err(err(
                    format!(
                        "column \"{colname}\" in child table \"{}\" must be marked NOT NULL",
                        child_rel.name()
                    ),
                    ERRCODE_DATATYPE_MISMATCH,
                ));
            }
            return Err(err(
                format!("child table is missing constraint \"{}\"", pcon.name.as_str()),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
    }
    conrel.close(RowExclusiveLock)
}

// AttachPartitionEnsureIndexes (tablecmds.c:20573).
fn AttachPartitionEnsureIndexes<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attachrel: &Relation<'mcx>,
) -> PgResult<()> {
    let idxes = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    let attach_rel_idxs = relcache::RelationGetIndexList(mcx, attachrel.rd_id)?;
    let mut attachrel_idx_rels: PgVec<'mcx, Relation<'mcx>> = PgVec::new_in(mcx);
    let mut attach_infos: PgVec<'mcx, execindexing::IndexInfo<'mcx>> = PgVec::new_in(mcx);
    for &cld in attach_rel_idxs.iter() {
        let r = indexam::index_open(mcx, cld, AccessShareLock)?;
        attach_infos.push(execindexing::BuildIndexInfo(mcx, &r)?);
        attachrel_idx_rels.push(r);
    }

    // A foreign table can carry no constraint indexes: refuse if any parent
    // index is unique/primary, otherwise nothing to ensure (C's goto out).
    if attachrel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        for &idx in idxes.iter() {
            let idx_rel = indexam::index_open(mcx, idx, AccessShareLock)?;
            let ix = idx_rel.rd_index.as_ref().expect("rd_index");
            if ix.indisunique || ix.indisprimary {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "cannot attach foreign table \"{}\" as partition of partitioned table \"{}\"",
                            attachrel.name(),
                            rel.name()
                        ),
                    )
                    .with_detail(format!(
                        "Partitioned table \"{}\" contains unique indexes.",
                        rel.name()
                    ))
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            indexam::index_close(idx_rel, AccessShareLock)?;
        }
        for r in attachrel_idx_rels.into_iter() {
            indexam::index_close(r, AccessShareLock)?;
        }
        return Ok(());
    }

    for &idx in idxes.iter() {
        let idx_rel = indexam::index_open(mcx, idx, AccessShareLock)?;
        if idx_rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_INDEX {
            indexam::index_close(idx_rel, AccessShareLock)?;
            continue;
        }
        let info = execindexing::BuildIndexInfo(mcx, &idx_rel)?;
        let attmap = tupdesc::build_attrmap_by_name(mcx, attachrel.descr(), rel.descr())?;
        let constraint_oid =
            pg_constraint::get_relation_idx_constraint_oid(mcx, rel.rd_id, idx)?;
        let mut found = false;
        for (i, cld_rel) in attachrel_idx_rels.iter().enumerate() {
            let cld_idx_id = cld_rel.rd_id;
            if cld_rel.rd_rel.relispartition {
                continue;
            }
            if !cld_rel.rd_index.as_ref().expect("rd_index").indisvalid {
                continue;
            }
            if catalog_index::CompareIndexInfo(
                mcx,
                &attach_infos[i],
                &info,
                cld_rel,
                &idx_rel,
                &attmap,
            )? {
                let mut cld_constr_oid = InvalidOid;
                if constraint_oid != InvalidOid {
                    cld_constr_oid = pg_constraint::get_relation_idx_constraint_oid(
                        mcx,
                        attachrel.rd_id,
                        cld_idx_id,
                    )?;
                    if cld_constr_oid == InvalidOid {
                        continue;
                    }
                    if lsyscache::misc::get_constraint_type(constraint_oid)?
                        != lsyscache::misc::get_constraint_type(cld_constr_oid)?
                    {
                        continue;
                    }
                }
                indexcmds_seams::index_set_parent_index::call(mcx, cld_rel, idx)?;
                if constraint_oid != InvalidOid {
                    pg_constraint::ConstraintSetParentConstraint(
                        mcx,
                        cld_constr_oid,
                        constraint_oid,
                        attachrel.rd_id,
                    )?;
                }
                found = true;
                xact::CommandCounterIncrement()?;
                break;
            }
        }
        if !found {
            let (stmt, con_oid) =
                parse_utilcmd::generateClonedIndexStmt(mcx, None, &idx_rel, &attmap)?;
            indexcmds_seams::define_index::call(
                mcx,
                attachrel.rd_id,
                &stmt,
                InvalidOid,
                idx_rel.rd_id,
                con_oid,
                false,
                false,
                false,
                false,
                false,
            )?;
        }
        indexam::index_close(idx_rel, AccessShareLock)?;
    }

    for r in attachrel_idx_rels.into_iter() {
        indexam::index_close(r, AccessShareLock)?;
    }
    Ok(())
}

// PartConstraintImpliedByRelConstraint + ConstraintImpliedByRelConstraint
// (tablecmds.c:20051-20164) over the landed predtest engine.
pub(crate) fn PartConstraintImpliedByRelConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    scanrel: &Relation<'mcx>,
    part_constraint: &NodeList<'mcx>,
) -> PgResult<bool> {
    let desc = scanrel.descr();
    let mut exist_constraint: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if let Some(constr) = desc.constr.as_deref() {
        if constr.has_not_null {
            for i in 0..desc.natts as usize {
                let att = desc.attr(i);
                if att.attnotnull && !att.attisdropped {
                    let var = Node::mk(
                        mcx,
                        Var {
                            varno: 1,
                            varattno: att.attnum,
                            vartype: att.atttypid,
                            vartypmod: att.atttypmod,
                            varcollid: att.attcollation,
                            varnosyn: 1,
                            varattnosyn: att.attnum,
                            ..Default::default()
                        },
                    )?;
                    let ntest = Node::mk(
                        mcx,
                        NullTest {
                            arg: Some(var),
                            nulltesttype: NullTestType::IS_NOT_NULL,
                            argisrow: false,
                            location: -1,
                        },
                    )?;
                    exist_constraint.push(ntest);
                }
            }
        }
        for chk in constr.check.iter() {
            if !chk.ccvalid {
                continue;
            }
            debug_assert!(chk.ccenforced);
            let cexpr = readfuncs::stringToNode(
                mcx,
                chk.ccbin.as_ref().expect("ccbin").as_str(),
            )?;
            let cexpr = clauses::eval_const_expressions(mcx, cexpr)?;
            let cexpr = planner::prepqual::canonicalize_qual(mcx, cexpr, true)?;
            for n in clauses::make_ands_implicit(mcx, Some(cexpr))?.iter() {
                exist_constraint.push(n);
            }
        }
    }
    let mut pred: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for n in part_constraint.iter() {
        pred.push(n);
    }
    planner::predtest::predicate_implied_by(mcx, &pred, &exist_constraint, true)
}

// QueuePartitionConstraintValidation (tablecmds.c:20177): skip the scan when
// existing constraints imply the partition constraint; otherwise queue a
// phase-3 scan (recursing into sub-partitions).
fn QueuePartitionConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    scanrel: &Relation<'mcx>,
    part_constraint: &NodeList<'mcx>,
    validate_default: bool,
) -> PgResult<()> {
    if PartConstraintImpliedByRelConstraint(mcx, scanrel, part_constraint)? {
        let msg = if !validate_default {
            format!(
                "partition constraint for table \"{}\" is implied by existing constraints",
                scanrel.name()
            )
        } else {
            format!(
                "updated partition constraint for default partition \"{}\" is implied by existing constraints",
                scanrel.name()
            )
        };
        elog_seams::ereport::call(PgError::new(DEBUG1, msg))?;
        return Ok(());
    }

    if scanrel.rd_rel.relkind == RELKIND_RELATION {
        let mut tab = AlteredTableInfo::new(mcx, scanrel);
        tab.partition_constraint = Some(
            part_constraint
                .iter()
                .next()
                .expect("partition constraint is a single implicit-AND node"),
        );
        tab.validate_default = validate_default;
        wqueue.push(tab);
    } else if scanrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let pdesc = partdesc::RelationGetPartitionDesc(scanrel, true)?;
        for &part_oid in pdesc.oids.iter() {
            let part_rel = table::table_open(mcx, part_oid, AccessExclusiveLock)?;
            let this_constraint = partbounds::map_partition_varattnos(
                mcx,
                part_constraint.clone_in(mcx)?,
                1,
                &part_rel,
                scanrel,
            )?;
            QueuePartitionConstraintValidation(
                mcx,
                wqueue,
                &part_rel,
                &this_constraint,
                validate_default,
            )?;
            part_rel.close(NoLock)?;
        }
    }
    Ok(())
}

// The concurrent strategy runs in two transactions (tablecmds.c:20893-20910):
// first mark the pg_inherits row detach-pending and commit so every new
// snapshot omits the partition, then wait out lockers that could have planned
// with it and finish the detach.
pub(crate) fn ATExecDetachPartition<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
    query_string: &str,
) -> PgResult<Relation<'mcx>> {
    let concurrent = cmd.concurrent;
    let pdesc = partdesc::RelationGetPartitionDesc(&rel, true)?;
    let strategy = pdesc.boundinfo.as_ref().map(|b| b.strategy as u8);
    let default_part_oid = pdesc
        .boundinfo
        .as_ref()
        .filter(|b| b.has_default())
        .map(|b| pdesc.oids[b.default_index as usize])
        .unwrap_or(InvalidOid);
    if default_part_oid != InvalidOid {
        if concurrent {
            return Err(err(
                "cannot detach partitions concurrently when a default partition exists"
                    .to_string(),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        }
        lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
    }

    let mut part_rel = open_by_rangevar(
        mcx,
        cmd.name.expect("PartitionCmd.name"),
        if concurrent { types_rel::ShareUpdateExclusiveLock } else { AccessExclusiveLock },
    )?;

    if !concurrent {
        RemoveInheritance(mcx, &part_rel, &rel, false)?;
    } else {
        MarkInheritDetached(mcx, &part_rel, &rel)?;
    }

    ATDetachCheckNoForeignKeyRefs(mcx, &part_rel)?;

    let mut rel = rel;
    if concurrent {
        if strategy != Some(partbounds::PARTITION_STRATEGY_HASH) {
            DetachAddConstraintIfNeeded(mcx, wqueue, &part_rel, query_string)?;
        }

        let partrelid = part_rel.rd_id;
        let parentrelid = rel.rd_id;
        let parentrelname = rel.name().to_string();
        let partrelname = part_rel.name().to_string();

        inval::invalidate::CacheInvalidateRelcache(&rel)?;
        part_rel.close(NoLock)?;
        rel.close(NoLock)?;

        if snapmgr::ActiveSnapshotSet() {
            snapmgr::PopActiveSnapshot()?;
        }
        xact::CommitTransactionCommand()?;
        xact::StartTransactionCommand()?;

        let tag = types_storage::lock::LOCKTAG::relation(
            init_small::globals::MyDatabaseId(),
            parentrelid,
        );
        lmgr::WaitForLockersMultiple(mcx, &[tag], AccessExclusiveLock)?;

        let reopened = relation_seams::try_relation_open::call(
            mcx,
            parentrelid,
            types_rel::ShareUpdateExclusiveLock,
        )?;
        let part_reopened =
            relation_seams::try_relation_open::call(mcx, partrelid, AccessExclusiveLock)?;
        let Some(r) = reopened else {
            if part_reopened.is_some() {
                elog_seams::ereport_msg::call(
                    types_error::WARNING,
                    format!("dangling partition \"{partrelname}\" remains, can't fix"),
                    None,
                )?;
            }
            return Err(err(
                format!("partitioned table \"{parentrelname}\" was removed concurrently"),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        };
        rel = r;
        let Some(p) = part_reopened else {
            return Err(err(
                format!("partition \"{partrelname}\" was removed concurrently"),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        };
        part_rel = p;
    }

    // Detaching may involve TOAST access, which needs a valid snapshot.
    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    let res = DetachPartitionFinalize(mcx, &rel, &part_rel, concurrent, default_part_oid);
    snapmgr::PopActiveSnapshot()?;
    res?;

    part_rel.close(NoLock)?;
    Ok(rel)
}

pub(crate) fn ATExecDetachPartitionFinalize<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    name: &types_nodes::primnodes::RangeVar<'_>,
) -> PgResult<()> {
    let snap = snapmgr::GetActiveSnapshot();
    let part_rel = open_by_rangevar(mcx, name, AccessExclusiveLock)?;
    // A canceled second transaction of DETACH CONCURRENTLY may leave snapshots
    // that still see the partition as attached; wait them out before
    // completing (tablecmds.c:21436-21448).
    indexcmds_seams::wait_for_older_snapshots::call(snap.xmin)?;
    DetachPartitionFinalize(mcx, rel, &part_rel, true, InvalidOid)?;
    part_rel.close(NoLock)
}

// MarkInheritDetached (tablecmds.c:17867).
fn MarkInheritDetached<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    debug_assert!(parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
    let catrel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let keys = [oid_scankey(
        pg_inherits::Anum_pg_inherits_inhparent as usize,
        parent_rel.rd_id,
    )];
    let mut scan =
        genam::systable_beginscan(mcx, &catrel, InheritsParentIndexId, true, None, &keys)?;
    let desc = catrel.descr();
    let mut found = false;
    let mut update: Option<types_tuple::ItemPointerData> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (pending, _) =
            getattr(tup, desc, pg_inherits::Anum_pg_inherits_inhdetachpending as usize);
        let (inhrelid, _) =
            getattr(tup, desc, pg_inherits::Anum_pg_inherits_inhrelid as usize);
        if pending.as_bool() {
            let relname = lsyscache::get_rel_name(mcx, inhrelid.as_oid())?
                .map(|n| n.to_string())
                .unwrap_or_default();
            let nspname =
                lsyscache::get_namespace_name(mcx, parent_rel.rd_rel.relnamespace)?
                    .map(|n| n.to_string())
                    .unwrap_or_default();
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "partition \"{relname}\" already pending detach in partitioned \
                         table \"{nspname}.{}\"",
                        parent_rel.name()
                    ),
                )
                .with_hint(
                    "Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the \
                     pending detach operation.",
                )
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if inhrelid.as_oid() == child_rel.rd_id {
            update = Some(tup.t_self);
            found = true;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    if let Some(otid) = update {
        let keys = [oid_scankey(
            pg_inherits::Anum_pg_inherits_inhparent as usize,
            parent_rel.rd_id,
        )];
        let mut scan =
            genam::systable_beginscan(mcx, &catrel, InheritsParentIndexId, true, None, &keys)?;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            if tup.t_self != otid {
                continue;
            }
            let natts = desc.natts as usize;
            let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[pg_inherits::Anum_pg_inherits_inhdetachpending as usize - 1] =
                Datum::from_bool(true);
            replace[pg_inherits::Anum_pg_inherits_inhdetachpending as usize - 1] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &catrel, &otid, &mut newtup)?;
            break;
        }
        genam::systable_endscan(mcx, scan)?;
    }
    catrel.close(RowExclusiveLock)?;
    if !found {
        return Err(err(
            format!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                child_rel.name(),
                parent_rel.name()
            ),
            ERRCODE_UNDEFINED_TABLE,
        ));
    }
    Ok(())
}

// DetachAddConstraintIfNeeded (tablecmds.c:21464): supplant the partition
// constraint with an equivalent CHECK constraint unless already implied.
fn DetachAddConstraintIfNeeded<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    part_rel: &Relation<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let mut constraint_expr = NodeList::nil();
    for q in partdesc::RelationGetPartitionQual(mcx, part_rel)?.iter() {
        constraint_expr.lappend(mcx, clauses::eval_const_expressions(mcx, q)?)?;
    }
    if PartConstraintImpliedByRelConstraint(mcx, part_rel, &constraint_expr)? {
        return Ok(());
    }
    let tabidx = crate::alter::ATGetQueueEntry(mcx, wqueue, part_rel);
    let explicit = partbounds::make_ands_explicit(mcx, constraint_expr)?;
    let cooked = outfuncs::nodeToString(mcx, explicit)?;
    let mut n = Node::build::<types_nodes::rawnodes::Constraint>(mcx)?;
    n.contype = types_nodes::rawnodes::ConstrType::CONSTR_CHECK;
    n.conname = None;
    n.is_no_inherit = false;
    n.raw_expr = None;
    n.cooked_expr = Some(crate::constraints::str_in(mcx, cooked.as_str())?);
    n.is_enforced = true;
    n.initially_valid = true;
    n.skip_validation = true;
    crate::alter::ATAddCheckNNConstraint(
        mcx,
        wqueue,
        tabidx,
        part_rel,
        n.seal(),
        true,
        false,
        true,
        types_rel::ShareUpdateExclusiveLock,
        query_string,
    )
}

// RemoveInheritance (tablecmds.c:17950), partition direction.
fn RemoveInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    expect_detached: bool,
) -> PgResult<()> {
    debug_assert!(parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
    let found = pg_inherits::DeleteInheritsTuple(
        mcx,
        child_rel.rd_id,
        parent_rel.rd_id,
        expect_detached,
        Some(child_rel.name()),
    )?;
    if !found {
        return Err(err(
            format!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                child_rel.name(),
                parent_rel.name()
            ),
            ERRCODE_UNDEFINED_TABLE,
        ));
    }

    let attrrel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    {
        let keys = [oid_scankey(1, child_rel.rd_id)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &attrrel,
            AttributeRelidNumIndexId,
            true,
            None,
            &keys,
        )?;
        let desc = attrrel.descr();
        struct Upd {
            tid: types_tuple::ItemPointerData,
            inhcount: i16,
        }
        let mut upds: PgVec<'mcx, Upd> = PgVec::new_in(mcx);
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let (dropped, _) = getattr(tup, desc, Anum_pg_attribute_attisdropped);
            if dropped.as_bool() {
                continue;
            }
            let (inhcount, _) = getattr(tup, desc, Anum_pg_attribute_attinhcount);
            if inhcount.as_i16() <= 0 {
                continue;
            }
            let (attnum, _) = getattr(tup, desc, Anum_pg_attribute_attnum);
            if attnum.as_i16() <= 0 {
                continue;
            }
            let colname = name_at(tup, desc, 2).to_string();
            if attname_lookup(mcx, parent_rel.rd_id, &colname, false)?.is_none() {
                continue;
            }
            upds.push(Upd { tid: tup.t_self, inhcount: inhcount.as_i16() });
        }
        genam::systable_endscan(mcx, scan)?;
        let desc = attrrel.descr();
        for u in upds.iter() {
            let keys = [oid_scankey(1, child_rel.rd_id)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &attrrel,
                AttributeRelidNumIndexId,
                true,
                None,
                &keys,
            )?;
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                if tup.t_self != u.tid {
                    continue;
                }
                let natts = desc.natts as usize;
                let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
                let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
                let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
                values.resize(natts, Datum::null());
                nulls.resize(natts, false);
                replace.resize(natts, false);
                let newcount = u.inhcount - 1;
                values[Anum_pg_attribute_attinhcount - 1] = Datum::from_i16(newcount);
                replace[Anum_pg_attribute_attinhcount - 1] = true;
                if newcount == 0 {
                    values[Anum_pg_attribute_attislocal - 1] = Datum::from_bool(true);
                    replace[Anum_pg_attribute_attislocal - 1] = true;
                }
                let mut newtup =
                    heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
                let otid = tup.t_self;
                catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
                break;
            }
            genam::systable_endscan(mcx, scan)?;
        }
    }
    attrrel.close(RowExclusiveLock)?;

    let conrel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    {
        let parent_cons = scan_constraints(mcx, &conrel, parent_rel.rd_id)?;
        let mut connames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
        let mut nncolumns: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
        // tablecmds.c:18032: parent NN attnos are matched in child attno space.
        let attmap = tupdesc::build_attrmap_by_name(mcx, child_rel.descr(), parent_rel.descr())?;
        for c in parent_cons.iter() {
            if c.connoinherit {
                continue;
            }
            if c.contype == CONSTRAINT_CHECK {
                connames.push(c.name.as_str());
            } else if c.contype == CONSTRAINT_NOTNULL {
                nncolumns.push(attmap[c.nn_attno as usize - 1]);
            }
        }
        let child_cons = scan_constraints(mcx, &conrel, child_rel.rd_id)?;
        let desc = conrel.descr();
        let mut matched_names = 0usize;
        let mut matched_cols = 0usize;
        for ccon in child_cons.iter() {
            let matches = if ccon.contype == CONSTRAINT_CHECK {
                if let Some(p) = connames.iter().position(|&n| n == ccon.name.as_str()) {
                    connames.remove(p);
                    matched_names += 1;
                    true
                } else {
                    false
                }
            } else if ccon.contype == CONSTRAINT_NOTNULL {
                if let Some(p) = nncolumns.iter().position(|&a| a == ccon.nn_attno) {
                    nncolumns.remove(p);
                    matched_cols += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if !matches {
                continue;
            }
            if ccon.coninhcount <= 0 {
                panic!(
                    "relation {} has non-inherited constraint \"{}\"",
                    child_rel.rd_id,
                    ccon.name.as_str()
                );
            }
            let keys = [oid_scankey(pg_constraint::Anum_pg_constraint_conrelid as usize, child_rel.rd_id)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &conrel,
                pg_constraint::ConstraintRelidTypidNameIndexId,
                true,
                None,
                &keys,
            )?;
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                let (oid, _) = getattr(tup, desc, pg_constraint::Anum_pg_constraint_oid as usize);
                if oid.as_oid() != ccon.oid {
                    continue;
                }
                let natts = desc.natts as usize;
                let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
                let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
                let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
                values.resize(natts, Datum::null());
                nulls.resize(natts, false);
                replace.resize(natts, false);
                let newcount = ccon.coninhcount - 1;
                values[pg_constraint::Anum_pg_constraint_coninhcount as usize - 1] =
                    Datum::from_i16(newcount);
                replace[pg_constraint::Anum_pg_constraint_coninhcount as usize - 1] = true;
                if newcount == 0 {
                    values[pg_constraint::Anum_pg_constraint_conislocal as usize - 1] =
                        Datum::from_bool(true);
                    replace[pg_constraint::Anum_pg_constraint_conislocal as usize - 1] = true;
                }
                let mut newtup =
                    heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
                let otid = tup.t_self;
                catalog_indexing::CatalogTupleUpdate(mcx, &conrel, &otid, &mut newtup)?;
                break;
            }
            genam::systable_endscan(mcx, scan)?;
        }
        if !connames.is_empty() || !nncolumns.is_empty() {
            panic!(
                "{} unmatched constraints while removing inheritance from \"{}\" to \"{}\"",
                connames.len() + nncolumns.len(),
                child_rel.name(),
                parent_rel.name()
            );
        }
        let _ = (matched_names, matched_cols);
    }
    conrel.close(RowExclusiveLock)?;

    drop_parent_dependency(mcx, child_rel.rd_id, parent_rel.rd_id)?;
    Ok(())
}

// drop_parent_dependency (tablecmds.c:18163), partition arm: the AUTO
// dependency StoreCatalogInheritance1 recorded on the parent.
fn drop_parent_dependency<'mcx>(mcx: Mcx<'mcx>, relid: Oid, parent_relid: Oid) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys = [oid_scankey(1, RELATION_RELATION_ID), oid_scankey(2, relid)];
    let mut scan =
        genam::systable_beginscan(mcx, &dep_rel, DependDependerIndexId, true, None, &keys)?;
    let desc = dep_rel.descr();
    let mut tids: PgVec<'mcx, types_tuple::ItemPointerData> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (objsubid, _) = getattr(tup, desc, 3);
        let (refclassid, _) = getattr(tup, desc, 4);
        let (refobjid, _) = getattr(tup, desc, 5);
        let (deptype, _) = getattr(tup, desc, 7);
        if objsubid.as_i32() == 0
            && refclassid.as_oid() == RELATION_RELATION_ID
            && refobjid.as_oid() == parent_relid
            && deptype.as_i8() as u8 == b'a'
        {
            tids.push(tup.t_self);
        }
    }
    genam::systable_endscan(mcx, scan)?;
    for tid in tids.iter() {
        catalog_indexing::CatalogTupleDelete(&dep_rel, tid)?;
    }
    dep_rel.close(RowExclusiveLock)
}

// GetParentedForeignKeyRefs (tablecmds.c:21942): inbound FKs that are children
// of a partitioned-table FK. RI machinery is fk-lane, so any hit is loud in
// ATDetachCheckNoForeignKeyRefs.
fn GetParentedForeignKeyRefs<'mcx>(
    mcx: Mcx<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<PgVec<'mcx, Oid>> {
    use pg_constraint::*;
    let conrel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_scankey(Anum_pg_constraint_confrelid as usize, partition.rd_id)];
    let mut scan = genam::systable_beginscan(mcx, &conrel, InvalidOid, false, None, &keys)?;
    let desc = conrel.descr();
    let mut out: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (contype, _) = getattr(tup, desc, Anum_pg_constraint_contype as usize);
        if contype.as_i8() as u8 != CONSTRAINT_FOREIGN {
            continue;
        }
        let (conparentid, _) = getattr(tup, desc, Anum_pg_constraint_conparentid as usize);
        if conparentid.as_oid() == InvalidOid {
            continue;
        }
        let (oid, _) = getattr(tup, desc, Anum_pg_constraint_oid as usize);
        out.push(oid.as_oid());
    }
    genam::systable_endscan(mcx, scan)?;
    conrel.close(AccessShareLock)?;
    Ok(out)
}

fn ATDetachCheckNoForeignKeyRefs<'mcx>(mcx: Mcx<'mcx>, partition: &Relation<'mcx>) -> PgResult<()> {
    let refs = GetParentedForeignKeyRefs(mcx, partition)?;
    for &constr_oid in refs.iter() {
        crate::fk::partition_remove_check(mcx, partition, constr_oid)?;
    }
    Ok(())
}

// DetachPartitionFinalize (tablecmds.c:21095).
fn DetachPartitionFinalize<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    part_rel: &Relation<'mcx>,
    concurrent: bool,
    default_part_oid: Oid,
) -> PgResult<()> {
    if concurrent {
        RemoveInheritance(mcx, part_rel, rel, true)?;
    }

    DropClonedTriggersFromPartition(mcx, part_rel.rd_id)?;

    crate::fk::detach_partition_finalize_fks(mcx, part_rel)?;

    for &constr_oid in GetParentedForeignKeyRefs(mcx, part_rel)?.iter() {
        crate::fk::detach_referenced_fk_sub_constraint(mcx, constr_oid)?;
    }

    let indexes = relcache::RelationGetIndexList(mcx, part_rel.rd_id)?;
    for &idxid in indexes.iter() {
        if !pg_inherits::has_superclass(mcx, idxid)? {
            continue;
        }
        let parentidx = pg_inherits::get_partition_parent(mcx, idxid, false)?;
        assert!(
            catalog_index::IndexGetRelation(mcx, parentidx, false)? == rel.rd_id,
            "detached index {idxid} has parent index {parentidx} on another relation"
        );
        let idx = indexam::index_open(mcx, idxid, AccessExclusiveLock)?;
        indexcmds_seams::index_set_parent_index::call(mcx, &idx, InvalidOid)?;
        let constr_oid =
            pg_constraint::get_relation_idx_constraint_oid(mcx, part_rel.rd_id, idxid)?;
        let parent_constr_oid =
            pg_constraint::get_relation_idx_constraint_oid(mcx, rel.rd_id, parentidx)?;
        if parent_constr_oid != InvalidOid && constr_oid != InvalidOid {
            pg_constraint::ConstraintSetParentConstraint(mcx, constr_oid, InvalidOid, InvalidOid)?;
        }
        indexam::index_close(idx, NoLock)?;
    }

    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    {
        let keys = [oid_scankey(1, part_rel.rd_id)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &class_rel,
            catalog::ClassOidIndexId,
            true,
            None,
            &keys,
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {}", part_rel.rd_id));
        let desc = class_rel.descr();
        let (ispart, _) = getattr(tup, desc, Anum_pg_class_relispartition);
        debug_assert!(ispart.as_bool());
        let natts = desc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        nulls[Anum_pg_class_relpartbound - 1] = true;
        replace[Anum_pg_class_relpartbound - 1] = true;
        values[Anum_pg_class_relispartition - 1] = Datum::from_bool(false);
        replace[Anum_pg_class_relispartition - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
    }
    class_rel.close(RowExclusiveLock)?;

    for i in 0..part_rel.descr().natts as usize {
        let att = part_rel.descr().attr(i);
        if !att.attisdropped && att.attidentity != 0 {
            let attname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
            crate::alter::ATExecDropIdentity(
                mcx,
                part_rel,
                attname,
                false,
                AccessExclusiveLock,
                true,
                true,
            )?;
        }
    }

    if default_part_oid != InvalidOid {
        if part_rel.rd_id == default_part_oid {
            catalog_heap::partition::update_default_partition_oid(mcx, rel.rd_id, InvalidOid)?;
        } else {
            inval::invalidate::CacheInvalidateRelcacheByRelid(default_part_oid)?;
        }
    }

    inval::invalidate::CacheInvalidateRelcache(rel)?;

    if part_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let children =
            pg_inherits::find_all_inheritors(mcx, part_rel.rd_id, AccessExclusiveLock)?;
        for &c in children.iter() {
            inval::invalidate::CacheInvalidateRelcacheByRelid(c)?;
        }
    }
    Ok(())
}


// DropClonedTriggersFromPartition (tablecmds.c:21506).
fn DropClonedTriggersFromPartition<'mcx>(mcx: Mcx<'mcx>, partition_id: Oid) -> PgResult<()> {
    const Anum_pg_trigger_oid: usize = 1;
    const Anum_pg_trigger_tgparentid: usize = 3;
    const Anum_pg_trigger_tgconstrrelid: usize = 9;
    let tgrel = table::table_open(mcx, TriggerRelationId, RowExclusiveLock)?;
    let keys = [oid_scankey(2, partition_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &tgrel, TriggerRelidNameIndexId, true, None, &keys)?;
    let desc = tgrel.descr();
    let mut objects = catalog_dependency::ObjectAddresses::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (tgparentid, _) = getattr(tup, desc, Anum_pg_trigger_tgparentid);
        if tgparentid.as_oid() == InvalidOid {
            continue;
        }
        let (tgconstrrelid, _) = getattr(tup, desc, Anum_pg_trigger_tgconstrrelid);
        if tgconstrrelid.as_oid() != InvalidOid {
            continue;
        }
        let (oid, _) = getattr(tup, desc, Anum_pg_trigger_oid);
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            TriggerRelationId,
            oid.as_oid(),
            TriggerRelationId,
            pg_depend::DependencyType::PartitionPri,
        )?;
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            TriggerRelationId,
            oid.as_oid(),
            RELATION_RELATION_ID,
            pg_depend::DependencyType::PartitionSec,
        )?;
        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            TriggerRelationId,
            oid.as_oid(),
        ));
    }
    genam::systable_endscan(mcx, scan)?;
    // C increments unconditionally (tablecmds.c:21560); DetachPartitionFinalize's
    // identity drop relies on it to see RemoveInheritance's pg_attribute updates.
    xact::CommandCounterIncrement()?;
    catalog_dependency::performMultipleDeletions(
        mcx,
        &objects,
        catalog_dependency::DropBehavior::DROP_RESTRICT,
        catalog_dependency::PERFORM_DELETION_INTERNAL,
    )?;
    tgrel.close(RowExclusiveLock)
}

// ATExecAttachPartitionIdx (tablecmds.c:21633): ALTER INDEX .. ATTACH
// PARTITION.
pub(crate) fn ATExecAttachPartitionIdx<'mcx>(
    mcx: Mcx<'mcx>,
    parent_idx: &Relation<'mcx>,
    name: &types_nodes::primnodes::RangeVar<'_>,
) -> PgResult<()> {
    // Lock the owning table before its index, and the partition's parent
    // table too, to read its tuple descriptor without deadlock risk.
    let parent_tbl_oid = parent_idx.rd_index.as_ref().expect("rd_index").indrelid;
    let mut partition_oid = InvalidOid;
    let mut locked_parent_tbl = false;
    let rv = rel_vocab::RangeVar {
        catalogname: name.catalogname,
        schemaname: name.schemaname,
        relname: name.relname.expect("RangeVar.relname"),
        inh: name.inh,
        relpersistence: name.relpersistence,
        location: name.location,
    };
    let mut callback = |rv: &rel_vocab::RangeVar<'_>,
                        rel_oid: Oid,
                        old_rel_oid: Oid|
     -> PgResult<()> {
        if !locked_parent_tbl {
            lmgr::LockRelationOid(parent_tbl_oid, AccessShareLock)?;
            locked_parent_tbl = true;
        }
        // A prior lookup's heap lock is useless if the name now resolves
        // elsewhere.
        if rel_oid != old_rel_oid && partition_oid != InvalidOid {
            lmgr::UnlockRelationOid(partition_oid, AccessShareLock)?;
            partition_oid = InvalidOid;
        }
        if rel_oid == InvalidOid {
            return Ok(());
        }
        let relkind = lsyscache::relation::get_rel_relkind(rel_oid)? as u8;
        if relkind == 0 {
            return Ok(());
        }
        if relkind != types_rel::RELKIND_PARTITIONED_INDEX
            && relkind != types_rel::RELKIND_INDEX
        {
            return Err(err(
                format!("\"{}\" is not an index", rv.relname),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
        // The heap's tupledesc is all we examine; AccessShareLock suffices.
        partition_oid = catalog_index::IndexGetRelation(mcx, rel_oid, false)?;
        lmgr::LockRelationOid(partition_oid, AccessShareLock)
    };
    let part_idx_id = catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        AccessExclusiveLock,
        0,
        Some(&mut callback),
    )?;
    if part_idx_id == InvalidOid {
        return Err(err(
            format!("index \"{}\" does not exist", rv.relname),
            types_error::ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    let state_partition_oid = partition_oid;

    let part_idx = indexam::index_open(mcx, part_idx_id, AccessExclusiveLock)?;
    let parent_tbl = table::table_open(mcx, parent_tbl_oid, AccessShareLock)?;
    let part_tbl = table::table_open(
        mcx,
        part_idx.rd_index.as_ref().expect("rd_index").indrelid,
        NoLock,
    )?;

    let curr_parent = if part_idx.rd_rel.relispartition {
        pg_inherits::get_partition_parent(mcx, part_idx_id, false)?
    } else {
        InvalidOid
    };
    if curr_parent != parent_idx.rd_id {
        refuse_dupe_index_attach(mcx, parent_idx, &part_idx, &part_tbl)?;

        if curr_parent != InvalidOid {
            return Err(Box::new(
                (*err(
                    format!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        part_idx.name(),
                        parent_idx.name()
                    ),
                    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                ))
                .with_detail(format!(
                    "Index \"{}\" is already attached to another index.",
                    part_idx.name()
                )),
            ));
        }

        let pd = partdesc::RelationGetPartitionDesc(&parent_tbl, true)?;
        if !pd.oids.iter().any(|&o| o == state_partition_oid) {
            return Err(Box::new(
                (*err(
                    format!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        part_idx.name(),
                        parent_idx.name()
                    ),
                    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                ))
                .with_detail(format!(
                    "Index \"{}\" is not an index on any partition of table \"{}\".",
                    part_idx.name(),
                    parent_tbl.name()
                )),
            ));
        }

        let child_info = execindexing::BuildIndexInfo(mcx, &part_idx)?;
        let parent_info = execindexing::BuildIndexInfo(mcx, parent_idx)?;
        let attmap = tupdesc::build_attrmap_by_name(mcx, part_tbl.descr(), parent_tbl.descr())?;
        if !catalog_index::CompareIndexInfo(
            mcx,
            &child_info,
            &parent_info,
            &part_idx,
            parent_idx,
            &attmap,
        )? {
            return Err(Box::new(
                (*err(
                    format!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        part_idx.name(),
                        parent_idx.name()
                    ),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ))
                .with_detail("The index definitions do not match.".to_string()),
            ));
        }

        // A constraint in the parent requires one in the child too.
        let constraint_oid =
            pg_constraint::get_relation_idx_constraint_oid(mcx, parent_tbl.rd_id, parent_idx.rd_id)?;
        let mut cld_constr_id = InvalidOid;
        if constraint_oid != InvalidOid {
            cld_constr_id =
                pg_constraint::get_relation_idx_constraint_oid(mcx, part_tbl.rd_id, part_idx_id)?;
            if cld_constr_id == InvalidOid {
                return Err(Box::new(
                    (*err(
                        format!(
                            "cannot attach index \"{}\" as a partition of index \"{}\"",
                            part_idx.name(),
                            parent_idx.name()
                        ),
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                    ))
                    .with_detail(format!(
                        "The index \"{}\" belongs to a constraint in table \"{}\" but no constraint exists for index \"{}\".",
                        parent_idx.name(),
                        parent_tbl.name(),
                        part_idx.name()
                    )),
                ));
            }
        }

        if parent_idx.rd_index.as_ref().expect("rd_index").indisprimary {
            verify_partition_index_not_null(&child_info, &part_tbl)?;
        }

        indexcmds_seams::index_set_parent_index::call(mcx, &part_idx, parent_idx.rd_id)?;
        if constraint_oid != InvalidOid {
            pg_constraint::ConstraintSetParentConstraint(
                mcx,
                cld_constr_id,
                constraint_oid,
                part_tbl.rd_id,
            )?;
        }

        validate_partitioned_index(mcx, parent_idx, &parent_tbl)?;
    }

    parent_tbl.close(AccessShareLock)?;
    part_tbl.close(NoLock)?;
    part_idx.close(NoLock)
}

// refuseDupeIndexAttach (tablecmds.c:21797).
fn refuse_dupe_index_attach<'mcx>(
    mcx: Mcx<'mcx>,
    parent_idx: &Relation<'mcx>,
    part_idx: &Relation<'mcx>,
    partition_tbl: &Relation<'mcx>,
) -> PgResult<()> {
    let existing_idx =
        pg_inherits::index_get_partition(mcx, partition_tbl.rd_id, parent_idx.rd_id)?;
    if existing_idx != InvalidOid {
        return Err(Box::new(
            (*err(
                format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    part_idx.name(),
                    parent_idx.name()
                ),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ))
            .with_detail(format!(
                "Another index is already attached for partition \"{}\".",
                partition_tbl.name()
            )),
        ));
    }
    Ok(())
}

// validatePartitionedIndex (tablecmds.c:21818): mark the parent index valid
// once every partition has a valid attached index.
fn validate_partitioned_index<'mcx>(
    mcx: Mcx<'mcx>,
    parted_idx: &Relation<'mcx>,
    parted_tbl: &Relation<'mcx>,
) -> PgResult<()> {
    const IndexRelationId: Oid = 2610;
    const IndexRelidIndexId: Oid = 2679;
    const Anum_pg_index_indisvalid: usize = 11;
    debug_assert!(parted_idx.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX);

    let mut tuples = 0usize;
    {
        let inherits_rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
        let keys = [oid_scankey(
            pg_inherits::Anum_pg_inherits_inhparent as usize,
            parted_idx.rd_id,
        )];
        let mut scan = genam::systable_beginscan(
            mcx,
            &inherits_rel,
            InheritsParentIndexId,
            true,
            None,
            &keys,
        )?;
        let desc = inherits_rel.descr();
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let inhrelid =
                getattr(tup, desc, pg_inherits::Anum_pg_inherits_inhrelid as usize).0.as_oid();
            if lsyscache::relation::get_index_isvalid(inhrelid)? {
                tuples += 1;
            }
        }
        genam::systable_endscan(mcx, scan)?;
        inherits_rel.close(AccessShareLock)?;
    }

    let mut updated = false;
    if tuples == partdesc::RelationGetPartitionDesc(parted_tbl, true)?.nparts {
        let idx_rel = table::table_open(mcx, IndexRelationId, RowExclusiveLock)?;
        let keys = [oid_scankey(1, parted_idx.rd_id)];
        let mut scan =
            genam::systable_beginscan(mcx, &idx_rel, IndexRelidIndexId, true, None, &keys)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {}", parted_idx.rd_id));
        let desc = idx_rel.descr();
        let natts = desc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_index_indisvalid - 1] = Datum::from_bool(true);
        replace[Anum_pg_index_indisvalid - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &idx_rel, &otid, &mut newtup)?;
        updated = true;
        idx_rel.close(RowExclusiveLock)?;
    }

    // Validating this index might complete a grandparent index too.
    if updated && parted_idx.rd_rel.relispartition {
        xact::CommandCounterIncrement()?;
        let parent_idx_id = pg_inherits::get_partition_parent(mcx, parted_idx.rd_id, false)?;
        let parent_tbl_id = pg_inherits::get_partition_parent(mcx, parted_tbl.rd_id, false)?;
        let parent_idx = indexam::index_open(mcx, parent_idx_id, AccessExclusiveLock)?;
        let parent_tbl = table::table_open(mcx, parent_tbl_id, AccessExclusiveLock)?;
        debug_assert!(!parent_idx.rd_index.as_ref().expect("rd_index").indisvalid);
        validate_partitioned_index(mcx, &parent_idx, &parent_tbl)?;
        indexam::index_close(parent_idx, AccessExclusiveLock)?;
        parent_tbl.close(AccessExclusiveLock)?;
    }
    Ok(())
}

// verifyPartitionIndexNotNull (tablecmds.c:21905): a primary key partition's
// columns must all be NOT NULL.
fn verify_partition_index_not_null<'mcx>(
    iinfo: &execindexing::IndexInfo<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<()> {
    for i in 0..iinfo.ii_NumIndexKeyAttrs as usize {
        let att = partition.descr().attr(iinfo.ii_IndexAttrNumbers[i] as usize - 1);
        if !att.attnotnull {
            let colname =
                core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
            return Err(Box::new(
                (*err(
                    "invalid primary key definition".to_string(),
                    types_error::ERRCODE_INVALID_TABLE_DEFINITION,
                ))
                .with_detail(format!(
                    "Column \"{colname}\" of relation \"{}\" is not marked NOT NULL.",
                    partition.name()
                )),
            ));
        }
    }
    Ok(())
}
