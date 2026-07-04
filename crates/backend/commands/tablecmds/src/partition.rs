// tablecmds.c partition DDL slice: transformPartitionSpec /
// ComputePartitionAttrs (column-name keys) / StoreCatalogInheritance /
// SetRelationHasSubclass. Expression keys, named collations, ATTACH/DETACH
// are loud.
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, BTREE_AM_OID, HASH_AM_OID, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_nodes::rawnodes::{PartitionElem, PartitionSpec, PartitionStrategy};
use types_rel::{Relation, RowExclusiveLock};

use crate::unported;
use types_nodes::{Node, NodeList};

pub(crate) struct PartKeyInfo<'mcx> {
    pub strategy: u8,
    pub partattrs: mcx::PgVec<'mcx, AttrNumber>,
    pub partopclass: mcx::PgVec<'mcx, Oid>,
    pub partcollation: mcx::PgVec<'mcx, Oid>,
}

// transformPartitionSpec + ComputePartitionAttrs, fused (no expressions, so
// there is no transformExpr pass to separate).
pub(crate) fn compute_partition_key<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    partspec: &PartitionSpec<'mcx>,
    query_string: &str,
) -> PgResult<PartKeyInfo<'mcx>> {
    let strategy = partspec.strategy;
    if strategy == PartitionStrategy::List && partspec.partParams.len() != 1 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot use \"list\" partition strategy with more than one column".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    if partspec.partParams.len() > partcache::PARTITION_MAX_KEYS {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot partition using more than {} columns",
                    partcache::PARTITION_MAX_KEYS
                ),
            )
            .with_sqlstate(types_error::ERRCODE_TOO_MANY_COLUMNS),
        ));
    }

    let n = partspec.partParams.len();
    let mut info = PartKeyInfo {
        strategy: strategy as u8,
        partattrs: mcx::vec_with_capacity_in(mcx, n)?,
        partopclass: mcx::vec_with_capacity_in(mcx, n)?,
        partcollation: mcx::vec_with_capacity_in(mcx, n)?,
    };

    for pnode in partspec.partParams.iter() {
        let pelem = pnode.as_variant::<PartitionElem>().expect("PartitionElem");
        if pelem.expr.is_some() {
            unported("expression partition keys");
        }
        if !pelem.collation.is_nil() {
            unported("COLLATE in partition keys");
        }
        let name = pelem.name.expect("PartitionElem name");
        let mut attnum: AttrNumber = 0;
        let mut atttype: Oid = InvalidOid;
        let mut attcollation: Oid = InvalidOid;
        let mut attgenerated: i8 = 0;
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            if att.attname.name_str() == name.as_bytes() && !att.attisdropped {
                attnum = att.attnum;
                atttype = att.atttypid;
                attcollation = att.attcollation;
                attgenerated = att.attgenerated;
                break;
            }
        }
        if attnum == 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("column \"{name}\" named in partition key does not exist"),
                )
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        if attnum < 0 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot use system column \"{name}\" in partition key"),
                )
                .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
        if attgenerated != 0 {
            return Err(Box::new(
                PgError::new(ERROR, "cannot use generated column in partition key".to_string())
                    .with_detail(format!("Column \"{name}\" is a generated column."))
                    .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .with_cursor_position(parser_small1::parser_errposition_source(
                        Some(query_string.as_bytes()),
                        pelem.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
            ));
        }
        info.partattrs.push(attnum);
        // Collation consistency (type_is_collatable arms): the collatable
        // path carries att's own collation; COLLATE overrides are loud above.
        info.partcollation.push(attcollation);

        let (am_oid, am_name) = if strategy == PartitionStrategy::Hash {
            (HASH_AM_OID, "hash")
        } else {
            (BTREE_AM_OID, "btree")
        };
        let opclass = if pelem.opclass.is_nil() {
            let oc = indexcmds_seams::get_default_opclass::call(atttype, am_oid)?;
            if oc == InvalidOid {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "data type {} has no default operator class for access method \"{am_name}\"",
                            format_type::format_type_be(atttype)?
                        ),
                    )
                    .with_hint(format!(
                        "You must specify a {am_name} operator class or define a default \
                         {am_name} operator class for the data type."
                    ))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            oc
        } else {
            unported("named operator classes in partition keys (ResolveOpClass)");
        };
        info.partopclass.push(opclass);
    }
    Ok(info)
}

// StoreCatalogInheritance + StoreCatalogInheritance1, partition arm.
pub(crate) fn store_catalog_inheritance1<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    parent_oid: Oid,
) -> PgResult<()> {
    pg_inherits::StoreSingleInheritance(mcx, relation_id, parent_oid, 1)?;
    let childobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, relation_id);
    let parentobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, parent_oid);
    pg_depend::recordDependencyOn(
        mcx,
        &childobject,
        &parentobject,
        pg_depend::DependencyType::Auto,
    )?;
    SetRelationHasSubclass(mcx, parent_oid, true)
}

// SetRelationHasSubclass (tablecmds.c).
pub fn SetRelationHasSubclass<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    relhassubclass: bool,
) -> PgResult<()> {
    const Anum_pg_class_relhassubclass: usize = 23;
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_scankey(1, relation_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &class_rel, catalog::ClassOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relation_id}"));
    let desc = class_rel.descr();
    let mut isnull = false;
    // SAFETY: relhassubclass is a fixed NOT NULL pg_class column.
    let current = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_class_relhassubclass as i32, desc, &mut isnull)
    }
    .as_bool();
    if current != relhassubclass {
        let natts = desc.natts as usize;
        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_class_relhassubclass - 1] = Datum::from_bool(relhassubclass);
        replace[Anum_pg_class_relhassubclass - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
    } else {
        genam::systable_endscan(mcx, scan)?;
        inval::invalidate::CacheInvalidateRelcacheByRelid(relation_id)?;
    }
    class_rel.close(RowExclusiveLock)
}

fn oid_scankey(attno: types_core::AttrNumber, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

// transformPartitionBound/transformPartitionBoundValue (C: parse_utilcmd.c),
// hosted here because parse_expr -> parse_utilcmd would cycle (constraints.rs
// precedent).
pub(crate) fn transformPartitionBound<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut parser_small1::ParseState<'_, 'mcx>,
    parent: &Relation<'mcx>,
    spec_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::rawnodes::{
        PartitionBoundSpec, PartitionRangeDatum, PartitionRangeDatumKind,
    };
    let spec = spec_node
        .as_variant::<PartitionBoundSpec>()
        .expect("transformPartitionBound on non-PartitionBoundSpec");
    let key = partcache::RelationGetPartitionKey(parent)?;
    let strategy = key.strategy as u8;
    let mut result = Node::build::<PartitionBoundSpec>(mcx)?;
    result.strategy = strategy;
    result.location = spec.location;
    if spec.is_default {
        if strategy == b'h' {
            return Err(hash_default_partition());
        }
        result.is_default = true;
        return Ok(result.seal());
    }

    let colinfo = |i: usize| -> PgResult<(String, Oid, i32, Oid)> {
        let attno = key.partattrs[i];
        debug_assert!(attno != 0, "expression keys loud in partcache");
        // get_attname via the open parent's descriptor (the syscache seam is
        // not part of the server init set).
        let att = parent.rd_att.attr(attno as usize - 1);
        let colname = core::str::from_utf8(att.attname.name_str())
            .expect("non-UTF-8 attname")
            .to_string();
        Ok((colname, key.parttypid[i], key.parttypmod[i], key.partcollation[i]))
    };

    match strategy {
        b'l' => {
            if spec.strategy != b'l' {
                return Err(invalid_bound_spec("list"));
            }
            let (colname, coltype, coltypmod, partcollation) = colinfo(0)?;
            let mut listdatums = NodeList::nil();
            for cell in spec.listdatums.iter() {
                let value = transformPartitionBoundValue(
                    mcx,
                    pstate,
                    cell,
                    &colname,
                    coltype,
                    coltypmod,
                    partcollation,
                )?;
                let duplicate = listdatums
                    .iter()
                    .any(|v| types_nodes::equal::equal(v, value));
                if duplicate {
                    continue;
                }
                listdatums.lappend(mcx, value)?;
            }
            result.listdatums = listdatums;
        }
        b'r' => {
            if spec.strategy != b'r' {
                return Err(invalid_bound_spec("range"));
            }
            let partnatts = key.partnatts as usize;
            if spec.lowerdatums.len() != partnatts {
                return Err(bound_count_error("FROM"));
            }
            if spec.upperdatums.len() != partnatts {
                return Err(bound_count_error("TO"));
            }
            let mut lower_out = NodeList::nil();
            let mut upper_out = NodeList::nil();
            for (bounds, out) in [
                (&spec.lowerdatums, &mut lower_out),
                (&spec.upperdatums, &mut upper_out),
            ] {
                // transformPartitionRangeBounds + validateInfiniteBounds.
                let mut seen_kind: Option<PartitionRangeDatumKind> = None;
                for (i, cell) in bounds.iter().enumerate() {
                    let mut prd = Node::build::<PartitionRangeDatum>(mcx)?;
                    let mut kind = PartitionRangeDatumKind::Value;
                    let mut infinite = false;
                    if let Some(cref) = cell.as_column_ref() {
                        if cref.fields.len() == 1 {
                            if let Some(s) = cref.fields.nth(0).as_string() {
                                if s.sval == "minvalue" {
                                    kind = PartitionRangeDatumKind::Minvalue;
                                    infinite = true;
                                } else if s.sval == "maxvalue" {
                                    kind = PartitionRangeDatumKind::Maxvalue;
                                    infinite = true;
                                }
                            }
                        }
                    }
                    if infinite {
                        prd.kind = kind;
                    } else {
                        let (colname, coltype, coltypmod, partcollation) = colinfo(i)?;
                        let value = transformPartitionBoundValue(
                            mcx,
                            pstate,
                            cell,
                            &colname,
                            coltype,
                            coltypmod,
                            partcollation,
                        )?;
                        let c = value
                            .as_variant::<types_nodes::primnodes::Const>()
                            .expect("transformPartitionBoundValue returns Const");
                        if c.constisnull {
                            return Err(null_range_bound());
                        }
                        prd.value = Some(value);
                    }
                    prd.location = parse_expr::expr_location(cell);
                    // validateInfiniteBounds: once MINVALUE/MAXVALUE, the
                    // rest must repeat it.
                    if let Some(k) = seen_kind {
                        if k != kind {
                            return Err(infinite_bounds_error(k, prd.location));
                        }
                    } else if kind != PartitionRangeDatumKind::Value {
                        seen_kind = Some(kind);
                    }
                    out.lappend(mcx, prd.seal())?;
                }
            }
            result.lowerdatums = lower_out;
            result.upperdatums = upper_out;
        }
        b'h' => {
            if spec.strategy != b'h' {
                return Err(invalid_bound_spec("hash"));
            }
            if spec.modulus <= 0 {
                return Err(hash_bound_error(
                    "modulus for hash partition must be an integer value greater than zero",
                ));
            }
            debug_assert!(spec.remainder >= 0);
            if spec.remainder >= spec.modulus {
                return Err(hash_bound_error(
                    "remainder for hash partition must be less than modulus",
                ));
            }
            result.modulus = spec.modulus;
            result.remainder = spec.remainder;
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }
    Ok(result.seal())
}

// transformPartitionBoundValue (parse_utilcmd.c). The evaluate_expr arm runs
// eval_const_expressions (covers the Const-folding cases this lane meets);
// anything still non-Const is loud.
fn transformPartitionBoundValue<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut parser_small1::ParseState<'_, 'mcx>,
    val: Node<'mcx>,
    col_name: &str,
    col_type: Oid,
    col_typmod: i32,
    part_collation: Oid,
) -> PgResult<Node<'mcx>> {
    use parser_small1::ParseExprKind;
    let value = parse_expr::transformExpr(
        mcx,
        pstate,
        val,
        ParseExprKind::EXPR_KIND_PARTITION_BOUND,
    )?;
    let value = coerce::coerce_to_target_type(
        mcx,
        pstate,
        value,
        parse_expr::expr_type(value),
        col_type,
        col_typmod,
        coerce::CoercionContext::COERCION_ASSIGNMENT,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    let Some(mut value) = value else {
        return Err(cannot_cast_bound(mcx, col_type, col_name));
    };
    if value.as_variant::<types_nodes::primnodes::Const>().is_none() {
        value = clauses::eval_const_expressions(mcx, value)?;
    }
    let location = parse_expr::expr_location(val);
    match value.as_variant::<types_nodes::primnodes::Const>() {
        Some(_) => {
            // SAFETY: freshly transformed tree; no derived refs live.
            unsafe {
                value
                    .with_mut::<types_nodes::primnodes::Const, _>(|c| {
                        c.constcollid = part_collation;
                        c.location = location;
                    })
                    .expect("Const");
            }
            Ok(value)
        }
        None => unported("evaluate_expr for non-foldable partition bounds"),
    }
}

#[cold]
#[inline(never)]
fn invalid_bound_spec(kind: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("invalid bound specification for a {kind} partition"))
            .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn hash_default_partition() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            "a hash-partitioned table may not have a default partition".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn hash_bound_error(msg: &'static str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn bound_count_error(which: &'static str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("{which} must specify exactly one value per partitioning column"),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn null_range_bound() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot specify NULL in range bound".to_string())
            .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn infinite_bounds_error(
    kind: types_nodes::rawnodes::PartitionRangeDatumKind,
    _location: i32,
) -> Box<PgError> {
    let what = match kind {
        types_nodes::rawnodes::PartitionRangeDatumKind::Minvalue => "MINVALUE",
        _ => "MAXVALUE",
    };
    Box::new(
        PgError::new(
            ERROR,
            format!("every bound following {what} must also be {what}"),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[cold]
#[inline(never)]
fn cannot_cast_bound(mcx: Mcx<'_>, col_type: Oid, col_name: &str) -> Box<PgError> {
    let _ = mcx;
    let tn = format_type::format_type_be(col_type)
        .unwrap_or_else(|_| format!("type {col_type}"));
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "specified value cannot be cast to type {tn} for column \"{col_name}\""
            ),
        )
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
    Ok(core::str::from_utf8(buf.leak()).expect("was UTF-8"))
}

// CloneRowTriggersToPartition (tablecmds.c): reconstruct each of the parent's
// non-internal row triggers as a CreateTrigStmt against the partition, with
// tgparentid pointing at the parent trigger.
pub(crate) fn CloneRowTriggersToPartition<'mcx>(
    mcx: Mcx<'mcx>,
    parent: &Relation<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<()> {
    use types_trigger::{
        TRIGGER_TYPE_AFTER, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_EVENT_MASK, TRIGGER_TYPE_ROW,
        TRIGGER_TYPE_TIMING_MASK,
    };
    let Some(trigdesc) = relcache::RelationGetTriggerDesc(parent.rd_id)? else {
        return Ok(());
    };
    for trig in trigdesc.triggers.iter() {
        if trig.tgtype & TRIGGER_TYPE_ROW == 0 {
            continue;
        }
        if trig.tgisinternal {
            continue;
        }
        let timing = trig.tgtype & TRIGGER_TYPE_TIMING_MASK;
        if timing != TRIGGER_TYPE_BEFORE && timing != TRIGGER_TYPE_AFTER {
            panic!("unexpected trigger \"{}\" found", trig.tgname.as_str());
        }
        let qual = match &trig.tgqual {
            Some(q) => {
                let node = readfuncs::stringToNode(mcx, q.as_str())?;
                Some(trigger::map_partition_qual(mcx, node, partition, parent)?)
            }
            None => None,
        };
        let mut cols = NodeList::nil();
        for &attnum in trig.tgattr.iter() {
            let att = parent.rd_att.attr(attnum as usize - 1);
            let name = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
            cols.lappend(mcx, Node::mk_string(mcx, str_in(mcx, name)?)?)?;
        }
        let mut trigargs = NodeList::nil();
        for a in trig.tgargs.iter() {
            trigargs.lappend(mcx, Node::mk_string(mcx, str_in(mcx, a.as_str())?)?)?;
        }
        let stmt = types_nodes::rawnodes::CreateTrigStmt {
            replace: false,
            isconstraint: trig.tgconstraint != InvalidOid,
            trigname: Some(str_in(mcx, trig.tgname.as_str())?),
            relation: None,
            funcname: NodeList::nil(),
            args: trigargs,
            row: true,
            timing: trig.tgtype & TRIGGER_TYPE_TIMING_MASK,
            events: trig.tgtype & TRIGGER_TYPE_EVENT_MASK,
            columns: cols,
            whenClause: None,
            transitionRels: NodeList::nil(),
            deferrable: trig.tgdeferrable,
            initdeferred: trig.tginitdeferred,
            constrrel: None,
        };
        trigger::CreateTriggerFiringOn(
            mcx,
            &stmt,
            None,
            partition.rd_id,
            trig.tgconstrrelid,
            InvalidOid,
            InvalidOid,
            trig.tgfoid,
            trig.tgoid,
            qual,
            false,
            true,
            trig.tgenabled,
        )?;
    }
    Ok(())
}
