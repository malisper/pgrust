// tablecmds.c partition DDL slice: transformPartitionSpec /
// ComputePartitionAttrs / StoreCatalogInheritance / SetRelationHasSubclass.
// Named opclasses and ATTACH/DETACH are loud.
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
    pub partexprs: NodeList<'mcx>,
    pub partopclass: mcx::PgVec<'mcx, Oid>,
    pub partcollation: mcx::PgVec<'mcx, Oid>,
}

// transformPartitionSpec + ComputePartitionAttrs, fused (the transformExpr
// pass runs inline per elem; the input parse tree is never scribbled on).
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
        partexprs: NodeList::nil(),
        partopclass: mcx::vec_with_capacity_in(mcx, n)?,
        partcollation: mcx::vec_with_capacity_in(mcx, n)?,
    };

    let mut pstate = parser_small1::make_parsestate(mcx, None);
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        types_rel::AccessShareLock,
        None,
        false,
        true,
    )?;
    parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, true, true, true)?;

    for (attn, pnode) in partspec.partParams.iter().enumerate() {
        let pelem = pnode.as_variant::<PartitionElem>().expect("PartitionElem");
        let atttype: Oid;
        let mut attcollation: Oid;
        if let Some(name) = pelem.name {
            let mut attnum: AttrNumber = 0;
            atttype = {
                let mut ty: Oid = InvalidOid;
                attcollation = InvalidOid;
                for i in 0..rel.rd_att.natts as usize {
                    let att = rel.rd_att.attr(i);
                    if att.attname.name_str() == name.as_bytes() && !att.attisdropped {
                        if att.attgenerated != 0 {
                            return Err(generated_partition_column(
                                name,
                                query_string,
                                pelem.location,
                            ));
                        }
                        attnum = att.attnum;
                        ty = att.atttypid;
                        attcollation = att.attcollation;
                        break;
                    }
                }
                ty
            };
            // C SearchSysCacheAttName sees system columns too; the compact
            // descriptor scan above cannot (tablecmds.c:19811-19823).
            if attnum == 0 && catalog_heap::SystemAttributeByName(name).is_some() {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("cannot use system column \"{name}\" in partition key"),
                    )
                    .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .with_cursor_position(parser_small1::parser_errposition_source(
                        Some(query_string.as_bytes()),
                        pelem.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            if attnum == 0 {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("column \"{name}\" named in partition key does not exist"),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_COLUMN)
                    .with_cursor_position(parser_small1::parser_errposition_source(
                        Some(query_string.as_bytes()),
                        pelem.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            info.partattrs.push(attnum);
        } else {
            // transformPartitionSpec's transformExpr pass, fused here.
            let raw = pelem.expr.expect("PartitionElem without name or expr");
            let transformed = parse_expr::transformExpr(
                mcx,
                &mut pstate,
                raw,
                parser_small1::ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION,
            )?;
            parse_collate::assign_expr_collations(mcx, &mut pstate, transformed)?;
            atttype = nodes_core::expr_type(transformed);
            attcollation = nodes_core::expr_collation(transformed);
            let mut rowtypes: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
            catalog_heap::CheckAttributeType(
                mcx,
                &(attn + 1).to_string(),
                atttype,
                attcollation,
                &mut rowtypes,
                catalog_heap::CHKATYPE_IS_PARTKEY,
            )?;
            let mut expr = transformed;
            while let Some(ce) = expr.as_variant::<types_nodes::primnodes::CollateExpr>() {
                expr = ce.arg;
            }

            const FLIHAN: i32 = types_tuple::FirstLowInvalidHeapAttributeNumber;
            let mut expr_attrs = types_nodes::Bitmapset::empty();
            vars::pull_varattnos(mcx, expr, 1, &mut expr_attrs)?;
            if expr_attrs.is_member(-FLIHAN) {
                expr_attrs.add_range(mcx, 1 - FLIHAN, rel.rd_att.natts as i32 - FLIHAN)?;
                expr_attrs.del_member(-FLIHAN);
            }
            let mut b = -1;
            loop {
                b = expr_attrs.next_member(b);
                if b < 0 {
                    break;
                }
                let attno = b + FLIHAN;
                debug_assert!(attno != 0);
                if attno < 0 {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "partition key expressions cannot contain system column references"
                                .to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                    ));
                }
                let att = rel.rd_att.attr(attno as usize - 1);
                if att.attgenerated != 0 {
                    let colname = core::str::from_utf8(att.attname.name_str())
                        .expect("non-UTF-8 attname");
                    return Err(generated_partition_column(colname, query_string, pelem.location));
                }
            }

            let var_shortcut = expr.as_var().filter(|v| v.varattno > 0).map(|v| v.varattno);
            if let Some(varattno) = var_shortcut {
                info.partattrs.push(varattno);
            } else {
                info.partattrs.push(0);
                info.partexprs.lappend(mcx, expr)?;
                let planned = clauses::eval_const_expressions(mcx, expr)?;
                if clauses::contain_mutable_functions(planned)? {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "functions in partition key expression must be marked IMMUTABLE"
                                .to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                    ));
                }
                if planned.as_variant::<types_nodes::primnodes::Const>().is_some() {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "cannot use constant expression as partition key".to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                    ));
                }
            }
        }

        if !pelem.collation.is_nil() {
            attcollation = catalog_namespace::get_collation_oid_list(&pelem.collation, false)?;
        }
        if lsyscache::type_is_collatable(atttype)? {
            if attcollation == InvalidOid {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "could not determine which collation to use for partition expression"
                            .to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_INDETERMINATE_COLLATION)
                    .with_hint("Use the COLLATE clause to set the collation explicitly."),
                ));
            }
        } else if attcollation != InvalidOid {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "collations are not supported by type {}",
                        format_type::format_type_be(atttype)?
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
            ));
        }
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
            indexcmds_seams::resolve_opclass::call(&pelem.opclass, atttype, am_name, am_oid)?
        };
        info.partopclass.push(opclass);
    }
    parser_small1::free_parsestate(pstate)?;
    Ok(info)
}

#[track_caller]
#[cold]
#[inline(never)]
fn generated_partition_column(colname: &str, query_string: &str, location: i32) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot use generated column in partition key".to_string())
            .with_detail(format!("Column \"{colname}\" is a generated column."))
            .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
            .with_cursor_position(parser_small1::parser_errposition_source(
                Some(query_string.as_bytes()),
                location,
                mbutils::GetDatabaseEncoding(),
            )),
    )
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
        let colname = if attno != 0 {
            // get_attname via the open parent's descriptor (the syscache seam
            // is not part of the server init set).
            let att = parent.rd_att.attr(attno as usize - 1);
            core::str::from_utf8(att.attname.name_str())
                .expect("non-UTF-8 attname")
                .to_string()
        } else {
            let exprno = key.partattrs[..i].iter().filter(|&&a| a == 0).count();
            let expr = key
                .partexprs
                .iter()
                .nth(exprno)
                .expect("wrong number of partition key expressions");
            ruleutils_seams::deparse_expression::call(mcx, expr, parent.rd_id)?
        };
        Ok((colname, key.parttypid[i], key.parttypmod[i], key.partcollation[i]))
    };

    match strategy {
        b'l' => {
            if spec.strategy != b'l' {
                return Err(invalid_bound_spec("list", pstate, spec.location));
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
                return Err(invalid_bound_spec("range", pstate, spec.location));
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
                            return Err(infinite_bounds_error(pstate, k, prd.location));
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
                return Err(invalid_bound_spec("hash", pstate, spec.location));
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

// transformPartitionBoundValue (parse_utilcmd.c).
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
        return Err(cannot_cast_bound(mcx, pstate, col_type, col_name, parse_expr::expr_location(val)));
    };
    if value.as_variant::<types_nodes::primnodes::Const>().is_none() {
        parse_collate::assign_expr_collations(mcx, pstate, value)?;
        value = clauses::eval_const_expressions(mcx, value)?;
        if value.as_variant::<types_nodes::primnodes::Const>().is_none() {
            value = execexpr::evaluate_expr(mcx, value, col_type, col_typmod, part_collation)?;
        }
        assert!(
            value.as_variant::<types_nodes::primnodes::Const>().is_some(),
            "could not evaluate partition bound expression"
        );
    } else {
        // coerce_to_target_type doesn't insert the partition collation.
        // SAFETY: freshly transformed tree; no derived refs live.
        unsafe {
            value
                .with_mut::<types_nodes::primnodes::Const, _>(|c| {
                    c.constcollid = part_collation;
                })
                .expect("Const");
        }
    }
    let location = parse_expr::expr_location(val);
    // SAFETY: freshly transformed tree; no derived refs live.
    unsafe {
        value
            .with_mut::<types_nodes::primnodes::Const, _>(|c| {
                c.location = location;
            })
            .expect("Const");
    }
    Ok(value)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_bound_spec(
    kind: &str,
    pstate: &parser_small1::ParseState<'_, '_>,
    location: i32,
) -> Box<PgError> {
    let mut e = PgError::new(ERROR, format!("invalid bound specification for a {kind} partition"))
        .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION);
    let pos = parser_small1::parser_errposition_source(
        pstate.p_sourcetext,
        location,
        mbutils::GetDatabaseEncoding(),
    );
    if pos > 0 {
        e = e.with_cursor_position(pos);
    }
    Box::new(e)
}

#[track_caller]
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

#[track_caller]
#[cold]
#[inline(never)]
fn hash_bound_error(msg: &'static str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[track_caller]
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

#[track_caller]
#[cold]
#[inline(never)]
fn null_range_bound() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot specify NULL in range bound".to_string())
            .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn infinite_bounds_error(
    pstate: &parser_small1::ParseState<'_, '_>,
    kind: types_nodes::rawnodes::PartitionRangeDatumKind,
    location: i32,
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
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
        .with_cursor_position(parser_small1::parser_errposition(
            pstate,
            location,
            mbutils::GetDatabaseEncoding(),
        )),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_cast_bound(
    mcx: Mcx<'_>,
    pstate: &parser_small1::ParseState<'_, '_>,
    col_type: Oid,
    col_name: &str,
    location: i32,
) -> Box<PgError> {
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
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
        // C parse_utilcmd.c:4623: parser_errposition(pstate, exprLocation(val)).
        .with_cursor_position(parser_small1::parser_errposition(
            pstate,
            location,
            mbutils::GetDatabaseEncoding(),
        )),
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

// has_partition_attrs (catalog/partition.c): attnums is offset by
// FirstLowInvalidHeapAttributeNumber, as pull_varattnos emits.
pub(crate) fn has_partition_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnums: &types_nodes::Bitmapset<'mcx>,
    used_in_expr: &mut bool,
) -> PgResult<bool> {
    if attnums.is_empty() || rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE {
        return Ok(false);
    }
    let key = partcache::RelationGetPartitionKey(rel)?;
    let mut partexprs_it = key.partexprs.iter();
    for i in 0..key.partnatts as usize {
        let partattno = key.partattrs[i];
        if partattno != 0 {
            if attnums
                .is_member(partattno as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber)
            {
                *used_in_expr = false;
                return Ok(true);
            }
        } else {
            let expr = partexprs_it.next().expect("partition key expression");
            let mut expr_attrs = types_nodes::Bitmapset::empty();
            vars::pull_varattnos(mcx, expr, 1, &mut expr_attrs)?;
            if attnums.overlap(&expr_attrs) {
                *used_in_expr = true;
                return Ok(true);
            }
        }
    }
    Ok(false)
}
