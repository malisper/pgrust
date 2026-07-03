// tablecmds.c traditional-inheritance slice: inheritOids lookup +
// MergeAttributes (columns, NOT NULL, CHECK) + StoreCatalogInheritance.
// Inherited defaults, generated, identity, compression and typed tables are
// loud; partitions take the empty-column arm in lib.rs.
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERROR, NOTICE};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, TypeName};
use types_nodes::{Node, NodeList};
use types_rel::{
    NoLock, Relation, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use crate::unported;

const MaxHeapAttributeNumber: usize = 1600;

pub(crate) struct InheritedNotNull<'mcx> {
    pub name: &'mcx str,
    pub attnum: AttrNumber,
}

pub(crate) struct InheritedCheck<'mcx> {
    pub name: &'mcx str,
    pub expr: Node<'mcx>,
    pub inhcount: i16,
    pub is_enforced: bool,
    pub skip_validation: bool,
}

pub(crate) struct MergedAttributes<'mcx> {
    pub columns: NodeList<'mcx>,
    pub checks: PgVec<'mcx, InheritedCheck<'mcx>>,
    pub notnulls: PgVec<'mcx, InheritedNotNull<'mcx>>,
}

// DefineRelation's inhRelations loop (tablecmds.c:99-116).
pub(crate) fn lookup_inherit_oids<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateStmt<'mcx>,
    parent_lockmode: types_rel::LOCKMODE,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut inherit_oids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for cell in stmt.inhRelations.iter() {
        let prv = cell
            .as_variant::<types_nodes::RangeVar>()
            .expect("inhRelations RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: prv.catalogname,
            schemaname: prv.schemaname,
            relname: prv.relname.expect("RangeVar.relname"),
            inh: prv.inh,
            relpersistence: prv.relpersistence,
            location: prv.location,
        };
        let parent_oid = catalog_namespace::RangeVarGetRelid(&rv, parent_lockmode, false)?;
        if inherit_oids.contains(&parent_oid) {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "relation \"{}\" would be inherited from more than once",
                        rv.relname
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_TABLE),
            ));
        }
        inherit_oids.push(parent_oid);
    }
    Ok(inherit_oids)
}

// MergeAttributes (tablecmds.c:2546), regular-inheritance leg. The partition
// leg stays in lib.rs (descriptor copy). Typed-table merging is dead (OF type
// loud upstream).
pub(crate) fn MergeAttributes<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &NodeList<'mcx>,
    supers: &[Oid],
    relpersistence: i8,
) -> PgResult<MergedAttributes<'mcx>> {
    if columns.len() > MaxHeapAttributeNumber {
        return Err(too_many_columns());
    }
    let mut local_defs: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for (i, elt) in columns.iter().enumerate() {
        let coldef = elt.as_variant::<ColumnDef>().expect("ColumnDef");
        let colname = coldef.colname.expect("ColumnDef.colname");
        for rest in columns.iter().skip(i + 1) {
            let restdef = rest.as_variant::<ColumnDef>().expect("ColumnDef");
            if restdef.colname == Some(colname) {
                debug_assert!(!coldef.is_from_type, "typed tables loud upstream");
                return Err(duplicate_column(colname));
            }
        }
        local_defs.push(elt);
    }

    // inh_defs entries are freshly built (never aliased into the parse tree),
    // so in-place merge edits go through plain owned structs.
    let mut inh_defs: PgVec<'mcx, ColumnDef<'mcx>> = PgVec::new_in(mcx);
    let mut checks: PgVec<'mcx, InheritedCheck<'mcx>> = PgVec::new_in(mcx);
    let mut notnulls: PgVec<'mcx, InheritedNotNull<'mcx>> = PgVec::new_in(mcx);
    let mut child_attno: usize = 0;

    for &parent in supers {
        let relation = table::table_open(mcx, parent, NoLock)?;
        let relkind = relation.rd_rel.relkind;
        let relname = relation.name().to_string();
        if relkind == RELKIND_PARTITIONED_TABLE {
            return Err(wrong_parent(format!(
                "cannot inherit from partitioned table \"{relname}\""
            )));
        }
        if relation.rd_rel.relispartition {
            return Err(wrong_parent(format!(
                "cannot inherit from partition \"{relname}\""
            )));
        }
        if relkind != RELKIND_RELATION && relkind != RELKIND_FOREIGN_TABLE {
            return Err(wrong_parent(format!(
                "inherited relation \"{relname}\" is not a table or foreign table"
            )));
        }
        if relpersistence != types_core::RELPERSISTENCE_TEMP
            && relation.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
        {
            return Err(wrong_parent(format!(
                "cannot inherit from temporary relation \"{relname}\""
            )));
        }
        if relation.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
            && !relation.rd_islocaltemp
        {
            return Err(wrong_parent(
                "cannot inherit from temporary relation of another session".to_string(),
            ));
        }
        // object_ownercheck: superuser fastpath (role-ACL walk loud; the
        // RangeVarCallbackOwnsRelation precedent).
        if !superuser::superuser_arg(miscinit::GetUserId())? {
            unported("MergeAttributes object_ownercheck for non-superusers");
        }

        let tupdesc = relation.descr();
        // newattmap: parent attno (1-based) -> child attno (1-based), 0 for
        // dropped parent columns.
        let mut newattmap: PgVec<'mcx, i16> = mcx::vec_from_elem_in(mcx, 0i16, tupdesc.natts as usize);

        let nnconstrs = pg_constraint::RelationGetNotNullConstraints(mcx, &relation, false)?;
        let mut nncols: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);
        let mut nnnames: PgVec<'mcx, &'mcx str> = PgVec::new_in(mcx);
        for cnode in nnconstrs.iter() {
            let c = cnode
                .as_variant::<types_nodes::rawnodes::Constraint>()
                .expect("Constraint");
            let colname = c.keys.nth(0).as_string().expect("nn keys").sval;
            let attnum = (0..tupdesc.natts as usize)
                .find(|&i| tupdesc.attr(i).attname.name_str() == colname.as_bytes())
                .map(|i| (i + 1) as AttrNumber)
                .unwrap_or_else(|| panic!("not-null column {colname:?} not found"));
            nncols.push(attnum);
            nnnames.push(c.conname.expect("catalogued nn constraint has a name"));
        }

        for parent_attno in 1..=tupdesc.natts as usize {
            let attribute = tupdesc.attr(parent_attno - 1);
            if attribute.attisdropped {
                continue;
            }
            let att_name: &'mcx str = str_in(
                mcx,
                core::str::from_utf8(attribute.attname.name_str()).expect("attname UTF-8"),
            )?;
            if attribute.atthasdef {
                unported("inherited column defaults (TupleDescGetDefault lane)");
            }
            if attribute.attgenerated != 0 || attribute.attidentity != 0 {
                unported("inherited generated/identity columns");
            }
            let mut newdef = make_column_def(
                mcx,
                att_name,
                attribute.atttypid,
                attribute.atttypmod,
                attribute.attcollation,
            )?;
            newdef.storage = attribute.attstorage as u8;

            let exist = inh_defs
                .iter()
                .position(|d| d.colname == Some(att_name));
            let merged_idx = match exist {
                Some(idx) => {
                    merge_inherited_attribute(mcx, &mut inh_defs[idx], &newdef)?;
                    newattmap[parent_attno - 1] = (idx + 1) as i16;
                    idx
                }
                None => {
                    newdef.inhcount = 1;
                    newdef.is_local = false;
                    inh_defs.push(newdef);
                    child_attno += 1;
                    newattmap[parent_attno - 1] = child_attno as i16;
                    inh_defs.len() - 1
                }
            };
            if nncols.contains(&(parent_attno as AttrNumber)) {
                inh_defs[merged_idx].is_not_null = true;
            }
        }

        if let Some(constr) = relation.rd_att.constr.as_deref() {
            for check in constr.check.iter() {
                if check.ccnoinherit {
                    continue;
                }
                let name_owned = check.ccname.as_ref().expect("check name").as_str();
                let name: &'mcx str = str_in(mcx, name_owned)?;
                let raw = readfuncs::stringToNode(
                    mcx,
                    check.ccbin.as_ref().expect("check ccbin").as_str(),
                )?;
                let (expr, found_whole_row) =
                    rewrite_manip::map_variable_attnos(mcx, raw, 1, 0, &newattmap)?;
                if found_whole_row {
                    return Err(Box::new(
                        PgError::new(ERROR, "cannot convert whole-row table reference".to_string())
                            .with_detail(format!(
                                "Constraint \"{name}\" contains a whole-row reference to table \"{relname}\"."
                            ))
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                merge_check_constraint(&mut checks, name, expr, check.ccenforced)?;
            }
        }

        for (i, &attnum) in nncols.iter().enumerate() {
            notnulls.push(InheritedNotNull {
                name: nnnames[i],
                attnum: newattmap[attnum as usize - 1],
            });
        }

        relation.close(NoLock)?;
    }

    let mut merged = NodeList::nil();
    if !inh_defs.is_empty() {
        let mut newcol_attno = 0usize;
        for elt in local_defs.iter() {
            let newdef = elt.as_variant::<ColumnDef>().expect("ColumnDef");
            let att_name = newdef.colname.expect("ColumnDef.colname");
            newcol_attno += 1;
            match inh_defs.iter().position(|d| d.colname == Some(att_name)) {
                Some(idx) => {
                    merge_child_attribute(mcx, &mut inh_defs, idx, newcol_attno, newdef)?
                }
                None => {
                    // Local columns append after all inherited ones; keep the
                    // parse-tree node so raw defaults survive untouched.
                    inh_defs.push(clone_column_def(newdef));
                }
            }
        }
        if inh_defs.len() > MaxHeapAttributeNumber {
            return Err(too_many_columns());
        }
        for def in inh_defs.drain(..) {
            merged.lappend(mcx, Node::mk(mcx, def)?)?;
        }
    } else {
        for elt in local_defs.iter() {
            merged.lappend(mcx, *elt)?;
        }
    }

    Ok(MergedAttributes { columns: merged, checks, notnulls })
}

// makeColumnDef (makefuncs.c): direct-OID TypeName.
fn make_column_def<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &'mcx str,
    typid: Oid,
    typmod: i32,
    collid: Oid,
) -> PgResult<ColumnDef<'mcx>> {
    let tn = TypeName {
        typeOid: typid,
        typemod: typmod,
        location: -1,
        ..TypeName::default()
    };
    Ok(ColumnDef {
        colname: Some(colname),
        typeName: Some(Node::mk(mcx, tn)?),
        is_local: true,
        collOid: collid,
        location: -1,
        ..ColumnDef::default()
    })
}

fn clone_column_def<'mcx>(d: &ColumnDef<'mcx>) -> ColumnDef<'mcx> {
    ColumnDef {
        colname: d.colname,
        typeName: d.typeName,
        compression: d.compression,
        inhcount: d.inhcount,
        is_local: d.is_local,
        is_not_null: d.is_not_null,
        is_from_type: d.is_from_type,
        storage: d.storage,
        storage_name: d.storage_name,
        raw_default: d.raw_default,
        cooked_default: d.cooked_default,
        identity: d.identity,
        identitySequence: d.identitySequence,
        generated: d.generated,
        collClause: d.collClause,
        collOid: d.collOid,
        constraints: d.constraints,
        fdwoptions: d.fdwoptions,
        location: d.location,
    }
}

fn coldef_type(def: &ColumnDef<'_>) -> (Oid, i32) {
    let tn = def
        .typeName
        .expect("ColumnDef.typeName")
        .as_variant::<TypeName>()
        .expect("TypeName");
    if tn.typeOid != InvalidOid {
        (tn.typeOid, tn.typemod)
    } else {
        parse_utilcmd::typenameTypeIdAndMod(mcx_dummy(), None, tn)
            .expect("typenameTypeIdAndMod on transformed column type")
    }
}

// typenameTypeIdAndMod needs an mcx only for typmod cstring scratch; local
// columns reaching the merge path were already validated by transformCreateStmt.
fn mcx_dummy() -> Mcx<'static> {
    thread_local! {
        static CTX: &'static mcx::MemoryContext =
            Box::leak(Box::new(mcx::MemoryContext::new("coldef-type-scratch")));
    }
    CTX.with(|c| c.mcx())
}

// GetColumnDefCollation (parse_type.c): collClause loud upstream.
fn coldef_collation(def: &ColumnDef<'_>, typeoid: Oid) -> PgResult<Oid> {
    debug_assert!(def.collClause.is_none(), "COLLATE loud upstream");
    if def.collOid != InvalidOid {
        return Ok(def.collOid);
    }
    Ok(syscache_seams::lookup_pg_type_shape::call(typeoid)?
        .expect("pg_type row vanished")
        .typcollation)
}

// MergeInheritedAttribute (tablecmds.c:3418).
fn merge_inherited_attribute<'mcx>(
    _mcx: Mcx<'mcx>,
    prevdef: &mut ColumnDef<'mcx>,
    newdef: &ColumnDef<'mcx>,
) -> PgResult<()> {
    let attname = newdef.colname.expect("colname");
    notice(format!(
        "merging multiple inherited definitions of column \"{attname}\""
    ))?;
    let (prevtypeid, prevtypmod) = coldef_type(prevdef);
    let (newtypeid, newtypmod) = coldef_type(newdef);
    if prevtypeid != newtypeid || prevtypmod != newtypmod {
        return Err(column_conflict(
            "inherited column \"{}\" has a type conflict",
            attname,
            format!(
                "{} versus {}",
                format_type::format_type_with_typemod(prevtypeid, prevtypmod)?,
                format_type::format_type_with_typemod(newtypeid, newtypmod)?
            ),
            types_error::ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    let prevcollid = coldef_collation(prevdef, prevtypeid)?;
    let newcollid = coldef_collation(newdef, newtypeid)?;
    if prevcollid != newcollid {
        return Err(collation_conflict(attname, prevcollid, newcollid, true)?);
    }
    if prevdef.storage == 0 {
        prevdef.storage = newdef.storage;
    } else if prevdef.storage != newdef.storage {
        unported("inherited storage parameter conflicts (storage_name deparse)");
    }
    debug_assert!(prevdef.compression.is_none() && newdef.compression.is_none());
    debug_assert!(prevdef.generated == 0 && newdef.generated == 0);
    if prevdef.inhcount == i16::MAX {
        return Err(too_many_parents());
    }
    prevdef.inhcount += 1;
    Ok(())
}

// MergeChildAttribute (tablecmds.c:3311).
fn merge_child_attribute<'mcx>(
    _mcx: Mcx<'mcx>,
    inh_defs: &mut PgVec<'mcx, ColumnDef<'mcx>>,
    exist_idx: usize,
    newcol_attno: usize,
    newdef: &ColumnDef<'mcx>,
) -> PgResult<()> {
    let attname = newdef.colname.expect("colname");
    if exist_idx + 1 == newcol_attno {
        notice(format!(
            "merging column \"{attname}\" with inherited definition"
        ))?;
    } else {
        notice_with_detail(
            format!("moving and merging column \"{attname}\" with inherited definition"),
            "User-specified column moved to the position of the inherited column.".to_string(),
        )?;
    }
    let inhdef = &mut inh_defs[exist_idx];
    let (inhtypeid, inhtypmod) = coldef_type(inhdef);
    let (newtypeid, newtypmod) = coldef_type(newdef);
    if inhtypeid != newtypeid || inhtypmod != newtypmod {
        return Err(column_conflict(
            "column \"{}\" has a type conflict",
            attname,
            format!(
                "{} versus {}",
                format_type::format_type_with_typemod(inhtypeid, inhtypmod)?,
                format_type::format_type_with_typemod(newtypeid, newtypmod)?
            ),
            types_error::ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    let inhcollid = coldef_collation(inhdef, inhtypeid)?;
    let newcollid = coldef_collation(newdef, newtypeid)?;
    if inhcollid != newcollid {
        return Err(collation_conflict(attname, inhcollid, newcollid, false)?);
    }
    debug_assert!(newdef.identity == 0, "identity loud upstream");
    if inhdef.storage == 0 {
        inhdef.storage = newdef.storage;
    } else if newdef.storage != 0 && inhdef.storage != newdef.storage {
        unported("inherited storage parameter conflicts (storage_name deparse)");
    }
    debug_assert!(inhdef.compression.is_none());
    inhdef.compression = newdef.compression;
    inhdef.is_not_null |= newdef.is_not_null;
    debug_assert!(inhdef.generated == 0 && newdef.generated == 0, "generated loud upstream");
    if newdef.raw_default.is_some() {
        inhdef.raw_default = newdef.raw_default;
        inhdef.cooked_default = newdef.cooked_default;
    }
    inhdef.is_local = true;
    Ok(())
}

// MergeCheckConstraint (tablecmds.c:3155).
fn merge_check_constraint<'mcx>(
    checks: &mut PgVec<'mcx, InheritedCheck<'mcx>>,
    name: &'mcx str,
    expr: Node<'mcx>,
    is_enforced: bool,
) -> PgResult<()> {
    for ccon in checks.iter_mut() {
        if ccon.name != name {
            continue;
        }
        if types_nodes::equal::equal(ccon.expr, expr) {
            if ccon.inhcount == i16::MAX {
                return Err(too_many_parents());
            }
            ccon.inhcount += 1;
            if !ccon.is_enforced && is_enforced {
                ccon.is_enforced = true;
                ccon.skip_validation = false;
            }
            return Ok(());
        }
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "check constraint name \"{name}\" appears multiple times but with different expressions"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    checks.push(InheritedCheck {
        name,
        expr,
        inhcount: 1,
        is_enforced,
        skip_validation: !is_enforced,
    });
    Ok(())
}

// StoreCatalogInheritance + StoreCatalogInheritance1 (tablecmds.c:3510);
// generalizes partition.rs's single-parent arm.
pub(crate) fn StoreCatalogInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    supers: &[Oid],
    child_is_partition: bool,
) -> PgResult<()> {
    for (i, &parent_oid) in supers.iter().enumerate() {
        pg_inherits::StoreSingleInheritance(mcx, relation_id, parent_oid, (i + 1) as i32)?;
        let childobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, relation_id);
        let parentobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, parent_oid);
        pg_depend::recordDependencyOn(
            mcx,
            &childobject,
            &parentobject,
            if child_is_partition {
                pg_depend::DependencyType::Auto
            } else {
                pg_depend::DependencyType::Normal
            },
        )?;
        crate::partition::SetRelationHasSubclass(mcx, parent_oid, true)?;
    }
    Ok(())
}

// StoreConstraints (heap.c), inherited-CHECK arm: cooked checks land with
// conislocal=false and the merged inhcount, then relchecks is refreshed.
pub(crate) fn store_inherited_checks<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    checks: &[InheritedCheck<'mcx>],
) -> PgResult<()> {
    if checks.is_empty() {
        return Ok(());
    }
    // Need the post-create pg_class/pg_attribute rows visible.
    xact::CommandCounterIncrement()?;
    let rel = table::table_open(mcx, relation_id, NoLock)?;
    for check in checks {
        let ccbin = outfuncs::nodeToString(mcx, check.expr)?;
        let var_list = vars::pull_var_clause(mcx, check.expr, 0)?;
        let mut att_nos: PgVec<'mcx, i16> = PgVec::new_in(mcx);
        for v in var_list.iter() {
            let attno = v.as_var().expect("pull_var_clause").varattno;
            if !att_nos.contains(&attno) {
                att_nos.push(attno);
            }
        }
        let mut entry = pg_constraint::ConstraintEntry::base(
            check.name,
            rel.rd_rel.relnamespace,
            pg_constraint::CONSTRAINT_CHECK,
            relation_id,
        );
        entry.conkey = &att_nos;
        entry.n_keys = att_nos.len();
        entry.is_enforced = check.is_enforced;
        entry.is_validated = !check.skip_validation;
        entry.conbin = Some(ccbin.as_str());
        entry.con_expr = Some(check.expr);
        entry.is_local = false;
        entry.inhcount = check.inhcount;
        pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    }
    crate::constraints::set_relation_num_checks(mcx, &rel, checks.len() as i16)?;
    rel.close(NoLock)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[cold]
#[inline(never)]
fn notice(msg: String) -> PgResult<()> {
    elog_seams::ereport::call(PgError::new(NOTICE, msg))
}

#[cold]
#[inline(never)]
fn notice_with_detail(msg: String, detail: String) -> PgResult<()> {
    elog_seams::ereport::call(PgError::new(NOTICE, msg).with_detail(detail))
}

#[cold]
#[inline(never)]
fn too_many_columns() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("tables can have at most {MaxHeapAttributeNumber} columns"),
        )
        .with_sqlstate(types_error::ERRCODE_TOO_MANY_COLUMNS),
    )
}

#[cold]
#[inline(never)]
fn duplicate_column(colname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("column \"{colname}\" specified more than once"))
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_COLUMN),
    )
}

#[cold]
#[inline(never)]
fn wrong_parent(msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
}

#[cold]
#[inline(never)]
fn too_many_parents() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "too many inheritance parents".to_string())
            .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[cold]
#[inline(never)]
fn column_conflict(
    template: &str,
    attname: &str,
    detail: String,
    sqlstate: types_error::SqlState,
) -> Box<PgError> {
    let msg = template.replacen("{}", attname, 1);
    Box::new(PgError::new(ERROR, msg).with_detail(detail).with_sqlstate(sqlstate))
}

#[cold]
#[inline(never)]
fn collation_conflict(
    attname: &str,
    prevcollid: Oid,
    newcollid: Oid,
    inherited: bool,
) -> PgResult<Box<PgError>> {
    let dummy = mcx_dummy();
    let prevname = lsyscache::get_collation_name(dummy, prevcollid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let newname = lsyscache::get_collation_name(dummy, newcollid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let msg = if inherited {
        format!("inherited column \"{attname}\" has a collation conflict")
    } else {
        format!("column \"{attname}\" has a collation conflict")
    };
    Ok(Box::new(
        PgError::new(ERROR, msg)
            .with_detail(format!("\"{prevname}\" versus \"{newname}\""))
            .with_sqlstate(types_error::ERRCODE_COLLATION_MISMATCH),
    ))
}
