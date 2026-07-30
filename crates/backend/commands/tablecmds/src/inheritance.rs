// tablecmds.c traditional-inheritance slice: inheritOids lookup +
// MergeAttributes (columns, defaults, NOT NULL, CHECK, generated,
// compression) + StoreCatalogInheritance. Partitions take the empty-column
// arm in lib.rs.
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERROR, NOTICE};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, TypeName};
use types_nodes::{Node, NodeList};
use types_rel::{
    NoLock, Relation, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};


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
    pub gendefs: PgVec<'mcx, (AttrNumber, Node<'mcx>)>,
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

// tablecmds.c:2589 duplicate-name scan, no-parents leg: typed-table column
// options (typeName == NULL) merge onto the is_from_type column the type
// contributed; leftover options and true duplicates are errors.
pub(crate) fn merge_column_options<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    if columns.len() > MaxHeapAttributeNumber {
        return Err(too_many_columns());
    }
    let mut elts: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    let mut removed: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    for n in columns.iter() {
        elts.push(n);
        removed.push(false);
    }
    for i in 0..elts.len() {
        if removed[i] {
            continue;
        }
        let (colname, has_typename) = {
            let cd = elts[i].as_variant::<ColumnDef>().expect("ColumnDef");
            (cd.colname.expect("ColumnDef.colname"), cd.typeName.is_some())
        };
        if !has_typename {
            return Err(Box::new(
                PgError::new(ERROR, format!("column \"{colname}\" does not exist"))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        for j in (i + 1)..elts.len() {
            if removed[j] {
                continue;
            }
            let matches = {
                let rd = elts[j].as_variant::<ColumnDef>().expect("ColumnDef");
                rd.colname == Some(colname)
            };
            if !matches {
                continue;
            }
            let is_from_type =
                elts[i].as_variant::<ColumnDef>().expect("ColumnDef").is_from_type;
            if !is_from_type {
                return Err(duplicate_column(colname));
            }
            // SAFETY (both blocks): parse tree is statement-owned; no derived
            // refs live across the edits.
            let (is_not_null, raw_default, cooked_default, constraints) = unsafe {
                elts[j]
                    .with_mut::<ColumnDef, _>(|c| {
                        (
                            c.is_not_null,
                            c.raw_default,
                            c.cooked_default,
                            core::mem::take(&mut c.constraints),
                        )
                    })
                    .expect("ColumnDef")
            };
            unsafe {
                elts[i]
                    .with_mut::<ColumnDef, _>(|c| {
                        c.is_not_null = is_not_null;
                        c.raw_default = raw_default;
                        c.cooked_default = cooked_default;
                        c.constraints = constraints;
                        c.is_from_type = false;
                    })
                    .expect("ColumnDef");
            }
            removed[j] = true;
        }
    }
    let mut out = NodeList::nil();
    for (k, n) in elts.iter().enumerate() {
        if !removed[k] {
            out.lappend(mcx, *n)?;
        }
    }
    Ok(out)
}

// MergeAttributes duplicate-name scan, partition leg: the dummy ColumnDefs
// carry no typeName (grammar enforced), so only true duplicates error.
pub(crate) fn partition_column_dup_scan(table_elts: &NodeList<'_>) -> PgResult<()> {
    if table_elts.len() > MaxHeapAttributeNumber {
        return Err(too_many_columns());
    }
    for (i, elt) in table_elts.iter().enumerate() {
        let colname =
            elt.as_variant::<ColumnDef>().expect("ColumnDef").colname.expect("ColumnDef.colname");
        for rest in table_elts.iter().skip(i + 1) {
            let restdef = rest.as_variant::<ColumnDef>().expect("ColumnDef");
            if restdef.colname == Some(colname) {
                return Err(duplicate_column(colname));
            }
        }
    }
    Ok(())
}

// MergeAttributes (tablecmds.c:2546), regular-inheritance leg. The partition
// leg stays in lib.rs (descriptor copy). Typed-table merging never reaches
// here: the grammar forbids OF plus INHERITS (parse_utilcmd.c:255).
pub(crate) fn MergeAttributes<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &NodeList<'mcx>,
    supers: &[Oid],
    relpersistence: u8,
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
                // Grammar forbids OF + INHERITS, so is_from_type merging
                // cannot arise in this leg.
                debug_assert!(!coldef.is_from_type);
                return Err(duplicate_column(colname));
            }
        }
        local_defs.push(elt);
    }

    // inh_defs entries are freshly built (never aliased into the parse tree),
    // so in-place merge edits go through plain owned structs.
    let mut inh_defs: PgVec<'mcx, ColumnDef<'mcx>> = PgVec::new_in(mcx);
    // bogus[i]: inh_defs[i] inherited unequal defaults from multiple parents
    // (C's bogus_marker); fatal below unless a local default overrides.
    let mut bogus: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    let mut have_bogus_defaults = false;
    let mut checks: PgVec<'mcx, InheritedCheck<'mcx>> = PgVec::new_in(mcx);
    let mut notnulls: PgVec<'mcx, InheritedNotNull<'mcx>> = PgVec::new_in(mcx);
    let mut gendefs: PgVec<'mcx, (AttrNumber, Node<'mcx>)> = PgVec::new_in(mcx);
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
        if !aclchk::object_ownercheck(
            types_core::RELATION_RELATION_ID,
            parent,
            miscinit::GetUserId(),
        )? {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                crate::get_relkind_objtype(relkind),
                &relname,
            )?;
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

        // Generation expressions can't be attno-mapped until newattmap is
        // complete; remember (inh_defs index, parent attno) for the pass below.
        let mut inherited_defaults: PgVec<'mcx, (usize, AttrNumber)> = PgVec::new_in(mcx);
        for parent_attno in 1..=tupdesc.natts as usize {
            let attribute = tupdesc.attr(parent_attno - 1);
            if attribute.attisdropped {
                continue;
            }
            let att_name: &'mcx str = str_in(
                mcx,
                core::str::from_utf8(attribute.attname.name_str()).expect("attname UTF-8"),
            )?;
            let mut newdef = make_column_def(
                mcx,
                att_name,
                attribute.atttypid,
                attribute.atttypmod,
                attribute.attcollation,
            )?;
            newdef.storage = attribute.attstorage as u8;
            newdef.generated = attribute.attgenerated as u8;
            if attribute.attcompression != 0 {
                newdef.compression = Some(compression_method_name(attribute.attcompression as u8));
            }
            // Regular inheritance children do not inherit identity; only
            // partitions do (they take the lib.rs descriptor-copy leg).

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
                    bogus.push(false);
                    child_attno += 1;
                    newattmap[parent_attno - 1] = child_attno as i16;
                    inh_defs.len() - 1
                }
            };
            if nncols.contains(&(parent_attno as AttrNumber)) {
                inh_defs[merged_idx].is_not_null = true;
            }
            if attribute.atthasdef {
                inherited_defaults.push((merged_idx, parent_attno as AttrNumber));
            }
        }

        for &(idx, parent_attno) in inherited_defaults.iter() {
            let adbin = pg_attrdef::GetAttrDefaultBin(mcx, parent, parent_attno)?
                .unwrap_or_else(|| {
                    panic!(
                        "default expression not found for attribute {parent_attno} of relation \"{relname}\""
                    )
                });
            let raw = readfuncs::stringToNode(mcx, &adbin)?;
            let (expr, found_whole_row) =
                rewrite_manip::map_variable_attnos(mcx, raw, 1, 0, &newattmap, types_core::InvalidOid)?;
            let def = &mut inh_defs[idx];
            if found_whole_row {
                return Err(Box::new(
                    PgError::new(ERROR, "cannot convert whole-row table reference".to_string())
                        .with_detail(format!(
                            "Generation expression for column \"{}\" contains a whole-row reference to table \"{relname}\".",
                            def.colname.expect("colname")
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            debug_assert!(def.raw_default.is_none());
            match def.cooked_default {
                None => def.cooked_default = Some(expr),
                Some(prev) => {
                    if !types_nodes::equal::equal(prev, expr) {
                        bogus[idx] = true;
                        have_bogus_defaults = true;
                    }
                }
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
                    rewrite_manip::map_variable_attnos(mcx, raw, 1, 0, &newattmap, types_core::InvalidOid)?;
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
                    merge_child_attribute(mcx, &mut inh_defs, idx, newcol_attno, newdef)?;
                    if newdef.raw_default.is_some() {
                        bogus[idx] = false;
                    }
                }
                None => {
                    // Local columns append after all inherited ones; keep the
                    // parse-tree node so raw defaults survive untouched.
                    inh_defs.push(clone_column_def(mcx, newdef)?);
                    bogus.push(false);
                }
            }
        }
        if inh_defs.len() > MaxHeapAttributeNumber {
            return Err(too_many_columns());
        }
        if have_bogus_defaults {
            for (idx, def) in inh_defs.iter().enumerate() {
                if bogus[idx] {
                    return Err(conflicting_inherited_defaults(
                        def.colname.expect("colname"),
                        def.generated != 0,
                    ));
                }
            }
        }
        for (i, mut def) in inh_defs.drain(..).enumerate() {
            if let Some(expr) = def.cooked_default.take() {
                gendefs.push(((i + 1) as AttrNumber, expr));
            }
            merged.lappend(mcx, Node::mk(mcx, def)?)?;
        }
    } else {
        for elt in local_defs.iter() {
            merged.lappend(mcx, *elt)?;
        }
    }

    Ok(MergedAttributes { columns: merged, checks, notnulls, gendefs })
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

fn clone_column_def<'mcx>(mcx: Mcx<'mcx>, d: &ColumnDef<'mcx>) -> PgResult<ColumnDef<'mcx>> {
    Ok(ColumnDef {
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
        constraints: d.constraints.clone_in(mcx)?,
        fdwoptions: d.fdwoptions.clone_in(mcx)?,
        location: d.location,
    })
}

// GetCompressionMethodName (toast_compression.c).
fn compression_method_name(c: u8) -> &'static str {
    match c {
        b'p' => "pglz",
        b'l' => "lz4",
        _ => panic!("invalid compression method {c}"),
    }
}

fn storage_name(c: u8) -> &'static str {
    match c {
        b'p' => "PLAIN",
        b'e' => "EXTERNAL",
        b'x' => "EXTENDED",
        b'm' => "MAIN",
        _ => "???",
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
            mcx::session_root("coldef-type-scratch");
    }
    CTX.with(|c| c.mcx())
}

fn coldef_collation(def: &ColumnDef<'_>, typeoid: Oid) -> PgResult<Oid> {
    crate::GetColumnDefCollation(def, typeoid)
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
        return Err(column_conflict(
            "inherited column \"{}\" has a storage parameter conflict",
            attname,
            format!(
                "{} versus {}",
                storage_name(prevdef.storage),
                storage_name(newdef.storage)
            ),
            types_error::ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if prevdef.compression.is_none() {
        prevdef.compression = newdef.compression;
    } else if let Some(newcomp) = newdef.compression {
        if prevdef.compression != Some(newcomp) {
            return Err(column_conflict(
                "column \"{}\" has a compression method conflict",
                attname,
                format!("{} versus {newcomp}", prevdef.compression.expect("compression")),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        }
    }
    if prevdef.generated != newdef.generated {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("inherited column \"{attname}\" has a generation conflict"),
            )
            .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
        ));
    }
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
    // Identity is never inherited by a regular child; the child's wins.
    inhdef.identity = newdef.identity;
    if inhdef.storage == 0 {
        inhdef.storage = newdef.storage;
    } else if newdef.storage != 0 && inhdef.storage != newdef.storage {
        return Err(column_conflict(
            "column \"{}\" has a storage parameter conflict",
            attname,
            format!(
                "{} versus {}",
                storage_name(inhdef.storage),
                storage_name(newdef.storage)
            ),
            types_error::ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if inhdef.compression.is_none() {
        inhdef.compression = newdef.compression;
    } else if let Some(newcomp) = newdef.compression {
        if inhdef.compression != Some(newcomp) {
            return Err(column_conflict(
                "column \"{}\" has a compression method conflict",
                attname,
                format!("{} versus {newcomp}", inhdef.compression.expect("compression")),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        }
    }
    inhdef.is_not_null |= newdef.is_not_null;
    if inhdef.generated != 0 {
        if newdef.raw_default.is_some() && newdef.generated == 0 {
            return Err(invalid_column_definition(format!(
                "column \"{attname}\" inherits from generated column but specifies default"
            )));
        }
        if newdef.identity != 0 {
            return Err(invalid_column_definition(format!(
                "column \"{attname}\" inherits from generated column but specifies identity"
            )));
        }
    } else if newdef.generated != 0 {
        return Err(child_generation_expression(attname));
    }
    if inhdef.generated != 0 && newdef.generated != 0 && newdef.generated != inhdef.generated {
        return Err(generation_kind_conflict(attname, inhdef.generated, newdef.generated));
    }
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

#[track_caller]
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
pub(crate) fn duplicate_column(colname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("column \"{colname}\" specified more than once"))
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_COLUMN),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn wrong_parent(msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_parents() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "too many inheritance parents".to_string())
            .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_column_definition(msg: String) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn child_generation_expression(attname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("child column \"{attname}\" specifies generation expression"),
        )
        .with_hint("A child table column cannot be generated unless its parent column is.")
        .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn generation_kind_conflict(attname: &str, parent: u8, child: u8) -> Box<PgError> {
    let kind = |g: u8| {
        if g == types_core::ATTRIBUTE_GENERATED_STORED { "STORED" } else { "VIRTUAL" }
    };
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{attname}\" inherits from generated column of different kind"),
        )
        .with_detail(format!(
            "Parent column is {}, child column is {}.",
            kind(parent),
            kind(child)
        ))
        .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn conflicting_inherited_defaults(colname: &str, generated: bool) -> Box<PgError> {
    let (msg, hint) = if generated {
        (
            format!("column \"{colname}\" inherits conflicting generation expressions"),
            "To resolve the conflict, specify a generation expression explicitly.",
        )
    } else {
        (
            format!("column \"{colname}\" inherits conflicting default values"),
            "To resolve the conflict, specify a default explicitly.",
        )
    };
    Box::new(
        PgError::new(ERROR, msg)
            .with_hint(hint)
            .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn column_conflict(
    template: &str,
    attname: &str,
    detail: String,
    sqlstate: types_error::SqlState,
) -> Box<PgError> {
    let msg = template.replacen("{}", attname, 1);
    let e = PgError::new(ERROR, msg).with_sqlstate(sqlstate);
    Box::new(if detail.is_empty() { e } else { e.with_detail(detail) })
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

const Anum_pg_attribute_attislocal: usize = 18;
const Anum_pg_attribute_attinhcount: usize = 19;

fn desc_attno_by_name(desc: &types_tuple::TupleDescData<'_>, name: &[u8]) -> Option<AttrNumber> {
    (0..desc.natts as usize)
        .find(|&i| !desc.attr(i).attisdropped && desc.attr(i).attname.name_str() == name)
        .map(|i| (i + 1) as AttrNumber)
}

// ATExecAddInherit (tablecmds.c). Transition-table triggers are loud at
// CREATE TRIGGER, so FindTriggerIncompatibleWithInheritance cannot match.
pub(crate) fn ATExecAddInherit<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    prv: &types_nodes::RangeVar<'_>,
) -> PgResult<()> {
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let parent_oid =
        catalog_namespace::RangeVarGetRelid(&rv, types_rel::ShareUpdateExclusiveLock, false)?;
    let parent_rel = table::table_open(mcx, parent_oid, NoLock)?;
    // ATSimplePermissions(parent); foreign tables loud.
    if !aclchk::object_ownercheck(
        types_core::RELATION_RELATION_ID,
        parent_oid,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(parent_rel.rd_rel.relkind),
            parent_rel.name(),
        )?;
    }
    if parent_rel.rd_rel.relkind != RELKIND_RELATION
        && parent_rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
    {
        // unported: ATExecAddInherit parent relkinds outside table/partitioned
        return Err(Box::new(
            PgError::error(
                "ALTER TABLE ... INHERIT for this type of parent relation is not supported yet",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let parent_name = parent_rel.name().to_string();
    let child_name = child_rel.name().to_string();
    if parent_rel.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
        && child_rel.rd_rel.relpersistence != types_core::RELPERSISTENCE_TEMP
    {
        return Err(wrong_parent(format!(
            "cannot inherit from temporary relation \"{parent_name}\""
        )));
    }
    if parent_rel.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
        && !parent_rel.rd_islocaltemp
    {
        return Err(wrong_parent(
            "cannot inherit from temporary relation of another session".to_string(),
        ));
    }
    if child_rel.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
        && !child_rel.rd_islocaltemp
    {
        return Err(wrong_parent(
            "cannot inherit to temporary relation of another session".to_string(),
        ));
    }
    if parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(wrong_parent(format!(
            "cannot inherit from partitioned table \"{}\"",
            rv.relname
        )));
    }
    if parent_rel.rd_rel.relispartition {
        return Err(wrong_parent("cannot inherit from a partition".to_string()));
    }

    let children =
        pg_inherits::find_all_inheritors(mcx, child_rel.rd_id, types_rel::AccessShareLock)?;
    if children.contains(&parent_oid) {
        return Err(Box::new(
            PgError::new(ERROR, "circular inheritance not allowed".to_string())
                .with_detail(format!(
                    "\"{}\" is already a child of \"{child_name}\".",
                    rv.relname
                ))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_TABLE),
        ));
    }

    // FindTriggerIncompatibleWithInheritance (tablecmds.c:17346-17353).
    if let Some(trigger_name) = crate::attach::find_transition_table_trigger(mcx, child_rel)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "trigger \"{trigger_name}\" prevents table \"{child_name}\" from becoming an inheritance child"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "ROW triggers with transition tables are not supported in inheritance hierarchies."
                    .to_string(),
            ),
        ));
    }

    CreateInheritance(mcx, child_rel, &parent_rel, false)?;
    // Keep the lock on the parent relation until commit.
    parent_rel.close(NoLock)
}

// CreateInheritance (tablecmds.c). DIVERGENCE: C holds one pg_inherits handle
// open across the seqno scan and StoreCatalogInheritance1; the port's
// StoreSingleInheritance re-opens it (catalog-only, same lock level).
pub(crate) fn CreateInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    ispartition: bool,
) -> PgResult<()> {
    let catalog_rel =
        table::table_open(mcx, pg_inherits::InheritsRelationId, types_rel::RowExclusiveLock)?;
    let key = crate::alter::oid_scankey(
        pg_inherits::Anum_pg_inherits_inhrelid as usize,
        child_rel.rd_id,
    );
    let mut scan = genam::systable_beginscan(
        mcx,
        &catalog_rel,
        pg_inherits::InheritsRelidSeqnoIndexId,
        true,
        None,
        &[key],
    )?;
    let desc = catalog_rel.descr();
    let mut inhseqno: i32 = 0;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_inherits columns under its descriptor.
        let inhparent = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_inherits::Anum_pg_inherits_inhparent as i32,
                desc,
                &mut isnull,
            )
        }
        .as_oid();
        if inhparent == parent_rel.rd_id {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "relation \"{}\" would be inherited from more than once",
                        parent_rel.name()
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_TABLE),
            ));
        }
        // SAFETY: as above.
        let seqno = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_inherits::Anum_pg_inherits_inhseqno as i32,
                desc,
                &mut isnull,
            )
        }
        .as_i32();
        if seqno > inhseqno {
            inhseqno = seqno;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    catalog_rel.close(types_rel::RowExclusiveLock)?;

    MergeAttributesIntoExisting(mcx, child_rel, parent_rel, ispartition)?;
    MergeConstraintsIntoExisting(mcx, child_rel, parent_rel)?;

    pg_inherits::StoreSingleInheritance(mcx, child_rel.rd_id, parent_rel.rd_id, inhseqno + 1)?;
    let childobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, child_rel.rd_id);
    let parentobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, parent_rel.rd_id);
    pg_depend::recordDependencyOn(
        mcx,
        &childobject,
        &parentobject,
        if parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            pg_depend::DependencyType::Auto
        } else {
            pg_depend::DependencyType::Normal
        },
    )?;
    crate::partition::SetRelationHasSubclass(mcx, parent_rel.rd_id, true)
}

// MergeAttributesIntoExisting (tablecmds.c).
fn MergeAttributesIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    ispartition: bool,
) -> PgResult<()> {
    let parent_desc = parent_rel.descr();
    let child_desc = child_rel.descr();
    let child_name = child_rel.name().to_string();
    for parent_attno in 1..=parent_desc.natts as usize {
        let parent_att = parent_desc.attr(parent_attno - 1);
        if parent_att.attisdropped {
            continue;
        }
        let attname_bytes = parent_att.attname.name_str();
        let parent_attname =
            core::str::from_utf8(attname_bytes).expect("attname UTF-8").to_string();
        let Some(child_attno) = desc_attno_by_name(child_desc, attname_bytes) else {
            return Err(column_conflict(
                "child table is missing column \"{}\"",
                &parent_attname,
                String::new(),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        };
        let child_att = child_desc.attr(child_attno as usize - 1);
        if parent_att.atttypid != child_att.atttypid
            || parent_att.atttypmod != child_att.atttypmod
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "child table \"{child_name}\" has different type for column \
                         \"{parent_attname}\""
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        if parent_att.attcollation != child_att.attcollation {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "child table \"{child_name}\" has different collation for column \
                         \"{parent_attname}\""
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_COLLATION_MISMATCH),
            ));
        }
        if parent_att.attnotnull && !child_att.attnotnull {
            if let Some(con) = pg_constraint::findNotNullConstraintAttnum(
                mcx,
                parent_rel.rd_id,
                parent_attno as AttrNumber,
            )? {
                if !con.connoinherit {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "column \"{parent_attname}\" in child table \"{child_name}\" \
                                 must be marked NOT NULL"
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                    ));
                }
            }
        }
        if parent_att.attgenerated != 0 && child_att.attgenerated == 0 {
            return Err(column_conflict(
                "column \"{}\" in child table must be a generated column",
                &parent_attname,
                String::new(),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        if child_att.attgenerated != 0 && parent_att.attgenerated == 0 {
            return Err(column_conflict(
                "column \"{}\" in child table must not be a generated column",
                &parent_attname,
                String::new(),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        if parent_att.attgenerated != 0
            && child_att.attgenerated != 0
            && parent_att.attgenerated != child_att.attgenerated
        {
            let kind = |g: i8| if g == b's' as i8 { "STORED" } else { "VIRTUAL" };
            return Err(column_conflict(
                "column \"{}\" inherits from generated column of different kind",
                &parent_attname,
                format!(
                    "Parent column is {}, child column is {}.",
                    kind(parent_att.attgenerated),
                    kind(child_att.attgenerated)
                ),
                types_error::ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        debug_assert!(!ispartition, "ATTACH PARTITION unported");
        if child_att.attinhcount == i16::MAX {
            return Err(too_many_parents());
        }
        let mut fields: PgVec<'mcx, (usize, datum::Datum)> = PgVec::new_in(mcx);
        fields.push((
            Anum_pg_attribute_attinhcount,
            datum::Datum::from_i16(child_att.attinhcount + 1),
        ));
        if parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            debug_assert!(child_att.attinhcount + 1 == 1);
            fields.push((Anum_pg_attribute_attislocal, datum::Datum::from_bool(false)));
        }
        crate::alter::update_pg_attribute(mcx, child_rel.rd_id, child_attno, &fields)?;
    }
    Ok(())
}

struct ConRow {
    oid: Oid,
    contype: u8,
    conname: String,
    connoinherit: bool,
    condeferrable: bool,
    condeferred: bool,
    conenforced: bool,
    convalidated: bool,
    coninhcount: i16,
    decompiled: Option<String>,
    nn_attname: Option<String>,
}

// std Vec: ConRow owns Strings (drop glue), cold DDL scratch — PgVec forbids
// droppy payloads.
fn collect_con_rows<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<Vec<ConRow>> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::RowExclusiveLock,
    )?;
    let key = crate::constraints::eq_key(
        pg_constraint::Anum_pg_constraint_conrelid,
        types_core::fmgr::F_OIDEQ,
        datum::Datum::from_oid(rel.rd_id),
    );
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &[key],
    )?;
    let desc = con_rel.descr();
    let mut rows: Vec<ConRow> = Vec::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get = |anum: types_core::AttrNumber| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let contype = get(pg_constraint::Anum_pg_constraint_contype).as_i8() as u8;
        if contype != pg_constraint::CONSTRAINT_CHECK
            && contype != pg_constraint::CONSTRAINT_NOTNULL
        {
            continue;
        }
        // SAFETY: NameData column is a 64-byte in-tuple buffer.
        let namebytes = unsafe {
            core::slice::from_raw_parts(
                get(pg_constraint::Anum_pg_constraint_conname).as_usize() as *const u8,
                64,
            )
        };
        let namelen = namebytes.iter().position(|&b| b == 0).unwrap_or(64);
        let conname =
            core::str::from_utf8(&namebytes[..namelen]).expect("conname UTF-8").to_string();
        let decompiled = if contype == pg_constraint::CONSTRAINT_CHECK {
            let mut isnull = false;
            // SAFETY: conbin under pg_constraint's descriptor; null-checked below.
            let val = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    pg_constraint::Anum_pg_constraint_conbin as i32,
                    desc,
                    &mut isnull,
                )
            };
            if isnull {
                panic!("null conbin for constraint \"{conname}\"");
            }
            // decompile_conbin: DirectFunctionCall2(pg_get_expr); the result
            // text lives in flinfo's fn_extra scratch — copy out before drop.
            let mut flinfo = fmgr_seams::fmgr_info::call(1716)?;
            let text = fmgr_core::function_call2_coll(
                &mut flinfo,
                types_core::InvalidOid,
                val,
                datum::Datum::from_oid(rel.rd_id),
            )?;
            let p = text.as_usize() as *const u8;
            // SAFETY: live varlena text result through its extent (flinfo
            // scratch alive until end of scope).
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let payload = varlena::open_image(mcx, image)?;
            Some(core::str::from_utf8(payload.as_bytes()).expect("text UTF-8").to_string())
        } else {
            None
        };
        let nn_attname = if contype == pg_constraint::CONSTRAINT_NOTNULL {
            let attnum = pg_constraint::extractNotNullColumn(mcx, tup, desc)?;
            let att = rel.rd_att.attr(attnum as usize - 1);
            if att.attisdropped {
                panic!("found not-null constraint on dropped columns");
            }
            Some(core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8").to_string())
        } else {
            None
        };
        rows.push(ConRow {
            oid: get(pg_constraint::Anum_pg_constraint_oid).as_oid(),
            contype,
            conname,
            connoinherit: get(pg_constraint::Anum_pg_constraint_connoinherit).as_bool(),
            condeferrable: get(pg_constraint::Anum_pg_constraint_condeferrable).as_bool(),
            condeferred: get(pg_constraint::Anum_pg_constraint_condeferred).as_bool(),
            conenforced: get(pg_constraint::Anum_pg_constraint_conenforced).as_bool(),
            convalidated: get(pg_constraint::Anum_pg_constraint_convalidated).as_bool(),
            coninhcount: get(pg_constraint::Anum_pg_constraint_coninhcount).as_i16(),
            decompiled,
            nn_attname,
        });
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::RowExclusiveLock)?;
    Ok(rows)
}

// MergeConstraintsIntoExisting (tablecmds.c). constraints_equivalent's
// decompile rides pg_get_expr through fmgr (C DirectFunctionCall2); NOT NULL
// column matching is by attribute name (C's build_attrmap_by_name by-name map).
fn MergeConstraintsIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let parent_cons = collect_con_rows(mcx, parent_rel)?;
    let child_cons = collect_con_rows(mcx, child_rel)?;
    let child_name = child_rel.name().to_string();
    for pcon in parent_cons.iter() {
        if pcon.connoinherit {
            continue;
        }
        let mut found = false;
        for ccon in child_cons.iter() {
            if ccon.contype != pcon.contype {
                continue;
            }
            if ccon.contype == pg_constraint::CONSTRAINT_CHECK {
                if ccon.conname != pcon.conname {
                    continue;
                }
            } else if ccon.nn_attname != pcon.nn_attname {
                continue;
            }
            if ccon.contype == pg_constraint::CONSTRAINT_CHECK
                && (ccon.condeferrable != pcon.condeferrable
                    || ccon.condeferred != pcon.condeferred
                    || ccon.decompiled != pcon.decompiled)
            {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "child table \"{child_name}\" has different definition for check \
                             constraint \"{}\"",
                            pcon.conname
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                ));
            }
            if ccon.connoinherit {
                return Err(child_con_conflict(
                    &ccon.conname,
                    &child_name,
                    "non-inherited",
                ));
            }
            if pcon.convalidated && ccon.conenforced && !ccon.convalidated {
                return Err(child_con_conflict(&ccon.conname, &child_name, "NOT VALID"));
            }
            if pcon.conenforced && !ccon.conenforced {
                return Err(child_con_conflict(&ccon.conname, &child_name, "NOT ENFORCED"));
            }
            if ccon.coninhcount == i16::MAX {
                return Err(too_many_parents());
            }
            let mut fields: PgVec<'mcx, (AttrNumber, datum::Datum)> = PgVec::new_in(mcx);
            fields.push((
                pg_constraint::Anum_pg_constraint_coninhcount,
                datum::Datum::from_i16(ccon.coninhcount + 1),
            ));
            if parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
                debug_assert!(ccon.coninhcount + 1 == 1);
                fields.push((
                    pg_constraint::Anum_pg_constraint_conislocal,
                    datum::Datum::from_bool(false),
                ));
            }
            pg_constraint::update_constraint_fields(mcx, ccon.oid, &fields)?;
            found = true;
            break;
        }
        if !found {
            if pcon.contype == pg_constraint::CONSTRAINT_NOTNULL {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "column \"{}\" in child table \"{child_name}\" must be marked \
                             NOT NULL",
                            pcon.nn_attname.as_deref().expect("nn attname")
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                ));
            }
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("child table is missing constraint \"{}\"", pcon.conname),
                )
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
            ));
        }
    }
    Ok(())
}

// ATExecDropInherit (tablecmds.c).
pub(crate) fn ATExecDropInherit<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    prv: &types_nodes::RangeVar<'_>,
) -> PgResult<()> {
    if rel.rd_rel.relispartition {
        return Err(wrong_parent("cannot change inheritance of a partition".to_string()));
    }
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let parent_oid = catalog_namespace::RangeVarGetRelid(&rv, types_rel::AccessShareLock, false)?;
    let parent_rel = table::table_open(mcx, parent_oid, NoLock)?;
    RemoveInheritance(mcx, rel, &parent_rel, false)?;
    // Keep the lock on the parent relation until commit.
    parent_rel.close(NoLock)
}

// RemoveInheritance (tablecmds.c).
pub(crate) fn RemoveInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    expect_detached: bool,
) -> PgResult<()> {
    let is_partitioning = parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    let child_name = child_rel.name().to_string();
    let parent_name = parent_rel.name().to_string();
    let found = pg_inherits::DeleteInheritsTuple(
        mcx,
        child_rel.rd_id,
        parent_rel.rd_id,
        expect_detached,
        Some(&child_name),
    )?;
    if !found {
        let msg = if is_partitioning {
            format!(
                "relation \"{child_name}\" is not a partition of relation \"{parent_name}\""
            )
        } else {
            format!("relation \"{parent_name}\" is not a parent of relation \"{child_name}\"")
        };
        return Err(Box::new(
            PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_UNDEFINED_TABLE),
        ));
    }

    let child_desc = child_rel.descr();
    let parent_desc = parent_rel.descr();
    for i in 0..child_desc.natts as usize {
        let att = child_desc.attr(i);
        if att.attisdropped || att.attinhcount <= 0 {
            continue;
        }
        if desc_attno_by_name(parent_desc, att.attname.name_str()).is_none() {
            continue;
        }
        let newcount = att.attinhcount - 1;
        let mut fields: PgVec<'mcx, (usize, datum::Datum)> = PgVec::new_in(mcx);
        fields.push((Anum_pg_attribute_attinhcount, datum::Datum::from_i16(newcount)));
        if newcount == 0 {
            fields.push((Anum_pg_attribute_attislocal, datum::Datum::from_bool(true)));
        }
        crate::alter::update_pg_attribute(mcx, child_rel.rd_id, (i + 1) as AttrNumber, &fields)?;
    }

    let parent_cons = collect_con_rows(mcx, parent_rel)?;
    let child_cons = collect_con_rows(mcx, child_rel)?;
    let mut connames: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    let mut nncolumns: PgVec<'mcx, &str> = PgVec::new_in(mcx);
    for pcon in parent_cons.iter() {
        if pcon.connoinherit {
            continue;
        }
        if pcon.contype == pg_constraint::CONSTRAINT_CHECK {
            connames.push(&pcon.conname);
        }
        if pcon.contype == pg_constraint::CONSTRAINT_NOTNULL {
            nncolumns.push(pcon.nn_attname.as_deref().expect("nn attname"));
        }
    }
    for ccon in child_cons.iter() {
        let matched = if ccon.contype == pg_constraint::CONSTRAINT_CHECK {
            match connames.iter().position(|&n| n == ccon.conname) {
                Some(i) => {
                    connames.swap_remove(i);
                    true
                }
                None => false,
            }
        } else if ccon.contype == pg_constraint::CONSTRAINT_NOTNULL {
            let nn = ccon.nn_attname.as_deref().expect("nn attname");
            match nncolumns.iter().position(|&n| n == nn) {
                Some(i) => {
                    nncolumns.swap_remove(i);
                    true
                }
                None => false,
            }
        } else {
            false
        };
        if !matched {
            continue;
        }
        if ccon.coninhcount <= 0 {
            panic!(
                "relation {} has non-inherited constraint \"{}\"",
                child_rel.rd_id, ccon.conname
            );
        }
        let newcount = ccon.coninhcount - 1;
        let mut fields: PgVec<'mcx, (AttrNumber, datum::Datum)> = PgVec::new_in(mcx);
        fields.push((
            pg_constraint::Anum_pg_constraint_coninhcount,
            datum::Datum::from_i16(newcount),
        ));
        if newcount == 0 {
            fields.push((
                pg_constraint::Anum_pg_constraint_conislocal,
                datum::Datum::from_bool(true),
            ));
        }
        pg_constraint::update_constraint_fields(mcx, ccon.oid, &fields)?;
    }
    if !connames.is_empty() || !nncolumns.is_empty() {
        panic!(
            "{} unmatched constraints while removing inheritance from \"{child_name}\" to \
             \"{parent_name}\"",
            connames.len() + nncolumns.len()
        );
    }

    drop_parent_dependency(
        mcx,
        child_rel.rd_id,
        parent_rel.rd_id,
        if is_partitioning {
            pg_depend::DependencyType::Auto
        } else {
            pg_depend::DependencyType::Normal
        },
    )
}

// drop_parent_dependency (tablecmds.c), pg_class-referencing arm.
fn drop_parent_dependency<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    refobjid: Oid,
    deptype: pg_depend::DependencyType,
) -> PgResult<()> {
    let dep_rel =
        table::table_open(mcx, pg_depend::DependRelationId, types_rel::RowExclusiveLock)?;
    let keys = [
        crate::alter::oid_scankey(1, RELATION_RELATION_ID),
        crate::alter::oid_scankey(2, relid),
        crate::alter::int4_key(3, 0),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    let mut tids: PgVec<'mcx, types_tuple::ItemPointerData> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid =
            unsafe { types_tuple::heap_getattr(tup, 4, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let dep_refobjid =
            unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let refobjsubid =
            unsafe { types_tuple::heap_getattr(tup, 6, desc, &mut isnull) }.as_i32();
        // SAFETY: as above.
        let dtype =
            unsafe { types_tuple::heap_getattr(tup, 7, desc, &mut isnull) }.as_i8();
        if refclassid == RELATION_RELATION_ID
            && dep_refobjid == refobjid
            && refobjsubid == 0
            && dtype == deptype.as_char()
        {
            tids.push(tup.t_self);
        }
    }
    genam::systable_endscan(mcx, scan)?;
    for tid in tids.iter() {
        catalog_indexing::CatalogTupleDelete(&dep_rel, tid)?;
    }
    dep_rel.close(types_rel::RowExclusiveLock)
}

#[track_caller]
#[cold]
#[inline(never)]
fn child_con_conflict(conname: &str, child_name: &str, kind: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "constraint \"{conname}\" conflicts with {kind} constraint on child table \
                 \"{child_name}\""
            ),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> &'static mcx::MemoryContext {
        mcx::session_root("merge-test")
    }

    fn coldef<'m>(mcx: Mcx<'m>, name: &'m str, from_type: bool, not_null: bool) -> Node<'m> {
        let mut def = Node::build::<ColumnDef>(mcx).unwrap();
        def.colname = Some(name);
        if from_type {
            def.typeName = Some(Node::build::<TypeName>(mcx).unwrap().seal());
            def.is_from_type = true;
        }
        def.is_not_null = not_null;
        def.seal()
    }

    #[test]
    fn column_option_merges_onto_type_column() {
        let mcx = ctx().mcx();
        let mut cols = NodeList::nil();
        cols.lappend(mcx, coldef(mcx, "id", true, false)).unwrap();
        cols.lappend(mcx, coldef(mcx, "name", true, false)).unwrap();
        cols.lappend(mcx, coldef(mcx, "name", false, true)).unwrap();
        let out = merge_column_options(mcx, &cols).unwrap();
        assert_eq!(out.len(), 2);
        let name = out.nth(1).as_variant::<ColumnDef>().unwrap();
        assert_eq!(name.colname, Some("name"));
        assert!(!name.is_from_type);
        assert!(name.is_not_null);
        let id = out.nth(0).as_variant::<ColumnDef>().unwrap();
        assert!(id.is_from_type && !id.is_not_null);
    }

    #[test]
    fn column_option_without_type_column_errors() {
        let mcx = ctx().mcx();
        let mut cols = NodeList::nil();
        cols.lappend(mcx, coldef(mcx, "id", true, false)).unwrap();
        cols.lappend(mcx, coldef(mcx, "myname", false, true)).unwrap();
        let e = merge_column_options(mcx, &cols).unwrap_err();
        assert_eq!(e.message(), "column \"myname\" does not exist");
    }

    #[test]
    fn duplicate_column_option_errors() {
        let mcx = ctx().mcx();
        let mut cols = NodeList::nil();
        cols.lappend(mcx, coldef(mcx, "name", true, false)).unwrap();
        cols.lappend(mcx, coldef(mcx, "name", false, true)).unwrap();
        cols.lappend(mcx, coldef(mcx, "name", false, false)).unwrap();
        let e = merge_column_options(mcx, &cols).unwrap_err();
        assert_eq!(e.message(), "column \"name\" specified more than once");
    }

    #[test]
    fn partition_dup_scan_errors_on_duplicates() {
        let mcx = ctx().mcx();
        let mut cols = NodeList::nil();
        cols.lappend(mcx, coldef(mcx, "b", false, true)).unwrap();
        cols.lappend(mcx, coldef(mcx, "b", false, false)).unwrap();
        let e = partition_column_dup_scan(&cols).unwrap_err();
        assert_eq!(e.message(), "column \"b\" specified more than once");
    }

    #[test]
    fn partition_dup_scan_accepts_distinct_names() {
        let mcx = ctx().mcx();
        let mut cols = NodeList::nil();
        cols.lappend(mcx, coldef(mcx, "a", false, true)).unwrap();
        cols.lappend(mcx, coldef(mcx, "b", false, false)).unwrap();
        partition_column_dup_scan(&cols).unwrap();
        partition_column_dup_scan(&NodeList::nil()).unwrap();
    }
}
