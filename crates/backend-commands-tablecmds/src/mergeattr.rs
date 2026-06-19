//! `MergeAttributes` (tablecmds.c:2546) — build the merged column descriptor
//! list from a `CREATE TABLE`'s explicit columns plus inherited parents.
//!
//! The non-inheritance path (`supers == NIL`, not a partition), the
//! legacy-inheritance path (`supers != NIL`), and the multi-parent merge helpers
//! (`MergeInheritedAttribute` / `MergeChildAttribute` / `MergeCheckConstraint`)
//! are ported here. The partition column-merge path (`is_partition`) is NOT yet
//! ported (it bottoms out on the partition machinery): it panics with a precise
//! handoff.

use backend_utils_error::ereport;
use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{InvalidOid, Oid};
use types_core::AttrNumber;
use types_error::{PgError, PgResult, ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_COLUMN_DEFINITION, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE};
use types_nodes::ddlnodes::{Constraint, ConstrType};
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::rawnodes::ColumnDef;
use types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELPERSISTENCE_TEMP};
use types_tuple::heaptuple::MaxHeapAttributeNumber;

use backend_access_common_next::attmap::{free_attrmap, make_attrmap};
use backend_access_common_relation::relation_open;
use backend_access_common_toast_compression::{get_compression_method_name,
    INVALID_COMPRESSION_METHOD};
use backend_catalog_pg_constraint::{NotNullConstraint, RelationGetNotNullConstraints};
use backend_nodes_core::bitmapset::{bms_add_member, bms_is_member};
use backend_nodes_core::makefuncs::make_column_def;
use backend_nodes_equalfuncs_seams::equal_node;
use backend_rewrite_rewritemanip_seams::map_variable_attnos_node;
use backend_utils_adt_format_type::format_type_with_typemod;
use backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_name;
use types_nodes::bitmapset::Bitmapset;
use types_storage::lock::NoLock;

use backend_access_common_tupdesc::TupleDescGetDefault;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_objectaddress_seams as objaddr_seam;
use backend_nodes_core::read::string_to_node;
use backend_utils_init_miscinit::GetUserId;
use types_acl::ACLCHECK_NOT_OWNER;

use backend_commands_tablecmds_seams::{self as seam, MergeAttributesResult};

use crate::create::findAttrByName;
use crate::helpers::{here, RelationRelationId};

const PG_INT16_MAX: i16 = i16::MAX;
const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;

/// `MergeAttributes(columns, supers, relpersistence, is_partition,
/// &supconstr, &supnotnulls)`.
pub fn merge_attributes<'mcx>(
    mcx: Mcx<'mcx>,
    mut columns: PgVec<'mcx, ColumnDef<'mcx>>,
    supers: &[Oid],
    relpersistence: u8,
    is_partition: bool,
) -> PgResult<MergeAttributesResult<'mcx>> {
    /* inh_columns / constraints / nnconstraints accumulators. */
    let mut inh_columns: PgVec<'mcx, ColumnDef<'mcx>> = vec_with_capacity_in(mcx, 0)?;
    let mut constraints: PgVec<'mcx, NodePtr<'mcx>> = vec_with_capacity_in(mcx, 0)?;
    let mut nnconstraints: PgVec<'mcx, NodePtr<'mcx>> = vec_with_capacity_in(mcx, 0)?;
    let mut have_bogus_defaults = false;
    let mut child_attno: i32;

    /*
     * Check for and reject tables with too many columns. We perform this check
     * relatively early to avoid overflowing an AttrNumber, and because the
     * dedup pass below is O(n^2).
     */
    if columns.len() as i32 > MaxHeapAttributeNumber {
        return ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "tables can have at most {MaxHeapAttributeNumber} columns"
            ))
            .finish(here("MergeAttributes"))
            .map(|()| unreachable!());
    }

    /*
     * Check for duplicate names in the explicit list of attributes.
     */
    let mut coldefpos = 0usize;
    while coldefpos < columns.len() {
        if !is_partition && columns[coldefpos].typeName.is_none() {
            let colname = colname_of(&columns[coldefpos]).to_string();
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" does not exist"))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        let mut restpos = coldefpos + 1;
        while restpos < columns.len() {
            if colname_of(&columns[coldefpos]) == colname_of(&columns[restpos]) {
                if columns[coldefpos].is_from_type {
                    /* merge the column options into the column from the type */
                    let restdef = columns.remove(restpos);
                    let coldef = &mut columns[coldefpos];
                    coldef.is_not_null = restdef.is_not_null;
                    coldef.raw_default = restdef.raw_default;
                    coldef.cooked_default = restdef.cooked_default;
                    coldef.constraints = restdef.constraints;
                    coldef.is_from_type = false;
                } else {
                    let colname = colname_of(&columns[restpos]).to_string();
                    return ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_COLUMN)
                        .errmsg(format!("column \"{colname}\" specified more than once"))
                        .finish(here("MergeAttributes"))
                        .map(|()| unreachable!());
                }
            } else {
                restpos += 1;
            }
        }
        coldefpos += 1;
    }

    /*
     * In case of a partition, there are no new column definitions, only dummy
     * ColumnDefs created for column constraints.  C sets them aside for now and
     * processes them at the end. The partition column-merge path is NOT yet
     * ported (it bottoms out on the partition machinery).
     */
    if is_partition {
        panic!(
            "MergeAttributes: partition column-merge path not yet ported \
             (is_partition=true); only the plain CREATE TABLE and \
             legacy-inheritance paths are ported"
        );
    }

    /*
     * Scan the parents left-to-right, and merge their attributes to form a
     * list of inherited columns (inh_columns).
     */
    child_attno = 0;
    for &parent in supers.iter() {
        /* caller already got lock */
        let relation = relation_open(mcx, parent, NoLock)?;

        /*
         * We do not allow partitioned tables and partitions to participate in
         * regular inheritance.
         */
        if relation.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "cannot inherit from partitioned table \"{}\"",
                    relation.rd_rel.relname.as_str()
                ))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }
        if relation.rd_rel.relispartition {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "cannot inherit from partition \"{}\"",
                    relation.rd_rel.relname.as_str()
                ))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        if relation.rd_rel.relkind != RELKIND_RELATION
            && relation.rd_rel.relkind != RELKIND_FOREIGN_TABLE
            && relation.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
        {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "inherited relation \"{}\" is not a table or foreign table",
                    relation.rd_rel.relname.as_str()
                ))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        /* Permanent rels cannot inherit from temporary ones */
        if relpersistence != RELPERSISTENCE_TEMP
            && relation.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "cannot inherit from temporary relation \"{}\"",
                    relation.rd_rel.relname.as_str()
                ))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        /* If existing rel is temp, it must belong to this session */
        if relation.rd_rel.relpersistence == RELPERSISTENCE_TEMP
            && !backend_utils_cache_relcache_seams::rd_islocaltemp::call(&relation)?
        {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("cannot inherit from temporary relation of another session")
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        /*
         * We should have an UNDER permission flag for this, but for now,
         * demand that creator of a child table own the parent.
         */
        if !aclchk_seam::object_ownercheck::call(
            RelationRelationId,
            relation.rd_id,
            GetUserId(),
        )? {
            aclchk_seam::aclcheck_error::call(
                ACLCHECK_NOT_OWNER,
                objaddr_seam::get_relkind_objtype::call(relation.rd_rel.relkind),
                Some(relation.rd_rel.relname.as_str().to_string()),
            )?;
        }

        let tuple_desc = &relation.rd_att;

        /*
         * newattmap->attnums[] will contain the child-table attribute numbers
         * for the attributes of this parent table.
         */
        let mut newattmap = make_attrmap(mcx, tuple_desc.natts)?;

        /* We can't process inherited defaults until newattmap is complete. */
        let mut inherited_defaults: Vec<NodePtr<'mcx>> = Vec::new();
        let mut cols_with_defaults: Vec<usize> = Vec::new();

        /*
         * Request attnotnull on columns that have a not-null constraint that's
         * not marked NO INHERIT (even if not valid).
         */
        let nnconstrs = RelationGetNotNullConstraints(mcx, relation.rd_id, true, false)?;
        let mut nncols: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
        for cc in nnconstrs.iter() {
            let attnum = cooked_attnum(cc);
            nncols = Some(bms_add_member(mcx, nncols.take(), attnum as i32)?);
        }

        let natts = tuple_desc.natts;
        for parent_attno in 1..=natts {
            let attribute = tuple_desc.attr((parent_attno - 1) as usize);

            /* Ignore dropped columns in the parent. */
            if attribute.attisdropped {
                continue; /* leave newattmap->attnums entry as zero */
            }

            let attribute_name = attname_str(attribute.attname.name_str())?;

            /* Create new column definition */
            let mut newdef = make_column_def(
                mcx,
                &attribute_name,
                attribute.atttypid,
                attribute.atttypmod,
                attribute.attcollation,
            )?;
            newdef.storage = attribute.attstorage;
            newdef.generated = attribute.attgenerated;
            if attribute.attcompression != INVALID_COMPRESSION_METHOD as i8 {
                let name = get_compression_method_name(attribute.attcompression as u8)?;
                newdef.compression = Some(PgString::from_str_in(name, mcx)?);
            }

            /*
             * Regular inheritance children are independent enough not to
             * inherit identity columns.  (is_partition handled separately.)
             */

            /*
             * Does it match some previously considered column from another
             * parent?
             */
            let exist_attno = findAttrByName(&attribute_name, &inh_columns);
            let mergeddef_idx: usize;
            if exist_attno > 0 {
                /* Yes, try to merge the two column definitions. */
                merge_inherited_attribute(mcx, &mut inh_columns, exist_attno, &newdef)?;
                mergeddef_idx = (exist_attno - 1) as usize;
                newattmap.attnums[(parent_attno - 1) as usize] = exist_attno as AttrNumber;
            } else {
                /* No, create a new inherited column */
                newdef.inhcount = 1;
                newdef.is_local = false;
                inh_columns.push(newdef);
                mergeddef_idx = inh_columns.len() - 1;
                child_attno += 1;
                newattmap.attnums[(parent_attno - 1) as usize] = child_attno as AttrNumber;
            }

            /* mark attnotnull if parent has it */
            if bms_is_member(parent_attno, nncols.as_deref()) {
                inh_columns[mergeddef_idx].is_not_null = true;
            }

            /* Locate default/generation expression if any */
            if attribute.atthasdef {
                let this_default = TupleDescGetDefault(
                    mcx,
                    &relation.rd_att,
                    parent_attno as AttrNumber,
                )?;
                let this_default = match this_default {
                    Some(d) => d,
                    None => {
                        return ereport(ERROR)
                            .errmsg(format!(
                                "default expression not found for attribute {parent_attno} of relation \"{}\"",
                                relation.rd_rel.relname.as_str()
                            ))
                            .finish(here("MergeAttributes"))
                            .map(|()| unreachable!());
                    }
                };
                inherited_defaults.push(this_default);
                cols_with_defaults.push(mergeddef_idx);
            }
        }

        /*
         * Now process any inherited default expressions, adjusting attnos
         * using the completed newattmap map.
         */
        for (this_default, &def_idx) in inherited_defaults
            .into_iter()
            .zip(cols_with_defaults.iter())
        {
            /* Adjust Vars to match new table's column numbering */
            let (this_default, found_whole_row) = map_variable_attnos_node::call(
                mcx,
                this_default,
                1,
                0,
                &newattmap.attnums,
                InvalidOid,
            )?;

            if found_whole_row {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot convert whole-row table reference")
                    .errdetail(format!(
                        "Generation expression for column \"{}\" contains a whole-row reference to table \"{}\".",
                        colname_of(&inh_columns[def_idx]),
                        relation.rd_rel.relname.as_str()
                    ))
                    .finish(here("MergeAttributes"))
                    .map(|()| unreachable!());
            }

            /*
             * If we already had a default from some prior parent, check to see
             * if they are the same.  If so, no problem; if not, mark the column
             * as having a bogus default.
             */
            let def = &mut inh_columns[def_idx];
            debug_assert!(def.raw_default.is_none());
            match def.cooked_default.take() {
                None => def.cooked_default = Some(this_default),
                Some(existing) => {
                    if equal_node::call(&existing, &this_default) {
                        def.cooked_default = Some(existing);
                    } else {
                        /* mark the column as having a bogus default */
                        def.cooked_default = Some(make_bogus_marker(mcx)?);
                        have_bogus_defaults = true;
                    }
                }
            }
        }

        /*
         * Now copy the CHECK constraints of this parent, adjusting attnos using
         * the completed newattmap map.  Identically named constraints are merged
         * if possible, else we throw error.
         */
        if let Some(constr) = relation.rd_att.constr.as_ref() {
            for i in 0..(constr.num_check as usize) {
                let check = &constr.check[i];

                /* ignore if the constraint is non-inheritable */
                if check.ccnoinherit {
                    continue;
                }

                let name = check
                    .ccname
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let ccbin = check.ccbin.as_ref().map(|s| s.as_str()).unwrap_or("");

                /* Adjust Vars to match new table's column numbering */
                let parsed = string_to_node(mcx, ccbin)?;
                let (expr, found_whole_row) = map_variable_attnos_node::call(
                    mcx,
                    parsed,
                    1,
                    0,
                    &newattmap.attnums,
                    InvalidOid,
                )?;

                if found_whole_row {
                    return ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot convert whole-row table reference")
                        .errdetail(format!(
                            "Constraint \"{name}\" contains a whole-row reference to table \"{}\".",
                            relation.rd_rel.relname.as_str()
                        ))
                        .finish(here("MergeAttributes"))
                        .map(|()| unreachable!());
                }

                merge_check_constraint(
                    mcx,
                    &mut constraints,
                    &name,
                    expr,
                    check.ccenforced,
                )?;
            }
        }

        /*
         * Also copy the not-null constraints from this parent.  The attnotnull
         * markings were already installed above.
         */
        for nn in nnconstrs.iter() {
            let old_attnum = cooked_attnum(nn);
            let new_attnum = newattmap.attnums[(old_attnum - 1) as usize];
            let name = cooked_name(nn);
            nnconstraints.push(make_notnull_carrier(mcx, name.as_deref(), new_attnum)?);
        }

        free_attrmap(newattmap);

        /*
         * Close the parent rel, but keep our lock on it until xact commit (RAII
         * drop releases the relcache ref; the lmgr lock is transaction-scoped).
         */
        drop(relation);
    }

    /*
     * If we had no inherited attributes, the result columns are just the
     * explicitly declared columns.  Otherwise, we need to merge the declared
     * columns into the inherited column list.
     */
    if !inh_columns.is_empty() {
        let mut newcol_attno = 0i32;

        for newdef in columns.into_iter() {
            newcol_attno += 1;
            let attribute_name = colname_of(&newdef).to_string();

            /* Does it match some inherited column? */
            let exist_attno = findAttrByName(&attribute_name, &inh_columns);
            if exist_attno > 0 {
                /* Yes, try to merge the two column definitions. */
                merge_child_attribute(mcx, &mut inh_columns, exist_attno, newcol_attno, &newdef)?;
            } else {
                /* No, attach new column unchanged to result columns. */
                inh_columns.push(newdef);
            }
        }

        columns = inh_columns;

        /*
         * Check that we haven't exceeded the legal # of columns after merging
         * in inherited columns.
         */
        if columns.len() as i32 > MaxHeapAttributeNumber {
            return ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_COLUMNS)
                .errmsg(format!(
                    "tables can have at most {MaxHeapAttributeNumber} columns"
                ))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }
    }

    /*
     * If we found any conflicting parent default values, check to make sure
     * they were overridden by the child.
     */
    if have_bogus_defaults {
        for def in columns.iter() {
            if is_bogus_marker(def.cooked_default.as_deref()) {
                if def.generated != 0 {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
                        .errmsg(format!(
                            "column \"{}\" inherits conflicting generation expressions",
                            colname_of(def)
                        ))
                        .errhint("To resolve the conflict, specify a generation expression explicitly.")
                        .finish(here("MergeAttributes"))
                        .map(|()| unreachable!());
                } else {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
                        .errmsg(format!(
                            "column \"{}\" inherits conflicting default values",
                            colname_of(def)
                        ))
                        .errhint("To resolve the conflict, specify a default explicitly.")
                        .finish(here("MergeAttributes"))
                        .map(|()| unreachable!());
                }
            }
        }
    }

    Ok(MergeAttributesResult {
        columns,
        old_constraints: constraints,
        old_notnulls: nnconstraints,
    })
}

/// `MergeCheckConstraint` (tablecmds.c:3142) — try to merge an inherited CHECK
/// constraint with previous ones (carried as `Node::Constraint`).
fn merge_check_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    constraints: &mut PgVec<'mcx, NodePtr<'mcx>>,
    name: &str,
    expr: NodePtr<'mcx>,
    is_enforced: bool,
) -> PgResult<()> {
    for con_node in constraints.iter_mut() {
        let ccon = con_node
            .as_constraint()
            .expect("MergeCheckConstraint: expected Constraint node");
        debug_assert!(ccon.contype == ConstrType::CONSTR_CHECK);

        /* Non-matching names never conflict */
        if ccon.conname.as_ref().map(|s| s.as_str()).unwrap_or("") != name {
            continue;
        }

        let con_expr = ccon
            .raw_expr
            .as_ref()
            .expect("MergeCheckConstraint: cooked CHECK missing expr");
        if equal_node::call(&expr, con_expr) {
            /* OK to merge constraint with existing */
            let ccon = con_node
                .as_constraint_mut()
                .expect("MergeCheckConstraint: expected Constraint node");
            /* inhcount carried in `location`; pg_add_s16_overflow */
            let inhcount = ccon.location as i32 + 1;
            if inhcount > PG_INT16_MAX as i32 {
                return ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg("too many inheritance parents")
                    .finish(here("MergeCheckConstraint"))
                    .map(|()| unreachable!());
            }
            ccon.location = inhcount;

            /*
             * When enforceability differs, the merged constraint should be
             * marked as ENFORCED because one of the parents is ENFORCED.
             */
            if !ccon.is_enforced && is_enforced {
                ccon.is_enforced = true;
                ccon.skip_validation = false;
            }
            return Ok(());
        }

        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "check constraint name \"{name}\" appears multiple times but with different expressions"
            ))
            .finish(here("MergeCheckConstraint"))
            .map(|()| unreachable!());
    }

    /*
     * Constraint couldn't be merged with an existing one and also didn't
     * conflict, so add it as a new one to the list. `inhcount = 1` rides the
     * `location` field of the Constraint carrier (mirroring the cooked node's
     * other fields the storage consumer does not read back).
     */
    let newcon = make_check_carrier(mcx, name, expr, is_enforced)?;
    constraints.push(newcon);
    Ok(())
}

/// `MergeChildAttribute` (tablecmds.c:3245) — merge a child attribute definition
/// into the matching inherited attribute (modified in `inh_columns`).
fn merge_child_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    inh_columns: &mut PgVec<'mcx, ColumnDef<'mcx>>,
    exist_attno: i32,
    newcol_attno: i32,
    newdef: &ColumnDef<'mcx>,
) -> PgResult<()> {
    let attribute_name = colname_of(newdef).to_string();

    if exist_attno == newcol_attno {
        ereport(NOTICE)
            .errmsg(format!(
                "merging column \"{attribute_name}\" with inherited definition"
            ))
            .finish(here("MergeChildAttribute"))?;
    } else {
        ereport(NOTICE)
            .errmsg(format!(
                "moving and merging column \"{attribute_name}\" with inherited definition"
            ))
            .errdetail("User-specified column moved to the position of the inherited column.")
            .finish(here("MergeChildAttribute"))?;
    }

    let idx = (exist_attno - 1) as usize;

    /* Must have the same type and typmod */
    let (inhtypeid, inhtypmod) = coldef_type(mcx, &inh_columns[idx])?;
    let (newtypeid, newtypmod) = coldef_type(mcx, newdef)?;
    if inhtypeid != newtypeid || inhtypmod != newtypmod {
        return type_conflict(mcx, &attribute_name, "column", inhtypeid, inhtypmod, newtypeid, newtypmod);
    }

    /* Must have the same collation */
    let inhcollid = seam::get_column_def_collation::call(mcx, &inh_columns[idx], inhtypeid)?;
    let newcollid = seam::get_column_def_collation::call(mcx, newdef, newtypeid)?;
    if inhcollid != newcollid {
        return collation_conflict(mcx, &attribute_name, "column", inhcollid, newcollid);
    }

    /*
     * Identity is never inherited by a regular inheritance child. Pick child's
     * identity definition if there's one.
     */
    inh_columns[idx].identity = newdef.identity;

    /* Copy storage parameter */
    if inh_columns[idx].storage == 0 {
        inh_columns[idx].storage = newdef.storage;
    } else if newdef.storage != 0 && inh_columns[idx].storage != newdef.storage {
        return storage_conflict(&attribute_name, "column", inh_columns[idx].storage, newdef.storage);
    }

    /* Copy compression parameter */
    if inh_columns[idx].compression.is_none() {
        inh_columns[idx].compression = clone_opt_str(mcx, &newdef.compression)?;
    } else if let Some(newcomp) = newdef.compression.as_ref() {
        let inhcomp = inh_columns[idx].compression.as_ref().unwrap().as_str().to_string();
        if inhcomp != newcomp.as_str() {
            return compression_conflict(&attribute_name, "column", &inhcomp, newcomp.as_str());
        }
    }

    /* Merge of not-null constraints = OR 'em together */
    inh_columns[idx].is_not_null |= newdef.is_not_null;

    /* Check for conflicts related to generated columns. */
    check_generated_conflicts(
        mcx,
        inh_columns[idx].generated,
        newdef,
        colname_of(&inh_columns[idx]),
    )?;

    /* If new def has a default, override previous default */
    if newdef.raw_default.is_some() {
        inh_columns[idx].raw_default = clone_opt_node(mcx, &newdef.raw_default)?;
        inh_columns[idx].cooked_default = clone_opt_node(mcx, &newdef.cooked_default)?;
    }

    /* Mark the column as locally defined */
    inh_columns[idx].is_local = true;

    Ok(())
}

/// `MergeInheritedAttribute` (tablecmds.c:3411) — merge a parent attribute into
/// the matching attribute inherited from previous parents.
fn merge_inherited_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    inh_columns: &mut PgVec<'mcx, ColumnDef<'mcx>>,
    exist_attno: i32,
    newdef: &ColumnDef<'mcx>,
) -> PgResult<()> {
    let attribute_name = colname_of(newdef).to_string();

    ereport(NOTICE)
        .errmsg(format!(
            "merging multiple inherited definitions of column \"{attribute_name}\""
        ))
        .finish(here("MergeInheritedAttribute"))?;

    let idx = (exist_attno - 1) as usize;

    /* Must have the same type and typmod */
    let (prevtypeid, prevtypmod) = coldef_type(mcx, &inh_columns[idx])?;
    let (newtypeid, newtypmod) = coldef_type(mcx, newdef)?;
    if prevtypeid != newtypeid || prevtypmod != newtypmod {
        return type_conflict(mcx, &attribute_name, "inherited column", prevtypeid, prevtypmod, newtypeid, newtypmod);
    }

    /* Must have the same collation */
    let prevcollid = seam::get_column_def_collation::call(mcx, &inh_columns[idx], prevtypeid)?;
    let newcollid = seam::get_column_def_collation::call(mcx, newdef, newtypeid)?;
    if prevcollid != newcollid {
        return collation_conflict(mcx, &attribute_name, "inherited column", prevcollid, newcollid);
    }

    /* Copy/check storage parameter */
    if inh_columns[idx].storage == 0 {
        inh_columns[idx].storage = newdef.storage;
    } else if inh_columns[idx].storage != newdef.storage {
        return storage_conflict(&attribute_name, "inherited column", inh_columns[idx].storage, newdef.storage);
    }

    /* Copy/check compression parameter */
    if inh_columns[idx].compression.is_none() {
        inh_columns[idx].compression = clone_opt_str(mcx, &newdef.compression)?;
    } else if let Some(newcomp) = newdef.compression.as_ref() {
        let prevcomp = inh_columns[idx].compression.as_ref().unwrap().as_str().to_string();
        if prevcomp != newcomp.as_str() {
            return compression_conflict(&attribute_name, "column", &prevcomp, newcomp.as_str());
        }
    }

    /* Check for GENERATED conflicts */
    if inh_columns[idx].generated != newdef.generated {
        return ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "inherited column \"{attribute_name}\" has a generation conflict"
            ))
            .finish(here("MergeInheritedAttribute"))
            .map(|()| unreachable!());
    }

    /* Default and other constraints are handled by the caller. */

    /* pg_add_s16_overflow(prevdef->inhcount, 1, ...) */
    let inhcount = inh_columns[idx].inhcount as i32 + 1;
    if inhcount > PG_INT16_MAX as i32 {
        return ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("too many inheritance parents")
            .finish(here("MergeInheritedAttribute"))
            .map(|()| unreachable!());
    }
    inh_columns[idx].inhcount = inhcount as i16;

    Ok(())
}

/* ---------------------------------------------------------------------------
 * helpers
 * ------------------------------------------------------------------------- */

/// The column name of a `ColumnDef`.
fn colname_of<'a>(col: &'a ColumnDef<'_>) -> &'a str {
    col.colname.as_ref().map(|s| s.as_str()).unwrap_or("")
}

/// `NameStr(attribute->attname)` — the parent attribute's name as a `&str`.
fn attname_str(bytes: &[u8]) -> PgResult<String> {
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Ok(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// `typenameTypeIdAndMod(NULL, coldef->typeName, ...)` for a `ColumnDef`.
fn coldef_type<'mcx>(mcx: Mcx<'mcx>, col: &ColumnDef<'mcx>) -> PgResult<(Oid, i32)> {
    let tn = col
        .typeName
        .as_ref()
        .ok_or_else(|| PgError::error("MergeAttributes: column has no type name"))?;
    seam::typename_type_id_and_mod::call(mcx, tn)
}

/// Attnum of a cooked not-null constraint (`RelationGetNotNullConstraints` with
/// `cooked == true` returns the `Cooked` variant).
fn cooked_attnum(nn: &NotNullConstraint<'_>) -> AttrNumber {
    match nn {
        NotNullConstraint::Cooked(c) => c.attnum,
        NotNullConstraint::Raw(_) => {
            unreachable!("RelationGetNotNullConstraints(cooked=true) returns Cooked")
        }
    }
}

fn cooked_name(nn: &NotNullConstraint<'_>) -> Option<String> {
    match nn {
        NotNullConstraint::Cooked(c) => c.name.as_ref().map(|s| s.as_str().to_string()),
        NotNullConstraint::Raw(_) => None,
    }
}

fn clone_opt_str<'mcx>(
    mcx: Mcx<'mcx>,
    s: &Option<PgString<'mcx>>,
) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(PgString::from_str_in(s.as_str(), mcx)?)),
        None => Ok(None),
    }
}

fn clone_opt_node<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Option<NodePtr<'mcx>>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    match n {
        Some(n) => Ok(Some(alloc_in(mcx, (**n).clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Build the `Node::Constraint` carrier for an inherited CHECK constraint, with
/// the cooked fields riding the carrier's slots (mirroring the storage
/// consumer's mapping: `initially_valid`=is_local, `location`=inhcount).
fn make_check_carrier<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    expr: NodePtr<'mcx>,
    is_enforced: bool,
) -> PgResult<NodePtr<'mcx>> {
    let mut c = empty_constraint(mcx, ConstrType::CONSTR_CHECK)?;
    c.conname = Some(PgString::from_str_in(name, mcx)?);
    c.raw_expr = Some(expr);
    c.is_enforced = is_enforced;
    c.skip_validation = !is_enforced;
    c.initially_valid = false; /* is_local: inherited CHECK is not local */
    c.is_no_inherit = false;
    c.location = 1; /* inhcount = 1 */
    alloc_in(mcx, Node::mk_constraint(mcx, c))
}

/// Build the `Node::Constraint` carrier for an inherited not-null constraint.
/// `location` carries the attnum (as the storage consumer reads it back).
fn make_notnull_carrier<'mcx>(
    mcx: Mcx<'mcx>,
    name: Option<&str>,
    attnum: AttrNumber,
) -> PgResult<NodePtr<'mcx>> {
    let mut c = empty_constraint(mcx, ConstrType::CONSTR_NOTNULL)?;
    c.conname = match name {
        Some(n) => Some(PgString::from_str_in(n, mcx)?),
        None => None,
    };
    c.location = attnum as i32;
    alloc_in(mcx, Node::mk_constraint(mcx, c))
}

fn empty_constraint<'mcx>(mcx: Mcx<'mcx>, contype: ConstrType) -> PgResult<Constraint<'mcx>> {
    Ok(Constraint {
        contype,
        conname: None,
        deferrable: false,
        initdeferred: false,
        is_enforced: true,
        skip_validation: false,
        initially_valid: true,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: PgVec::new_in(mcx),
        without_overlaps: false,
        including: PgVec::new_in(mcx),
        exclusions: PgVec::new_in(mcx),
        options: PgVec::new_in(mcx),
        indexname: None,
        indexspace: None,
        reset_default_tblspc: false,
        access_method: None,
        where_clause: None,
        pktable: None,
        fk_attrs: PgVec::new_in(mcx),
        pk_attrs: PgVec::new_in(mcx),
        fk_with_period: false,
        pk_with_period: false,
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: PgVec::new_in(mcx),
        old_conpfeqop: PgVec::new_in(mcx),
        old_pktable_oid: InvalidOid,
        location: 0,
    })
}

/// `&bogus_marker` (a static sentinel in C). We model the conflicting-default
/// marker as a distinguished empty `Constraint` node; `is_bogus_marker` tests
/// for it by identity-of-shape (contype `CONSTR_NULL`-style sentinel via
/// `location == BOGUS_DEFAULT_MARKER`).
const BOGUS_DEFAULT_MARKER: i32 = i32::MIN;

fn make_bogus_marker<'mcx>(mcx: Mcx<'mcx>) -> PgResult<NodePtr<'mcx>> {
    let mut c = empty_constraint(mcx, ConstrType::CONSTR_DEFAULT)?;
    c.location = BOGUS_DEFAULT_MARKER;
    alloc_in(mcx, Node::mk_constraint(mcx, c))
}

fn is_bogus_marker(node: Option<&Node<'_>>) -> bool {
    match node.and_then(|n| n.as_constraint()) {
        Some(c) => c.contype == ConstrType::CONSTR_DEFAULT && c.location == BOGUS_DEFAULT_MARKER,
        None => false,
    }
}

fn check_generated_conflicts<'mcx>(
    _mcx: Mcx<'mcx>,
    inh_generated: i8,
    newdef: &ColumnDef<'mcx>,
    colname: &str,
) -> PgResult<()> {
    if inh_generated != 0 {
        if newdef.raw_default.is_some() && newdef.generated == 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
                .errmsg(format!(
                    "column \"{colname}\" inherits from generated column but specifies default"
                ))
                .finish(here("MergeChildAttribute"))
                .map(|()| unreachable!());
        }
        if newdef.identity != 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
                .errmsg(format!(
                    "column \"{colname}\" inherits from generated column but specifies identity"
                ))
                .finish(here("MergeChildAttribute"))
                .map(|()| unreachable!());
        }
    } else if newdef.generated != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
            .errmsg(format!(
                "child column \"{colname}\" specifies generation expression"
            ))
            .errhint("A child table column cannot be generated unless its parent column is.")
            .finish(here("MergeChildAttribute"))
            .map(|()| unreachable!());
    }

    if inh_generated != 0 && newdef.generated != 0 && newdef.generated != inh_generated {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
            .errmsg(format!(
                "column \"{colname}\" inherits from generated column of different kind"
            ))
            .errdetail(format!(
                "Parent column is {}, child column is {}.",
                if inh_generated == ATTRIBUTE_GENERATED_STORED { "STORED" } else { "VIRTUAL" },
                if newdef.generated == ATTRIBUTE_GENERATED_STORED { "STORED" } else { "VIRTUAL" },
            ))
            .finish(here("MergeChildAttribute"))
            .map(|()| unreachable!());
    }

    Ok(())
}

fn type_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    which: &str,
    t1: Oid,
    m1: i32,
    t2: Oid,
    m2: i32,
) -> PgResult<()> {
    let s1 = format_type_with_typemod(mcx, t1, m1)?;
    let s2 = format_type_with_typemod(mcx, t2, m2)?;
    ereport(ERROR)
        .errcode(ERRCODE_DATATYPE_MISMATCH)
        .errmsg(format!("{which} \"{name}\" has a type conflict"))
        .errdetail(format!("{} versus {}", s1.as_str(), s2.as_str()))
        .finish(here("MergeAttributes"))
        .map(|()| unreachable!())
}

fn collation_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    which: &str,
    c1: Oid,
    c2: Oid,
) -> PgResult<()> {
    let n1 = get_collation_name(mcx, c1)?;
    let n2 = get_collation_name(mcx, c2)?;
    let n1 = n1.as_ref().map(|s| s.as_str()).unwrap_or("(null)");
    let n2 = n2.as_ref().map(|s| s.as_str()).unwrap_or("(null)");
    ereport(ERROR)
        .errcode(ERRCODE_COLLATION_MISMATCH)
        .errmsg(format!("{which} \"{name}\" has a collation conflict"))
        .errdetail(format!("\"{n1}\" versus \"{n2}\""))
        .finish(here("MergeAttributes"))
        .map(|()| unreachable!())
}

fn storage_conflict(name: &str, which: &str, s1: i8, s2: i8) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_DATATYPE_MISMATCH)
        .errmsg(format!("{which} \"{name}\" has a storage parameter conflict"))
        .errdetail(format!(
            "{} versus {}",
            crate::create::storage_name(s1),
            crate::create::storage_name(s2)
        ))
        .finish(here("MergeAttributes"))
        .map(|()| unreachable!())
}

fn compression_conflict(name: &str, _which: &str, c1: &str, c2: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_DATATYPE_MISMATCH)
        .errmsg(format!("column \"{name}\" has a compression method conflict"))
        .errdetail(format!("{c1} versus {c2}"))
        .finish(here("MergeAttributes"))
        .map(|()| unreachable!())
}
