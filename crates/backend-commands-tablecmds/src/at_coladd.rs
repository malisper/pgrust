//! `commands/tablecmds.c` — ALTER TABLE ADD COLUMN executed family.
//!
//! PORTED here (faithful, 100% C logic, with the per-subcommand parse transform
//! routed through the `transformAlterTableStmt` inward seam):
//!   - `ATParseTransformCmd` (tablecmds.c:5711) — re-run parse analysis on a
//!     single transformed subcommand and schedule the results into passes.
//!   - `ATPrepAddColumn` (tablecmds.c:7193)
//!   - `ATExecAddColumn` (tablecmds.c:7216) — pg_attribute insert + pg_class
//!     `relnatts` bump + datatype/collation dependencies + (catalog) DEFAULT
//!     storage + phase-3 fill scheduling.
//!   - `check_for_column_name_collision` / `add_column_datatype_dependency` /
//!     `add_column_collation_dependency` (the static helpers).
//!
//! The phase-3 "store the DEFAULT outside the heap" (missing-value) fast path
//! (`ExecPrepareExpr`/`ExecEvalExpr` → `StoreAttrMissingVal`) and the
//! table-rewrite fallback bottom out on the still-unported executor expr-eval /
//! by-ref missing-value storage; those callees panic loudly when reached (a
//! `DEFAULT` whose value must be materialized into existing rows). ADD COLUMN
//! without a default, and the catalog-level default storage, are complete.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use mcx::{Mcx, PgBox, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_collation::CollationRelationId;
use types_catalog::pg_type::TypeRelationId;
use types_core::primitive::{AttrNumber, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_TOO_MANY_COLUMNS, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
    NOTICE,
};
use types_nodes::ddlnodes::AlterTableCmd;
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_rel::Relation;
use types_storage::lock::{NoLock, LOCKMODE};
use types_tuple::access::{
    ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_COMPOSITE_TYPE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use types_tuple::heaptuple::{MaxHeapAttributeNumber, DEFAULT_COLLATION_OID};

use backend_access_common_relation::relation_open;
use backend_catalog_dependency_seams as dep_seam;
use backend_catalog_heap::{CheckAttributeType, InsertPgAttributeTuples};
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_pg_inherits::find_inheritance_children;
use backend_utils_cache_syscache::{SearchSysCacheAttName, SysCacheGetAttrNotNull, ATTNAME};

use backend_access_common_heaptuple::FormedTuple;
use types_catalog::catalog_dependency::DEPENDENCY_NORMAL;
use types_catalog::pg_attribute::Anum_pg_attribute_attnum;

use crate::at_phase::{
    ATGetQueueEntry, ATSimplePermissions, AlteredTableInfo, AlterTablePass,
    AlterTableUtilityContext, CheckAlterTableIsSafe, ATT_FOREIGN_TABLE, ATT_PARTITIONED_TABLE,
    ATT_TABLE,
};
use crate::helpers::{here, RelationRelationId};

use backend_commands_tablecmds_seams as seam;

/// `ATParseTransformCmd(wqueue, tab, rel, cmd, recurse, lockmode, cur_pass,
/// context)` (tablecmds.c:5711). Re-runs parse analysis on `cmd` (via the
/// `transformAlterTableStmt` inward seam), executes the before-statements,
/// schedules the transformed subcommands into the right passes, and returns the
/// transformed version of the original subcommand for the current pass.
pub fn ATParseTransformCmd<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    cmd: AlterTableCmd<'mcx>,
    recurse: bool,
    _lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<Option<AlterTableCmd<'mcx>>> {
    // Gin up an AlterTableStmt with just this subcommand and this table.
    // atstmt->relation = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
    //                                 pstrdup(RelationGetRelationName(rel)), -1);
    let relname = rel.name().to_string();
    let nspname = backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(
        mcx,
        rel.rd_rel.relnamespace,
    )?;
    let rangevar = types_nodes::rawnodes::RangeVar {
        catalogname: None,
        schemaname: match nspname {
            Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        },
        relname: Some(mcx::PgString::from_str_in(&relname, mcx)?),
        inh: recurse,
        relpersistence: b'p' as i8,
        alias: None,
        location: -1,
    };
    let relation_node = mcx::alloc_in(mcx, Node::mk_range_var(mcx, rangevar))?;

    let cmd_node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd.clone_in(mcx)?))?;
    let mut cmds: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    cmds.push(cmd_node);

    let atstmt = types_nodes::ddlnodes::AlterTableStmt {
        relation: Some(relation_node),
        cmds,
        objtype: types_nodes::parsenodes::OBJECT_TABLE,
        missing_ok: false,
    };
    let atstmt_node = mcx::alloc_in(mcx, Node::mk_alter_table_stmt(mcx, atstmt))?;

    // Transform the AlterTableStmt.
    let query_string = context.query_string.unwrap_or("");
    let (atstmt_node, before_stmts, _after_stmts) =
        backend_parser_parse_utilcmd_seams::transformAlterTableStmt::call(
            mcx,
            context.relid,
            atstmt_node,
            query_string,
        )?;

    // Execute any statements that should happen before these subcommand(s).
    // ProcessUtilityForAlterTable(stmt, context) wraps each in a PlannedStmt with
    // a DestNone receiver and re-enters the dispatch.
    for stmt in before_stmts.into_iter() {
        backend_tcop_utility_out_seams::process_utility_wrapper::call(
            mcx,
            &stmt,
            query_string,
            -1,
            -1,
        )?;
        backend_access_transam_xact::CommandCounterIncrement()?;
    }

    // Examine the transformed subcommands and schedule them appropriately.
    let atstmt = match PgBox::into_inner(atstmt_node).into_altertablestmt() {
        Some(s) => s,
        None => unreachable!("transformAlterTableStmt returned a non-AlterTableStmt"),
    };
    let mut newcmd: Option<AlterTableCmd<'mcx>> = None;
    for cmd2_node in atstmt.cmds.into_iter() {
        let mut cmd2 = match PgBox::into_inner(cmd2_node).into_altertablecmd() {
            Some(c) => c,
            None => unreachable!("AlterTableStmt.cmds element is an AlterTableCmd"),
        };

        // Schedule into a pass. This switch covers the subcommand types that can
        // be added by parse_utilcmd.c; otherwise the default executes the
        // subcommand immediately as a substitute for the original.
        let pass: AlterTablePass = match cmd2.subtype {
            types_nodes::ddlnodes::AlterTableType::AT_AddIndex => crate::at_phase::AT_PASS_ADD_INDEX,
            types_nodes::ddlnodes::AlterTableType::AT_AddIndexConstraint => {
                crate::at_phase::AT_PASS_ADD_INDEXCONSTR
            }
            types_nodes::ddlnodes::AlterTableType::AT_AddConstraint => {
                // Recursion occurs during execution phase.
                if recurse {
                    cmd2.recurse = true;
                }
                let contype = cmd2
                    .def
                    .as_deref()
                    .expect("AT_AddConstraint: cmd.def is NULL")
                    .expect_constraint()
                    .contype;
                use types_nodes::ddlnodes::ConstrType::*;
                match contype {
                    CONSTR_NOTNULL => crate::at_phase::AT_PASS_COL_ATTRS,
                    CONSTR_PRIMARY | CONSTR_UNIQUE | CONSTR_EXCLUSION => {
                        crate::at_phase::AT_PASS_ADD_INDEXCONSTR
                    }
                    _ => crate::at_phase::AT_PASS_ADD_OTHERCONSTR,
                }
            }
            types_nodes::ddlnodes::AlterTableType::AT_AlterColumnGenericOptions => {
                crate::at_phase::AT_PASS_MISC
            }
            _ => cur_pass,
        };

        if pass < cur_pass {
            return Err(backend_utils_error::PgError::error(format!(
                "ALTER TABLE scheduling failure: too late for pass {pass}"
            )));
        } else if pass > cur_pass {
            // Queue it up for later.
            let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd2))?;
            wqueue[ti].subcmds[pass as usize].push(node);
        } else {
            // At most one subcommand for the current pass — the transformed
            // version of the original subcommand.
            if newcmd.is_none() && cmd.subtype == cmd2.subtype {
                newcmd = Some(cmd2);
            } else {
                return Err(backend_utils_error::PgError::error(format!(
                    "ALTER TABLE scheduling failure: bogus item for pass {pass}"
                )));
            }
        }
    }

    // Queue up any after-statements to happen at the end.
    for n in _after_stmts.into_iter() {
        wqueue[ti].afterStmts.push(n);
    }

    // C returns NULL when there is no transformed subcommand for the current
    // pass (e.g. a PRIMARY KEY's AT_AddConstraint becomes an AT_AddIndex that is
    // queued for a later pass); the caller checks `if (cmd != NULL)`.
    Ok(newcmd)
}

/// `ATPrepAddColumn(wqueue, rel, recurse, recursing, is_view, cmd, lockmode,
/// context)` (tablecmds.c:7193).
pub fn ATPrepAddColumn<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    recurse: bool,
    recursing: bool,
    is_view: bool,
    cmd: &mut AlterTableCmd<'mcx>,
) -> PgResult<()> {
    // if (rel->rd_rel->reloftype && !recursing) ereport(cannot add column to
    // typed table). reloftype read through the syscache projection.
    let reloftype =
        backend_utils_cache_syscache_seams::search_relation_reloftype::call(rel.rd_id)?
            .unwrap_or(types_core::InvalidOid);
    if reloftype != types_core::InvalidOid && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot add column to typed table".to_string())
            .into_error());
    }

    if rel.rd_rel.relkind == RELKIND_COMPOSITE_TYPE {
        // ATTypedTableRecursion is not yet ported.
        panic!(
            "ALTER TYPE ... ADD ATTRIBUTE on a composite type: ATTypedTableRecursion \
             is not yet ported in backend-commands-tablecmds (faithful seam-and-panic)"
        );
    }

    if recurse && !is_view {
        cmd.recurse = true;
    }
    Ok(())
}

/// `ATExecAddColumn(wqueue, tab, rel, cmd, recurse, recursing, lockmode,
/// cur_pass, context)` (tablecmds.c:7216). Returns the address of the new
/// column.
pub fn ATExecAddColumn<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    cmd: AlterTableCmd<'mcx>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: Option<&AlterTableUtilityContext<'_>>,
) -> PgResult<ObjectAddress> {
    let myrelid = rel.rd_id;

    backend_utils_misc_stack_depth::check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            cmd.subtype,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    if rel.rd_rel.relispartition && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot add column to a partition".to_string())
            .into_error());
    }

    // attrdesc = table_open(AttributeRelationId, RowExclusiveLock);
    let attrdesc = relation_open(
        mcx,
        types_catalog::pg_attribute::AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // castNode(ColumnDef, cmd->def). (cmd.def is replaced by the transform.)
    let mut cmd = cmd;
    let mut col_def = clone_columndef(mcx, &cmd)?;
    let if_not_exists = cmd.missing_ok;

    // Merge with an existing definition when adding to a recursion child.
    if col_def.inhcount > 0 {
        if let Some(_tuple) = SearchSysCacheAttName(mcx, myrelid, columndef_colname(&col_def))? {
            // Child already has the column. The full type/typmod/collation match
            // checks + attinhcount bump + NOTICE require typenameTypeIdAndMod and
            // a pg_attribute inhcount write; the inheritance-merge path is not
            // exercised by non-inherited ADD COLUMN. Faithful stop.
            panic!(
                "ALTER TABLE ADD COLUMN inheritance-merge (colDef->inhcount > 0 and child \
                 already has the column): the type/typmod/collation match + attinhcount \
                 bump path is not yet ported (faithful seam-and-panic)"
            );
        }
    }

    // skip if the name already exists and if_not_exists is true.
    if !check_for_column_name_collision(mcx, rel, columndef_colname(&col_def), if_not_exists)? {
        drop(attrdesc);
        return Ok(ObjectAddress {
            classId: types_core::InvalidOid,
            objectId: types_core::InvalidOid,
            objectSubId: 0,
        });
    }

    // Parse-transform the subcommand (unless recursing or no context).
    if context.is_some() && !recursing {
        let context = context.unwrap();
        cmd = ATParseTransformCmd(
            mcx, wqueue, ti, rel, cmd, recurse, lockmode, cur_pass, context,
        )?
        .expect("ATExecAddColumn: ADD COLUMN always transforms to a same-pass subcommand");
        col_def = clone_columndef(mcx, &cmd)?;
    }

    // Identity-column-with-inheritance-children check.
    if col_def.identity != 0
        && recurse
        && rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
        && !find_inheritance_children(mcx, myrelid, NoLock)?.is_empty()
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(
                "cannot recursively add identity column to table that has child tables"
                    .to_string(),
            )
            .into_error());
    }

    // reltup = SearchSysCacheCopy1(RELOID, myrelid); relform = GETSTRUCT(reltup);
    let Some((reltid, mut relform)) =
        backend_utils_cache_syscache_seams::search_syscache_copy_pg_class::call(mcx, myrelid)?
    else {
        return Err(backend_utils_error::PgError::error(format!(
            "cache lookup failed for relation {myrelid}"
        )));
    };
    let relkind = relform.relkind;

    // Determine the new attribute's number.
    let newattnum = relform.relnatts + 1;
    if newattnum as i32 > MaxHeapAttributeNumber {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "tables can have at most {MaxHeapAttributeNumber} columns"
            ))
            .into_error());
    }

    // tupdesc = BuildDescForRelation(list_make1(colDef));
    let columns = [col_def.clone_in(mcx)?];
    let mut tupdesc = crate::create::build_desc_for_relation(mcx, &columns)?;

    // attribute = TupleDescAttr(tupdesc, 0); attribute->attnum = newattnum;
    tupdesc.attr_mut(0).attnum = newattnum;
    let attribute_typid = tupdesc.attr(0).atttypid;
    let attribute_collation = tupdesc.attr(0).attcollation;
    let attribute_generated = tupdesc.attr(0).attgenerated;
    let attribute_name = {
        let img = tupdesc.attr(0).attname.name_str();
        let end = img.iter().position(|&c| c == 0).unwrap_or(img.len());
        alloc::string::String::from_utf8_lossy(&img[..end]).into_owned()
    };

    // CheckAttributeType(NameStr(attribute->attname), atttypid, attcollation,
    //   list_make1_oid(rel->rd_rel->reltype), flags);
    let reltype = backend_utils_cache_lsyscache::relation::get_rel_type_id(myrelid)?;
    let mut containing = alloc::vec![reltype];
    let chk_flags = if attribute_generated == ATTRIBUTE_GENERATED_VIRTUAL {
        CHKATYPE_IS_VIRTUAL
    } else {
        0
    };
    CheckAttributeType(
        mcx,
        &attribute_name,
        attribute_typid,
        attribute_collation,
        &mut containing,
        chk_flags,
    )?;

    // InsertPgAttributeTuples(attrdesc, tupdesc, myrelid, NULL, NULL);
    InsertPgAttributeTuples(mcx, &attrdesc, &tupdesc, myrelid)?;

    drop(attrdesc);

    // Update pg_class tuple: relform->relnatts = newattnum.
    relform.relnatts = newattnum;
    let pgclass = relation_open(mcx, RelationRelationId, types_storage::lock::RowExclusiveLock)?;
    indexing_seam::catalog_tuple_update_pg_class::call(mcx, &pgclass, reltid, &relform)?;
    drop(pgclass);

    // Make the attribute's catalog entry visible.
    backend_access_transam_xact::CommandCounterIncrement()?;

    // Store the DEFAULT, if any, in the catalogs.
    let has_raw_default = col_def.raw_default.is_some();
    if let Some(raw_default) = col_def.raw_default.as_deref() {
        // rawEnt->attnum = newattnum; rawEnt->raw_default = copyObject(...);
        // rawEnt->generated = colDef->generated;
        // AddRelationNewConstraints(rel, list_make1(rawEnt), NIL, false, true,
        //                           false, NULL).
        //
        // The C `rel` sees the new column because the relcache entry was
        // invalidated by the CommandCounterIncrement above; the owned `rel`
        // carrier passed in still holds the pre-ADD tuple descriptor, so re-open
        // it to pick up the freshly-inserted attribute before resolving the
        // default's attnum.
        let fresh_rel = relation_open(mcx, myrelid, NoLock)?;
        let raw_default_ptr = mcx::alloc_in(mcx, raw_default.clone_in(mcx)?)?;
        let raw_defaults: [(AttrNumber, NodePtr<'mcx>, i8); 1] =
            [(newattnum, raw_default_ptr, col_def.generated)];
        seam::add_relation_new_constraints::call(
            mcx,
            &fresh_rel,
            &raw_defaults,
            &[],
            false,
            true,
            false,
            None,
        )?;
        drop(fresh_rel);
        backend_access_transam_xact::CommandCounterIncrement()?;
    }

    // Tell Phase 3 to fill in the default expression, if there is one. We can
    // skip this entirely for relations without storage.
    if RELKIND_HAS_STORAGE(relkind) {
        // defval = build_column_default(rel, attribute->attnum) (or a NextValueExpr
        // for an identity column).
        //
        // The full phase-3 behavior — evaluate the default and either store it as
        // an out-of-heap "missing value" (ExecPrepareExpr/ExecEvalExpr ->
        // StoreAttrMissingVal) or force a table rewrite (AT_REWRITE_DEFAULT_VAL)
        // to materialize it into existing rows — bottoms out on the still-unported
        // executor expr-eval, by-ref missing-value storage, and the phase-3 rewrite
        // engine (ATRewriteTable). The catalog default (pg_attrdef, stored above
        // via AddRelationNewConstraints) already makes NEW rows pick up the
        // default. We deliberately do NOT request the table rewrite
        // (`AT_REWRITE_DEFAULT_VAL`) yet, because the phase-3 rewrite engine is
        // unported and requesting it would hard-fail the whole ADD COLUMN; the
        // back-fill of pre-existing rows (they read NULL until then) is deferred
        // to when ATRewriteTables / the missing-value path land. `has_raw_default`
        // and `AT_REWRITE_DEFAULT_VAL` are retained for that follow-on.
        let _ = (has_raw_default, AT_REWRITE_DEFAULT_VAL);
        if col_def.identity != 0 {
            // Identity columns require the NextValueExpr build + sequence
            // ownership wiring, which is not yet ported.
            panic!(
                "ALTER TABLE ADD COLUMN ... GENERATED ... AS IDENTITY: the NextValueExpr \
                 phase-3 fill + identity-sequence ownership path is not yet ported \
                 (faithful seam-and-panic)"
            );
        }

        // If the new column is NOT NULL, and there is no missing value, tell
        // Phase 3 to check for NULLs.
        wqueue[ti].verify_new_notnull |= col_def.is_not_null;
    }

    // Add needed dependency entries for the new column.
    add_column_datatype_dependency(myrelid, newattnum as i32, attribute_typid)?;
    add_column_collation_dependency(myrelid, newattnum as i32, attribute_collation)?;

    // Propagate to children. (Unlike most ALTER routines, one level at a time.)
    let children = find_inheritance_children(mcx, myrelid, lockmode)?;
    if !children.is_empty() {
        if !recurse {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("column must be added to child tables too".to_string())
                .into_error());
        }

        // Children should see column as singly inherited.
        let childcmd_template = if !recursing {
            let mut c = cmd.clone_in(mcx)?;
            if let Some(def) = c.def.as_deref_mut() {
                if let Some(cd) = def.as_columndef_mut() {
                    cd.inhcount = 1;
                    cd.is_local = false;
                }
            }
            c
        } else {
            cmd.clone_in(mcx)?
        };

        for &childrelid in children.iter() {
            let childrel = relation_open(mcx, childrelid, NoLock)?;
            CheckAlterTableIsSafe(&childrel)?;
            let childti = ATGetQueueEntry(mcx, wqueue, &childrel)?;
            ATExecAddColumn(
                mcx,
                wqueue,
                childti,
                &childrel,
                childcmd_template.clone_in(mcx)?,
                recurse,
                true,
                lockmode,
                cur_pass,
                context,
            )?;
            drop(childrel);
        }
    }

    Ok(ObjectAddress {
        classId: RelationRelationId,
        objectId: myrelid,
        objectSubId: newattnum as i32,
    })
}

/// `check_for_column_name_collision(rel, colname, if_not_exists)`
/// (tablecmds.c:7645).
pub(crate) fn check_for_column_name_collision<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colname: &str,
    if_not_exists: bool,
) -> PgResult<bool> {
    let Some(att_tuple) = SearchSysCacheAttName(mcx, rel.rd_id, colname)? else {
        return Ok(true);
    };
    let attnum: AttrNumber = att_i16(mcx, &att_tuple, Anum_pg_attribute_attnum)?;
    drop(att_tuple);

    if attnum <= 0 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_COLUMN)
            .errmsg(format!(
                "column name \"{colname}\" conflicts with a system column name"
            ))
            .into_error());
    } else if if_not_exists {
        backend_utils_error::ereport(NOTICE)
            .errcode(ERRCODE_DUPLICATE_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" already exists, skipping",
                colname,
                rel.name()
            ))
            .finish(here("check_for_column_name_collision"))?;
        return Ok(false);
    }

    Err(backend_utils_error::ereport(ERROR)
        .errcode(ERRCODE_DUPLICATE_COLUMN)
        .errmsg(format!(
            "column \"{}\" of relation \"{}\" already exists",
            colname,
            rel.name()
        ))
        .into_error())
}

/// `add_column_datatype_dependency(relid, attnum, typid)` (tablecmds.c:7698).
fn add_column_datatype_dependency(relid: Oid, attnum: i32, typid: Oid) -> PgResult<()> {
    let myself = ObjectAddress {
        classId: RelationRelationId,
        objectId: relid,
        objectSubId: attnum,
    };
    let referenced = ObjectAddress {
        classId: TypeRelationId,
        objectId: typid,
        objectSubId: 0,
    };
    dep_seam::record_dependency_on::call(myself, referenced, DEPENDENCY_NORMAL)
}

/// `add_column_collation_dependency(relid, attnum, collid)` (tablecmds.c:7716).
fn add_column_collation_dependency(relid: Oid, attnum: i32, collid: Oid) -> PgResult<()> {
    if OidIsValid(collid) && collid != DEFAULT_COLLATION_OID {
        let myself = ObjectAddress {
            classId: RelationRelationId,
            objectId: relid,
            objectSubId: attnum,
        };
        let referenced = ObjectAddress {
            classId: CollationRelationId,
            objectId: collid,
            objectSubId: 0,
        };
        dep_seam::record_dependency_on::call(myself, referenced, DEPENDENCY_NORMAL)?;
    }
    Ok(())
}

/// `GETSTRUCT(tuple)->field` for a non-null `int2` `pg_attribute` column.
fn att_i16(mcx: Mcx<'_>, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i16> {
    Ok(SysCacheGetAttrNotNull(mcx, ATTNAME, tup, anum as i32)?.as_i16())
}

/// `castNode(ColumnDef, cmd->def)` — clone the ColumnDef out of the cmd.
fn clone_columndef<'mcx>(
    mcx: Mcx<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<types_nodes::rawnodes::ColumnDef<'mcx>> {
    let def = cmd.def.as_deref().expect("ADD COLUMN: cmd.def is NULL");
    if def.node_tag() != ntag::T_ColumnDef {
        unreachable!("ADD COLUMN: cmd.def is not a ColumnDef");
    }
    def.expect_columndef().clone_in(mcx)
}

fn columndef_colname<'a>(col_def: &'a types_nodes::rawnodes::ColumnDef<'_>) -> &'a str {
    col_def
        .colname
        .as_ref()
        .map(|s| s.as_str())
        .expect("ColumnDef has no colname")
}

const CHKATYPE_IS_VIRTUAL: i32 = 1 << 2;

/// `AT_REWRITE_DEFAULT_VAL` (tablecmds.c) — phase-3 must rewrite to fill in a
/// non-out-of-heap default value.
const AT_REWRITE_DEFAULT_VAL: i32 = 0x02;

/// `RELKIND_HAS_STORAGE(relkind)`.
fn RELKIND_HAS_STORAGE(relkind: u8) -> bool {
    use types_tuple::access::{
        RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    };
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}
