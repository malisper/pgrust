//! `commands/tablecmds.c` — ALTER TABLE per-column executed families dispatched
//! from [`crate::at_phase::ATExecCmd`].
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATExecColumnDefault` (tablecmds.c:8126) — ALTER COLUMN SET / DROP DEFAULT
//!   - `ATExecCookedColumnDefault` (tablecmds.c:8210) — add a pre-cooked default
//!
//! SEAM-AND-PANIC (faithful) — the column-attribute mutating families
//! (`ATExecSetStatistics` / `ATExecSetOptions` / `ATExecSetStorage`) and the
//! relation-level `ATExecSetRelOptions`. These C routines do
//! `attTup = GETSTRUCT(syscache_copy_tuple); attTup->field = x;
//! CatalogTupleUpdate(...)` (or the `heap_modify_tuple(repl_val/null/repl)`
//! variant building a fresh `text[]`/`int` Datum). The repo's safe model has no
//! `Form_pg_attribute` GETSTRUCT field-mutation path and no typed pg_attribute /
//! pg_class update-row helper; faithfully landing them requires building the
//! `heap_deform_tuple` + per-`Anum` `Datum` (re)assembly + `heap_modify_tuple`
//! machinery (and, for STORAGE, the `SetIndexStorageProperties` index recursion;
//! for relOPTIONS, the VIEW `check_option` validation and toast-table
//! recursion). Those are mirrored here as loud stops rather than partial /
//! restructured bodies — see the per-fn rationale.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{AttrNumber, InvalidAttrNumber};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN, ERROR};
use types_nodes::ddlnodes::AlterTableType;
use types_nodes::nodes::Node;
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_nodes::parsenodes::DROP_RESTRICT;
use types_tuple::access::ATTRIBUTE_GENERATED_STORED;

use backend_catalog_pg_attrdef::{RemoveAttrDefault, StoreAttrDefault};
use backend_utils_cache_lsyscache::attribute::get_attnum;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, RelationRelationId};

/// `ObjectAddressSubSet(addr, class, object, sub)`.
fn object_address_subset(class_id: types_core::Oid, object_id: types_core::Oid, sub: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub,
    }
}

/// Faithful seam-and-panic for an unported column-attribute family. See module
/// docs for why these are not yet landed.
fn unported(what: &str) -> ! {
    panic!(
        "ALTER TABLE: {what} is not yet ported in backend-commands-tablecmds \
         (faithful seam-and-panic — needs the pg_attribute/pg_class \
         heap_deform_tuple + per-Anum Datum + heap_modify_tuple write path; \
         see at_column.rs)"
    );
}

// ===========================================================================
// ATExecColumnDefault (tablecmds.c:8126) — ALTER COLUMN SET / DROP DEFAULT
// ===========================================================================

/// `ATExecColumnDefault(rel, colName, newDefault, lockmode)` (tablecmds.c:8126).
/// `newDefault == NULL` is DROP DEFAULT; otherwise SET DEFAULT.
pub fn ATExecColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    newDefault: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // attnum = get_attnum(RelationGetRelid(rel), colName);
    let attnum = get_attnum(rel.rd_id, colName)?;
    if attnum == InvalidAttrNumber {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                colName,
                rel.name()
            ))
            .finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    // Prevent them from altering a system attribute.
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colName}\""))
            .finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    let att = rel.rd_att.attr((attnum - 1) as usize);

    if att.attidentity != 0 {
        // column is an identity column
        let mut b = backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is an identity column",
                colName,
                rel.name()
            ));
        if newDefault.is_none() {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY"
            ));
        }
        return b.finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    if att.attgenerated != 0 {
        // column is a generated column
        let mut b = backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is a generated column",
                colName,
                rel.name()
            ));
        if newDefault.is_some() {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION"
            ));
        } else if att.attgenerated == ATTRIBUTE_GENERATED_STORED {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION"
            ));
        }
        return b.finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    // Remove any old default for the column. RESTRICT for safety. Treated as an
    // internal op when preparatory to adding a new default, else user-initiated.
    // RemoveAttrDefault(relid, attnum, DROP_RESTRICT, false, newDefault != NULL);
    RemoveAttrDefault(
        rel.rd_id,
        attnum,
        DROP_RESTRICT,
        false,
        newDefault.is_some(),
    )?;

    if let Some(new_default) = newDefault {
        // SET DEFAULT: build one RawColumnDefault and run AddRelationNewConstraints.
        //   rawEnt->attnum = attnum; rawEnt->raw_default = newDefault;
        //   rawEnt->generated = '\0';
        //   AddRelationNewConstraints(rel, list_make1(rawEnt), NIL,
        //                             false, true, false, NULL);
        let raw_default_ptr = mcx::alloc_in(mcx, new_default.clone_in(mcx)?)?;
        let raw_defaults: [(AttrNumber, types_nodes::nodes::NodePtr<'mcx>, i8); 1] =
            [(attnum, raw_default_ptr, 0)];
        seam::add_relation_new_constraints::call(
            mcx,
            rel,
            &raw_defaults,
            &[],
            false,
            true,
            false,
            None,
        )?;
    }

    // ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

// ===========================================================================
// ATExecCookedColumnDefault (tablecmds.c:8210) — add a pre-cooked default
// ===========================================================================

/// `ATExecCookedColumnDefault(rel, attnum, newDefault)` (tablecmds.c:8210).
pub fn ATExecCookedColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: i16,
    newDefault: &Node<'mcx>,
) -> PgResult<ObjectAddress> {
    // We assume no checking is required.

    // Remove any old default for the column. RESTRICT for safety; internal op.
    // RemoveAttrDefault(relid, attnum, DROP_RESTRICT, false, true);
    RemoveAttrDefault(rel.rd_id, attnum, DROP_RESTRICT, false, true)?;

    // (void) StoreAttrDefault(rel, attnum, newDefault, true);
    let _ = StoreAttrDefault(mcx, rel.rd_id, attnum, newDefault, true)?;

    // ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

// ===========================================================================
// Unported column-attribute / relation-option families (faithful seam-panic)
// ===========================================================================

/// `ATExecSetStatistics` (tablecmds.c:8906). See module docs.
pub fn ATExecSetStatistics<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _colName: Option<&str>,
    _colNum: i16,
    _newValue: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported("ALTER COLUMN SET STATISTICS (pg_attribute attstattarget heap_modify_tuple)");
}

/// `ATExecSetOptions` (tablecmds.c:9050). See module docs.
pub fn ATExecSetOptions<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _colName: &str,
    _options: Option<&Node<'mcx>>,
    _isReset: bool,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported("ALTER COLUMN SET/RESET OPTIONS (pg_attribute attoptions transformRelOptions + heap_modify_tuple)");
}

/// `ATExecSetStorage` (tablecmds.c:9192). See module docs.
pub fn ATExecSetStorage<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _colName: &str,
    _newValue: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported("ALTER COLUMN SET STORAGE (pg_attribute attstorage GETSTRUCT-mutate + SetIndexStorageProperties)");
}

/// `ATExecSetRelOptions` (tablecmds.c:16645). See module docs.
pub fn ATExecSetRelOptions<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _def_list: Vec<backend_access_common_reloptions::DefElem>,
    _operation: AlterTableType,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported("SET/RESET/REPLACE relOPTIONS (pg_class reloptions transformRelOptions + heap_modify_tuple + VIEW check_option + toast recursion)");
}
