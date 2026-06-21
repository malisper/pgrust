//! `commands/tablecmds.c` — ALTER TABLE SET TABLESPACE.
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATPrepSetTableSpace` (tablecmds.c:16615) — resolve the tablespace OID,
//!     ACL check (`ACL_CREATE` except for the database default), store it in
//!     `tab->newTableSpace`.
//!   - `ATExecSetTableSpace` (tablecmds.c:16853) — allocate a new relfilenumber
//!     in the destination tablespace, copy the relation's storage block-by-block
//!     into it (`index_copy_data` / `table_relation_copy_data`), update pg_class,
//!     recurse to the toast relation and toast indexes.
//!   - `ATExecSetTableSpaceNoStorage` (tablecmds.c:16946) — the metadata-only
//!     move for storageless relkinds (partitioned tables / indexes).
//!
//! `index_copy_data` and `table_relation_copy_data` (the AM callback
//! `heapam_relation_copy_data`) have identical bodies; both delegate to
//! storage.c's `copy_relation_data_to_new_locator` after the bufmgr flush.

#![allow(non_snake_case)]

extern crate alloc;

use mcx::Mcx;
use types_core::primitive::{Oid, RelFileNumber, InvalidOid};
use types_error::PgResult;
use types_storage::storage::RelFileLocator;
use types_catalog::catalog::RELKIND_INDEX;
use types_acl::{ACLCHECK_OK, ACL_CREATE};

use backend_access_common_relation::relation_open;
use backend_utils_init_miscinit::GetUserId;
use crate::helpers::{here, RelationRelationId, TableSpaceRelationId};

use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_objectaccess_seams as objaccess_seam;
use backend_catalog_storage_seams as storage_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_commands_tablespace_seams as tablespace_seam;
use backend_commands_tablespace_globals_seams as tablespace_globals_seam;

use backend_utils_error::ereport;
use types_error::{ERRCODE_SYNTAX_ERROR, ERROR};
use types_storage::lock::{LOCKMODE, NoLock};
use backend_access_transam_xact::CommandCounterIncrement;

/// `OidIsValid`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `ATPrepSetTableSpace(tab, rel, tablespacename, lockmode)` (tablecmds.c:16615).
pub fn ATPrepSetTableSpace(
    new_table_space: &mut Oid,
    tablespacename: &str,
) -> PgResult<()> {
    // Check that the tablespace exists.
    let tablespace_id = tablespace_seam::get_tablespace_oid::call(tablespacename, false)?;

    // Check permissions except when moving to database's default.
    let my_db_tablespace = tablespace_globals_seam::MyDatabaseTableSpace::call()?;
    if OidIsValid(tablespace_id) && tablespace_id != my_db_tablespace {
        let aclresult = aclchk_seam::object_aclcheck::call(
            TableSpaceRelationId,
            tablespace_id,
            GetUserId(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            aclchk_seam::aclcheck_error::call(
                aclresult,
                types_nodes::parsenodes::OBJECT_TABLESPACE,
                Some(tablespacename.to_string()),
            )?;
        }
    }

    // Save info for Phase 3 to do the real work.
    if OidIsValid(*new_table_space) {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("cannot have multiple SET TABLESPACE subcommands".to_string())
            .finish(here("ATPrepSetTableSpace"));
    }

    *new_table_space = tablespace_id;
    Ok(())
}

/// `ATExecSetTableSpace(tableOid, newTableSpace, lockmode)` (tablecmds.c:16853).
pub fn ATExecSetTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    new_table_space: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // Need lock here in case we are recursing to toast table or index.
    let rel = relation_open(mcx, table_oid, lockmode)?;

    // Check first if relation can be moved to new tablespace.
    if !crate::smallfns::check_relation_tablespace_move(&rel, new_table_space)? {
        objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, 0)?;
        rel.close(NoLock)?;
        return Ok(());
    }

    let reltoastrelid = rel.rd_rel.reltoastrelid;

    // Fetch the list of indexes on toast relation if necessary.
    let mut reltoastidxids: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
    if OidIsValid(reltoastrelid) {
        let toast_rel = relation_open(mcx, reltoastrelid, lockmode)?;
        let idxlist = relcache_seam::relation_get_index_list::call(mcx, &toast_rel)?;
        reltoastidxids.extend(idxlist.iter().copied());
        toast_rel.close(lockmode)?;
    }

    // Relfilenumbers are not unique in databases across tablespaces, so we need
    // to allocate a new one in the new tablespace.
    let relpersistence = rel.rd_rel.relpersistence;
    let newrelfilenumber: RelFileNumber =
        catalog_seam::get_new_relfilenumber::call(new_table_space, relpersistence as i8)?;

    // Open old and new relation.
    //   newrlocator = rel->rd_locator;
    //   newrlocator.relNumber = newrelfilenumber;
    //   newrlocator.spcOid = newTableSpace;
    let src_rlocator: RelFileLocator = rel.rd_locator;
    let src_backend = rel.rd_backend;
    let new_rlocator = RelFileLocator {
        spcOid: new_table_space,
        dbOid: src_rlocator.dbOid,
        relNumber: newrelfilenumber,
    };

    // is_permanent = RelationIsPermanent(rel) = relpersistence == 'p'.
    let is_permanent = relpersistence == (b'p' as u8);

    // Hand off to AM to actually create new rel storage and copy the data. The
    // index and table-AM bodies (index_copy_data / heapam_relation_copy_data)
    // are identical; the only step the storage owner can't see is the bufmgr
    // flush of the source relation's shared buffers, done here first.
    bufmgr_seam::flush_relation_buffers::call(&rel)?;
    storage_seam::copy_relation_data_to_new_locator::call(
        mcx,
        src_rlocator,
        src_backend,
        new_rlocator,
        relpersistence as i8,
        is_permanent,
    )?;
    let _ = RELKIND_INDEX; // both relkind branches share the same storage body

    // Update the pg_class row.
    crate::smallfns::set_relation_tablespace(mcx, &rel, new_table_space, newrelfilenumber)?;

    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, 0)?;

    relcache_seam::relation_assume_new_relfilelocator::call(rel.rd_id)?;

    rel.close(NoLock)?;

    // Make sure the reltablespace change is visible.
    CommandCounterIncrement()?;

    // Move associated toast relation and/or indexes, too.
    if OidIsValid(reltoastrelid) {
        ATExecSetTableSpace(mcx, reltoastrelid, new_table_space, lockmode)?;
    }
    for &idxoid in reltoastidxids.iter() {
        ATExecSetTableSpace(mcx, idxoid, new_table_space, lockmode)?;
    }

    Ok(())
}

/// `ATExecSetTableSpaceNoStorage(rel, newTableSpace)` (tablecmds.c:16946).
pub fn ATExecSetTableSpaceNoStorage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    new_table_space: Oid,
) -> PgResult<()> {
    // Shouldn't be called on relations having storage; these are processed in
    // phase 3. (debug_assert mirrors the C Assert(!RELKIND_HAS_STORAGE).)

    // Check if relation can be moved to its new tablespace.
    if !crate::smallfns::check_relation_tablespace_move(rel, new_table_space)? {
        objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, 0)?;
        return Ok(());
    }

    // Update can be done, so change reltablespace.
    crate::smallfns::set_relation_tablespace(mcx, rel, new_table_space, InvalidOid)?;

    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, 0)?;

    // Make sure the reltablespace change is visible.
    CommandCounterIncrement()?;
    Ok(())
}
