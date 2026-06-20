//! `commands/tablecmds.c` — the ALTER TABLE DROP CONSTRAINT and VALIDATE
//! CONSTRAINT subcommand families.
//!
//! Ported faithfully from PostgreSQL 18.3:
//!   - `ATExecDropConstraint` (tablecmds.c:14012) + `dropconstraint_internal`
//!     (tablecmds.c:14075) — find the target `pg_constraint` row and drop it
//!     (and its dependent objects) via `performDeletion`.
//!   - `ATExecValidateConstraint` (tablecmds.c:12908) + the three
//!     `Queue{FK,Check,NN}ConstraintValidation` helpers — queue a phase-3
//!     verification scan for a NOT VALID constraint and flip its
//!     `convalidated` flag.
//!   - `validateForeignKeyConstraint` (tablecmds.c:13694) — validate an FK by a
//!     full table scan (single LEFT JOIN via `RI_Initial_Check`, else firing
//!     `RI_FKey_check_ins` per row).
//!
//! The partition / multi-level-inheritance recursion legs and the NOT NULL
//! attnotnull-reset guards on DROP are faithfully seam-and-panicked where their
//! supporting substrate (one-level child recursion of a `dropconstraint_internal`
//! over inherited CHECK/NOT NULL rows; the pg_attribute attnotnull clear) is not
//! reachable from this crate.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{AttributeRelationId, PgAttributeUpdateRow};
use types_catalog::pg_constraint::{
    CONSTRAINT_CHECK, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::parsenodes::DropBehavior;
use types_rel::Relation;
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, LOCKMODE, NoLock, RowExclusiveLock,
};

use backend_access_common_relation::relation_open;
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_objectaddress::consts::ConstraintRelationId;
use backend_catalog_pg_inherits::{find_all_inheritors, find_inheritance_children};
use backend_utils_cache_lsyscache_seams::get_attname;
use backend_utils_cache_relcache::derived::{IndexAttrBitmapKind, RelationGetIndexAttrBitmap};
use backend_utils_misc_stack_depth::check_stack_depth;

/// `FirstLowInvalidHeapAttributeNumber` (`access/sysattr.h`) = -7. The offset
/// applied to attribute numbers stored in the index-attribute bitmaps.
const FirstLowInvalidHeapAttributeNumber: i32 = -7;

/// `bms_is_member(x, set)` over the `Vec<i32>` offset-member bitmap
/// representation `RelationGetIndexAttrBitmap` returns.
fn bms_is_member(x: i32, set: &[i32]) -> bool {
    set.contains(&x)
}

use crate::at_phase::{
    AlteredTableInfo, ATGetQueueEntry, ATSimplePermissions, CheckAlterTableIsSafe, NewConstraint,
};
use crate::helpers::here;

const ATT_TABLE: i32 = 1 << 0;
const ATT_PARTITIONED_TABLE: i32 = 1 << 4;
const ATT_FOREIGN_TABLE: i32 = 1 << 6;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `NewConstraint.contype` is the `ConstrType` enum-as-i32; the FK value the
/// validate-queue uses.
const CONSTR_FOREIGN_I32: i32 = types_nodes::ddlnodes::ConstrType::CONSTR_FOREIGN as i32;

fn unported(what: &str) -> ! {
    panic!(
        "{what} is not yet ported in backend-commands-tablecmds (faithful seam-and-panic)"
    );
}

// ===========================================================================
// ATExecDropConstraint (tablecmds.c:14012)
// ===========================================================================

/// `ATExecDropConstraint(rel, constrName, behavior, recurse, missing_ok,
/// lockmode)` (tablecmds.c:14012) — find and drop the target constraint.
pub fn ATExecDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    behavior: DropBehavior,
    recurse: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // conrel = table_open(ConstraintRelationId, RowExclusiveLock);
    // scan for (conrelid = rel, contypid = Invalid, conname = constrName).
    // There can be at most one matching row.
    let conoid = backend_catalog_pg_constraint::get_relation_constraint_oid(
        mcx,
        rel.rd_id,
        constr_name,
        true, // missing_ok at this level — we issue our own NOTICE/ERROR below
    )?;

    if !OidIsValid(conoid) {
        // not found
        if !missing_ok {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of relation \"{}\" does not exist",
                    rel.name()
                ))
                .into_error());
        } else {
            return backend_utils_error::ereport(NOTICE)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of relation \"{}\" does not exist, skipping",
                    rel.name()
                ))
                .finish(here("ATExecDropConstraint"));
        }
    }

    dropconstraint_internal(mcx, rel, conoid, behavior, recurse, false, missing_ok, lockmode)?;

    Ok(())
}

// ===========================================================================
// dropconstraint_internal (tablecmds.c:14075)
// ===========================================================================

/// `dropconstraint_internal(rel, constraintTup, behavior, recurse, recursing,
/// missing_ok, lockmode)` (tablecmds.c:14075) — remove a constraint, given its
/// `pg_constraint` OID (we re-fetch the form by OID rather than carry the heap
/// tuple, since the driver lives above the deform substrate).
pub fn dropconstraint_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    conoid: Oid,
    behavior: DropBehavior,
    _recurse: bool,
    recursing: bool,
    _missing_ok: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Guard against stack overflow due to overly deep inheritance tree.
    check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            types_nodes::ddlnodes::AlterTableType::AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    // con = GETSTRUCT(constraintTup);  constrName = NameStr(con->conname);
    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    // Don't allow drop of inherited constraints.
    if conform.coninhcount > 0 && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!(
                "cannot drop inherited constraint \"{constrname}\" of relation \"{}\"",
                rel.name(),
                constrname = conform.conname_str()
            ))
            .into_error());
    }

    // colname is used by the inheritance-recursion step (C carries it across the
    // child loop). We resolve it from the not-null constraint's sole conkey
    // column for the NOT NULL case; CHECK recursion is seam-panicked below.
    let mut colname: Option<String> = None;

    // Reset pg_constraint.attnotnull, if this is a not-null constraint.
    //
    // While doing that, we're in a good position to disallow dropping a not-
    // null constraint underneath a primary key, a replica identity index, or
    // a generated identity column.
    if conform.contype == CONSTRAINT_NOTNULL {
        // attrel = table_open(AttributeRelationId, RowExclusiveLock);
        let attrel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

        // attnum = extractNotNullColumn(constraintTup): the sole conkey member.
        // The scalar `search_constraint_form_by_oid` projection above does not
        // carry the `conkey` array, so re-fetch the full tuple by OID and run the
        // shared extractor (which reads conkey via the syscache and validates the
        // 1-D smallint[1] shape).
        let con_tup = backend_utils_cache_syscache_seams::search_constraint_tuple_by_oid::call(
            mcx, conoid,
        )?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
        let attnum: AttrNumber = backend_catalog_pg_constraint::extractNotNullColumn(&con_tup)?;

        // save column name for recursion step
        // colname = get_attname(RelationGetRelid(rel), attnum, false);
        colname = Some(
            get_attname::call(mcx, rel.rd_id, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default(),
        );

        // Disallow if it's in the primary key.  For partitioned tables we
        // cannot rely solely on RelationGetIndexAttrBitmap, because it'll
        // return NULL if the primary key is invalid; but we still need to
        // protect not-null constraints under such a constraint, so check the
        // slow way.
        // pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);
        let pkattrs = RelationGetIndexAttrBitmap(rel.rd_id, IndexAttrBitmapKind::PrimaryKey)?;

        // C's slow-way fallback for an *invalid* primary key on a partitioned
        // table reads the full pk index's `indkey.values[]`; that array is not
        // carried on the trimmed FormData_pg_index here (out-of-lane carrier
        // widen). The fast path (valid PK) is fully covered by the bitmap.
        if pkattrs.is_empty() && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            let pkindex = backend_utils_cache_relcache::derived::RelationGetPrimaryKeyIndex(
                rel.rd_id, true,
            )?;
            if OidIsValid(pkindex) {
                unported(
                    "DROP NOT NULL under an invalid primary key on a partitioned table (RelationGetPrimaryKeyIndex indkey slow-path; pg_index.indkey array carrier)",
                );
            }
        }

        // if (pkattrs && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs))
        if bms_is_member(attnum as i32 - FirstLowInvalidHeapAttributeNumber, &pkattrs) {
            let aname = get_attname::call(mcx, rel.rd_id, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!("column \"{aname}\" is in a primary key"))
                .into_error());
        }

        // Disallow if it's in the replica identity.
        // irattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
        let irattrs = RelationGetIndexAttrBitmap(rel.rd_id, IndexAttrBitmapKind::Identity)?;
        if bms_is_member(attnum as i32 - FirstLowInvalidHeapAttributeNumber, &irattrs) {
            let aname = get_attname::call(mcx, rel.rd_id, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "column \"{aname}\" is in index used as replica identity"
                ))
                .into_error());
        }

        // Disallow if it's a GENERATED AS IDENTITY column. The live relcache
        // descriptor carries attidentity (C reads it from a fresh syscache copy;
        // the live form is equivalent here).
        let att = rel.rd_att.attr((attnum - 1) as usize);
        if att.attidentity != 0 {
            let aname = get_attname::call(mcx, rel.rd_id, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "column \"{aname}\" of relation \"{}\" is an identity column",
                    rel.name()
                ))
                .into_error());
        }

        // All good -- reset attnotnull if needed.
        if att.attnotnull {
            // attForm->attnotnull = false; CatalogTupleUpdate(attrel, &atttup->t_self, atttup);
            let tuple = backend_utils_cache_syscache::SearchSysCacheAttNum(mcx, rel.rd_id, attnum)?
                .ok_or_else(|| {
                    backend_utils_error::ereport(ERROR)
                        .errmsg_internal(format!(
                            "cache lookup failed for attribute {attnum} of relation {}",
                            rel.rd_id
                        ))
                        .into_error()
                })?;
            let row = PgAttributeUpdateRow {
                attnotnull: Some(false),
                ..Default::default()
            };
            indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrel, &tuple, &row)?;
        }

        // table_close(attrel, RowExclusiveLock): RAII drop.
        drop(attrel);
    }

    let is_no_inherit_constraint = conform.connoinherit;

    // If it's a foreign-key constraint, we'd better lock the referenced table
    // and check that that's not in use (unless self-referential).
    if conform.contype == CONSTRAINT_FOREIGN && conform.confrelid != rel.rd_id {
        // Must match lock taken by RemoveTriggerById:
        let frel = relation_open(mcx, conform.confrelid, AccessExclusiveLock)?;
        CheckAlterTableIsSafe(&frel)?;
        frel.close(NoLock)?;
    }

    // Perform the actual constraint deletion.
    let conobj = ObjectAddress {
        classId: ConstraintRelationId,
        objectId: conform.oid,
        objectSubId: 0,
    };
    backend_catalog_dependency_seams::perform_deletion::call(
        conobj.classId,
        conobj.objectId,
        conobj.objectSubId,
        behavior,
        0,
    )?;

    // For partitioned tables, non-CHECK, non-NOT-NULL inherited constraints are
    // dropped via the dependency mechanism, so we're done here.
    if conform.contype != CONSTRAINT_CHECK
        && conform.contype != CONSTRAINT_NOTNULL
        && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
    {
        return Ok(conobj);
    }

    // Propagate to children as appropriate. CHECK is the only type (besides
    // NOT NULL, handled above) that can be inherited here. For a leaf table
    // (no children) this loop is empty; the multi-level inheritance recursion
    // over inherited CHECK rows needs a by-name/by-OID child constraint lookup
    // that is not reachable from this crate, so we faithfully seam-and-panic
    // when children exist.
    let children = if !is_no_inherit_constraint {
        find_inheritance_children(mcx, rel.rd_id, lockmode)?
    } else {
        PgVec::new_in(mcx)
    };

    if !children.is_empty() {
        // C carries `colname` (resolved above for NOT NULL) into the per-child
        // recursion; that one-level-at-a-time child loop is not reachable here.
        let _ = &colname;
        unported("DROP CONSTRAINT recursion to inheritance children (dropconstraint_internal child loop)");
    }

    Ok(conobj)
}

// ===========================================================================
// ATExecDropNotNull (tablecmds.c:7546)
// ===========================================================================

/// `ATExecDropNotNull(rel, colName, recurse, lockmode)` (tablecmds.c) — ALTER
/// COLUMN DROP NOT NULL. Look up the column, run the partition-parent /
/// identity-column guards, then drop the not-null `pg_constraint` row via
/// [`dropconstraint_internal`] (which resets `attnotnull` and runs the
/// primary-key / replica-identity guards).
pub fn ATExecDropNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    col_name: &str,
    recurse: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    let attr_rel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    // We resolve attnum via the live descriptor (equivalent to the syscache
    // attname lookup): a column with this name must exist.
    let attnum: AttrNumber =
        backend_utils_cache_lsyscache_seams::get_attnum::call(rel.rd_id, col_name)?;
    if attnum == 0 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{col_name}\" of relation \"{}\" does not exist",
                rel.name()
            ))
            .into_error());
    }

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    let address = ObjectAddress {
        classId: backend_catalog_objectaddress::consts::RelationRelationId,
        objectId: rel.rd_id,
        objectSubId: attnum as i32,
    };

    let att = rel.rd_att.attr((attnum - 1) as usize);

    // If the column is already nullable there's nothing to do.
    if !att.attnotnull {
        drop(attr_rel);
        return Ok(ObjectAddress {
            classId: InvalidOid,
            objectId: InvalidOid,
            objectSubId: 0,
        });
    }

    // Prevent them from altering a system attribute.
    if attnum <= 0 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{col_name}\""))
            .into_error());
    }

    // if (attTup->attidentity) ereport(...identity column...)
    if att.attidentity != 0 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{col_name}\" of relation \"{}\" is an identity column",
                rel.name()
            ))
            .into_error());
    }

    // If rel is partition, shouldn't drop NOT NULL if parent has the same.
    if rel.rd_rel.relispartition {
        let parent_id = backend_catalog_partition_seams::get_partition_parent::call(rel.rd_id, false)?;
        let parent = relation_open(mcx, parent_id, AccessShareLock)?;
        let parent_attnum =
            backend_utils_cache_lsyscache_seams::get_attnum::call(parent_id, col_name)?;
        let parent_att = parent.rd_att.attr((parent_attnum - 1) as usize);
        let parent_notnull = parent_att.attnotnull;
        if parent_notnull {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "column \"{col_name}\" is marked NOT NULL in parent table"
                ))
                .into_error());
        }
        parent.close(AccessShareLock)?;
    }

    // Find the constraint that makes this column NOT NULL, and drop it.
    // dropconstraint_internal() resets attnotnull.
    let con_tup = backend_catalog_pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for not-null constraint on column \"{col_name}\" of relation \"{}\"",
                    rel.name()
                ))
                .into_error()
        })?;
    let conoid = backend_utils_cache_syscache_seams::read_constraint_form::call(&con_tup)?.oid;

    // The normal case: we have a pg_constraint row, remove it.
    dropconstraint_internal(
        mcx,
        rel,
        conoid,
        DropBehavior::Restrict,
        recurse,
        false,
        false,
        lockmode,
    )?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);

    // table_close(attr_rel, RowExclusiveLock): RAII drop.
    drop(attr_rel);

    Ok(address)
}

// ===========================================================================
// ATExecValidateConstraint (tablecmds.c:12908)
// ===========================================================================

/// `ATExecValidateConstraint(wqueue, rel, constrName, recurse, recursing,
/// lockmode)` (tablecmds.c:12908) — find the target constraint and, if not yet
/// validated, queue a phase-3 verification scan and flip `convalidated`.
pub fn ATExecValidateConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    _recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Find and check the target constraint.
    let conoid = backend_catalog_pg_constraint::get_relation_constraint_oid(
        mcx, rel.rd_id, constr_name, false,
    )?;
    // get_relation_constraint_oid raises "constraint ... does not exist" itself
    // when missing_ok=false; here it always returns a valid OID.

    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    if conform.contype != CONSTRAINT_FOREIGN
        && conform.contype != CONSTRAINT_CHECK
        && conform.contype != CONSTRAINT_NOTNULL
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot validate constraint \"{constr_name}\" of relation \"{}\"",
                rel.name()
            ))
            .errdetail("This operation is not supported for this type of constraint.".to_string())
            .into_error());
    }

    if !conform.conenforced {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot validate NOT ENFORCED constraint".to_string())
            .into_error());
    }

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    if !conform.convalidated {
        if conform.contype == CONSTRAINT_FOREIGN {
            QueueFKConstraintValidation(
                mcx, wqueue, rel, conform.confrelid, conoid, &conform, constr_name, lockmode,
            )?;
        } else if conform.contype == CONSTRAINT_CHECK {
            QueueCheckConstraintValidation(mcx, wqueue, constr_name, conoid, recursing, lockmode)?;
        } else {
            // CONSTRAINT_NOTNULL
            QueueNNConstraintValidation(mcx, wqueue, rel, conoid, recursing, lockmode)?;
        }

        address = ObjectAddress {
            classId: ConstraintRelationId,
            objectId: conform.oid,
            objectSubId: 0,
        };
    }
    // else: already validated => InvalidObjectAddress.

    Ok(address)
}

// ===========================================================================
// QueueFKConstraintValidation (tablecmds.c:12990)
// ===========================================================================

/// `QueueFKConstraintValidation(...)` — add an FK validation entry to phase 3
/// and flip `convalidated`. The partition-recursion leg (when either end is
/// partitioned) faithfully seam-and-panics.
fn QueueFKConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    fkrel: &Relation<'mcx>,
    pkrelid: Oid,
    conoid: Oid,
    conform: &types_catalog::pg_constraint::FormData_pg_constraint,
    constr_name: &str,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    debug_assert_eq!(conform.contype, CONSTRAINT_FOREIGN);
    debug_assert!(!conform.convalidated);

    // Add the validation to phase 3's queue; only for plain tables whose FK row
    // points directly at the referenced (root) table.
    if fkrel.rd_rel.relkind == RELKIND_RELATION && conform.confrelid == pkrelid {
        let newcon = NewConstraint {
            name: Some(mcx::PgString::from_str_in(constr_name, mcx)?),
            contype: CONSTR_FOREIGN_I32,
            refrelid: conform.confrelid,
            refindid: conform.conindid,
            conid: conoid,
            qual: None,
        };
        let tab = ATGetQueueEntry(mcx, wqueue, fkrel)?;
        wqueue[tab].constraints.push(newcon);
    }

    // If either end is partitioned, recurse over child constraints.
    if fkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
        || backend_utils_cache_lsyscache_seams::get_rel_relkind::call(conform.confrelid)?
            == RELKIND_PARTITIONED_TABLE
    {
        unported("VALIDATE CONSTRAINT FK recursion over partition child constraints");
    }

    // Mark the pg_constraint row as validated.
    backend_catalog_pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}

// ===========================================================================
// QueueCheckConstraintValidation (tablecmds.c:13110)
// ===========================================================================

/// `QueueCheckConstraintValidation(...)` — queue a CHECK validation entry and
/// flip `convalidated`. The cooked-expr (`conbin`) extraction and the
/// inheritance-child recursion legs are faithfully seam-and-panicked.
fn QueueCheckConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    constr_name: &str,
    conoid: Oid,
    _recursing: bool,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    // The phase-3 CHECK revalidation requires the cooked-expr (conbin) of the
    // constraint to be deparsed and evaluated over the existing rows in
    // ATRewriteTable; that expression-eval phase-3 path is not yet ported (see
    // ATRewriteTables). Faithfully seam-and-panic rather than silently mark the
    // constraint valid without scanning.
    let _ = (mcx, wqueue, constr_name, conoid);
    unported("VALIDATE CONSTRAINT for a CHECK constraint (phase-3 CHECK revalidation scan)");
}

// ===========================================================================
// QueueNNConstraintValidation (tablecmds.c:13216)
// ===========================================================================

/// `QueueNNConstraintValidation(...)` — queue a NOT NULL validation scan and
/// flip `convalidated`. The inheritance-child recursion leg faithfully
/// seam-and-panics; the single-table leg reuses the phase-3 NOT NULL verify
/// scan (`at_verify_not_null`) by setting `verify_new_notnull`.
fn QueueNNConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    conoid: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // If we're recursing, the parent already handled children; also, a NO
    // INHERIT constraint isn't looked for in children.
    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;

    if !recursing && !con.form.connoinherit {
        let (children, _) = find_all_inheritors(mcx, rel.rd_id, lockmode, false)?;
        // children always contains the parent rel itself; >1 means real children.
        if children.len() > 1 {
            unported("VALIDATE CONSTRAINT NOT NULL recursion to inheritance children");
        }
    }

    // Queue validation for phase 3: a full-table NOT NULL recheck.
    let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
    wqueue[tab].verify_new_notnull = true;

    // Mark the pg_constraint row as validated.
    backend_catalog_pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}
