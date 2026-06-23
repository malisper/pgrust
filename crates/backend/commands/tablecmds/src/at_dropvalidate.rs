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

use ::mcx::{Mcx, PgVec};

use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::pg_attribute::{AttributeRelationId, PgAttributeUpdateRow};
use ::types_catalog::pg_constraint::{
    CONSTRAINT_CHECK, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
};
use ::types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use ::types_error::{
    PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use ::nodes::parsenodes::DropBehavior;
use ::rel::Relation;
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, LOCKMODE, NoLock, RowExclusiveLock,
};

use ::heaptuple::heap_deform_tuple;
use ::common_relation::relation_open;
use ::scankey::ScanKeyInit;
use ::transam_xact::CommandCounterIncrement;
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use indexing_seams as indexing_seam;
use ::objectaddress::consts::ConstraintRelationId;
use ::pg_inherits::{find_all_inheritors, find_inheritance_children};
use ::lsyscache_seams::get_attname;
use ::relcache::derived::{IndexAttrBitmapKind, RelationGetIndexAttrBitmap};
use ::stack_depth::check_stack_depth;

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
    ATT_FOREIGN_TABLE, ATT_PARTITIONED_TABLE, ATT_TABLE,
};
use crate::helpers::here;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `NewConstraint.contype` is the `ConstrType` enum-as-i32; the FK value the
/// validate-queue uses.
const CONSTR_FOREIGN_I32: i32 = ::nodes::ddlnodes::ConstrType::CONSTR_FOREIGN as i32;

/// `NewConstraint.contype` for a CHECK validation entry (the value
/// `run_at_rewrite_table_scan` filters on to evaluate the qual per row).
const CONSTR_CHECK_I32: i32 = ::nodes::ddlnodes::ConstrType::CONSTR_CHECK as i32;

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
    let conoid = pg_constraint::get_relation_constraint_oid(
        mcx,
        rel.rd_id,
        constr_name,
        true, // missing_ok at this level — we issue our own NOTICE/ERROR below
    )?;

    if !OidIsValid(conoid) {
        // not found
        if !missing_ok {
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of relation \"{}\" does not exist",
                    rel.name()
                ))
                .into_error());
        } else {
            return utils_error::ereport(NOTICE)
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
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Guard against stack overflow due to overly deep inheritance tree.
    check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            ::nodes::ddlnodes::AlterTableType::AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    // con = GETSTRUCT(constraintTup);  constrName = NameStr(con->conname);
    let con = syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    // Don't allow drop of inherited constraints.
    if conform.coninhcount > 0 && !recursing {
        return Err(utils_error::ereport(ERROR)
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
        let con_tup = syscache_seams::search_constraint_tuple_by_oid::call(
            mcx, conoid,
        )?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
        let attnum: AttrNumber = pg_constraint::extractNotNullColumn(&con_tup)?;

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
            let pkindex = ::relcache::derived::RelationGetPrimaryKeyIndex(
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
            return Err(utils_error::ereport(ERROR)
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
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "column \"{aname}\" is in index used as replica identity"
                ))
                .into_error());
        }

        // Disallow if it's a GENERATED AS IDENTITY column, and reset attnotnull
        // if needed. C reads attidentity/attnotnull from a *fresh* syscache copy
        // (`SearchSysCacheCopyAttName`), not the relcache descriptor: an earlier
        // subcommand in the same ALTER pass (e.g. DROP IDENTITY, which runs in
        // AT_PASS_DROP just like DROP NOT NULL) may already have cleared
        // attidentity in pg_attribute. The owned `rel.rd_att` snapshot was taken
        // at pass open and would still show the column as an identity column.
        let tuple = cache_syscache::SearchSysCacheAttNum(mcx, rel.rd_id, attnum)?
            .ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for attribute {attnum} of relation {}",
                        rel.rd_id
                    ))
                    .into_error()
            })?;
        let attidentity = cache_syscache::SysCacheGetAttrNotNull(
            mcx,
            cache_syscache::ATTNUM,
            &tuple,
            ::types_catalog::pg_attribute::Anum_pg_attribute_attidentity as i32,
        )?
        .as_char();
        let attnotnull = cache_syscache::SysCacheGetAttrNotNull(
            mcx,
            cache_syscache::ATTNUM,
            &tuple,
            ::types_catalog::pg_attribute::Anum_pg_attribute_attnotnull as i32,
        )?
        .as_bool();

        if attidentity != 0 {
            let aname = get_attname::call(mcx, rel.rd_id, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "column \"{aname}\" of relation \"{}\" is an identity column",
                    rel.name()
                ))
                .into_error());
        }

        // All good -- reset attnotnull if needed.
        if attnotnull {
            // attForm->attnotnull = false; CatalogTupleUpdate(attrel, &atttup->t_self, atttup);
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
    dependency_seams::perform_deletion::call(
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

    // constrName = NameStr(con->conname) — the parent constraint's name, used to
    // match the inherited child constraint by name (CHECK case).
    let constr_name = conform.conname_str().to_string();
    let parent_contype = conform.contype;

    for &childrelid in children.iter() {
        // find_inheritance_children already got lock.
        let childrel = relation_open(mcx, childrelid, NoLock)?;
        CheckAlterTableIsSafe(&childrel)?;

        // We search for not-null constraints by column name, and others by
        // constraint name.  Each lookup yields the child constraint's form plus
        // the heap TID we write the coninhcount/conislocal update at.
        let (mut childcon, child_tid, child_conoid): (
            ::types_catalog::pg_constraint::FormData_pg_constraint,
            types_tuple::heaptuple::ItemPointerData,
            Oid,
        ) = if parent_contype == CONSTRAINT_NOTNULL {
            // tuple = findNotNullConstraint(childrelid, colname);
            let colname = colname.as_deref().unwrap_or_default();
            let tuple = pg_constraint::findNotNullConstraint(
                mcx, childrelid, colname,
            )?
            .ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for not-null constraint on column \"{colname}\" of relation {}",
                        childrel.rd_id
                    ))
                    .into_error()
            })?;
            let form =
                syscache_seams::read_constraint_form::call(&tuple)?;
            let oid = form.oid;
            (form, tuple.tuple.t_self, oid)
        } else {
            // Scan (conrelid = childrelid, contypid = Invalid, conname = constrName);
            // there can only be one.
            let oid = pg_constraint::get_relation_constraint_oid(
                mcx, childrelid, &constr_name, true,
            )?;
            if !OidIsValid(oid) {
                return Err(utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!(
                        "constraint \"{constr_name}\" of relation \"{}\" does not exist",
                        childrel.name()
                    ))
                    .into_error());
            }
            let copy =
                syscache_seams::search_constraint_form_by_oid::call(oid)?
                    .ok_or_else(|| {
                        utils_error::ereport(ERROR)
                            .errmsg_internal(format!("cache lookup failed for constraint {oid}"))
                            .into_error()
                    })?;
            (copy.form, copy.tid, copy.form.oid)
        };

        // Right now only CHECK and not-null constraints can be inherited.
        if childcon.contype != CONSTRAINT_CHECK && childcon.contype != CONSTRAINT_NOTNULL {
            return Err(utils_error::ereport(ERROR)
                .errmsg_internal(
                    "inherited constraint is not a CHECK or not-null constraint".to_string(),
                )
                .into_error());
        }

        if childcon.coninhcount <= 0 {
            // shouldn't happen
            return Err(utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "relation {childrelid} has non-inherited constraint \"{}\"",
                    childcon.conname_str()
                ))
                .into_error());
        }

        if recurse {
            // If the child constraint has other definition sources, just
            // decrement its inheritance count; if not, recurse to delete it.
            if childcon.coninhcount == 1 && !childcon.conislocal {
                // Time to delete this child constraint, too.
                dropconstraint_internal(
                    mcx,
                    &childrel,
                    child_conoid,
                    behavior,
                    recurse,
                    true,
                    missing_ok,
                    lockmode,
                )?;
            } else {
                // Child constraint must survive my deletion.
                childcon.coninhcount -= 1;
                update_constraint_inhcount(mcx, &childcon, child_tid)?;

                // Make update visible.
                CommandCounterIncrement()?;
            }
        } else {
            // If we were told to drop ONLY in this table (no recursion) and
            // there are no further parents for this constraint, we need to mark
            // the inheritors' constraints as locally defined rather than
            // inherited.
            childcon.coninhcount -= 1;
            if childcon.coninhcount == 0 {
                childcon.conislocal = true;
            }
            update_constraint_inhcount(mcx, &childcon, child_tid)?;

            // Make update visible.
            CommandCounterIncrement()?;
        }

        // table_close(childrel, NoLock): RAII drop.
        childrel.close(NoLock)?;
    }

    Ok(conobj)
}

/// `CatalogTupleUpdate(conrel, &tuple->t_self, tuple)` for the child-recursion
/// coninhcount/conislocal scribble in `dropconstraint_internal`: re-store the
/// child `pg_constraint` row at `tid` with the (already-decremented) inheritance
/// bookkeeping fields, carrying through every other column from `childcon`.
fn update_constraint_inhcount<'mcx>(
    mcx: Mcx<'mcx>,
    childcon: &::types_catalog::pg_constraint::FormData_pg_constraint,
    tid: types_tuple::heaptuple::ItemPointerData,
) -> PgResult<()> {
    let conrel = relation_open(mcx, ConstraintRelationId, RowExclusiveLock)?;
    let fields = ::types_catalog::pg_constraint::ConstraintFieldUpdate {
        conname: childcon.conname,
        connamespace: childcon.connamespace,
        conislocal: childcon.conislocal,
        coninhcount: childcon.coninhcount,
        conparentid: childcon.conparentid,
        convalidated: childcon.convalidated,
        connoinherit: childcon.connoinherit,
        conenforced: childcon.conenforced,
        condeferrable: childcon.condeferrable,
        condeferred: childcon.condeferred,
        conindid: childcon.conindid,
    };
    indexing_seam::catalog_tuple_update_pg_constraint::call(&conrel, tid, &fields)?;
    conrel.close(RowExclusiveLock)?;
    Ok(())
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
        ::lsyscache_seams::get_attnum::call(rel.rd_id, col_name)?;
    if attnum == 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{col_name}\" of relation \"{}\" does not exist",
                rel.name()
            ))
            .into_error());
    }

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    let address = ObjectAddress {
        classId: ::objectaddress::consts::RelationRelationId,
        objectId: rel.rd_id,
        objectSubId: attnum as i32,
    };

    // C reads attnotnull/attidentity from a *fresh* syscache copy
    // (`SearchSysCacheCopyAttName`), not the relcache descriptor. An earlier
    // subcommand in the same ALTER pass (e.g. DROP IDENTITY, which also runs in
    // AT_PASS_DROP) may already have cleared attidentity in pg_attribute; the
    // owned `rel.rd_att` snapshot was taken at pass open and would still show
    // the column as an identity column.
    let att_tuple = cache_syscache::SearchSysCacheAttNum(mcx, rel.rd_id, attnum)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for attribute {attnum} of relation {}",
                    rel.rd_id
                ))
                .into_error()
        })?;
    let att_attnotnull = cache_syscache::SysCacheGetAttrNotNull(
        mcx,
        cache_syscache::ATTNUM,
        &att_tuple,
        ::types_catalog::pg_attribute::Anum_pg_attribute_attnotnull as i32,
    )?
    .as_bool();
    let att_attidentity = cache_syscache::SysCacheGetAttrNotNull(
        mcx,
        cache_syscache::ATTNUM,
        &att_tuple,
        ::types_catalog::pg_attribute::Anum_pg_attribute_attidentity as i32,
    )?
    .as_char();

    // If the column is already nullable there's nothing to do.
    if !att_attnotnull {
        drop(attr_rel);
        return Ok(ObjectAddress {
            classId: InvalidOid,
            objectId: InvalidOid,
            objectSubId: 0,
        });
    }

    // Prevent them from altering a system attribute.
    if attnum <= 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{col_name}\""))
            .into_error());
    }

    // if (attTup->attidentity) ereport(...identity column...)
    if att_attidentity != 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{col_name}\" of relation \"{}\" is an identity column",
                rel.name()
            ))
            .into_error());
    }

    // If rel is partition, shouldn't drop NOT NULL if parent has the same.
    if rel.rd_rel.relispartition {
        let parent_id = partition_seams::get_partition_parent::call(rel.rd_id, false)?;
        let parent = relation_open(mcx, parent_id, AccessShareLock)?;
        let parent_attnum =
            ::lsyscache_seams::get_attnum::call(parent_id, col_name)?;
        let parent_att = parent.rd_att.attr((parent_attnum - 1) as usize);
        let parent_notnull = parent_att.attnotnull;
        if parent_notnull {
            return Err(utils_error::ereport(ERROR)
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
    let con_tup = pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for not-null constraint on column \"{col_name}\" of relation \"{}\"",
                    rel.name()
                ))
                .into_error()
        })?;
    let conoid = syscache_seams::read_constraint_form::call(&con_tup)?.oid;

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
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Find and check the target constraint.
    let conoid = pg_constraint::get_relation_constraint_oid(
        mcx, rel.rd_id, constr_name, false,
    )?;
    // get_relation_constraint_oid raises "constraint ... does not exist" itself
    // when missing_ok=false; here it always returns a valid OID.

    let con = syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    if conform.contype != CONSTRAINT_FOREIGN
        && conform.contype != CONSTRAINT_CHECK
        && conform.contype != CONSTRAINT_NOTNULL
    {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot validate constraint \"{constr_name}\" of relation \"{}\"",
                rel.name()
            ))
            .errdetail("This operation is not supported for this type of constraint.".to_string())
            .into_error());
    }

    if !conform.conenforced {
        return Err(utils_error::ereport(ERROR)
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
            QueueCheckConstraintValidation(
                mcx, wqueue, rel, constr_name, conoid, recurse, recursing, lockmode,
            )?;
        } else {
            // CONSTRAINT_NOTNULL
            QueueNNConstraintValidation(mcx, wqueue, rel, conoid, recurse, recursing, lockmode)?;
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
pub(crate) fn QueueFKConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    fkrel: &Relation<'mcx>,
    pkrelid: Oid,
    conoid: Oid,
    conform: &::types_catalog::pg_constraint::FormData_pg_constraint,
    constr_name: &str,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    debug_assert_eq!(conform.contype, CONSTRAINT_FOREIGN);
    debug_assert!(!conform.convalidated);

    // Add the validation to phase 3's queue; only for plain tables whose FK row
    // points directly at the referenced (root) table.
    if fkrel.rd_rel.relkind == RELKIND_RELATION && conform.confrelid == pkrelid {
        let newcon = NewConstraint {
            name: Some(::mcx::PgString::from_str_in(constr_name, mcx)?),
            contype: CONSTR_FOREIGN_I32,
            refrelid: conform.confrelid,
            refindid: conform.conindid,
            conid: conoid,
            qual: None,
        };
        let tab = ATGetQueueEntry(mcx, wqueue, fkrel)?;
        wqueue[tab].constraints.push(newcon);
    }

    // If the table at either end of the constraint is partitioned, we need to
    // recurse and handle every unvalidated constraint that is a child of this
    // constraint (tablecmds.c:13043).
    if fkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
        || ::lsyscache_seams::get_rel_relkind::call(conform.confrelid)?
            == RELKIND_PARTITIONED_TABLE
    {
        // ScanKeyInit(&pkey, Anum_pg_constraint_conparentid, BTEqual, F_OIDEQ, con->oid);
        // pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, NULL, 1, &pkey);
        let conrel = table_seams::table_open::call(
            mcx,
            ConstraintRelationId,
            RowExclusiveLock,
        )?;

        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            ::types_catalog::pg_constraint::Anum_pg_constraint_conparentid,
            BTEqualStrategyNumber,
            ::types_core::fmgr::F_OIDEQ,
            types_tuple::heaptuple::Datum::from_oid(conoid),
        )?;
        let keys = [key];

        let mut scan = genam_seams::systable_beginscan::call(
            &conrel,
            ::types_catalog::pg_constraint::ConstraintParentIndexId,
            true,
            None,
            &keys,
        )?;

        // Collect child constraint OIDs first; the recursion below re-opens
        // pg_constraint (it marks each child validated), so we don't hold the
        // scan across the recursive calls. We capture convalidated here so we
        // can skip already-valid subtrees without a re-lookup.
        let mut children: Vec<(Oid, bool)> = Vec::new();
        while let Some(tup) =
            genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        {
            let cols = heap_deform_tuple(mcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
            let child_oid = cols
                [::types_catalog::pg_constraint::Anum_pg_constraint_oid as usize - 1]
                .0
                .as_oid();
            let child_validated = cols
                [::types_catalog::pg_constraint::Anum_pg_constraint_convalidated as usize - 1]
                .0
                .as_bool();
            children.push((child_oid, child_validated));
        }
        drop(scan);
        conrel.close(NoLock)?;

        for (child_oid, child_validated) in children {
            // If the child constraint has already been validated, no further
            // action is required for it or its descendants, as they are all
            // valid.
            if child_validated {
                continue;
            }

            let childcon =
                syscache_seams::search_constraint_form_by_oid::call(child_oid)?
                    .ok_or_else(|| {
                        utils_error::ereport(ERROR)
                            .errmsg_internal(format!(
                                "cache lookup failed for constraint {child_oid}"
                            ))
                            .into_error()
                    })?;
            let childform = childcon.form;

            let childrel = relation_open(mcx, childform.conrelid, lockmode)?;

            // NB: pkrelid is passed as-is during recursion, as it is required
            // to identify the root referenced table.
            QueueFKConstraintValidation(
                mcx,
                wqueue,
                &childrel,
                pkrelid,
                child_oid,
                &childform,
                constr_name,
                lockmode,
            )?;
            childrel.close(NoLock)?;
        }
    }

    // Mark the pg_constraint row as validated.
    pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}

// ===========================================================================
// QueueCheckConstraintValidation (tablecmds.c:13110)
// ===========================================================================

/// `QueueCheckConstraintValidation(...)` (tablecmds.c:13117) — queue a CHECK
/// validation entry into phase 3 (`tab->constraints`, evaluated per-row by the
/// `ATRewriteTable` scan), then flip `convalidated`. The qual is the cooked
/// `conbin` parsed via `stringToNode` and run through
/// `expand_generated_columns_in_expr`. The inheritance-child recursion leg is
/// faithfully seam-and-panicked (matches the NOT NULL / FK legs' frontier).
fn QueueCheckConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    conoid: Oid,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let con = syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    debug_assert_eq!(con.form.contype, CONSTRAINT_CHECK);

    // If we're recursing, the parent has already done this; also a NO INHERIT
    // constraint isn't looked for in children. We recurse before validating on
    // the parent (C tablecmds.c:13140-13175).
    let children = if !recursing && !con.form.connoinherit {
        find_all_inheritors(mcx, rel.rd_id, lockmode, false)?.0
    } else {
        PgVec::new_in(mcx)
    };

    // For CHECK constraints we must validate the children before marking the
    // parent valid. We recurse before validating on the parent, to reduce risk
    // of deadlocks (tablecmds.c:13150-13175).
    for &childoid in children.iter() {
        if childoid == rel.rd_id {
            continue;
        }
        // If told not to recurse there had better not be any child tables: we
        // can't mark the parent constraint valid unless it is valid for all
        // child tables.
        if !recurse {
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("constraint must be validated on child tables too".to_string())
                .into_error());
        }

        // find_all_inheritors already got lock.
        let childrel = relation_open(mcx, childoid, NoLock)?;
        ATExecValidateConstraint(mcx, wqueue, &childrel, constr_name, false, true, lockmode)?;
        // table_close(childrel, NoLock): RAII drop.
        drop(childrel);
    }

    // Queue validation for phase 3: a NewConstraint of type CONSTR_CHECK whose
    // qual is the cooked conbin, expanded for virtual generated columns. The
    // ATRewriteTable scan (run_at_rewrite_table_scan) evaluates it per row.
    //
    // val = SysCacheGetAttrNotNull(CONSTROID, contuple, Anum_pg_constraint_conbin);
    // conbin = TextDatumGetCString(val);
    // newcon->qual = expand_generated_columns_in_expr(stringToNode(conbin), rel, 1);
    let conbin = pg_constraint::get_check_constraint_conbin(
        mcx, rel.rd_id, constr_name,
    )?;
    let cnode = read_seams::string_to_node::call(mcx, &conbin)?;
    let cexpr = ::mcx::PgBox::into_inner(cnode).into_expr().ok_or_else(|| {
        utils_error::ereport(ERROR)
            .errmsg_internal("CHECK constraint conbin did not parse to an Expr".to_string())
            .into_error()
    })?;
    let expanded = rewritehandler_seams::expand_generated_columns_in_expr::call(
        mcx,
        Some(cexpr.erase_lifetime()),
        rel.rd_id,
        1,
    )?
    .ok_or_else(|| {
        utils_error::ereport(ERROR)
            .errmsg_internal("expand_generated_columns_in_expr returned None".to_string())
            .into_error()
    })?;
    let qual_node = ::mcx::alloc_in(mcx, ::nodes::nodes::Node::mk_expr(mcx, expanded.clone_in(mcx)?)?)?;

    let newcon = NewConstraint {
        name: Some(::mcx::PgString::from_str_in(constr_name, mcx)?),
        contype: CONSTR_CHECK_I32,
        refrelid: InvalidOid,
        refindid: InvalidOid,
        conid: conoid,
        qual: Some(qual_node),
    };
    let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
    wqueue[tab].constraints.push(newcon);

    // Invalidate relcache so that others see the new validated constraint
    // (tablecmds.c:13197). Without this, the relation's cached TupleConstr keeps
    // ccvalid=false for this CHECK constraint — a pg_constraint update only
    // invalidates the constrained relation's relcache for FOREIGN keys, not for
    // CHECK constraints — so constraint-exclusion in the same session would not
    // pick up the now-validated constraint.
    inval_seams::cache_invalidate_relcache::call(rel.rd_id)?;

    // Mark the pg_constraint row as validated (CatalogTupleUpdate +
    // InvokeObjectPostAlterHook).
    pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
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
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // If we're recursing, the parent already handled children; also, a NO
    // INHERIT constraint isn't looked for in children.
    let con = syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;

    // attnum = extractNotNullColumn(contuple): the sole conkey member. The
    // scalar `search_constraint_form_by_oid` projection does not carry the
    // `conkey` array, so re-fetch the full tuple by OID and run the shared
    // extractor (tablecmds.c:13235).
    let con_tup =
        syscache_seams::search_constraint_tuple_by_oid::call(mcx, conoid)?
            .ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                    .into_error()
            })?;
    let attnum: AttrNumber = pg_constraint::extractNotNullColumn(&con_tup)?;

    // We recurse before validating on the parent, to reduce risk of deadlocks.
    let children = if !recursing && !con.form.connoinherit {
        find_all_inheritors(mcx, rel.rd_id, lockmode, false)?.0
    } else {
        PgVec::new_in(mcx)
    };

    // colname = get_attname(RelationGetRelid(rel), attnum, false): the child
    // column may have a different attnum, so children are searched by name.
    let colname = ::lsyscache_seams::get_attname::call(mcx, rel.rd_id, attnum, false)?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for attribute {attnum} of relation {}",
                    rel.rd_id
                ))
                .into_error()
        })?;
    let colname = colname.as_str();

    for &childoid in children.iter() {
        if childoid == rel.rd_id {
            continue;
        }
        // If told not to recurse there had better not be any child tables.
        if !recurse {
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("constraint must be validated on child tables too".to_string())
                .into_error());
        }

        // The column on the child might have a different attnum, so search by
        // column name (tablecmds.c:13261).
        let contup = pg_constraint::findNotNullConstraint(mcx, childoid, colname)?
            .ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for not-null constraint on column \"{colname}\" of relation {childoid}"
                    ))
                    .into_error()
            })?;
        let childcon = syscache_seams::read_constraint_form::call(&contup)?;
        if childcon.convalidated {
            continue;
        }

        // find_all_inheritors already got lock.
        let childrel = relation_open(mcx, childoid, NoLock)?;
        let conname = childcon.conname_str().to_string();
        // XXX improve ATExecValidateConstraint API to avoid double search.
        ATExecValidateConstraint(mcx, wqueue, &childrel, &conname, false, true, lockmode)?;
        drop(childrel);
    }

    // Set attnotnull appropriately without queueing another validation
    // (set_attnotnull(NULL, rel, attnum, true, false), tablecmds.c:13292).
    crate::create::set_attnotnull(mcx, rel, attnum, true, false)?;

    // Queue validation for phase 3: a full-table NOT NULL recheck.
    let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
    wqueue[tab].verify_new_notnull = true;

    // Invalidate relcache so that others see the new validated constraint
    // (tablecmds.c:13297).
    inval_seams::cache_invalidate_relcache::call(rel.rd_id)?;

    // Mark the pg_constraint row as validated.
    pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}
