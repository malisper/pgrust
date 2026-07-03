#![allow(non_snake_case)]
// lockcmds.c, plain-table lane: views (LockViewRecurse) and inheritance
// children (find_all_inheritors) are loud.
use mcx::Mcx;
use types_core::{
    InvalidOid, Oid, RELPERSISTENCE_TEMP, XACT_FLAGS_ACCESSEDTEMPNAMESPACE,
};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::{LockStmt, ObjectType};
use types_rel::{NoLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW};
use types_storage::{AccessShareLock, RowExclusiveLock, LOCKMODE};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: lockcmds.c {what}")
}

pub fn LockTableCommand<'mcx>(mcx: Mcx<'mcx>, lockstmt: &LockStmt<'mcx>) -> PgResult<()> {
    for cell in lockstmt.relations.iter() {
        let rv = cell.as_range_var().expect("LOCK target is a RangeVar");
        let recurse = rv.inh;
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("relation_expr always carries relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };

        let mode = lockstmt.mode;
        let mut callback = |rv: &rel_vocab::RangeVar<'_>, relid: Oid, _old: Oid| {
            RangeVarCallbackForLockTable(rv, relid, mode)
        };
        let flags = if lockstmt.nowait { catalog_namespace::RVR_NOWAIT } else { 0 };
        let reloid =
            catalog_namespace::RangeVarGetRelidExtended(&rv, mode, flags, Some(&mut callback))?;

        if lsyscache::get_rel_relkind(reloid)? as u8 == RELKIND_VIEW {
            unported("LockViewRecurse (view lane)");
        } else if recurse {
            LockTableRecurse(mcx, reloid)?;
        }
    }
    Ok(())
}

fn RangeVarCallbackForLockTable(
    rv: &rel_vocab::RangeVar<'_>,
    relid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if relid == InvalidOid {
        return Ok(());
    }
    let relkind = lsyscache::get_rel_relkind(relid)? as u8;
    if relkind == 0 {
        return Ok(());
    }

    if relkind != RELKIND_RELATION
        && relkind != RELKIND_PARTITIONED_TABLE
        && relkind != RELKIND_VIEW
    {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot lock relation \"{}\"", rv.relname))
                .with_detail(detail)
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    if lsyscache::get_rel_persistence(relid)? as u8 == RELPERSISTENCE_TEMP {
        xact::OrMyXactFlags(XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
    }

    let aclresult = LockTableAclCheck(relid, lockmode, miscinit::GetUserId())?;
    if aclresult != aclchk::ACLCHECK_OK {
        let objtype = if relkind == RELKIND_VIEW {
            ObjectType::OBJECT_VIEW
        } else {
            ObjectType::OBJECT_TABLE
        };
        aclchk_seams::aclcheck_error::call(aclresult, objtype as i32, rv.relname)?;
    }
    Ok(())
}

// find_all_inheritors is unported: with no children the C loop is a no-op, so
// only rels that actually have children (relhassubclass) go loud.
fn LockTableRecurse<'mcx>(mcx: Mcx<'mcx>, reloid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, reloid, NoLock)?;
    let has_children = rel.rd_rel.relhassubclass;
    rel.close(NoLock)?;
    if has_children {
        unported("LockTableRecurse: find_all_inheritors (inheritance/partition lane)");
    }
    Ok(())
}

fn LockTableAclCheck(reloid: Oid, lockmode: LOCKMODE, userid: Oid) -> PgResult<i32> {
    let mut aclmask =
        adt_acl::ACL_MAINTAIN | adt_acl::ACL_UPDATE | adt_acl::ACL_DELETE | adt_acl::ACL_TRUNCATE;
    if lockmode <= AccessShareLock {
        aclmask |= adt_acl::ACL_SELECT;
    }
    if lockmode <= RowExclusiveLock {
        aclmask |= adt_acl::ACL_INSERT;
    }
    aclchk::pg_class_aclcheck(reloid, userid, aclmask)
}
