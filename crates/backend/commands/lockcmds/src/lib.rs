#![allow(non_snake_case)]
use mcx::Mcx;
use types_core::{
    InvalidOid, Oid, RELPERSISTENCE_TEMP, XACT_FLAGS_ACCESSEDTEMPNAMESPACE,
};
use types_error::{
    PgError, PgResult, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::{LockStmt, ObjectType, Query};
use types_nodes::Node;
use types_rel::{NoLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW};
use types_storage::{AccessShareLock, RowExclusiveLock, LOCKMODE};

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
            let mut ancestor_views: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
            LockViewRecurse(mcx, reloid, mode, lockstmt.nowait, &mut ancestor_views)?;
        } else if recurse {
            LockTableRecurse(mcx, reloid, mode, lockstmt.nowait)?;
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

struct LockViewRecurseCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    lockmode: LOCKMODE,
    nowait: bool,
    check_as_user: Oid,
    ancestor_views: &'a mut mcx::PgVec<'mcx, Oid>,
}

impl<'a, 'mcx> LockViewRecurseCtx<'a, 'mcx> {
    fn query(&mut self, query: &Query<'mcx>) -> PgResult<bool> {
        for rnode in query.rtable.iter() {
            let rte = rnode.as_range_tbl_entry().expect("rtable entry");
            let relid = rte.relid;
            let relkind = rte.relkind;
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_PARTITIONED_TABLE
                && relkind != RELKIND_VIEW
            {
                continue;
            }
            if self.ancestor_views.iter().any(|&v| v == relid) {
                continue;
            }
            let relname = lsyscache::get_rel_name(self.mcx, relid)?
                .map(|n| n.to_string())
                .unwrap_or_default();
            let aclresult = LockTableAclCheck(relid, self.lockmode, self.check_as_user)?;
            if aclresult != aclchk::ACLCHECK_OK {
                let objtype = if relkind == RELKIND_VIEW {
                    ObjectType::OBJECT_VIEW
                } else {
                    ObjectType::OBJECT_TABLE
                };
                aclchk_seams::aclcheck_error::call(aclresult, objtype as i32, &relname)?;
            }
            if !self.nowait {
                lmgr::LockRelationOid(relid, self.lockmode)?;
            } else if !lmgr::ConditionalLockRelationOid(relid, self.lockmode)? {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("could not obtain lock on relation \"{relname}\""),
                    )
                    .with_sqlstate(ERRCODE_LOCK_NOT_AVAILABLE),
                ));
            }
            if relkind == RELKIND_VIEW {
                LockViewRecurse(self.mcx, relid, self.lockmode, self.nowait, self.ancestor_views)?;
            } else if rte.inh {
                LockTableRecurse(self.mcx, relid, self.lockmode, self.nowait)?;
            }
        }
        nodes_core::query_tree_walker(query, self, nodes_core::QTW_IGNORE_JOINALIASES)
    }
}

impl<'a, 'mcx> nodes_core::NodeWalker<'mcx> for LockViewRecurseCtx<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(q) = node.as_query() {
            return self.query(q);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.query(q)
    }
}

pub fn LockViewRecurse<'mcx>(
    mcx: Mcx<'mcx>,
    reloid: Oid,
    lockmode: LOCKMODE,
    nowait: bool,
    ancestor_views: &mut mcx::PgVec<'mcx, Oid>,
) -> PgResult<()> {
    let view = table::table_open(mcx, reloid, NoLock)?;
    let viewquery = rewrite_handler::get_view_query(mcx, &view)?;

    let check_as_user = if view
        .rd_options
        .as_ref()
        .and_then(|o| o.view())
        .is_some_and(|v| v.security_invoker)
    {
        miscinit::GetUserId()
    } else {
        view.rd_rel.relowner
    };
    ancestor_views.push(reloid);
    {
        let mut ctx = LockViewRecurseCtx { mcx, lockmode, nowait, check_as_user, ancestor_views };
        ctx.query(viewquery)?;
    }
    ancestor_views.pop();

    view.close(NoLock)
}

// Children are locked without their own ACL check: permission on the parent
// suffices (lockcmds.c LockTableRecurse).
fn LockTableRecurse<'mcx>(
    mcx: Mcx<'mcx>,
    reloid: Oid,
    lockmode: LOCKMODE,
    nowait: bool,
) -> PgResult<()> {
    let children = pg_inherits::find_all_inheritors(mcx, reloid, NoLock)?;
    for &childreloid in children.iter() {
        if childreloid == reloid {
            continue;
        }
        if !nowait {
            lmgr::LockRelationOid(childreloid, lockmode)?;
        } else if !lmgr::ConditionalLockRelationOid(childreloid, lockmode)? {
            let Some(relname) = lsyscache::get_rel_name(mcx, childreloid)? else {
                continue;
            };
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("could not obtain lock on relation \"{relname}\""),
                )
                .with_sqlstate(ERRCODE_LOCK_NOT_AVAILABLE),
            ));
        }
        if !syscache_seams::search_syscache_exists_reloid::call(childreloid)? {
            lmgr::UnlockRelationOid(childreloid, lockmode)?;
            continue;
        }
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
