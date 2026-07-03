// RemoveRelations + RangeVarCallbackForDropRelation (tablecmds.c), bounded to
// OBJECT_TABLE over plain permanent tables; other removeTypes and the
// index/partition callback arms are loud.
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_TABLE, ERROR, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType};
use rel_vocab::RangeVar;
use types_nodes::NodeList;
use types_rel::{AccessExclusiveLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: tablecmds {what}")
}

fn makeRangeVarFromNameList<'mcx>(names: &NodeList<'mcx>) -> RangeVar<'mcx> {
    let parts: Vec<&'mcx str> = names
        .iter()
        .map(|n| {
            n.as_string()
                .expect("qualified name component is a String node")
                .sval
        })
        .collect();
    let mut rv = RangeVar {
        catalogname: None,
        schemaname: None,
        relname: "",
        inh: true,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    match parts.as_slice() {
        [r] => rv.relname = r,
        [s, r] => {
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        [c, s, r] => {
            rv.catalogname = Some(c);
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        _ => panic!("improper relation name (too many dotted names)"),
    }
    rv
}

fn DropErrorMsgNonExistent(rel: &RangeVar<'_>, missing_ok: bool) -> PgResult<()> {
    if let Some(schemaname) = rel.schemaname {
        if catalog_namespace::get_namespace_oid(schemaname, true)? == InvalidOid {
            if !missing_ok {
                return Err(Box::new(
                    PgError::new(ERROR, format!("schema \"{schemaname}\" does not exist"))
                        .with_sqlstate(types_error::ERRCODE_INVALID_SCHEMA_NAME),
                ));
            }
            elog_seams::ereport_msg::call(
                NOTICE,
                format!("schema \"{schemaname}\" does not exist, skipping"),
                None,
            )?;
            return Ok(());
        }
    }
    let relname = rel.relname;
    if !missing_ok {
        return Err(Box::new(
            PgError::new(ERROR, format!("table \"{relname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
        ));
    }
    elog_seams::ereport_msg::call(
        NOTICE,
        format!("table \"{relname}\" does not exist, skipping"),
        None,
    )?;
    Ok(())
}

pub fn RemoveRelations<'mcx>(mcx: Mcx<'mcx>, drop: &DropStmt<'mcx>) -> PgResult<()> {
    if drop.concurrent {
        unported("RemoveRelations: DROP INDEX CONCURRENTLY");
    }
    let expected_relkind = match drop.removeType {
        ObjectType::OBJECT_TABLE => RELKIND_RELATION,
        ObjectType::OBJECT_INDEX => types_rel::RELKIND_INDEX,
        other => unported(&format!(
            "RemoveRelations: removeType {other:?} (its DDL lane does not exist)"
        )),
    };

    let mut objects = catalog_dependency::ObjectAddresses::new();

    for cell in drop.objects.iter() {
        let names = cell.as_list().expect("DROP object is a name list");
        let rel = makeRangeVarFromNameList(&names);

        inval::local::AcceptInvalidationMessages()?;

        let mut callback = |rv: &RangeVar<'_>, relOid: Oid, oldRelOid: Oid| {
            RangeVarCallbackForDropRelation(mcx, rv, relOid, oldRelOid, expected_relkind)
        };
        let relOid = catalog_namespace::RangeVarGetRelidExtended(
            &rel,
            AccessExclusiveLock,
            catalog_namespace::RVR_MISSING_OK,
            Some(&mut callback),
        )?;

        if relOid == InvalidOid {
            DropErrorMsgNonExistent(&rel, drop.missing_ok)?;
            continue;
        }

        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            RELATION_RELATION_ID,
            relOid,
        ));
    }

    catalog_dependency::performMultipleDeletions(mcx, &objects, drop.behavior, 0)
}

fn RangeVarCallbackForDropRelation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RangeVar<'_>,
    relOid: Oid,
    _oldRelOid: Oid,
    expected_relkind: u8,
) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
    }

    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = 1 as AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_oid(relOid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_class,
        catalog::ClassOidIndexId,
        true,
        None,
        &[key],
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
        return Ok(()); // concurrently dropped
    };
    let desc = pg_class.descr();
    let get = |attnum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let relnamespace = get(3).as_oid();
    let relkind = get(18).as_i8() as u8;
    let relispartition = get(28).as_bool();
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;

    if relispartition {
        unported("RangeVarCallbackForDropRelation: partition parent locking");
    }

    let actual_expected = if relkind == RELKIND_PARTITIONED_TABLE {
        RELKIND_RELATION
    } else {
        relkind
    };
    if actual_expected != expected_relkind {
        unported("RangeVarCallbackForDropRelation: DropErrorMsgWrongType (42809)");
    }

    // object_ownercheck (aclchk.c): superuser fast path; role ACL walks are
    // the unported remainder.
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        unported("RangeVarCallbackForDropRelation: object_ownercheck for non-superusers");
    }

    // IsSystemClass: catalog oid range or pg_toast namespace.
    let is_system = catalog::IsCatalogRelationOid(relOid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "permission denied: \"{}\" is a system catalog",
                    rel.relname
                ),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    if expected_relkind == types_rel::RELKIND_INDEX {
        // C locks the index's heap before the index (deadlock ordering).
        // DIVERGENCE: the lookup-retry unlock bookkeeping (state->heapOid)
        // is dropped; a stale-lookup retry leaves an extra heap lock held
        // until end of transaction.
        let heap_oid = catalog_index::IndexGetRelation(mcx, relOid, true)?;
        if heap_oid != InvalidOid {
            lmgr::LockRelationOid(heap_oid, AccessExclusiveLock)?;
        }
    }
    let _ = rel;
    Ok(())
}
