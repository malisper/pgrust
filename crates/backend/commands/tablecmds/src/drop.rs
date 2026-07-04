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

// dropmsgstringarray (tablecmds.c), reachable-relkind subset.
struct DropMsgStrings {
    nonexistent_msg: &'static str,
    skipping_msg: &'static str,
    nota_msg: &'static str,
    drophint_msg: &'static str,
}

fn drop_msg_strings(relkind: u8) -> &'static DropMsgStrings {
    match relkind {
        RELKIND_RELATION => &DropMsgStrings {
            nonexistent_msg: "table \"{}\" does not exist",
            skipping_msg: "table \"{}\" does not exist, skipping",
            nota_msg: "\"{}\" is not a table",
            drophint_msg: "Use DROP TABLE to remove a table.",
        },
        types_rel::RELKIND_SEQUENCE => &DropMsgStrings {
            nonexistent_msg: "sequence \"{}\" does not exist",
            skipping_msg: "sequence \"{}\" does not exist, skipping",
            nota_msg: "\"{}\" is not a sequence",
            drophint_msg: "Use DROP SEQUENCE to remove a sequence.",
        },
        types_rel::RELKIND_VIEW => &DropMsgStrings {
            nonexistent_msg: "view \"{}\" does not exist",
            skipping_msg: "view \"{}\" does not exist, skipping",
            nota_msg: "\"{}\" is not a view",
            drophint_msg: "Use DROP VIEW to remove a view.",
        },
        types_rel::RELKIND_MATVIEW => &DropMsgStrings {
            nonexistent_msg: "materialized view \"{}\" does not exist",
            skipping_msg: "materialized view \"{}\" does not exist, skipping",
            nota_msg: "\"{}\" is not a materialized view",
            drophint_msg: "Use DROP MATERIALIZED VIEW to remove a materialized view.",
        },
        types_rel::RELKIND_INDEX => &DropMsgStrings {
            nonexistent_msg: "index \"{}\" does not exist",
            skipping_msg: "index \"{}\" does not exist, skipping",
            nota_msg: "\"{}\" is not an index",
            drophint_msg: "Use DROP INDEX to remove an index.",
        },
        other => panic!("drop_msg_strings: relkind {other} entry unported"),
    }
}

fn fmt1(template: &str, arg: &str) -> String {
    template.replacen("{}", arg, 1)
}

fn DropErrorMsgWrongType(relname: &str, wrongkind: u8, rightkind: u8) -> Box<PgError> {
    let rentry = drop_msg_strings(rightkind);
    let wentry = drop_msg_strings(wrongkind);
    let mut e = PgError::new(ERROR, fmt1(rentry.nota_msg, relname))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE);
    if !wentry.drophint_msg.is_empty() {
        e = e.with_hint(wentry.drophint_msg);
    }
    Box::new(e)
}

fn DropErrorMsgNonExistent(rel: &RangeVar<'_>, rightkind: u8, missing_ok: bool) -> PgResult<()> {
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
    let rentry = drop_msg_strings(rightkind);
    let relname = rel.relname;
    if !missing_ok {
        return Err(Box::new(
            PgError::new(ERROR, fmt1(rentry.nonexistent_msg, relname))
                .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
        ));
    }
    elog_seams::ereport_msg::call(NOTICE, fmt1(rentry.skipping_msg, relname), None)?;
    Ok(())
}

pub fn RemoveRelations<'mcx>(mcx: Mcx<'mcx>, drop: &DropStmt<'mcx>) -> PgResult<()> {
    if drop.concurrent {
        unported("RemoveRelations: DROP INDEX CONCURRENTLY");
    }
    let expected_relkind = match drop.removeType {
        ObjectType::OBJECT_TABLE => RELKIND_RELATION,
        ObjectType::OBJECT_INDEX => types_rel::RELKIND_INDEX,
        ObjectType::OBJECT_MATVIEW => types_rel::RELKIND_MATVIEW,
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
            DropErrorMsgNonExistent(&rel, expected_relkind, drop.missing_ok)?;
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

    let actual_expected = if relkind == RELKIND_PARTITIONED_TABLE {
        RELKIND_RELATION
    } else if relkind == types_rel::RELKIND_PARTITIONED_INDEX {
        types_rel::RELKIND_INDEX
    } else {
        relkind
    };
    if actual_expected != expected_relkind {
        return Err(DropErrorMsgWrongType(rel.relname, relkind, expected_relkind));
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

    // Queries lock parents before partitions; same DIVERGENCE note as above
    // for the retry bookkeeping (state->partParentOid).
    if relispartition {
        let part_parent_oid = pg_inherits::get_partition_parent(mcx, relOid, true)?;
        if part_parent_oid != InvalidOid {
            lmgr::LockRelationOid(part_parent_oid, AccessExclusiveLock)?;
        }
    }
    let _ = rel;
    Ok(())
}
