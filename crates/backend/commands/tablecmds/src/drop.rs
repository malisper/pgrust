// RemoveRelations + RangeVarCallbackForDropRelation (tablecmds.c) over the
// relation removeTypes.
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_TABLE, ERROR, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType};
use rel_vocab::RangeVar;
use types_nodes::NodeList;
use types_rel::{AccessExclusiveLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

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

// dropmsgstringarray (tablecmds.c): (kind, nonexistent_code, noun, drop hint).
struct DropMsgStrings {
    kind: u8,
    nonexistent_code: types_error::SqlState,
    noun: &'static str,
    nota: &'static str,
    drophint: &'static str,
}

const DROP_MSG_STRINGS: &[DropMsgStrings] = &[
    DropMsgStrings {
        kind: RELKIND_RELATION,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE,
        noun: "table",
        nota: "is not a table",
        drophint: "Use DROP TABLE to remove a table.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_SEQUENCE,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE,
        noun: "sequence",
        nota: "is not a sequence",
        drophint: "Use DROP SEQUENCE to remove a sequence.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_VIEW,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE,
        noun: "view",
        nota: "is not a view",
        drophint: "Use DROP VIEW to remove a view.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_MATVIEW,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE,
        noun: "materialized view",
        nota: "is not a materialized view",
        drophint: "Use DROP MATERIALIZED VIEW to remove a materialized view.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_INDEX,
        nonexistent_code: types_error::ERRCODE_UNDEFINED_OBJECT,
        noun: "index",
        nota: "is not an index",
        drophint: "Use DROP INDEX to remove an index.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_COMPOSITE_TYPE,
        nonexistent_code: types_error::ERRCODE_UNDEFINED_OBJECT,
        noun: "type",
        nota: "is not a type",
        drophint: "Use DROP TYPE to remove a type.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_FOREIGN_TABLE,
        nonexistent_code: types_error::ERRCODE_UNDEFINED_OBJECT,
        noun: "foreign table",
        nota: "is not a foreign table",
        drophint: "Use DROP FOREIGN TABLE to remove a foreign table.",
    },
    DropMsgStrings {
        kind: RELKIND_PARTITIONED_TABLE,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE,
        noun: "table",
        nota: "is not a table",
        drophint: "Use DROP TABLE to remove a table.",
    },
    DropMsgStrings {
        kind: types_rel::RELKIND_PARTITIONED_INDEX,
        nonexistent_code: types_error::ERRCODE_UNDEFINED_OBJECT,
        noun: "index",
        nota: "is not an index",
        drophint: "Use DROP INDEX to remove an index.",
    },
];

fn drop_msg_entry(kind: u8) -> Option<&'static DropMsgStrings> {
    DROP_MSG_STRINGS.iter().find(|e| e.kind == kind)
}

fn DropErrorMsgNonExistent(rel: &RangeVar<'_>, rightkind: u8, missing_ok: bool) -> PgResult<()> {
    if let Some(schemaname) = rel.schemaname {
        if catalog_namespace::get_namespace_oid(schemaname, true)? == InvalidOid {
            if !missing_ok {
                return Err(Box::new(
                    PgError::new(ERROR, format!("schema \"{schemaname}\" does not exist"))
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
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
    let rentry = drop_msg_entry(rightkind).expect("relkind is in dropmsgstringarray");
    let relname = rel.relname;
    let noun = rentry.noun;
    if !missing_ok {
        return Err(Box::new(
            PgError::new(ERROR, format!("{noun} \"{relname}\" does not exist"))
                .with_sqlstate(rentry.nonexistent_code),
        ));
    }
    elog_seams::ereport_msg::call(
        NOTICE,
        format!("{noun} \"{relname}\" does not exist, skipping"),
        None,
    )?;
    Ok(())
}

// DropErrorMsgWrongType (tablecmds.c).
#[track_caller]
#[cold]
fn DropErrorMsgWrongType(relname: &str, wrongkind: u8, rightkind: u8) -> Box<PgError> {
    let rentry = drop_msg_entry(rightkind).expect("relkind is in dropmsgstringarray");
    let mut e = PgError::new(ERROR, format!("\"{relname}\" {}", rentry.nota))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE);
    if let Some(wentry) = drop_msg_entry(wrongkind) {
        e = e.with_hint(wentry.drophint);
    }
    Box::new(e)
}

pub fn RemoveRelations<'mcx>(mcx: Mcx<'mcx>, drop: &DropStmt<'mcx>) -> PgResult<()> {
    let mut flags = 0;
    let mut lockmode = AccessExclusiveLock;
    if drop.concurrent {
        lockmode = types_rel::ShareUpdateExclusiveLock;
        debug_assert!(matches!(drop.removeType, ObjectType::OBJECT_INDEX));
        if drop.objects.len() != 1 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "DROP INDEX CONCURRENTLY does not support dropping multiple objects"
                        .to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if matches!(drop.behavior, types_nodes::parsenodes::DropBehavior::DROP_CASCADE) {
            return Err(Box::new(
                PgError::new(ERROR, "DROP INDEX CONCURRENTLY does not support CASCADE".to_string())
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    }
    let expected_relkind = match drop.removeType {
        ObjectType::OBJECT_TABLE => RELKIND_RELATION,
        ObjectType::OBJECT_INDEX => types_rel::RELKIND_INDEX,
        ObjectType::OBJECT_SEQUENCE => types_rel::RELKIND_SEQUENCE,
        ObjectType::OBJECT_VIEW => types_rel::RELKIND_VIEW,
        ObjectType::OBJECT_MATVIEW => types_rel::RELKIND_MATVIEW,
        ObjectType::OBJECT_FOREIGN_TABLE => types_rel::RELKIND_FOREIGN_TABLE,
        other => panic!("unrecognized drop object type: {other:?}"),
    };

    let mut objects = catalog_dependency::ObjectAddresses::new();

    for cell in drop.objects.iter() {
        let names = cell.as_list().expect("DROP object is a name list");
        let rel = makeRangeVarFromNameList(&names);

        inval::local::AcceptInvalidationMessages()?;

        let heap_lockmode = if drop.concurrent {
            types_rel::ShareUpdateExclusiveLock
        } else {
            AccessExclusiveLock
        };
        let actual_relkind = core::cell::Cell::new(0u8);
        let actual_relpersistence = core::cell::Cell::new(0u8);
        let heap_oid = core::cell::Cell::new(InvalidOid);
        let mut callback = |rv: &RangeVar<'_>, relOid: Oid, oldRelOid: Oid| {
            RangeVarCallbackForDropRelation(
                mcx,
                rv,
                relOid,
                oldRelOid,
                expected_relkind,
                heap_lockmode,
                &actual_relkind,
                &actual_relpersistence,
                &heap_oid,
            )
        };
        let relOid = catalog_namespace::RangeVarGetRelidExtended(
            &rel,
            lockmode,
            catalog_namespace::RVR_MISSING_OK,
            Some(&mut callback),
        )?;

        if relOid == InvalidOid {
            DropErrorMsgNonExistent(&rel, expected_relkind, drop.missing_ok)?;
            continue;
        }

        if drop.concurrent
            && actual_relpersistence.get() != types_core::RELPERSISTENCE_TEMP
        {
            debug_assert!(
                drop.objects.len() == 1
                    && matches!(drop.removeType, ObjectType::OBJECT_INDEX)
            );
            flags |= catalog_dependency::PERFORM_DELETION_CONCURRENTLY;
        }

        if flags & catalog_dependency::PERFORM_DELETION_CONCURRENTLY != 0
            && actual_relkind.get() == types_rel::RELKIND_PARTITIONED_INDEX
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cannot drop partitioned index \"{}\" concurrently",
                        rel.relname
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }

        // DROP of a partitioned index locks every table of the partition
        // tree before any child index is locked (tablecmds.c:1671-1682):
        // otherwise child indexes get locked before their tables, deadlocking
        // against sessions locking table-then-index.
        if actual_relkind.get() == types_rel::RELKIND_PARTITIONED_INDEX {
            pg_inherits::find_all_inheritors(mcx, heap_oid.get(), heap_lockmode)?;
        }

        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            RELATION_RELATION_ID,
            relOid,
        ));
    }

    catalog_dependency::performMultipleDeletions(mcx, &objects, drop.behavior, flags)
}

#[allow(clippy::too_many_arguments)]
fn RangeVarCallbackForDropRelation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RangeVar<'_>,
    relOid: Oid,
    _oldRelOid: Oid,
    expected_relkind: u8,
    heap_lockmode: types_storage::lock::LOCKMODE,
    actual_relkind: &core::cell::Cell<u8>,
    actual_relpersistence: &core::cell::Cell<u8>,
    heap_oid_out: &core::cell::Cell<Oid>,
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
    let relpersistence = get(17).as_i8() as u8;
    let relkind = get(18).as_i8() as u8;
    let relispartition = get(28).as_bool();
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;
    actual_relkind.set(relkind);
    actual_relpersistence.set(relpersistence);

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

    // DROP is allowed to either the table owner or the schema owner.
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relOid, miscinit::GetUserId())?
        && !aclchk::object_ownercheck(
            types_core::NAMESPACE_RELATION_ID,
            relnamespace,
            miscinit::GetUserId(),
        )?
    {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(relkind),
            rel.relname,
        )?;
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
        heap_oid_out.set(heap_oid);
        if heap_oid != InvalidOid {
            lmgr::LockRelationOid(heap_oid, heap_lockmode)?;
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
    Ok(())
}
