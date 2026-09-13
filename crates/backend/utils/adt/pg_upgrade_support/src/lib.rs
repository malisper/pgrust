//! pg_upgrade_support.c: backend-side setters for pg_upgrade's OID/
//! relfilenumber overrides plus a handful of one-off upgrade helpers.
#![allow(non_snake_case)]

use datum::Datum;
use elog::{elog, ereport};
use types_core::{InvalidXLogRecPtr, Oid, TEXTOID};
use types_error::{ErrorLocation, PgResult, ERRCODE_CANT_CHANGE_RUNTIME_PARAM, ERROR};
use types_fmgr::{
    datum_varlena_packed, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use types_rel::{AccessShareLock, RowExclusiveLock};

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn check_is_binary_upgrade(funcname: &'static str) -> PgResult<()> {
    if init_small::globals::IsBinaryUpgrade() {
        return Ok(());
    }
    ereport(ERROR)
        .errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
        .errmsg("function can only be called when server is in binary upgrade mode")
        .finish(loc(funcname))
}

fn null_arg(funcname: &'static str) -> PgResult<()> {
    elog(ERROR, format!("null argument to {funcname} is not allowed")).map(|_| ())
}

fn arg_bytes<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: catalog arg `i` is text, non-null per caller.
    Ok(unsafe { fcinfo.arg_varlena_packed(i)? }.data())
}

fn arg_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog arg `i` is text (or name-shaped text), non-null per caller.
    let bytes = unsafe { fcinfo.arg_varlena_packed(i)? }.data();
    Ok(core::str::from_utf8(bytes).expect("pg_upgrade_support: text arg is UTF-8"))
}

fn datum_str<'m>(mcx: mcx::Mcx<'m>, d: Datum) -> PgResult<&'m str> {
    // SAFETY: `d` is a live text datum sourced from a catalog array element.
    let bytes = unsafe { datum_varlena_packed(d, mcx)? }.data();
    Ok(core::str::from_utf8(bytes).expect("pg_upgrade_support: text datum is UTF-8"))
}

pub fn fc_binary_upgrade_set_next_pg_type_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_pg_type_oid")?;
    pg_type::SetNextPgTypeOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_array_pg_type_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_array_pg_type_oid")?;
    pg_type::SetNextArrayPgTypeOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_multirange_pg_type_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_multirange_pg_type_oid")?;
    pg_type::SetNextMultirangePgTypeOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_multirange_array_pg_type_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_multirange_array_pg_type_oid")?;
    pg_type::SetNextMultirangeArrayPgTypeOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_pg_enum_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_pg_enum_oid")?;
    pg_enum::SetNextPgEnumOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_pg_tablespace_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_pg_tablespace_oid")?;
    commands_tablespace::SetNextPgTablespaceOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_pg_authid_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_pg_authid_oid")?;
    user::SetNextPgAuthidOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_heap_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_heap_pg_class_oid")?;
    catalog_heap::SetNextHeapPgClassOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_heap_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_heap_relfilenode")?;
    catalog_heap::SetNextHeapPgClassRelfilenumber(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_index_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_index_pg_class_oid")?;
    catalog_index::SetNextIndexPgClassOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_index_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_index_relfilenode")?;
    catalog_index::SetNextIndexPgClassRelfilenumber(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_toast_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_toast_pg_class_oid")?;
    catalog_heap::SetNextToastPgClassOid(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_toast_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_toast_relfilenode")?;
    catalog_heap::SetNextToastPgClassRelfilenumber(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_record_init_privs(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_record_init_privs")?;
    aclchk::SetRecordInitPrivs(fcinfo.arg_bool(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_missing_value(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_missing_value")?;
    let table_id = fcinfo.arg_oid(0);
    let attname = arg_str(fcinfo, 1)?;
    let value = arg_str(fcinfo, 2)?;
    catalog_heap::SetAttrMissing(fcinfo.result_mcx(), table_id, attname, value)?;
    Ok(Datum::from_usize(0))
}

// binary_upgrade_logical_slot_has_caught_up (pg_upgrade_support.c:284):
// true when no decodable WAL records remain after the slot's
// confirmed_flush_lsn (the slot can be upgraded without data loss).
pub fn fc_binary_upgrade_logical_slot_has_caught_up(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_logical_slot_has_caught_up")?;

    // Binary upgrades only allow super-user connections so we must have
    // permission to use replication slots (C: Assert(has_rolreplication)).

    // SAFETY: catalog arg 0 is `name` (NAMEDATALEN block), non-null (strict).
    let slot_name = unsafe { fcinfo.arg_name(0) };
    let slot_name = name_cstr(slot_name);

    // Acquire the given slot.
    slot::ReplicationSlotAcquire(slot_name, true, true)?;

    let my_slot = slot::MyReplicationSlot()
        .expect("binary_upgrade_logical_slot_has_caught_up: slot not acquired");
    debug_assert!(slot::SlotIsLogical(my_slot));
    // Slots must be valid as otherwise we won't be able to scan the WAL.
    debug_assert!(unsafe { my_slot.data.get() }.invalidated == slot::RS_INVAL_NONE);

    let end_of_wal = transam_xlog::GetFlushRecPtr(None);
    let found_pending_wal = slotfuncs::LogicalReplicationSlotHasPendingWal(end_of_wal)?;

    // Clean up.
    slot::ReplicationSlotRelease()?;

    Ok(Datum::from_bool(!found_pending_wal))
}

// NameStr: the NUL-terminated prefix of a NAMEDATALEN block.
fn name_cstr(n: &[u8; 64]) -> &str {
    let len = n.iter().position(|&b| b == 0).unwrap_or(n.len());
    core::str::from_utf8(&n[..len]).expect("pg_upgrade_support: name arg is UTF-8")
}

// binary_upgrade_replorigin_advance (pg_upgrade_support.c:368): update the
// remote_lsn for the subscriber's replication origin.
pub fn fc_binary_upgrade_replorigin_advance(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_replorigin_advance")?;

    // We must ensure a non-NULL subscription name before dereferencing the
    // arguments.
    if fcinfo.argisnull(0) {
        null_arg("binary_upgrade_replorigin_advance")?;
    }

    let subname = arg_str(fcinfo, 0)?;
    let remote_commit = if fcinfo.argisnull(1) {
        InvalidXLogRecPtr
    } else {
        fcinfo.arg_i64(1) as u64
    };

    let mcx = fcinfo.result_mcx();
    let rel = table::table_open(mcx, pg_subscription::SubscriptionRelationId, RowExclusiveLock)?;
    let subid = lsyscache::get_subscription_oid(subname, false)?;

    // ReplicationOriginNameForLogicalRep(subid, InvalidOid) (worker.c):
    // the subscription's own origin is "pg_%u".
    let originname = format!("pg_{subid}");

    // Lock to prevent the replication origin from vanishing.
    lmgr::LockRelationOid(catalog::ReplicationOriginRelationId, RowExclusiveLock)?;
    let node = origin::replorigin_by_name(&originname, false)?;

    // The server will be stopped after setting up the objects in the new
    // cluster and the origins will be flushed during the shutdown checkpoint.
    // This will ensure that the latest LSN values for origin will be
    // available after the upgrade.
    origin::replorigin_advance(
        node,
        remote_commit,
        InvalidXLogRecPtr,
        false, /* backward */
        false, /* WAL log */
    )?;

    lmgr::UnlockRelationOid(catalog::ReplicationOriginRelationId, RowExclusiveLock)?;
    rel.close(RowExclusiveLock)?;

    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_add_sub_rel_state(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_add_sub_rel_state")?;

    if fcinfo.argisnull(0) || fcinfo.argisnull(1) || fcinfo.argisnull(2) {
        null_arg("binary_upgrade_add_sub_rel_state")?;
    }

    let subname = arg_str(fcinfo, 0)?;
    let relid = fcinfo.arg_oid(1);
    let relstate = fcinfo.arg_char(2) as u8;
    let sublsn = if fcinfo.argisnull(3) {
        InvalidXLogRecPtr
    } else {
        fcinfo.arg_i64(3) as u64
    };

    let mcx = fcinfo.result_mcx();
    let subrel = table::table_open(mcx, pg_subscription::SubscriptionRelationId, RowExclusiveLock)?;
    let subid = lsyscache::get_subscription_oid(subname, false)?;
    let rel = relation::relation_open(mcx, relid, AccessShareLock)?;

    pg_subscription::AddSubscriptionRelState(mcx, subid, relid, relstate, sublsn, false)?;

    rel.close(AccessShareLock)?;
    subrel.close(RowExclusiveLock)?;

    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_create_empty_extension(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_create_empty_extension")?;

    if fcinfo.argisnull(0) || fcinfo.argisnull(1) || fcinfo.argisnull(2) || fcinfo.argisnull(3) {
        null_arg("binary_upgrade_create_empty_extension")?;
    }

    let mcx = fcinfo.result_mcx();
    let ext_name = arg_bytes(fcinfo, 0)?;
    let schema_name = arg_str(fcinfo, 1)?;
    let relocatable = fcinfo.arg_bool(2);
    let ext_version = arg_bytes(fcinfo, 3)?;

    let ext_config = if fcinfo.argisnull(4) { None } else { Some(fcinfo.arg(4)) };
    let ext_condition = if fcinfo.argisnull(5) { None } else { Some(fcinfo.arg(5)) };

    let mut required_extensions: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    if !fcinfo.argisnull(6) {
        // SAFETY: catalog arg 6 is `_text`, non-null per the check above.
        let raw = unsafe { fcinfo.arg_varlena_raw(6) };
        let image = detoast::detoast_attr(mcx, raw)?;
        let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &image, TEXTOID, false)?;
        required_extensions = mcx::vec_with_capacity_in(mcx, elems.len())?;
        for &d in elems.iter() {
            let name = datum_str(mcx, d)?;
            required_extensions.push(extension::get_extension_oid(name, false)?);
        }
    }

    extension::create::InsertExtensionTuple(
        mcx,
        ext_name,
        miscinit::GetUserId(),
        catalog_namespace::get_namespace_oid(schema_name, false)?,
        relocatable,
        ext_version,
        ext_config,
        ext_condition,
        &required_extensions,
    )?;

    Ok(Datum::from_usize(0))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

pub const PG_UPGRADE_SUPPORT_BUILTINS: &[FmgrBuiltin] = &[
    b(3582, "binary_upgrade_set_next_pg_type_oid", 1, true, fc_binary_upgrade_set_next_pg_type_oid),
    b(
        3584,
        "binary_upgrade_set_next_array_pg_type_oid",
        1,
        true,
        fc_binary_upgrade_set_next_array_pg_type_oid,
    ),
    b(
        3586,
        "binary_upgrade_set_next_heap_pg_class_oid",
        1,
        true,
        fc_binary_upgrade_set_next_heap_pg_class_oid,
    ),
    b(
        3587,
        "binary_upgrade_set_next_index_pg_class_oid",
        1,
        true,
        fc_binary_upgrade_set_next_index_pg_class_oid,
    ),
    b(
        3588,
        "binary_upgrade_set_next_toast_pg_class_oid",
        1,
        true,
        fc_binary_upgrade_set_next_toast_pg_class_oid,
    ),
    b(3589, "binary_upgrade_set_next_pg_enum_oid", 1, true, fc_binary_upgrade_set_next_pg_enum_oid),
    b(3590, "binary_upgrade_set_next_pg_authid_oid", 1, true, fc_binary_upgrade_set_next_pg_authid_oid),
    b(3591, "binary_upgrade_create_empty_extension", 7, false, fc_binary_upgrade_create_empty_extension),
    b(4083, "binary_upgrade_set_record_init_privs", 1, true, fc_binary_upgrade_set_record_init_privs),
    b(4101, "binary_upgrade_set_missing_value", 3, true, fc_binary_upgrade_set_missing_value),
    b(
        4390,
        "binary_upgrade_set_next_multirange_pg_type_oid",
        1,
        true,
        fc_binary_upgrade_set_next_multirange_pg_type_oid,
    ),
    b(
        4391,
        "binary_upgrade_set_next_multirange_array_pg_type_oid",
        1,
        true,
        fc_binary_upgrade_set_next_multirange_array_pg_type_oid,
    ),
    b(4545, "binary_upgrade_set_next_heap_relfilenode", 1, true, fc_binary_upgrade_set_next_heap_relfilenode),
    b(
        4546,
        "binary_upgrade_set_next_index_relfilenode",
        1,
        true,
        fc_binary_upgrade_set_next_index_relfilenode,
    ),
    b(
        4547,
        "binary_upgrade_set_next_toast_relfilenode",
        1,
        true,
        fc_binary_upgrade_set_next_toast_relfilenode,
    ),
    b(
        4548,
        "binary_upgrade_set_next_pg_tablespace_oid",
        1,
        true,
        fc_binary_upgrade_set_next_pg_tablespace_oid,
    ),
    b(
        6312,
        "binary_upgrade_logical_slot_has_caught_up",
        1,
        true,
        fc_binary_upgrade_logical_slot_has_caught_up,
    ),
    b(6319, "binary_upgrade_add_sub_rel_state", 4, false, fc_binary_upgrade_add_sub_rel_state),
    b(6320, "binary_upgrade_replorigin_advance", 2, false, fc_binary_upgrade_replorigin_advance),
];

#[cfg(test)]
mod tests {
    use super::*;
    use types_error::ERRCODE_CANT_CHANGE_RUNTIME_PARAM;

    #[test]
    fn guard_rejects_outside_binary_upgrade_mode() {
        init_small::globals::SetIsBinaryUpgrade(false);
        let err = check_is_binary_upgrade("binary_upgrade_set_next_pg_type_oid").unwrap_err();
        assert_eq!(err.sqlstate, ERRCODE_CANT_CHANGE_RUNTIME_PARAM);
        assert_eq!(
            err.message,
            "function can only be called when server is in binary upgrade mode"
        );
    }

    #[test]
    fn guard_passes_in_binary_upgrade_mode() {
        init_small::globals::SetIsBinaryUpgrade(true);
        assert!(check_is_binary_upgrade("binary_upgrade_set_next_pg_type_oid").is_ok());
        init_small::globals::SetIsBinaryUpgrade(false);
    }

}
