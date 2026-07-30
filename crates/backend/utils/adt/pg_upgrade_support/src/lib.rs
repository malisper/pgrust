//! pg_upgrade_support.c: backend-side setters for pg_upgrade's OID/
//! relfilenumber overrides plus a handful of one-off upgrade helpers.
#![allow(non_snake_case)]

use core::cell::Cell;

use datum::Datum;
use elog::{elog, ereport};
use types_core::{InvalidOid, InvalidXLogRecPtr, Oid, OidIsValid, TEXTOID};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERROR,
};
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

// binary_upgrade_next_{heap,index,toast}_pg_class_{oid,relfilenumber} and
// binary_upgrade_next_pg_authid_oid, binary_upgrade_record_init_privs: no
// consumer is ported yet (heap.c relfilenode assignment, user.c pg_authid
// creation, aclchk.c init-privs recording are all still C-shaped elsewhere
// in this repo). The setter itself is real state, held here until a
// consumer lands; nothing reads it back yet.
macro_rules! local_next_oid {
    ($cell:ident, $take:ident) => {
        thread_local! {
            static $cell: Cell<Oid> = const { Cell::new(InvalidOid) };
        }

        #[allow(dead_code)]
        fn $take() -> Option<Oid> {
            let oid = $cell.get();
            if OidIsValid(oid) {
                $cell.set(InvalidOid);
                Some(oid)
            } else {
                None
            }
        }
    };
}

local_next_oid!(NEXT_HEAP_PG_CLASS_OID, take_next_heap_pg_class_oid);
local_next_oid!(NEXT_HEAP_PG_CLASS_RELFILENUMBER, take_next_heap_pg_class_relfilenumber);
local_next_oid!(NEXT_INDEX_PG_CLASS_OID, take_next_index_pg_class_oid);
local_next_oid!(NEXT_INDEX_PG_CLASS_RELFILENUMBER, take_next_index_pg_class_relfilenumber);
local_next_oid!(NEXT_TOAST_PG_CLASS_OID, take_next_toast_pg_class_oid);
local_next_oid!(NEXT_TOAST_PG_CLASS_RELFILENUMBER, take_next_toast_pg_class_relfilenumber);
local_next_oid!(NEXT_PG_AUTHID_OID, take_next_pg_authid_oid);

thread_local! {
    static RECORD_INIT_PRIVS: Cell<bool> = const { Cell::new(false) };
}

#[allow(dead_code)]
fn record_init_privs() -> bool {
    RECORD_INIT_PRIVS.get()
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
    NEXT_PG_AUTHID_OID.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_heap_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_heap_pg_class_oid")?;
    NEXT_HEAP_PG_CLASS_OID.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_heap_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_heap_relfilenode")?;
    NEXT_HEAP_PG_CLASS_RELFILENUMBER.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_index_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_index_pg_class_oid")?;
    NEXT_INDEX_PG_CLASS_OID.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_index_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_index_relfilenode")?;
    NEXT_INDEX_PG_CLASS_RELFILENUMBER.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_toast_pg_class_oid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_toast_pg_class_oid")?;
    NEXT_TOAST_PG_CLASS_OID.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_next_toast_relfilenode(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_next_toast_relfilenode")?;
    NEXT_TOAST_PG_CLASS_RELFILENUMBER.set(fcinfo.arg_oid(0));
    Ok(Datum::from_usize(0))
}

pub fn fc_binary_upgrade_set_record_init_privs(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_record_init_privs")?;
    RECORD_INIT_PRIVS.set(fcinfo.arg_bool(0));
    Ok(Datum::from_usize(0))
}

#[track_caller]
#[cold]
#[inline(never)]
fn upgrade_unported(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("function {name} is not yet implemented"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn fc_binary_upgrade_set_missing_value(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_set_missing_value")?;
    let _ = fcinfo;
    // unported: SetAttrMissing (commands/tablecmds.c).
    Err(upgrade_unported("binary_upgrade_set_missing_value"))
}

pub fn fc_binary_upgrade_logical_slot_has_caught_up(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_logical_slot_has_caught_up")?;
    let _ = fcinfo;
    // unported: LogicalReplicationSlotHasPendingWal (replication/walsender.c).
    Err(upgrade_unported("binary_upgrade_logical_slot_has_caught_up"))
}

pub fn fc_binary_upgrade_replorigin_advance(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_is_binary_upgrade("binary_upgrade_replorigin_advance")?;
    if fcinfo.argisnull(0) {
        null_arg("binary_upgrade_replorigin_advance")?;
    }
    // unported: ReplicationOriginNameForLogicalRep / replorigin_by_name
    // (replication/origin.c).
    Err(upgrade_unported("binary_upgrade_replorigin_advance"))
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
    let ext_name = arg_str(fcinfo, 0)?;
    let schema_name = arg_str(fcinfo, 1)?;
    let relocatable = fcinfo.arg_bool(2);
    let ext_version = arg_str(fcinfo, 3)?;

    let ext_config = if fcinfo.argisnull(4) { None } else { Some(fcinfo.arg(4)) };
    let ext_condition = if fcinfo.argisnull(5) { None } else { Some(fcinfo.arg(5)) };

    let mut required_extensions: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    if !fcinfo.argisnull(6) {
        // SAFETY: catalog arg 6 is `_text`, non-null per the check above.
        let image = unsafe { fcinfo.arg_varlena_packed(6)? };
        let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, image.data(), TEXTOID, false)?;
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

    #[test]
    fn local_next_oid_set_take_once_roundtrip() {
        assert_eq!(take_next_heap_pg_class_oid(), None);
        NEXT_HEAP_PG_CLASS_OID.set(500);
        assert_eq!(take_next_heap_pg_class_oid(), Some(500));
        assert_eq!(take_next_heap_pg_class_oid(), None);

        assert_eq!(take_next_pg_authid_oid(), None);
        NEXT_PG_AUTHID_OID.set(501);
        assert_eq!(take_next_pg_authid_oid(), Some(501));
        assert_eq!(take_next_pg_authid_oid(), None);
    }

    #[test]
    fn record_init_privs_set_get_roundtrip() {
        RECORD_INIT_PRIVS.set(false);
        assert!(!record_init_privs());
        RECORD_INIT_PRIVS.set(true);
        assert!(record_init_privs());
        RECORD_INIT_PRIVS.set(false);
    }
}
