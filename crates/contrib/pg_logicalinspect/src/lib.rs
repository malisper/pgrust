//! `contrib/pg_logicalinspect` — print the header and contents of the
//! serialized logical-decoding snapshot files under pg_logical/snapshots/.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{TransactionId, XIDOID};
use types_error::{PgError, PgResult};
use types_fmgr::{
    byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use types_tuple::tupdesc::TupleDescData;

const LIBRARY: &str = "pg_logicalinspect";

fn composite_tupdesc<'m>(mcx: Mcx<'m>, flinfo: &FmgrInfo) -> PgResult<TupleDescData<'m>> {
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    Ok(resolved.result_tuple_desc.expect("composite result has tupdesc"))
}

fn composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn get_snapbuild_state_desc(state: i32) -> &'static str {
    // SnapBuildState (snapbuild.h): START=-1, BUILDING=0, FULL=1, CONSISTENT=2.
    match state {
        -1 => "start",
        0 => "building",
        1 => "full",
        2 => "consistent",
        _ => "unknown state",
    }
}

#[track_caller]
#[cold]
fn invalid_filename_err(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("invalid snapshot file name \"{name}\"")))
}

// parse_snapshot_filename: sscanf("%X-%X.snap") then a strict round-trip
// compare against "%X-%X.snap", so anything but the canonical spelling of an
// LSN-named snapshot file is rejected.
fn parse_snapshot_filename(name: &str) -> PgResult<u64> {
    fn take_hex(s: &str) -> Option<(u32, &str)> {
        let end = s.find(|c: char| !c.is_ascii_hexdigit()).unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        // >8 hex digits can't round-trip through "%X"; parse wide and let the
        // round-trip check reject.
        let v = u64::from_str_radix(&s[..end], 16).unwrap_or(u64::MAX);
        Some((v as u32, &s[end..]))
    }
    let parsed = take_hex(name).and_then(|(hi, rest)| {
        let rest = rest.strip_prefix('-')?;
        let (lo, _) = take_hex(rest)?;
        Some((hi, lo))
    });
    if let Some((hi, lo)) = parsed {
        if format!("{hi:X}-{lo:X}.snap") == name {
            return Ok(((hi as u64) << 32) | lo as u64);
        }
    }
    Err(invalid_filename_err(name))
}

fn filename_arg(fcinfo: &Fcinfo) -> PgResult<String> {
    // SAFETY: arg 0 is a non-null text (STRICT).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(String::from_utf8_lossy(v.data()).into_owned())
}

fn fc_pg_get_logical_snapshot_meta(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_logical_snapshot_meta: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let lsn = parse_snapshot_filename(&filename_arg(fcinfo)?)?;
    let ondisk = snapbuild::ondisk::restore_snapshot(lsn, false)?
        .expect("missing_ok=false: restore errors instead of returning None");

    let values = [
        Datum::from_u32(ondisk.magic),
        Datum::from_i64(ondisk.checksum as i64),
        Datum::from_u32(ondisk.version),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 3])
}

fn xid_array_or_null(mcx: Mcx<'_>, xids: &[TransactionId]) -> PgResult<(Datum, bool)> {
    if xids.is_empty() {
        return Ok((Datum::null(), true));
    }
    let elems: Vec<Datum> = xids.iter().map(|&x| Datum::from_transaction_id(x)).collect();
    let image = datum::array_build::construct_array_image(mcx, &elems, XIDOID, 4, true, b'i')?;
    Ok((byref_result(mcx, &image)?, false))
}

fn fc_pg_get_logical_snapshot_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_logical_snapshot_info: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let lsn = parse_snapshot_filename(&filename_arg(fcinfo)?)?;
    let ondisk = snapbuild::ondisk::restore_snapshot(lsn, false)?
        .expect("missing_ok=false: restore errors instead of returning None");

    let state_text = varlena_result(varlena::cstring_to_text(
        mcx,
        get_snapbuild_state_desc(ondisk.state).as_bytes(),
    )?);
    let (committed_xip, committed_null) = xid_array_or_null(mcx, &ondisk.committed)?;
    let (catchange_xip, catchange_null) = xid_array_or_null(mcx, &ondisk.catchange)?;

    let values = [
        state_text,
        Datum::from_transaction_id(ondisk.xmin),
        Datum::from_transaction_id(ondisk.xmax),
        Datum::from_u64(ondisk.start_decoding_at),
        Datum::from_u64(ondisk.two_phase_at),
        Datum::from_transaction_id(ondisk.initial_xmin_horizon),
        Datum::from_bool(ondisk.building_full_snapshot),
        Datum::from_bool(ondisk.in_slot_creation),
        Datum::from_u64(ondisk.last_serialized_snapshot),
        Datum::from_transaction_id(ondisk.next_phase_at),
        Datum::from_u32(ondisk.committed.len() as u32),
        committed_xip,
        Datum::from_u32(ondisk.catchange.len() as u32),
        catchange_xip,
    ];
    let mut nulls = [false; 14];
    nulls[11] = committed_null;
    nulls[13] = catchange_null;
    composite_result(mcx, &tupdesc, &values, &nulls)
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_get_logical_snapshot_meta" => fc_pg_get_logical_snapshot_meta,
        "pg_get_logical_snapshot_info" => fc_pg_get_logical_snapshot_info,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_filename_roundtrip() {
        assert_eq!(parse_snapshot_filename("0-40796E18.snap").unwrap(), 0x40796E18);
        assert_eq!(
            parse_snapshot_filename("A-1.snap").unwrap(),
            (0xA_u64 << 32) | 1
        );
        for bad in [
            "0-40796E18.foo",
            "0-40796E18.foo.snap",
            "0--40796E18.snap",
            "-1--40796E18.snap",
            "0/40796E18.snap",
            "",
            "../snapshots",
            "../snapshots/0-40796E18.snap",
            "0-abc.snap",          // lowercase can't round-trip through %X
            "00-1.snap",           // leading zeros can't round-trip
            "1-123456789.snap",    // >8 hex digits can't round-trip
        ] {
            assert!(parse_snapshot_filename(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn snapbuild_state_desc() {
        assert_eq!(get_snapbuild_state_desc(-1), "start");
        assert_eq!(get_snapbuild_state_desc(0), "building");
        assert_eq!(get_snapbuild_state_desc(1), "full");
        assert_eq!(get_snapbuild_state_desc(2), "consistent");
        assert_eq!(get_snapbuild_state_desc(7), "unknown state");
    }
}
