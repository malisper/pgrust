//! src/test/modules/test_custom_types: `int_custom`, a fake int4 whose typanalyze
//! the test switches to one that refuses the column or leaves its stats invalid.
// upstream 017e4e395d0d (18.4): test_custom_types: Test module with fancy custom data types

#![allow(non_snake_case)]

use commands_analyze::{FetchSource, VacAttrStats};
use datum::Datum;
use types_error::PgResult;
use types_fmgr::{cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "test_custom_types";

fn int_custom_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    adt_int::builtins::fc_int4in(flinfo, fcinfo)
}

fn int_custom_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let arg1 = fcinfo.arg_i32(0);
    let mut tmp = [0u8; 12];
    let len = adt_int::int4out(arg1, &mut tmp);
    let mut out: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(fcinfo.result_mcx(), len + 1)?;
    mcx::vec_append_bytes(&mut out, &tmp[..=len])?;
    Ok(cstring_result(out))
}

fn int_custom_typanalyze_false(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(false))
}

fn int_custom_invalid_stats(
    stats: &mut VacAttrStats<'_>,
    _fetch: &FetchSource<'_, '_>,
    _samplerows: i32,
    _totalrows: f64,
) -> PgResult<()> {
    stats.set_stats_valid(false);
    Ok(())
}

fn int_custom_typanalyze_invalid(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    commands_analyze::with_typanalyze_stats(fcinfo, |stats| {
        if stats.attstattarget() < 0 {
            stats.set_attstattarget(guc_tables::vars::default_statistics_target.read());
        }
        // Buggy number, no need to care as long as it is positive.
        stats.set_minrows(300);
        stats.set_compute_stats(int_custom_invalid_stats);
    });
    Ok(Datum::from_bool(true))
}

macro_rules! int_custom_cmp_fn {
    ($name:ident, $op:tt) => {
        fn $name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let arg1 = fcinfo.arg_i32(0);
            let arg2 = fcinfo.arg_i32(1);
            Ok(Datum::from_bool(arg1 $op arg2))
        }
    };
}

int_custom_cmp_fn!(int_custom_eq, ==);
int_custom_cmp_fn!(int_custom_ne, !=);
int_custom_cmp_fn!(int_custom_lt, <);
int_custom_cmp_fn!(int_custom_le, <=);
int_custom_cmp_fn!(int_custom_gt, >);
int_custom_cmp_fn!(int_custom_ge, >=);

fn int_custom_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let arg1 = fcinfo.arg_i32(0);
    let arg2 = fcinfo.arg_i32(1);
    Ok(Datum::from_i32(if arg1 < arg2 {
        -1
    } else if arg1 > arg2 {
        1
    } else {
        0
    }))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "int_custom_in" => int_custom_in,
        "int_custom_out" => int_custom_out,
        "int_custom_typanalyze_false" => int_custom_typanalyze_false,
        "int_custom_typanalyze_invalid" => int_custom_typanalyze_invalid,
        "int_custom_eq" => int_custom_eq,
        "int_custom_ne" => int_custom_ne,
        "int_custom_lt" => int_custom_lt,
        "int_custom_le" => int_custom_le,
        "int_custom_gt" => int_custom_gt,
        "int_custom_ge" => int_custom_ge,
        "int_custom_cmp" => int_custom_cmp,
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
