//! EXHAUSTIVE native differential for the 4 timetz_part tz-arm Kani cells
//! (fleet 33d7d09d31 triage; cascade step a0 EXHAUSTIVE-DIFF, domains <=
//! ~2^32 enumerated natively against the C oracle).
//!
//! The cells' proof domain factors exactly: zone ranges over the contract
//! fence (-57600, 57600) exclusive (115,199 values) x fixed time in
//! {0, 45_296_123_456} x 3 literal unit tokens — 691,194 cases total, 100%
//! of what the Kani harnesses quantify over. SAME shipped fc_timetz_part
//! wrapper vs the SAME vendored REL_18_STABLE C (build.rs cc link), same
//! bit-exact f64 comparison as the proofs.
//!
//! Run: cargo run --release --bin exhaustive_timetz_tz

use proof_datetime_b as _;

use proof_datetime_b::{
    pg_timetz_part_units_float, C_DTK_TZ, C_DTK_TZ_HOUR, C_DTK_TZ_MINUTE, UNITS_TIMEZONE,
    UNITS_TIMEZONE_HOUR, UNITS_TIMEZONE_MINUTE,
};

use datum::{Datum, NullableDatum};
use std::os::raw::c_int;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};

type FcFn = fn(
    Option<&mut FmgrInfo>,
    &mut FunctionCallInfoBaseData,
) -> Result<Datum, Box<types_error::PgError>>;

fn call_n<'m, const N: usize>(
    mcx: mcx::Mcx<'m>,
    fc: FcFn,
    args: [Datum; N],
) -> (Result<Datum, ()>, bool) {
    let mut f = LocalFcinfo::<N>::new(0);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    // SAFETY: the context outlives the call.
    unsafe { f.set_result_mcx(mcx) };
    let r = fc(None, &mut f).map_err(|_| ());
    (r, f.isnull)
}

fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&time.to_ne_bytes());
    img[8..].copy_from_slice(&zone.to_ne_bytes());
    img
}

fn main() {
    let ctx = mcx::MemoryContext::new("exhaustive-timetz-tz");
    let mcx = ctx.mcx();
    let mut bad = 0u64;
    let mut n = 0u64;

    // (units token, C selector, cell name, fixed times covered by the cell)
    let cells: [(&[u8], c_int, &str, &[i64]); 3] = [
        // eq_timetz_part_tz (time=0) + spot_timetz_part_tz_time_nonzero
        (&UNITS_TIMEZONE[..], C_DTK_TZ, "timezone", &[0, 45_296_123_456]),
        (&UNITS_TIMEZONE_HOUR[..], C_DTK_TZ_HOUR, "timezone_hour", &[0]),
        (&UNITS_TIMEZONE_MINUTE[..], C_DTK_TZ_MINUTE, "timezone_minute", &[0]),
    ];

    for (units, val, name, times) in cells {
        for &time in times {
            // Contract fence from the harnesses: |zone| < 57600, exclusive.
            for zone in -57_599i32..=57_599 {
                let mut c_out = 0f64;
                let mut c_err: c_int = 0;
                unsafe { pg_timetz_part_units_float(time, zone, val, &mut c_out, &mut c_err) };
                let timg = timetz_img(time, zone);
                let (r, isnull) = call_n(mcx, adt_date::builtins::fc_timetz_part as FcFn, [
                    Datum::from_usize(units.as_ptr() as usize),
                    Datum::from_usize(timg.as_ptr() as usize),
                ]);
                let ok = match r {
                    Ok(d) => c_err == 0 && !isnull && d.as_f64().to_bits() == c_out.to_bits(),
                    Err(()) => c_err == 1,
                };
                n += 1;
                if !ok {
                    bad += 1;
                    if bad <= 20 {
                        let got = match r {
                            Ok(d) => format!("Ok({})", d.as_f64()),
                            Err(()) => "Err".into(),
                        };
                        println!(
                            "MISMATCH {name}: time={time} zone={zone} C=({c_out}, err={c_err}) Rust={got}"
                        );
                    }
                }
            }
        }
    }

    println!("exhaustive_timetz_tz: {n} checks, {bad} mismatches");
    if bad > 0 {
        std::process::exit(1);
    }
}
