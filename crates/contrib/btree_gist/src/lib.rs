//! contrib/btree_gist — GiST operator classes over the btree-equivalent
//! types. Support procs dispatch through the dfmgr builtin-library registry
//! (pg_trgm precedent); the num/var frameworks are monomorphized per type
//! where C uses gbtree_ninfo/gbtree_vinfo fn-pointer tables.

mod num;
mod var;

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_gist::{GistEntryVector, GistSortSupportShim, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

use adt_datetime::consts::Interval;
use num::{interval_to_sec, penalty_check_max_float, penalty_num, Ctx, NumOps};
use var::VarOps;

const LIBRARY: &str = "btree_gist";
const VARHDRSZ: usize = 4;

// ===========================================================================
// fmgr gist protocol helpers (pg_trgm/tsgistidx precedent).
// ===========================================================================

unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    // SAFETY: plain-old-data copy of the entry into the result mcx.
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

fn image_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

// PG_DETOAST_DATUM: always yields a 4B-header image.
fn detoasted_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = VARHDRSZ + src.len();
            let mut buf: mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
            mcx::vec_append_bytes(
                &mut buf,
                &varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
            )?;
            mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, raw)?;
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        }
    }
}

fn num_key<'a, T: NumOps>(d: Datum) -> &'a [u8] {
    // SAFETY: gbtreekey images are plain fixed-size byte blocks.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 2 * T::SIZE) }
}

fn var_key<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: var keys are plain 4B-header images (gbt_var_decompress ran).
    unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) }
}

fn ctx<'a>(f: Option<&'a mut FmgrInfo>, fcinfo: &Fcinfo) -> Ctx<'a> {
    Ctx { flinfo: f, collation: fcinfo.get_collation() }
}

fn out_bool(fcinfo: &Fcinfo, i: usize, v: bool) {
    // SAFETY: out-param live in the caller frame.
    unsafe { *(fcinfo.arg(i).as_usize() as *mut bool) = v };
}

fn strategy_arg(fcinfo: &Fcinfo) -> u16 {
    fcinfo.arg(2).as_u32() as u16
}

fn interval_result(fcinfo: &Fcinfo, i: &Interval) -> PgResult<Datum> {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&i.time.to_ne_bytes());
    img[8..12].copy_from_slice(&i.day.to_ne_bytes());
    img[12..].copy_from_slice(&i.month.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &img)
}

fn read_interval(b: &[u8]) -> Interval {
    Interval {
        time: i64::from_ne_bytes(b[..8].try_into().unwrap()),
        day: i32::from_ne_bytes(b[8..12].try_into().unwrap()),
        month: i32::from_ne_bytes(b[12..16].try_into().unwrap()),
    }
}

fn deref_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: by-ref fixed-length datum.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

// ===========================================================================
// Per-type num descriptors.
// ===========================================================================

trait NumProc: NumOps + Sized {
    fn val_from_datum(d: Datum, mcx: Mcx<'_>) -> PgResult<Self::V>;
    fn fetch_datum(lower: Self::V, key: Datum) -> Datum;
    const CONSISTENT_RECHECK: bool = false;
    fn penalty(
        o: (Self::V, Self::V),
        n: (Self::V, Self::V),
        natts: u16,
        ctx: &mut Ctx,
    ) -> PgResult<f32>;
    fn ssup_cmp(x: Datum, y: Datum, coll: Oid, mcx: Mcx<'_>) -> PgResult<i32>;
}

fn rd_i16(b: &[u8]) -> i16 {
    i16::from_ne_bytes(b[..2].try_into().unwrap())
}
fn rd_i32(b: &[u8]) -> i32 {
    i32::from_ne_bytes(b[..4].try_into().unwrap())
}
fn rd_i64(b: &[u8]) -> i64 {
    i64::from_ne_bytes(b[..8].try_into().unwrap())
}
fn rd_u32(b: &[u8]) -> u32 {
    u32::from_ne_bytes(b[..4].try_into().unwrap())
}
fn rd_f32(b: &[u8]) -> f32 {
    f32::from_ne_bytes(b[..4].try_into().unwrap())
}
fn rd_f64(b: &[u8]) -> f64 {
    f64::from_ne_bytes(b[..8].try_into().unwrap())
}

macro_rules! scalar_numops {
    ($name:ident, $v:ty, $size:expr, $indexsize:expr, $read:expr) => {
        struct $name;
        impl NumOps for $name {
            const SIZE: usize = $size;
            const INDEXSIZE: usize = $indexsize;
            type V = $v;
            fn read(b: &[u8]) -> $v {
                $read(b)
            }
            fn write(out: &mut [u8], v: $v) {
                out.copy_from_slice(&v.to_ne_bytes())
            }
            fn gt(a: $v, b: $v, _: &mut Ctx) -> PgResult<bool> {
                Ok(a > b)
            }
            fn ge(a: $v, b: $v, _: &mut Ctx) -> PgResult<bool> {
                Ok(a >= b)
            }
            fn eq(a: $v, b: $v, _: &mut Ctx) -> PgResult<bool> {
                Ok(a == b)
            }
            fn le(a: $v, b: $v, _: &mut Ctx) -> PgResult<bool> {
                Ok(a <= b)
            }
            fn lt(a: $v, b: $v, _: &mut Ctx) -> PgResult<bool> {
                Ok(a < b)
            }
            // C's comparator shape kept exactly (floats: NaN pairs fall
            // through == to the -1 arm).
            #[allow(clippy::float_cmp)]
            fn key_cmp(a: ($v, $v), b: ($v, $v), _: &mut Ctx) -> PgResult<i32> {
                Ok(if a.0 == b.0 {
                    if a.1 == b.1 {
                        0
                    } else if a.1 > b.1 {
                        1
                    } else {
                        -1
                    }
                } else if a.0 > b.0 {
                    1
                } else {
                    -1
                })
            }
            const HAS_DIST: bool = true;
            fn dist(a: $v, b: $v, _: &mut Ctx) -> PgResult<f64> {
                Ok(((a as f64) - (b as f64)).abs())
            }
        }
    };
}

macro_rules! scalar_penalty {
    () => {
        fn penalty(
            o: (Self::V, Self::V),
            n: (Self::V, Self::V),
            natts: u16,
            _: &mut Ctx,
        ) -> PgResult<f32> {
            Ok(penalty_num(o.0 as f64, o.1 as f64, n.0 as f64, n.1 as f64, natts))
        }
    };
}

macro_rules! lower_ssup {
    () => {
        fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
            let a = Self::read(num_key::<Self>(x));
            let b = Self::read(num_key::<Self>(y));
            Ok(if a < b {
                -1
            } else if a > b {
                1
            } else {
                0
            })
        }
    };
}

scalar_numops!(Int2, i16, 2, 4, rd_i16);
impl NumProc for Int2 {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i16> {
        Ok(d.as_i16())
    }
    fn fetch_datum(l: i16, _: Datum) -> Datum {
        Datum::from_i16(l)
    }
    scalar_penalty!();
    lower_ssup!();
}

scalar_numops!(Int4, i32, 4, 8, rd_i32);
impl NumProc for Int4 {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i32> {
        Ok(d.as_i32())
    }
    fn fetch_datum(l: i32, _: Datum) -> Datum {
        Datum::from_i32(l)
    }
    scalar_penalty!();
    lower_ssup!();
}

scalar_numops!(Int8, i64, 8, 16, rd_i64);
impl NumProc for Int8 {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i64> {
        Ok(d.as_i64())
    }
    fn fetch_datum(l: i64, _: Datum) -> Datum {
        Datum::from_i64(l)
    }
    scalar_penalty!();
    lower_ssup!();
}

scalar_numops!(OidT, u32, 4, 8, rd_u32);
impl NumProc for OidT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<u32> {
        Ok(d.as_oid())
    }
    fn fetch_datum(l: u32, _: Datum) -> Datum {
        Datum::from_oid(l)
    }
    scalar_penalty!();
    lower_ssup!();
}

scalar_numops!(Float4, f32, 4, 8, rd_f32);
impl NumProc for Float4 {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<f32> {
        Ok(d.as_f32())
    }
    fn fetch_datum(l: f32, _: Datum) -> Datum {
        Datum::from_f32(l)
    }
    scalar_penalty!();
    // C's ssup deliberately switches to the total order (float4_cmp_internal:
    // NaN greatest), unlike every other float comparison in this opclass.
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_float::float4_cmp_internal(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
        ))
    }
}

scalar_numops!(Float8, f64, 8, 16, rd_f64);
impl NumProc for Float8 {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<f64> {
        Ok(d.as_f64())
    }
    fn fetch_datum(l: f64, _: Datum) -> Datum {
        Datum::from_f64(l)
    }
    scalar_penalty!();
    // As Float4: C ssup uses the float8_cmp_internal total order.
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_float::float8_cmp_internal(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
        ))
    }
}

scalar_numops!(CashT, i64, 8, 16, rd_i64);
impl NumProc for CashT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i64> {
        Ok(d.as_i64())
    }
    fn fetch_datum(l: i64, _: Datum) -> Datum {
        Datum::from_i64(l)
    }
    scalar_penalty!();
    lower_ssup!();
}

impl Float8 {
    // gbt_float8_dist: error when the subtraction overflows to infinity.
    fn dist_checked(a: f64, b: f64) -> PgResult<f64> {
        let r = a - b;
        if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
            return Err(float_overflow_error());
        }
        Ok(r.abs())
    }
}

// inet keys hold convert_network_to_scalar doubles.
scalar_numops!(InetT, f64, 8, 16, rd_f64);
impl NumProc for InetT {
    fn val_from_datum(d: Datum, mcx: Mcx<'_>) -> PgResult<f64> {
        let img = detoasted_image(mcx, d)?;
        Ok(adt_network::convert_network_to_scalar(adt_network::InetRef::from_payload(
            &img[VARHDRSZ..],
        )))
    }
    fn fetch_datum(_: f64, key: Datum) -> Datum {
        key
    }
    const CONSISTENT_RECHECK: bool = true;
    scalar_penalty!();
    lower_ssup!();
}

// bool: C compares through int promotion of the 1-byte values.
scalar_numops!(BoolT, u8, 1, 2, |b: &[u8]| b[0]);
impl NumProc for BoolT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<u8> {
        Ok(d.as_bool() as u8)
    }
    fn fetch_datum(l: u8, _: Datum) -> Datum {
        Datum::from_bool(l != 0)
    }
    scalar_penalty!();
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        let a = Self::read(num_key::<Self>(x)) as i32;
        let b = Self::read(num_key::<Self>(y)) as i32;
        Ok(a - b)
    }
}

// Timestamp / timestamptz (shared key form; tstz_to_ts_gmt is the identity).
struct Ts;
impl NumOps for Ts {
    const SIZE: usize = 8;
    const INDEXSIZE: usize = 16;
    type V = i64;
    fn read(b: &[u8]) -> i64 {
        rd_i64(b)
    }
    fn write(out: &mut [u8], v: i64) {
        out.copy_from_slice(&v.to_ne_bytes())
    }
    fn gt(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a > b)
    }
    fn ge(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a >= b)
    }
    fn eq(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a == b)
    }
    fn le(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a <= b)
    }
    fn lt(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a < b)
    }
    fn key_cmp(a: (i64, i64), b: (i64, i64), _: &mut Ctx) -> PgResult<i32> {
        let res = adt_timestamp::timestamp_cmp_internal(a.0, b.0);
        Ok(if res == 0 { adt_timestamp::timestamp_cmp_internal(a.1, b.1) } else { res })
    }
    const HAS_DIST: bool = true;
    fn dist(a: i64, b: i64, _: &mut Ctx) -> PgResult<f64> {
        if adt_timestamp::TIMESTAMP_NOT_FINITE(a) || adt_timestamp::TIMESTAMP_NOT_FINITE(b) {
            return Ok(f64::INFINITY);
        }
        let i = adt_timestamp::interval::timestamp_mi(a, b)?;
        Ok(interval_to_sec(&i).abs())
    }
}
impl NumProc for Ts {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i64> {
        Ok(d.as_i64())
    }
    fn fetch_datum(l: i64, _: Datum) -> Datum {
        Datum::from_i64(l)
    }
    fn penalty(o: (i64, i64), n: (i64, i64), natts: u16, _: &mut Ctx) -> PgResult<f32> {
        Ok(penalty_num(
            penalty_check_max_float(o.0 as f64),
            penalty_check_max_float(o.1 as f64),
            penalty_check_max_float(n.0 as f64),
            penalty_check_max_float(n.1 as f64),
            natts,
        ))
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_timestamp::timestamp_cmp_internal(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
        ))
    }
}

// time / timetz (timetz compresses to time + zone).
struct TimeT;
impl NumOps for TimeT {
    const SIZE: usize = 8;
    const INDEXSIZE: usize = 16;
    type V = i64;
    fn read(b: &[u8]) -> i64 {
        rd_i64(b)
    }
    fn write(out: &mut [u8], v: i64) {
        out.copy_from_slice(&v.to_ne_bytes())
    }
    fn gt(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a > b)
    }
    fn ge(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a >= b)
    }
    fn eq(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a == b)
    }
    fn le(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a <= b)
    }
    fn lt(a: i64, b: i64, _: &mut Ctx) -> PgResult<bool> {
        Ok(a < b)
    }
    fn key_cmp(a: (i64, i64), b: (i64, i64), _: &mut Ctx) -> PgResult<i32> {
        let res = adt_date::time_cmp_internal(a.0, b.0);
        Ok(if res == 0 { adt_date::time_cmp_internal(a.1, b.1) } else { res })
    }
    const HAS_DIST: bool = true;
    fn dist(a: i64, b: i64, _: &mut Ctx) -> PgResult<f64> {
        Ok(interval_to_sec(&adt_date::time_mi_time(a, b)).abs())
    }
}
impl NumProc for TimeT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i64> {
        Ok(d.as_i64())
    }
    fn fetch_datum(l: i64, _: Datum) -> Datum {
        Datum::from_i64(l)
    }
    fn penalty(o: (i64, i64), n: (i64, i64), natts: u16, _: &mut Ctx) -> PgResult<f32> {
        let mut res = interval_to_sec(&adt_date::time_mi_time(n.1, o.1)).max(0.0);
        res += interval_to_sec(&adt_date::time_mi_time(o.0, n.0)).max(0.0);
        let mut result = 0.0f32;
        if res > 0.0 {
            let span = interval_to_sec(&adt_date::time_mi_time(o.1, o.0));
            result += f32::MIN_POSITIVE;
            result += (res / (res + span)) as f32;
            result *= f32::MAX / (natts as f32 + 1.0);
        }
        Ok(result)
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_date::time_cmp_internal(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
        ))
    }
}

struct DateT;
impl NumOps for DateT {
    const SIZE: usize = 4;
    const INDEXSIZE: usize = 8;
    type V = i32;
    fn read(b: &[u8]) -> i32 {
        rd_i32(b)
    }
    fn write(out: &mut [u8], v: i32) {
        out.copy_from_slice(&v.to_ne_bytes())
    }
    fn gt(a: i32, b: i32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a > b)
    }
    fn ge(a: i32, b: i32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a >= b)
    }
    fn eq(a: i32, b: i32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a == b)
    }
    fn le(a: i32, b: i32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a <= b)
    }
    fn lt(a: i32, b: i32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a < b)
    }
    fn key_cmp(a: (i32, i32), b: (i32, i32), _: &mut Ctx) -> PgResult<i32> {
        let res = adt_date::date_cmp_internal(a.0, b.0);
        Ok(if res == 0 { adt_date::date_cmp_internal(a.1, b.1) } else { res })
    }
    const HAS_DIST: bool = true;
    fn dist(a: i32, b: i32, _: &mut Ctx) -> PgResult<f64> {
        Ok((adt_date::date_mi(a, b)? as f64).abs())
    }
}
impl NumProc for DateT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<i32> {
        Ok(d.as_i32())
    }
    fn fetch_datum(l: i32, _: Datum) -> Datum {
        Datum::from_i32(l)
    }
    fn penalty(o: (i32, i32), n: (i32, i32), natts: u16, _: &mut Ctx) -> PgResult<f32> {
        let mut res = adt_date::date_mi(n.1, o.1)?.max(0);
        res += adt_date::date_mi(o.0, n.0)?.max(0);
        let mut result = 0.0f32;
        if res > 0 {
            let diff = adt_date::date_mi(o.1, o.0)?;
            result += f32::MIN_POSITIVE;
            result += (res as f64 / (res as f64 + diff as f64)) as f32;
            result *= f32::MAX / (natts as f32 + 1.0);
        }
        Ok(result)
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_date::date_cmp_internal(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
        ))
    }
}

struct IntervalT;
impl NumOps for IntervalT {
    const SIZE: usize = 16;
    const INDEXSIZE: usize = 32;
    type V = Interval;
    fn read(b: &[u8]) -> Interval {
        read_interval(b)
    }
    fn write(out: &mut [u8], v: Interval) {
        out[..8].copy_from_slice(&v.time.to_ne_bytes());
        out[8..12].copy_from_slice(&v.day.to_ne_bytes());
        out[12..16].copy_from_slice(&v.month.to_ne_bytes());
    }
    fn gt(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<bool> {
        Ok(adt_timestamp::interval::interval_cmp_internal(&a, &b) > 0)
    }
    fn ge(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<bool> {
        Ok(adt_timestamp::interval::interval_cmp_internal(&a, &b) >= 0)
    }
    fn eq(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<bool> {
        Ok(adt_timestamp::interval::interval_cmp_internal(&a, &b) == 0)
    }
    fn le(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<bool> {
        Ok(adt_timestamp::interval::interval_cmp_internal(&a, &b) <= 0)
    }
    fn lt(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<bool> {
        Ok(adt_timestamp::interval::interval_cmp_internal(&a, &b) < 0)
    }
    fn key_cmp(a: (Interval, Interval), b: (Interval, Interval), _: &mut Ctx) -> PgResult<i32> {
        let res = adt_timestamp::interval::interval_cmp_internal(&a.0, &b.0);
        Ok(if res == 0 {
            adt_timestamp::interval::interval_cmp_internal(&a.1, &b.1)
        } else {
            res
        })
    }
    const HAS_DIST: bool = true;
    fn dist(a: Interval, b: Interval, _: &mut Ctx) -> PgResult<f64> {
        Ok((interval_to_sec(&a) - interval_to_sec(&b)).abs())
    }
}
impl NumProc for IntervalT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<Interval> {
        Ok(read_interval(deref_bytes(d, 16)))
    }
    fn fetch_datum(_: Interval, key: Datum) -> Datum {
        key
    }
    fn penalty(
        o: (Interval, Interval),
        n: (Interval, Interval),
        natts: u16,
        _: &mut Ctx,
    ) -> PgResult<f32> {
        Ok(penalty_num(
            interval_to_sec(&o.0),
            interval_to_sec(&o.1),
            interval_to_sec(&n.0),
            interval_to_sec(&n.1),
            natts,
        ))
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(adt_timestamp::interval::interval_cmp_internal(
            &Self::read(num_key::<Self>(x)),
            &Self::read(num_key::<Self>(y)),
        ))
    }
}

// uuid / macaddr / macaddr8: memcmp order (C's per-type cmps reduce to it).
macro_rules! bytes_numops {
    ($name:ident, $n:expr, $indexsize:expr) => {
        struct $name;
        impl NumOps for $name {
            const SIZE: usize = $n;
            const INDEXSIZE: usize = $indexsize;
            type V = [u8; $n];
            fn read(b: &[u8]) -> [u8; $n] {
                b[..$n].try_into().unwrap()
            }
            fn write(out: &mut [u8], v: [u8; $n]) {
                out.copy_from_slice(&v)
            }
            fn gt(a: Self::V, b: Self::V, _: &mut Ctx) -> PgResult<bool> {
                Ok(a > b)
            }
            fn ge(a: Self::V, b: Self::V, _: &mut Ctx) -> PgResult<bool> {
                Ok(a >= b)
            }
            fn eq(a: Self::V, b: Self::V, _: &mut Ctx) -> PgResult<bool> {
                Ok(a == b)
            }
            fn le(a: Self::V, b: Self::V, _: &mut Ctx) -> PgResult<bool> {
                Ok(a <= b)
            }
            fn lt(a: Self::V, b: Self::V, _: &mut Ctx) -> PgResult<bool> {
                Ok(a < b)
            }
            fn key_cmp(
                a: (Self::V, Self::V),
                b: (Self::V, Self::V),
                _: &mut Ctx,
            ) -> PgResult<i32> {
                Ok(match a.0.cmp(&b.0).then(a.1.cmp(&b.1)) {
                    core::cmp::Ordering::Less => -1,
                    core::cmp::Ordering::Greater => 1,
                    core::cmp::Ordering::Equal => 0,
                })
            }
        }
        impl $name {
            fn ssup_bytes_cmp(x: Datum, y: Datum) -> i32 {
                let a = <$name as NumOps>::read(num_key::<$name>(x));
                let b = <$name as NumOps>::read(num_key::<$name>(y));
                match a.cmp(&b) {
                    core::cmp::Ordering::Less => -1,
                    core::cmp::Ordering::Greater => 1,
                    core::cmp::Ordering::Equal => 0,
                }
            }
        }
    };
}

bytes_numops!(UuidT, 16, 32);
impl NumProc for UuidT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<[u8; 16]> {
        Ok(deref_bytes(d, 16).try_into().unwrap())
    }
    fn fetch_datum(_: [u8; 16], key: Datum) -> Datum {
        key
    }
    fn penalty(
        o: ([u8; 16], [u8; 16]),
        n: ([u8; 16], [u8; 16]),
        natts: u16,
        _: &mut Ctx,
    ) -> PgResult<f32> {
        Ok(penalty_num(
            uuid_2_double(&o.0),
            uuid_2_double(&o.1),
            uuid_2_double(&n.0),
            uuid_2_double(&n.1),
            natts,
        ))
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(Self::ssup_bytes_cmp(x, y))
    }
}

fn uuid_2_double(u: &[u8; 16]) -> f64 {
    const TWO64: f64 = 18446744073709551616.0;
    let hi = u64::from_be_bytes(u[..8].try_into().unwrap());
    let lo = u64::from_be_bytes(u[8..].try_into().unwrap());
    hi as f64 + lo as f64 / TWO64
}

bytes_numops!(MacT, 6, 16);
impl NumProc for MacT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<[u8; 6]> {
        Ok(deref_bytes(d, 6).try_into().unwrap())
    }
    fn fetch_datum(_: [u8; 6], key: Datum) -> Datum {
        key
    }
    fn penalty(
        o: ([u8; 6], [u8; 6]),
        n: ([u8; 6], [u8; 6]),
        natts: u16,
        _: &mut Ctx,
    ) -> PgResult<f32> {
        let f = |m: &[u8; 6]| -> f64 {
            let mut r = 0u64;
            for (i, &b) in m.iter().enumerate() {
                r += (b as u64) << ((5 - i) * 8);
            }
            r as f64
        };
        Ok(penalty_num(f(&o.0), f(&o.1), f(&n.0), f(&n.1), natts))
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(Self::ssup_bytes_cmp(x, y))
    }
}

bytes_numops!(Mac8T, 8, 16);
impl NumProc for Mac8T {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<[u8; 8]> {
        Ok(deref_bytes(d, 8).try_into().unwrap())
    }
    fn fetch_datum(_: [u8; 8], key: Datum) -> Datum {
        key
    }
    fn penalty(
        o: ([u8; 8], [u8; 8]),
        n: ([u8; 8], [u8; 8]),
        natts: u16,
        _: &mut Ctx,
    ) -> PgResult<f32> {
        let f = |m: &[u8; 8]| u64::from_be_bytes(*m) as f64;
        Ok(penalty_num(f(&o.0), f(&o.1), f(&n.0), f(&n.1), natts))
    }
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        Ok(Self::ssup_bytes_cmp(x, y))
    }
}

struct EnumT;
fn enum_cmp(a: u32, b: u32, ctx: &mut Ctx) -> PgResult<i32> {
    adt_enum::enum_cmp_with_flinfo(a, b, ctx.flinfo.as_deref_mut())
}
impl NumOps for EnumT {
    const SIZE: usize = 4;
    const INDEXSIZE: usize = 8;
    type V = u32;
    fn read(b: &[u8]) -> u32 {
        rd_u32(b)
    }
    fn write(out: &mut [u8], v: u32) {
        out.copy_from_slice(&v.to_ne_bytes())
    }
    fn gt(a: u32, b: u32, ctx: &mut Ctx) -> PgResult<bool> {
        Ok(enum_cmp(a, b, ctx)? > 0)
    }
    fn ge(a: u32, b: u32, ctx: &mut Ctx) -> PgResult<bool> {
        Ok(enum_cmp(a, b, ctx)? >= 0)
    }
    fn eq(a: u32, b: u32, _: &mut Ctx) -> PgResult<bool> {
        Ok(a == b)
    }
    fn le(a: u32, b: u32, ctx: &mut Ctx) -> PgResult<bool> {
        Ok(enum_cmp(a, b, ctx)? <= 0)
    }
    fn lt(a: u32, b: u32, ctx: &mut Ctx) -> PgResult<bool> {
        Ok(enum_cmp(a, b, ctx)? < 0)
    }
    fn key_cmp(a: (u32, u32), b: (u32, u32), ctx: &mut Ctx) -> PgResult<i32> {
        if a.0 == b.0 {
            if a.1 == b.1 {
                return Ok(0);
            }
            return enum_cmp(a.1, b.1, ctx);
        }
        enum_cmp(a.0, b.0, ctx)
    }
}
impl NumProc for EnumT {
    fn val_from_datum(d: Datum, _: Mcx<'_>) -> PgResult<u32> {
        Ok(d.as_oid())
    }
    fn fetch_datum(l: u32, _: Datum) -> Datum {
        Datum::from_oid(l)
    }
    scalar_penalty!();
    // C memoizes an FmgrInfo in ssup_extra; here the odd-OID fallback pays a
    // syscache probe per comparison (cold: sorted enum index builds only).
    fn ssup_cmp(x: Datum, y: Datum, _c: Oid, _m: Mcx<'_>) -> PgResult<i32> {
        adt_enum::enum_cmp_with_flinfo(
            Self::read(num_key::<Self>(x)),
            Self::read(num_key::<Self>(y)),
            None,
        )
    }
}

// ===========================================================================
// Generic num fmgr wrappers.
// ===========================================================================

fn num_compress<T: NumProc>(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let v = T::val_from_datum(entry.key, mcx)?;
    let img = num::make_key::<T>(v, v);
    let key = image_result(fcinfo, &img)?;
    entry_result(fcinfo, &GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf))
}

fn num_fetch<T: NumProc>(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let lower = T::read(num_key::<T>(entry.key));
    let d = T::fetch_datum(lower, entry.key);
    entry_result(fcinfo, &GISTENTRY::init(d, entry.offset, false, entry.page_is_leaf))
}

fn num_consistent<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = strategy_arg(fcinfo);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query = T::val_from_datum(fcinfo.arg(1), mcx)?;
    out_bool(fcinfo, 4, T::CONSISTENT_RECHECK);
    let key = num::read_pair::<T>(num_key::<T>(entry.key));
    let mut c = ctx(f, fcinfo);
    let r = num::consistent::<T>(key, query, strategy, entry.page_is_leaf, &mut c)?;
    Ok(Datum::from_bool(r))
}

fn num_distance<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query = T::val_from_datum(fcinfo.arg(1), mcx)?;
    let key = num::read_pair::<T>(num_key::<T>(entry.key));
    let mut c = ctx(f, fcinfo);
    let r = num::distance::<T>(key, query, entry.page_is_leaf, &mut c)?;
    Ok(Datum::from_f64(r))
}

fn num_union<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let keys: Vec<&[u8]> = entryvec.vector[..entryvec.n as usize]
        .iter()
        .map(|e| num_key::<T>(e.key))
        .collect();
    let mut c = ctx(f, fcinfo);
    let img = num::union::<T>(&keys, &mut c)?;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *(fcinfo.arg(1).as_usize() as *mut i32) = T::INDEXSIZE as i32 };
    image_result(fcinfo, &img)
}

fn num_same<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = num_key::<T>(fcinfo.arg(0));
    let b = num_key::<T>(fcinfo.arg(1));
    let mut c = ctx(f, fcinfo);
    let r = num::same::<T>(a, b, &mut c)?;
    out_bool(fcinfo, 2, r);
    Ok(fcinfo.arg(2))
}

fn num_penalty<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let o = num::read_pair::<T>(num_key::<T>(origentry.key));
    let n = num::read_pair::<T>(num_key::<T>(newentry.key));
    let mut c = ctx(f, fcinfo);
    let p = T::penalty(o, n, origentry.rel_natts, &mut c)?;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *(fcinfo.arg(2).as_usize() as *mut f32) = p };
    Ok(fcinfo.arg(2))
}

fn num_picksplit<T: NumProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let maxoff = (entryvec.n - 1) as usize;
    let mut keys: Vec<&[u8]> = Vec::with_capacity(maxoff + 1);
    keys.push(&[]);
    for e in &entryvec.vector[1..=maxoff] {
        keys.push(num_key::<T>(e.key));
    }
    let mut c = ctx(f, fcinfo);
    let (l, r, ld, rd) = num::picksplit::<T>(&keys, &mut c)?;
    v.spl_left = l;
    v.spl_right = r;
    v.spl_ldatum = image_result(fcinfo, &ld)?;
    v.spl_rdatum = image_result(fcinfo, &rd)?;
    Ok(fcinfo.arg(1))
}

fn num_sortsupport<T: NumProc>(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist sortsupport protocol (GistSortSupportShim).
    let shim = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut GistSortSupportShim) };
    shim.comparator = Some(T::ssup_cmp);
    Ok(Datum::from_usize(0))
}

fn timetz_to_time(d: Datum) -> i64 {
    let b = deref_bytes(d, 12);
    let time = rd_i64(&b[..8]);
    let zone = rd_i32(&b[8..12]);
    time + (zone as i64) * 1_000_000
}

fn fc_gbt_timetz_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    let v = timetz_to_time(entry.key);
    let img = num::make_key::<TimeT>(v, v);
    let key = image_result(fcinfo, &img)?;
    entry_result(fcinfo, &GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf))
}

fn fc_gbt_timetz_consistent(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = strategy_arg(fcinfo);
    let query = timetz_to_time(fcinfo.arg(1));
    // All cases served by this function are inexact.
    out_bool(fcinfo, 4, true);
    let key = num::read_pair::<TimeT>(num_key::<TimeT>(entry.key));
    let mut c = ctx(f, fcinfo);
    let r = num::consistent::<TimeT>(key, query, strategy, entry.page_is_leaf, &mut c)?;
    Ok(Datum::from_bool(r))
}

fn fc_gbt_float8_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // gbt_float8_dist's overflow check replaces GET_FLOAT_DISTANCE.
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let query = fcinfo.arg(1).as_f64();
    let (lower, upper) = num::read_pair::<Float8>(num_key::<Float8>(entry.key));
    let r = if query <= lower {
        Float8::dist_checked(query, lower)?
    } else if query >= upper {
        Float8::dist_checked(query, upper)?
    } else {
        0.0
    };
    Ok(Datum::from_f64(r))
}

// ===========================================================================
// Var types.
// ===========================================================================

trait VarProc: VarOps + Sized {
    fn ssup_cmp(x: Datum, y: Datum, coll: Oid, mcx: Mcx<'_>) -> PgResult<i32>;
    // bit's non-leaf consistent transforms the query with gbt_bit_xfrm.
    fn query_for_node(q: &[u8]) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(q)
    }
}

fn var_ssup_lower<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let img = detoasted_image(mcx, d)?;
    let r = var::key_readable(img);
    let (start, len) = (r.lower.as_ptr(), r.lower.len());
    // SAFETY: the lower slice borrows the detoasted image living in mcx.
    Ok(unsafe { core::slice::from_raw_parts(start, len) })
}

struct TextV;
impl VarOps for TextV {
    const TRNC: bool = false;
    fn eml() -> i32 {
        mbutils::pg_database_encoding_max_length()
    }
    fn cmp(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<i32> {
        varlena::bttextcmp(&a[VARHDRSZ..], &b[VARHDRSZ..], ctx.collation)
    }
}
impl VarProc for TextV {
    fn ssup_cmp(x: Datum, y: Datum, coll: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
        let a = var_ssup_lower(mcx, x)?;
        let b = var_ssup_lower(mcx, y)?;
        varlena::bttextcmp(&a[VARHDRSZ..], &b[VARHDRSZ..], coll)
    }
}

struct BpcharV;
impl VarOps for BpcharV {
    const TRNC: bool = false;
    fn eml() -> i32 {
        mbutils::pg_database_encoding_max_length()
    }
    fn cmp(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<i32> {
        varchar::bpcharcmp(&a[VARHDRSZ..], &b[VARHDRSZ..], ctx.collation)
    }
}
impl VarProc for BpcharV {
    fn ssup_cmp(x: Datum, y: Datum, coll: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
        let a = var_ssup_lower(mcx, x)?;
        let b = var_ssup_lower(mcx, y)?;
        varchar::bpcharcmp(&a[VARHDRSZ..], &b[VARHDRSZ..], coll)
    }
}

struct ByteaV;
impl VarOps for ByteaV {
    const TRNC: bool = true;
    fn cmp(a: &[u8], b: &[u8], _: &mut Ctx) -> PgResult<i32> {
        Ok(varlena::bytea::byteacmp(&a[VARHDRSZ..], &b[VARHDRSZ..]))
    }
}
impl VarProc for ByteaV {
    fn ssup_cmp(x: Datum, y: Datum, _coll: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
        let a = var_ssup_lower(mcx, x)?;
        let b = var_ssup_lower(mcx, y)?;
        Ok(varlena::bytea::byteacmp(&a[VARHDRSZ..], &b[VARHDRSZ..]))
    }
}

struct NumericV;
impl VarOps for NumericV {
    const TRNC: bool = false;
    fn cmp(a: &[u8], b: &[u8], _: &mut Ctx) -> PgResult<i32> {
        Ok(adt_numeric::cmp_numerics(
            adt_numeric::Num::from_payload(&a[VARHDRSZ..]),
            adt_numeric::Num::from_payload(&b[VARHDRSZ..]),
        ))
    }
}
impl VarProc for NumericV {
    fn ssup_cmp(x: Datum, y: Datum, _coll: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
        let a = var_ssup_lower(mcx, x)?;
        let b = var_ssup_lower(mcx, y)?;
        Ok(adt_numeric::cmp_numerics(
            adt_numeric::Num::from_payload(&a[VARHDRSZ..]),
            adt_numeric::Num::from_payload(&b[VARHDRSZ..]),
        ))
    }
}

// gbt_bit_xfrm: bit payload bytes as an INTALIGN-padded bytea image.
fn bit_xfrm(leaf: &[u8]) -> Vec<u8> {
    // leaf = full varbit image: [4B varlena hdr | 4B bit count | bit bytes].
    let bitbytes = var::varsize(leaf) - VARHDRSZ - 4;
    let sz = bitbytes + VARHDRSZ;
    let padded = (sz + 3) & !3;
    let mut out = vec![0u8; padded];
    var::set_varsize(&mut out, padded);
    out[VARHDRSZ..VARHDRSZ + bitbytes]
        .copy_from_slice(&leaf[VARHDRSZ + 4..VARHDRSZ + 4 + bitbytes]);
    out
}

struct BitV;
impl VarOps for BitV {
    const TRNC: bool = true;
    fn cmp(a: &[u8], b: &[u8], _: &mut Ctx) -> PgResult<i32> {
        Ok(varlena::bytea::byteacmp(&a[VARHDRSZ..], &b[VARHDRSZ..]))
    }
    fn leaf_cmp(a: &[u8], b: &[u8], _: &mut Ctx) -> PgResult<i32> {
        Ok(adt_varbit::bit_cmp_payload(&a[VARHDRSZ..], &b[VARHDRSZ..]))
    }
    // C biteq: equal bit counts + memcmp of VARBITBYTES(a). On truncated node
    // keys C's memcmp over-reads (UB); payload equality is the OOB-free
    // equivalent (identical on well-formed values, conservative on nodes).
    fn eq(a: &[u8], b: &[u8], _: &mut Ctx) -> PgResult<bool> {
        Ok(a[VARHDRSZ..] == b[VARHDRSZ..])
    }
    fn l2n(leaf: &[u8]) -> Option<Vec<u8>> {
        Some(bit_xfrm(leaf))
    }
}
impl VarProc for BitV {
    fn ssup_cmp(x: Datum, y: Datum, _coll: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
        let a = var_ssup_lower(mcx, x)?;
        let b = var_ssup_lower(mcx, y)?;
        Ok(varlena::bytea::byteacmp(&a[VARHDRSZ..], &b[VARHDRSZ..]))
    }
    fn query_for_node(q: &[u8]) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Owned(bit_xfrm(q))
    }
}

// ===========================================================================
// Generic var fmgr wrappers.
// ===========================================================================

fn var_compress<T: VarProc>(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let leaf = detoasted_image(mcx, entry.key)?;
    let img = var::key_from_datum(leaf);
    let key = image_result(fcinfo, &img)?;
    entry_result(fcinfo, &GISTENTRY::init(key, entry.offset, true, entry.page_is_leaf))
}

fn var_consistent<T: VarProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = strategy_arg(fcinfo);
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let query = detoasted_image(mcx, fcinfo.arg(1))?;
    out_bool(fcinfo, 4, false);
    let key_img = var_key(entry.key);
    let key = var::key_readable(key_img);
    let mut c = ctx(f, fcinfo);
    let r = if entry.page_is_leaf {
        var::consistent::<T>(&key, query, strategy, true, &mut c)?
    } else {
        let q = T::query_for_node(query);
        var::consistent::<T>(&key, &q, strategy, false, &mut c)?
    };
    Ok(Datum::from_bool(r))
}

fn var_union<T: VarProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let keys: Vec<&[u8]> = entryvec.vector[..entryvec.n as usize]
        .iter()
        .map(|e| var_key(e.key))
        .collect();
    let mut c = ctx(f, fcinfo);
    let img = var::union::<T>(&keys, &mut c)?;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *(fcinfo.arg(1).as_usize() as *mut i32) = img.len() as i32 };
    image_result(fcinfo, &img)
}

fn var_same<T: VarProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = var_key(fcinfo.arg(0));
    let b = var_key(fcinfo.arg(1));
    let mut c = ctx(f, fcinfo);
    let r = var::same::<T>(a, b, &mut c)?;
    out_bool(fcinfo, 2, r);
    Ok(fcinfo.arg(2))
}

fn var_penalty<T: VarProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let mut c = ctx(f, fcinfo);
    let p = var::penalty::<T>(
        var_key(origentry.key),
        var_key(newentry.key),
        origentry.rel_natts,
        &mut c,
    )?;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *(fcinfo.arg(2).as_usize() as *mut f32) = p };
    Ok(fcinfo.arg(2))
}

fn var_picksplit<T: VarProc>(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let maxoff = (entryvec.n - 1) as usize;
    let mut keys: Vec<&[u8]> = Vec::with_capacity(maxoff + 1);
    keys.push(&[]);
    for e in &entryvec.vector[1..=maxoff] {
        keys.push(var_key(e.key));
    }
    let mut c = ctx(f, fcinfo);
    let (l, r, ld, rd) = var::picksplit::<T>(&keys, &mut c)?;
    v.spl_left = l;
    v.spl_right = r;
    v.spl_ldatum = image_result(fcinfo, &ld)?;
    v.spl_rdatum = image_result(fcinfo, &rd)?;
    Ok(fcinfo.arg(1))
}

fn var_sortsupport<T: VarProc>(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist sortsupport protocol (GistSortSupportShim).
    let shim = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut GistSortSupportShim) };
    shim.comparator = Some(T::ssup_cmp);
    Ok(Datum::from_usize(0))
}

// gbt_numeric_penalty (btree_numeric.c) — range-width ratio in numeric space.
fn fc_gbt_numeric_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use adt_numeric as nm;
    // SAFETY: gist fmgr protocol.
    let o = unsafe { entry_arg(fcinfo, 0) };
    let n = unsafe { entry_arg(fcinfo, 1) };
    let org = var_key(o.key);
    let newe = var_key(n.key);
    let mut c = ctx(f, fcinfo);

    let rk = var::key_readable(org);
    let mut uni = Some(var::key_copy(rk.lower, rk.upper));
    var::bin_union::<NumericV>(&mut uni, newe, &mut c)?;
    let uni = uni.expect("union set");
    let ok = var::key_readable(org);
    let uk = var::key_readable(&uni);

    fn num(v: &[u8]) -> adt_numeric::Num<'_> {
        adt_numeric::Num::from_payload(&v[VARHDRSZ..])
    }
    let us = nm::numeric_sub_common(num(uk.upper), num(uk.lower))?;
    let os = nm::numeric_sub_common(num(ok.upper), num(ok.lower))?;
    let ds = nm::numeric_sub_common(us.num(), os.num())?;

    let mut result: f32;
    if nm::numeric_is_nan(us.num()) {
        result = if nm::numeric_is_nan(os.num()) { 0.0 } else { 1.0 };
    } else {
        let nul = nm::int64_to_numeric(0);
        result = 0.0;
        if nm::numeric_gt(ds.num(), nul.num()) {
            result += f32::MIN_POSITIVE;
            let ratio = nm::numeric_div_common(ds.num(), us.num())?;
            result += nm::numeric_float8_no_overflow_any(ratio.payload()) as f32;
        }
    }
    if result > 0.0 {
        result *= f32::MAX / (o.rel_natts as f32 + 1.0);
    }
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *(fcinfo.arg(2).as_usize() as *mut f32) = result };
    Ok(fcinfo.arg(2))
}

// ===========================================================================
// Shared decompress/fetch and module-level functions.
// ===========================================================================

fn fc_gbt_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

fn fc_gbt_var_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let p = entry.key.as_usize() as *const u8;
    // SAFETY: non-null varlena key readable through its header.
    if unsafe { varatt::varatt_is_4b_u(p) } {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let retval = GISTENTRY::init(
        Datum::from_usize(img.as_ptr() as usize),
        entry.offset,
        false,
        entry.page_is_leaf,
    );
    entry_result(fcinfo, &retval)
}

fn fc_gbt_var_fetch(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let r = var::key_readable(img);
    let retval = GISTENTRY::init(
        Datum::from_usize(r.lower.as_ptr() as usize),
        entry.offset,
        true,
        entry.page_is_leaf,
    );
    entry_result(fcinfo, &retval)
}

fn fc_gbtreekey_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typioparam = fcinfo.arg(1).as_oid();
    let name =
        format_type::format_type_extended(typioparam, -1, format_type::FORMAT_TYPE_ALLOW_INVALID)?
            .unwrap_or_else(|| "-".to_string());
    Err(PgError::error(format!("cannot accept a value of type {name}"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

fn fc_gbtreekey_out(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of type gbtreekey?")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

fn fc_gist_translate_cmptype_btree(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // access/cmptype.h: COMPARE_LT..COMPARE_GT (1..5) map 1:1 onto the btree
    // strategies; everything else is InvalidStrategy.
    let cmptype = fcinfo.arg(0).as_i32();
    let strat: u16 = match cmptype {
        1..=5 => cmptype as u16,
        _ => 0,
    };
    Ok(Datum::from_u16(strat))
}

// ===========================================================================
// <-> distance operator functions.
// ===========================================================================

fn float_overflow_error() -> Box<PgError> {
    PgError::error("value out of range: overflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .into()
}

fn out_of_range(what: &str) -> Box<PgError> {
    PgError::error(format!("{what} out of range"))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .into()
}

fn fc_int2_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i16(), fcinfo.arg(1).as_i16());
    match a.checked_sub(b) {
        Some(r) if r != i16::MIN => Ok(Datum::from_i16(r.abs())),
        _ => Err(out_of_range("smallint")),
    }
}

fn fc_int4_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    match a.checked_sub(b) {
        Some(r) if r != i32::MIN => Ok(Datum::from_i32(r.abs())),
        _ => Err(out_of_range("integer")),
    }
}

fn fc_int8_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i64(), fcinfo.arg(1).as_i64());
    match a.checked_sub(b) {
        Some(r) if r != i64::MIN => Ok(Datum::from_i64(r.abs())),
        _ => Err(out_of_range("bigint")),
    }
}

fn fc_cash_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i64(), fcinfo.arg(1).as_i64());
    match a.checked_sub(b) {
        Some(r) if r != i64::MIN => Ok(Datum::from_i64(r.abs())),
        _ => Err(out_of_range("money")),
    }
}

fn fc_oid_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_oid(), fcinfo.arg(1).as_oid());
    Ok(Datum::from_oid(if a < b { b - a } else { a - b }))
}

fn fc_float4_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_f32(), fcinfo.arg(1).as_f32());
    let r = a - b;
    if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(Datum::from_f32(r.abs()))
}

fn fc_float8_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_f64(), fcinfo.arg(1).as_f64());
    let r = a - b;
    if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(Datum::from_f64(r.abs()))
}

fn fc_date_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let diff = adt_date::date_mi(fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32())?;
    Ok(Datum::from_i32(diff.abs()))
}

fn abs_interval(a: Interval) -> PgResult<Interval> {
    let zero = Interval { time: 0, day: 0, month: 0 };
    if adt_timestamp::interval::interval_cmp_internal(&a, &zero) < 0 {
        adt_timestamp::interval::interval_um(&a)
    } else {
        Ok(a)
    }
}

fn fc_time_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let d = adt_date::time_mi_time(fcinfo.arg(0).as_i64(), fcinfo.arg(1).as_i64());
    interval_result(fcinfo, &abs_interval(d)?)
}

fn ts_dist_common(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i64(), fcinfo.arg(1).as_i64());
    if adt_timestamp::TIMESTAMP_NOT_FINITE(a) || adt_timestamp::TIMESTAMP_NOT_FINITE(b) {
        let p = Interval { time: i64::MAX, day: i32::MAX, month: i32::MAX };
        return interval_result(fcinfo, &p);
    }
    let r = adt_timestamp::interval::timestamp_mi(a, b)?;
    interval_result(fcinfo, &abs_interval(r)?)
}

fn fc_ts_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_dist_common(fcinfo)
}

fn fc_tstz_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_dist_common(fcinfo)
}

fn fc_interval_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = read_interval(deref_bytes(fcinfo.arg(0), 16));
    let b = read_interval(deref_bytes(fcinfo.arg(1), 16));
    let diff = adt_timestamp::interval::interval_mi(&a, &b)?;
    interval_result(fcinfo, &abs_interval(diff)?)
}

// ===========================================================================
// Registration.
// ===========================================================================

fn lookup(function: &str) -> Option<PGFunction> {
    macro_rules! num_entries {
        ($prefix:literal, $t:ty) => {
            match function {
                concat!("gbt_", $prefix, "_compress") => return Some(num_compress::<$t>),
                concat!("gbt_", $prefix, "_fetch") => return Some(num_fetch::<$t>),
                concat!("gbt_", $prefix, "_consistent") => return Some(num_consistent::<$t>),
                concat!("gbt_", $prefix, "_union") => return Some(num_union::<$t>),
                concat!("gbt_", $prefix, "_same") => return Some(num_same::<$t>),
                concat!("gbt_", $prefix, "_penalty") => return Some(num_penalty::<$t>),
                concat!("gbt_", $prefix, "_picksplit") => return Some(num_picksplit::<$t>),
                concat!("gbt_", $prefix, "_sortsupport") => return Some(num_sortsupport::<$t>),
                _ => {}
            }
        };
    }
    macro_rules! var_entries {
        ($prefix:literal, $t:ty) => {
            match function {
                concat!("gbt_", $prefix, "_compress") => return Some(var_compress::<$t>),
                concat!("gbt_", $prefix, "_consistent") => return Some(var_consistent::<$t>),
                concat!("gbt_", $prefix, "_union") => return Some(var_union::<$t>),
                concat!("gbt_", $prefix, "_same") => return Some(var_same::<$t>),
                concat!("gbt_", $prefix, "_penalty") => return Some(var_penalty::<$t>),
                concat!("gbt_", $prefix, "_picksplit") => return Some(var_picksplit::<$t>),
                concat!("gbt_", $prefix, "_sortsupport") => return Some(var_sortsupport::<$t>),
                _ => {}
            }
        };
    }

    num_entries!("int2", Int2);
    num_entries!("int4", Int4);
    num_entries!("int8", Int8);
    num_entries!("oid", OidT);
    num_entries!("float4", Float4);
    num_entries!("float8", Float8);
    num_entries!("cash", CashT);
    num_entries!("date", DateT);
    num_entries!("time", TimeT);
    num_entries!("ts", Ts);
    num_entries!("intv", IntervalT);
    num_entries!("uuid", UuidT);
    num_entries!("macad", MacT);
    num_entries!("macad8", Mac8T);
    num_entries!("enum", EnumT);
    num_entries!("bool", BoolT);
    num_entries!("inet", InetT);

    var_entries!("text", TextV);
    var_entries!("bytea", ByteaV);
    var_entries!("numeric", NumericV);
    var_entries!("bit", BitV);

    Some(match function {
        // KNN distance procs.
        "gbt_int2_distance" => num_distance::<Int2>,
        "gbt_int4_distance" => num_distance::<Int4>,
        "gbt_int8_distance" => num_distance::<Int8>,
        "gbt_oid_distance" => num_distance::<OidT>,
        "gbt_float4_distance" => num_distance::<Float4>,
        "gbt_float8_distance" => fc_gbt_float8_distance,
        "gbt_cash_distance" => num_distance::<CashT>,
        "gbt_date_distance" => num_distance::<DateT>,
        "gbt_time_distance" => num_distance::<TimeT>,
        "gbt_ts_distance" => num_distance::<Ts>,
        "gbt_tstz_distance" => num_distance::<Ts>,
        "gbt_intv_distance" => num_distance::<IntervalT>,
        // timestamptz / timetz variants.
        "gbt_tstz_compress" => num_compress::<Ts>,
        "gbt_tstz_consistent" => num_consistent::<Ts>,
        "gbt_timetz_compress" => fc_gbt_timetz_compress,
        "gbt_timetz_consistent" => fc_gbt_timetz_consistent,
        // interval no-op decompress (INTERVALSIZE == sizeof(Interval)).
        "gbt_intv_decompress" => fc_gbt_decompress,
        // bpchar shares text's compress; its own consistent/sortsupport.
        "gbt_bpchar_compress" => var_compress::<TextV>,
        "gbt_bpchar_consistent" => var_consistent::<BpcharV>,
        "gbt_bpchar_sortsupport" => var_sortsupport::<BpcharV>,
        // bit's sortsupport doubles for varbit; macaddr's SQL name differs.
        "gbt_varbit_sortsupport" => var_sortsupport::<BitV>,
        "gbt_macaddr_sortsupport" => num_sortsupport::<MacT>,
        // numeric penalty is special-cased.
        "gbt_numeric_penalty" => fc_gbt_numeric_penalty,
        // Shared module functions.
        "gbt_decompress" => fc_gbt_decompress,
        "gbt_var_decompress" => fc_gbt_var_decompress,
        "gbt_var_fetch" => fc_gbt_var_fetch,
        "gbtreekey_in" => fc_gbtreekey_in,
        "gbtreekey_out" => fc_gbtreekey_out,
        "gist_translate_cmptype_btree" => fc_gist_translate_cmptype_btree,
        // <-> operator functions.
        "int2_dist" => fc_int2_dist,
        "int4_dist" => fc_int4_dist,
        "int8_dist" => fc_int8_dist,
        "oid_dist" => fc_oid_dist,
        "float4_dist" => fc_float4_dist,
        "float8_dist" => fc_float8_dist,
        "cash_dist" => fc_cash_dist,
        "date_dist" => fc_date_dist,
        "time_dist" => fc_time_dist,
        "ts_dist" => fc_ts_dist,
        "tstz_dist" => fc_tstz_dist,
        "interval_dist" => fc_interval_dist,
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
