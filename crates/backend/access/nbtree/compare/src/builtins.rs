use ::array::oidvector;
use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

macro_rules! fc_cmp {
    ($($fname:ident: $core:ident($a:ident, $b:ident);)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_i32(crate::$core(a.value.$a(), b.value.$b())))
        }
    )*};
}

fc_cmp! {
    fc_btboolcmp: btboolcmp(as_bool, as_bool);
    fc_btint2cmp: btint2cmp(as_i16, as_i16);
    fc_btint4cmp: btint4cmp(as_i32, as_i32);
    fc_btint8cmp: btint8cmp(as_i64, as_i64);
    fc_btint48cmp: btint48cmp(as_i32, as_i64);
    fc_btint84cmp: btint84cmp(as_i64, as_i32);
    fc_btint24cmp: btint24cmp(as_i16, as_i32);
    fc_btint42cmp: btint42cmp(as_i32, as_i16);
    fc_btint28cmp: btint28cmp(as_i16, as_i64);
    fc_btint82cmp: btint82cmp(as_i64, as_i16);
    fc_btoidcmp: btoidcmp(as_oid, as_oid);
    fc_btcharcmp: btcharcmp(as_u8, as_u8);
}

unsafe fn arg_oidvector<'a>(fcinfo: &Fcinfo, i: usize) -> PgResult<(&'a oidvector, &'a [Oid])> {
    let p = fcinfo.arg(i).as_usize() as *const oidvector;
    let v = unsafe { &*p };
    crate::check_valid_oidvector(v)?;
    let values =
        unsafe { core::slice::from_raw_parts(p.add(1) as *const Oid, v.dim1.max(0) as usize) };
    Ok((v, values))
}

pub fn fc_btoidvectorcmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict catalog args are non-null detoasted oidvectors; the
    // values tail is dim1 Oids immediately after the 24-byte header
    // (ndim 1, no nulls bitmap once check_valid_oidvector passes).
    let (a, a_values) = unsafe { arg_oidvector(fcinfo, 0) }?;
    let (b, b_values) = unsafe { arg_oidvector(fcinfo, 1) }?;
    Ok(Datum::from_i32(crate::btoidvectorcmp(a, a_values, b, b_values)))
}

// btequalimage (nbtutils.c): unconditionally true for these opclasses.
pub fn fc_btequalimage(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_bool(true))
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs: 2,
        strict: true,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (all proisstrict 2-arg int4-returning), OID-ascending.
pub const NBT_BUILTINS: &[FmgrBuiltin] = &[
    b(350, "btint2cmp", fc_btint2cmp),
    b(351, "btint4cmp", fc_btint4cmp),
    b(356, "btoidcmp", fc_btoidcmp),
    b(358, "btcharcmp", fc_btcharcmp),
    b(404, "btoidvectorcmp", fc_btoidvectorcmp),
    b(842, "btint8cmp", fc_btint8cmp),
    b(1693, "btboolcmp", fc_btboolcmp),
    b(2188, "btint48cmp", fc_btint48cmp),
    b(2189, "btint84cmp", fc_btint84cmp),
    b(2190, "btint24cmp", fc_btint24cmp),
    b(2191, "btint42cmp", fc_btint42cmp),
    b(2192, "btint28cmp", fc_btint28cmp),
    b(2193, "btint82cmp", fc_btint82cmp),
    FmgrBuiltin {
        foid: 5051,
        name: "btequalimage",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_btequalimage,
    },
];
