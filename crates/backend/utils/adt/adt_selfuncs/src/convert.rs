//! `convert_to_scalar` and its per-type conversion helpers (`selfuncs.c`,
//! PostgreSQL 18.3) — the scalar-scale mapping that `ineq_histogram_selectivity`
//! uses to interpolate within a histogram bin.
//!
//! `convert_to_scalar` maps a value and the two bracketing histogram-bin
//! boundaries onto a common numeric scale (a `double`) so the inequality
//! estimator can linearly interpolate. It dispatches on the operator's declared
//! input type. The arithmetic is ported 1:1 against the C; see the per-leg
//! notes below.
//!
//! ## Datum model boundary (the by-reference legs)
//!
//! In this port a [`Datum`] is the canonical bare machine word
//! (`datum::Datum`). The histogram-bin boundary values arrive as the
//! `Datum`s of an [`crate::ineq`] `AttStatsSlot`, which `get_attstatsslot`
//! deconstructs through `deconstruct_array` — and for a *pass-by-reference*
//! element type (`numeric`, `text`/`varchar`/`bpchar`/`name`, `bytea`,
//! `interval`, `timetz`, the network types) `deconstruct_array` stores only an
//! in-buffer offset in that bare word, NOT a dereferenceable pointer to the
//! element payload (see `backend-utils-adt-arrayfuncs::deconstruct_array`'s own
//! doc and the workspace `Datum`-unification keystone). The element bytes are
//! therefore not reachable from a bare-word `Datum` here.
//!
//! Consequently the by-reference legs (`convert_numeric_to_scalar`'s `numeric`
//! arm, `convert_string_datum`'s varlena/`name` arms, `convert_bytea_to_scalar`,
//! `convert_timevalue_to_scalar`'s `interval`/`timetz` arms, and
//! `convert_network_to_scalar`) cannot read their operand and *panic with a
//! precise blocker* rather than fabricate a scalar (returning `false`/punting to
//! `binfrac = 0.5` for a genuinely-supported type would corrupt the estimate;
//! that is a fake, which the porting discipline forbids). The by-*value* legs
//! (numeric `bool`/`int*`/`float*`/`oid`/`reg*`; timevalue
//! `timestamp`/`timestamptz`/`date`/`time`) carry their value inline in the
//! word and are computed faithfully and in full.
//!
//! The genuinely-unsupported-type fall-through (the bottom of
//! `convert_to_scalar`, and each helper's `*failure = true`) returns `false`
//! exactly as the C does — that is the faithful "don't know how to convert"
//! path, distinct from the by-reference deref blocker above.

use types_core::primitive::Oid;
use datum::datum::Datum;

use adt_datetime::date2timestamp_no_overflow;

/* ---------------------------------------------------------------------------
 * Type OIDs (catalog/pg_type_d.h). Defined locally as several other adt crates
 * do, since types-core carries only a partial set.
 * ------------------------------------------------------------------------- */

const BOOLOID: Oid = 16;
const BYTEAOID: Oid = 17;
const CHAROID: Oid = 18;
const NAMEOID: Oid = 19;
const INT8OID: Oid = 20;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const REGPROCOID: Oid = 24;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const CIDROID: Oid = 650;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
const MACADDR8OID: Oid = 774;
const MACADDROID: Oid = 829;
const INETOID: Oid = 869;
const BPCHAROID: Oid = 1042;
const VARCHAROID: Oid = 1043;
const DATEOID: Oid = 1082;
const TIMEOID: Oid = 1083;
const TIMESTAMPOID: Oid = 1114;
const TIMESTAMPTZOID: Oid = 1184;
const INTERVALOID: Oid = 1186;
const TIMETZOID: Oid = 1266;
const NUMERICOID: Oid = 1700;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGNAMESPACEOID: Oid = 4089;
const REGROLEOID: Oid = 4096;
const REGCOLLATIONOID: Oid = 4191;

/* ---------------------------------------------------------------------------
 * convert_to_scalar (selfuncs.c:4578)
 * ------------------------------------------------------------------------- */

/// `convert_to_scalar(value, valuetypid, collid, &scaledvalue, lobound,
/// hibound, boundstypid, &scaledlobound, &scaledhibound)` (selfuncs.c) — map a
/// value and the two bracketing histogram bounds onto a common numeric scale
/// for linear interpolation. Returns `(ok, scaledvalue, scaledlobound,
/// scaledhibound)`; `ok == false` means "don't know how to convert" (the C
/// returns `false`, leaving the caller to use `binfrac = 0.5`).
///
/// 1:1 with the C `switch (valuetypid)`. `valuetypid` and `boundstypid` should
/// match the operator's declared input type(s), but an extension might use
/// `scalarineqsel` for an operator over types we don't handle, so the
/// fall-through returns `false` rather than failing.
pub(crate) fn convert_to_scalar(
    value: Datum,
    valuetypid: Oid,
    collid: Oid,
    lobound: Datum,
    hibound: Datum,
    boundstypid: Oid,
) -> (bool, f64, f64, f64) {
    match valuetypid {
        // Built-in numeric types.
        BOOLOID | INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | NUMERICOID | OIDOID
        | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID | REGTYPEOID
        | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID | REGNAMESPACEOID => {
            let mut failure = false;
            let scaledvalue = convert_numeric_to_scalar(value, valuetypid, &mut failure);
            let scaledlobound = convert_numeric_to_scalar(lobound, boundstypid, &mut failure);
            let scaledhibound = convert_numeric_to_scalar(hibound, boundstypid, &mut failure);
            (!failure, scaledvalue, scaledlobound, scaledhibound)
        }

        // Built-in string types.
        CHAROID | BPCHAROID | VARCHAROID | TEXTOID | NAMEOID => {
            let mut failure = false;
            let valstr = convert_string_datum(value, valuetypid, collid, &mut failure);
            let lostr = convert_string_datum(lobound, boundstypid, collid, &mut failure);
            let histr = convert_string_datum(hibound, boundstypid, collid, &mut failure);

            // Bail out if any of the values is not of string type.
            if failure {
                return (false, 0.0, 0.0, 0.0);
            }

            let (scaledvalue, scaledlobound, scaledhibound) =
                convert_string_to_scalar(&valstr, &lostr, &histr);
            (true, scaledvalue, scaledlobound, scaledhibound)
        }

        // Built-in bytea type.
        BYTEAOID => {
            // We only support bytea vs bytea comparison.
            if boundstypid != BYTEAOID {
                return (false, 0.0, 0.0, 0.0);
            }
            let (scaledvalue, scaledlobound, scaledhibound) =
                convert_bytea_to_scalar(value, lobound, hibound);
            (true, scaledvalue, scaledlobound, scaledhibound)
        }

        // Built-in time types.
        TIMESTAMPOID | TIMESTAMPTZOID | DATEOID | INTERVALOID | TIMEOID | TIMETZOID => {
            let mut failure = false;
            let scaledvalue = convert_timevalue_to_scalar(value, valuetypid, &mut failure);
            let scaledlobound = convert_timevalue_to_scalar(lobound, boundstypid, &mut failure);
            let scaledhibound = convert_timevalue_to_scalar(hibound, boundstypid, &mut failure);
            (!failure, scaledvalue, scaledlobound, scaledhibound)
        }

        // Built-in network types.
        INETOID | CIDROID | MACADDROID | MACADDR8OID => {
            let mut failure = false;
            let scaledvalue = convert_network_to_scalar(value, valuetypid, &mut failure);
            let scaledlobound = convert_network_to_scalar(lobound, boundstypid, &mut failure);
            let scaledhibound = convert_network_to_scalar(hibound, boundstypid, &mut failure);
            (!failure, scaledvalue, scaledlobound, scaledhibound)
        }

        // Don't know how to convert.
        _ => (false, 0.0, 0.0, 0.0),
    }
}

/* ---------------------------------------------------------------------------
 * convert_numeric_to_scalar (selfuncs.c:4725)
 * ------------------------------------------------------------------------- */

/// Do `convert_to_scalar`'s work for any numeric data type. On failure (an
/// unsupported `typid`) sets `*failure`; otherwise leaves it unchanged.
fn convert_numeric_to_scalar(value: Datum, typid: Oid, failure: &mut bool) -> f64 {
    match typid {
        BOOLOID => value.as_bool() as i32 as f64,
        INT2OID => value.as_i16() as f64,
        INT4OID => value.as_i32() as f64,
        INT8OID => value.as_i64() as f64,
        FLOAT4OID => value.as_f32() as f64,
        FLOAT8OID => value.as_f64(),
        NUMERICOID => {
            // C: DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow,
            //                                        value)).
            //
            // `numeric` is pass-by-reference; the bare-word histogram-bin Datum
            // is an in-buffer offset from `deconstruct_array`, not a
            // dereferenceable pointer to the `numeric` payload, so the operand
            // bytes are unreachable here (workspace Datum-unification keystone).
            // Punting would fabricate a scalar; panic instead.
            panic!(
                "selfuncs: convert_numeric_to_scalar numeric arm is unreachable — `numeric` is \
                 pass-by-reference and the histogram-bin AttStatsSlot Datum carries only a \
                 deconstruct_array in-buffer offset, not a dereferenceable payload pointer \
                 (Datum-unification keystone); numeric_float8_no_overflow cannot read it"
            )
        }
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID
        | REGNAMESPACEOID => {
            // We can treat OIDs as integers.
            value.as_oid() as f64
        }
        _ => {
            *failure = true;
            0.0
        }
    }
}

/* ---------------------------------------------------------------------------
 * convert_string_to_scalar (selfuncs.c:4787) + convert_one_string_to_scalar
 * ------------------------------------------------------------------------- */

/// Do `convert_to_scalar`'s work for any character-string data type.
///
/// Strings are mapped to a scale that ranges from 0 to 1, where the bytes of
/// the string are visualized as fractional digits. The base is not 256 (which
/// inflates estimates); instead the smallest/largest byte values seen in the
/// bounds estimate the per-byte range, after fudging. A common prefix of the
/// three strings is discarded first to "zoom in" on a narrow data range.
///
/// Returns `(scaledvalue, scaledlobound, scaledhibound)`. The inputs are the
/// NUL-terminated images produced by [`convert_string_datum`]; here they are
/// `&[u8]` byte slices (the C operates on `char *` up to the NUL).
fn convert_string_to_scalar(value: &[u8], lobound: &[u8], hibound: &[u8]) -> (f64, f64, f64) {
    // rangelo = rangehi = (unsigned char) hibound[0];
    // (hibound[0] in C reads the first byte; an empty `hibound` would read the
    // NUL terminator, i.e. 0 — mirror that with 0 for an empty slice.)
    let mut rangelo: i32 = *hibound.first().unwrap_or(&0) as i32;
    let mut rangehi: i32 = rangelo;
    for &c in lobound {
        let uc = c as i32;
        if rangelo > uc {
            rangelo = uc;
        }
        if rangehi < uc {
            rangehi = uc;
        }
    }
    for &c in hibound {
        let uc = c as i32;
        if rangelo > uc {
            rangelo = uc;
        }
        if rangehi < uc {
            rangehi = uc;
        }
    }
    // If range includes any upper-case ASCII chars, make it include all.
    if rangelo <= b'Z' as i32 && rangehi >= b'A' as i32 {
        if rangelo > b'A' as i32 {
            rangelo = b'A' as i32;
        }
        if rangehi < b'Z' as i32 {
            rangehi = b'Z' as i32;
        }
    }
    // Ditto lower-case.
    if rangelo <= b'z' as i32 && rangehi >= b'a' as i32 {
        if rangelo > b'a' as i32 {
            rangelo = b'a' as i32;
        }
        if rangehi < b'z' as i32 {
            rangehi = b'z' as i32;
        }
    }
    // Ditto digits.
    if rangelo <= b'9' as i32 && rangehi >= b'0' as i32 {
        if rangelo > b'0' as i32 {
            rangelo = b'0' as i32;
        }
        if rangehi < b'9' as i32 {
            rangehi = b'9' as i32;
        }
    }

    // If range includes less than 10 chars, assume we have not got enough
    // data, and make it include the regular ASCII set.
    if rangehi - rangelo < 9 {
        rangelo = b' ' as i32;
        rangehi = 127;
    }

    // Now strip any common prefix of the three strings.
    //
    // C walks `*lobound` (stops at lobound's NUL), comparing
    // `*lobound != *hibound || *lobound != *value`. With NUL-terminated images
    // reading past the end of `hibound`/`value` would read their NUL; emulate
    // that by treating out-of-range as 0.
    let mut prefix = 0usize;
    loop {
        let lo = lobound.get(prefix).copied();
        match lo {
            None | Some(0) => break, // lobound exhausted (C: `*lobound` is NUL)
            Some(lb) => {
                let hb = *hibound.get(prefix).unwrap_or(&0);
                let vb = *value.get(prefix).unwrap_or(&0);
                if lb != hb || lb != vb {
                    break;
                }
                prefix += 1;
            }
        }
    }
    let value = &value[prefix.min(value.len())..];
    let lobound = &lobound[prefix.min(lobound.len())..];
    let hibound = &hibound[prefix.min(hibound.len())..];

    // Now we can do the conversions.
    let scaledvalue = convert_one_string_to_scalar(value, rangelo, rangehi);
    let scaledlobound = convert_one_string_to_scalar(lobound, rangelo, rangehi);
    let scaledhibound = convert_one_string_to_scalar(hibound, rangelo, rangehi);
    (scaledvalue, scaledlobound, scaledhibound)
}

/// `convert_one_string_to_scalar(value, rangelo, rangehi)` (selfuncs.c). The C
/// reads `value` up to its NUL; here `value` is the (NUL-free) byte slice.
fn convert_one_string_to_scalar(value: &[u8], rangelo: i32, rangehi: i32) -> f64 {
    // slen = strlen(value); (the slice length stops at the implied NUL)
    let mut slen = value.len();

    if slen == 0 {
        return 0.0; // empty string has scalar value 0
    }

    // There seems little point in considering more than a dozen bytes.
    if slen > 12 {
        slen = 12;
    }

    // Convert initial characters to fraction.
    let base = (rangehi - rangelo + 1) as f64;
    let mut num = 0.0f64;
    let mut denom = base;
    for &b in value.iter().take(slen) {
        let mut ch = b as i32;
        if ch < rangelo {
            ch = rangelo - 1;
        } else if ch > rangehi {
            ch = rangehi + 1;
        }
        num += ((ch - rangelo) as f64) / denom;
        denom *= base;
    }

    num
}

/* ---------------------------------------------------------------------------
 * convert_string_datum (selfuncs.c:4918)
 * ------------------------------------------------------------------------- */

/// `convert_string_datum(value, typid, collid, &failure)` (selfuncs.c) — turn a
/// string-type Datum into a NUL-terminated byte image (here a `Vec<u8>`),
/// applying `pg_strxfrm` in a non-C locale. On failure (unsupported `typid`)
/// sets `*failure` and returns an empty image.
fn convert_string_datum(
    value: Datum,
    typid: Oid,
    collid: Oid,
    failure: &mut bool,
) -> alloc::vec::Vec<u8> {
    let _ = collid;
    match typid {
        CHAROID => {
            // C: val = palloc(2); val[0] = DatumGetChar(value); val[1] = '\0';
            // `char` is the only pass-by-value string-leg type, so its single
            // byte is reachable directly from the bare-word Datum.
            alloc::vec![value.as_char() as u8]
        }
        BPCHAROID | VARCHAROID | TEXTOID | NAMEOID => {
            // C: val = TextDatumGetCString(value) / NameStr(*nm), then in a
            // non-C locale val = pg_strxfrm(val, mylocale).
            //
            // All of these are pass-by-reference; the bare-word histogram-bin
            // Datum is an in-buffer offset from `deconstruct_array`, not a
            // dereferenceable pointer to the varlena/`name` payload (workspace
            // Datum-unification keystone), so the operand bytes (and hence the
            // collation-aware pg_strxfrm transform) are unreachable here.
            // Punting would fabricate a scalar; panic instead.
            panic!(
                "selfuncs: convert_string_datum varlena/name arm is unreachable — text/varchar/\
                 bpchar/name are pass-by-reference and the histogram-bin AttStatsSlot Datum \
                 carries only a deconstruct_array in-buffer offset, not a dereferenceable payload \
                 pointer (Datum-unification keystone); TextDatumGetCString / pg_strxfrm cannot \
                 read it"
            )
        }
        _ => {
            *failure = true;
            alloc::vec::Vec::new()
        }
    }
}

/* ---------------------------------------------------------------------------
 * convert_bytea_to_scalar (selfuncs.c:5006) + convert_one_bytea_to_scalar
 * ------------------------------------------------------------------------- */

/// `convert_bytea_to_scalar(value, &scaledvalue, lobound, &scaledlobound,
/// hibound, &scaledhibound)` (selfuncs.c). Very similar to
/// `convert_string_to_scalar`, but bytea cannot assume NUL-termination so
/// lengths are explicit, and a uniform 0..255 byte range is always used.
fn convert_bytea_to_scalar(value: Datum, lobound: Datum, hibound: Datum) -> (f64, f64, f64) {
    let _ = (value, lobound, hibound);
    // C: DatumGetByteaPP(value) then VARDATA_ANY / VARSIZE_ANY_EXHDR.
    //
    // `bytea` is pass-by-reference; the bare-word histogram-bin Datum is an
    // in-buffer offset from `deconstruct_array`, not a dereferenceable pointer
    // to the bytea payload (workspace Datum-unification keystone), so the
    // operand bytes are unreachable here. Punting would fabricate a scalar;
    // panic instead.
    panic!(
        "selfuncs: convert_bytea_to_scalar is unreachable — `bytea` is pass-by-reference and the \
         histogram-bin AttStatsSlot Datum carries only a deconstruct_array in-buffer offset, not \
         a dereferenceable payload pointer (Datum-unification keystone); DatumGetByteaPP cannot \
         read it"
    )
}

/// `convert_one_bytea_to_scalar(value, valuelen, rangelo, rangehi)`
/// (selfuncs.c). Retained as a faithful private helper; reached only once the
/// `bytea` deref blocker above is lifted.
#[allow(dead_code)]
fn convert_one_bytea_to_scalar(
    value: &[u8],
    mut valuelen: usize,
    rangelo: i32,
    rangehi: i32,
) -> f64 {
    if valuelen == 0 {
        return 0.0; // empty string has scalar value 0
    }

    // Since base is 256, need not consider more than about 10 chars.
    if valuelen > 10 {
        valuelen = 10;
    }

    // Convert initial characters to fraction.
    let base = (rangehi - rangelo + 1) as f64;
    let mut num = 0.0f64;
    let mut denom = base;
    for &b in value.iter().take(valuelen) {
        let mut ch = b as i32;
        if ch < rangelo {
            ch = rangelo - 1;
        } else if ch > rangehi {
            ch = rangehi + 1;
        }
        num += ((ch - rangelo) as f64) / denom;
        denom *= base;
    }

    num
}

/* ---------------------------------------------------------------------------
 * convert_timevalue_to_scalar (selfuncs.c:5097)
 * ------------------------------------------------------------------------- */

/// Do `convert_to_scalar`'s work for any timevalue data type. On failure (an
/// unsupported `typid`) sets `*failure`; otherwise leaves it unchanged.
fn convert_timevalue_to_scalar(value: Datum, typid: Oid, failure: &mut bool) -> f64 {
    match typid {
        // TIMESTAMP / TIMESTAMPTZ are int64 microseconds, pass-by-value.
        TIMESTAMPOID => value.as_i64() as f64,
        TIMESTAMPTZOID => value.as_i64() as f64,
        // DATE is int32 days-since-2000, pass-by-value.
        DATEOID => date2timestamp_no_overflow(value.as_i32()),
        INTERVALOID => {
            // C: interval->time + interval->day * USECS_PER_DAY
            //    + interval->month * (DAYS_PER_YEAR/MONTHS_PER_YEAR) * USECS_PER_DAY
            //
            // `interval` is pass-by-reference; the bare-word histogram-bin Datum
            // is an in-buffer offset from `deconstruct_array`, not a
            // dereferenceable pointer to the `Interval` struct (workspace
            // Datum-unification keystone). Punting would fabricate a scalar;
            // panic instead.
            panic!(
                "selfuncs: convert_timevalue_to_scalar interval arm is unreachable — `interval` \
                 is pass-by-reference and the histogram-bin AttStatsSlot Datum carries only a \
                 deconstruct_array in-buffer offset, not a dereferenceable payload pointer \
                 (Datum-unification keystone); DatumGetIntervalP cannot read it"
            )
        }
        // TIME is int64 microseconds since midnight, pass-by-value.
        TIMEOID => value.as_i64() as f64,
        TIMETZOID => {
            // C: timetz->time + timetz->zone * 1000000.0
            //
            // `timetz` is pass-by-reference; same blocker as the interval arm.
            panic!(
                "selfuncs: convert_timevalue_to_scalar timetz arm is unreachable — `timetz` is \
                 pass-by-reference and the histogram-bin AttStatsSlot Datum carries only a \
                 deconstruct_array in-buffer offset, not a dereferenceable payload pointer \
                 (Datum-unification keystone); DatumGetTimeTzADTP cannot read it"
            )
        }
        _ => {
            *failure = true;
            0.0
        }
    }
}

/* ---------------------------------------------------------------------------
 * convert_network_to_scalar (network.c:1467)
 * ------------------------------------------------------------------------- */

/// `convert_network_to_scalar(value, typid, &failure)` (network.c) — does
/// `convert_to_scalar`'s work for the network types. On failure (an
/// unsupported `typid`) sets `*failure`; otherwise leaves it unchanged.
fn convert_network_to_scalar(value: Datum, typid: Oid, failure: &mut bool) -> f64 {
    let _ = value;
    match typid {
        INETOID | CIDROID | MACADDROID | MACADDR8OID => {
            // C reads ip_family/ip_addr(ip) (inet/cidr) or the macaddr struct
            // bytes.
            //
            // All network types are pass-by-reference; the bare-word
            // histogram-bin Datum is an in-buffer offset from
            // `deconstruct_array`, not a dereferenceable pointer to the
            // `inet`/`macaddr` payload (workspace Datum-unification keystone).
            // Punting would fabricate a scalar; panic instead.
            panic!(
                "selfuncs: convert_network_to_scalar is unreachable — inet/cidr/macaddr/macaddr8 \
                 are pass-by-reference and the histogram-bin AttStatsSlot Datum carries only a \
                 deconstruct_array in-buffer offset, not a dereferenceable payload pointer \
                 (Datum-unification keystone); DatumGetInetPP / DatumGetMacaddrP cannot read it"
            )
        }
        _ => {
            *failure = true;
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- convert_numeric_to_scalar (by-value arms) ----

    #[test]
    fn numeric_byvalue_arms_match_c() {
        let mut f = false;
        assert_eq!(convert_numeric_to_scalar(Datum::from_bool(true), BOOLOID, &mut f), 1.0);
        assert_eq!(convert_numeric_to_scalar(Datum::from_bool(false), BOOLOID, &mut f), 0.0);
        assert_eq!(convert_numeric_to_scalar(Datum::from_i16(-5), INT2OID, &mut f), -5.0);
        assert_eq!(
            convert_numeric_to_scalar(Datum::from_i32(1_000_000), INT4OID, &mut f),
            1_000_000.0
        );
        assert_eq!(
            convert_numeric_to_scalar(Datum::from_i64(9_000_000_000), INT8OID, &mut f),
            9_000_000_000.0
        );
        assert_eq!(convert_numeric_to_scalar(Datum::from_f32(2.5), FLOAT4OID, &mut f), 2.5);
        assert_eq!(convert_numeric_to_scalar(Datum::from_f64(-3.25), FLOAT8OID, &mut f), -3.25);
        assert_eq!(convert_numeric_to_scalar(Datum::from_oid(415), OIDOID, &mut f), 415.0);
        assert_eq!(
            convert_numeric_to_scalar(Datum::from_oid(2202), REGPROCEDUREOID, &mut f),
            2202.0
        );
        // None of the supported arms set failure.
        assert!(!f);
    }

    #[test]
    fn numeric_unsupported_type_sets_failure() {
        let mut f = false;
        let r = convert_numeric_to_scalar(Datum::from_i32(1), 9999, &mut f);
        assert!(f);
        assert_eq!(r, 0.0);
    }

    // ---- convert_to_scalar dispatch (by-value numeric leg) ----

    #[test]
    fn convert_to_scalar_int4_leg_interpolates() {
        // value=15 within [10, 20] on INT4 → scaled 15 between 10 and 20.
        let (ok, v, lo, hi) = convert_to_scalar(
            Datum::from_i32(15),
            INT4OID,
            0,
            Datum::from_i32(10),
            Datum::from_i32(20),
            INT4OID,
        );
        assert!(ok);
        assert_eq!((v, lo, hi), (15.0, 10.0, 20.0));
        // binfrac would be (15-10)/(20-10) = 0.5.
        assert!((((v - lo) / (hi - lo)) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn convert_to_scalar_unknown_type_returns_false() {
        let (ok, v, lo, hi) = convert_to_scalar(
            Datum::from_i32(1),
            9999,
            0,
            Datum::from_i32(0),
            Datum::from_i32(2),
            9999,
        );
        assert!(!ok);
        assert_eq!((v, lo, hi), (0.0, 0.0, 0.0));
    }

    // ---- convert_one_string_to_scalar / convert_string_to_scalar ----

    #[test]
    fn one_string_to_scalar_empty_is_zero() {
        assert_eq!(convert_one_string_to_scalar(b"", b' ' as i32, 127), 0.0);
    }

    #[test]
    fn one_string_to_scalar_monotonic_and_bounded() {
        // With the regular ASCII range, "aaa" < "aab" < "abb".
        let lo = b' ' as i32;
        let hi = 127;
        let a = convert_one_string_to_scalar(b"aaa", lo, hi);
        let b = convert_one_string_to_scalar(b"aab", lo, hi);
        let c = convert_one_string_to_scalar(b"abb", lo, hi);
        assert!(a < b, "{a} < {b}");
        assert!(b < c, "{b} < {c}");
        assert!((0.0..=1.0).contains(&a));
        assert!((0.0..=1.0).contains(&c));
    }

    #[test]
    fn string_to_scalar_strips_common_prefix_and_orders() {
        // "abx" within ["aba","abz"]: common prefix "ab" stripped, then a single
        // byte 'x' interpolated between 'a' and 'z'.
        let (v, lo, hi) = convert_string_to_scalar(b"abx", b"aba", b"abz");
        assert!(lo < v && v < hi, "{lo} < {v} < {hi}");
        // The scaled value must reproduce a binfrac in (0,1).
        let binfrac = (v - lo) / (hi - lo);
        assert!((0.0..=1.0).contains(&binfrac), "binfrac={binfrac}");
    }

    #[test]
    fn convert_to_scalar_char_leg_orders() {
        // CHAR is the pass-by-value string-leg type; 'b' within ['a','c'].
        let (ok, v, lo, hi) = convert_to_scalar(
            Datum::from_char(b'b' as i8),
            CHAROID,
            0,
            Datum::from_char(b'a' as i8),
            Datum::from_char(b'c' as i8),
            CHAROID,
        );
        assert!(ok);
        assert!(lo < v && v < hi, "{lo} < {v} < {hi}");
    }

    // ---- convert_one_bytea_to_scalar ----

    #[test]
    fn one_bytea_to_scalar_empty_is_zero() {
        assert_eq!(convert_one_bytea_to_scalar(b"", 0, 0, 255), 0.0);
    }

    #[test]
    fn one_bytea_to_scalar_monotonic() {
        let a = convert_one_bytea_to_scalar(&[0x10, 0x20], 2, 0, 255);
        let b = convert_one_bytea_to_scalar(&[0x10, 0x21], 2, 0, 255);
        let c = convert_one_bytea_to_scalar(&[0x11, 0x00], 2, 0, 255);
        assert!(a < b && b < c, "{a} < {b} < {c}");
        // First byte 0x10 / 256 = 0.0625, second adds 0x20/65536.
        assert!((a - (16.0 / 256.0 + 32.0 / 65536.0)).abs() < 1e-12);
    }

    // ---- convert_timevalue_to_scalar (by-value arms) ----

    #[test]
    fn timevalue_byvalue_arms_match_c() {
        let mut f = false;
        // TIMESTAMP/TIME are int64 microseconds.
        assert_eq!(
            convert_timevalue_to_scalar(Datum::from_i64(123_456_789), TIMESTAMPOID, &mut f),
            123_456_789.0
        );
        assert_eq!(
            convert_timevalue_to_scalar(Datum::from_i64(-42), TIMESTAMPTZOID, &mut f),
            -42.0
        );
        assert_eq!(
            convert_timevalue_to_scalar(Datum::from_i64(86_400_000_000), TIMEOID, &mut f),
            86_400_000_000.0
        );
        // DATE: day 0 (2000-01-01) → 0 microseconds; day 1 → USECS_PER_DAY.
        assert_eq!(convert_timevalue_to_scalar(Datum::from_i32(0), DATEOID, &mut f), 0.0);
        assert_eq!(
            convert_timevalue_to_scalar(Datum::from_i32(1), DATEOID, &mut f),
            86_400_000_000.0
        );
        assert!(!f);
    }

    #[test]
    fn timevalue_unsupported_type_sets_failure() {
        let mut f = false;
        let r = convert_timevalue_to_scalar(Datum::from_i64(1), 9999, &mut f);
        assert!(f);
        assert_eq!(r, 0.0);
    }

    // ---- convert_network_to_scalar (unsupported fall-through) ----

    #[test]
    fn network_unsupported_type_sets_failure() {
        let mut f = false;
        let r = convert_network_to_scalar(Datum::from_usize(0), 9999, &mut f);
        assert!(f);
        assert_eq!(r, 0.0);
    }
}
