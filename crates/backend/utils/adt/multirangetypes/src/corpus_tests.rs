//! Differential corpus vs live PostgreSQL 18.3 (Homebrew): \x01-separated
//! rows; OK rows carry exact output text, ERR rows sqlstate/message/detail.
//! date/timestamp element types need session GUC state and are exercised by
//! the fleet two-binary e2e instead.

use std::sync::Once;

use ::adt_rangetypes::io::{cached_range_io_data, range_parse_flags, RangeIOData};
use ::adt_rangetypes::{ops as rops, range_deserialize, RangeBound, RangeInfo};
use ::datum::Datum;
use ::lsyscache::IOFuncSelector;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::typcache::lookup_type_cache;
use ::types_core::{InvalidOid, Oid, BTREE_AM_OID};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrInfo;
use ::types_tuple::NameData;

use crate::io::{cached_multirange_io_data, MultirangeIOData};

const CORPUS: &str = include_str!("corpus.tsv");
const SEP: char = '\x01';

const INT4: Oid = 23;
const INT8: Oid = 20;
const NUMERIC: Oid = 1700;
const INT4RANGE: Oid = 3904;
const INT8RANGE: Oid = 3926;
const NUMRANGE: Oid = 3906;
const INT4MULTI: Oid = 4451;
const INT8MULTI: Oid = 4536;
const NUMMULTI: Oid = 4532;
const HASH_AM_OID: Oid = 405;

struct T {
    oid: Oid,
    name: &'static str,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    typstorage: u8,
    typtype: u8,
    io: [Oid; 4], // in, out, recv, send
}

// pg_type.dat rows for the unit-testable range family.
const TYPES: &[T] = &[
    T { oid: INT4, name: "int4", typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p', typtype: b'b', io: [42, 43, 2406, 2407] },
    T { oid: INT8, name: "int8", typlen: 8, typbyval: true, typalign: b'd', typstorage: b'p', typtype: b'b', io: [460, 461, 2408, 2409] },
    T { oid: NUMERIC, name: "numeric", typlen: -1, typbyval: false, typalign: b'i', typstorage: b'm', typtype: b'b', io: [1701, 1702, 2460, 2461] },
    T { oid: INT4RANGE, name: "int4range", typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x', typtype: b'r', io: [3834, 3835, 3836, 3837] },
    T { oid: INT8RANGE, name: "int8range", typlen: -1, typbyval: false, typalign: b'd', typstorage: b'x', typtype: b'r', io: [3834, 3835, 3836, 3837] },
    T { oid: NUMRANGE, name: "numrange", typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x', typtype: b'r', io: [3834, 3835, 3836, 3837] },
    T { oid: INT4MULTI, name: "int4multirange", typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x', typtype: b'm', io: [4231, 4232, 4233, 4234] },
    T { oid: INT8MULTI, name: "int8multirange", typlen: -1, typbyval: false, typalign: b'd', typstorage: b'x', typtype: b'm', io: [4231, 4232, 4233, 4234] },
    T { oid: NUMMULTI, name: "nummultirange", typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x', typtype: b'm', io: [4231, 4232, 4233, 4234] },
];

fn typ(oid: Oid) -> &'static T {
    TYPES.iter().find(|t| t.oid == oid).unwrap()
}

fn name(s: &str) -> NameData {
    let mut n = NameData::default();
    n.namestrcpy(s);
    n
}

static SEAMS: Once = Once::new();

fn install() {
    SEAMS.call_once(|| {
        use syscache_seams as s;
        fmgr_core::init_seams();
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        s::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(TYPES.iter().find(|t| t.oid == typid).map(|t| s::PgTypeTypcacheShape {
                typname: name(t.name),
                typlen: t.typlen,
                typbyval: t.typbyval,
                typalign: t.typalign as i8,
                typstorage: t.typstorage as i8,
                typtype: t.typtype as i8,
                typisdefined: true,
                typrelid: InvalidOid,
                typsubscript: InvalidOid,
                typelem: InvalidOid,
                typarray: InvalidOid,
                typcollation: InvalidOid,
            }))
        });
        s::pg_type_io_shape::set(|typid| {
            Ok(TYPES.iter().find(|t| t.oid == typid).map(|t| s::PgTypeIoShape {
                oid: t.oid,
                typinput: t.io[0],
                typoutput: t.io[1],
                typreceive: t.io[2],
                typsend: t.io[3],
                typmodin: InvalidOid,
                typmodout: InvalidOid,
                typelem: InvalidOid,
                typlen: t.typlen,
                typbyval: t.typbyval,
                typalign: t.typalign as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        s::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(0x9e37_79b1)));
        s::lookup_pg_range_shape::set(|range_oid| {
            Ok(match range_oid {
                INT4RANGE => Some((INT4, INT4MULTI, 1978, 3914, 3922)),
                INT8RANGE => Some((INT8, INT8MULTI, 3124, 3928, 3923)),
                NUMRANGE => Some((NUMERIC, NUMMULTI, 3125, InvalidOid, 3924)),
                _ => None,
            }
            .map(|(sub, multi, opc, canon, diff)| s::PgRangeShape {
                rngsubtype: sub,
                rngmultitypid: multi,
                rngcollation: InvalidOid,
                rngsubopc: opc,
                rngcanonical: canon,
                rngsubdiff: diff,
            }))
        });
        s::lookup_pg_range_by_multirange::set(|mr| {
            Ok(match mr {
                INT4MULTI => Some(INT4RANGE),
                INT8MULTI => Some(INT8RANGE),
                NUMMULTI => Some(NUMRANGE),
                _ => None,
            })
        });
        s::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                1978 => Some((BTREE_AM_OID, 1976, INT4)),
                3124 => Some((BTREE_AM_OID, 1976, INT8)),
                3125 => Some((BTREE_AM_OID, 1988, NUMERIC)),
                1979 => Some((HASH_AM_OID, 1977, INT4)),
                _ => None,
            }
            .map(|(m, f, i)| s::PgOpclassShape { opcmethod: m, opcfamily: f, opcintype: i, opckeytype: 0 }))
        });
        s::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            Ok(match (opfamily, lefttype, righttype, procnum) {
                (1976, INT4, INT4, 1) => 351,
                (1976, INT8, INT8, 1) => 842,
                (1988, NUMERIC, NUMERIC, 1) => 1769,
                (1977, INT4, INT4, 1) => 450,
                (1977, INT4, INT4, 2) => 425,
                _ => InvalidOid,
            })
        });
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4, BTREE_AM_OID) => 1978,
                (INT4, HASH_AM_OID) => 1979,
                _ => InvalidOid,
            })
        });
    });
}

fn range_io<'f>(fl: &'f mut FmgrInfo, oid: Oid, f: IOFuncSelector) -> &'f mut RangeIOData {
    cached_range_io_data(fl, oid, f).unwrap()
}

fn mr_io<'f>(fl: &'f mut FmgrInfo, oid: Oid, f: IOFuncSelector) -> &'f mut MultirangeIOData {
    cached_multirange_io_data(fl, oid, f).unwrap()
}

fn out_range(mcx: Mcx<'_>, oid: Oid, img: &[u8]) -> PgResult<String> {
    let mut fl = FmgrInfo::unresolved();
    let cache = cached_range_io_data(&mut fl, oid, IOFuncSelector::IOFunc_output)?;
    let v = ::adt_rangetypes::io::range_out(mcx, cache, img)?;
    Ok(String::from_utf8_lossy(&v[..v.len() - 1]).into_owned())
}

fn parse_mr<'m>(mcx: Mcx<'m>, oid: Oid, s: &str) -> PgResult<PgVec<'m, u8>> {
    let mut fl = FmgrInfo::unresolved();
    let cache = cached_multirange_io_data(&mut fl, oid, IOFuncSelector::IOFunc_input)?;
    Ok(crate::io::multirange_in(mcx, cache, s.as_bytes(), -1, None)?
        .expect("hard error path returns Some"))
}

fn out_mr(mcx: Mcx<'_>, oid: Oid, img: &[u8]) -> PgResult<String> {
    let mut fl = FmgrInfo::unresolved();
    let cache = cached_multirange_io_data(&mut fl, oid, IOFuncSelector::IOFunc_output)?;
    let v = crate::io::multirange_out(mcx, cache, img)?;
    Ok(String::from_utf8_lossy(&v[..v.len() - 1]).into_owned())
}

fn bool_s(b: bool) -> String {
    if b { "true" } else { "false" }.to_string()
}

fn range_oid_of(name: &str) -> Option<Oid> {
    Some(match name {
        "int4range" => INT4RANGE,
        "int8range" => INT8RANGE,
        "numrange" => NUMRANGE,
        "int4multirange" => INT4MULTI,
        "int8multirange" => INT8MULTI,
        "nummultirange" => NUMMULTI,
        _ => return None,
    })
}

fn is_multi(oid: Oid) -> bool {
    matches!(oid, INT4MULTI | INT8MULTI | NUMMULTI)
}

fn eval(mcx: Mcx<'_>, kind: &str, oid: Oid, a1: &str, a2: &str) -> PgResult<Option<String>> {
    if kind == "IN" {
        let mut fl = FmgrInfo::unresolved();
        return Ok(Some(if is_multi(oid) {
            let cache = mr_io(&mut fl, oid, IOFuncSelector::IOFunc_input);
            let img = crate::io::multirange_in(mcx, cache, a1.as_bytes(), -1, None)?
                .expect("hard error path returns Some");
            out_mr(mcx, oid, &img)?
        } else {
            let cache = range_io(&mut fl, oid, IOFuncSelector::IOFunc_input);
            let img = ::adt_rangetypes::io::range_in(mcx, cache, a1.as_bytes(), -1, None)?
                .expect("hard error path returns Some");
            out_range(mcx, oid, &img)?
        }));
    }
    if kind == "SEND" {
        let mut fl = FmgrInfo::unresolved();
        let hex = if is_multi(oid) {
            let img = parse_mr(mcx, oid, a1)?;
            let cache = mr_io(&mut fl, oid, IOFuncSelector::IOFunc_send);
            let b = crate::io::multirange_send(mcx, cache, &img)?;
            hex(b.data())
        } else {
            let img = parse_range_full(mcx, oid, a1)?;
            let cache = range_io(&mut fl, oid, IOFuncSelector::IOFunc_send);
            let b = ::adt_rangetypes::io::range_send(mcx, cache, &img)?;
            hex(b.data())
        };
        return Ok(Some(hex));
    }
    if let Some(fname) = kind.strip_prefix("FN") {
        return eval_fn(mcx, fname, oid, a1, a2);
    }
    if let Some(op) = kind.strip_prefix("OPelem") {
        let img = parse_range_full(mcx, oid, a1)?;
        let mut ri = RangeInfo::lookup(oid)?;
        let v = Datum::from_i32(a2.parse::<i32>().unwrap());
        let r = rops::range_contains_elem_internal(mcx, &mut ri, &img, v)?;
        let _ = op;
        return Ok(Some(bool_s(r)));
    }
    if let Some(op) = kind.strip_prefix("OPr_m") {
        return eval_mixed(mcx, op, oid, a1, a2, false);
    }
    if let Some(op) = kind.strip_prefix("OPm_r") {
        return eval_mixed(mcx, op, oid, a1, a2, true);
    }
    if let Some(op) = kind.strip_prefix("OP") {
        if is_multi(oid) {
            return eval_mr_op(mcx, op, oid, a1, a2);
        }
        return eval_range_op(mcx, op, oid, a1, a2);
    }
    if kind == "CTOR2" || kind == "CTOR3" {
        return eval_ctor(mcx, kind, oid, a1, a2);
    }
    panic!("unknown corpus kind {kind}");
}

fn parse_range_full<'m>(mcx: Mcx<'m>, oid: Oid, s: &str) -> PgResult<PgVec<'m, u8>> {
    let mut fl = FmgrInfo::unresolved();
    let cache = cached_range_io_data(&mut fl, oid, IOFuncSelector::IOFunc_input)?;
    Ok(::adt_rangetypes::io::range_in(mcx, cache, s.as_bytes(), -1, None)?
        .expect("hard error path returns Some"))
}

fn hex(payload: &[u8]) -> String {
    payload.iter().map(|b| format!("{b:02x}")).collect()
}

fn eval_fn(mcx: Mcx<'_>, fname: &str, oid: Oid, a1: &str, a2: &str) -> PgResult<Option<String>> {
    use ::adt_rangetypes::{
        range_get_flags, RANGE_EMPTY, RANGE_LB_INC, RANGE_LB_INF, RANGE_UB_INC, RANGE_UB_INF,
    };
    if is_multi(oid) {
        let img = parse_mr(mcx, oid, a1)?;
        let mut mi = crate::MultirangeInfo::lookup(oid)?;
        let n = crate::multirange_count(&img) as usize;
        let empty = n == 0;
        return Ok(match fname {
            "isempty" => Some(bool_s(empty)),
            "lower" | "upper" | "lower_inc" | "upper_inc" | "lower_inf" | "upper_inf" => {
                if empty {
                    match fname {
                        "lower" | "upper" => None,
                        _ => Some(bool_s(false)),
                    }
                } else {
                    let (lower, _u) = crate::multirange_get_bounds(&mi.rng, &img, 0);
                    let (_l, upper) = crate::multirange_get_bounds(&mi.rng, &img, n - 1);
                    match fname {
                        "lower" => (!lower.infinite).then(|| lower.val.as_i32().to_string()),
                        "upper" => (!upper.infinite).then(|| upper.val.as_i32().to_string()),
                        "lower_inc" => Some(bool_s(lower.inclusive)),
                        "upper_inc" => Some(bool_s(upper.inclusive)),
                        "lower_inf" => Some(bool_s(lower.infinite)),
                        "upper_inf" => Some(bool_s(upper.infinite)),
                        _ => unreachable!(),
                    }
                }
            }
            "range_merge" => {
                let rimg = if empty {
                    ::adt_rangetypes::make_empty_range(mcx, &mut mi.rng)?
                } else if n == 1 {
                    crate::multirange_get_range(mcx, &mi.rng, &img, 0)?
                } else {
                    crate::multirange_get_union_range(mcx, &mut mi.rng, &img)?
                };
                Some(out_range(mcx, mi.rng.rngtypid, &rimg)?)
            }
            "hash" => Some((crate::hash_multirange_internal(mcx, &mut mi, &img)? as i32).to_string()),
            "hash_extended" => Some(
                (crate::hash_multirange_extended_internal(mcx, &mut mi, &img, Datum::from_i64(42))?
                    as i64)
                    .to_string(),
            ),
            other => panic!("unknown FN {other}"),
        });
    }
    let img = parse_range_full(mcx, oid, a1)?;
    let mut ri = RangeInfo::lookup(oid)?;
    let flags = range_get_flags(&img);
    let (lower, upper, empty) = range_deserialize(&ri.elem, &img);
    Ok(match fname {
        "isempty" => Some(bool_s(flags & RANGE_EMPTY != 0)),
        "lower_inc" => Some(bool_s(flags & RANGE_LB_INC != 0)),
        "upper_inc" => Some(bool_s(flags & RANGE_UB_INC != 0)),
        "lower_inf" => Some(bool_s(flags & RANGE_LB_INF != 0)),
        "upper_inf" => Some(bool_s(flags & RANGE_UB_INF != 0)),
        "lower" => (!empty && !lower.infinite).then(|| elem_text(mcx, &ri, lower.val)),
        "upper" => (!empty && !upper.infinite).then(|| elem_text(mcx, &ri, upper.val)),
        "range_merge" => {
            let img2 = parse_range_full(mcx, oid, a2)?;
            let out = match rops::range_union_internal(mcx, &mut ri, &img, &img2, false)? {
                rops::UnionResult::Input1 => out_range(mcx, oid, &img)?,
                rops::UnionResult::Input2 => out_range(mcx, oid, &img2)?,
                rops::UnionResult::New(u) => out_range(mcx, oid, &u)?,
            };
            Some(out)
        }
        "hash" => Some((rops::hash_range_internal(mcx, &mut ri, &img)? as i32).to_string()),
        "hash_extended" => Some(
            (rops::hash_range_extended_internal(mcx, &mut ri, &img, Datum::from_i64(42))? as i64)
                .to_string(),
        ),
        other => panic!("unknown FN {other}"),
    })
}

// int4-only corpora reach lower/upper for byval elems; numrange goes through
// the element out function.
fn elem_text(mcx: Mcx<'_>, ri: &RangeInfo, val: Datum) -> String {
    if ri.elem_typid == INT4 {
        return val.as_i32().to_string();
    }
    let mut fl = FmgrInfo::unresolved();
    let cache = range_io(&mut fl, ri.rngtypid, IOFuncSelector::IOFunc_output);
    let d =
        ::types_fmgr::function_call1_coll_in(&mut cache.typioproc, InvalidOid, mcx, val).unwrap();
    let p = d.as_usize() as *const u8;
    // SAFETY: an out function's result is a live NUL-terminated cstring.
    unsafe {
        let mut n = 0;
        while *p.add(n) != 0 {
            n += 1;
        }
        String::from_utf8_lossy(core::slice::from_raw_parts(p, n)).into_owned()
    }
}

fn eval_range_op(mcx: Mcx<'_>, op: &str, oid: Oid, a1: &str, a2: &str) -> PgResult<Option<String>> {
    let r1 = parse_range_full(mcx, oid, a1)?;
    let r2 = parse_range_full(mcx, oid, a2)?;
    let mut ri = RangeInfo::lookup(oid)?;
    Ok(Some(match op {
        "=" => bool_s(rops::range_eq_internal(mcx, &mut ri, &r1, &r2)?),
        "<>" => bool_s(rops::range_ne_internal(mcx, &mut ri, &r1, &r2)?),
        "<" => bool_s(rops::range_cmp_internal(mcx, &mut ri, &r1, &r2)? < 0),
        "<=" => bool_s(rops::range_cmp_internal(mcx, &mut ri, &r1, &r2)? <= 0),
        ">" => bool_s(rops::range_cmp_internal(mcx, &mut ri, &r1, &r2)? > 0),
        ">=" => bool_s(rops::range_cmp_internal(mcx, &mut ri, &r1, &r2)? >= 0),
        "@>" => bool_s(rops::range_contains_internal(mcx, &mut ri, &r1, &r2)?),
        "<@" => bool_s(rops::range_contained_by_internal(mcx, &mut ri, &r1, &r2)?),
        "&&" => bool_s(rops::range_overlaps_internal(mcx, &mut ri, &r1, &r2)?),
        "<<" => bool_s(rops::range_before_internal(mcx, &mut ri, &r1, &r2)?),
        ">>" => bool_s(rops::range_after_internal(mcx, &mut ri, &r1, &r2)?),
        "-|-" => bool_s(rops::range_adjacent_internal(mcx, &mut ri, &r1, &r2)?),
        "+" => match rops::range_union_internal(mcx, &mut ri, &r1, &r2, true)? {
            rops::UnionResult::Input1 => out_range(mcx, oid, &r1)?,
            rops::UnionResult::Input2 => out_range(mcx, oid, &r2)?,
            rops::UnionResult::New(u) => out_range(mcx, oid, &u)?,
        },
        "*" => out_range(mcx, oid, &rops::range_intersect_internal(mcx, &mut ri, &r1, &r2)?)?,
        "-" => match rops::range_minus_internal(mcx, &mut ri, &r1, &r2)? {
            rops::MinusResult::Input1 => out_range(mcx, oid, &r1)?,
            rops::MinusResult::New(m) => out_range(mcx, oid, &m)?,
        },
        other => panic!("unknown range op {other}"),
    }))
}

fn eval_mr_op(mcx: Mcx<'_>, op: &str, oid: Oid, a1: &str, a2: &str) -> PgResult<Option<String>> {
    let mr1 = parse_mr(mcx, oid, a1)?;
    let mr2 = parse_mr(mcx, oid, a2)?;
    let mut mi = crate::MultirangeInfo::lookup(oid)?;
    let rng = &mut mi.rng;
    Ok(Some(match op {
        "=" => bool_s(crate::multirange_eq_internal(mcx, rng, &mr1, &mr2)?),
        "<>" => bool_s(!crate::multirange_eq_internal(mcx, rng, &mr1, &mr2)?),
        "<" => bool_s(crate::multirange_cmp_internal(mcx, rng, &mr1, &mr2)? < 0),
        "<=" => bool_s(crate::multirange_cmp_internal(mcx, rng, &mr1, &mr2)? <= 0),
        ">" => bool_s(crate::multirange_cmp_internal(mcx, rng, &mr1, &mr2)? > 0),
        ">=" => bool_s(crate::multirange_cmp_internal(mcx, rng, &mr1, &mr2)? >= 0),
        "@>" => bool_s(crate::multirange_contains_multirange_internal(mcx, rng, &mr1, &mr2)?),
        "<@" => bool_s(crate::multirange_contains_multirange_internal(mcx, rng, &mr2, &mr1)?),
        "&&" => bool_s(crate::multirange_overlaps_multirange_internal(mcx, rng, &mr1, &mr2)?),
        "<<" => bool_s(crate::multirange_before_multirange_internal(mcx, rng, &mr1, &mr2)?),
        ">>" => bool_s(crate::multirange_before_multirange_internal(mcx, rng, &mr2, &mr1)?),
        "-|-" => {
            if crate::multirange_is_empty(&mr1) || crate::multirange_is_empty(&mr2) {
                bool_s(false)
            } else {
                let n1 = crate::multirange_count(&mr1) as usize;
                let n2 = crate::multirange_count(&mr2) as usize;
                let (mut lower1, mut upper1) = crate::multirange_get_bounds(rng, &mr1, n1 - 1);
                let (mut lower2, mut upper2) = crate::multirange_get_bounds(rng, &mr2, 0);
                let adj = if rops::bounds_adjacent(mcx, rng, upper1, lower2)? {
                    true
                } else {
                    if n1 > 1 {
                        (lower1, upper1) = crate::multirange_get_bounds(rng, &mr1, 0);
                    }
                    if n2 > 1 {
                        (lower2, upper2) = crate::multirange_get_bounds(rng, &mr2, n2 - 1);
                    }
                    let _ = (lower2, upper1);
                    rops::bounds_adjacent(mcx, rng, upper2, lower1)?
                };
                bool_s(adj)
            }
        }
        "+" => {
            let img = if crate::multirange_is_empty(&mr1) {
                mr2
            } else if crate::multirange_is_empty(&mr2) {
                mr1
            } else {
                let ranges1 = crate::multirange_deserialize(mcx, rng, &mr1)?;
                let ranges2 = crate::multirange_deserialize(mcx, rng, &mr2)?;
                let mut all: PgVec<'_, &[u8]> =
                    ::mcx::vec_with_capacity_in(mcx, ranges1.len() + ranges2.len())?;
                for r in ranges1.iter().chain(ranges2.iter()) {
                    all.push(*r);
                }
                crate::make_multirange(mcx, oid, rng, &mut all)?
            };
            out_mr(mcx, oid, &img)?
        }
        "*" => {
            let img = if crate::multirange_is_empty(&mr1) || crate::multirange_is_empty(&mr2) {
                crate::make_empty_multirange(mcx, oid, rng)?
            } else {
                let ranges1 = crate::multirange_deserialize(mcx, rng, &mr1)?;
                let ranges2 = crate::multirange_deserialize(mcx, rng, &mr2)?;
                crate::multirange_intersect_internal(mcx, oid, rng, &ranges1, &ranges2)?
            };
            out_mr(mcx, oid, &img)?
        }
        "-" => {
            let img = if crate::multirange_is_empty(&mr1) || crate::multirange_is_empty(&mr2) {
                mr1
            } else {
                let ranges1 = crate::multirange_deserialize(mcx, rng, &mr1)?;
                let ranges2 = crate::multirange_deserialize(mcx, rng, &mr2)?;
                crate::multirange_minus_internal(mcx, oid, rng, &ranges1, &ranges2)?
            };
            out_mr(mcx, oid, &img)?
        }
        other => panic!("unknown multirange op {other}"),
    }))
}

fn eval_mixed(
    mcx: Mcx<'_>,
    op: &str,
    oid: Oid,
    a1: &str,
    a2: &str,
    mr_first: bool,
) -> PgResult<Option<String>> {
    let rng_oid = match oid {
        INT4MULTI => INT4RANGE,
        INT8MULTI => INT8RANGE,
        NUMMULTI => NUMRANGE,
        other => other,
    };
    let (rtxt, mtxt) = if mr_first { (a2, a1) } else { (a1, a2) };
    let r = parse_range_full(mcx, rng_oid, rtxt)?;
    let mr = parse_mr(mcx, oid, mtxt)?;
    let mut mi = crate::MultirangeInfo::lookup(oid)?;
    let rng = &mut mi.rng;
    let v = match (op, mr_first) {
        ("@>", false) => crate::range_contains_multirange_internal(mcx, rng, &r, &mr)?,
        ("@>", true) => crate::multirange_contains_range_internal(mcx, rng, &mr, &r)?,
        ("<@", false) => crate::multirange_contains_range_internal(mcx, rng, &mr, &r)?,
        ("<@", true) => crate::range_contains_multirange_internal(mcx, rng, &r, &mr)?,
        ("&&", _) => crate::range_overlaps_multirange_internal(mcx, rng, &r, &mr)?,
        ("<<", false) => crate::range_before_multirange_internal(mcx, rng, &r, &mr)?,
        ("<<", true) => crate::range_after_multirange_internal(mcx, rng, &r, &mr)?,
        (">>", false) => crate::range_after_multirange_internal(mcx, rng, &r, &mr)?,
        (">>", true) => crate::range_before_multirange_internal(mcx, rng, &r, &mr)?,
        ("-|-", _) => crate::range_adjacent_multirange_internal(mcx, rng, &r, &mr)?,
        (other, _) => panic!("unknown mixed op {other}"),
    };
    Ok(Some(bool_s(v)))
}

fn eval_ctor(mcx: Mcx<'_>, kind: &str, oid: Oid, a1: &str, a2: &str) -> PgResult<Option<String>> {
    let mut ri = RangeInfo::lookup(oid)?;
    // CTOR3 rows carry "a,b,flags" in arg2 (generator quirk); CTOR2 carry b.
    let (b_str, flags) = if kind == "CTOR3" {
        let mut it = a2.splitn(3, ',');
        let _a = it.next().unwrap();
        let b = it.next().unwrap();
        let fl = it.next().unwrap();
        (b.to_string(), range_parse_flags(fl.as_bytes())?)
    } else {
        (a2.to_string(), ::adt_rangetypes::RANGE_LB_INC)
    };
    let parse_arg = |s: &str| -> PgResult<Option<Datum>> {
        if s == "NULL" {
            return Ok(None);
        }
        Ok(Some(Datum::from_i32(s.parse::<i32>().unwrap())))
    };
    let a = parse_arg(a1)?;
    let b = parse_arg(&b_str)?;
    let mut lower = RangeBound {
        val: a.unwrap_or(Datum::from_usize(0)),
        infinite: a.is_none(),
        inclusive: flags & ::adt_rangetypes::RANGE_LB_INC != 0,
        lower: true,
    };
    let mut upper = RangeBound {
        val: b.unwrap_or(Datum::from_usize(0)),
        infinite: b.is_none(),
        inclusive: flags & ::adt_rangetypes::RANGE_UB_INC != 0,
        lower: false,
    };
    let img = ::adt_rangetypes::make_range(mcx, &mut ri, &mut lower, &mut upper, false, None)?
        .expect("hard error path returns Some");
    Ok(Some(out_range(mcx, oid, &img)?))
}

fn check_err(e: &PgError, want: &str) -> Option<String> {
    let mut parts = want.splitn(3, SEP);
    let state = parts.next().unwrap_or("");
    let msg = parts.next().unwrap_or("");
    let detail = parts.next().unwrap_or("");
    let want_state = if state.len() == 5 {
        let mut b = [0u8; 5];
        b.copy_from_slice(state.as_bytes());
        Some(::types_error::make_sqlstate(b))
    } else {
        None
    };
    if want_state != Some(e.sqlstate()) || e.message() != msg || e.detail().unwrap_or("") != detail
    {
        return Some(format!(
            "got ({:?}, {:?}, {:?}) want ({state}, {msg:?}, {detail:?})",
            e.sqlstate(),
            e.message(),
            e.detail()
        ));
    }
    None
}

#[test]
fn differential_corpus_vs_live_pg() {
    install();
    let mut n = 0usize;
    let mut skipped = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for line in CORPUS.lines() {
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.splitn(6, SEP).collect();
        let (kind, tname, a1, a2, status, want) = (f[0], f[1], f[2], f[3], f[4], f[5]);
        let Some(oid) = range_oid_of(tname) else {
            skipped += 1;
            continue;
        };
        if kind.starts_with("CTOR") && tname != "int4range" {
            skipped += 1;
            continue;
        }
        // hashint4extended (425) is unported; the extended-hash lane stays
        // blocked on the hashfunc-extended unit.
        if kind == "FNhash_extended" {
            skipped += 1;
            continue;
        }
        n += 1;
        let cx = MemoryContext::new("corpus");
        let mcx = cx.mcx();
        // A panicking row identifies itself instead of aborting the sweep.
        let got = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eval(mcx, kind, oid, a1, a2)
        })) {
            Ok(r) => r,
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("<non-string panic>");
                failures.push(format!("{line}: PANIC {msg}"));
                continue;
            }
        };
        let diverged = match (status, got) {
            ("OK", Ok(Some(g))) => (g != want).then(|| format!("{line}: got {g:?} want {want:?}")),
            ("OK", Ok(None)) => (!want.is_empty()).then(|| format!("{line}: got NULL want {want:?}")),
            ("OK", Err(e)) => Some(format!("{line}: got error {e} want {want:?}")),
            ("ERR", Ok(g)) => Some(format!("{line}: got {g:?} want error")),
            ("ERR", Err(e)) => check_err(&e, want).map(|d| format!("{line}: {d}")),
            other => panic!("bad corpus status {other:?}"),
        };
        if let Some(d) = diverged {
            failures.push(d);
        }
    }
    assert!(n > 3000, "corpus unexpectedly small: {n} rows ({skipped} skipped)");
    // "FAILED-ROW" rides the fleet log's grep filter.
    assert!(
        failures.is_empty(),
        "{} of {n} corpus rows diverged; first 40:\n{}",
        failures.len(),
        failures
            .iter()
            .take(40)
            .map(|f| format!("FAILED-ROW: {f}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
