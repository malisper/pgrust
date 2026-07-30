//! contrib/btree_gin — GIN opclasses emulating btree semantics; the GIN
//! core's BtreeOps arms call the gin_btree_seams installed here. For < / <=
//! extractQuery emits the type's leftmost value (numeric/anyenum: a null
//! pointer / InvalidOid sentinel only their comparators see) and the partial
//! scan compares against the original datum (C's QueryInfo). The gin_* SQL
//! symbols exist for CREATE EXTENSION validation; internal-arg procs error
//! via fmgr.

use datum::Datum;
use gin_vocab::GinBtreeType;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_tuple::{varatt, NameData};

use adt_datetime::consts::Interval;

const LIBRARY: &str = "btree_gin";
const VARHDRSZ: usize = 4;

const BT_LESS: u16 = 1;
const BT_LESS_EQUAL: u16 = 2;
const BT_EQUAL: u16 = 3;
const BT_GREATER_EQUAL: u16 = 4;
const BT_GREATER: u16 = 5;

#[track_caller]
#[cold]
fn unrecognized_strategy(strategy: u16) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized strategy number: {strategy}")))
}

fn var_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null inline varlena image readable through its header.
    unsafe {
        if varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            )
        } else {
            debug_assert!(varatt::varatt_is_4b_u(p));
            core::slice::from_raw_parts(p.add(VARHDRSZ), varatt::varsize_4b(p) - VARHDRSZ)
        }
    }
}

fn fixed_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: by-ref fixed-length datum.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

// PG_DETOAST_DATUM to a flat 4B image in mcx (borrowed when already flat).
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

fn image_in(mcx: Mcx<'_>, bytes: &[u8]) -> PgResult<Datum> {
    let img = mcx::slice_in(mcx, bytes)?.leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

fn cmp_ord<T: Ord>(a: T, b: T) -> i32 {
    match a.cmp(&b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => 0,
    }
}

fn read_timetz(d: Datum) -> adt_date::TimeTzADT {
    let b = fixed_bytes(d, 12);
    adt_date::TimeTzADT {
        time: i64::from_ne_bytes(b[..8].try_into().unwrap()),
        zone: i32::from_ne_bytes(b[8..12].try_into().unwrap()),
    }
}

fn read_interval(d: Datum) -> Interval {
    let b = fixed_bytes(d, 16);
    Interval {
        time: i64::from_ne_bytes(b[..8].try_into().unwrap()),
        day: i32::from_ne_bytes(b[8..12].try_into().unwrap()),
        month: i32::from_ne_bytes(b[12..16].try_into().unwrap()),
    }
}

fn name_ref<'a>(d: Datum) -> &'a NameData {
    // SAFETY: by-ref NAMEDATALEN-byte name datum.
    unsafe { &*(d.as_usize() as *const NameData) }
}

// Short-header keys pack digits at odd addresses (headers are 2/4B, so digit
// parity == payload parity); realign by copy (C DatumGetNumeric; short <= 125B).
fn aligned_num<'a>(payload: &'a [u8], buf: &'a mut [u16; 64]) -> adt_numeric::Num<'a> {
    let num = adt_numeric::Num::from_payload(payload);
    if num.is_special() || payload.as_ptr() as usize % 2 == 0 {
        return num;
    }
    assert!(payload.len() <= 128);
    // SAFETY: u16 buffer reinterpreted as bytes, sized above payload.
    let dst = unsafe {
        core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), payload.len())
    };
    dst.copy_from_slice(payload);
    adt_numeric::Num::from_payload(dst)
}

fn inline_image(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe { (varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p)) || varatt::varatt_is_4b_u(p) }
}

fn payload_cmp(ty: GinBtreeType, pa: &[u8], pb: &[u8], collation: Oid) -> PgResult<i32> {
    use GinBtreeType as T;
    Ok(match ty {
        T::Inet => adt_network::network_cmp_internal(
            adt_network::InetRef::from_payload(pa),
            adt_network::InetRef::from_payload(pb),
        ),
        T::Text => varlena::varstr_cmp(pa, pb, collation)?,
        T::Bpchar => adt_varchar::bpcharcmp(pa, pb, collation)?,
        T::Bytea => varlena::bytea::byteacmp(pa, pb),
        T::Bit => adt_varbit::bit_cmp_payload(pa, pb),
        T::Numeric => {
            let (mut ba, mut bb) = ([0u16; 64], [0u16; 64]);
            adt_numeric::cmp_numerics(aligned_num(pa, &mut ba), aligned_num(pb, &mut bb))
        }
        _ => unreachable!("non-varlena type in payload_cmp"),
    })
}

// index_form_tuple inline-compresses varlena keys > 2040B (TOAST_INDEX_HACK);
// C's typecmp fns detoast per compare. The scratch context is cold.
fn varlena_cmp(ty: GinBtreeType, a: Datum, b: Datum, collation: Oid) -> PgResult<i32> {
    if inline_image(a) && inline_image(b) {
        return payload_cmp(ty, var_payload(a), var_payload(b), collation);
    }
    let cx = mcx::MemoryContext::new("btree_gin key detoast");
    let pa = &detoasted_image(cx.mcx(), a)?[VARHDRSZ..];
    let pb = &detoasted_image(cx.mcx(), b)?[VARHDRSZ..];
    payload_cmp(ty, pa, pb, collation)
}

fn enum_sentinel_cmp(a: Oid, b: Oid) -> PgResult<i32> {
    // gin_enum_cmp: InvalidOid = leftmost sentinel. Flinfo-less: odd-OID
    // pairs re-probe ENUMOID per call (adt_enum's documented divergence).
    if a == InvalidOid {
        Ok(if b == InvalidOid { 0 } else { -1 })
    } else if b == InvalidOid {
        Ok(1)
    } else {
        adt_enum::enum_cmp_with_flinfo(a, b, None)
    }
}

fn compare(ty: GinBtreeType, a: Datum, b: Datum, collation: Oid) -> PgResult<i32> {
    use GinBtreeType as T;
    Ok(match ty {
        T::Int2 => cmp_ord(a.as_i16(), b.as_i16()),
        T::Int4 => cmp_ord(a.as_i32(), b.as_i32()),
        T::Int8 | T::Money => cmp_ord(a.as_i64(), b.as_i64()),
        T::Float4 => adt_float::float4_cmp_internal(a.as_f32(), b.as_f32()),
        T::Float8 => adt_float::float8_cmp_internal(a.as_f64(), b.as_f64()),
        T::Oid => cmp_ord(a.as_oid(), b.as_oid()),
        T::Timestamp => adt_timestamp::timestamp_cmp_internal(a.as_i64(), b.as_i64()),
        T::Time => adt_date::time_cmp_internal(a.as_i64(), b.as_i64()),
        T::Timetz => adt_date::timetz_cmp_internal(&read_timetz(a), &read_timetz(b)),
        T::Date => adt_date::date_cmp_internal(a.as_i32(), b.as_i32()),
        T::Interval => {
            adt_timestamp::interval::interval_cmp_internal(&read_interval(a), &read_interval(b))
        }
        T::Macaddr => cmp_ord(fixed_bytes(a, 6), fixed_bytes(b, 6)),
        T::Macaddr8 => cmp_ord(fixed_bytes(a, 8), fixed_bytes(b, 8)),
        T::Uuid => cmp_ord(fixed_bytes(a, 16), fixed_bytes(b, 16)),
        T::Name => name::btnamecmp(name_ref(a), name_ref(b), collation)?,
        T::Bool | T::Char => a.as_u8() as i32 - b.as_u8() as i32,
        T::Inet | T::Text | T::Bpchar | T::Bytea | T::Bit => varlena_cmp(ty, a, b, collation)?,
        // gin_numeric_cmp: PointerGetDatum(NULL) is the leftmost sentinel.
        T::Numeric => {
            if a.as_usize() == 0 {
                if b.as_usize() == 0 {
                    0
                } else {
                    -1
                }
            } else if b.as_usize() == 0 {
                1
            } else {
                varlena_cmp(ty, a, b, collation)?
            }
        }
        T::Enum => enum_sentinel_cmp(a.as_oid(), b.as_oid())?,
    })
}

fn leftmost(mcx: Mcx<'_>, ty: GinBtreeType) -> PgResult<Datum> {
    use GinBtreeType as T;
    Ok(match ty {
        T::Int2 => Datum::from_i16(i16::MIN),
        T::Int4 => Datum::from_i32(i32::MIN),
        T::Int8 | T::Money => Datum::from_i64(i64::MIN),
        T::Float4 => Datum::from_f32(f32::NEG_INFINITY),
        T::Float8 => Datum::from_f64(f64::NEG_INFINITY),
        T::Oid => Datum::from_oid(0),
        // DT_NOBEGIN / DATEVAL_NOBEGIN below are the i64/i32 minima.
        T::Timestamp => Datum::from_i64(i64::MIN),
        T::Time => Datum::from_i64(0),
        T::Timetz => {
            let mut b = [0u8; 12];
            b[8..].copy_from_slice(&(-24 * 3600i32).to_ne_bytes());
            image_in(mcx, &b)?
        }
        T::Date => Datum::from_i32(i32::MIN),
        T::Interval => {
            // INTERVAL_NOBEGIN
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&i64::MIN.to_ne_bytes());
            b[8..12].copy_from_slice(&i32::MIN.to_ne_bytes());
            b[12..].copy_from_slice(&i32::MIN.to_ne_bytes());
            image_in(mcx, &b)?
        }
        T::Macaddr => image_in(mcx, &[0u8; 6])?,
        T::Macaddr8 => image_in(mcx, &[0u8; 8])?,
        T::Uuid => image_in(mcx, &[0u8; 16])?,
        T::Name => image_in(mcx, &[0u8; 64])?,
        T::Bool => Datum::from_bool(false),
        T::Char => Datum::from_u8(0),
        T::Inet => {
            // inet_in("0.0.0.0/0")
            let mut b = [0u8; 10];
            b[..4].copy_from_slice(&varatt::set_varsize_4b_word(10).to_ne_bytes());
            b[4] = adt_network::PGSQL_AF_INET;
            image_in(mcx, &b)?
        }
        T::Text | T::Bpchar | T::Bytea => {
            image_in(mcx, &varatt::set_varsize_4b_word(4).to_ne_bytes())?
        }
        T::Bit => {
            // bit_in("") / varbit_in(""): zero-length bit string.
            let mut b = [0u8; 8];
            b[..4].copy_from_slice(&varatt::set_varsize_4b_word(8).to_ne_bytes());
            image_in(mcx, &b)?
        }
        // Sentinels (see compare).
        T::Numeric => Datum::from_usize(0),
        T::Enum => Datum::from_oid(InvalidOid),
    })
}

fn extract_value<'m>(mcx: Mcx<'m>, ty: GinBtreeType, value: Datum) -> PgResult<Datum> {
    if ty.is_varlena() {
        let img = detoasted_image(mcx, value)?;
        Ok(Datum::from_usize(img.as_ptr() as usize))
    } else {
        Ok(value)
    }
}

fn extract_query<'m>(
    mcx: Mcx<'m>,
    ty: GinBtreeType,
    query: Datum,
    strategy: u16,
) -> PgResult<(Datum, bool, Datum)> {
    let orig = extract_value(mcx, ty, query)?;
    Ok(match strategy {
        BT_LESS | BT_LESS_EQUAL => (leftmost(mcx, ty)?, true, orig),
        BT_GREATER_EQUAL | BT_GREATER => (orig, true, orig),
        BT_EQUAL => (orig, false, orig),
        other => return Err(unrecognized_strategy(other)),
    })
}

fn compare_prefix(
    ty: GinBtreeType,
    orig: Datum,
    key: Datum,
    strategy: u16,
    collation: Oid,
) -> PgResult<i32> {
    // C compares `a` for the non-less strategies, but there a == orig by
    // construction (extractQuery emitted the query datum itself).
    let cmp = compare(ty, orig, key, collation)?;
    Ok(match strategy {
        BT_LESS => (cmp <= 0) as i32,
        BT_LESS_EQUAL => (cmp < 0) as i32,
        BT_EQUAL => (cmp != 0) as i32,
        BT_GREATER_EQUAL => (cmp > 0) as i32,
        BT_GREATER => {
            if cmp < 0 {
                0
            } else if cmp == 0 {
                -1
            } else {
                1
            }
        }
        other => return Err(unrecognized_strategy(other)),
    })
}

#[track_caller]
#[cold]
fn internal_symbol_via_fmgr() -> Box<PgError> {
    Box::new(PgError::error(
        "btree_gin: GIN opclass support functions dispatch through the GIN core's \
         opclass enum (gin_btree_seams), never via fmgr",
    ))
}

fn fc_internal_stub(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(internal_symbol_via_fmgr())
}

fn fc_gin_numeric_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — non-null numeric varlenas.
    let (a, b) = unsafe { (adt_numeric::builtins::num_arg(fcinfo, 0)?, adt_numeric::builtins::num_arg(fcinfo, 1)?) };
    Ok(Datum::from_i32(adt_numeric::cmp_numerics(a, b)))
}

fn fc_gin_enum_cmp(mut f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_oid(), fcinfo.arg(1).as_oid());
    let res = if a == InvalidOid || b == InvalidOid {
        enum_sentinel_cmp(a, b)?
    } else {
        adt_enum::enum_cmp_with_flinfo(a, b, f.as_deref_mut())?
    };
    Ok(Datum::from_i32(res))
}

fn lookup(function: &str) -> Option<PGFunction> {
    match function {
        "gin_btree_consistent" => return Some(fc_internal_stub),
        "gin_numeric_cmp" => return Some(fc_gin_numeric_cmp),
        "gin_enum_cmp" => return Some(fc_gin_enum_cmp),
        _ => {}
    }
    for prefix in ["gin_extract_value_", "gin_extract_query_", "gin_compare_prefix_"] {
        if let Some(suffix) = function.strip_prefix(prefix) {
            if GinBtreeType::from_type_name(suffix).is_some() {
                return Some(fc_internal_stub);
            }
        }
    }
    None
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    gin_btree_seams::btree_compare::set(compare);
    gin_btree_seams::btree_extract_value::set(extract_value);
    gin_btree_seams::btree_extract_query::set(extract_query);
    gin_btree_seams::btree_compare_prefix::set(compare_prefix);
}

#[cfg(test)]
mod tests;
