//! heapfuncs.c — heap_page_items, tuple_data_split, heap_tuple_infomask_flags.

use crate::*;
use types_rel::pg_class::RELKIND_SEQUENCE;

// pg_am.dat: the heap table AM's pinned OID.
const HEAP_TABLE_AM_OID: types_core::Oid = 2;
use types_error::{ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_tuple::htup::{
    HEAP_COMBOCID, HEAP_HASEXTERNAL, HEAP_HASNULL, HEAP_HASOID_OLD, HEAP_HASVARWIDTH,
    HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_MOVED_IN, HEAP_MOVED_OFF,
    HEAP_NATTS_MASK, HEAP_ONLY_TUPLE, HEAP_UPDATED, HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK,
    HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
    HEAP_XMAX_SHR_LOCK, HEAP_XMIN_COMMITTED, HEAP_XMIN_FROZEN, HEAP_XMIN_INVALID, BITMAPLEN,
};
use types_tuple::tupmacs::{att_isnull, att_nominal_alignby, att_pointer_alignby};
use types_tuple::varatt::{varatt_is_1b_e, varsize_any, vartag_external, VARTAG_INDIRECT, VARTAG_ONDISK};

const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;
const MIN_HEAP_TUPLE_SIZE: usize = maxalign(SIZEOF_HEAP_TUPLE_HEADER);

fn bits_to_text(bits: &[u8], len: usize) -> String {
    let mut s = String::with_capacity(len);
    for i in 0..len {
        s.push(if bits[i / 8] & (1 << (i % 8)) != 0 { '1' } else { '0' });
    }
    s
}

fn text_to_bits(s: &str) -> PgResult<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut bits = vec![0u8; bytes.len() / 8 + 1];
    for (off, &c) in bytes.iter().enumerate() {
        match c {
            b'0' => {}
            b'1' => bits[off / 8] |= 1 << (off % 8),
            _ => {
                return Err(Box::new(
                    PgError::error(format!(
                        "invalid character \"{}\" in t_bits string",
                        &s[off..off + s[off..].chars().next().map_or(1, char::len_utf8)]
                    ))
                    .with_sqlstate(ERRCODE_DATA_CORRUPTED),
                ))
            }
        }
    }
    Ok(bits)
}

pub(crate) fn fc_heap_page_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("heap_page_items: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        // SAFETY: STRICT bytea arg.
        let v = unsafe { fcinfo.arg_varlena_packed(0)? };
        let b = v.data();
        if b.len() < SizeOfPageHeaderData {
            return Err(param_err(format!("input page too small ({} bytes)", b.len())));
        }
        let tupdesc = composite_tupdesc(mcx, flinfo)?;
        let maxoff = page_max_offset_number(b);
        let mut rows = Vec::with_capacity(maxoff);
        for offnum in 1..=maxoff {
            rows.push(heap_page_item_row(mcx, &tupdesc, b, offnum)?);
        }
        Some(rows)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

fn heap_page_item_row(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    b: &[u8],
    offnum: usize,
) -> PgResult<Vec<u8>> {
    let mut values = [Datum::null(); 14];
    let mut nulls = [false; 14];

    let id = page_item_id(b, offnum);
    values[0] = Datum::from_i16(offnum as i16);
    values[1] = Datum::from_i16(id.off as i16);
    values[2] = Datum::from_i16(id.flags as i16);
    values[3] = Datum::from_i16(id.len as i16);

    let lp_off = id.off as usize;
    let lp_len = id.len as usize;

    // Just enough validity checking to stay inside the page image.
    if id.has_storage()
        && lp_len >= MIN_HEAP_TUPLE_SIZE
        && lp_off == maxalign(lp_off)
        && lp_off + lp_len <= b.len()
    {
        let t = &b[lp_off..lp_off + lp_len];
        let t_infomask2 = r_u16(t, 18);
        let t_infomask = r_u16(t, 20);
        let t_hoff = t[22] as usize;

        values[4] = Datum::from_transaction_id(r_u32(t, 0));
        values[5] = Datum::from_transaction_id(r_u32(t, 4));
        values[6] = Datum::from_i32(r_u32(t, 8) as i32); // shared with xvac
        values[7] = tid_datum(mcx, &t[12..18])?;
        values[8] = Datum::from_i32(t_infomask2 as i32);
        values[9] = Datum::from_i32(t_infomask as i32);
        values[10] = Datum::from_i16(t_hoff as i16);

        if t_hoff >= SIZEOF_HEAP_TUPLE_HEADER && t_hoff <= lp_len && t_hoff == maxalign(t_hoff) {
            if t_infomask & HEAP_HASNULL != 0 {
                let natts = (t_infomask2 & HEAP_NATTS_MASK) as i32;
                let bitmaplen = BITMAPLEN(natts) as usize;
                if bitmaplen <= t_hoff - SIZEOF_HEAP_TUPLE_HEADER {
                    let bits = &t[SIZEOF_HEAP_TUPLE_HEADER..SIZEOF_HEAP_TUPLE_HEADER + bitmaplen];
                    values[11] = text_datum(mcx, bits_to_text(bits, bitmaplen * 8).as_bytes())?;
                } else {
                    nulls[11] = true;
                }
            } else {
                nulls[11] = true;
            }

            if t_infomask & HEAP_HASOID_OLD != 0 {
                values[12] = Datum::from_oid(r_u32(t, t_hoff - 4));
            } else {
                nulls[12] = true;
            }

            values[13] = bytea_datum(mcx, &t[t_hoff..lp_len])?;
        } else {
            nulls[11] = true;
            nulls[12] = true;
            nulls[13] = true;
        }
    } else {
        for n in nulls.iter_mut().skip(4) {
            *n = true;
        }
    }

    tuple_image(mcx, tupdesc, &values, &nulls)
}

pub(crate) fn fc_tuple_data_split(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg(0).as_oid();
    let raw_data_null = fcinfo.argisnull(1);
    let t_infomask = fcinfo.arg(2).as_i32() as u16;
    let t_infomask2 = fcinfo.arg(3).as_i32() as u16;
    let t_bits_str: Option<String> = if fcinfo.argisnull(4) {
        None
    } else {
        // SAFETY: null-checked above.
        let v = unsafe { fcinfo.arg_varlena_packed(4)? };
        Some(String::from_utf8_lossy(v.data()).into_owned())
    };
    let do_detoast = if fcinfo.nargs() >= 6 && !fcinfo.argisnull(5) {
        fcinfo.arg(5).as_bool()
    } else {
        false
    };

    require_superuser("raw page")?;

    if raw_data_null {
        return Ok(fcinfo.return_null());
    }

    let t_bits: Option<Vec<u8>> = if t_infomask & HEAP_HASNULL != 0 {
        let bits_len = (BITMAPLEN((t_infomask2 & HEAP_NATTS_MASK) as i32) * 8) as usize;
        let Some(ref s) = t_bits_str else {
            return Err(Box::new(
                PgError::error("t_bits string must not be NULL")
                    .with_sqlstate(ERRCODE_DATA_CORRUPTED),
            ));
        };
        if s.len() != bits_len {
            return Err(Box::new(
                PgError::error(format!(
                    "unexpected length of t_bits string: {}, expected {}",
                    s.len(),
                    bits_len
                ))
                .with_sqlstate(ERRCODE_DATA_CORRUPTED),
            ));
        }
        Some(text_to_bits(s)?)
    } else {
        if let Some(ref s) = t_bits_str {
            return Err(Box::new(
                PgError::error(format!(
                    "t_bits string is expected to be NULL, but instead it is {} bytes long",
                    s.len()
                ))
                .with_sqlstate(ERRCODE_DATA_CORRUPTED),
            ));
        }
        None
    };

    // SAFETY: null-checked above.
    let raw = unsafe { fcinfo.arg_varlena_packed(1)? };
    tuple_data_split_internal(
        fcinfo,
        relid,
        raw.data(),
        t_infomask,
        t_infomask2,
        t_bits.as_deref(),
        do_detoast,
    )
}

fn tuple_data_split_internal(
    fcinfo: &Fcinfo,
    relid: types_core::Oid,
    tupdata: &[u8],
    t_infomask: u16,
    t_infomask2: u16,
    t_bits: Option<&[u8]>,
    do_detoast: bool,
) -> PgResult<Datum> {
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    let tupdesc = rel.rd_att.clone();
    let nattrs = tupdesc.natts as usize;

    // Sequences always use heap AM without showing it in the catalogs.
    if rel.rd_rel.relkind != RELKIND_SEQUENCE
        && rel.rd_rel.relam != HEAP_TABLE_AM_OID
    {
        return Err(Box::new(
            PgError::error("only heap AM is supported").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if nattrs < (t_infomask2 & HEAP_NATTS_MASK) as usize {
        return Err(Box::new(
            PgError::error(
                "number of attributes in tuple header is greater than number of attributes in tuple descriptor",
            )
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        ));
    }

    let corrupt_end = || {
        Box::new(
            PgError::error("unexpected end of tuple data").with_sqlstate(ERRCODE_DATA_CORRUPTED),
        )
    };

    let mut astate = None;
    let mut off = 0usize;
    let hdr_natts = (t_infomask2 & HEAP_NATTS_MASK) as usize;
    for i in 0..nattrs {
        let attr = tupdesc.compact_attr(i);

        // Attributes above the header count were added without a rewrite and
        // read as NULL.
        let is_null = i >= hdr_natts
            || (t_infomask & HEAP_HASNULL != 0
                // SAFETY: t_bits length covers hdr_natts bits (checked above).
                && unsafe { att_isnull(i, t_bits.expect("HEAP_HASNULL implies bits").as_ptr()) });

        let mut attr_image: Option<Vec<u8>> = None;
        if !is_null {
            let len: usize;
            if attr.attlen == -1 {
                if off >= tupdata.len() {
                    return Err(corrupt_end());
                }
                // SAFETY: byte at `off` is in bounds (checked above).
                off = unsafe {
                    att_pointer_alignby(off, attr.attalignby, -1, tupdata.as_ptr().add(off))
                };
                if off >= tupdata.len() {
                    return Err(corrupt_end());
                }
                let p = &tupdata[off..];
                // SAFETY: first byte in bounds; external tags carry no
                // payload reads here.
                unsafe {
                    if varatt_is_1b_e(p.as_ptr())
                        && vartag_external(p.as_ptr()) != VARTAG_ONDISK
                        && vartag_external(p.as_ptr()) != VARTAG_INDIRECT
                    {
                        return Err(Box::new(
                            PgError::error(format!(
                                "first byte of varlena attribute is incorrect for attribute {i}"
                            ))
                            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
                        ));
                    }
                    if !varatt_is_1b_e(p.as_ptr()) && !crate::heapfuncs::header_fits(p) {
                        return Err(corrupt_end());
                    }
                    len = varsize_any(p.as_ptr());
                }
            } else {
                off = att_nominal_alignby(off, attr.attalignby);
                len = attr.attlen as usize;
            }

            if tupdata.len() < off + len {
                return Err(corrupt_end());
            }

            let image: Vec<u8> = if attr.attlen == -1 && do_detoast {
                detoast_seams::detoast_attr::call(mcx, &tupdata[off..off + len])?.to_vec()
            } else {
                let mut img = Vec::with_capacity(len + 4);
                img.extend_from_slice(&(((len + 4) as u32) << 2).to_ne_bytes());
                img.extend_from_slice(&tupdata[off..off + len]);
                img
            };
            off += len;
            attr_image = Some(image);
        }

        let (dvalue, disnull) = match &attr_image {
            Some(img) => (Datum::from_usize(img.as_ptr() as usize), false),
            None => (Datum::null(), true),
        };
        astate = Some(arrayfuncs::build::accum_array_result(
            mcx,
            astate,
            dvalue,
            disnull,
            types_core::BYTEAOID,
        )?);
    }

    if tupdata.len() != off {
        return Err(Box::new(
            PgError::error("end of tuple reached without looking at all its data")
                .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        ));
    }

    rel.close(types_rel::AccessShareLock)?;

    let astate = astate.expect("natts >= 1 accumulated");
    let image = arrayfuncs::build::make_array_result(mcx, &astate)?;
    byref_result(mcx, &image)
}

/// A 4-byte-header varlena length read needs 4 in-bounds bytes; 1-byte
/// headers need only the first.
pub(crate) fn header_fits(p: &[u8]) -> bool {
    // SAFETY: caller guarantees at least one byte.
    unsafe {
        if types_tuple::varatt::varatt_is_1b(p.as_ptr()) {
            true
        } else {
            p.len() >= 4
        }
    }
}

pub(crate) fn fc_heap_tuple_infomask_flags(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("heap_tuple_infomask_flags: resolved FmgrInfo required");
    let t_infomask = fcinfo.arg(0).as_i32() as u16;
    let t_infomask2 = fcinfo.arg(1).as_i32() as u16;

    require_superuser("raw page")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let (raw_names, combined_names) = infomask_flag_names(t_infomask, t_infomask2);
    let mut raw: Vec<Datum> = Vec::with_capacity(raw_names.len());
    for name in raw_names {
        raw.push(text_datum(mcx, name.as_bytes())?);
    }
    let mut combined: Vec<Datum> = Vec::with_capacity(combined_names.len());
    for name in combined_names {
        combined.push(text_datum(mcx, name.as_bytes())?);
    }

    let values = [text_array_datum(mcx, &raw)?, text_array_datum(mcx, &combined)?];
    composite_result(mcx, &tupdesc, &values, &[false; 2])
}

pub(crate) fn infomask_flag_names(
    t_infomask: u16,
    t_infomask2: u16,
) -> (Vec<&'static str>, Vec<&'static str>) {
    let mut raw = Vec::new();
    for (mask, name) in [
        (HEAP_HASNULL, "HEAP_HASNULL"),
        (HEAP_HASVARWIDTH, "HEAP_HASVARWIDTH"),
        (HEAP_HASEXTERNAL, "HEAP_HASEXTERNAL"),
        (HEAP_HASOID_OLD, "HEAP_HASOID_OLD"),
        (HEAP_XMAX_KEYSHR_LOCK, "HEAP_XMAX_KEYSHR_LOCK"),
        (HEAP_COMBOCID, "HEAP_COMBOCID"),
        (HEAP_XMAX_EXCL_LOCK, "HEAP_XMAX_EXCL_LOCK"),
        (HEAP_XMAX_LOCK_ONLY, "HEAP_XMAX_LOCK_ONLY"),
        (HEAP_XMIN_COMMITTED, "HEAP_XMIN_COMMITTED"),
        (HEAP_XMIN_INVALID, "HEAP_XMIN_INVALID"),
        (HEAP_XMAX_COMMITTED, "HEAP_XMAX_COMMITTED"),
        (HEAP_XMAX_INVALID, "HEAP_XMAX_INVALID"),
        (HEAP_XMAX_IS_MULTI, "HEAP_XMAX_IS_MULTI"),
        (HEAP_UPDATED, "HEAP_UPDATED"),
        (HEAP_MOVED_OFF, "HEAP_MOVED_OFF"),
        (HEAP_MOVED_IN, "HEAP_MOVED_IN"),
    ] {
        if t_infomask & mask != 0 {
            raw.push(name);
        }
    }
    for (mask, name) in [
        (HEAP_KEYS_UPDATED, "HEAP_KEYS_UPDATED"),
        (HEAP_HOT_UPDATED, "HEAP_HOT_UPDATED"),
        (HEAP_ONLY_TUPLE, "HEAP_ONLY_TUPLE"),
    ] {
        if t_infomask2 & mask != 0 {
            raw.push(name);
        }
    }

    let mut combined = Vec::new();
    for (mask, name) in [
        (HEAP_XMAX_SHR_LOCK, "HEAP_XMAX_SHR_LOCK"),
        (HEAP_XMIN_FROZEN, "HEAP_XMIN_FROZEN"),
        (HEAP_MOVED, "HEAP_MOVED"),
    ] {
        if t_infomask & mask == mask {
            combined.push(name);
        }
    }
    (raw, combined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infomask_flag_decode() {
        // HEAP_XMAX_SHR_LOCK == EXCL|KEYSHR: raw lists both, combined lists it.
        let (raw, combined) = infomask_flag_names(0x0050, 0);
        assert_eq!(raw, vec!["HEAP_XMAX_KEYSHR_LOCK", "HEAP_XMAX_EXCL_LOCK"]);
        assert_eq!(combined, vec!["HEAP_XMAX_SHR_LOCK"]);

        // HEAP_XMIN_FROZEN == COMMITTED|INVALID.
        let (raw, combined) = infomask_flag_names(0x0300, 0);
        assert_eq!(raw, vec!["HEAP_XMIN_COMMITTED", "HEAP_XMIN_INVALID"]);
        assert_eq!(combined, vec!["HEAP_XMIN_FROZEN"]);

        // HEAP_MOVED == MOVED_IN|MOVED_OFF; infomask2 keeps only its 3 bits.
        let (raw, combined) = infomask_flag_names(0xC000, 0xC000);
        assert_eq!(
            raw,
            vec!["HEAP_MOVED_OFF", "HEAP_MOVED_IN", "HEAP_HOT_UPDATED", "HEAP_ONLY_TUPLE"]
        );
        assert_eq!(combined, vec!["HEAP_MOVED"]);

        let (raw, combined) = infomask_flag_names(0, 0);
        assert!(raw.is_empty() && combined.is_empty());
    }

    #[test]
    fn t_bits_round_trip() {
        assert_eq!(bits_to_text(&[0b0000_0101], 8), "10100000");
        let bits = text_to_bits("10100000").unwrap();
        assert_eq!(bits[0], 0b0000_0101);
        assert!(text_to_bits("10x").is_err());
    }
}
