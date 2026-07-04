//! datum.c copy/serialize kernels. Toast pointers are never detoasted here:
//! datumCopy/datumSerialize move the compressed data or toast reference
//! verbatim (only expanded objects flatten, since another process cannot
//! dereference them).

use core::alloc::Layout;
use core::ptr::NonNull;

use datum::expandeddatum::{datum_get_eohp, eoh_flatten_into, eoh_get_flat_size};
use datum::Datum;
use mcx::{check_alloc_size, vec_append_bytes, Allocator, Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use types_tuple::varatt::{varatt_is_external_expanded, varsize_any};

#[cold]
#[inline(never)]
fn invalid_datum_pointer() -> Box<PgError> {
    Box::new(
        PgError::error("invalid Datum pointer".to_string()).with_sqlstate(ERRCODE_DATA_EXCEPTION),
    )
}

#[cold]
#[inline(never)]
fn invalid_typlen(typlen: i16) -> Box<PgError> {
    Box::new(PgError::error(format!("invalid typLen: {typlen}")))
}

fn alloc_bytes<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<NonNull<u8>> {
    check_alloc_size(n)?;
    // C palloc is maxaligned; varlena/EOH consumers rely on it.
    let layout = Layout::from_size_align(n, 8).map_err(|_| mcx.oom(n))?;
    Ok(Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(n))?.cast())
}

/// `datumGetSize`. The datum must be live for by-ref types.
pub fn datum_get_size(value: Datum, typbyval: bool, typlen: i16) -> PgResult<usize> {
    if typbyval {
        debug_assert!(typlen > 0 && typlen as usize <= core::mem::size_of::<Datum>());
        return Ok(typlen as usize);
    }
    if typlen > 0 {
        return Ok(typlen as usize);
    }
    let p = value.as_usize() as *const u8;
    match typlen {
        -1 => {
            if p.is_null() {
                return Err(invalid_datum_pointer());
            }
            // SAFETY: non-null by-ref datum points at a varlena per caller contract.
            Ok(unsafe { varsize_any(p) })
        }
        -2 => {
            if p.is_null() {
                return Err(invalid_datum_pointer());
            }
            // SAFETY: non-null by-ref datum points at a NUL-terminated cstring.
            Ok(unsafe { core::ffi::CStr::from_ptr(p as *const core::ffi::c_char) }
                .to_bytes_with_nul()
                .len())
        }
        _ => Err(invalid_typlen(typlen)),
    }
}

/// `datumCopy` of a non-NULL datum; by-ref copies land in `mcx`.
pub fn datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    typbyval: bool,
    typlen: i16,
) -> PgResult<Datum> {
    if typbyval {
        return Ok(value);
    }
    let p = value.as_usize() as *const u8;
    // SAFETY: by-ref datum is live per caller contract.
    if typlen == -1 && unsafe { varatt_is_external_expanded(p) } {
        // SAFETY: expanded-datum header checked just above; methods installed
        // by the object's builder; dst holds flat bytes, 8-aligned.
        unsafe {
            let eoh = datum_get_eohp(value);
            let flat = eoh_get_flat_size(eoh);
            let dst = alloc_bytes(mcx, flat)?;
            eoh_flatten_into(eoh, dst.as_ptr(), flat);
            return Ok(Datum::from_usize(dst.as_ptr() as usize));
        }
    }
    let sz = datum_get_size(value, typbyval, typlen)?;
    let dst = alloc_bytes(mcx, sz)?;
    // SAFETY: src live for sz bytes (datum_get_size read it); dst fresh.
    unsafe { core::ptr::copy_nonoverlapping(p, dst.as_ptr(), sz) };
    Ok(Datum::from_usize(dst.as_ptr() as usize))
}

/// `datumEstimateSpace`: bytes `datum_serialize` will append.
pub fn datum_estimate_space(
    value: Datum,
    isnull: bool,
    typbyval: bool,
    typlen: i16,
) -> PgResult<usize> {
    let mut sz = core::mem::size_of::<i32>();
    if !isnull {
        if typbyval {
            sz += core::mem::size_of::<Datum>();
        } else if typlen == -1
            // SAFETY: by-ref datum is live per caller contract.
            && unsafe { varatt_is_external_expanded(value.as_usize() as *const u8) }
        {
            // SAFETY: expanded-datum header checked just above.
            sz += unsafe { eoh_get_flat_size(datum_get_eohp(value)) };
        } else {
            sz += datum_get_size(value, typbyval, typlen)?;
        }
    }
    Ok(sz)
}

/// `datumSerialize`: header word (-2 NULL, -1 by-val, else byte length) then
/// the payload, appended to `out`. Byte layout matches C exactly.
pub fn datum_serialize<'mcx>(
    value: Datum,
    isnull: bool,
    typbyval: bool,
    typlen: i16,
    out: &mut PgVec<'mcx, u8>,
) -> PgResult<()> {
    let mut eoh = core::ptr::null_mut();
    let header: i32 = if isnull {
        -2
    } else if typbyval {
        -1
    } else if typlen == -1
        // SAFETY: by-ref datum is live per caller contract.
        && unsafe { varatt_is_external_expanded(value.as_usize() as *const u8) }
    {
        // SAFETY: expanded-datum header checked just above; methods installed.
        unsafe {
            eoh = datum_get_eohp(value);
            eoh_get_flat_size(eoh) as i32
        }
    } else {
        datum_get_size(value, typbyval, typlen)? as i32
    };
    vec_append_bytes(out, &header.to_ne_bytes())?;
    if isnull {
        return Ok(());
    }
    if typbyval {
        let raw = value.as_usize() as u64;
        return vec_append_bytes(out, &raw.to_ne_bytes());
    }
    if !eoh.is_null() {
        let mcx = *out.allocator();
        let tmp = alloc_bytes(mcx, header as usize)?;
        // SAFETY: tmp holds header bytes, 8-aligned as EOH_flatten_into
        // requires; flattener fills exactly header bytes.
        unsafe {
            eoh_flatten_into(eoh, tmp.as_ptr(), header as usize);
            return vec_append_bytes(
                out,
                core::slice::from_raw_parts(tmp.as_ptr(), header as usize),
            );
        }
    }
    // SAFETY: by-ref datum live for header bytes (datum_get_size read it).
    vec_append_bytes(out, unsafe {
        core::slice::from_raw_parts(value.as_usize() as *const u8, header as usize)
    })
}

/// `datumRestore`; by-ref payloads are copied into `mcx`. Returns (value, isnull).
pub fn datum_restore<'mcx>(mcx: Mcx<'mcx>, cursor: &mut &[u8]) -> PgResult<(Datum, bool)> {
    let header = i32::from_ne_bytes(cursor[..4].try_into().expect("datum_restore: short header"));
    *cursor = &cursor[4..];
    match header {
        -2 => Ok((Datum::null(), true)),
        -1 => {
            let raw = u64::from_ne_bytes(
                cursor[..8].try_into().expect("datum_restore: short by-val payload"),
            );
            *cursor = &cursor[8..];
            Ok((Datum::from_usize(raw as usize), false))
        }
        n => {
            assert!(n > 0, "datum_restore: corrupt header {n}");
            let n = n as usize;
            let dst = alloc_bytes(mcx, n)?;
            assert!(cursor.len() >= n, "datum_restore: short by-ref payload");
            // SAFETY: dst fresh, n bytes; source bounds asserted.
            unsafe { core::ptr::copy_nonoverlapping(cursor.as_ptr(), dst.as_ptr(), n) };
            *cursor = &cursor[n..];
            Ok((Datum::from_usize(dst.as_ptr() as usize), false))
        }
    }
}
