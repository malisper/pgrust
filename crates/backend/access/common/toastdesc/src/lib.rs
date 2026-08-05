//! TOAST pointer/descriptor vocabulary (varatt.h external-pointer shapes,
//! toast_internals.h compressed-header shapes) plus get_toast_snapshot from
//! toast_internals.c. The chunked save/delete/fetch bodies stay behind
//! toast_internals_seams' loud panics until their keystones land
//! (heaptoast fetch, toast_compression, GetNewOidWithIndex).

use ::mcx::Mcx;
use ::snapshot::{SnapshotData, SnapshotType};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_tuple::varatt::{VARHDRSZ, VARHDRSZ_EXTERNAL, VARTAG_ONDISK};

#[cfg(test)]
mod tests;

pub const VARLENA_EXTSIZE_BITS: u32 = 30;
pub const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;
pub const TOAST_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + 16;

pub const TOAST_PGLZ_COMPRESSION_ID: u32 = 0;
pub const TOAST_LZ4_COMPRESSION_ID: u32 = 1;
pub const TOAST_INVALID_COMPRESSION_ID: u32 = 2;
pub const TOAST_PGLZ_COMPRESSION: u8 = b'p';
pub const TOAST_LZ4_COMPRESSION: u8 = b'l';
pub const INVALID_COMPRESSION_METHOD: u8 = 0;

#[inline]
pub const fn compression_method_is_valid(cmethod: u8) -> bool {
    cmethod == TOAST_PGLZ_COMPRESSION || cmethod == TOAST_LZ4_COMPRESSION
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VarattExternal {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: Oid,
    pub va_toastrelid: Oid,
}

impl VarattExternal {
    pub fn from_image(attr: &[u8]) -> PgResult<Self> {
        let p = attr
            .get(VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 16)
            .ok_or_else(truncated_pointer)?;
        Ok(Self::from_payload(p.try_into().expect("16-byte slice")))
    }

    pub fn from_payload(p: [u8; 16]) -> Self {
        VarattExternal {
            va_rawsize: i32::from_ne_bytes([p[0], p[1], p[2], p[3]]),
            va_extinfo: u32::from_ne_bytes([p[4], p[5], p[6], p[7]]),
            va_valueid: u32::from_ne_bytes([p[8], p[9], p[10], p[11]]),
            va_toastrelid: u32::from_ne_bytes([p[12], p[13], p[14], p[15]]),
        }
    }

    pub fn to_payload(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..4].copy_from_slice(&self.va_rawsize.to_ne_bytes());
        out[4..8].copy_from_slice(&self.va_extinfo.to_ne_bytes());
        out[8..12].copy_from_slice(&self.va_valueid.to_ne_bytes());
        out[12..16].copy_from_slice(&self.va_toastrelid.to_ne_bytes());
        out
    }

    /// The full on-disk pointer image toast_save_datum returns.
    pub fn to_ondisk_image(self) -> [u8; TOAST_POINTER_SIZE] {
        let mut out = [0u8; TOAST_POINTER_SIZE];
        out[0] = ondisk_marker_byte();
        out[1] = VARTAG_ONDISK;
        out[2..].copy_from_slice(&self.to_payload());
        out
    }

    #[inline]
    pub fn extsize(self) -> u32 {
        self.va_extinfo & VARLENA_EXTSIZE_MASK
    }

    #[inline]
    pub fn compress_method(self) -> u32 {
        self.va_extinfo >> VARLENA_EXTSIZE_BITS
    }

    #[inline]
    pub fn set_size_and_compress_method(&mut self, len: u32, cm: u32) {
        debug_assert!(cm == TOAST_PGLZ_COMPRESSION_ID || cm == TOAST_LZ4_COMPRESSION_ID);
        self.va_extinfo = len | (cm << VARLENA_EXTSIZE_BITS);
    }

    /// extsize (headerless) < rawsize - VARHDRSZ; uint32 < int in C wraps, as here.
    #[inline]
    pub fn is_compressed(self) -> bool {
        self.extsize() < (self.va_rawsize as u32).wrapping_sub(VARHDRSZ as u32)
    }
}

#[inline]
const fn ondisk_marker_byte() -> u8 {
    // VARATT_IS_1B_E marker.
    #[cfg(target_endian = "little")]
    {
        0x01
    }
    #[cfg(target_endian = "big")]
    {
        0x80
    }
}

#[inline]
pub fn varatt_is_external_ondisk(attr: &[u8]) -> bool {
    attr.len() >= 2 && attr[0] == ondisk_marker_byte() && attr[1] == VARTAG_ONDISK
}

#[track_caller]
#[cold]
#[inline(never)]
fn truncated_pointer() -> Box<PgError> {
    Box::new(PgError::error("truncated external TOAST pointer"))
}

pub fn toast_compress_extsize(datum: &[u8]) -> PgResult<u32> {
    Ok(tcinfo(datum)? & VARLENA_EXTSIZE_MASK)
}

pub fn toast_compress_method(datum: &[u8]) -> PgResult<u32> {
    Ok(tcinfo(datum)? >> VARLENA_EXTSIZE_BITS)
}

pub fn toast_compress_set_size_and_compress_method(
    datum: &mut [u8],
    len: i32,
    cm_method: u32,
) -> PgResult<()> {
    debug_assert!(len > 0 && (len as u32) <= VARLENA_EXTSIZE_MASK);
    debug_assert!(cm_method == TOAST_PGLZ_COMPRESSION_ID || cm_method == TOAST_LZ4_COMPRESSION_ID);
    let word = datum
        .get_mut(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(truncated_compressed)?;
    let v = (len as u32) | (cm_method << VARLENA_EXTSIZE_BITS);
    word.copy_from_slice(&v.to_ne_bytes());
    Ok(())
}

fn tcinfo(datum: &[u8]) -> PgResult<u32> {
    let w = datum
        .get(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(truncated_compressed)?;
    Ok(u32::from_ne_bytes([w[0], w[1], w[2], w[3]]))
}

#[track_caller]
#[cold]
#[inline(never)]
fn truncated_compressed() -> Box<PgError> {
    Box::new(PgError::error("truncated compressed datum header"))
}

// HaveRegisteredOrActiveSnapshot() is threaded in: snapmgr owns that state.
pub fn get_toast_snapshot<'mcx>(
    mcx: Mcx<'mcx>,
    have_registered_or_active_snapshot: bool,
) -> PgResult<SnapshotData<'mcx>> {
    if !have_registered_or_active_snapshot {
        // C: elog(ERROR, ...) — internal error, XX000.
        return Err(
            Box::new(PgError::error("cannot fetch toast data without an active snapshot")),
        );
    }
    Ok(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_TOAST))
}
