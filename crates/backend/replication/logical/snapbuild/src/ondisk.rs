//! On-disk serialization of the snapshot builder (`SnapBuildOnDisk`).
//!
//! The C writes `SnapBuildOnDisk` (magic, checksum, version, length, then the
//! whole `SnapBuild` struct memcpy'd with its pointers NULL'd) followed by the
//! `committed.xcnt` and `catchange.xcnt` trailing `TransactionId` arrays, and
//! checksums everything after the checksum field. Our `SnapBuild` has owned
//! `Vec`s and can't be memcpy'd, so we serialize the version-dependent scalar
//! fields explicitly in the C struct's field order/size (the C-NULL pointer
//! fields are simply omitted), then the same trailing arrays, with the same
//! magic/version/length and CRC discipline. The format round-trips faithfully
//! for this implementation's own restarts.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use ::types_core::primitive::{TransactionId, XLogRecPtr};
use ::types_error::{PgError, PgResult};
use ::types_error::error::{ERRCODE_DATA_CORRUPTED, ERROR};

use crate::{crc32c, SnapBuild, SnapBuildState, SNAPBUILD_MAGIC, SNAPBUILD_VERSION};

/// The restored builder state (the version-dependent fields the C copies out of
/// `ondisk.builder`, plus the trailing xid arrays).
pub struct OnDisk {
    pub state: SnapBuildState,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub start_decoding_at: XLogRecPtr,
    pub two_phase_at: XLogRecPtr,
    pub initial_xmin_horizon: TransactionId,
    pub building_full_snapshot: bool,
    pub in_slot_creation: bool,
    pub last_serialized_snapshot: XLogRecPtr,
    pub next_phase_at: TransactionId,
    pub committed_includes_all_transactions: bool,
    pub committed_xcnt_space: usize,
    pub committed_xip: Vec<TransactionId>,
    pub catchange_xip: Vec<TransactionId>,
}

/// The fixed-size, version-dependent scalar image of `SnapBuild` (the part the
/// C memcpy's, minus the NULL'd pointer fields), in C struct field order.
///
/// Layout (little-endian, the only target):
///   state:i32, xmin:u32, xmax:u32, start_decoding_at:u64, two_phase_at:u64,
///   initial_xmin_horizon:u32, building_full_snapshot:u8, in_slot_creation:u8,
///   last_serialized_snapshot:u64, next_phase_at:u32,
///   committed.xcnt:u64, committed.xcnt_space:u64,
///   committed.includes_all_transactions:u8,
///   catchange.xcnt:u64
const BUILDER_IMAGE_LEN: usize =
    4 + 4 + 4 + 8 + 8 + 4 + 1 + 1 + 8 + 4 + 8 + 8 + 1 + 8;

const HEADER_LEN: usize = 4 /*magic*/ + 4 /*checksum*/ + 4 /*version*/ + 4 /*length*/;

/// Build the complete on-disk byte image.
pub fn serialize(
    builder: &SnapBuild,
    catchange_xcnt: usize,
    catchange_xip: &[TransactionId],
) -> Vec<u8> {
    let committed_xcnt = builder.committed.xcnt;

    let trailing = (committed_xcnt + catchange_xcnt) * 4;
    let total = HEADER_LEN + BUILDER_IMAGE_LEN + trailing;

    let mut out = Vec::with_capacity(total);

    // magic
    out.extend_from_slice(&SNAPBUILD_MAGIC.to_le_bytes());
    // checksum placeholder (filled in after)
    out.extend_from_slice(&0u32.to_le_bytes());
    // version, length
    out.extend_from_slice(&SNAPBUILD_VERSION.to_le_bytes());
    out.extend_from_slice(&(total as u32).to_le_bytes());

    // builder image (the version-dependent fields)
    push_builder_image(&mut out, builder, catchange_xcnt);

    // committed xids (->xcnt of them)
    for &x in &builder.committed.xip[..committed_xcnt] {
        out.extend_from_slice(&x.to_le_bytes());
    }
    // catchange xids
    for &x in &catchange_xip[..catchange_xcnt] {
        out.extend_from_slice(&x.to_le_bytes());
    }

    // checksum covers everything after the checksum field (version onward),
    // matching the C SnapBuildOnDiskNotChecksummedSize discipline.
    let checksum = crc32c(&out[8..]);
    out[4..8].copy_from_slice(&checksum.to_le_bytes());

    out
}

fn push_builder_image(out: &mut Vec<u8>, b: &SnapBuild, catchange_xcnt: usize) {
    out.extend_from_slice(&b.state.to_le_bytes());
    out.extend_from_slice(&b.xmin.to_le_bytes());
    out.extend_from_slice(&b.xmax.to_le_bytes());
    out.extend_from_slice(&b.start_decoding_at.to_le_bytes());
    out.extend_from_slice(&b.two_phase_at.to_le_bytes());
    out.extend_from_slice(&b.initial_xmin_horizon.to_le_bytes());
    out.push(b.building_full_snapshot as u8);
    out.push(b.in_slot_creation as u8);
    out.extend_from_slice(&b.last_serialized_snapshot.to_le_bytes());
    out.extend_from_slice(&b.next_phase_at.to_le_bytes());
    out.extend_from_slice(&(b.committed.xcnt as u64).to_le_bytes());
    out.extend_from_slice(&(b.committed.xcnt_space as u64).to_le_bytes());
    out.push(b.committed.includes_all_transactions as u8);
    // C stores catchange.xcnt as the count to be serialized (the running
    // catalog-changing xacts), not the in-memory builder->catchange.xcnt.
    out.extend_from_slice(&(catchange_xcnt as u64).to_le_bytes());
}

/// Parse and validate an on-disk image (the C `SnapBuildRestoreSnapshot`).
pub fn deserialize(bytes: &[u8], path: &str) -> PgResult<OnDisk> {
    if bytes.len() < HEADER_LEN + BUILDER_IMAGE_LEN {
        return Err(corrupt(path, alloc::format!(
            "snapbuild state file \"{}\" is too short", path
        )));
    }

    let magic = u32_at(bytes, 0);
    let stored_checksum = u32_at(bytes, 4);
    let version = u32_at(bytes, 8);
    let _length = u32_at(bytes, 12);

    if magic != SNAPBUILD_MAGIC {
        return Err(corrupt(path, alloc::format!(
            "snapbuild state file \"{}\" has wrong magic number: {} instead of {}",
            path, magic, SNAPBUILD_MAGIC
        )));
    }
    if version != SNAPBUILD_VERSION {
        return Err(corrupt(path, alloc::format!(
            "snapbuild state file \"{}\" has unsupported version: {} instead of {}",
            path, version, SNAPBUILD_VERSION
        )));
    }

    // builder image
    let mut p = HEADER_LEN;
    let state = i32_at(bytes, p);
    p += 4;
    let xmin = u32_at(bytes, p);
    p += 4;
    let xmax = u32_at(bytes, p);
    p += 4;
    let start_decoding_at = u64_at(bytes, p);
    p += 8;
    let two_phase_at = u64_at(bytes, p);
    p += 8;
    let initial_xmin_horizon = u32_at(bytes, p);
    p += 4;
    let building_full_snapshot = bytes[p] != 0;
    p += 1;
    let in_slot_creation = bytes[p] != 0;
    p += 1;
    let last_serialized_snapshot = u64_at(bytes, p);
    p += 8;
    let next_phase_at = u32_at(bytes, p);
    p += 4;
    let committed_xcnt = u64_at(bytes, p) as usize;
    p += 8;
    let committed_xcnt_space = u64_at(bytes, p) as usize;
    p += 8;
    let committed_includes_all_transactions = bytes[p] != 0;
    p += 1;
    let catchange_xcnt = u64_at(bytes, p) as usize;
    p += 8;

    // trailing arrays
    let need = (committed_xcnt + catchange_xcnt) * 4;
    if bytes.len() < p + need {
        return Err(corrupt(path, alloc::format!(
            "could not read file \"{}\": read {} of expected", path, bytes.len()
        )));
    }

    let mut committed_xip = Vec::with_capacity(committed_xcnt);
    for _ in 0..committed_xcnt {
        committed_xip.push(u32_at(bytes, p));
        p += 4;
    }
    let mut catchange_xip = Vec::with_capacity(catchange_xcnt);
    for _ in 0..catchange_xcnt {
        catchange_xip.push(u32_at(bytes, p));
        p += 4;
    }

    // verify checksum over everything after the checksum field
    let checksum = crc32c(&bytes[8..p]);
    if checksum != stored_checksum {
        return Err(corrupt(path, alloc::format!(
            "checksum mismatch for snapbuild state file \"{}\": is {}, should be {}",
            path, checksum, stored_checksum
        )));
    }

    Ok(OnDisk {
        state,
        xmin,
        xmax,
        start_decoding_at,
        two_phase_at,
        initial_xmin_horizon,
        building_full_snapshot,
        in_slot_creation,
        last_serialized_snapshot,
        next_phase_at,
        committed_includes_all_transactions,
        committed_xcnt_space,
        committed_xip,
        catchange_xip,
    })
}

fn corrupt(_path: &str, msg: String) -> PgError {
    PgError::new(ERROR, msg).with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
fn i32_at(b: &[u8], off: usize) -> i32 {
    i32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
fn u64_at(b: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(a)
}
