use elog::{ereport, errno};
use types_core::{TransactionId, XLogRecPtr};
use types_error::{PgResult, ERRCODE_DATA_CORRUPTED, ERROR};

use crate::{loc, SnapBuild};

pub const SNAPBUILD_MAGIC: u32 = 0x51A1E001;
pub const SNAPBUILD_VERSION: u32 = 6;
pub const PG_LOGICAL_SNAPSHOTS_DIR: &str = "pg_logical/snapshots";

pub const SNAP_BUILD_ON_DISK_CONSTANT_SIZE: usize = 16;
pub const SNAP_BUILD_ON_DISK_NOT_CHECKSUMMED_SIZE: usize = 8;
pub const SIZEOF_SNAP_BUILD: usize = 128;
pub const SNAP_BUILD_HEADER_SIZE: usize = SNAP_BUILD_ON_DISK_CONSTANT_SIZE + SIZEOF_SNAP_BUILD;

const OFF_MAGIC: usize = 0;
const OFF_CHECKSUM: usize = 4;
const OFF_VERSION: usize = 8;
const OFF_LENGTH: usize = 12;
const OFF_BUILDER: usize = 16;

const OFF_STATE: usize = 0;
const OFF_CONTEXT: usize = 8;
const OFF_XMIN: usize = 16;
const OFF_XMAX: usize = 20;
const OFF_START_DECODING_AT: usize = 24;
const OFF_TWO_PHASE_AT: usize = 32;
const OFF_INITIAL_XMIN_HORIZON: usize = 40;
const OFF_BUILDING_FULL_SNAPSHOT: usize = 44;
const OFF_IN_SLOT_CREATION: usize = 45;
const OFF_SNAPSHOT: usize = 48;
const OFF_LAST_SERIALIZED_SNAPSHOT: usize = 56;
const OFF_REORDER: usize = 64;
const OFF_NEXT_PHASE_AT: usize = 72;
const OFF_COMMITTED_XCNT: usize = 80;
const OFF_COMMITTED_XCNT_SPACE: usize = 88;
const OFF_COMMITTED_INCLUDES_ALL: usize = 96;
const OFF_COMMITTED_XIP: usize = 104;
const OFF_CATCHANGE_XCNT: usize = 112;
const OFF_CATCHANGE_XIP: usize = 120;

// C struct SnapBuild / SnapBuildOnDisk mirrors (snapbuild_internal.h); the
// offset constants above are the serialization ground truth and must match
// the LP64 layout a C server writes and restores.
#[repr(C)]
struct CSnapBuildCommitted {
    xcnt: usize,
    xcnt_space: usize,
    includes_all_transactions: bool,
    xip: *mut TransactionId,
}

#[repr(C)]
struct CSnapBuildCatchange {
    xcnt: usize,
    xip: *mut TransactionId,
}

#[repr(C)]
struct CSnapBuild {
    state: i32,
    context: *mut core::ffi::c_void,
    xmin: u32,
    xmax: u32,
    start_decoding_at: u64,
    two_phase_at: u64,
    initial_xmin_horizon: u32,
    building_full_snapshot: bool,
    in_slot_creation: bool,
    snapshot: *mut core::ffi::c_void,
    last_serialized_snapshot: u64,
    reorder: *mut core::ffi::c_void,
    next_phase_at: u32,
    committed: CSnapBuildCommitted,
    catchange: CSnapBuildCatchange,
}

#[repr(C)]
struct CSnapBuildOnDisk {
    magic: u32,
    checksum: u32,
    version: u32,
    length: u32,
    builder: CSnapBuild,
}

// wasm32: CSnapBuild embeds raw pointers (serialized as garbage, fixed up on
// read — C's own shape), so the on-disk layout is pointer-width-dependent and
// these 64-bit pins cannot hold on a 32-bit target. A wasm-written snapshot
// file is therefore NOT byte-compatible with a native one (C-on-32-bit has
// the same property); logical decoding is not on the wasm ladder.
#[cfg(not(target_family = "wasm"))]
const _: () = {
    use core::mem::{offset_of, size_of};
    assert!(size_of::<CSnapBuild>() == SIZEOF_SNAP_BUILD);
    assert!(size_of::<CSnapBuildOnDisk>() == SNAP_BUILD_HEADER_SIZE);
    assert!(offset_of!(CSnapBuildOnDisk, magic) == OFF_MAGIC);
    assert!(offset_of!(CSnapBuildOnDisk, checksum) == OFF_CHECKSUM);
    assert!(offset_of!(CSnapBuildOnDisk, version) == OFF_VERSION);
    assert!(offset_of!(CSnapBuildOnDisk, length) == OFF_LENGTH);
    assert!(offset_of!(CSnapBuildOnDisk, builder) == OFF_BUILDER);
    assert!(offset_of!(CSnapBuild, state) == OFF_STATE);
    assert!(offset_of!(CSnapBuild, context) == OFF_CONTEXT);
    assert!(offset_of!(CSnapBuild, xmin) == OFF_XMIN);
    assert!(offset_of!(CSnapBuild, xmax) == OFF_XMAX);
    assert!(offset_of!(CSnapBuild, start_decoding_at) == OFF_START_DECODING_AT);
    assert!(offset_of!(CSnapBuild, two_phase_at) == OFF_TWO_PHASE_AT);
    assert!(offset_of!(CSnapBuild, initial_xmin_horizon) == OFF_INITIAL_XMIN_HORIZON);
    assert!(offset_of!(CSnapBuild, building_full_snapshot) == OFF_BUILDING_FULL_SNAPSHOT);
    assert!(offset_of!(CSnapBuild, in_slot_creation) == OFF_IN_SLOT_CREATION);
    assert!(offset_of!(CSnapBuild, snapshot) == OFF_SNAPSHOT);
    assert!(offset_of!(CSnapBuild, last_serialized_snapshot) == OFF_LAST_SERIALIZED_SNAPSHOT);
    assert!(offset_of!(CSnapBuild, reorder) == OFF_REORDER);
    assert!(offset_of!(CSnapBuild, next_phase_at) == OFF_NEXT_PHASE_AT);
    assert!(offset_of!(CSnapBuild, committed) + offset_of!(CSnapBuildCommitted, xcnt) == OFF_COMMITTED_XCNT);
    assert!(
        offset_of!(CSnapBuild, committed) + offset_of!(CSnapBuildCommitted, xcnt_space)
            == OFF_COMMITTED_XCNT_SPACE
    );
    assert!(
        offset_of!(CSnapBuild, committed)
            + offset_of!(CSnapBuildCommitted, includes_all_transactions)
            == OFF_COMMITTED_INCLUDES_ALL
    );
    assert!(offset_of!(CSnapBuild, committed) + offset_of!(CSnapBuildCommitted, xip) == OFF_COMMITTED_XIP);
    assert!(offset_of!(CSnapBuild, catchange) + offset_of!(CSnapBuildCatchange, xcnt) == OFF_CATCHANGE_XCNT);
    assert!(offset_of!(CSnapBuild, catchange) + offset_of!(CSnapBuildCatchange, xip) == OFF_CATCHANGE_XIP);
};

fn cstring(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).expect("no interior NUL in paths")
}

pub(crate) fn c_unlink(path: &str) -> i32 {
    // SAFETY: NUL-terminated path.
    unsafe { libc::unlink(cstring(path).as_ptr()) }
}

pub(crate) fn c_rename(old: &str, new: &str) -> i32 {
    // SAFETY: NUL-terminated paths.
    unsafe { libc::rename(cstring(old).as_ptr(), cstring(new).as_ptr()) }
}

pub fn snapshot_path(lsn: XLogRecPtr) -> String {
    format!(
        "{}/{:X}-{:X}.snap",
        PG_LOGICAL_SNAPSHOTS_DIR,
        (lsn >> 32) as u32,
        lsn as u32
    )
}

// sscanf("%X-%X.snap") semantics: both conversions succeeding counts as a
// parse even if the ".snap" literal never matches (tmp files rely on this).
pub(crate) fn parse_snap_name(name: &str) -> Option<(u32, u32)> {
    let (hi_s, rest) = name.split_once('-')?;
    if hi_s.is_empty() || !hi_s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let hi = u32::from_str_radix(hi_s, 16).ok()?;
    let lo_len = rest
        .bytes()
        .take_while(|b| b.is_ascii_hexdigit())
        .count();
    if lo_len == 0 {
        return None;
    }
    let lo = u32::from_str_radix(&rest[..lo_len], 16).ok()?;
    Some((hi, lo))
}

fn put(buf: &mut [u8], off: usize, bytes: &[u8]) {
    buf[off..off + bytes.len()].copy_from_slice(bytes);
}

fn get_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

fn get_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

fn get_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(buf[off..off + 8].try_into().unwrap())
}

// SnapBuildOnDisk after SnapBuildRestoreSnapshot: the version-dependent
// builder fields plus the trailing xid arrays.
pub struct SnapBuildOnDisk {
    pub magic: u32,
    pub checksum: u32,
    pub version: u32,
    pub length: u32,
    pub state: i32,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub start_decoding_at: XLogRecPtr,
    pub two_phase_at: XLogRecPtr,
    pub initial_xmin_horizon: TransactionId,
    pub building_full_snapshot: bool,
    pub in_slot_creation: bool,
    pub last_serialized_snapshot: XLogRecPtr,
    pub next_phase_at: TransactionId,
    pub committed_xcnt_space: u64,
    pub committed_includes_all_transactions: bool,
    pub committed: Vec<TransactionId>,
    pub catchange: Vec<TransactionId>,
}

pub(crate) fn build_image(builder: &SnapBuild, catchange_xip: &[TransactionId]) -> Vec<u8> {
    let committed: &[TransactionId] = &builder.committed_xip;
    let needed_length =
        SNAP_BUILD_HEADER_SIZE + 4 * (committed.len() + catchange_xip.len());
    let mut image = vec![0u8; needed_length];

    put(&mut image, OFF_MAGIC, &SNAPBUILD_MAGIC.to_ne_bytes());
    put(&mut image, OFF_VERSION, &SNAPBUILD_VERSION.to_ne_bytes());
    put(&mut image, OFF_LENGTH, &(needed_length as u32).to_ne_bytes());

    let b = &mut image[OFF_BUILDER..OFF_BUILDER + SIZEOF_SNAP_BUILD];
    put(b, OFF_STATE, &(builder.state as i32).to_ne_bytes());
    put(b, OFF_XMIN, &builder.xmin.to_ne_bytes());
    put(b, OFF_XMAX, &builder.xmax.to_ne_bytes());
    put(b, OFF_START_DECODING_AT, &builder.start_decoding_at.to_ne_bytes());
    put(b, OFF_TWO_PHASE_AT, &builder.two_phase_at.to_ne_bytes());
    put(
        b,
        OFF_INITIAL_XMIN_HORIZON,
        &builder.initial_xmin_horizon.to_ne_bytes(),
    );
    b[OFF_BUILDING_FULL_SNAPSHOT] = builder.building_full_snapshot as u8;
    b[OFF_IN_SLOT_CREATION] = builder.in_slot_creation as u8;
    put(
        b,
        OFF_LAST_SERIALIZED_SNAPSHOT,
        &builder.last_serialized_snapshot.to_ne_bytes(),
    );
    put(b, OFF_NEXT_PHASE_AT, &builder.next_phase_at.to_ne_bytes());
    put(b, OFF_COMMITTED_XCNT, &(committed.len() as u64).to_ne_bytes());
    put(
        b,
        OFF_COMMITTED_XCNT_SPACE,
        &(builder.committed_xcnt_space as u64).to_ne_bytes(),
    );
    b[OFF_COMMITTED_INCLUDES_ALL] = builder.committed_includes_all_transactions as u8;
    put(b, OFF_CATCHANGE_XCNT, &(catchange_xip.len() as u64).to_ne_bytes());

    let mut off = SNAP_BUILD_HEADER_SIZE;
    for &xid in committed {
        put(&mut image, off, &xid.to_ne_bytes());
        off += 4;
    }
    for &xid in catchange_xip {
        put(&mut image, off, &xid.to_ne_bytes());
        off += 4;
    }
    debug_assert_eq!(off, needed_length);

    let checksum = image_checksum(&image);
    put(&mut image, OFF_CHECKSUM, &checksum.to_ne_bytes());
    image
}

pub(crate) fn image_checksum(image: &[u8]) -> u32 {
    let crc = crc32c::pg_comp_crc32c(
        crc32c::CRC32C_INIT,
        &image[SNAP_BUILD_ON_DISK_NOT_CHECKSUMMED_SIZE..],
    );
    crc32c::fin_crc32c(crc)
}

fn restore_contents(fd: i32, dest: &mut [u8], path: &str) -> PgResult<()> {
    // SAFETY: dest is a live writable buffer of dest.len() bytes.
    let read_bytes = unsafe { libc::read(fd, dest.as_mut_ptr().cast(), dest.len()) };
    if read_bytes != dest.len() as isize {
        let save_errno = errno::current_errno();
        fd::CloseTransientFile(fd);

        if read_bytes < 0 {
            return ereport(ERROR)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc("SnapBuildRestoreContents"));
        }
        return ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read file \"{path}\": read {} of {}",
                read_bytes,
                dest.len()
            ))
            .finish(loc("SnapBuildRestoreContents"));
    }
    Ok(())
}

pub fn restore_snapshot(lsn: XLogRecPtr, missing_ok: bool) -> PgResult<Option<SnapBuildOnDisk>> {
    let path = snapshot_path(lsn);

    let fd_ = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if fd_ < 0 {
        if missing_ok && errno::current_errno() == libc::ENOENT {
            return Ok(None);
        }
        return ereport(ERROR)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc("SnapBuildRestoreSnapshot"))
            .map(|_| None);
    }

    fd::fsync_fname(&path, false)?;
    fd::fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

    let mut header = [0u8; SNAP_BUILD_ON_DISK_CONSTANT_SIZE];
    restore_contents(fd_, &mut header, &path)?;

    let magic = get_u32(&header, OFF_MAGIC);
    let checksum = get_u32(&header, OFF_CHECKSUM);
    let version = get_u32(&header, OFF_VERSION);
    let length = get_u32(&header, OFF_LENGTH);

    if magic != SNAPBUILD_MAGIC {
        return ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "snapbuild state file \"{path}\" has wrong magic number: {magic} instead of {SNAPBUILD_MAGIC}"
            ))
            .finish(loc("SnapBuildRestoreSnapshot"))
            .map(|_| None);
    }

    if version != SNAPBUILD_VERSION {
        return ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "snapbuild state file \"{path}\" has unsupported version: {version} instead of {SNAPBUILD_VERSION}"
            ))
            .finish(loc("SnapBuildRestoreSnapshot"))
            .map(|_| None);
    }

    let mut crc = crc32c::pg_comp_crc32c(
        crc32c::CRC32C_INIT,
        &header[SNAP_BUILD_ON_DISK_NOT_CHECKSUMMED_SIZE..],
    );

    let mut builder = [0u8; SIZEOF_SNAP_BUILD];
    restore_contents(fd_, &mut builder, &path)?;
    crc = crc32c::pg_comp_crc32c(crc, &builder);

    let committed_xcnt = get_u64(&builder, OFF_COMMITTED_XCNT) as usize;
    let catchange_xcnt = get_u64(&builder, OFF_CATCHANGE_XCNT) as usize;

    let mut committed_bytes = vec![0u8; committed_xcnt * 4];
    if committed_xcnt > 0 {
        restore_contents(fd_, &mut committed_bytes, &path)?;
        crc = crc32c::pg_comp_crc32c(crc, &committed_bytes);
    }

    let mut catchange_bytes = vec![0u8; catchange_xcnt * 4];
    if catchange_xcnt > 0 {
        restore_contents(fd_, &mut catchange_bytes, &path)?;
        crc = crc32c::pg_comp_crc32c(crc, &catchange_bytes);
    }

    if fd::CloseTransientFile(fd_) != 0 {
        return ereport(ERROR)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(loc("SnapBuildRestoreSnapshot"))
            .map(|_| None);
    }

    let crc = crc32c::fin_crc32c(crc);

    if crc != checksum {
        return ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "checksum mismatch for snapbuild state file \"{path}\": is {crc}, should be {checksum}"
            ))
            .finish(loc("SnapBuildRestoreSnapshot"))
            .map(|_| None);
    }

    let committed = committed_bytes
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    let catchange = catchange_bytes
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect();

    Ok(Some(SnapBuildOnDisk {
        magic,
        checksum,
        version,
        length,
        state: get_i32(&builder, OFF_STATE),
        xmin: get_u32(&builder, OFF_XMIN),
        xmax: get_u32(&builder, OFF_XMAX),
        start_decoding_at: get_u64(&builder, OFF_START_DECODING_AT),
        two_phase_at: get_u64(&builder, OFF_TWO_PHASE_AT),
        initial_xmin_horizon: get_u32(&builder, OFF_INITIAL_XMIN_HORIZON),
        building_full_snapshot: builder[OFF_BUILDING_FULL_SNAPSHOT] != 0,
        in_slot_creation: builder[OFF_IN_SLOT_CREATION] != 0,
        last_serialized_snapshot: get_u64(&builder, OFF_LAST_SERIALIZED_SNAPSHOT),
        next_phase_at: get_u32(&builder, OFF_NEXT_PHASE_AT),
        committed_xcnt_space: get_u64(&builder, OFF_COMMITTED_XCNT_SPACE),
        committed_includes_all_transactions: builder[OFF_COMMITTED_INCLUDES_ALL] != 0,
        committed,
        catchange,
    }))
}
