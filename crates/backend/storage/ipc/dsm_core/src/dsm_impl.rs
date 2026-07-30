use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::cell::Cell;
use std::sync::Mutex;

use elog::ereport;
use types_error::{ErrorLevel, ErrorLocation, PgResult, ERRCODE_OUT_OF_MEMORY};
use types_guc::config_enum_entry;
use types_storage::dsm_handle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DsmOp {
    Create,
    Attach,
    Detach,
    Destroy,
}

pub const DSM_IMPL_POSIX: i32 = 1;
pub const DSM_IMPL_SYSV: i32 = 2;
pub const DSM_IMPL_WINDOWS: i32 = 3;
pub const DSM_IMPL_MMAP: i32 = 4;
pub const DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE: i32 = DSM_IMPL_POSIX;

pub const PG_DYNSHMEM_DIR: &str = "pg_dynshmem";
pub const PG_DYNSHMEM_MMAP_FILE_PREFIX: &str = "mmap.";

pub static DYNAMIC_SHARED_MEMORY_OPTIONS: &[config_enum_entry] = &[
    config_enum_entry {
        name: "posix",
        val: DSM_IMPL_POSIX,
        hidden: false,
    },
    config_enum_entry {
        name: "sysv",
        val: DSM_IMPL_SYSV,
        hidden: false,
    },
    config_enum_entry {
        name: "mmap",
        val: DSM_IMPL_MMAP,
        hidden: false,
    },
];

thread_local! {
    static DYNAMIC_SHARED_MEMORY_TYPE: Cell<i32> =
        const { Cell::new(DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE) };
    static MIN_DYNAMIC_SHARED_MEMORY: Cell<i32> = const { Cell::new(0) };
}

pub fn dynamic_shared_memory_type() -> i32 {
    DYNAMIC_SHARED_MEMORY_TYPE.with(|c| c.get())
}

pub fn set_dynamic_shared_memory_type(value: i32) {
    DYNAMIC_SHARED_MEMORY_TYPE.with(|c| c.set(value));
}

pub fn min_dynamic_shared_memory() -> i32 {
    MIN_DYNAMIC_SHARED_MEMORY.with(|c| c.get())
}

pub fn set_min_dynamic_shared_memory(value: i32) {
    MIN_DYNAMIC_SHARED_MEMORY.with(|c| c.set(value));
}

// Model mapping (single process, thread-per-backend): the OS shm namespace
// behind dsm_impl.c's posix/sysv/mmap/windows arms collapses to heap regions
// keyed by handle, and every dynamic_shared_memory_type routes here (the OS
// arms are unported). dsm.c's refcount protocol runs DSM_OP_DESTROY only
// once no backend holds a mapping, so freeing the region here is safe.
struct Region {
    handle: dsm_handle,
    addr: usize,
    size: usize,
}

static REGIONS: Mutex<Vec<Region>> = Mutex::new(Vec::new());

const REGION_ALIGN: usize = 4096;

fn region_layout(size: usize) -> Layout {
    Layout::from_size_align(size.max(1), REGION_ALIGN).expect("dsm region layout")
}

fn segment_name(handle: dsm_handle) -> String {
    format!("/PostgreSQL.{handle}")
}

#[cold]
fn report_errno(elevel: ErrorLevel, saved_errno: i32, msg: String) -> PgResult<()> {
    let builder = ereport(elevel).with_saved_errno(saved_errno);
    let builder = if saved_errno == libc::EFBIG || saved_errno == libc::ENOMEM {
        builder.errcode(ERRCODE_OUT_OF_MEMORY)
    } else {
        builder.errcode_for_file_access()
    };
    builder
        .errmsg(msg)
        .finish(ErrorLocation::new(file!(), line!() as i32, "dsm_impl_op"))
}

pub fn dsm_impl_op(
    op: DsmOp,
    handle: dsm_handle,
    request_size: usize,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    debug_assert!(op == DsmOp::Create || request_size == 0);
    debug_assert!(
        (op != DsmOp::Create && op != DsmOp::Attach)
            || (mapped_address.is_null() && *mapped_size == 0)
    );

    match op {
        DsmOp::Create => {
            let mut regions = REGIONS.lock().unwrap();
            if regions.iter().any(|r| r.handle == handle) {
                return Ok(false);
            }
            // Zeroed like fresh shm/tmpfs pages; consumers rely on it.
            // SAFETY: layout size is non-zero.
            let ptr = unsafe { alloc_zeroed(region_layout(request_size)) };
            if ptr.is_null() {
                drop(regions);
                report_errno(
                    elevel,
                    libc::ENOMEM,
                    format!(
                        "could not resize shared memory segment \"{}\" to {request_size} bytes: %m",
                        segment_name(handle)
                    ),
                )?;
                return Ok(false);
            }
            regions.push(Region {
                handle,
                addr: ptr as usize,
                size: request_size,
            });
            *mapped_address = ptr;
            *mapped_size = request_size;
            Ok(true)
        }
        DsmOp::Attach => {
            let found = REGIONS
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.handle == handle)
                .map(|r| (r.addr, r.size));
            match found {
                Some((addr, size)) => {
                    *mapped_address = addr as *mut u8;
                    *mapped_size = size;
                    Ok(true)
                }
                None => {
                    report_errno(
                        elevel,
                        libc::ENOENT,
                        format!(
                            "could not open shared memory segment \"{}\": %m",
                            segment_name(handle)
                        ),
                    )?;
                    Ok(false)
                }
            }
        }
        DsmOp::Detach => {
            *mapped_address = std::ptr::null_mut();
            *mapped_size = 0;
            Ok(true)
        }
        DsmOp::Destroy => {
            *mapped_address = std::ptr::null_mut();
            *mapped_size = 0;
            let region = {
                let mut regions = REGIONS.lock().unwrap();
                regions
                    .iter()
                    .position(|r| r.handle == handle)
                    .map(|i| regions.swap_remove(i))
            };
            match region {
                Some(r) => {
                    // SAFETY: allocated by Create with this same layout; the
                    // refcount protocol guarantees no live mappings remain.
                    unsafe { dealloc(r.addr as *mut u8, region_layout(r.size)) };
                    Ok(true)
                }
                None => {
                    report_errno(
                        elevel,
                        libc::ENOENT,
                        format!(
                            "could not remove shared memory segment \"{}\": %m",
                            segment_name(handle)
                        ),
                    )?;
                    Ok(false)
                }
            }
        }
    }
}

// Only the unported Windows arm does work in pin/unpin.
pub fn dsm_impl_pin_segment(_handle: dsm_handle) {}

pub fn dsm_impl_unpin_segment(_handle: dsm_handle) {}
