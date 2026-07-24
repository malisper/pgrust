//! aio_io.c: op association + method-independent synchronous execution.

use std::sync::atomic::Ordering;

use init_small::globals as g;

use types_storage::aio::{PGAIO_OP_INVALID, PGAIO_OP_READV, PGAIO_OP_WRITEV};

use crate::{ioh, my_backend, NO_HANDLE, PGAIO_HS_HANDED_OUT};

const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_DATA_FILE_READ: u32 = PG_WAIT_IO + 21;
const WAIT_EVENT_DATA_FILE_WRITE: u32 = PG_WAIT_IO + 24;

/// merging adjacent pages (C buffers_to_iovec). Returns iovcnt.
/// SAFETY contract (caller): each pointer addresses `page_len` writable bytes
pub fn pgaio_io_set_iovec_pages(pages: &[*mut u8], page_len: usize) -> i32 {
    let index = current_handed_out("pgaio_io_set_iovec_pages");
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    debug_assert!(pages.len() <= guc_tables::vars::io_max_combine_limit.read() as usize);

    // SAFETY: owner fills its own handle's iovec region pre-submission.
    unsafe {
        let iov = crate::iovec_region(h.iovec_off);
        let mut iovcnt: usize = 0;
        for &page in pages {
            if iovcnt > 0 {
                let prev = &mut *iov.add(iovcnt - 1);
                if (prev.iov_base as *mut u8).add(prev.iov_len) == page {
                    prev.iov_len += page_len;
                    continue;
                }
            }
            let slot = &mut *iov.add(iovcnt);
            slot.iov_base = page as *mut libc::c_void;
            slot.iov_len = page_len;
            iovcnt += 1;
        }
        iovcnt as i32
    }
}

pub fn pgaio_io_start_readv_current(fd: i32, iovcnt: i32, offset: i64) -> types_error::PgResult<()> {
    let index = current_handed_out("pgaio_io_start_readv");
    pgaio_io_before_start(index);
    let h = ioh(index);
    // SAFETY: HANDED_OUT, owner thread.
    let d = unsafe { h.data() };
    d.op_data.fd = fd;
    d.op_data.offset = offset as u64;
    d.op_data.iov_length = iovcnt as u16;

    crate::handle::pgaio_io_stage(index, PGAIO_OP_READV)
}

fn current_handed_out(who: &str) -> u32 {
    // SAFETY: owner-thread slot access.
    let index = unsafe { my_backend() }.handed_out_io;
    assert!(index != NO_HANDLE, "{who}: no handed-out AIO handle");
    index
}

fn pgaio_io_before_start(index: u32) {
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: owner-thread slot access.
    debug_assert!(unsafe { my_backend() }.handed_out_io == index);
    debug_assert!(crate::target::pgaio_io_has_target(index));
    // SAFETY: HANDED_OUT, owner thread.
    debug_assert!(unsafe { h.data() }.op == PGAIO_OP_INVALID);
    // C: Assert(!INTERRUPTS_CAN_BE_PROCESSED()) — the fd must not be closable.
    debug_assert!(!g::InterruptsCanBeProcessed());
}

pub(crate) fn pgaio_io_perform_synchronously(index: u32) {
    let h = ioh(index);

    g::StartCriticalSection();

    // SAFETY: executing side owns d between SUBMITTED and completion.
    let (op, op_data) = unsafe {
        let d = h.data();
        (d.op, d.op_data)
    };

    let result: isize = match op {
        PGAIO_OP_READV => {
            waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_DATA_FILE_READ);
            // SAFETY: the iovec region was filled by the definer and the pages
            let r = unsafe {
                pg_preadv_raw(op_data.fd, crate::iovec_region(h.iovec_off), op_data.iov_length as i32, op_data.offset as i64)
            };
            waitevent_seams::pgstat_report_wait_end::call();
            r
        }
        PGAIO_OP_WRITEV => {
            waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_DATA_FILE_WRITE);
            // SAFETY: as READV.
            let r = unsafe {
                pg_pwritev_raw(op_data.fd, crate::iovec_region(h.iovec_off), op_data.iov_length as i32, op_data.offset as i64)
            };
            waitevent_seams::pgstat_report_wait_end::call();
            r
        }
        _ => panic!("trying to execute invalid IO operation"),
    };

    let result_i32: i32 = if result < 0 {
        -std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)
    } else {
        result as i32
    };
    h.result.store(result_i32, Ordering::Relaxed);

    crate::handle::pgaio_io_process_completion(index, result_i32);

    g::EndCriticalSection();
}

// Vector IO routes through the vfs provider, NEVER raw libc: the op fd came
// from FileRawDescForAio (the provider's descriptor domain). Product builds
// monomorphize to the identical single preadv/pwritev syscall (vfs contract
// §1.2 zero-cost law); under `--cfg pgrust_sim` the fd is a SimVfs handle
// foreign to the kernel — raw libc here EBADF'd every AIO read and killed
// the whole-server sim boot (GL-TESTFIX-1 F-R2-2). EINTR retry stays here:
// ops are single-shot below the trait (contract §1.1), and SimVfs never
// emits EINTR.
unsafe fn pg_preadv_raw(fd: i32, iov: *const libc::iovec, iovcnt: i32, offset: i64) -> isize {
    let iov = std::slice::from_raw_parts(iov, iovcnt as usize);
    loop {
        let r = vfs::preadv(fd, iov, offset as libc::off_t);
        if r < 0 && vfs::get_errno() == libc::EINTR {
            continue;
        }
        return r;
    }
}

unsafe fn pg_pwritev_raw(fd: i32, iov: *const libc::iovec, iovcnt: i32, offset: i64) -> isize {
    let iov = std::slice::from_raw_parts(iov, iovcnt as usize);
    loop {
        let r = vfs::pwritev(fd, iov, offset as libc::off_t);
        if r < 0 && vfs::get_errno() == libc::EINTR {
            continue;
        }
        return r;
    }
}

// Current-handle conveniences for the smgr/md chain (C threads the PgAioHandle
pub fn pgaio_io_current() -> u32 {
    current_handed_out("pgaio_io_current")
}
