//! `storage/aio/aio_io.c` — low-level IO handling: the per-op "start" routines,
//! the synchronous executor, and the op/fd introspection helpers.
//!
//! `op_data` is a single shared read/write layout in our model
//! ([`crate::PgAioOpData`] `{ fd, iov_length, offset }`), matching the C union
//! whose `read`/`write` arms share the layout. The actual `pg_preadv`/
//! `pg_pwritev` syscall is the one genuinely-unported leaf (it needs the fd/smgr
//! IO layer), seamed as `pgaio_perform_io_syscall`; everything around it (the
//! critical section, the result/-errno handling, and driving
//! `pgaio_io_process_completion`) is real here.

extern crate alloc;

use ::types_error::{PgError, PgResult};

use miscinit_seams as misc;

use crate::aio::{ioh, pgaio_io_process_completion, pgaio_io_stage};
use crate::aio_target::pgaio_io_has_target;
use crate::{pgaio_my_backend, PgAioHandleState, PGAIO_OP_INVALID, PGAIO_OP_READV, PGAIO_OP_WRITEV};

/// `int pgaio_io_get_iovec(PgAioHandle *ioh, struct iovec **iov)` (aio_io.c).
///
/// Scatter/gather IO associates an iovec sub-range with the handle. In C this
/// returns `*iov = &pgaio_ctl->iovecs[ioh->iovec_off]` (a pointer into the shared
/// iovec array) and `PG_IOV_MAX` as its capacity. The shared iovec array lives
/// behind a `Mutex` here, so the "pointer" is expressed as the `iovec_off` base
/// index into [`crate::pgaio_ctl().iovecs`]; the caller indexes
/// `[off .. off + PG_IOV_MAX]`.
pub fn pgaio_io_get_iovec(ioh_index: usize) -> (usize, usize) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == PgAioHandleState::HandedOut);
    (h.iovec_off as usize, crate::PG_IOV_MAX)
}

/// `PgAioOp pgaio_io_get_op(PgAioHandle *ioh)` (aio_io.c).
pub fn pgaio_io_get_op(ioh_index: usize) -> u8 {
    ioh(ioh_index).data().op
}

/// `PgAioOpData *pgaio_io_get_op_data(PgAioHandle *ioh)` (aio_io.c).
///
/// C returns `&ioh->op_data` (a mutable pointer); the handle's `op_data` is
/// filled only by the start routines (which mutate it through the per-handle
/// data lock), so this exposes a copy for the read/introspection consumers
/// (method_io_uring / aio_funcs).
pub fn pgaio_io_get_op_data(ioh_index: usize) -> crate::PgAioOpData {
    ioh(ioh_index).data().op_data
}

/// `void pgaio_io_start_readv(PgAioHandle *ioh, int fd, int iovcnt, uint64 off)`
/// (aio_io.c). Reached through the `pgaio_io_start_readv` seam from the VFD
/// layer; the handle is `pgaio_my_backend->handed_out_io`.
pub fn pgaio_io_start_readv(fd: i32, iovcnt: i32, offset: u64) {
    let ioh_index = pgaio_my_backend()
        .and_then(|b| crate::pgaio_ctl().backend_state[b].lock().unwrap().handed_out_io)
        .expect("pgaio_io_start_readv: no handed-out IO handle");

    pgaio_io_before_start(ioh_index);

    {
        let h = ioh(ioh_index);
        let mut d = h.data();
        d.op_data.fd = fd;
        d.op_data.offset = offset;
        d.op_data.iov_length = iovcnt as u16;
    }

    pgaio_io_stage(ioh_index, PGAIO_OP_READV).expect("pgaio_io_start_readv: stage");
}

/// `void pgaio_io_start_writev(PgAioHandle *ioh, int fd, int iovcnt, uint64 off)`
/// (aio_io.c).
pub fn pgaio_io_start_writev(ioh_index: usize, fd: i32, iovcnt: i32, offset: u64) -> PgResult<()> {
    pgaio_io_before_start(ioh_index);

    {
        let h = ioh(ioh_index);
        let mut d = h.data();
        d.op_data.fd = fd;
        d.op_data.offset = offset;
        d.op_data.iov_length = iovcnt as u16;
    }

    pgaio_io_stage(ioh_index, PGAIO_OP_WRITEV)
}

/// `void pgaio_io_perform_synchronously(PgAioHandle *ioh)` (aio_io.c).
pub fn pgaio_io_perform_synchronously(ioh_index: usize) -> PgResult<()> {
    misc::start_crit_section::call();

    // Perform IO. The raw preadv/pwritev bottoms out in the unported fd/smgr
    // layer; the result is the raw `ssize_t` (`-errno` on failure).
    let op = ioh(ioh_index).data().op;
    let result: i64 = match op {
        PGAIO_OP_READV | PGAIO_OP_WRITEV => {
            completion_seams::pgaio_perform_io_syscall::call(ioh_index as u32)?
        }
        PGAIO_OP_INVALID => {
            return Err(PgError::error("trying to execute invalid IO operation"));
        }
        other => {
            return Err(PgError::error(alloc::format!(
                "trying to execute invalid IO operation {other}"
            )));
        }
    };

    let raw = result as i32;
    ioh(ioh_index).result.store(raw, core::sync::atomic::Ordering::Relaxed);

    pgaio_io_process_completion(ioh_index, raw)?;

    misc::end_crit_section::call();
    Ok(())
}

/// `static void pgaio_io_before_start(PgAioHandle *ioh)` (aio_io.c) — the shared
/// pre-start assertions.
fn pgaio_io_before_start(ioh_index: usize) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == PgAioHandleState::HandedOut);
    debug_assert!(
        pgaio_my_backend()
            .map(|b| crate::pgaio_ctl().backend_state[b].lock().unwrap().handed_out_io)
            == Some(Some(ioh_index))
    );
    debug_assert!(pgaio_io_has_target(ioh_index));
    debug_assert!(h.data().op == PGAIO_OP_INVALID);
    // Assert(!INTERRUPTS_CAN_BE_PROCESSED()).
}

/// `const char *pgaio_io_get_op_name(PgAioHandle *ioh)` (aio_io.c).
pub fn pgaio_io_get_op_name(ioh_index: usize) -> &'static str {
    match ioh(ioh_index).data().op {
        PGAIO_OP_INVALID => "invalid",
        PGAIO_OP_READV => "readv",
        PGAIO_OP_WRITEV => "writev",
        _ => "invalid",
    }
}

/// `bool pgaio_io_uses_fd(PgAioHandle *ioh, int fd)` (aio_io.c).
pub fn pgaio_io_uses_fd(ioh_index: usize, fd: i32) -> bool {
    let h = ioh(ioh_index);
    debug_assert!(h.state() as u8 >= PgAioHandleState::Defined as u8);
    let d = h.data();
    match d.op {
        PGAIO_OP_READV | PGAIO_OP_WRITEV => d.op_data.fd == fd,
        _ => false,
    }
}

/// `int pgaio_io_get_iovec_length(PgAioHandle *ioh, struct iovec **iov)`
/// (aio_io.c). Currently only expected to be used by debugging infrastructure.
///
/// Sets `*iov = &pgaio_ctl->iovecs[ioh->iovec_off]` (returned as the `iovec_off`
/// base index, like [`pgaio_io_get_iovec`]) and returns the op's actual iovec
/// length.
pub fn pgaio_io_get_iovec_length(ioh_index: usize) -> (usize, i32) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() as u8 >= PgAioHandleState::Defined as u8);
    let d = h.data();
    let len = match d.op {
        PGAIO_OP_READV | PGAIO_OP_WRITEV => d.op_data.iov_length as i32,
        // pg_unreachable() in C.
        _ => unreachable!("pgaio_io_get_iovec_length: invalid op"),
    };
    (h.iovec_off as usize, len)
}
