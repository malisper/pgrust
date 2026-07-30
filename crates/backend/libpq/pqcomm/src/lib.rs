use core::cell::{Cell, UnsafeCell};
use core::mem::ManuallyDrop;

use elog::ereport;
use mcx::{McxOwned, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_CONNECTION_DOES_NOT_EXIST,
    ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL,
};

pub const EOF: i32 = -1;

// Fixed 8KB buffers, as C (fragments large results at non-MTU boundaries);
// the vectored/coalesced-send lever (docs/beat-postgres.md §6) lands at
// internal_flush, not here.
pub const PQ_SEND_BUFFER_SIZE: usize = 8192;
pub const PQ_RECV_BUFFER_SIZE: usize = 8192;

struct PqState {
    send_pointer: Cell<usize>,
    send_start: Cell<usize>,
    recv_pointer: Cell<usize>,
    recv_length: Cell<usize>,
    comm_busy: Cell<bool>,
    comm_reading_msg: Cell<bool>,
    last_reported_send_errno: Cell<i32>,
    recv_buffer: UnsafeCell<[u8; PQ_RECV_BUFFER_SIZE]>,
}

struct SendBuf<'mcx> {
    buf: PgVec<'mcx, u8>,
}

mcx::bind!(SendBufTy => SendBuf<'mcx>);

thread_local! {
    static PQ: PqState = const {
        assert!(!core::mem::needs_drop::<PqState>());
        PqState {
            send_pointer: Cell::new(0),
            send_start: Cell::new(0),
            recv_pointer: Cell::new(0),
            recv_length: Cell::new(0),
            comm_busy: Cell::new(false),
            comm_reading_msg: Cell::new(false),
            last_reported_send_errno: Cell::new(0),
            recv_buffer: UnsafeCell::new([0; PQ_RECV_BUFFER_SIZE]),
        }
    };
    // C's TopMemoryContext PqSendBuffer, created by pq_init_buffers (C's
    // pq_init). ManuallyDrop: a droppy TLS payload costs a per-access state
    // machine. UnsafeCell, not RefCell: the borrow flag re-paid per call what
    // comm_busy guards (+3 insns/call, docs/benchmarks/pqcomm.md).
    static SEND: UnsafeCell<Option<ManuallyDrop<McxOwned<SendBufTy>>>> = const {
        assert!(!core::mem::needs_drop::<Option<ManuallyDrop<McxOwned<SendBufTy>>>>());
        UnsafeCell::new(None)
    };
}

#[cfg(debug_assertions)]
thread_local! {
    static SEND_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn grow_zeroed(buf: &mut PgVec<'_, u8>, new_len: usize) -> PgResult<()> {
    let mcx = *buf.allocator();
    let old = buf.len();
    buf.try_reserve_exact(new_len - old).map_err(|_| mcx.oom(new_len))?;
    // SAFETY: capacity >= new_len after the reserve; the tail is fully
    // initialized before set_len.
    unsafe {
        core::ptr::write_bytes(buf.as_mut_ptr().add(old), 0, new_len - old);
        buf.set_len(new_len);
    }
    Ok(())
}

fn new_send_buf() -> PgResult<McxOwned<SendBufTy>> {
    McxOwned::try_new(MemoryContext::new("PqComm"), |mcx| {
        let mut buf = PgVec::new_in(mcx);
        grow_zeroed(&mut buf, PQ_SEND_BUFFER_SIZE)?;
        Ok(SendBuf { buf })
    })
}

#[cfg(debug_assertions)]
struct SendActiveGuard;

#[cfg(debug_assertions)]
impl Drop for SendActiveGuard {
    fn drop(&mut self) {
        SEND_ACTIVE.with(|b| b.set(false));
    }
}

#[cfg(debug_assertions)]
fn send_active_guard() -> SendActiveGuard {
    SEND_ACTIVE.with(|b| assert!(!b.replace(true), "reentrant SEND access"));
    SendActiveGuard
}

#[cold]
#[inline(never)]
fn send_init_cold(
    slot: &mut Option<ManuallyDrop<McxOwned<SendBufTy>>>,
) -> PgResult<&mut ManuallyDrop<McxOwned<SendBufTy>>> {
    register_send_teardown();
    Ok(slot.insert(ManuallyDrop::new(new_send_buf()?)))
}

// Session-memory teardown (FPBUDGET-1): the PqComm send buffer is freed at
// clean task end (after the exit-callback stack; nothing sends afterwards).
// Idempotent per thread: both init paths call it, only the first registers.
fn register_send_teardown() {
    thread_local! {
        static REGISTERED: Cell<bool> = const { Cell::new(false) };
    }
    if !REGISTERED.replace(true) {
        ::mcx::register_session_cleanup(Box::new(|| {
            SEND.with(|cell| {
                // SAFETY: same single-thread slot ownership as with_send; no
                // send routine is live at task-end teardown.
                if let Some(old) = unsafe { &mut *cell.get() }.take() {
                    drop(ManuallyDrop::into_inner(old));
                }
            });
        }));
    }
}

fn with_send<R>(
    f: impl for<'mcx> FnOnce(&mut SendBuf<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    #[cfg(debug_assertions)]
    let _active = send_active_guard();
    SEND.with(|cell| -> PgResult<R> {
        // SAFETY: one backend = one thread and each thread owns its own SEND
        // TLS slot, so the only aliasing hazard is same-thread reentry — and
        // no path reachable from `f` re-enters SEND: the secure_* seams do
        // not call back into pqcomm, and errors below them are COMMERROR
        // (never client-directed) precisely so they cannot recurse into the
        // send side; comm_busy suppresses reentrant putmessage/flush at the
        // API boundary; SEND_ACTIVE re-checks the claim in debug builds.
        let slot = unsafe { &mut *cell.get() };
        match slot {
            Some(sb) => sb.with_mut(f),
            None => send_init_cold(slot)?.with_mut(f),
        }
    })
}

/// The "initialize state variables" block of C `pq_init`; the socket half
/// (Port setup, FeBeWaitSet, keepalives) lands with the socket/port unit.
pub fn pq_init_buffers() -> PgResult<()> {
    #[cfg(debug_assertions)]
    let _active = send_active_guard();
    register_send_teardown();
    let fresh = ManuallyDrop::new(new_send_buf()?);
    SEND.with(|cell| {
        // SAFETY: same single-thread slot ownership as with_send; no send
        // routine is live here (checked in debug by the guard above).
        let slot = unsafe { &mut *cell.get() };
        if let Some(old) = slot.replace(fresh) {
            drop(ManuallyDrop::into_inner(old));
        }
    });
    PQ.with(|st| {
        st.send_pointer.set(0);
        st.send_start.set(0);
        st.recv_pointer.set(0);
        st.recv_length.set(0);
        st.comm_busy.set(false);
        st.comm_reading_msg.set(false);
    });
    Ok(())
}

fn socket_comm_reset() {
    PQ.with(|st| st.comm_busy.set(false));
}

fn socket_set_nonblocking(nonblocking: bool) -> PgResult<()> {
    if !be_secure_seams::set_port_noblock::call(nonblocking) {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST)
            .errmsg("there is no client connection")
            .finish(loc("socket_set_nonblocking"))?;
    }
    Ok(())
}

fn pq_recvbuf() -> PgResult<i32> {
    PQ.with(|st| {
        let rp = st.recv_pointer.get();
        if rp > 0 {
            let rl = st.recv_length.get();
            if rl > rp {
                // SAFETY: exclusive access confined to this closure.
                let buf = unsafe { &mut *st.recv_buffer.get() };
                buf.copy_within(rp..rl, 0);
                st.recv_length.set(rl - rp);
            } else {
                st.recv_length.set(0);
            }
            st.recv_pointer.set(0);
        }
    });

    socket_set_nonblocking(false)?;

    loop {
        let r = PQ.with(|st| {
            let rl = st.recv_length.get();
            // SAFETY: one backend = one thread; held across the seam call as C
            // shares its static buffer with secure_read — nothing reachable
            // from there touches the recv buffer (reentry would corrupt C too).
            let buf = unsafe { &mut *st.recv_buffer.get() };
            be_secure_seams::secure_read::call(&mut buf[rl..])
        })?;
        match r {
            Err(e) if e == libc::EINTR => continue,
            Err(e) => {
                // COMMERROR: a client-directed report would recurse here;
                // errno 0 is assumed EOF (the caller complains).
                if e != 0 {
                    let _ = ereport(COMMERROR)
                        .with_saved_errno(e)
                        .errcode_for_socket_access()
                        .errmsg("could not receive data from client: %m")
                        .finish(loc("pq_recvbuf"));
                }
                return Ok(EOF);
            }
            Ok(0) => return Ok(EOF),
            Ok(r) => {
                PQ.with(|st| st.recv_length.set(st.recv_length.get() + r));
                return Ok(0);
            }
        }
    }
}

pub fn pq_getbyte() -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    loop {
        let b = PQ.with(|st| {
            let rp = st.recv_pointer.get();
            if rp < st.recv_length.get() {
                // SAFETY: shared read confined to this closure.
                let buf = unsafe { &*st.recv_buffer.get() };
                st.recv_pointer.set(rp + 1);
                Some(buf[rp])
            } else {
                None
            }
        });
        match b {
            Some(b) => return Ok(b as i32),
            None => {
                if pq_recvbuf()? != 0 {
                    return Ok(EOF);
                }
            }
        }
    }
}

pub fn pq_peekbyte() -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    loop {
        let b = PQ.with(|st| {
            let rp = st.recv_pointer.get();
            if rp < st.recv_length.get() {
                // SAFETY: shared read confined to this closure.
                let buf = unsafe { &*st.recv_buffer.get() };
                Some(buf[rp])
            } else {
                None
            }
        });
        match b {
            Some(b) => return Ok(b as i32),
            None => {
                if pq_recvbuf()? != 0 {
                    return Ok(EOF);
                }
            }
        }
    }
}

/// `Ok(1)` byte stored in `*c`, `Ok(0)` no data available, `Ok(EOF)` trouble.
pub fn pq_getbyte_if_available(c: &mut u8) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    let buffered = PQ.with(|st| {
        let rp = st.recv_pointer.get();
        if rp < st.recv_length.get() {
            // SAFETY: shared read confined to this closure.
            let buf = unsafe { &*st.recv_buffer.get() };
            st.recv_pointer.set(rp + 1);
            Some(buf[rp])
        } else {
            None
        }
    });
    if let Some(b) = buffered {
        *c = b;
        return Ok(1);
    }

    socket_set_nonblocking(true)?;

    let mut buf = [0u8; 1];
    Ok(match be_secure_seams::secure_read::call(&mut buf)? {
        Err(e) if e == libc::EAGAIN || e == libc::EWOULDBLOCK || e == libc::EINTR => 0,
        Err(e) => {
            if e != 0 {
                let _ = ereport(COMMERROR)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg("could not receive data from client: %m")
                    .finish(loc("pq_getbyte_if_available"));
            }
            EOF
        }
        Ok(0) => EOF,
        Ok(r) => {
            *c = buf[0];
            r as i32
        }
    })
}

pub fn pq_getbytes(b: &mut [u8]) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    let mut off = 0usize;
    while off < b.len() {
        let copied = PQ.with(|st| {
            let rp = st.recv_pointer.get();
            let rl = st.recv_length.get();
            if rp >= rl {
                return 0;
            }
            let amount = (rl - rp).min(b.len() - off);
            // SAFETY: shared read confined to this closure; `b` is caller
            // memory, disjoint from the TLS buffer.
            let buf = unsafe { &*st.recv_buffer.get() };
            b[off..off + amount].copy_from_slice(&buf[rp..rp + amount]);
            st.recv_pointer.set(rp + amount);
            amount
        });
        if copied == 0 {
            if pq_recvbuf()? != 0 {
                return Ok(EOF);
            }
        } else {
            off += copied;
        }
    }
    Ok(0)
}

fn pq_discardbytes(mut len: usize) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    while len > 0 {
        let taken = PQ.with(|st| {
            let rp = st.recv_pointer.get();
            let rl = st.recv_length.get();
            if rp >= rl {
                return 0;
            }
            let amount = (rl - rp).min(len);
            st.recv_pointer.set(rp + amount);
            amount
        });
        if taken == 0 {
            if pq_recvbuf()? != 0 {
                return Ok(EOF);
            }
        } else {
            len -= taken;
        }
    }
    Ok(0)
}

pub fn pq_buffer_remaining_data() -> isize {
    PQ.with(|st| {
        let rp = st.recv_pointer.get();
        let rl = st.recv_length.get();
        debug_assert!(rl >= rp);
        (rl - rp) as isize
    })
}

pub fn pq_startmsgread() -> PgResult<()> {
    if pq_is_reading_msg() {
        ereport(FATAL)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("terminating connection because protocol synchronization was lost")
            .finish(loc("pq_startmsgread"))?;
    }
    PQ.with(|st| st.comm_reading_msg.set(true));
    Ok(())
}

pub fn pq_endmsgread() {
    debug_assert!(pq_is_reading_msg());
    PQ.with(|st| st.comm_reading_msg.set(false));
}

pub fn pq_is_reading_msg() -> bool {
    PQ.with(|st| st.comm_reading_msg.get())
}

/// Body only (length word removed) into caller-reset `s`; `Ok(EOF)` aborts the
/// connection past `maxlen`. C's PG_TRY around enlargeStringInfo (discard the
/// body, clear the reading flag, re-throw) is the `Err` arm here.
pub fn pq_getmessage(s: &mut StringInfo<'_>, maxlen: i32) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    s.reset();

    let mut lenbuf = [0u8; 4];
    if pq_getbytes(&mut lenbuf)? == EOF {
        let _ = ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("unexpected EOF within message length word")
            .finish(loc("pq_getmessage"));
        return Ok(EOF);
    }

    let len = i32::from_be_bytes(lenbuf);

    if len < 4 || len > maxlen {
        let _ = ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid message length")
            .finish(loc("pq_getmessage"));
        return Ok(EOF);
    }

    let len = (len - 4) as usize;

    if len > 0 {
        if let Err(e) = s.enlarge(len) {
            if pq_discardbytes(len)? == EOF {
                let _ = ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("incomplete message from client")
                    .finish(loc("pq_getmessage"));
            }
            PQ.with(|st| st.comm_reading_msg.set(false));
            return Err(e);
        }

        let mut remaining = len;
        while remaining > 0 {
            let copied = PQ.with(|st| -> PgResult<usize> {
                let rp = st.recv_pointer.get();
                let rl = st.recv_length.get();
                if rp >= rl {
                    return Ok(0);
                }
                let amount = (rl - rp).min(remaining);
                // SAFETY: shared read confined to this closure; `s` is caller
                // memory, disjoint from the TLS buffer.
                let buf = unsafe { &*st.recv_buffer.get() };
                s.append_bytes(&buf[rp..rp + amount])?;
                st.recv_pointer.set(rp + amount);
                Ok(amount)
            })?;
            if copied == 0 {
                if pq_recvbuf()? != 0 {
                    // C leaves s->len == 0 on this path
                    s.reset();
                    let _ = ereport(COMMERROR)
                        .errcode(ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg("incomplete message from client")
                        .finish(loc("pq_getmessage"));
                    return Ok(EOF);
                }
            } else {
                remaining -= copied;
            }
        }
    }

    PQ.with(|st| st.comm_reading_msg.set(false));

    Ok(0)
}

#[inline(always)] // static inline in C
fn internal_putbytes(st: &PqState, sb: &mut SendBuf<'_>, b: &[u8]) -> PgResult<i32> {
    let size = sb.buf.len();
    let mut off = 0usize;
    let mut len = b.len();

    while len > 0 {
        if st.send_pointer.get() >= size {
            socket_set_nonblocking(false)?;
            if internal_flush(st, sb)? != 0 {
                return Ok(EOF);
            }
        }

        let (pointer, start) = (st.send_pointer.get(), st.send_start.get());
        if len >= size && start == pointer {
            let mut fstart = 0usize;
            let mut fend = len;
            socket_set_nonblocking(false)?;
            if internal_flush_buffer(st, &b[off..off + len], &mut fstart, &mut fend)? != 0 {
                return Ok(EOF);
            }
            // full success resets the end cursor to 0 and the loop exits; a
            // would-block partial send is unreachable in blocking mode
            len = fend;
        } else {
            let amount = (size - pointer).min(len);
            // SAFETY: pointer + amount <= size == sb.buf.len() and
            // off + amount <= b.len(); `b` never aliases the send buffer.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    b.as_ptr().add(off),
                    sb.buf.as_mut_ptr().add(pointer),
                    amount,
                );
            }
            st.send_pointer.set(pointer + amount);
            off += amount;
            len -= amount;
        }
    }

    Ok(0)
}

fn socket_flush() -> PgResult<i32> {
    PQ.with(|st| {
        if st.comm_busy.get() {
            return Ok(0);
        }
        st.comm_busy.set(true);
        let res = (|| {
            socket_set_nonblocking(false)?;
            with_send(|sb| internal_flush(st, sb))
        })();
        // Err (the C longjmp) leaves the busy flag set until pq_comm_reset.
        if res.is_ok() {
            st.comm_busy.set(false);
        }
        res
    })
}

#[inline]
fn internal_flush(st: &PqState, sb: &mut SendBuf<'_>) -> PgResult<i32> {
    let mut start = st.send_start.get();
    let mut end = st.send_pointer.get();
    let res = internal_flush_buffer(st, &sb.buf, &mut start, &mut end);
    // Cursors written back also on Err: C advanced its statics in place.
    st.send_start.set(start);
    st.send_pointer.set(end);
    res
}

#[inline(never)] // pg_noinline in C
fn internal_flush_buffer(
    st: &PqState,
    buf: &[u8],
    start: &mut usize,
    end: &mut usize,
) -> PgResult<i32> {
    while *start < *end {
        match be_secure_seams::secure_write::call(&buf[*start..*end])? {
            Ok(r) if r > 0 => {
                st.last_reported_send_errno.set(0);
                *start += r;
            }
            other => {
                // C's r <= 0 arm reads errno; a zero-byte send takes it with
                // errno 0.
                let e = match other {
                    Err(e) => e,
                    Ok(_) => 0,
                };
                if e == libc::EINTR {
                    continue;
                }
                if e == libc::EAGAIN || e == libc::EWOULDBLOCK {
                    return Ok(0);
                }

                // COMMERROR dedup: a lost client can bring us here many
                // times before a safe abort point.
                if e != st.last_reported_send_errno.get() {
                    st.last_reported_send_errno.set(e);
                    let _ = ereport(COMMERROR)
                        .with_saved_errno(e)
                        .errcode_for_socket_access()
                        .errmsg("could not send data to client: %m")
                        .finish(loc("internal_flush_buffer"));
                }

                // Drop the buffered data so processing can continue; the next
                // CHECK_FOR_INTERRUPTS terminates the connection.
                *start = 0;
                *end = 0;
                init_small::globals::SetClientConnectionLost(true);
                init_small::globals::SetInterruptPending(true);
                return Ok(EOF);
            }
        }
    }

    *start = 0;
    *end = 0;
    Ok(0)
}

fn socket_flush_if_writable() -> PgResult<i32> {
    PQ.with(|st| {
        if st.send_pointer.get() == st.send_start.get() {
            return Ok(0);
        }

        if st.comm_busy.get() {
            return Ok(0);
        }

        socket_set_nonblocking(true)?;

        st.comm_busy.set(true);
        let res = with_send(|sb| internal_flush(st, sb));
        if res.is_ok() {
            st.comm_busy.set(false);
        }
        res
    })
}

fn socket_is_send_pending() -> bool {
    PQ.with(|st| st.send_start.get() < st.send_pointer.get())
}

/// Suppressed while busy (quickdie during a pqcomm routine); a length word of
/// `len + 4` follows the type byte.
fn socket_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    debug_assert!(msgtype != 0);

    PQ.with(|st| {
        if st.comm_busy.get() {
            return Ok(0);
        }
        st.comm_busy.set(true);
        let res = with_send(|sb| {
            if internal_putbytes(st, sb, &[msgtype])? != 0 {
                return Ok(EOF);
            }
            let n32 = ((s.len() + 4) as u32).to_be_bytes();
            if internal_putbytes(st, sb, &n32)? != 0 {
                return Ok(EOF);
            }
            if internal_putbytes(st, sb, s)? != 0 {
                return Ok(EOF);
            }
            Ok(0)
        });
        if res.is_ok() {
            st.comm_busy.set(false);
        }
        res
    })
}

fn socket_putmessage_noblock(msgtype: u8, s: &[u8]) -> PgResult<()> {
    let required = PQ.with(|st| st.send_pointer.get()) + 1 + 4 + s.len();
    with_send(|sb| {
        if required > sb.buf.len() {
            grow_zeroed(&mut sb.buf, required)?;
        }
        Ok(())
    })?;
    let res = pq_putmessage(msgtype, s)?;
    debug_assert_eq!(res, 0, "should not fail when the message fits in buffer");
    let _ = res;
    Ok(())
}

/// Protocol-2 framing (type byte, no length word), kept only so the
/// "unsupported protocol version" courtesy error can reach a v2 client.
pub fn pq_putmessage_v2(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    debug_assert!(msgtype != 0);

    PQ.with(|st| {
        if st.comm_busy.get() {
            return Ok(0);
        }
        st.comm_busy.set(true);
        let res = with_send(|sb| {
            if internal_putbytes(st, sb, &[msgtype])? != 0 {
                return Ok(EOF);
            }
            if internal_putbytes(st, sb, s)? != 0 {
                return Ok(EOF);
            }
            Ok(0)
        });
        if res.is_ok() {
            st.comm_busy.set(false);
        }
        res
    })
}

/// `PQcommMethods` (libpq/libpq.h); pqmq swaps in shm_mq-backed methods for
/// background workers.
pub struct PQcommMethods {
    pub comm_reset: fn(),
    pub flush: fn() -> PgResult<i32>,
    pub flush_if_writable: fn() -> PgResult<i32>,
    pub is_send_pending: fn() -> bool,
    pub putmessage: fn(u8, &[u8]) -> PgResult<i32>,
    pub putmessage_noblock: fn(u8, &[u8]) -> PgResult<()>,
}

pub static PQ_COMM_SOCKET_METHODS: PQcommMethods = PQcommMethods {
    comm_reset: socket_comm_reset,
    flush: socket_flush,
    flush_if_writable: socket_flush_if_writable,
    is_send_pending: socket_is_send_pending,
    putmessage: socket_putmessage,
    putmessage_noblock: socket_putmessage_noblock,
};

thread_local! {
    static PQ_COMM_METHODS: Cell<&'static PQcommMethods> =
        const { Cell::new(&PQ_COMM_SOCKET_METHODS) };
}

pub fn set_pq_comm_methods(methods: &'static PQcommMethods) {
    PQ_COMM_METHODS.with(|c| c.set(methods));
}

pub fn pq_comm_reset() {
    (PQ_COMM_METHODS.with(Cell::get).comm_reset)()
}

pub fn pq_flush() -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).flush)()
}

pub fn pq_flush_if_writable() -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).flush_if_writable)()
}

pub fn pq_is_send_pending() -> bool {
    (PQ_COMM_METHODS.with(Cell::get).is_send_pending)()
}

pub fn pq_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).putmessage)(msgtype, s)
}

pub fn pq_putmessage_noblock(msgtype: u8, s: &[u8]) -> PgResult<()> {
    (PQ_COMM_METHODS.with(Cell::get).putmessage_noblock)(msgtype, s)
}

/// Transport-BLIND message-layer installs only: pq_putmessage/pq_flush
/// delegate through `PQ_COMM_METHODS` whatever byte provider sits under
/// them. The listen/accept half moved to [`socket::init_socket_seams`]
/// (P4 sim-net pin, wasm-net-seam worklog §8): those slots are set-once
/// and PROVIDER-owned, so installing the socket arms unconditionally here
/// locked every other transport (sim-net) out of them. Mirrors the
/// init_socket_seams → init_socket_gucs split.
pub fn init_seams() {
    pqcomm_seams::pq_putmessage::set(pq_putmessage);
    pqcomm_seams::pq_putmessage_v2::set(pq_putmessage_v2);
    pqcomm_seams::pq_flush::set(pq_flush);
}

pub mod socket;
pub use socket::*;

#[cfg(test)]
mod tests;
