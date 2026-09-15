//! D6 connection admission queue (docs/design/connection-scaling.md §D6).
//!
//! pgrust-only. When a regular backend's PGPROC freelist pop comes up empty
//! and `connection_queue_size` > 0, the connection parks HERE — on its own
//! backend thread, holding a socket, a pmchild slot, and nothing else (no
//! PGPROC, no snapshot, no locks) — instead of raising the immediate
//! FATAL 53300 ("sorry, too many clients already") stock PostgreSQL raises.
//!
//! Shape:
//! - Bounded FIFO of waiters (`connection_queue_size`; entrants beyond the
//!   bound get the immediate 53300, which is also the pre-auth DoS bound).
//! - Wake-one: every push of a PGPROC onto the Regular freelist (ProcKill,
//!   KillRetainedProc, the lock-group deferred-return arms) grants a wake
//!   token to the HEAD waiter only — no thundering herd. The token is a
//!   hint, not a reserved proc: the woken waiter retries the freelist pop.
//! - No barging: while waiters exist (`queued_count() > 0`), InitProcess
//!   diverts newcomers straight into the queue tail without popping, so
//!   service order is FIFO. Residual races (a token holder vs. the
//!   post-enqueue stall-guard pop below) make this approximate FIFO, and
//!   every waiter also retries the pop on its 100ms tick, so a freed slot
//!   can never strand while the queue is non-empty.
//! - Each 100ms tick the waiter also: drains pending interrupts
//!   (postmaster SIGTERM/SIGQUIT land as ProcDiePending → the same FATAL
//!   "terminating connection due to administrator command" unwind a live
//!   backend gets, so shutdown wakes the whole queue within one tick), and
//!   polls the client socket for hangup (`pq_check_connection`,
//!   WL_SOCKET_CLOSED) — a queued client that gave up is reaped silently.
//! - `connection_queue_timeout` ms (0 = wait forever): on expiry the waiter
//!   raises the exact 53300 error it would have gotten unqueued.
//!
//! Exemptions are structural: walsenders/autovacuum/bgworkers pop their own
//! freelists and never reach this module. Superusers jump the queue
//! (ruling 2026-09-14) through the reserved band: waiters are admitted only
//! while more than `superuser_reserved_connections + reserved_connections`
//! regular slots are free (InitProcess's pop closure), so the band is never
//! consumed by queued clients; an arrival that finds the pool inside the
//! band bypasses the queue, takes a slot and authenticates, where the stock
//! reserved-slot check (postinit) admits a superuser / reserved-role member
//! and refuses anyone else with C's 53300. Identity cannot be known
//! pre-auth (no catalog access without a PGPROC), which is why the rule is
//! on counts rather than on roles.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use types_core::ProcNumber;
use types_error::{PgError, PgResult, ERRCODE_TOO_MANY_CONNECTIONS, FATAL, LOG};

const TICK: Duration = Duration::from_millis(100);

struct Waiter {
    granted: Mutex<bool>,
    cv: Condvar,
}

struct Queue {
    waiters: VecDeque<Arc<Waiter>>,
}

static QUEUE: Mutex<Queue> = Mutex::new(Queue { waiters: VecDeque::new() });

// Stats (readable via `pgrust: admission stats` in tcop simple-query).
static TOTAL_QUEUED: AtomicU64 = AtomicU64::new(0);
static TOTAL_SERVED: AtomicU64 = AtomicU64::new(0);
static TOTAL_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_HANGUPS: AtomicU64 = AtomicU64::new(0);

fn lock_queue() -> std::sync::MutexGuard<'static, Queue> {
    QUEUE.lock().unwrap_or_else(|e| e.into_inner())
}

/// The InitProcess fast-path barge guard reads this: while waiters exist,
/// newcomers must join the tail instead of popping past them.
pub fn queued_count() -> usize {
    lock_queue().waiters.len()
}

pub fn enabled() -> bool {
    guc_tables::backing::connection_queue_size() > 0
}

/// (queued_now, total_queued, total_served, total_timeouts, total_hangups)
pub fn stats() -> (usize, u64, u64, u64, u64) {
    (
        queued_count(),
        TOTAL_QUEUED.load(Relaxed),
        TOTAL_SERVED.load(Relaxed),
        TOTAL_TIMEOUTS.load(Relaxed),
        TOTAL_HANGUPS.load(Relaxed),
    )
}

/// A PGPROC just returned to the Regular freelist: wake the head waiter
/// (one token per freed slot — wake-one, no herd). Called from every
/// Regular-freelist push site in lib.rs, after ProcStructLock is released.
/// Cheap when idle: one uncontended mutex lock on backend exit.
pub(crate) fn slot_released() {
    let waiter = {
        let mut q = lock_queue();
        q.waiters.pop_front()
    };
    if let Some(w) = waiter {
        let mut g = w.granted.lock().unwrap_or_else(|e| e.into_inner());
        *g = true;
        w.cv.notify_one();
    }
}

fn too_many_error() -> Box<PgError> {
    // The exact error the unqueued path raises (InitProcess empty-pop arm).
    Box::new(
        PgError::new(FATAL, "sorry, too many clients already")
            .with_sqlstate(ERRCODE_TOO_MANY_CONNECTIONS),
    )
}

/// Queue-leave bookkeeping that must run on EVERY exit path, including the
/// `?` unwinds out of check_for_interrupts (postmaster shutdown) and
/// pq_check_connection: dequeue self, and pass an unconsumed wake token to
/// the next waiter so a freed slot's grant never dies with us.
struct QueueGuard {
    me: Arc<Waiter>,
    consumed_token: bool,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        let was_queued = {
            let mut q = lock_queue();
            match q.waiters.iter().position(|w| Arc::ptr_eq(w, &self.me)) {
                Some(pos) => {
                    q.waiters.remove(pos);
                    true
                }
                None => false,
            }
        };
        if !was_queued && !self.consumed_token {
            // We were popped by slot_released but are leaving without using
            // the token (timeout/hangup/shutdown): grant the next waiter.
            let granted = *self.me.granted.lock().unwrap_or_else(|e| e.into_inner());
            if granted {
                slot_released();
            }
        }
    }
}

/// Park until a Regular PGPROC can be popped, the queue times out, the
/// client hangs up, or an interrupt (shutdown) unwinds us. `try_pop` is the
/// caller's freelist pop (InitProcess's own, under ProcStructLock).
///
/// Returns Ok(procno) when admitted. Never returns on client hangup: the
/// waiter is reaped via proc_exit(0) (silent — the client is gone).
pub(crate) fn queue_for_slot(
    mut try_pop: impl FnMut() -> Option<ProcNumber>,
) -> PgResult<ProcNumber> {
    let queue_size = guc_tables::backing::connection_queue_size().max(0) as usize;
    let timeout_ms = guc_tables::backing::connection_queue_timeout();

    let me = Arc::new(Waiter { granted: Mutex::new(false), cv: Condvar::new() });
    let depth = {
        let mut q = lock_queue();
        if q.waiters.len() >= queue_size {
            // Bounded: beyond the queue, today's immediate behavior.
            return Err(too_many_error());
        }
        q.waiters.push_back(Arc::clone(&me));
        q.waiters.len()
    };
    TOTAL_QUEUED.fetch_add(1, Relaxed);
    let entered = Instant::now();
    let deadline =
        (timeout_ms > 0).then(|| entered + Duration::from_millis(timeout_ms as u64));
    let _ = elog::elog(
        LOG,
        format!(
            "connection admission queue: entered (depth {depth} of {queue_size}, timeout {timeout_ms} ms)"
        ),
    );

    let mut guard = QueueGuard { me: Arc::clone(&me), consumed_token: false };

    loop {
        // Shutdown / termination FIRST — during a shutdown cascade freed
        // slots rain into the freelist, and a dying waiter must not win one
        // and march into InitPostgres. SIGTERM and SIGQUIT broadcasts land
        // as pended thread signals + InterruptPending on this thread (the
        // pre-identity fallback covers the no-ProcSignal-slot window);
        // ProcessInterrupts converts them into the FATAL unwind. The
        // QueueGuard drop dequeues us on that path.
        if postgres_seams::check_for_interrupts::is_installed() {
            postgres_seams::check_for_interrupts::call()?;
        }

        // Pop retry, every iteration: the first pass is the stall guard (a
        // slot freed — and pushed to an empty queue — between our failed
        // pop and our enqueue would otherwise never wake us), later passes
        // serve both the wake token and the self-healing tick.
        if let Some(procno) = try_pop() {
            guard.consumed_token = true;
            drop(guard); // dequeues self
            TOTAL_SERVED.fetch_add(1, Relaxed);
            let _ = elog::elog(
                LOG,
                format!(
                    "connection admission queue: admitted after {} ms",
                    entered.elapsed().as_millis()
                ),
            );
            return Ok(procno);
        }

        // Client hangup while parked: reap silently (WL_SOCKET_CLOSED poll;
        // the client is gone, nobody to report to).
        if pqcomm_seams::pq_check_connection::is_installed()
            && !pqcomm_seams::pq_check_connection::call()?
        {
            TOTAL_HANGUPS.fetch_add(1, Relaxed);
            drop(guard);
            let _ = elog::elog(
                LOG,
                format!(
                    "connection admission queue: client disconnected while queued ({} ms)",
                    entered.elapsed().as_millis()
                ),
            );
            elog::config::set_where_to_send_output(types_dest::CommandDest::None);
            ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
        }

        let now = Instant::now();
        if let Some(dl) = deadline {
            if now >= dl {
                TOTAL_TIMEOUTS.fetch_add(1, Relaxed);
                drop(guard);
                let _ = elog::elog(
                    LOG,
                    format!(
                        "connection admission queue: timeout after {timeout_ms} ms"
                    ),
                );
                return Err(too_many_error());
            }
        }

        // Park: wake on a token (a slot freed with us at the head) or the
        // tick (interrupt/hangup poll + the self-healing pop retry).
        let wait = deadline
            .map(|dl| dl.saturating_duration_since(now).min(TICK))
            .unwrap_or(TICK);
        let woken_by_token = {
            let g = me.granted.lock().unwrap_or_else(|e| e.into_inner());
            let mut g = if *g {
                g
            } else {
                let (g, _timed_out) = me
                    .cv
                    .wait_timeout(g, wait)
                    .unwrap_or_else(|e| e.into_inner());
                g
            };
            let woken = *g;
            *g = false; // consume; the loop retries the pop either way
            woken
        };
        if woken_by_token {
            // slot_released popped us off the queue with the token. If the
            // pop retry above us loses a race, we must be findable again —
            // and at the FRONT, keeping our FIFO position. A successful pop
            // just removes us via the guard as usual.
            let mut q = lock_queue();
            if !q.waiters.iter().any(|w| Arc::ptr_eq(w, &me)) {
                q.waiters.push_front(Arc::clone(&me));
            }
        }
    }
}
