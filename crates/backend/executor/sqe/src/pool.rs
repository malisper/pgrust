//! Persistent worker pool (phase-2A charter item 1d): the phase-1 finding
//! was that `std::thread::scope` spawn cost (~0.5-1ms) dominates sub-10ms
//! kernels. This pool spawns ONCE per process and replays the morsel-claim
//! law of `scan::par_range` (one shared atomic fetch_add cursor, dynamic
//! claiming, thread-local state) over long-lived workers.
//!
//! Safety: `run` erases the job's lifetime to hand it to the workers, and
//! does not return until every worker has finished the generation — the
//! borrowed init/work closures and state slots strictly outlive every use.
//! Zero external deps (Mutex + Condvar).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

type Job = Arc<dyn Fn(usize) + Send + Sync + 'static>;

// ---------------------------------------------------------------------------
// Worker capability arming (P2-1 seams: pool fd/resowner arming). The
// engine crate stays backend-dependency-free: the SHELL installs a hook
// (fd::InitFileAccess + InitTemporaryFileAccess + a standing
// "sqe-pool-helper" resource owner) at first server engagement; each
// worker runs it once before its first job after installation. A failed
// or absent arming leaves the worker unarmed and the spill substrate's
// FAIL-CLOSED admission probe (SpillSet::create -> typed ERROR) carries
// the refusal at use — never a stumble mid-I/O.
// ---------------------------------------------------------------------------

static ARM_HOOK: std::sync::atomic::AtomicPtr<()> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

/// Install the process-wide worker arming hook (idempotent; the shell's
/// engagement path calls this where process seams are settled facts).
pub fn set_worker_arm_hook(h: fn()) {
    ARM_HOOK.store(h as *mut (), Ordering::Release);
}

fn worker_arm_hook() -> Option<fn()> {
    let p = ARM_HOOK.load(Ordering::Acquire);
    if p.is_null() {
        None
    } else {
        // SAFETY: only ever stored from a fn() by set_worker_arm_hook.
        Some(unsafe { std::mem::transmute::<*mut (), fn()>(p) })
    }
}

thread_local! {
    static CUR_POOL: std::cell::Cell<*const Pool> = const { std::cell::Cell::new(std::ptr::null()) };
    static IN_WORKER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Scope guard publishing the resident pool to this thread's fan-outs
/// (engine::par_parts); restores the previous publication on drop.
pub struct PoolScope(*const Pool);

impl PoolScope {
    pub fn enter(p: &Pool) -> PoolScope {
        PoolScope(CUR_POOL.with(|c| c.replace(p as *const Pool)))
    }
}

impl Drop for PoolScope {
    fn drop(&mut self) {
        CUR_POOL.with(|c| c.set(self.0));
    }
}

/// The published pool, DRIVER threads only: pool workers get None (a
/// worker-side fan-out must fall back to scoped spawns, never re-enter
/// the pool it runs on).
///
/// Closure-scoped accessor: the `&Pool` handed to `f` carries a fresh,
/// higher-ranked lifetime (`for<'a> FnOnce(Option<&'a Pool>) -> R`) that
/// the return type `R` cannot name, so the borrow provably cannot escape
/// the call. This makes the guard protocol a type-system fact: safe code
/// can neither store the reference past the `PoolScope` guard nor consume
/// a dangling publication left by a forgotten guard — there is no owned
/// `&Pool` to hold. The previous `scoped<'a>() -> Option<&'a Pool>` let
/// the caller pick `'a` (even `'static`) for a reference conjured from a
/// raw TLS pointer, which safe code could hold past the pool's drop.
pub fn with_scoped<R>(f: impl FnOnce(Option<&Pool>) -> R) -> R {
    if IN_WORKER.with(|w| w.get()) {
        return f(None);
    }
    // SAFETY: set from a live borrow by PoolScope::enter; the guard
    // clears it before that borrow ends, and TLS never crosses threads.
    // The reference is confined to `f`'s invocation (it cannot outlive
    // this call), so it can never be observed after the guard restores.
    let pool = unsafe { CUR_POOL.with(|c| c.get()).as_ref() };
    f(pool)
}

struct Shared {
    m: Mutex<State>,
    work_cv: Condvar,
    done_cv: Condvar,
}

struct State {
    seq: u64,
    job: Option<Job>,
    done: usize,
    // Engagement tickets for the current generation: only the first
    // `tickets` workers to observe the new seq run the job (the
    // claim-depth guard — see `engaged_width`); the rest re-park
    // without touching it.
    tickets: usize,
    shutdown: bool,
    // First panic payload of the generation (workers are catch_unwind'd
    // so `done` always completes; the caller re-raises).
    panicked: Option<Box<dyn std::any::Any + Send>>,
}

/// [RULED 2026-08-18, width-ladder-cell.md §3.3/§5 cond. 1] The
/// claim-depth guard: the ladder found that engaging the full pool over
/// a shallow claim plane (`n < 4·threads`) degenerates warm engagement
/// to 10ms-class stalls (w=64 over 7–184 parts: 39–47ms vs the 0.4ms
/// no-op wake). Engagement width is an EXECUTION decision — legal under
/// the width-independence law (answers never depend on it) — so `run`
/// caps the engaged workers so every engaged worker has at least ~4
/// units to claim: full width iff `n >= 4·threads`, else
/// `max(1, n/4)`. Worst-case makespan cost of the cap is 4 units of
/// work on one worker — sub-ms at claim grains — against the measured
/// 10ms-class stall it removes.
#[inline]
pub fn engaged_width(threads: usize, n: usize) -> usize {
    let threads = threads.max(1);
    if n >= 4 * threads {
        threads
    } else {
        (n / 4).clamp(1, threads)
    }
}

/// [sqe-topn-gap] Deepen a shallow PART-grain claim plane: contiguous
/// unit ranges `(pi, u0, u1)` covering `part_units` (indexed by part,
/// `(first_unit, one_past_last_unit)`), each range inside one part, split
/// so the claim count reaches the well-conditioned regime the claim-depth
/// guard admits at full width (`>= 4·width` claims). The p72 ledger's
/// worst columnar cells (top-n 13.7x, cmp-fold 6.7x, filtered-fold 5.5x
/// vs lanev2) were all this one mechanism: a 10-part 10M-row bank claimed
/// at part grain engages `10/4 = 2` of the pool's workers — the
/// width-ladder guard is right (shallow planes stall), so the fix is a
/// DEEPER plane, not a wider cap. A function of bank geometry and pool
/// width only (election-inputs law), never of the query. Banks already
/// carrying `>= 4·width` parts get one chunk per part — the 96-wide rig
/// regime's behavior of record is unchanged. Chunks never split a granule
/// and never cross a part (cursor locality).
pub fn part_claim_chunks(
    part_units: &[(usize, usize)],
    width: usize,
) -> Vec<(usize, usize, usize)> {
    let nparts = part_units.iter().filter(|&&(a, b)| b > a).count().max(1);
    let pieces = (4 * width.max(1)).div_ceil(nparts).max(1);
    let mut out = Vec::new();
    for (pi, &(u0, u1)) in part_units.iter().enumerate() {
        let n = u1.saturating_sub(u0);
        if n == 0 {
            continue;
        }
        // Split at BLOCK grain so chunk edges land on band boundaries
        // (walk units are part-local granules in order; granule 0 opens
        // band 0), honoring the whole-band claim-alignment law
        // (production-plan §7: misaligned claims were a measured CPU
        // regression on the morsel path).
        let nb = n.div_ceil(CLAIM_ALIGN_UNITS);
        let p = pieces.min(nb);
        for c in 0..p {
            let lo = u0 + (nb * c / p) * CLAIM_ALIGN_UNITS;
            let hi = (u0 + (nb * (c + 1) / p) * CLAIM_ALIGN_UNITS).min(u1);
            if hi > lo {
                out.push((pi, lo, hi));
            }
        }
    }
    out
}

/// Claim-chunk alignment block, in walk units (= part-local granules):
/// the format's band is exactly 8 granules (pgrc2_format::geom::
/// GRANULES_PER_BAND, const-asserted there), and whole-band claim
/// alignment is a format guarantee (production-plan §7).
pub const CLAIM_ALIGN_UNITS: usize = 8;

pub struct Pool {
    shared: Arc<Shared>,
    threads: usize,
    handles: Vec<std::thread::JoinHandle<()>>,
    // Generation lock (finding-117): the single `State` slot
    // (seq/job/done/tickets/panicked) has room for exactly ONE live
    // generation, and the lifetime erasure in `run*`/`run_feed` is only
    // sound while each driver outlives its own workers. Concurrent
    // drivers sharing the slot would clobber each other's generation
    // (a second `run` resetting `done`/`job`/`seq` lets the first driver
    // return before its workers quiesce -> its stack-erased job outlives
    // into the next generation -> stack UAF). This mutex serializes a
    // FULL generation (publish -> workers execute -> all engaged workers
    // quiesced -> generation retired) so a driver's erased job is
    // provably dead before the next generation starts and before that
    // driver returns. Held for the whole synchronous `run*`; carried by
    // the `FeedGen` guard for the async leader-fed `run_feed`.
    run_lock: Mutex<()>,
}

/// A live leader-fed generation (see [`Pool::run_feed`]). Dropping joins;
/// a worker panic re-raises at `join` (drop swallows it only while the
/// current thread is itself panicking — never a double panic).
pub struct FeedGen<'p> {
    pool: &'p Pool,
    engaged: usize,
    joined: bool,
    // The pool's generation lock (finding-117), held for the whole live
    // generation: acquired in `run_feed` before publishing and released
    // in `join_inner` only after the generation is retired (and before
    // any re-raise, so a worker panic never poisons the run lock). While
    // this guard is alive no other driver can start a generation on the
    // pool, so this driver's stack-erased job cannot alias another's.
    _gen: Option<std::sync::MutexGuard<'p, ()>>,
}

impl FeedGen<'_> {
    pub fn join(mut self) {
        self.join_inner(true);
    }
    fn join_inner(&mut self, reraise: bool) {
        if self.joined {
            return;
        }
        self.joined = true;
        let sh = &self.pool.shared;
        let mut st = sh.m.lock().unwrap();
        while st.done < self.engaged {
            st = sh.done_cv.wait(st).unwrap();
        }
        st.job = None;
        let p = st.panicked.take();
        drop(st);
        // Generation retired: every engaged worker has reported done and
        // the erased job Arc is dropped, so this driver's stack-borrowed
        // job is provably dead. Release the run lock now — before any
        // re-raise — so the next driver may start its generation and a
        // re-raised worker panic never poisons the run lock.
        self._gen = None;
        if let Some(p) = p {
            if reraise {
                std::panic::resume_unwind(p);
            }
        }
    }
}

impl Drop for FeedGen<'_> {
    fn drop(&mut self) {
        self.join_inner(!std::thread::panicking());
    }
}

impl Pool {
    pub fn new(threads: usize) -> Pool {
        let shared = Arc::new(Shared {
            m: Mutex::new(State {
                seq: 0,
                job: None,
                done: 0,
                tickets: 0,
                shutdown: false,
                panicked: None,
            }),
            work_cv: Condvar::new(),
            done_cv: Condvar::new(),
        });
        let handles = (0..threads)
            .map(|t| {
                let sh = Arc::clone(&shared);
                std::thread::spawn(move || {
                    IN_WORKER.with(|w| w.set(true));
                    let mut last_seen = 0u64;
                    // Attempt-once-per-thread capability arming (plain
                    // local, not TLS: the loop frame IS the thread).
                    let mut armed = false;
                    loop {
                        let job = {
                            let mut st = sh.m.lock().unwrap();
                            loop {
                                if st.shutdown {
                                    return;
                                }
                                if st.seq > last_seen {
                                    last_seen = st.seq;
                                    // Engagement ticket: run only while
                                    // the generation has tickets left;
                                    // otherwise re-park untouched (the
                                    // claim-depth guard).
                                    if st.tickets > 0 {
                                        st.tickets -= 1;
                                        break Arc::clone(st.job.as_ref().unwrap());
                                    }
                                    continue;
                                }
                                st = sh.work_cv.wait(st).unwrap();
                            }
                        };
                        if !armed {
                            if let Some(h) = worker_arm_hook() {
                                h();
                                armed = true;
                            }
                        }
                        // unwind-ok: worker-containment
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            job(t)
                        }));
                        let mut st = sh.m.lock().unwrap();
                        if let Err(p) = r {
                            if st.panicked.is_none() {
                                st.panicked = Some(p);
                            }
                        }
                        st.done += 1;
                        sh.done_cv.notify_all();
                        drop(st);
                    }
                })
            })
            .collect();
        Pool {
            shared,
            threads,
            handles,
            run_lock: Mutex::new(()),
        }
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Leader-FED generation (the heap pack pipeline): publish `job` to
    /// `width` workers and RETURN, so the CALLER thread produces the work
    /// the job consumes (a queue the job drains) while the pool runs it.
    /// The guard joins the generation (on `join` or drop) and re-raises
    /// the first worker panic. Same lifetime-erasure contract as `run`:
    /// the join must precede the death of anything `job` borrows — bind
    /// the guard AFTER the borrowed state, and ensure the queue closes on
    /// every exit path (an unclosed queue deadlocks the join).
    pub fn run_feed<'a, F>(&'a self, width: usize, job: F) -> FeedGen<'a>
    where
        F: Fn(usize) + Send + Sync + 'a,
    {
        let width = width.min(self.threads).max(1);
        // Generation lock (finding-117): held from before publish until the
        // FeedGen retires this generation, so no other driver can observe
        // or clobber this generation's shared state slot. Acquired BEFORE
        // the state mutex — the single lock order across all run paths.
        let gen = self.run_lock.lock().unwrap();
        let job: Arc<dyn Fn(usize) + Send + Sync + 'a> = Arc::new(job);
        // SAFETY: lifetime erasure as in `run` — the guard's join blocks
        // until every engaged worker reports done, and callers bind the
        // guard inside the borrowed state's scope (contract above).
        let job: Job = unsafe { std::mem::transmute(job) };
        let mut st = self.shared.m.lock().unwrap();
        st.job = Some(job);
        st.done = 0;
        st.tickets = width;
        st.panicked = None;
        st.seq += 1;
        if width == self.threads {
            self.shared.work_cv.notify_all();
        } else {
            for _ in 0..width {
                self.shared.work_cv.notify_one();
            }
        }
        drop(st);
        FeedGen { pool: self, engaged: width, joined: false, _gen: Some(gen) }
    }

    /// Parallel teardown (the hot-shape lesson: the wall can hide in untimed
    /// teardown): drop `items` across the pool workers instead of serially
    /// on the caller. Each worker claims items dynamically and drops them.
    pub fn drop_par<T: Send>(&self, items: Vec<T>) {
        if items.is_empty() {
            return;
        }
        let slots: Vec<Mutex<Option<T>>> = items.into_iter().map(|x| Mutex::new(Some(x))).collect();
        self.run(
            slots.len(),
            |_| (),
            |_, i| {
                drop(slots[i].lock().unwrap().take());
            },
        );
    }

    /// par_range with pool-resident threads: claim indices 0..n dynamically,
    /// return every ENGAGED worker's final state in worker order. The
    /// engaged count is `engaged_width(threads, n)` — the claim-depth
    /// guard [RULED 2026-08-18]: only workers that can claim usefully
    /// are woken, so a shallow claim plane never pays the full-pool
    /// wake + per-worker init bill (the width-ladder §3.3 cliff).
    pub fn run<S, FI, FW>(&self, n: usize, init: FI, work: FW) -> Vec<S>
    where
        S: Send,
        FI: Fn(usize) -> S + Sync,
        FW: Fn(&mut S, usize) + Sync,
    {
        self.run_finish_capped(n, self.threads, init, work, |s| s)
    }

    /// `run` at a narrower participation width: at most `width` workers
    /// take engagement tickets for the generation (the F5-capped
    /// fan-outs keep their elected width on the resident pool instead
    /// of spawning); the claim-depth guard then bounds engagement to
    /// workers that can claim usefully within that cap.
    pub fn run_capped<S, FI, FW>(&self, n: usize, width: usize, init: FI, work: FW) -> Vec<S>
    where
        S: Send,
        FI: Fn(usize) -> S + Sync,
        FW: Fn(&mut S, usize) + Sync,
    {
        self.run_finish_capped(n, width, init, work, |s| s)
    }

    /// `run` with a worker-side `finish` map: each engaged worker calls
    /// `finish(state)` ON ITS OWN THREAD after its claim loop, so
    /// thread-resident resources (the scratch depot) park where they
    /// live; only the finished value crosses back to the caller. A
    /// panicking generation skips finish — its state drops on the worker
    /// during unwind (freed, never parked).
    pub fn run_finish<S, T, FI, FW, FF>(&self, n: usize, init: FI, work: FW, finish: FF) -> Vec<T>
    where
        T: Send,
        FI: Fn(usize) -> S + Sync,
        FW: Fn(&mut S, usize) + Sync,
        FF: Fn(S) -> T + Sync,
    {
        self.run_finish_capped(n, self.threads, init, work, finish)
    }

    /// The general primitive: capped participation width AND a
    /// worker-side finish map. `run`, `run_capped`, and `run_finish`
    /// are all delegations onto this body.
    pub fn run_finish_capped<S, T, FI, FW, FF>(
        &self,
        n: usize,
        width: usize,
        init: FI,
        work: FW,
        finish: FF,
    ) -> Vec<T>
    where
        T: Send,
        FI: Fn(usize) -> S + Sync,
        FW: Fn(&mut S, usize) + Sync,
        FF: Fn(S) -> T + Sync,
    {
        let width = width.min(self.threads).max(1);
        // Generation lock (finding-117): held for this whole synchronous
        // generation (publish -> workers execute -> all engaged workers
        // quiesced -> generation retired) so a concurrent driver can
        // neither observe nor clobber this generation's shared state slot,
        // and this driver's stack-erased job is provably dead before the
        // lock is released. Acquired BEFORE the state mutex — the single
        // lock order across all run paths. Explicitly dropped after the
        // generation is retired and before any re-raise (below), so a
        // re-raised worker panic never poisons the run lock.
        let gen = self.run_lock.lock().unwrap();
        // Claim-depth guard [RULED 2026-08-18] applied AT the capped
        // participation width: full cap iff n >= 4·width, else
        // max(1, n/4) — the ticket count enforces both bounds (workers
        // without a ticket re-park untouched, so no per-index width
        // check is needed in the body).
        let engaged = engaged_width(width, n);
        let cursor = AtomicUsize::new(0);
        let slots: Vec<Mutex<Option<T>>> = (0..self.threads).map(|_| Mutex::new(None)).collect();
        let cancel = crate::cancel::current();
        let panicked = {
            // The whole per-worker loop as one closure over borrowed state.
            let body = |t: usize| {
                let _inh = crate::cancel::inherit(&cancel);
                let mut s = init(t);
                loop {
                    if crate::cancel::fired_of(&cancel) {
                        break;
                    }
                    let i = cursor.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    work(&mut s, i);
                }
                *slots[t].lock().unwrap() = Some(finish(s));
            };
            // Lifetime erasure: `body` borrows locals; `run` blocks below
            // until all workers report done, so the borrows outlive use.
            let job: Arc<dyn Fn(usize) + Send + Sync + '_> = Arc::new(body);
            let job: Job = unsafe { std::mem::transmute(job) };
            let mut st = self.shared.m.lock().unwrap();
            st.job = Some(job);
            st.done = 0;
            st.tickets = engaged;
            st.panicked = None;
            st.seq += 1;
            // Wake only the workers that can claim: a parked worker
            // beyond the ticket count never leaves the futex at all.
            // (Lost notify_one races are benign: any worker between
            // generations re-checks `seq` under the lock before parking
            // and takes a remaining ticket there.)
            if engaged == self.threads {
                self.shared.work_cv.notify_all();
            } else {
                for _ in 0..engaged {
                    self.shared.work_cv.notify_one();
                }
            }
            while st.done < engaged {
                let (g, _) = self
                    .shared
                    .done_cv
                    .wait_timeout(st, std::time::Duration::from_millis(10))
                    .unwrap();
                st = g;
                if st.done < engaged {
                    drop(st);
                    crate::cancel::poll_now();
                    st = self.shared.m.lock().unwrap();
                }
            }
            st.job = None; // drop the erased Arc before locals go away
            st.panicked.take()
        };
        // Generation retired (job Arc dropped, all engaged workers done):
        // this driver's stack-erased job is provably dead, so releasing the
        // run lock here is safe. Released before the re-raise below so a
        // worker panic never poisons the run lock; `slots`/`states` below
        // are this driver's own locals, untouched by any other generation.
        drop(gen);
        if let Some(p) = panicked {
            let canceled =
                p.is::<crate::cancel::Canceled>() && crate::cancel::fired_of(&cancel);
            if !canceled {
                std::panic::resume_unwind(p);
            }
        }
        crate::cancel::checkpoint();
        let states: Vec<T> = slots
            .into_iter()
            .filter_map(|s| s.into_inner().unwrap())
            .collect();
        assert_eq!(states.len(), engaged, "engaged worker state missing");
        states
    }

}

impl Drop for Pool {
    fn drop(&mut self) {
        {
            let mut st = self.shared.m.lock().unwrap();
            st.shutdown = true;
            self.shared.work_cv.notify_all();
        }
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [RULED 2026-08-18] The claim-depth guard's gate: full width iff
    /// the claim plane is at least 4 units per worker; below that the
    /// engaged width is max(1, n/4) — the width-ladder cell's
    /// well-conditioned boundary (parts >= 4·w), exactly.
    #[test]
    fn claim_depth_guard_arithmetic() {
        // Deep plane: full engagement, boundary included.
        assert_eq!(engaged_width(96, 4 * 96), 96);
        assert_eq!(engaged_width(96, 1_000_000), 96);
        // One unit short of deep: guarded.
        assert_eq!(engaged_width(96, 4 * 96 - 1), 95);
        // The ladder's cliff rungs (w=64 over 7..184 parts) all guard.
        assert_eq!(engaged_width(64, 7), 1);
        assert_eq!(engaged_width(64, 184), 46);
        // Shallow-to-empty planes engage exactly one worker.
        assert_eq!(engaged_width(96, 0), 1);
        assert_eq!(engaged_width(96, 3), 1);
        // Never exceeds the pool, never returns zero.
        assert_eq!(engaged_width(1, 1_000), 1);
        assert_eq!(engaged_width(0, 10), 1);
    }

    /// [sqe-topn-gap] The part-subdivided claim plane: exact coverage
    /// (every unit exactly once, in order, never crossing a part), the
    /// shallow regime deepens past the claim-depth guard's 4·width bar
    /// (or exhausts to unit grain), and the deep regime (parts >= 4·w,
    /// the width-ladder rig posture) is byte-identical to part grain.
    #[test]
    fn part_claim_chunks_law() {
        // The p72 cell's geometry: 10 parts x 122 units, width 14.
        let pu: Vec<(usize, usize)> = (0..10).map(|p| (p * 122, (p + 1) * 122)).collect();
        let ch = part_claim_chunks(&pu, 14);
        assert!(ch.len() >= 4 * 14, "deepened: {}", ch.len());
        assert_eq!(engaged_width(14, ch.len()), 14, "full width engaged");
        // Coverage: concatenated ranges per part reproduce the part exactly,
        // and every interior edge is band-aligned (§7 claim-alignment law).
        let mut cursor = pu.iter().map(|&(a, _)| a).collect::<Vec<_>>();
        for &(pi, lo, hi) in &ch {
            assert_eq!(lo, cursor[pi], "in-order, gap-free");
            assert!(hi <= pu[pi].1, "never crosses the part");
            assert!(hi > lo);
            assert_eq!((lo - pu[pi].0) % CLAIM_ALIGN_UNITS, 0, "band-aligned edge");
            cursor[pi] = hi;
        }
        assert!(cursor.iter().zip(&pu).all(|(&c, &(_, b))| c == b), "full coverage");
        // Sub-band parts stay one claim; empty parts vanish.
        let tiny: Vec<(usize, usize)> = vec![(0, 2), (2, 2), (2, 3)];
        let ch = part_claim_chunks(&tiny, 96);
        assert_eq!(ch, vec![(0, 0, 2), (2, 2, 3)]);
        // Deep regime: one chunk per part — the rig behavior of record.
        let deep: Vec<(usize, usize)> = (0..400).map(|p| (p * 8, (p + 1) * 8)).collect();
        let ch = part_claim_chunks(&deep, 96);
        assert_eq!(ch.len(), 400);
        assert!(ch.iter().enumerate().all(|(pi, &c)| c == (pi, pi * 8, (pi + 1) * 8)));
    }

    /// The closure-scoped pool accessor: the publication is visible only
    /// inside a live `PoolScope` and only through the closure, and the
    /// guard's drop restores the previous (here: absent) publication. The
    /// `&Pool` cannot escape `with_scoped` — a caller trying to return it
    /// (`with_scoped(|p| p.unwrap())`) fails to compile, since `R` cannot
    /// name the reference's higher-ranked lifetime — so a held-past-guard
    /// or forgotten-guard dangling reference is not expressible in safe
    /// code (the finding-280 soundness fix).
    #[test]
    fn with_scoped_borrow_is_closure_bound() {
        // Driver thread outside any scope: no published pool.
        assert!(with_scoped(|p| p.is_none()));
        let pool = Pool::new(2);
        {
            let _g = PoolScope::enter(&pool);
            assert_eq!(with_scoped(|p| p.map(|p| p.threads())), Some(2));
        }
        // Guard dropped: publication restored to absent.
        assert!(with_scoped(|p| p.is_none()));
    }

    /// Width independence at the engagement seam: the same claims fold
    /// to the same answer whatever the engaged width, and exactly
    /// `engaged_width` states come back.
    #[test]
    fn guarded_engagement_answers_and_state_count() {
        let p = Pool::new(8);
        for &n in &[0usize, 1, 3, 7, 31, 32, 100] {
            let states = p.run(n, |_| 0u64, |s, i| *s += i as u64 + 1);
            assert_eq!(states.len(), engaged_width(8, n), "n={n}");
            let total: u64 = states.iter().sum();
            let expect = (n as u64) * (n as u64 + 1) / 2;
            assert_eq!(total, expect, "n={n}");
        }
    }
}
