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
//! freelists and never reach this module.
//!
//! Admission bypass (ruling 2026-09-14, Michael): (1) at the ordinary
//! ceiling a user goes to the BACK of the queue until a slot is available;
//! (2) a superuser skips the queue and gets a slot at once. Identity cannot
//! be known pre-auth (no catalog access without a PGPROC), so the split is
//! on a pre-auth CLAIM read off the startup packet, verified after login by
//! the stock reserved-slot check (postinit). Let `band` =
//! `superuser_reserved_connections + reserved_connections`:
//! - Non-claimers pop a Regular PGPROC only while MORE than `band` slots
//!   are free (arrivals and waiters alike); otherwise they queue, FIFO. So
//!   an ordinary user is never refused at its real ceiling
//!   (`max_connections - band`) — it waits — and never consumes the band.
//! - Claimers never divert into the queue while waiters exist and pop ANY
//!   free slot, band included; postinit then keeps a superuser /
//!   pg_use_reserved_connections member and refuses anyone else with C's
//!   53300 ("remaining connection slots are reserved ..."). A false claim
//!   therefore buys nothing but a faster refusal: the claim decides who
//!   waits, the reserved check decides who gets in. A claimer that finds NO
//!   free slot queues like everyone else (the band is full of superusers).
//! - Claim sources, any one suffices, both read BEFORE the pop: the
//!   startup-packet GUC `pgrust.admission_bypass=on` (as a startup
//!   parameter or inside `options='-c pgrust.admission_bypass=on'`), or a
//!   startup-packet `application_name` that case-insensitively PREFIX-
//!   matches an entry of `pgrust.admission_bypass_applications` (default:
//!   interactive/admin tools — psql, pgcli, pgAdmin 4, HeidiSQL, ...; never
//!   drivers/ORMs/poolers, whose connections must queue).
//! - Trade-off: a superuser that sends neither (e.g. a driver connection
//!   with a custom application_name) is ordinary at the ceiling and waits.
//! - `connection_queue_size = 0` (queue off) and `band = 0` keep today's
//!   behavior exactly, claim or not.

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

/// The pre-auth admission-bypass claim for THIS backend, read off the
/// startup packet (Port) — see the module doc. False without a Port
/// (single-user, stdio/sim harnesses) or when neither source claims.
/// Lock-cheap: one thread-local borrow plus a GUC cell read; no catalog.
pub fn bypass_claimed() -> bool {
    init_small::globals::TryWithMyProcPort(|port| {
        if options_claim_bypass(port.cmdline_options.as_deref(), &port.guc_options) {
            return true;
        }
        match (&port.application_name, guc_tables::backing::pgrust_admission_bypass_applications()) {
            (Some(app), Some(list)) => application_matches(&list, app),
            _ => false,
        }
    })
    .unwrap_or(false)
}

/// The claim GUC's name; compared ASCII-case-insensitively like every GUC
/// name (guc.c's guc_name_compare).
pub const BYPASS_GUC: &str = "pgrust.admission_bypass";

/// Does the startup packet claim `pgrust.admission_bypass=on`? Looks at
/// both places a client can put a GUC: the name/value pairs of the packet
/// (`guc_options`, applied later by process_startup_options) and the
/// `options` string (`cmdline_options`: `-c name=value`, `-cname=value`,
/// `--name=value`, split like pg_split_opts). The last spelling wins, in
/// the order the GUC machinery will apply them later
/// (process_startup_options: the options switches first, then the packet
/// pairs — so a packet pair beats the options string).
pub fn options_claim_bypass(cmdline_options: Option<&str>, guc_options: &[String]) -> bool {
    let mut claim: Option<bool> = None;
    if let Some(opts) = cmdline_options {
        let argv = split_opts(opts);
        let mut i = 0;
        while i < argv.len() {
            let arg = &argv[i];
            let assignment: Option<&str> = if arg == "-c" {
                i += 1;
                argv.get(i).map(String::as_str)
            } else if let Some(rest) = arg.strip_prefix("--") {
                Some(rest)
            } else if let Some(rest) = arg.strip_prefix("-c") {
                Some(rest)
            } else {
                None
            };
            if let Some(a) = assignment {
                if let Some((name, value)) = a.split_once('=') {
                    if guc_name_eq(&name.replace('-', "_"), BYPASS_GUC) {
                        claim = Some(parse_bool_lite(value).unwrap_or(false));
                    }
                }
            }
            i += 1;
        }
    }
    let mut it = guc_options.iter();
    while let (Some(name), Some(value)) = (it.next(), it.next()) {
        if guc_name_eq(name, BYPASS_GUC) {
            claim = Some(parse_bool_lite(value).unwrap_or(false));
        }
    }
    claim.unwrap_or(false)
}

fn guc_name_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// bool GUC input, C's parse_bool spellings: any non-empty prefix of
/// true/false/yes/no, on/off (off needs two chars), 1/0; case-insensitive.
fn parse_bool_lite(value: &str) -> Option<bool> {
    let v = value.trim().to_ascii_lowercase();
    if v.is_empty() {
        return None;
    }
    let prefix_of = |word: &str| word.starts_with(v.as_str());
    if prefix_of("true") || prefix_of("yes") || v == "on" || v == "1" {
        Some(true)
    } else if prefix_of("false") || prefix_of("no") || (v.len() >= 2 && prefix_of("off")) || v == "0" {
        Some(false)
    } else {
        None
    }
}

/// pg_split_opts's tokenization (postinit): split on ASCII whitespace,
/// backslash escapes the next byte. Duplicated here because postinit
/// depends on this crate, not the reverse.
fn split_opts(optstr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut escape = false;
    for &c in optstr.as_bytes() {
        if escape {
            cur.push(c);
            escape = false;
        } else if c == b'\\' {
            escape = true;
        } else if c.is_ascii_whitespace() {
            if !cur.is_empty() {
                out.push(String::from_utf8_lossy(&cur).into_owned());
                cur.clear();
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(String::from_utf8_lossy(&cur).into_owned());
    }
    out
}

/// Split `pgrust.admission_bypass_applications`: comma-separated entries,
/// surrounding whitespace and one pair of double quotes stripped, empties
/// dropped. Entries may contain spaces ("pgAdmin 4").
pub fn parse_application_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(|e| {
            let e = e.trim();
            e.strip_prefix('"')
                .and_then(|e| e.strip_suffix('"'))
                .unwrap_or(e)
                .trim()
        })
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Does `app_name` (the startup packet's application_name) claim the
/// bypass under `list`? Case-insensitive PREFIX match against each entry:
/// tools append versions and connection ids ("pgAdmin 4 - CONN:123",
/// "DBeaver 24.1.0 - Main <db>"). Empty list or empty name: no.
pub fn application_matches(list: &str, app_name: &str) -> bool {
    let app = app_name.trim();
    if app.is_empty() {
        return false;
    }
    let app_lc = app.to_lowercase();
    parse_application_list(list)
        .iter()
        .any(|entry| app_lc.starts_with(&entry.to_lowercase()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_prefix_match_is_case_insensitive() {
        let list = "psql, pgAdmin 4, DBeaver";
        assert!(application_matches(list, "psql"));
        assert!(application_matches(list, "PSQL"));
        assert!(application_matches(list, "pgadmin 4 - CONN:123"));
        assert!(application_matches(list, "DBeaver 24.1.0 - Main postgres"));
        assert!(!application_matches(list, "PostgreSQL JDBC Driver"));
        assert!(!application_matches(list, "my-psql-wrapper")); // prefix, not substring
        assert!(!application_matches(list, ""));
        assert!(!application_matches(list, "   "));
    }

    #[test]
    fn application_list_parsing() {
        assert_eq!(parse_application_list(""), Vec::<String>::new());
        assert_eq!(parse_application_list(" , ,"), Vec::<String>::new());
        assert_eq!(
            parse_application_list(" psql ,\"pgAdmin 4\", DBeaver ,"),
            vec!["psql", "pgAdmin 4", "DBeaver"]
        );
        assert!(!application_matches("", "psql"));
        assert!(!application_matches("  ", "psql"));
        // A quoted entry with spaces matches the spaced name.
        assert!(application_matches("\"pgAdmin 4\"", "pgAdmin 4 - DB:postgres"));
    }

    #[test]
    fn default_list_admits_interactive_tools_not_drivers() {
        let d = guc_tables::backing::ADMISSION_BYPASS_APPLICATIONS_DEFAULT;
        for app in ["psql", "pgcli", "pgAdmin 4 - CONN:42", "DBeaver 24.3.0 - SQLEditor <x>", "DataGrip 2024.2"] {
            assert!(application_matches(d, app), "{app} should claim");
        }
        for app in [
            "PostgreSQL JDBC Driver",
            "Npgsql",
            "pg_dump",
            "pg_restore",
            "pgbench",
            "pg_basebackup",
            "psycopg",
            "pgbouncer",
            "",
        ] {
            assert!(!application_matches(d, app), "{app:?} must not claim");
        }
    }

    fn sv(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn startup_packet_pairs_claim() {
        assert!(options_claim_bypass(None, &sv(&["pgrust.admission_bypass", "on"])));
        assert!(options_claim_bypass(None, &sv(&["PGRUST.Admission_Bypass", "true"])));
        assert!(options_claim_bypass(None, &sv(&["application_name", "x", "pgrust.admission_bypass", "1"])));
        assert!(!options_claim_bypass(None, &sv(&["pgrust.admission_bypass", "off"])));
        assert!(!options_claim_bypass(None, &sv(&["pgrust.admission_bypass", "garbage"])));
        assert!(!options_claim_bypass(None, &sv(&["pgrust.admission_bypass_applications", "on"])));
        assert!(!options_claim_bypass(None, &[]));
        // Last spelling wins.
        assert!(!options_claim_bypass(None, &sv(&["pgrust.admission_bypass", "on", "pgrust.admission_bypass", "off"])));
    }

    #[test]
    fn options_string_claims() {
        assert!(options_claim_bypass(Some("-c pgrust.admission_bypass=on"), &[]));
        assert!(options_claim_bypass(Some("-cpgrust.admission_bypass=yes"), &[]));
        assert!(options_claim_bypass(Some("--pgrust.admission_bypass=on"), &[]));
        assert!(options_claim_bypass(Some("-c work_mem=4MB -c pgrust.admission-bypass=t"), &[]));
        assert!(!options_claim_bypass(Some("-c pgrust.admission_bypass=off"), &[]));
        assert!(!options_claim_bypass(Some("-c work_mem=4MB"), &[]));
        assert!(!options_claim_bypass(Some("-c pgrust.admission_bypass"), &[]));
        assert!(!options_claim_bypass(Some(""), &[]));
        // Escaped space inside a value does not break tokenization.
        assert!(options_claim_bypass(Some("-c application_name=a\\ b -c pgrust.admission_bypass=on"), &[]));
        // Packet pairs are applied after the options switches, so they win.
        assert!(!options_claim_bypass(
            Some("-c pgrust.admission_bypass=on"),
            &sv(&["pgrust.admission_bypass", "off"])
        ));
    }

    #[test]
    fn bool_spellings() {
        for t in ["on", "ON", "true", "t", "yes", "y", "1", " on "] {
            assert_eq!(parse_bool_lite(t), Some(true), "{t}");
        }
        for f in ["off", "of", "false", "f", "no", "n", "0"] {
            assert_eq!(parse_bool_lite(f), Some(false), "{f}");
        }
        for bad in ["", "o", "2", "maybe", "onn"] {
            assert_eq!(parse_bool_lite(bad), None, "{bad}");
        }
    }

    #[test]
    fn no_port_means_no_claim() {
        assert!(!bypass_claimed());
    }
}
