//! The reap predicate and the zero-backend streak tracker — pure logic,
//! kept free of catalog/procarray/clock I/O so the exemption rules are
//! unit-testable (spec D1 items 1-3). Time enters as caller-supplied
//! monotonic nanoseconds (`pg_clock::mono_ns`, the determinism choke);
//! tests feed synthetic stamps.

use std::collections::HashMap;

use types_core::Oid;

/// The membership half of the reap predicate: is this database one the
/// janitor may EVER touch? Applied identically by the startup sweep and the
/// per-tick reap pass (the reap pass additionally requires the grace-long
/// zero-backend streak).
///
/// - Template exemption is load-bearing (spec item 1): `datistemplate =
///   true` is never touched, prefix-matching or not.
/// - Pins exempt for one postmaster lifetime (spec item 5).
/// - The janitor's own home database is excluded defensively: `dropdb`
///   would refuse it anyway (ERRCODE_OBJECT_IN_USE), but a prefix that
///   happens to cover the home database must not produce a per-cycle error
///   drumbeat.
pub fn reap_candidate(
    name: &str,
    prefix: &str,
    istemplate: bool,
    pinned: bool,
    is_own_db: bool,
) -> bool {
    !prefix.is_empty()
        && name.as_bytes().starts_with(prefix.as_bytes())
        && !istemplate
        && !pinned
        && !is_own_db
}

/// Continuous zero-backend streaks, keyed by database oid, living in janitor
/// memory only (spec item 2): restarts forget streaks (the startup sweep
/// makes that moot), and a database observed with any backend loses its
/// streak entirely.
///
/// Polling caveat, documented deliberately: "continuously" means
/// "at every ~500ms observation"; a connection that arrives and leaves
/// wholly between two ticks is invisible. The final `dropdb` still counts
/// backends authoritatively (and refuses occupied databases), so the streak
/// is an eligibility heuristic, never the safety mechanism.
pub struct StreakTracker {
    /// oid -> mono-ns stamp of the first observation of the current
    /// all-zero streak.
    zero_since_ns: HashMap<Oid, u64>,
}

impl StreakTracker {
    pub fn new() -> Self {
        StreakTracker {
            zero_since_ns: HashMap::new(),
        }
    }

    /// Record this tick's backend count for `oid` at monotonic time
    /// `now_ns`. Returns the length in ns of the continuous zero-backend
    /// streak (0 on the first idle observation), or `None` — streak reset —
    /// when backends exist.
    pub fn observe(&mut self, oid: Oid, backends: i32, now_ns: u64) -> Option<u64> {
        if backends > 0 {
            self.zero_since_ns.remove(&oid);
            return None;
        }
        let since = *self.zero_since_ns.entry(oid).or_insert(now_ns);
        Some(now_ns.saturating_sub(since))
    }

    /// Forget every oid not in `seen` this cycle: dropped, renamed out of
    /// the prefix, re-templated, or newly pinned databases all restart from
    /// a fresh streak if they ever become candidates again.
    pub fn retain_seen(&mut self, seen: &[Oid]) {
        self.zero_since_ns.retain(|oid, _| seen.contains(oid));
    }

    /// Forget `oid`'s streak entirely — the failed-drop backoff
    /// (main_loop::drop_batch): a database whose drop failed must re-earn a
    /// full grace period of observed idleness before the janitor attempts
    /// it again. Without this, a drop that `dropdb` refuses
    /// deterministically (a prepared transaction, a logical-replication
    /// subscription) would be re-attempted every tick forever.
    pub fn reset(&mut self, oid: Oid) {
        self.zero_since_ns.remove(&oid);
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.zero_since_ns.len()
    }
}

impl Default for StreakTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: u64 = 1_000_000_000;
    const GRACE: u64 = 15 * SEC;

    #[test]
    fn template_exemption_is_absolute() {
        // Prefix-matching template: never a candidate (spec item 1).
        assert!(!reap_candidate("tv_tpl", "tv_", true, false, false));
        // Sealed template under the recommended out-of-prefix convention.
        assert!(!reap_candidate("tpl_abc", "tv_", true, false, false));
        // The same name without the template bit IS a candidate.
        assert!(reap_candidate("tv_tpl", "tv_", false, false, false));
    }

    #[test]
    fn pin_exempts() {
        assert!(!reap_candidate("tv_pinned", "tv_", false, true, false));
        assert!(reap_candidate("tv_pinned", "tv_", false, false, false));
    }

    #[test]
    fn prefix_scoping_is_exact() {
        assert!(reap_candidate("tv_x", "tv_", false, false, false));
        assert!(!reap_candidate("tx_x", "tv_", false, false, false));
        assert!(!reap_candidate("tv", "tv_", false, false, false));
        // Empty prefix means the feature is off: nothing ever matches, even
        // though every string starts with "".
        assert!(!reap_candidate("tv_x", "", false, false, false));
    }

    #[test]
    fn own_database_is_never_a_candidate() {
        assert!(!reap_candidate("postgres", "post", false, false, true));
    }

    #[test]
    fn streak_accumulates_only_while_idle() {
        let mut t = StreakTracker::new();
        let t0 = 100 * SEC;
        let oid: Oid = 90001;

        // First idle observation starts the streak at zero.
        assert_eq!(t.observe(oid, 0, t0), Some(0));
        // Idle 10s in: not yet at grace.
        let d = t.observe(oid, 0, t0 + 10 * SEC).unwrap();
        assert!(d < GRACE);
        // Idle 15s in: grace reached.
        let d = t.observe(oid, 0, t0 + 15 * SEC).unwrap();
        assert!(d >= GRACE);
    }

    #[test]
    fn a_connection_resets_the_streak() {
        let mut t = StreakTracker::new();
        let t0 = 100 * SEC;
        let oid: Oid = 90002;

        t.observe(oid, 0, t0);
        t.observe(oid, 0, t0 + 14 * SEC);
        // A backend appears: streak gone.
        assert_eq!(t.observe(oid, 1, t0 + 14 * SEC), None);
        // Idle again: the clock restarts from the new observation.
        assert_eq!(t.observe(oid, 0, t0 + 20 * SEC), Some(0));
        let d = t.observe(oid, 0, t0 + 30 * SEC).unwrap();
        assert!(d < GRACE);
    }

    #[test]
    fn unseen_oids_are_forgotten() {
        let mut t = StreakTracker::new();
        let t0 = 100 * SEC;
        t.observe(90003, 0, t0);
        t.observe(90004, 0, t0);
        assert_eq!(t.tracked(), 2);
        // 90004 left the candidate set (dropped, pinned, re-templated...).
        t.retain_seen(&[90003]);
        assert_eq!(t.tracked(), 1);
        // If it comes back it starts over.
        assert_eq!(t.observe(90004, 0, t0 + 60 * SEC), Some(0));
    }

    #[test]
    fn zero_grace_reaps_on_first_idle_observation() {
        let mut t = StreakTracker::new();
        // streak 0 >= grace 0: reaped at the first idle tick.
        assert_eq!(t.observe(90005, 0, 100 * SEC), Some(0));
    }

    #[test]
    fn reset_forces_a_fresh_full_grace() {
        let mut t = StreakTracker::new();
        let t0 = 100 * SEC;
        let oid: Oid = 90006;
        t.observe(oid, 0, t0);
        assert!(t.observe(oid, 0, t0 + 20 * SEC).unwrap() >= GRACE);
        // Failed drop: the streak restarts from the next observation.
        t.reset(oid);
        assert_eq!(t.observe(oid, 0, t0 + 21 * SEC), Some(0));
        let d = t.observe(oid, 0, t0 + 30 * SEC).unwrap();
        assert!(d < GRACE);
    }
}
