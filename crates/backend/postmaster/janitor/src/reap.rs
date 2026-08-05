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
/// - An in-flight or just-completed mint Ensure (`ensure_shielded`, from
///   `registry::ensure_shields`) exempts like a pin: the startup/deferred
///   sweep is graceless and a zero/short per-template grace reaps at the
///   first idle observation, so without the shield a minted database could
///   be dropped in the mint-to-first-connect window (D2 race suite).
/// - A listed warm-pool spare (`spare_shielded`, from
///   `registry::spare_shields`) exempts like a pin for as long as it is
///   listed (D3 warm pool): spares are zero-connection by construction, so
///   without the shield the pool would reap itself after grace. Unlisted
///   leftovers (post-restart — the registry is restart-lossy — or spares
///   dropped from the pool after a poisoned handout) are ordinary
///   candidates, which is exactly the wanted cold-start/self-heal behavior.
/// - An in-flight `pgrust_seal_template()` request (`seal_shielded`, from
///   `registry::seal_shields`) exempts like a pin while non-terminal: the
///   seal's target is by definition NOT yet a template, so an in-prefix
///   target already grace-idle when the seal was posted (built, then
///   abandoned, then sealed from another database) would otherwise be
///   reaped out from under its own seal. Terminal seals stop shielding —
///   Done hands over to the template exemption, Failed resumes ordinary
///   lifecycle.
#[allow(clippy::too_many_arguments)]
pub fn reap_candidate(
    name: &str,
    prefix: &str,
    istemplate: bool,
    pinned: bool,
    is_own_db: bool,
    ensure_shielded: bool,
    spare_shielded: bool,
    seal_shielded: bool,
) -> bool {
    !prefix.is_empty()
        && name.as_bytes().starts_with(prefix.as_bytes())
        && !istemplate
        && !pinned
        && !is_own_db
        && !ensure_shielded
        && !spare_shielded
        && !seal_shielded
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
        assert!(!reap_candidate("tv_tpl", "tv_", true, false, false, false, false, false));
        // Sealed template under the recommended out-of-prefix convention.
        assert!(!reap_candidate("tpl_abc", "tv_", true, false, false, false, false, false));
        // The same name without the template bit IS a candidate.
        assert!(reap_candidate("tv_tpl", "tv_", false, false, false, false, false, false));
    }

    #[test]
    fn pin_exempts() {
        assert!(!reap_candidate("tv_pinned",
            "tv_",
            false,
            true,
            false,
            false, false, false));
        assert!(reap_candidate("tv_pinned",
            "tv_",
            false,
            false,
            false,
            false, false, false));
    }

    #[test]
    fn prefix_scoping_is_exact() {
        assert!(reap_candidate("tv_x", "tv_", false, false, false, false, false, false));
        assert!(!reap_candidate("tx_x", "tv_", false, false, false, false, false, false));
        assert!(!reap_candidate("tv", "tv_", false, false, false, false, false, false));
        // Empty prefix means the feature is off: nothing ever matches, even
        // though every string starts with "".
        assert!(!reap_candidate("tv_x", "", false, false, false, false, false, false));
    }

    #[test]
    fn own_database_is_never_a_candidate() {
        assert!(!reap_candidate("postgres", "post", false, false, true, false, false, false));
    }

    #[test]
    fn ensure_shield_exempts_like_a_pin() {
        // An in-flight/lingering mint Ensure shields from sweep AND reap
        // (deleting the ensure_shielded clause fails this).
        assert!(!reap_candidate("tv_minting",
            "tv_",
            false,
            false,
            false,
            true, false, false));
        assert!(reap_candidate("tv_minting",
            "tv_",
            false,
            false,
            false,
            false, false, false));
    }

    #[test]
    fn spare_shield_exempts_like_a_pin() {
        // A listed warm-pool spare is exempt from sweep AND reap (deleting
        // the spare_shielded clause fails this); an unlisted leftover with
        // the same shape is an ordinary candidate (cold-start sweep /
        // poisoned-spare self-heal).
        assert!(!reap_candidate(
            "tv_spare_1",
            "tv_",
            false,
            false,
            false,
            false,
            true
        , false));
        assert!(reap_candidate(
            "tv_spare_1",
            "tv_",
            false,
            false,
            false,
            false,
            false
        , false));
    }

    #[test]
    fn seal_shield_exempts_like_a_pin() {
        // An in-flight pgrust_seal_template target is exempt from sweep AND
        // reap (deleting the seal_shielded clause fails this); the same
        // name without an in-flight seal is an ordinary candidate.
        assert!(!reap_candidate(
            "tv_sealing", "tv_", false, false, false, false, false, true
        ));
        assert!(reap_candidate(
            "tv_sealing", "tv_", false, false, false, false, false, false
        ));
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
