//! FaultDriver — the H2 seam between plan-format v1's RESERVED fault tags
//! (`Crash(<pt>)` / `TornWrite` / `Env(<what>)`, H1 plan schema: render+parse
//! OK, execute refused) and the P4 SimVfs fault-plan engine
//! (crates/backend/storage/file/vfs/src/sim.rs — `SeededFaultPlan::install`,
//! `FaultRule::{nth_matching,crash_at_op}`, `OpMatch`, `FaultDecision`,
//! `CrashImage`; API shape pinned against dst/p4-faults-inc2 @ bf119f11c).
//!
//! ## Why these are MIRROR types, not the engine's
//!
//! simharness is its OWN cargo workspace (client-side; Cargo.toml header:
//! deps here must never enter the product build closure) and the engine
//! types are `cfg(pgrust_sim)`-only INSIDE the product vfs crate. The
//! harness therefore speaks a serialized SPEC (this module's `FaultPlanSpec`
//! JSON) and the sim-built DUT owns the real types. Field-for-field the spec
//! mirrors `SeededFaultPlan::new(seed, rules)`:
//!
//!   FaultPlanSpec{seed, rules}          = SeededFaultPlan::install(seed, rules)
//!   FaultRuleSpec{matcher,nth,action,sticky} = FaultRule
//!   OpMatchSpec{kinds,class,path_contains}   = OpMatch
//!   FaultActionSpec                          = FaultDecision (non-Proceed arms)
//!   OpKindSpec / PathClassSpec               = OpKind / PathClass (subset the
//!                                              mapping emits; extend as needed)
//!
//! ## The seam (H2) and the t28 activation step
//!
//! `execute_plan`'s reserved-tag arm now routes through the [`FaultDriver`]
//! in `ExecOptions` instead of refusing inline:
//!
//! - [`NoOpFaultDriver`] (DEFAULT, always compiled): `arm()` returns
//!   `Unsupported`, which the driver dispatch reports as the SAME
//!   `fault-reserved-refused` P1 the H1 arm minted — main-lineage behavior
//!   is unchanged (native no-op gate) and a reserved tag reaching execution
//!   stays LOUD, never a silent skip.
//! - [`SimVfsFaultDriver`] (feature `simvfs-faults`): the real mapping
//!   ([`map_fault_step`], pure + deterministic, unit-tested against a mock
//!   installer below). Its `arm()` serializes the spec and delivers it via
//!   the [`DELIVERY_ENV`] channel. UNTIL the fault engine's server-side
//!   reader boards main (t28: dst/p4-faults-inc2), there is nothing on the
//!   DUT to read the channel, so `arm()` refuses with `NotWiredYet` unless
//!   `SIMHARNESS_FAULT_DELIVERY=env` asserts the DUT-side reader exists.
//!
//! t28 ACTIVATION (the step this module documents; do NOT do it before the
//! engine boards main):
//!   1. sim-server boot (main_main, `cfg(pgrust_sim)`) reads
//!      `PGRUST_SIM_FAULT_PLAN` (JSON of `FaultPlanSpec`), deserializes with
//!      the engine-owned twin of these types, and calls
//!      `SeededFaultPlan::install(spec.seed, rules)` — which also arms
//!      `CrashImage::SeededSubset(seed)` (the engine's documented common
//!      harness shape).
//!   2. build simharness with `--features simvfs-faults`, run with
//!      `SIMHARNESS_FAULT_DELIVERY=env`, DUT restarted per faulted plan
//!      (a fault plan is per-boot state; the runner's `--restart-cmd` is the
//!      existing lever).
//!   3. flip the reserved tags executable in the generator vocabulary (their
//!      weights are already profile knobs; emission stays 0 until then).

use serde::{Deserialize, Serialize};

use super::planface::FaultPoint;

/// Env var the sim-built DUT will read at boot (t28 activation step 1).
pub const DELIVERY_ENV: &str = "PGRUST_SIM_FAULT_PLAN";

// ---------------------------------------------------------------------------
// Mirror spec types (serde JSON = the delivery format)
// ---------------------------------------------------------------------------

/// Mirror of engine `OpKind` — only the kinds the v1 mapping emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKindSpec {
    Open,
    PWriteV,
    Fsync,
    Fdatasync,
    Rename,
    Unlink,
}

/// Mirror of engine `PathClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathClassSpec {
    Wal,
    Config,
    Temp,
    Heap,
    Other,
}

/// Mirror of engine `OpMatch` (empty = any op).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpMatchSpec {
    pub kinds: Option<Vec<OpKindSpec>>,
    pub class: Option<PathClassSpec>,
    pub path_contains: Option<String>,
}

/// Mirror of engine `FaultDecision`, non-`Proceed` arms. Injection errnos
/// are restricted to fd's `errcode_for_file_access` vocabulary (engine
/// contract §1.1); the mapping only emits the named constants below.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FaultActionSpec {
    Errno(i32),
    ShortRead(usize),
    ShortWrite(usize),
    TornWrite { persist_prefix: usize },
    Crash,
}

/// Mirror of engine `FaultRule`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultRuleSpec {
    pub matcher: OpMatchSpec,
    pub nth: u64,
    pub action: FaultActionSpec,
    pub sticky: bool,
}

impl FaultRuleSpec {
    /// Mirror of `FaultRule::nth_matching`.
    pub fn nth_matching(matcher: OpMatchSpec, nth: u64, action: FaultActionSpec) -> Self {
        FaultRuleSpec { matcher, nth, action, sticky: false }
    }

    /// Mirror of `FaultRule::crash_at_op`.
    pub fn crash_at_op(nth: u64) -> Self {
        FaultRuleSpec::nth_matching(OpMatchSpec::default(), nth, FaultActionSpec::Crash)
    }
}

/// Mirror of `SeededFaultPlan::install(seed, rules)` — one installed plan.
/// The engine derives `CrashImage::SeededSubset(seed)` from the same seed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultPlanSpec {
    pub seed: u64,
    pub rules: Vec<FaultRuleSpec>,
}

// errno constants the mapping emits (numeric so the spec JSON is
// platform-pinned; values are the universal ones).
pub const ENOSPC: i32 = 28;
pub const EIO: i32 = 5;
pub const EMFILE: i32 = 24;

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum FaultDriverError {
    /// This driver does not execute reserved fault tags (NoOp default).
    /// The dispatch arm reports the H1-identical `fault-reserved-refused` P1.
    Unsupported(String),
    /// The mapping exists but the DUT-side reader is not on main yet (t28).
    NotWiredYet(String),
    /// Unknown fault payload vocabulary — loud, never guessed.
    BadPayload(String),
    /// Delivery attempted and failed.
    DeliveryFailed(String),
}

impl std::fmt::Display for FaultDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultDriverError::Unsupported(m) => write!(f, "unsupported: {m}"),
            FaultDriverError::NotWiredYet(m) => write!(f, "not wired yet (t28): {m}"),
            FaultDriverError::BadPayload(m) => write!(f, "bad fault payload: {m}"),
            FaultDriverError::DeliveryFailed(m) => write!(f, "delivery failed: {m}"),
        }
    }
}

/// The seam. `map` is pure/deterministic (same plan seed + step index ⇒ the
/// same spec, the harness determinism law); `arm` performs the delivery.
/// `&self` because `ExecOptions` is shared immutably through `execute_plan`;
/// stateful drivers use interior mutability.
pub trait FaultDriver {
    fn name(&self) -> &'static str;

    /// Map one reserved fault step to the engine fault-plan spec.
    fn map(
        &self,
        fault: &FaultPoint,
        plan_seed: u64,
        step_idx: usize,
    ) -> Result<FaultPlanSpec, FaultDriverError>;

    /// Deliver/install the spec on the DUT (before the faulted span runs).
    fn arm(&self, spec: &FaultPlanSpec) -> Result<(), FaultDriverError>;
}

// ---------------------------------------------------------------------------
// The mapping (pure; shared by the real driver and the mock unit tests)
// ---------------------------------------------------------------------------

/// splitmix64 — the SimEntropy generator family (engine determinism rule:
/// all entropy is the explicit seed).
fn splitmix64(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Per-step fault seed: the plan seed and the step index, mixed. Two fault
/// steps in one plan get independent, reproducible streams.
fn step_seed(plan_seed: u64, step_idx: usize) -> u64 {
    let mut s = plan_seed ^ (step_idx as u64).wrapping_mul(0xA24BAED4963EE407);
    splitmix64(&mut s)
}

/// The v1 fault-step → fault-plan mapping. Vocabulary (plan payloads are
/// whitespace-free tokens per plan.rs `validate_step`):
///
/// - `Crash(<pt>)`:
///     `op:<n>`      crash at the nth op of the run (`FaultRule::crash_at_op`)
///     `wal-fsync`   crash on a seed-picked nth WAL fsync/fdatasync
///     `wal-write`   crash on a seed-picked nth WAL pwritev
///     `control`     crash on a seed-picked nth Config-class write/fsync
///     `any`         crash at a seed-picked op number (1..=256)
/// - `TornWrite`: torn WAL pwritev — seed-picked nth, seed-picked surviving
///     prefix (the engine floors it to the 512 B sector boundary; the run
///     CRASHES during that write per the engine's TornWrite semantics)
/// - `Env(<what>)`:
///     `enospc`      sticky ENOSPC on every pwritev from a seed-picked point
///     `eio`         one-shot EIO on a seed-picked nth fsync/fdatasync
///                   (fires the engine's fsyncgate state machine)
///     `emfile`      one-shot EMFILE on a seed-picked nth open
///     `short-write` one-shot posix-legal short pwritev (seed-picked length)
///
/// Unknown payloads are `BadPayload` — loud, never guessed (vocab law).
pub fn map_fault_step(
    fault: &FaultPoint,
    plan_seed: u64,
    step_idx: usize,
) -> Result<FaultPlanSpec, FaultDriverError> {
    let seed = step_seed(plan_seed, step_idx);
    let mut s = seed;
    let nth_small = (splitmix64(&mut s) % 8) + 1; // 1..=8
    let rules = match fault {
        FaultPoint::Crash(pt) => match pt.as_str() {
            "any" => vec![FaultRuleSpec::crash_at_op((splitmix64(&mut s) % 256) + 1)],
            "wal-fsync" => vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::Fsync, OpKindSpec::Fdatasync]),
                    class: Some(PathClassSpec::Wal),
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::Crash,
            )],
            "wal-write" => vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::PWriteV]),
                    class: Some(PathClassSpec::Wal),
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::Crash,
            )],
            "control" => vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![
                        OpKindSpec::PWriteV,
                        OpKindSpec::Fsync,
                        OpKindSpec::Rename,
                    ]),
                    class: Some(PathClassSpec::Config),
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::Crash,
            )],
            other => match other.strip_prefix("op:").and_then(|n| n.parse::<u64>().ok()) {
                Some(n) if n >= 1 => vec![FaultRuleSpec::crash_at_op(n)],
                _ => {
                    return Err(FaultDriverError::BadPayload(format!(
                        "crash point '{other}' not in v1 vocabulary (any|wal-fsync|wal-write|control|op:<n>)"
                    )))
                }
            },
        },
        FaultPoint::TornWrite => {
            // Surviving prefix: an arbitrary byte count; the ENGINE floors it
            // to the sector boundary (rule 1) — the spec deliberately does
            // not pre-floor, so the engine's own law is what's exercised.
            let persist_prefix = (splitmix64(&mut s) % 16384) as usize;
            vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::PWriteV]),
                    class: Some(PathClassSpec::Wal),
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::TornWrite { persist_prefix },
            )]
        }
        FaultPoint::Env(what) => match what.as_str() {
            "enospc" => vec![FaultRuleSpec {
                matcher: OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::PWriteV]),
                    class: None,
                    path_contains: None,
                },
                nth: nth_small,
                action: FaultActionSpec::Errno(ENOSPC),
                sticky: true, // disk-full persists until the plan is torn down
            }],
            "eio" => vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::Fsync, OpKindSpec::Fdatasync]),
                    class: None,
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::Errno(EIO),
            )],
            "emfile" => vec![FaultRuleSpec::nth_matching(
                OpMatchSpec {
                    kinds: Some(vec![OpKindSpec::Open]),
                    class: None,
                    path_contains: None,
                },
                nth_small,
                FaultActionSpec::Errno(EMFILE),
            )],
            "short-write" => {
                let n = (splitmix64(&mut s) % 4096) as usize;
                vec![FaultRuleSpec::nth_matching(
                    OpMatchSpec {
                        kinds: Some(vec![OpKindSpec::PWriteV]),
                        class: None,
                        path_contains: None,
                    },
                    nth_small,
                    FaultActionSpec::ShortWrite(n),
                )]
            }
            other => {
                return Err(FaultDriverError::BadPayload(format!(
                    "env fault '{other}' not in v1 vocabulary (enospc|eio|emfile|short-write)"
                )))
            }
        },
        // Executable-v1 tags never reach the driver (dispatch handles them).
        FaultPoint::Disconnect | FaultPoint::ReconnectServer => {
            return Err(FaultDriverError::Unsupported(
                "executable-v1 fault tags are dispatched by the driver arm, not FaultDriver".into(),
            ))
        }
    };
    Ok(FaultPlanSpec { seed, rules })
}

// ---------------------------------------------------------------------------
// NoOp default (main-lineage: fault engine absent)
// ---------------------------------------------------------------------------

/// The default driver on the main lineage: reserved tags stay refused with
/// the H1-identical `fault-reserved-refused` P1 (loud, never a silent skip).
/// It still MAPS (the mapping is engine-independent) so `map`-then-`arm`
/// failures carry the spec they would have installed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpFaultDriver;

impl FaultDriver for NoOpFaultDriver {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn map(
        &self,
        fault: &FaultPoint,
        plan_seed: u64,
        step_idx: usize,
    ) -> Result<FaultPlanSpec, FaultDriverError> {
        map_fault_step(fault, plan_seed, step_idx)
    }

    fn arm(&self, _spec: &FaultPlanSpec) -> Result<(), FaultDriverError> {
        Err(FaultDriverError::Unsupported(
            "NoOpFaultDriver: fault engine not activated (reserved tag, H4 refusal)".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// SimVfs-backed driver (feature-gated; lights up at t28)
// ---------------------------------------------------------------------------

/// The real driver. Compiles under `--features simvfs-faults` WITHOUT any
/// product-crate dependency (the spec is the wire format); until the DUT-side
/// [`DELIVERY_ENV`] reader boards main (t28), `arm` refuses `NotWiredYet`
/// unless the operator asserts the reader exists via
/// `SIMHARNESS_FAULT_DELIVERY=env`.
#[cfg(feature = "simvfs-faults")]
pub struct SimVfsFaultDriver {
    /// Where arm() writes the spec JSON for the (restarted) DUT to pick up:
    /// a file whose path the boot wrapper exports as [`DELIVERY_ENV`].
    pub spec_path: std::path::PathBuf,
}

#[cfg(feature = "simvfs-faults")]
impl FaultDriver for SimVfsFaultDriver {
    fn name(&self) -> &'static str {
        "simvfs"
    }

    fn map(
        &self,
        fault: &FaultPoint,
        plan_seed: u64,
        step_idx: usize,
    ) -> Result<FaultPlanSpec, FaultDriverError> {
        map_fault_step(fault, plan_seed, step_idx)
    }

    fn arm(&self, spec: &FaultPlanSpec) -> Result<(), FaultDriverError> {
        if std::env::var("SIMHARNESS_FAULT_DELIVERY").as_deref() != Ok("env") {
            return Err(FaultDriverError::NotWiredYet(format!(
                "DUT-side {DELIVERY_ENV} reader is a t28 item (dst/p4-faults-inc2); \
                 set SIMHARNESS_FAULT_DELIVERY=env once it boards main"
            )));
        }
        let json = serde_json::to_string(spec)
            .map_err(|e| FaultDriverError::DeliveryFailed(e.to_string()))?;
        std::fs::write(&self.spec_path, json)
            .map_err(|e| FaultDriverError::DeliveryFailed(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Unit tests: the mapping against a mock installer
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Mock of the engine side: records what `SeededFaultPlan::install`
    /// would have been called with.
    #[derive(Default)]
    struct MockInstaller {
        installed: RefCell<Vec<FaultPlanSpec>>,
    }

    impl FaultDriver for MockInstaller {
        fn name(&self) -> &'static str {
            "mock"
        }
        fn map(
            &self,
            fault: &FaultPoint,
            plan_seed: u64,
            step_idx: usize,
        ) -> Result<FaultPlanSpec, FaultDriverError> {
            map_fault_step(fault, plan_seed, step_idx)
        }
        fn arm(&self, spec: &FaultPlanSpec) -> Result<(), FaultDriverError> {
            self.installed.borrow_mut().push(spec.clone());
            Ok(())
        }
    }

    fn map_arm(mock: &MockInstaller, f: &FaultPoint, seed: u64, idx: usize) -> FaultPlanSpec {
        let spec = mock.map(f, seed, idx).expect("map");
        mock.arm(&spec).expect("arm");
        spec
    }

    #[test]
    fn crash_op_n_maps_to_crash_at_op() {
        let mock = MockInstaller::default();
        let spec = map_arm(&mock, &FaultPoint::Crash("op:17".into()), 42, 3);
        assert_eq!(spec.rules, vec![FaultRuleSpec::crash_at_op(17)]);
        assert_eq!(mock.installed.borrow().len(), 1);
    }

    #[test]
    fn crash_wal_fsync_maps_to_wal_class_fsync_rule() {
        let mock = MockInstaller::default();
        let spec = map_arm(&mock, &FaultPoint::Crash("wal-fsync".into()), 42, 3);
        assert_eq!(spec.rules.len(), 1);
        let r = &spec.rules[0];
        assert_eq!(r.action, FaultActionSpec::Crash);
        assert_eq!(r.matcher.class, Some(PathClassSpec::Wal));
        assert_eq!(
            r.matcher.kinds.as_deref(),
            Some(&[OpKindSpec::Fsync, OpKindSpec::Fdatasync][..])
        );
        assert!((1..=8).contains(&r.nth), "nth in the small deterministic band");
        assert!(!r.sticky);
    }

    #[test]
    fn torn_write_maps_to_wal_pwritev_torn_rule() {
        let mock = MockInstaller::default();
        let spec = map_arm(&mock, &FaultPoint::TornWrite, 7, 0);
        let r = &spec.rules[0];
        assert!(matches!(r.action, FaultActionSpec::TornWrite { .. }));
        assert_eq!(r.matcher.kinds.as_deref(), Some(&[OpKindSpec::PWriteV][..]));
        assert_eq!(r.matcher.class, Some(PathClassSpec::Wal));
    }

    #[test]
    fn env_enospc_is_sticky_env_eio_is_oneshot() {
        let mock = MockInstaller::default();
        let a = map_arm(&mock, &FaultPoint::Env("enospc".into()), 7, 0);
        assert!(a.rules[0].sticky);
        assert_eq!(a.rules[0].action, FaultActionSpec::Errno(ENOSPC));
        let b = map_arm(&mock, &FaultPoint::Env("eio".into()), 7, 1);
        assert!(!b.rules[0].sticky);
        assert_eq!(b.rules[0].action, FaultActionSpec::Errno(EIO));
    }

    #[test]
    fn mapping_is_deterministic_and_step_independent() {
        let f = FaultPoint::TornWrite;
        let s1 = map_fault_step(&f, 1234, 5).unwrap();
        let s2 = map_fault_step(&f, 1234, 5).unwrap();
        assert_eq!(s1, s2, "same (seed, step) ⇒ same spec");
        let s3 = map_fault_step(&f, 1234, 6).unwrap();
        assert_ne!(s1.seed, s3.seed, "different step ⇒ independent stream");
    }

    #[test]
    fn unknown_payloads_are_loud() {
        assert!(matches!(
            map_fault_step(&FaultPoint::Crash("bogus".into()), 1, 0),
            Err(FaultDriverError::BadPayload(_))
        ));
        assert!(matches!(
            map_fault_step(&FaultPoint::Env("bogus".into()), 1, 0),
            Err(FaultDriverError::BadPayload(_))
        ));
        assert!(matches!(
            map_fault_step(&FaultPoint::Crash("op:0".into()), 1, 0),
            Err(FaultDriverError::BadPayload(_))
        ));
    }

    #[test]
    fn noop_driver_maps_but_refuses_arm() {
        let d = NoOpFaultDriver;
        let spec = d.map(&FaultPoint::Crash("any".into()), 9, 2).unwrap();
        assert!(matches!(d.arm(&spec), Err(FaultDriverError::Unsupported(_))));
    }

    #[test]
    fn spec_json_round_trips() {
        let spec = map_fault_step(&FaultPoint::Env("short-write".into()), 77, 4).unwrap();
        let j = serde_json::to_string(&spec).unwrap();
        let back: FaultPlanSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(spec, back);
    }
}
