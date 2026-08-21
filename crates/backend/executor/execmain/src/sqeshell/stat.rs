//! The sqe census surface (P1-4 census-surface.md, landed with P2-1):
//! process-global counters + the pg_stat-style SRF builtins the
//! on-demand views read (`scripts/sqe-stat-views.sql`).
//!
//! Grain and cost: refusal/engagement ticks happen ONLY on columnar
//! statements (the dispatch slot consults the AM first), and every such
//! statement either errors or runs an engine query — a mutex'd map
//! update is noise there. Heap statements execute zero instructions of
//! this module (refusal-enum §6.5).
//!
//! Recorded deviation from the design doc: the refusal ledger is a
//! key-string map (parameterized causes make a dense static array
//! impossible without an index registry — exactly the lanev4 numbering
//! tax Rule B deletes). Variant-grain keys are pre-seeded at zero so
//! absent≠zero stays decidable (the lanev2 zeros-included witness
//! contract); full keys appear on first observation.

use pgsync::Mutex;
use std::borrow::Cow;
use std::collections::BTreeMap;

use super::refusal::RefuseCause;

pub struct RefusalRow {
    pub family: &'static str,
    pub sqlstate: String,
    pub count: u64,
    pub last_fp: u64,
}

struct Census {
    refusals: BTreeMap<String, RefusalRow>,
    /// Serving families: (family, tier) -> (engaged, completed).
    /// engaged - completed = mid-flight errors (must be explainable).
    engagements: BTreeMap<(&'static str, &'static str), (u64, u64)>,
    /// Posture witnesses: (family, tier) -> observed. Witness families
    /// record that a posture was taken (ring armed, fill pooled, memo
    /// hit, ...) — they have NO completion by construction, and the
    /// census surface must never print a `completed` they don't
    /// maintain (the B4 false-alarm lesson,
    /// b4-heapfill-scrutiny-20260819.md §6): a witness row that showed
    /// `completed=0` read as abandoned work.
    witnesses: BTreeMap<(&'static str, &'static str), u64>,
}

static CENSUS: Mutex<Option<Census>> = Mutex::new(None);

fn with_census<R>(f: impl FnOnce(&mut Census) -> R) -> R {
    let mut g = CENSUS.lock().unwrap_or_else(|e| e.into_inner());
    let c = g.get_or_insert_with(|| {
        // Zero-row seed at variant grain (zeros-included).
        let mut refusals = BTreeMap::new();
        for cause in RefuseCause::all_for_test() {
            refusals.insert(
                cause.variant_key().to_string(),
                RefusalRow {
                    family: cause.family(),
                    sqlstate: sqlstate_str(cause),
                    count: 0,
                    last_fp: 0,
                },
            );
        }
        Census { refusals, engagements: BTreeMap::new(), witnesses: BTreeMap::new() }
    });
    f(c)
}

fn sqlstate_str(cause: RefuseCause) -> String {
    let chars = ::types_error::unpack_sqlstate(cause.sqlstate());
    String::from_utf8_lossy(&chars).into_owned()
}

/// THE production tick (called only from `RefuseCause::refuse`).
pub(super) fn tick_refusal(cause: RefuseCause, source_fp: u64) {
    with_census(|c| {
        let row = c.refusals.entry(cause.census_key()).or_insert(RefusalRow {
            family: cause.family(),
            sqlstate: sqlstate_str(cause),
            count: 0,
            last_fp: 0,
        });
        row.count += 1;
        row.last_fp = source_fp;
    });
}

/// Engagement tick at the drive face — AFTER the last declinable point
/// (the lanev3 U-06 lesson: engaged==will-run by construction).
pub fn tick_engaged(family: &'static str, tier: &'static str) {
    with_census(|c| {
        c.engagements.entry((family, tier)).or_insert((0, 0)).0 += 1;
    });
}

/// Completion tick (engaged - completed = mid-flight errors; must be
/// explainable, never silently drifting). ONLY for serving families —
/// posture witnesses tick `tick_witness` and never appear here.
pub fn tick_completed(family: &'static str, tier: &'static str) {
    with_census(|c| {
        c.engagements.entry((family, tier)).or_insert((0, 0)).1 += 1;
    });
}

/// Posture-witness tick: records that a posture was observed (ring
/// armed, fill pooled, memo hit, engine reused, miss class, ...).
/// Witness families have no completion BY CONSTRUCTION — the census
/// surface reports them `posture=witness` with a NULL `completed` so
/// they can never masquerade as mid-flight errors (Michael's ruling
/// 2026-08-19, the B4 false alarm).
pub fn tick_witness(family: &'static str, tier: &'static str) {
    with_census(|c| {
        *c.witnesses.entry((family, tier)).or_insert(0) += 1;
    });
}

/// Test/gate probe: current count for a census key (0 if never seeded).
pub fn refusal_count(key: &str) -> u64 {
    with_census(|c| c.refusals.get(key).map(|r| r.count).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// The SRF builtins (the lanev2 pgrust_lane_coverage pattern verbatim:
// extra-builtin table, reserved-oid range 9000..=9099, catalog untouched
// until scripts/sqe-stat-views.sql runs CREATE FUNCTION ... internal).
// ---------------------------------------------------------------------------

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

/// Documented pgrust-reserved function-oid range for pgrust-native internal
/// builtins. PostgreSQL documents OIDs 9000–9999 as reserved for forks and
/// other projects needing stable assignments (bki.sgml "OID Assignment"),
/// so C 18.3's pg_proc bootstrap has no row here; pgrust claims the first
/// hundred. This matters because `fmgr_core::extra_builtin` matches raw
/// oids on every fmgr_info canonical miss — an oid a catalog row (or a
/// user object, >= 16384) could carry would silently hijack resolution.
/// Enforced by the coverage/stat unit tests (absent from CANONICAL, below
/// the user oid space) and by the e2e's live pg_proc probe; Michael
/// sign-off on the range flagged at integrate (contract, WS-C amendment 5).
/// (Moved here from lanev2/coverage.rs at p72 R-1: the law outlives the
/// lane — sqe's builtins share it, and this module is the surviving home.)
pub const PGRUST_FOID_RANGE: core::ops::RangeInclusive<u32> = 9000..=9099;

pub const PGRUST_SQE_REFUSALS_FOID: ::types_core::Oid = 9010;
pub const PGRUST_SQE_ENGAGEMENTS_FOID: ::types_core::Oid = 9011;

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(::varlena::cstring_to_text(mcx, s.as_bytes())?))
}

/// `pgrust_sqe_refusals() -> setof (family, cause, sqlstate, refusals,
/// last_fp)` — the roadmap view's SRF (Amendment 7: the refusal census
/// IS the build roadmap). All seeded causes appear, zeros included.
pub fn fc_pgrust_sqe_refusals(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgrust_sqe_refusals: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = ::funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 5);
    let rows: Vec<(Cow<'static, str>, String, String, u64, u64)> = with_census(|c| {
        c.refusals
            .iter()
            .map(|(k, r)| {
                (
                    Cow::Borrowed(r.family),
                    k.clone(),
                    r.sqlstate.clone(),
                    r.count,
                    r.last_fp,
                )
            })
            .collect()
    });
    for (family, cause, sqlstate, count, fp) in rows {
        let values = [
            text_datum(mcx, &family)?,
            text_datum(mcx, &cause)?,
            text_datum(mcx, &sqlstate)?,
            Datum::from_i64(count as i64),
            text_datum(mcx, &format!("{fp:016x}"))?,
        ];
        srf.putvalues(&values, &[false; 5])?;
    }
    Ok(srf.finish(fcinfo))
}

/// `pgrust_sqe_engagements() -> setof (family, tier, posture, engaged,
/// completed)`. Serving rows carry `posture='serving'` and a maintained
/// `completed`; posture-witness rows carry `posture='witness'` and a
/// NULL `completed` (they maintain none — never print a counter that is
/// structurally zero, the B4 false-alarm lesson).
pub fn fc_pgrust_sqe_engagements(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgrust_sqe_engagements: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = ::funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 5);
    let (serving, witness): (
        Vec<((&'static str, &'static str), (u64, u64))>,
        Vec<((&'static str, &'static str), u64)>,
    ) = with_census(|c| {
        (
            c.engagements.iter().map(|(k, v)| (*k, *v)).collect(),
            c.witnesses.iter().map(|(k, v)| (*k, *v)).collect(),
        )
    });
    for ((family, tier), (engaged, completed)) in serving {
        let values = [
            text_datum(mcx, family)?,
            text_datum(mcx, tier)?,
            text_datum(mcx, "serving")?,
            Datum::from_i64(engaged as i64),
            Datum::from_i64(completed as i64),
        ];
        srf.putvalues(&values, &[false; 5])?;
    }
    for ((family, tier), engaged) in witness {
        let values = [
            text_datum(mcx, family)?,
            text_datum(mcx, tier)?,
            text_datum(mcx, "witness")?,
            Datum::from_i64(engaged as i64),
            Datum::from_i64(0), // NULL: witnesses maintain no completion
        ];
        srf.putvalues(&values, &[false, false, false, false, true])?;
    }
    Ok(srf.finish(fcinfo))
}

/// The extra-builtin table (installed beside LANEV2_BUILTINS by
/// seams_init; reserved-oid law shared with pgrust_lane_coverage).
pub static SQE_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: PGRUST_SQE_REFUSALS_FOID,
        name: "pgrust_sqe_refusals",
        nargs: 0,
        strict: false,
        retset: true,
        func: fc_pgrust_sqe_refusals,
    },
    FmgrBuiltin {
        foid: PGRUST_SQE_ENGAGEMENTS_FOID,
        name: "pgrust_sqe_engagements",
        nargs: 0,
        strict: false,
        retset: true,
        func: fc_pgrust_sqe_engagements,
    },
    FmgrBuiltin {
        foid: crate::p8census::PGRUST_P8_CENSUS_FOID,
        name: "pgrust_p8_census",
        nargs: 0,
        strict: false,
        retset: true,
        func: crate::p8census::fc_pgrust_p8_census,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_rows_are_seeded_at_variant_grain() {
        with_census(|c| {
            assert!(c.refusals.contains_key("run/epq"));
            assert!(c.refusals.contains_key("tier/engine-disarmed"));
            assert!(c.refusals.contains_key("resource/memory-budget"));
        });
    }

    #[test]
    fn engagement_ledger_ticks() {
        tick_engaged("metaanswer", "A");
        tick_completed("metaanswer", "A");
        with_census(|c| {
            let (e, d) = c.engagements[&("metaanswer", "A")];
            assert!(e >= 1 && d >= 1 && e >= d);
        });
    }

    #[test]
    fn witness_ledger_is_disjoint_from_serving() {
        tick_witness("heap-fill-pooled", "-");
        with_census(|c| {
            assert!(c.witnesses[&("heap-fill-pooled", "-")] >= 1);
            // A witness tick must never fabricate a serving row with a
            // structurally-zero `completed` (the B4 false alarm).
            assert!(!c.engagements.contains_key(&("heap-fill-pooled", "-")));
        });
    }

    #[test]
    fn reserved_oids_are_clear_of_canonical() {
        for b in SQE_BUILTINS {
            assert!((9000..=9099).contains(&b.foid));
            // Claimed siblings: 9000 lane_coverage; 9001/9002/9005 janitor.
            assert!(![9000, 9001, 9002, 9005].contains(&b.foid));
        }
        for &(oid, name, ..) in ::fmgr_core::CANONICAL.iter() {
            for b in SQE_BUILTINS {
                assert_ne!(oid, b.foid, "CANONICAL claims reserved oid {oid}");
                assert_ne!(name, b.name);
            }
        }
    }
}
