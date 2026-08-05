//! `pgrust_lane_coverage` — the single-executor migration's progress
//! instrument (docs/design/single-executor-migration.md §0.2: "% of
//! dispatched plan nodes executing on lanes"), WS-C increment 1.
//!
//! A materialized-SRF internal builtin over the lane-v2 engagement counters
//! (stats.rs), the M5 router counters (router.rs), and the living coverage
//! matrix (crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv). Flat, TSV-aligned schema so the
//! fleet dump vocabulary and the view never diverge:
//!
//!   (surface text, class text, counter text, detail text, value bigint)
//!
//!   surface = "meta"   — arming state (class="armed"; an unarmed read is
//!                        self-describing instead of silently zero)
//!           | "lane"   — every ShapeClass × owned (zeros kept) + every
//!                        nonzero (class, reason) refusal
//!           | "router" — the query routing counters + every arm × counter
//!                        cell + the arms' refusal taxonomy
//!           | "matrix" — the m5-coverage.tsv rows (status/route_to/
//!                        probe_key), schema-pinned by router.rs's
//!                        coverage_matrix_is_consistent test
//!
//! Rows are ENUMERATED from the classifier tables (ShapeClass::ALL,
//! RefuseReason::from_index, ArmClass::ALL × ArmCounter::ALL) — the source
//! of truth, never a hand-written list (integration contract R-VOCAB).
//!
//! Counters are process-cumulative (backends are threads of one process:
//! totals across all sessions since server start) and opt-in-armed —
//! `PGRUST_LANE_V2_STATS=<dir>` (the fleet dump switch) or the new
//! no-dump-dir `PGRUST_LANE_V2_COVERAGE=1`. Reads are relaxed atomic loads;
//! never blocks a query.
//!
//! The catalog is NEVER touched by default: scripts/lane-coverage-view.sql
//! creates the function (LANGUAGE internal AS 'pgrust_lane_coverage') and
//! the view on demand — no pg_proc/pg_class delta until a user runs it, and
//! C errors identically on the unknown internal name.

use std::borrow::Cow;

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};

use super::{router, stats};

/// Documented pgrust-reserved function-oid range for pgrust-native internal
/// builtins. PostgreSQL documents OIDs 9000–9999 as reserved for forks and
/// other projects needing stable assignments (bki.sgml "OID Assignment"),
/// so C 18.3's pg_proc bootstrap has no row here; pgrust claims the first
/// hundred. This matters because `fmgr_core::extra_builtin` matches raw
/// oids on every fmgr_info canonical miss — an oid a catalog row (or a
/// user object, >= 16384) could carry would silently hijack resolution.
/// Enforced by the unit test below (absent from CANONICAL, below the user
/// oid space) and by the e2e's live pg_proc probe; Michael sign-off on the
/// range flagged at integrate (contract, WS-C amendment 5).
pub const PGRUST_FOID_RANGE: core::ops::RangeInclusive<u32> = 9000..=9099;

/// pg_proc-style oid for `pgrust_lane_coverage` (the first reserved slot).
pub const PGRUST_LANE_COVERAGE_FOID: ::types_core::Oid = 9000;

/// One row of the pgrust_lane_coverage SRF (module doc for the vocabulary).
pub struct CoverageRow {
    pub surface: &'static str,
    pub class: Cow<'static, str>,
    pub counter: &'static str,
    pub detail: Cow<'static, str>,
    pub value: i64,
}

/// Snapshot of the process-global lane + router counters and the embedded
/// coverage matrix. Total for the process (all backend threads) since
/// server start; relaxed loads, never blocks a query.
pub fn coverage_snapshot() -> Vec<CoverageRow> {
    // Capacity from the vocabulary constants (also what pins them live for
    // the derived-count tests): meta + every class's owned row + every
    // router cell; refusals/matrix rows grow past it as needed.
    let mut rows =
        Vec::with_capacity(1 + stats::n_classes() + router::n_arm_counter_cells());
    // Arming state first: an unarmed read reports armed=0 up front instead
    // of a wall of silent zeros.
    rows.push(CoverageRow {
        surface: "meta",
        class: Cow::Borrowed("armed"),
        counter: "armed",
        detail: Cow::Borrowed(""),
        value: stats::armed() as i64,
    });
    for (class, n) in stats::owned_snapshot() {
        rows.push(CoverageRow {
            surface: "lane",
            class: Cow::Borrowed(class.name()),
            counter: "owned",
            detail: Cow::Borrowed(""),
            value: n as i64,
        });
    }
    for (class, reason, n) in stats::refused_snapshot() {
        rows.push(CoverageRow {
            surface: "lane",
            class: Cow::Borrowed(class.name()),
            counter: "refused",
            detail: Cow::Borrowed(reason.name()),
            value: n as i64,
        });
    }
    for (class, counter, n) in router::arm_counter_snapshot() {
        rows.push(CoverageRow {
            surface: "router",
            class: Cow::Borrowed(class),
            counter,
            detail: Cow::Borrowed(""),
            value: n as i64,
        });
    }
    for (arm, reason, n) in router::refused_snapshot() {
        rows.push(CoverageRow {
            surface: "router",
            class: Cow::Borrowed(arm),
            counter: "refused",
            detail: Cow::Borrowed(reason),
            value: n as i64,
        });
    }
    rows.extend(matrix_rows());
    rows
}

// The same file router.rs's coverage_matrix_is_consistent test pins (header
// schema, closed vocabularies), embedded at the equivalent relative path.
const M5_COVERAGE_TSV: &str = include_str!("../../../../../../crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv");

/// The living coverage-matrix rows: one row per data line, status in
/// `detail` alongside route_to/probe_key. `value` = 1 (presence).
fn matrix_rows() -> Vec<CoverageRow> {
    let mut out = Vec::new();
    for line in M5_COVERAGE_TSV.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols[0] == "class" {
            continue; // header (schema pinned by the router.rs test)
        }
        debug_assert_eq!(cols.len(), 9, "m5-coverage.tsv row width: {line}");
        if cols.len() < 8 {
            continue; // never a query error over a malformed doc row
        }
        let (class, status, route_to, probe_key) = (cols[0], cols[5], cols[6], cols[7]);
        out.push(CoverageRow {
            surface: "matrix",
            class: Cow::Owned(class.to_string()),
            counter: "status",
            detail: Cow::Owned(format!("{status};route_to={route_to};probe_key={probe_key}")),
            value: 1,
        });
    }
    out
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(::varlena::cstring_to_text(mcx, s.as_bytes())?))
}

/// fmgr builtin: `pgrust_lane_coverage()` — materialized SRF of the
/// coverage snapshot (guc_funcs fc_show_all_settings pattern). The row type
/// comes from the CREATE FUNCTION's OUT parameters
/// (scripts/lane-coverage-view.sql); NOT in any bootstrapped catalog.
pub fn fc_pgrust_lane_coverage(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgrust_lane_coverage: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = ::funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 5);
    for row in coverage_snapshot() {
        let values = [
            text_datum(mcx, row.surface)?,
            text_datum(mcx, &row.class)?,
            text_datum(mcx, row.counter)?,
            text_datum(mcx, &row.detail)?,
            Datum::from_i64(row.value),
        ];
        srf.putvalues(&values, &[false; 5])?;
    }
    Ok(srf.finish(fcinfo))
}

/// The extra-builtin table seams_init appends to EXTRA_BUILTINS. The name
/// resolves through `fmgr_core::fmgr_lookup_by_name` (extended to search
/// extra tables) for CREATE FUNCTION ... LANGUAGE internal; the oid is
/// consulted only on fmgr_info canonical misses (reserved-range law above).
pub static LANEV2_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: PGRUST_LANE_COVERAGE_FOID,
    name: "pgrust_lane_coverage",
    nargs: 0,
    strict: false,
    retset: true,
    func: fc_pgrust_lane_coverage,
}];

#[cfg(test)]
mod tests {
    use super::*;

    /// The view is generated from the classifier tables: every ShapeClass
    /// has exactly one owned row, every router arm × counter has exactly one
    /// cell, and the reason vocabulary is complete and distinct — all counts
    /// derived from the stats.rs/router.rs constants, never literals
    /// (integration contract R-VOCAB / WS-C amendment 8).
    #[test]
    fn snapshot_enumerates_the_classifier_vocabulary() {
        let rows = coverage_snapshot();
        let owned: Vec<_> = rows
            .iter()
            .filter(|r| r.surface == "lane" && r.counter == "owned")
            .collect();
        assert_eq!(owned.len(), stats::n_classes());
        let mut class_names: Vec<_> = owned.iter().map(|r| r.class.clone()).collect();
        class_names.sort();
        class_names.dedup();
        assert_eq!(class_names.len(), stats::n_classes(), "class names must be distinct");

        let mut reasons = stats::reason_names();
        assert_eq!(reasons.len(), stats::n_reasons());
        reasons.sort();
        reasons.dedup();
        assert_eq!(reasons.len(), stats::n_reasons(), "reason names must be distinct");

        let router_cells = rows
            .iter()
            .filter(|r| r.surface == "router" && r.class != "query" && r.counter != "refused")
            .count();
        assert_eq!(router_cells, router::n_arm_counter_cells());

        assert!(rows.iter().any(|r| r.surface == "meta" && r.class == "armed"));
        assert!(
            rows.iter().filter(|r| r.surface == "matrix").count() >= 1,
            "the embedded m5-coverage.tsv must contribute matrix rows"
        );
    }

    /// Reserved-oid law (contract, WS-C amendment 5): the coverage builtin's
    /// oid sits inside the documented pgrust range, which is verifiably
    /// absent from the canonical C 18.3 builtin table and below the user oid
    /// space (FirstNormalObjectId). `install_extra_builtins` additionally
    /// asserts no live-canonical collision at startup, and the e2e probes
    /// the initdb'd pg_proc for the whole range.
    #[test]
    fn reserved_oid_is_clear_of_canonical() {
        for b in LANEV2_BUILTINS {
            assert!(PGRUST_FOID_RANGE.contains(&b.foid), "{} outside the range", b.foid);
            assert!(b.foid < 16384, "user oid space starts at FirstNormalObjectId");
        }
        // The whole reserved range is clear of CANONICAL (oids AND names).
        for &(oid, name, ..) in ::fmgr_core::CANONICAL.iter() {
            assert!(!PGRUST_FOID_RANGE.contains(&oid), "CANONICAL claims reserved oid {oid}");
            for b in LANEV2_BUILTINS {
                assert_ne!(name, b.name, "CANONICAL claims the name {name}");
            }
        }
    }
}
