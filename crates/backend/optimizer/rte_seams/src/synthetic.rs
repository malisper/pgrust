//! Synthetic RTE-table fixture for unit-testing the optimizer (relnode /
//! allpaths / costsize) against a hand-built range-table index space, BEFORE
//! the planner-entry crate exists to install the real seams.
//!
//! The real install (added later by `subquery_planner` / `planner.c`) closes
//! over the `&'mcx Query` being planned and resolves each `RangeTblEntryId`
//! to the owned `RangeTblEntry<'mcx>`. This fixture mirrors that contract with
//! an owned, in-memory table indexed 1-based by RT index — exactly what the
//! optimizer reads through the seams — so the seam call paths are exercised
//! end-to-end without the parser.
//!
//! The `seam!` slots are `fn` pointers (no captured state), so the fixture
//! stores the hand-built table in a process-global `OnceLock` that the seam
//! closures read by `rti`. Because both the table and the seam slots are
//! install-once (`OnceLock`), the whole installation is idempotent within a
//! test binary: the first call installs, later calls are no-ops, and a mismatched
//! re-install panics (the same loud failure `seam!` gives on double-install).

use crate::*;
use alloc::string::String;
use alloc::vec::Vec;
use std::sync::OnceLock;
use pathnodes::{QueryId, RangeTblEntryId};

/// One synthetic range-table entry: the projected fields the optimizer reads,
/// mirroring `RangeTblEntry` field-for-field for the projection surface above.
#[derive(Clone, Debug)]
pub struct SyntheticRte {
    pub rtekind: RTEKind,
    pub relkind: i8,
    pub relid: types_core::primitive::Oid,
    pub inh: bool,
    /// `subquery` presence + handle (the owned Query lives elsewhere).
    pub subquery: Option<QueryId>,
    pub functions_len: i32,
    pub funcordinality: bool,
    pub ctename: String,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub has_tablesample: bool,
    pub has_tablefunc: bool,
    pub security_barrier: bool,
    pub values_lists_len: i32,
    pub lateral: bool,
    pub jointype: JoinType,
    pub enrtuples: f64,
}

impl Default for SyntheticRte {
    fn default() -> Self {
        SyntheticRte {
            rtekind: ::pathnodes::RTE_RELATION,
            relkind: 0,
            relid: types_core::primitive::Oid::default(),
            inh: false,
            subquery: None,
            functions_len: 0,
            funcordinality: false,
            ctename: String::new(),
            ctelevelsup: 0,
            self_reference: false,
            has_tablesample: false,
            has_tablefunc: false,
            security_barrier: false,
            values_lists_len: 0,
            lateral: false,
            jointype: ::pathnodes::JOIN_INNER,
            enrtuples: 0.0,
        }
    }
}

/// The whole synthetic range table, indexed 1-based by RT index (mirrors the
/// 1-based `simple_rte_array` / `parse->rtable`).
#[derive(Clone, Debug, Default)]
pub struct SyntheticRtable {
    /// `entries[0]` is RT index 1 (the C list is 1-based; slot 0 of
    /// `simple_rte_array` is always NULL).
    pub entries: Vec<SyntheticRte>,
    /// `list_length(parse->rteperminfos)`.
    pub rteperminfos_len: i32,
}

static TABLE: OnceLock<SyntheticRtable> = OnceLock::new();

fn table() -> &'static SyntheticRtable {
    TABLE
        .get()
        .expect("synthetic RTE table not installed: call install_synthetic_rte_table() first")
}

/// `rti` is 1-based (the C range-table index); `entries[rti - 1]` is the RTE.
fn rte(rti: Index) -> &'static SyntheticRte {
    let t = table();
    let i = (rti as usize)
        .checked_sub(1)
        .expect("RT index is 1-based; rti == 0 is the NULL slot");
    t.entries
        .get(i)
        .unwrap_or_else(|| panic!("synthetic RT index {rti} out of range (len {})", t.entries.len()))
}

/// Install the synthetic range table AND wire every `rte_*` / `parse_*` seam to
/// read from it. Idempotent within a process: the first call installs, repeats
/// with an equal table are no-ops, and the underlying `OnceLock`s reject a
/// genuinely conflicting re-install (matching `seam!`'s double-install panic).
///
/// Consumer crates (relnode/allpaths/costsize) call this from their own
/// `#[cfg(test)]` fixtures to exercise the optimizer against a hand-built RT
/// space while the planner-entry owner is unported.
pub fn install_synthetic_rte_table(t: SyntheticRtable) {
    // Install the backing table first so the closures have data to read.
    let _ = TABLE.set(t);

    // Install every seam exactly once. `OnceLock::set` (inside `seam!`) returns
    // Err on a second install; we tolerate that so multiple test fixtures in one
    // binary can each call this helper without tripping the double-install panic.
    install_once();
}

fn install_once() {
    // A private gate so the per-seam `::set` calls run exactly once even if the
    // helper is invoked from several tests in the same binary.
    static INSTALLED: OnceLock<()> = OnceLock::new();
    if INSTALLED.set(()).is_err() {
        return;
    }

    rte_rtekind::set(|_run, _root, rti| rte(rti).rtekind);
    rte_relkind::set(|_run, _root, rti| rte(rti).relkind);
    rte_relid::set(|_run, _root, rti| rte(rti).relid);
    rte_inh::set(|_run, _root, rti| rte(rti).inh);
    rte_subquery::set(|_run, _root, rti| rte(rti).subquery);
    rte_functions_len::set(|_run, _root, rti| rte(rti).functions_len);
    rte_funcordinality::set(|_run, _root, rti| rte(rti).funcordinality);
    rte_ctename::set(|_run, _root, rti| rte(rti).ctename.clone());
    rte_ctelevelsup::set(|_run, _root, rti| rte(rti).ctelevelsup);
    rte_self_reference::set(|_run, _root, rti| rte(rti).self_reference);
    rte_has_tablesample::set(|_run, _root, rti| rte(rti).has_tablesample);
    rte_has_tablefunc::set(|_run, _root, rti| rte(rti).has_tablefunc);
    rte_security_barrier::set(|_run, _root, rti| rte(rti).security_barrier);
    rte_values_lists_len::set(|_run, _root, rti| rte(rti).values_lists_len);
    rte_lateral::set(|_run, _root, rti| rte(rti).lateral);
    rte_jointype::set(|_run, _root, rti| rte(rti).jointype);
    rte_enrtuples::set(|_run, _root, rti| rte(rti).enrtuples);

    parse_rtable_len::set(|_run, _root| table().entries.len() as i32);
    parse_rte::set(|_run, _root, rti| {
        // The flat-rtable handle is just the 1-based RT index (mirrors the C
        // `list_nth(parse->rtable, rti-1)` identity within this synthetic space).
        let _ = rte(rti); // bounds-check
        RangeTblEntryId(rti)
    });
    parse_rteperminfos_len::set(|_run, _root| table().rteperminfos_len);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::pathnodes::planner_run::PlannerRun;
    use ::pathnodes::PlannerInfo;

    #[test]
    fn synthetic_projections_round_trip() {
        let mut a = SyntheticRte::default();
        a.rtekind = ::pathnodes::RTE_RELATION;
        a.relid = 1259u32;
        a.relkind = b'r' as i8;
        a.inh = true;

        let mut b = SyntheticRte::default();
        b.rtekind = 1; // RTE_SUBQUERY
        b.subquery = Some(QueryId(7));
        b.security_barrier = true;
        b.lateral = true;

        let mut c = SyntheticRte::default();
        c.rtekind = 3; // RTE_FUNCTION
        c.functions_len = 2;
        c.funcordinality = true;

        install_synthetic_rte_table(SyntheticRtable {
            entries: alloc::vec![a, b, c],
            rteperminfos_len: 1,
        });

        let root = PlannerInfo::default();
        // The synthetic install reads from its own global table and ignores the
        // `run` parameter; an empty run satisfies the (re-signed) seam contract.
        let cx = mcx::MemoryContext::new("rte-seams-synth-test");
        let run = PlannerRun::new(cx.mcx());

        // RT index 1 (relation).
        assert_eq!(rte_rtekind::call(&run, &root, 1), ::pathnodes::RTE_RELATION);
        assert_eq!(rte_relid::call(&run, &root, 1), 1259);
        assert_eq!(rte_relkind::call(&run, &root, 1), b'r' as i8);
        assert!(rte_inh::call(&run, &root, 1));
        assert_eq!(rte_subquery::call(&run, &root, 1), None);

        // RT index 2 (subquery).
        assert_eq!(rte_subquery::call(&run, &root, 2), Some(QueryId(7)));
        assert!(rte_security_barrier::call(&run, &root, 2));
        assert!(rte_lateral::call(&run, &root, 2));

        // RT index 3 (function).
        assert_eq!(rte_functions_len::call(&run, &root, 3), 2);
        assert!(rte_funcordinality::call(&run, &root, 3));

        // Query-level.
        assert_eq!(parse_rtable_len::call(&run, &root), 3);
        assert_eq!(parse_rte::call(&run, &root, 2), RangeTblEntryId(2));
        assert_eq!(parse_rteperminfos_len::call(&run, &root), 1);
    }
}
