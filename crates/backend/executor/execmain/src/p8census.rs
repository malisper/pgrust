//! P8 Volcano execution census (p8-readiness-map Phase 0(i)): per
//! node-tag init/exec ticks in procnode dispatch, keyed by statement
//! class = CmdType × {epq,spi,cursor} context. Default OFF
//! (`PGRUST_P8_CENSUS=1`/`on`): disarmed cost is one relaxed byte load
//! + compare per dispatch (the fused-arm knob discipline).

use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub use ::execmain_seams::p8ctx::{
    armed, arm_for_tests, ctx_bits, cursor_scope, spi_scope, CtxGuard, CTX_CURSOR,
    CTX_EPQ, CTX_SPI,
};

use ::executils::EStateData;
use ::types_nodes::NodeTag;

pub const NTAG: usize = 40;

pub static TAG_NAMES: [&str; NTAG] = [
    "result",
    "project-set",
    "seq-scan",
    "sample-scan",
    "function-scan",
    "table-func-scan",
    "values-scan",
    "foreign-scan",
    "cte-scan",
    "index-scan",
    "tid-scan",
    "tid-range-scan",
    "index-only-scan",
    "bitmap-heap-scan",
    "bitmap-index-scan",
    "bitmap-and",
    "bitmap-or",
    "material",
    "memoize",
    "sort",
    "incremental-sort",
    "unique",
    "group",
    "limit",
    "lock-rows",
    "agg",
    "window-agg",
    "nest-loop",
    "hash-join",
    "merge-join",
    "append",
    "merge-append",
    "subquery-scan",
    "set-op",
    "recursive-union",
    "work-table-scan",
    "named-tuplestore-scan",
    "modify-table",
    "gather",
    "gather-merge",
];

/// exec_init_node's match set → census index; None = never dispatched.
pub fn tag_ix(tag: NodeTag) -> Option<usize> {
    Some(match tag {
        NodeTag::T_Result => 0,
        NodeTag::T_ProjectSet => 1,
        NodeTag::T_SeqScan => 2,
        NodeTag::T_SampleScan => 3,
        NodeTag::T_FunctionScan => 4,
        NodeTag::T_TableFuncScan => 5,
        NodeTag::T_ValuesScan => 6,
        NodeTag::T_ForeignScan => 7,
        NodeTag::T_CteScan => 8,
        NodeTag::T_IndexScan => 9,
        NodeTag::T_TidScan => 10,
        NodeTag::T_TidRangeScan => 11,
        NodeTag::T_IndexOnlyScan => 12,
        NodeTag::T_BitmapHeapScan => 13,
        NodeTag::T_BitmapIndexScan => 14,
        NodeTag::T_BitmapAnd => 15,
        NodeTag::T_BitmapOr => 16,
        NodeTag::T_Material => 17,
        NodeTag::T_Memoize => 18,
        NodeTag::T_Sort => 19,
        NodeTag::T_IncrementalSort => 20,
        NodeTag::T_Unique => 21,
        NodeTag::T_Group => 22,
        NodeTag::T_Limit => 23,
        NodeTag::T_LockRows => 24,
        NodeTag::T_Agg => 25,
        NodeTag::T_WindowAgg => 26,
        NodeTag::T_NestLoop => 27,
        NodeTag::T_HashJoin => 28,
        NodeTag::T_MergeJoin => 29,
        NodeTag::T_Append => 30,
        NodeTag::T_MergeAppend => 31,
        NodeTag::T_SubqueryScan => 32,
        NodeTag::T_SetOp => 33,
        NodeTag::T_RecursiveUnion => 34,
        NodeTag::T_WorkTableScan => 35,
        NodeTag::T_NamedTuplestoreScan => 36,
        NodeTag::T_ModifyTable => 37,
        NodeTag::T_Gather => 38,
        NodeTag::T_GatherMerge => 39,
        _ => return None,
    })
}

const NCMD: usize = 8;
static CMD_NAMES: [&str; NCMD] = [
    "unknown", "select", "update", "insert", "delete", "merge", "utility", "nothing",
];

const NCTX: usize = 8;

const NCELL: usize = NCMD * NCTX * NTAG;
static INITS: [AtomicU64; NCELL] = [const { AtomicU64::new(0) }; NCELL];
static EXECS: [AtomicU64; NCELL] = [const { AtomicU64::new(0) }; NCELL];

fn cell_ix(tag: usize, estate: &EStateData<'_>) -> usize {
    let cmd = estate
        .es_plannedstmt
        .map_or(0, |ps| ps.commandType as usize)
        & (NCMD - 1);
    let mut ctx = ctx_bits();
    if estate.es_epq_active {
        ctx |= CTX_EPQ;
    }
    (cmd * NCTX + ctx as usize) * NTAG + tag
}

pub fn tick_init(tag: usize, estate: &EStateData<'_>) {
    INITS[cell_ix(tag, estate)].fetch_add(1, Relaxed);
}

pub fn tick_exec(tag: usize, estate: &EStateData<'_>) {
    EXECS[cell_ix(tag, estate)].fetch_add(1, Relaxed);
}

fn class_name(cmd: usize, ctx: usize) -> String {
    let mut s = String::from(CMD_NAMES[cmd]);
    if ctx as u8 & CTX_EPQ != 0 {
        s.push_str("+epq");
    }
    if ctx as u8 & CTX_SPI != 0 {
        s.push_str("+spi");
    }
    if ctx as u8 & CTX_CURSOR != 0 {
        s.push_str("+cursor");
    }
    s
}

// The SRF builtin (stat.rs idiom, SQE_BUILTINS install): nonzero cells
// only — the reader re-seeds the closed universe from node-checklist.tsv.

use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub const PGRUST_P8_CENSUS_FOID: ::types_core::Oid = 9012;

/// `pgrust_p8_census() -> setof (class, node, inits, execs)`.
pub fn fc_pgrust_p8_census(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgrust_p8_census: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = ::funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 4);
    for cell in 0..NCELL {
        let inits = INITS[cell].load(Relaxed);
        let execs = EXECS[cell].load(Relaxed);
        if inits == 0 && execs == 0 {
            continue;
        }
        let cmd = cell / (NCTX * NTAG);
        let ctx = (cell / NTAG) % NCTX;
        let tag = cell % NTAG;
        let class = class_name(cmd, ctx);
        let values = [
            varlena_result(::varlena::cstring_to_text(mcx, class.as_bytes())?),
            varlena_result(::varlena::cstring_to_text(mcx, TAG_NAMES[tag].as_bytes())?),
            Datum::from_i64(inits as i64),
            Datum::from_i64(execs as i64),
        ];
        srf.putvalues(&values, &[false; 4])?;
    }
    Ok(srf.finish(fcinfo))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_names_align_with_tag_ix() {
        assert_eq!(tag_ix(NodeTag::T_Result), Some(0));
        assert_eq!(tag_ix(NodeTag::T_GatherMerge), Some(NTAG - 1));
        assert_eq!(tag_ix(NodeTag::T_Hash), None);
        let mut seen = std::collections::BTreeSet::new();
        for n in TAG_NAMES {
            assert!(seen.insert(n), "duplicate census tag {n}");
        }
    }

    #[test]
    fn ctx_guards_nest_and_restore() {
        arm_for_tests(true);
        assert_eq!(ctx_bits(), 0);
        {
            let _spi = spi_scope();
            assert_eq!(ctx_bits(), CTX_SPI);
            {
                let _cur = cursor_scope();
                assert_eq!(ctx_bits(), CTX_SPI | CTX_CURSOR);
            }
            assert_eq!(ctx_bits(), CTX_SPI);
        }
        assert_eq!(ctx_bits(), 0);
        arm_for_tests(false);
        let g = spi_scope();
        assert!(!g.is_active());
    }

    #[test]
    fn cell_decode_roundtrips() {
        let cell = (3 * NCTX + 5) * NTAG + 37;
        assert_eq!(cell / (NCTX * NTAG), 3);
        assert_eq!((cell / NTAG) % NCTX, 5);
        assert_eq!(cell % NTAG, 37);
        assert_eq!(class_name(3, 5), "insert+epq+cursor");
        assert_eq!(class_name(1, 0), "select");
    }

    #[test]
    fn reserved_oid_in_range() {
        assert!(crate::sqeshell::stat::PGRUST_FOID_RANGE.contains(&PGRUST_P8_CENSUS_FOID));
    }
}
