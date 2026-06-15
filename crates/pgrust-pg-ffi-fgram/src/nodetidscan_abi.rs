//! `#[repr(C)]` ABI for `nodeTidscan.c` (the TID-scan executor node).
//!
//! The TID-scan node is ported in-crate (`backend-executor-nodeTidscan`), so
//! its state node is a complete, address-stable `#[repr(C)]` struct laid out
//! exactly like the C `TidScanState` (execnodes.h). The `TidScan` plan node and
//! the TID-yielding qual expression nodes (`Var`, `OpExpr`, `ScalarArrayOpExpr`,
//! `CurrentOfExpr`) it navigates are spelled out here too.
//!
//! The embedded `ScanState`/`PlanState`/`Scan` heads reuse the shared
//! [`crate::ScanStateData`] / [`crate::PlanStateData`] / [`crate::Scan`] layouts
//! defined in `execnodes`.

use core::ffi::{c_char, c_int};

use crate::{
    AttrNumber, Bitmapset, Index, ItemPointerData, List, NodeTag, Oid, ParseLoc, PlanStateData,
    Scan, ScanStateData,
};

/// NodeTag for `Var` (primnodes.h / nodetags.h order).
pub const T_Var: NodeTag = 6;
/// NodeTag for `OpExpr`.
pub const T_OpExpr: NodeTag = 17;
/// NodeTag for `ScalarArrayOpExpr`.
pub const T_ScalarArrayOpExpr: NodeTag = 20;
/// NodeTag for `CurrentOfExpr`.
pub const T_CurrentOfExpr: NodeTag = 58;

/// `Expr` — the abstract superclass header carried by every primitive
/// expression node (`primnodes.h`). Its only member is the `NodeTag`. Named
/// `PrimExpr` here to avoid clashing with the executor module's `Expr`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrimExpr {
    /// `NodeTag type`
    pub type_: NodeTag,
}

/// `VarReturningType` — `Var.varreturningtype` discriminant (primnodes.h).
pub type VarReturningType = c_int;

/// `Var` (primnodes.h) — a reference to a table column or system attribute. The
/// TID-scan node only reads `xpr.type_` and `varattno` to recognise the CTID
/// pseudo-column, but the whole struct is spelled out for layout fidelity.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Var {
    pub xpr: PrimExpr,
    pub varno: c_int,
    pub varattno: AttrNumber,
    pub vartype: Oid,
    pub vartypmod: i32,
    pub varcollid: Oid,
    pub varnullingrels: *mut Bitmapset,
    pub varlevelsup: Index,
    pub varreturningtype: VarReturningType,
    pub varnosyn: Index,
    pub varattnosyn: AttrNumber,
    pub location: ParseLoc,
}

/// `OpExpr` (primnodes.h) — an operator-application expression. The TID-scan
/// node reads `xpr.type_` and `args` (to extract the left/right operands).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OpExpr {
    pub xpr: PrimExpr,
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

/// `ScalarArrayOpExpr` (primnodes.h) — `scalar op ANY/ALL (array)`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ScalarArrayOpExpr {
    pub xpr: PrimExpr,
    pub opno: Oid,
    pub opfuncid: Oid,
    pub hashfuncid: Oid,
    pub negfuncid: Oid,
    pub useOr: bool,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

/// `CurrentOfExpr` (primnodes.h) — the `WHERE CURRENT OF cursor` expression.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CurrentOfExpr {
    pub xpr: PrimExpr,
    pub cvarno: Index,
    pub cursor_name: *mut c_char,
    pub cursor_param: c_int,
}

/// `TidScan` plan node (plannodes.h). Embeds the abstract `Scan` base (which
/// itself embeds `Plan`) and adds the list of `CTID = something` quals.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TidScan {
    /// `Scan scan` head — embeds `Plan` then `Index scanrelid`.
    pub scan: Scan,
    /// `List *tidquals` — qual(s) involving `CTID = something`.
    pub tidquals: *mut List,
}

/// `TidScanState` (execnodes.h) — faithful `#[repr(C)]` ABI struct for the
/// TID-scan executor node. The leading `ss` field's first member is a
/// `NodeTag`, so a `*mut TidScanState` is also a valid `Node *`.
///
/// `tss_tidexprs` is a `List *` of crate-internal `TidExpr` records, and
/// `tss_TidList` is a heap array of `ItemPointerData`; both are owned and laid
/// out by the node crate (idiomatic internal state reached through these raw
/// pointers).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TidScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `List *tss_tidexprs` — compiled TID-yielding subexpressions.
    pub tss_tidexprs: *mut List,
    /// `bool tss_isCurrentOf` — true if this is a `WHERE CURRENT OF` scan.
    pub tss_isCurrentOf: bool,
    /// `int tss_NumTids` — number of TIDs in `tss_TidList`.
    pub tss_NumTids: c_int,
    /// `int tss_TidPtr` — index of the current TID, or `-1` before the scan.
    pub tss_TidPtr: c_int,
    /// `ItemPointerData *tss_TidList` — sorted, de-duplicated TID array.
    pub tss_TidList: *mut ItemPointerData,
}

impl TidScanState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut TidScanState` can be navigated as the C struct, and the plan/expr nodes
// match the PostgreSQL 18.3 ABI.
const _: () = {
    assert!(core::mem::offset_of!(TidScanState, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(core::mem::offset_of!(PlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(TidScan, scan) == 0);
    assert!(core::mem::offset_of!(Scan, plan) == 0);
    assert!(core::mem::size_of::<PrimExpr>() == 4);
    // Var: xpr(4) varno(4) varattno(2)+pad(2) vartype(4) ... location at 48.
    assert!(core::mem::offset_of!(Var, varattno) == 8);
    assert!(core::mem::offset_of!(Var, location) == 48);
    assert!(core::mem::size_of::<Var>() == 56);
    // OpExpr: args at 32, location at 40, size 48.
    assert!(core::mem::offset_of!(OpExpr, args) == 32);
    assert!(core::mem::size_of::<OpExpr>() == 48);
    // ScalarArrayOpExpr: 4 Oids + useOr(bool) + inputcollid → args at 32, size 48.
    assert!(core::mem::offset_of!(ScalarArrayOpExpr, args) == 32);
    assert!(core::mem::size_of::<ScalarArrayOpExpr>() == 48);
    // CurrentOfExpr: cursor_name at 8, cursor_param at 16, size 24.
    assert!(core::mem::offset_of!(CurrentOfExpr, cursor_name) == 8);
    assert!(core::mem::size_of::<CurrentOfExpr>() == 24);
};
