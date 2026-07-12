// Lane qual shape vocabulary (harvested from the old branch's execexpr
// lane_scan_qual surface). These are the STRUCTURAL clause forms a scan
// qual's compiled step stream decodes into; the extraction walker itself
// (ExprState -> LaneQualShape) lands with the executor-wiring tranche —
// until then callers (and tests) construct shapes directly. The fn-oid
// legality gate (which comparators are in-core non-erroring) stays in this
// crate's translate module, so the shape vocabulary carries oids raw.
use types_core::Oid;

/// One implicitly-ANDed comparison clause of a scan qual. `col` is the Var
/// feeding arg0 (or, for const clauses with the Var at arg1, the sole Var
/// with `commuted` set). The comparator's fn oid is carried raw: the
/// legality gate (which oids are in-core non-erroring int comparators)
/// lives in translate, so its vocabulary can grow without touching the
/// shape vocabulary.
pub enum LaneCmpRhs {
    Const(::datum::Datum),
    Col(u16),
}

pub struct LaneCmpClause {
    pub col: u16,
    pub fn_oid: Oid,
    pub commuted: bool,
    /// The call's input collation (fcinfo fncollation) — collation-sensitive
    /// predicates (text eq/LIKE over dict lanes) re-evaluate with it.
    pub collation: Oid,
    pub rhs: LaneCmpRhs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneBoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
}

pub enum LaneClause {
    Cmp(LaneCmpClause),
    /// col IS [NOT] NULL — NullTest is non-strict, non-erroring, no fn call.
    NullTest { col: u16, want_null: bool },
    /// Bare boolean Var clause (`WHERE boolcol`): the Var writes the result
    /// slot and Qual tests it directly (NULL or false fails).
    BoolVar { col: u16 },
    /// col IS [NOT] TRUE/FALSE — BooleanTest is non-strict, non-erroring.
    BoolTest { col: u16, kind: LaneBoolTest },
    /// col <op> ANY(non-null Const array): useOr SAOP over a strict
    /// comparator, elements decoded at classify time (flat byval arrays
    /// only, structurally capped). NULL elements are kept: they flip a miss
    /// to NULL, which a Qual fails exactly like false, so the evaluator may
    /// skip them — the shape stays exact for the census.
    InList { col: u16, fn_oid: Oid, elems: Vec<::datum::NullableDatum> },
}

/// Trailing clauses the walker could not decode (the hybrid split's per-row
/// suffix). `Calls` carries every call fn oid found there so translate can
/// gate on volatility; `Opaque` = the suffix holds step kinds the collector
/// does not enumerate (treated as volatile downstream, fail-closed).
pub enum LaneSuffix {
    None,
    Calls(Vec<Oid>),
    Opaque,
}

pub struct LaneQualShape {
    pub clauses: Vec<LaneClause>,
    /// Parsed clauses' columns (translate recomputes over its whitelisted
    /// prefix; suffix columns deform lazily from the stored tuple on the
    /// per-row requal).
    pub max_attnum: u16,
    pub suffix: LaneSuffix,
}
