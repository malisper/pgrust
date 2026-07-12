// Lane qual shape vocabulary — the STRUCTURAL clause forms a scan qual's
// compiled step stream decodes into. The vocabulary AND the extraction
// walker (`execexpr::lane_scan_qual`, ExprState -> LaneQualShape) live in
// execexpr, next to the step stream they decode; this module re-exports the
// types so translate (and its tests, which construct shapes directly) keep
// one name for them. The fn-oid legality gate (which comparators are
// in-core non-erroring) stays in this crate's translate module, so the
// shape vocabulary carries oids raw.
pub use ::execexpr::{
    LaneBoolTest, LaneClause, LaneCmpClause, LaneCmpRhs, LaneQualShape, LaneSuffix,
};
