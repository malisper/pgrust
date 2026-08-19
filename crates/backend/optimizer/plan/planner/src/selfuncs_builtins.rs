//! fmgr rows for the selectivity estimators (selfuncs.c, like_support.c,
//! geo_selfuncs.c, array/network/ts/rangetypes/multirangetypes_selfuncs.c).
//! The planner calls the native implementations in this crate directly; these
//! rows exist for catalog-dispatch parity (CREATE OPERATOR ... RESTRICT/JOIN,
//! regproc lookups). Bodies are the shared internal-dispatch panic.
use types_fmgr::{fc_internal_dispatch_only, FmgrBuiltin};

const fn sel(foid: types_core::Oid, name: &'static str) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 4, strict: true, retset: false, func: fc_internal_dispatch_only }
}

const fn joinsel(foid: types_core::Oid, name: &'static str) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 5, strict: true, retset: false, func: fc_internal_dispatch_only }
}

pub const SELFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    sel(101, "eqsel"),
    sel(102, "neqsel"),
    sel(103, "scalarltsel"),
    sel(104, "scalargtsel"),
    joinsel(105, "eqjoinsel"),
    joinsel(106, "neqjoinsel"),
    joinsel(107, "scalarltjoinsel"),
    joinsel(108, "scalargtjoinsel"),
    sel(139, "areasel"),
    joinsel(140, "areajoinsel"),
    sel(336, "scalarlesel"),
    sel(337, "scalargesel"),
    joinsel(386, "scalarlejoinsel"),
    joinsel(398, "scalargejoinsel"),
    sel(1300, "positionsel"),
    joinsel(1301, "positionjoinsel"),
    sel(1302, "contsel"),
    joinsel(1303, "contjoinsel"),
    sel(1814, "iclikesel"),
    sel(1815, "icnlikesel"),
    joinsel(1816, "iclikejoinsel"),
    joinsel(1817, "icnlikejoinsel"),
    sel(1818, "regexeqsel"),
    sel(1819, "likesel"),
    sel(1820, "icregexeqsel"),
    sel(1821, "regexnesel"),
    sel(1822, "nlikesel"),
    sel(1823, "icregexnesel"),
    joinsel(1824, "regexeqjoinsel"),
    joinsel(1825, "likejoinsel"),
    joinsel(1826, "icregexeqjoinsel"),
    joinsel(1827, "regexnejoinsel"),
    joinsel(1828, "nlikejoinsel"),
    joinsel(1829, "icregexnejoinsel"),
    sel(3169, "rangesel"),
    sel(3437, "prefixsel"),
    joinsel(3438, "prefixjoinsel"),
    sel(3560, "networksel"),
    joinsel(3561, "networkjoinsel"),
    sel(3686, "tsmatchsel"),
    joinsel(3687, "tsmatchjoinsel"),
    sel(3817, "arraycontsel"),
    joinsel(3818, "arraycontjoinsel"),
    sel(4243, "multirangesel"),
    sel(5040, "matchingsel"),
    joinsel(5041, "matchingjoinsel"),
];

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::SELFUNCS_BUILTINS);
        assert_eq!(super::SELFUNCS_BUILTINS.len(), 46);
    }
}
