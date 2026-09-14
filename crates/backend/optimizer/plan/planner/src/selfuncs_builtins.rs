//! fmgr rows for the selectivity estimators (selfuncs.c, like_support.c,
//! geo_selfuncs.c, array/network/ts/rangetypes/multirangetypes_selfuncs.c).
//! The planner calls the native implementations in this crate directly; these
//! rows exist for catalog-dispatch parity (CREATE OPERATOR ... RESTRICT/JOIN,
//! regproc lookups). Bodies are the shared internal-dispatch panic.
use types_fmgr::{fc_internal_dispatch_only, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};
use types_error::PgResult;

const fn sel(foid: types_core::Oid, name: &'static str) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 4, strict: true, retset: false, func: fc_internal_dispatch_only }
}

const fn joinsel(foid: types_core::Oid, name: &'static str) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 5, strict: true, retset: false, func: fc_internal_dispatch_only }
}

// geo_selfuncs.c:48-95: the six geometric estimators ignore their arguments
// and return constants, so a LANGUAGE internal alias call returns them too.
macro_rules! const_sel {
    ($name:ident, $v:expr) => {
        fn $name(
            _flinfo: Option<&mut FmgrInfo>,
            _fcinfo: &mut FunctionCallInfoBaseData,
        ) -> PgResult<datum::Datum> {
            Ok(datum::Datum::from_f64($v))
        }
    };
}
const_sel!(areasel, 0.005);
const_sel!(areajoinsel, 0.005);
const_sel!(positionsel, 0.1);
const_sel!(positionjoinsel, 0.1);
const_sel!(contsel, 0.001);
const_sel!(contjoinsel, 0.001);

const fn geosel(foid: types_core::Oid, name: &'static str, func: types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 4, strict: true, retset: false, func }
}

const fn geojoinsel(foid: types_core::Oid, name: &'static str, func: types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 5, strict: true, retset: false, func }
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
    geosel(139, "areasel", areasel),
    geojoinsel(140, "areajoinsel", areajoinsel),
    sel(336, "scalarlesel"),
    sel(337, "scalargesel"),
    joinsel(386, "scalarlejoinsel"),
    joinsel(398, "scalargejoinsel"),
    geosel(1300, "positionsel", positionsel),
    geojoinsel(1301, "positionjoinsel", positionjoinsel),
    geosel(1302, "contsel", contsel),
    geojoinsel(1303, "contjoinsel", contjoinsel),
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

    #[test]
    fn geo_estimators_return_c_constants() {
        let want = [(139, 0.005), (140, 0.005), (1300, 0.1), (1301, 0.1), (1302, 0.001), (1303, 0.001)];
        for (foid, v) in want {
            let row = super::SELFUNCS_BUILTINS.iter().find(|r| r.foid == foid).unwrap();
            let mut fcinfo = types_fmgr::LocalFcinfo::<0>::fresh(0);
            let d = (row.func)(None, &mut fcinfo).unwrap();
            assert!(!fcinfo.isnull);
            assert_eq!(d.as_f64(), v, "{}", row.name);
        }
    }
}
