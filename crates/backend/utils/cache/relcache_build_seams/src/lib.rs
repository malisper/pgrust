// RelationBuildDesc's catalog-scan half; installed by the future build unit.
use std::rc::Rc;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_rel::{FormData_pg_class, FormData_pg_index, RdOptions};
use types_tuple::TupleDescData;

// options: RelationParseRelOptions folds into the installer (parsed form only).
pub struct ScannedPgClass {
    pub form: FormData_pg_class,
    pub options: Option<RdOptions>,
}

pub struct IndexAccessInfo {
    pub index: FormData_pg_index<'static>,
    pub opcintype: PgVec<'static, Oid>,
    pub opfamily: PgVec<'static, Oid>,
    pub indoption: PgVec<'static, i16>,
    pub indcollation: PgVec<'static, Oid>,
}

seam_core::seam!(
    pub fn scan_pg_relation(
        target_rel_id: Oid,
        index_ok: bool,
        force_non_historic: bool,
    ) -> PgResult<Option<ScannedPgClass>>
);

seam_core::seam!(
    pub fn relation_build_tuple_desc(
        mcx: Mcx<'static>,
        relid: Oid,
        form: &FormData_pg_class,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    pub fn relation_init_index_access_info(
        mcx: Mcx<'static>,
        relid: Oid,
        form: &FormData_pg_class,
    ) -> PgResult<IndexAccessInfo>
);
