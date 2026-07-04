use types_core::Oid;
use types_error::PgResult;
use types_nodes::NodeList;

seam_core::seam!(
    // objectaddress <- opclasscmds cycle via catalog_dependency.
    pub fn get_index_am_oid(amname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn get_opclass_oid<'a, 'mcx>(
        am_id: Oid,
        opclassname: &'a NodeList<'mcx>,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn get_opfamily_oid<'a, 'mcx>(
        am_id: Oid,
        opfamilyname: &'a NodeList<'mcx>,
        missing_ok: bool,
    ) -> PgResult<Oid>
);
