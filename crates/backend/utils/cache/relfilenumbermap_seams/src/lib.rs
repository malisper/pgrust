seam_core::seam!(
    pub fn relid_by_relfilenumber(
        reltablespace: types_core::Oid,
        relfilenumber: types_core::RelFileNumber,
    ) -> types_error::PgResult<types_core::Oid>
);
