//! Seam declarations for the `backend-catalog-pg-class` unit
//! (`catalog/pg_class.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `errdetail_relkind_not_supported(relkind)` (pg_class.c): the
    /// translated "Operation is not supported for relations of kind X."
    /// detail line attached to wrong-relkind errors. C appends it to the
    /// error being built and returns 0; the owned model returns the detail
    /// string for the caller to attach. `Err` carries OOM from building the
    /// message.
    pub fn errdetail_relkind_not_supported(
        relkind: u8,
    ) -> types_error::PgResult<std::string::String>
);
