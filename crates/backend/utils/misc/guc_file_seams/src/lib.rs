seam_core::seam!(
    // ProcessConfigFile(context) — config parse/apply can ereport(ERROR).
    pub fn process_config_file(context: types_guc::GucContext) -> types_error::PgResult<()>
);
