use types_error::PgResult;

seam_core::seam!(
    pub fn new_guc_nest_level() -> i32
);

seam_core::seam!(
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> PgResult<()>
);

seam_core::seam!(
    // AtStart_GUC (guc.c): sanity-reset the GUC nest level to 1.
    pub fn at_start_guc()
);

seam_core::seam!(
    // SetConfigOption(name, value, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT) (guc.c);
    // miscinit's SetOuterUserId keeps the is_superuser GUC in sync through it.
    pub fn set_config_option_internal_dynamic_default(name: &str, value: &str) -> PgResult<()>
);

seam_core::seam!(
    // GUC_check_errdetail(...) (utils/misc/guc.c): record the errdetail a
    // check hook reports before returning false.
    pub fn guc_check_errdetail(detail: String)
);

seam_core::seam!(
    // ProcessConfigFileInternal(context, applySettings, elevel) (guc.c); the
    // guc-file.l wrapper reaches back across the guc <-> guc-file cycle.
    pub fn process_config_file_internal(
        context: types_guc::GucContext,
        apply_settings: bool,
        elevel: types_error::ErrorLevel,
    ) -> PgResult<()>
);
