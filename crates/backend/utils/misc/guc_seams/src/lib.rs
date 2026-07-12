use types_error::PgResult;

seam_core::seam!(
    pub fn new_guc_nest_level() -> i32
);

seam_core::seam!(
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> PgResult<()>
);

seam_core::seam!(
    // fmgr_security_definer's proconfig loop (fmgr.c:740-753): each
    // "name=value" entry applied at superuser()?PGC_SUSET:PGC_USERSET,
    // PGC_S_SESSION, GUC_ACTION_SAVE, changeVal=true.
    pub fn process_guc_array_secdef<'a>(array: &'a [String]) -> PgResult<()>
);

seam_core::seam!(
    // SetConfigOption(name, value, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT) (guc.c);
    // miscinit's SetOuterUserId keeps the is_superuser GUC in sync through it.
    pub fn set_config_option_internal_dynamic_default(name: &str, value: &str) -> PgResult<()>
);

seam_core::seam!(
    // SetConfigOption(name, value, context, source) (guc.c); miscinit's
    // InitializeSessionUserId sets session_authorization at PGC_S_OVERRIDE
    // through it (guc depends on miscinit, so no direct edge back).
    pub fn set_config_option(
        name: &str,
        value: Option<&str>,
        context: types_guc::GucContext,
        source: types_guc::GucSource,
    ) -> PgResult<()>
);

seam_core::seam!(
    // GUC_check_errdetail (guc.c).
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

seam_core::seam!(
    // SelectConfigFiles(userDoption, progname) (guc.c) — deferred half of the
    // ported guc unit; false = C's "exit(2)" failure return.
    pub fn select_config_files(user_d_option: Option<&str>, progname: &str) -> PgResult<bool>
);

seam_core::seam!(
    // InitializeGUCOptions (guc.c) — same deferred half.
    pub fn initialize_guc_options() -> PgResult<()>
);


seam_core::seam!(
    // get_explain_guc_options (guc.c): (name, GetConfigOptionByName value)
    // for GUC_EXPLAIN vars modified from boot; EXPLAIN (SETTINGS) reads it
    // (installed by guc_funcs, which owns the ConfigOptionIsVisible deps).
    pub fn get_explain_guc_options() -> PgResult<Vec<(&'static str, Option<String>)>>
);

seam_core::seam!(
    // GetConfigOption(name, missing_ok=true, restrict_privileged=false)
    // (guc.c) — lets low-level crates (planner costing, executor arming)
    // read placeholder customized options (e.g. `pgrust.lane_parallel_pool`)
    // without a dependency edge onto the full guc crate. None = never set.
    pub fn get_config_option_missing_ok(name: &str) -> PgResult<Option<String>>
);
