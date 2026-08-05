seam_core::seam!(
    // set_ps_display_suffix(suffix) (ps_status.c) — the suffix is copied into
    // the title buffer, not retained.
    pub fn set_ps_display_suffix(suffix: &str)
);

seam_core::seam!(
    pub fn set_ps_display_remove_suffix()
);

seam_core::seam!(
    // init_ps_display(fixed_part) (utils/misc/ps_status.c); None mirrors C's
    // NULL -> GetBackendTypeDesc(MyBackendType).
    pub fn init_ps_display(fixed_part: Option<&str>)
);

seam_core::seam!(
    // set_ps_display(activity) (utils/misc/ps_status.c).
    pub fn set_ps_display(activity: &str)
);
