seam_core::seam!(
    pub fn set_ps_display_suffix(suffix: &'static str)
);

seam_core::seam!(
    pub fn set_ps_display_remove_suffix()
);

seam_core::seam!(
    // init_ps_display(fixed_part) (utils/misc/ps_status.c).
    pub fn init_ps_display(fixed_part: &str)
);

seam_core::seam!(
    // set_ps_display(activity) (utils/misc/ps_status.c).
    pub fn set_ps_display(activity: &str)
);
