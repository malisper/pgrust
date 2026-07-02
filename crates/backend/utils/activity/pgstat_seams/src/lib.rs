seam_core::seam!(
    pub fn pgstat_set_session_end_cause_fatal()
);

seam_core::seam!(
    pub fn pgstat_get_slru_index(name: &str) -> i32
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_zeroed(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_hit(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_read(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_written(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_exists(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_flush(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_truncate(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_slru_written()
);
