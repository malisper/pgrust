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

// Returns C's rel->pgstat_enabled; pgstat keys pgstat_info by relid.
seam_core::seam!(
    pub fn pgstat_init_relation(relid: types_core::Oid, relkind: u8) -> bool
);

seam_core::seam!(
    // `pgstat_report_tempfile(filesize)` (utils/activity/pgstat_database.c).
    pub fn pgstat_report_tempfile(file_size: u64)
);

seam_core::seam!(
    // pgstat_initialize (pgstat.c).
    pub fn pgstat_initialize() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_before_server_shutdown(code, arg) (pgstat.c), before_shmem_exit shape.
    pub fn pgstat_before_server_shutdown(code: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_restore_stats() (pgstat.c).
    pub fn pgstat_restore_stats() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_discard_stats() (pgstat.c).
    pub fn pgstat_discard_stats() -> types_error::PgResult<()>
);
