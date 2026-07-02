use types_core::TimestampTz;

seam_core::seam!(
    pub fn get_current_timestamp() -> TimestampTz
);
