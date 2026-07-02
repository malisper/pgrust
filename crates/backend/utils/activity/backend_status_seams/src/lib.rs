use types_core::TimestampTz;

seam_core::seam!(
    pub fn pgstat_report_xact_timestamp(ts: TimestampTz)
);
