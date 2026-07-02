use types_error::PgResult;

seam_core::seam!(
    pub fn pq_putmessage(msgtype: u8, body: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    pub fn pq_putmessage_v2(msgtype: u8, body: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    pub fn pq_flush() -> PgResult<i32>
);
