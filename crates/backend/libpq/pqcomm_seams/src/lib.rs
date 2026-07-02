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

seam_core::seam!(
    // pq_init (pqcomm.c), socket half; buffer half is pqcomm::pq_init_buffers.
    pub fn pq_init(client_sock: &types_startup::ClientSocket) -> PgResult<types_startup::Port>
);
