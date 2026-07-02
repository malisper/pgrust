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

seam_core::seam!(
    // miscinit's SwitchToSharedLatch/SwitchBackToLocalLatch repoint:
    // `if (FeBeWaitSet) ModifyWaitEvent(..., FeBeWaitSetLatchPos, WL_LATCH_SET,
    // MyLatch)`; the impl no-ops when FeBeWaitSet is unset.
    pub fn modify_fe_be_wait_set_latch(
        latch: types_storage::latch::LatchHandle,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ListenServerPort(family, hostname, port, unix_socket_dir, ListenSockets,
    // &NumListenSockets, maxListen) (pqcomm.c socket half, deferred there).
    // hostname None = C NULL ("*"/AF_UNIX); Ok = appended fds, Err = STATUS_ERROR.
    pub fn listen_server_port(
        hostname: Option<&str>,
        port: u16,
        unix_socket_dir: Option<&str>,
        listen_sockets: &mut Vec<i32>,
        max_listen: usize,
    ) -> PgResult<()>
);

seam_core::seam!(
    // AcceptConnection(server_fd, &client_sock) (pqcomm.c socket half);
    // Err is C's STATUS_ERROR arm.
    pub fn accept_connection(server_fd: i32) -> PgResult<types_startup::ClientSocket>
);
