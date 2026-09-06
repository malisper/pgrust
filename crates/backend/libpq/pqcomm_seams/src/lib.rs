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
    // TouchSocketFiles (pqcomm.c): utime() every Unix-domain socket file this
    // postmaster created — ServerLoop's 58-minute guard against /tmp cleaners
    // (postmaster.c:1798).
    pub fn touch_socket_files()
);

seam_core::seam!(
    // RemoveSocketFiles (pqcomm.c): unlink every Unix-domain socket file this
    // postmaster created — CloseServerPorts' on_proc_exit arm
    // (postmaster.c:1438), after the listen fds close and before the lock
    // files go.
    pub fn remove_socket_files()
);

seam_core::seam!(
    // AcceptConnection(server_fd, &client_sock) (pqcomm.c socket half);
    // Err is C's STATUS_ERROR arm.
    pub fn accept_connection(server_fd: i32) -> PgResult<types_startup::ClientSocket>
);

seam_core::seam!(
    // pq_check_connection (pqcomm.c): true = client still connected. The
    // ProcessInterrupts CLIENT_CONNECTION_CHECK arm dispatches through this
    // seam because it is TRANSPORT-owned: only the real socket transport
    // installs it (init_socket_seams — its FeBeWaitSet + WL_SOCKET_CLOSED
    // are what the poll rides); sim-net/wasm leave it vacant and the caller
    // treats the connection as alive.
    pub fn pq_check_connection() -> PgResult<bool>
);

seam_core::seam!(
    // pgrust extension (no C counterpart): a HOST-OWNED fd the postmaster
    // may block on for "something happened", or PGINVALID_SOCKET when this
    // transport offers none (every transport but host-pipes, and host-pipes
    // itself when its host did not hand one in). Installed by the host-pipes
    // provider; read once, at the postmaster's first wait, by ServerLoop's
    // wasm arm — the target where a listen fd cannot be registered in a wait
    // set and the postmaster would otherwise have to POLL for connections.
    // The fd is both ends of one pipe: the host writes a wake token when it
    // announces a connection or closes the listener, and the postmaster's
    // own waiter writes the same token from fd-park mode, so a guest-side
    // `SetLatch` and a host-side event wake the same single block.
    pub fn transport_wake_fd() -> i32
);

seam_core::seam!(
    // pgrust extension (no C counterpart): does this transport OWN its
    // listener rather than bind one? Installed only by the host-pipes
    // provider, whose listener is a file descriptor the host hands us
    // (there is nothing to bind: WASI p1 has no socket()). PostmasterMain
    // asks — through `is_installed()`, so every other transport keeps C's
    // control flow byte-for-byte — before running its listen_addresses /
    // unix_socket_directories loops; when true it calls listen_server_port
    // exactly once with no address, no port and no socket directory.
    pub fn transport_owns_listener() -> bool
);
