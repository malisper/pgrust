//! Seam declarations for the `backend-access-common-printtup` unit
//! (`access/common/printtup.c`), for callers that would otherwise cycle.
//!
//! The `DestRemote` / `DestRemoteExecute` `DestReceiver`s are created by
//! printtup's `printtup_create_DR`, and the `DestDebug` receiver is the static
//! `debugtupDR` (`debugtup` / `debugStartup`); both live in `printtup.c`, but
//! `tcop/dest.c`'s `CreateDestReceiver` switch — which lives in a crate that
//! cannot depend on printtup directly without a cycle — needs to delegate to
//! those constructors. So dest reaches them through these seams, exactly as it
//! reaches copyto's `CreateCopyDestReceiver` through
//! `backend-commands-copyto-seams`.
//!
//! printtup installs these from its own `init_seams()`.

seam_core::seam!(
    /// `printtup_create_DR(CommandDest dest)` (printtup.c:81): build the printtup
    /// `DestReceiver` for one of the `DestRemote` / `DestRemoteExecute` kinds and
    /// register it into the tcop-dest router, returning its
    /// [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver` switch calls
    /// this for those kinds (the owner cannot live in dest.c, so dest delegates
    /// here; printtup installs the seam from its own `init_seams()`).
    pub fn printtup_create_dr(
        dest: types_dest::CommandDest,
    ) -> types_nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `&debugtupDR` (printtup.c `debugtup` / `debugStartup`, dest.c:75): build the
    /// `DestDebug` `DestReceiver` and register it into the tcop-dest router,
    /// returning its [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver`
    /// switch returns `unconstify(DestReceiver *, &debugtupDR)` for `DestDebug`;
    /// the standalone (`--single`) backend's `whereToSendOutput = DestDebug`
    /// routes `SELECT` output here, printing each tuple to stdout via
    /// `debugtup` / `debugStartup`. printtup installs the seam from its own
    /// `init_seams()`.
    pub fn create_debug_dest_receiver() -> types_nodes::parsestmt::DestReceiverHandle
);
