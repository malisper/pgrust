//! Seam declarations for the `backend-access-common-printtup` unit
//! (`access/common/printtup.c`), for callers that would otherwise cycle.
//!
//! The `DestRemote` / `DestRemoteExecute` / `DestDebug` `DestReceiver`s are
//! created by printtup's `printtup_create_DR` (and `debugtup`'s `debugStartup`),
//! but `tcop/dest.c`'s `CreateDestReceiver` switch — which lives in a crate that
//! cannot depend on printtup directly without a cycle — needs to delegate to
//! that constructor. So dest reaches the constructor through this seam, exactly
//! as it reaches copyto's `CreateCopyDestReceiver` through
//! `backend-commands-copyto-seams`.
//!
//! printtup installs this from its own `init_seams()`.

seam_core::seam!(
    /// `printtup_create_DR(CommandDest dest)` (printtup.c:81): build the printtup
    /// `DestReceiver` for one of the `DestRemote` / `DestRemoteExecute` /
    /// `DestDebug` kinds and register it into the tcop-dest router, returning its
    /// [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver` switch calls
    /// this for those kinds (the owner cannot live in dest.c, so dest delegates
    /// here; printtup installs the seam from its own `init_seams()`).
    pub fn printtup_create_dr(
        dest: types_dest::CommandDest,
    ) -> types_nodes::parsestmt::DestReceiverHandle
);
