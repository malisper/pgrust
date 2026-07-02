use types_core::{TimeLineID, XLogRecPtr};

seam_core::seam!(
    // reachedConsistency (xlogrecovery.c global).
    pub fn reached_consistency() -> bool
);

seam_core::seam!(
    // GetXLogReplayRecPtr(&replayTLI) (xlogrecovery.c).
    pub fn get_xlog_replay_rec_ptr() -> (XLogRecPtr, TimeLineID)
);
