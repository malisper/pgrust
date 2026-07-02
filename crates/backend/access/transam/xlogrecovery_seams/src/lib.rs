use types_core::{TimeLineID, XLogRecPtr};

seam_core::seam!(
    pub fn reached_consistency() -> bool
);

seam_core::seam!(
    pub fn get_xlog_replay_rec_ptr() -> (XLogRecPtr, TimeLineID)
);
