// WalSndWakeup (walsender.c:3717): broadcast the per-kind wakeup CVs so
// walsenders sleeping in WalSndWait notice new flushed/replayed WAL.
#![allow(non_snake_case)]

use crate::WalSndCtl;

// void WalSndWakeup(bool physical, bool logical). Installed as the
// walsender_seams::wal_snd_wakeup seam, called from the WAL flush/replay tails.
pub fn WalSndWakeup(physical: bool, logical: bool) {
    let ctl = WalSndCtl();
    if physical {
        condition_variable::ConditionVariableBroadcast(&ctl.wal_flush_cv);
    }
    if logical {
        condition_variable::ConditionVariableBroadcast(&ctl.wal_replay_cv);
    }
}
