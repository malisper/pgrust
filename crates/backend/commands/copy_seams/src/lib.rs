use types_error::PgResult;
use types_slot::SlotData;

pub struct CopyDestState {
    pub frame: *mut core::ffi::c_void,
    pub processed: u64,
}

impl CopyDestState {
    pub fn new(frame: *mut core::ffi::c_void) -> Self {
        CopyDestState { frame, processed: 0 }
    }
}

seam_core::seam!(
    pub fn copy_dest_receive<'mcx>(
        state: &mut CopyDestState,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool>
);
