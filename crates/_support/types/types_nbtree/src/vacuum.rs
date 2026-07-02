use ::mcx::PgVec;
use ::types_core::{uint16, BlockNumber, FullTransactionId, OffsetNumber};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTDedupInterval {
    pub baseoff: OffsetNumber,
    pub nitems: uint16,
}

const _: () = assert!(core::mem::size_of::<BTDedupInterval>() == 4);

// C convention: itup is the original posting tuple image on input and the
// palloc'd final image on output; deletetids is the uint16 FAM.
pub struct BTVacuumPosting<'mcx> {
    pub itup: PgVec<'mcx, u8>,
    pub updatedoffset: OffsetNumber,
    pub deletetids: PgVec<'mcx, u16>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTPendingFSM {
    pub target: BlockNumber,
    pub safexid: FullTransactionId,
}
