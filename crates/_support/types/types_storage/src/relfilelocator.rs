use ::types_core::ProcNumber;

pub use crate::storage::{RelFileLocator, RelFileLocatorEquals};

// `backend` is INVALID_PROC_NUMBER unless the relation is backend-local (temp).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

const _: () = assert!(core::mem::size_of::<RelFileLocatorBackend>() == 16);
