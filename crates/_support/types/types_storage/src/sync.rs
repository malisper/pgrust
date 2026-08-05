use alloc::string::String;

use ::types_core::primitive::ForkNumber;

use crate::storage::RelFileLocator;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SyncRequestType {
    SYNC_REQUEST = 0,
    SYNC_UNLINK_REQUEST = 1,
    SYNC_FORGET_REQUEST = 2,
    SYNC_FILTER_REQUEST = 3,
}

// Values must match the syncsw[] indexes in sync.c.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum SyncRequestHandler {
    #[default]
    SYNC_HANDLER_MD = 0,
    SYNC_HANDLER_CLOG = 1,
    SYNC_HANDLER_COMMIT_TS = 2,
    SYNC_HANDLER_MULTIXACT_OFFSET = 3,
    SYNC_HANDLER_MULTIXACT_MEMBER = 4,
    SYNC_HANDLER_NONE = 5,
}

// Pending-operations hash key; C used HASH_BLOBS raw-bytes equality, which
// merges duplicate fsync requests.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct FileTag {
    pub handler: SyncRequestHandler,
    pub forknum: i16,
    pub rlocator: RelFileLocator,
    pub segno: u64,
}

impl FileTag {
    #[inline]
    pub fn new(
        handler: SyncRequestHandler,
        forknum: ForkNumber,
        rlocator: RelFileLocator,
        segno: u64,
    ) -> Self {
        Self {
            handler,
            forknum: forknum as i16,
            rlocator,
            segno,
        }
    }

    pub fn for_slru(handler: SyncRequestHandler, segno: u64) -> Self {
        Self {
            handler,
            segno,
            ..Self::default()
        }
    }
}

// C returns int + a char path[MAXPGPATH] out-param + global errno; carried
// together so the errno branches (FILE_POSSIBLY_DELETED, != ENOENT) port
// faithfully. std String justified: checkpointer fsync path, no query context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileTagOpResult {
    pub result: i32,
    pub path: String,
    pub errno: i32,
}
