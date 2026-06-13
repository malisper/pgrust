//! Sync-request vocabulary (`storage/sync.h`): the request/handler enums and
//! the `FileTag` identifying a file to the checkpointer's sync machinery.

use crate::storage::RelFileLocator;

/// `SyncRequestType` (`storage/sync.h`) — what a sync request asks for.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRequestType {
    /// `SYNC_REQUEST` — schedule a call of the sync function.
    SYNC_REQUEST = 0,
    /// `SYNC_UNLINK_REQUEST` — schedule a call of the unlink function.
    SYNC_UNLINK_REQUEST = 1,
    /// `SYNC_FORGET_REQUEST` — forget all calls for a tag.
    SYNC_FORGET_REQUEST = 2,
    /// `SYNC_FILTER_REQUEST` — forget all calls satisfying the match fn.
    SYNC_FILTER_REQUEST = 3,
}

/// `SyncRequestHandler` (`storage/sync.h`) — which set of functions handles a
/// given request; the values must match the indexes of `syncsw[]` in sync.c.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyncRequestHandler {
    #[default]
    SYNC_HANDLER_MD = 0,
    SYNC_HANDLER_CLOG = 1,
    SYNC_HANDLER_COMMIT_TS = 2,
    SYNC_HANDLER_MULTIXACT_OFFSET = 3,
    SYNC_HANDLER_MULTIXACT_MEMBER = 4,
    SYNC_HANDLER_NONE = 5,
}

/// `FileTag` (`storage/sync.h`) — a tag identifying a file to sync.c. The C
/// struct stores `handler` as an `int16` "saving space"; the typed enum keeps
/// the same width.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileTag {
    /// `handler` — a `SyncRequestHandler` value (int16 in C).
    pub handler: SyncRequestHandler,
    /// `forknum` — a `ForkNumber`, saving space (int16 in C).
    pub forknum: i16,
    pub rlocator: RelFileLocator,
    pub segno: u64,
}

impl FileTag {
    /// `INIT_SLRUFILETAG(a, xx_handler, xx_segno)` (slru.c) — zero the tag and
    /// fill only the handler and segment number.
    pub fn for_slru(handler: SyncRequestHandler, segno: u64) -> Self {
        Self {
            handler,
            segno,
            ..Self::default()
        }
    }
}
