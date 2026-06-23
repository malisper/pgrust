//! Sync-request vocabulary (`storage/sync.h`): the request/handler enums and
//! the `FileTag` identifying a file to the checkpointer's sync machinery, plus
//! [`FileTagOpResult`], the owned three-way return (code + resolved path +
//! `errno`) of a `syncsw[]` sync/unlink handler call.

use crate::storage::RelFileLocator;
use alloc::string::String;
use ::types_core::primitive::ForkNumber;

/// `SyncRequestType` (`storage/sync.h`) — what a sync request asks for. The
/// discriminant order matches the C enum exactly (the value is used as the
/// wire `type` in `ForwardSyncRequest`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum SyncRequestHandler {
    /// `SYNC_HANDLER_MD` — magnetic disk (`md.c`).
    #[default]
    SYNC_HANDLER_MD = 0,
    /// `SYNC_HANDLER_CLOG` — `pg_xact`.
    SYNC_HANDLER_CLOG = 1,
    /// `SYNC_HANDLER_COMMIT_TS` — `pg_commit_ts`.
    SYNC_HANDLER_COMMIT_TS = 2,
    /// `SYNC_HANDLER_MULTIXACT_OFFSET` — `pg_multixact/offsets`.
    SYNC_HANDLER_MULTIXACT_OFFSET = 3,
    /// `SYNC_HANDLER_MULTIXACT_MEMBER` — `pg_multixact/members`.
    SYNC_HANDLER_MULTIXACT_MEMBER = 4,
    /// `SYNC_HANDLER_NONE`.
    SYNC_HANDLER_NONE = 5,
}

/// `FileTag` (`storage/sync.h`) — a tag identifying a file to sync.c. The C
/// struct stores `handler` as an `int16` "saving space"; the typed enum keeps
/// the same width. Used as the pending-operations hash key, so it is
/// `Hash`/`Eq` (C used `HASH_BLOBS`, raw-bytes equality of the whole struct,
/// which merges duplicate fsync requests).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct FileTag {
    /// `handler` — a `SyncRequestHandler` value (int16 in C).
    pub handler: SyncRequestHandler,
    /// `forknum` — a `ForkNumber`, saving space (int16 in C).
    pub forknum: i16,
    pub rlocator: RelFileLocator,
    pub segno: u64,
}

impl FileTag {
    /// Construct a `FileTag` from typed components (mirrors `md.c`'s
    /// `INIT_MD_FILETAG` field layout).
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

/// Outcome of a `syncsw[handler].sync_syncfiletag` / `sync_unlinkfiletag` call.
///
/// C returns an `int` and reports the file path through an out-param
/// `char path[MAXPGPATH]`, leaving the failure detail in the global `errno`.
/// We carry all three back together so the orchestration can faithfully port
/// the `errno`-based branches (`FILE_POSSIBLY_DELETED(errno)`,
/// `errno != ENOENT`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileTagOpResult {
    /// The handler's return code: `0` on success, `< 0` on failure.
    pub result: i32,
    /// The on-disk path the handler resolved the tag to (for error messages).
    pub path: String,
    /// `errno` as left by the handler on failure (ignored on success).
    pub errno: i32,
}
