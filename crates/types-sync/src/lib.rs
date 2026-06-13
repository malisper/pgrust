//! Signature vocabulary for `storage/sync/sync.c` / `storage/sync.h`.
//!
//! These are the shared types that appear in the `backend-storage-sync` public
//! API and its seam declarations: [`SyncRequestType`] and [`SyncRequestHandler`]
//! are the `sync.h` enums, [`FileTag`] is the `sync.h` file-identity struct used
//! as the pending-operations hash key, and [`FileTagOpResult`] is the owned
//! three-way return (return code + resolved path + `errno`) of a sync/unlink
//! handler call.

use types_core::primitive::ForkNumber;
use types_storage::RelFileLocator;

/// `SyncRequestType` (`sync.h`) — type of sync request. These manage the set of
/// pending requests to call a sync handler's sync or unlink functions at the
/// next checkpoint. The discriminant order matches the C enum exactly (the
/// value is used as the wire `type` in `ForwardSyncRequest`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum SyncRequestType {
    /// `SYNC_REQUEST` — schedule a call of the sync function.
    SyncRequest = 0,
    /// `SYNC_UNLINK_REQUEST` — schedule a call of the unlink function.
    SyncUnlinkRequest = 1,
    /// `SYNC_FORGET_REQUEST` — forget all calls for a tag.
    SyncForgetRequest = 2,
    /// `SYNC_FILTER_REQUEST` — forget all calls satisfying the match fn.
    SyncFilterRequest = 3,
}

/// `SyncRequestHandler` (`sync.h`) — which set of functions handles a request.
/// The enumerator values MUST match the indexes of the `syncsw` function table
/// in `sync.c` (which the per-handler seam dispatches on via the stored raw
/// `int16` in [`FileTag::handler`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i16)]
pub enum SyncRequestHandler {
    /// `SYNC_HANDLER_MD` — magnetic disk (`md.c`).
    Md = 0,
    /// `SYNC_HANDLER_CLOG` — `pg_xact`.
    Clog = 1,
    /// `SYNC_HANDLER_COMMIT_TS` — `pg_commit_ts`.
    CommitTs = 2,
    /// `SYNC_HANDLER_MULTIXACT_OFFSET` — `pg_multixact/offsets`.
    MultiXactOffset = 3,
    /// `SYNC_HANDLER_MULTIXACT_MEMBER` — `pg_multixact/members`.
    MultiXactMember = 4,
    /// `SYNC_HANDLER_NONE`.
    None = 5,
}

impl SyncRequestHandler {
    /// The raw `int16` value stored in [`FileTag::handler`] (the `syncsw`
    /// index).
    #[inline]
    pub const fn as_raw(self) -> i16 {
        self as i16
    }

    /// Rebuild a handler from its stored `int16` value. Returns `None` for an
    /// out-of-range value (C indexes `syncsw[]` directly, trusting the value).
    #[inline]
    pub const fn from_raw(raw: i16) -> Option<Self> {
        match raw {
            0 => Some(Self::Md),
            1 => Some(Self::Clog),
            2 => Some(Self::CommitTs),
            3 => Some(Self::MultiXactOffset),
            4 => Some(Self::MultiXactMember),
            5 => Some(Self::None),
            _ => None,
        }
    }
}

/// `FileTag` (`sync.h`) — a tag identifying a file. It carries the members
/// `md.c` needs, but `sync.c` has no knowledge of the internal structure. Used
/// as the pending-operations hash key, so it is `Hash`/`Eq` (C used
/// `HASH_BLOBS`, i.e. raw-bytes equality of the whole struct, which merges
/// duplicate fsync requests).
///
/// Field layout matches the C struct: `int16 handler`, `int16 forknum`,
/// `RelFileLocator rlocator`, `uint64 segno`. `sync.c` reads `tag.handler` to
/// pick the `syncsw[]` row; everything else is opaque to it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileTag {
    /// `SyncRequestHandler` value, stored compactly as `int16` to save space.
    pub handler: i16,
    /// `ForkNumber`, stored compactly as `int16` to save space.
    pub forknum: i16,
    /// The relation locator.
    pub rlocator: RelFileLocator,
    /// The segment number.
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
            handler: handler.as_raw(),
            forknum: forknum as i16,
            rlocator,
            segno,
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
