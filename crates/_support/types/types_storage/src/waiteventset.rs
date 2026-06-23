//! Wait-event vocabulary (`storage/waiteventset.h`), trimmed to the items the
//! pqcomm port consumes.

use types_core::pgsocket;

pub const WL_LATCH_SET: u32 = 1 << 0;
pub const WL_SOCKET_READABLE: u32 = 1 << 1;
pub const WL_SOCKET_WRITEABLE: u32 = 1 << 2;
/// Not for `WaitEventSetWait()`.
pub const WL_TIMEOUT: u32 = 1 << 3;
pub const WL_POSTMASTER_DEATH: u32 = 1 << 4;
pub const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;
/// Non-Windows: same as `WL_SOCKET_WRITEABLE`.
pub const WL_SOCKET_CONNECTED: u32 = WL_SOCKET_WRITEABLE;
pub const WL_SOCKET_CLOSED: u32 = 1 << 7;
/// Non-Windows: same as `WL_SOCKET_READABLE`.
pub const WL_SOCKET_ACCEPT: u32 = WL_SOCKET_READABLE;
pub const WL_SOCKET_MASK: u32 = WL_SOCKET_READABLE
    | WL_SOCKET_WRITEABLE
    | WL_SOCKET_CONNECTED
    | WL_SOCKET_ACCEPT
    | WL_SOCKET_CLOSED;

/// Raw owner-side id for a `WaitEventSet *` (the C type is header-opaque:
/// `typedef struct WaitEventSet WaitEventSet`); `0` is never a valid handle.
/// Seam-marshaling vocabulary only: consumers hold the owning
/// `WaitEventSet` guard (`backend-storage-ipc-waiteventset-seams`), whose
/// `Drop` is `FreeWaitEventSet`, never this bare id.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WaitEventSetHandle(usize);

impl WaitEventSetHandle {
    /// Mint a handle. Only the waiteventset owner (and test fakes) creates
    /// these.
    pub fn new(id: usize) -> Self {
        WaitEventSetHandle(id)
    }

    /// The owner-side id this handle names.
    pub fn as_usize(self) -> usize {
        self.0
    }
}

/// `struct WaitEvent` (`storage/waiteventset.h`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct WaitEvent {
    /// Position in the event data structure.
    pub pos: i32,
    /// Triggered events.
    pub events: u32,
    /// Socket fd associated with the event, if any.
    pub fd: pgsocket,
    /// `void *user_data` — the opaque per-event payload supplied at
    /// `AddWaitEventToSet` time and handed back by `WaitEventSetWait`. The C
    /// `void *` is an aliasing back-pointer to whatever the registrant owns;
    /// the owned model carries a non-aliasing key instead (`None` = the C
    /// `NULL`). nodeAppend's `ExecAsyncConfigureWait` registers the owning
    /// `AsyncRequest`'s `request_index` here so the delivery loop can recover
    /// the single matched request, mirroring C's `(AsyncRequest *) w->user_data`.
    pub user_data: Option<i32>,
}
