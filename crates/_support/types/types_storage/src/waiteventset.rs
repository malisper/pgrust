use ::types_core::pgsocket;

pub const WL_LATCH_SET: u32 = 1 << 0;
pub const WL_SOCKET_READABLE: u32 = 1 << 1;
pub const WL_SOCKET_WRITEABLE: u32 = 1 << 2;
pub const WL_TIMEOUT: u32 = 1 << 3;
pub const WL_POSTMASTER_DEATH: u32 = 1 << 4;
pub const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;
// Non-Windows aliases (waiteventset.h).
pub const WL_SOCKET_CONNECTED: u32 = WL_SOCKET_WRITEABLE;
pub const WL_SOCKET_CLOSED: u32 = 1 << 7;
pub const WL_SOCKET_ACCEPT: u32 = WL_SOCKET_READABLE;
pub const WL_SOCKET_MASK: u32 = WL_SOCKET_READABLE
    | WL_SOCKET_WRITEABLE
    | WL_SOCKET_CONNECTED
    | WL_SOCKET_ACCEPT
    | WL_SOCKET_CLOSED;

// Raw owner-side id for the header-opaque C `WaitEventSet *`; 0 is never
// valid. Consumers hold the owning guard, not this bare id.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WaitEventSetHandle(usize);

impl WaitEventSetHandle {
    pub fn new(id: usize) -> Self {
        WaitEventSetHandle(id)
    }

    pub fn as_usize(self) -> usize {
        self.0
    }
}

// user_data: C's aliasing void* back-pointer carried as a non-aliasing key
// (None = NULL); nodeAppend registers the AsyncRequest's request_index.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct WaitEvent {
    pub pos: i32,
    pub events: u32,
    pub fd: pgsocket,
    pub user_data: Option<i32>,
}
