//! Wake-up event bitmasks for `WaitLatch()` / `WaitLatchOrSocket()` /
//! `WaitEventSetWait()` (`storage/waiteventset.h`), non-WIN32 values.

pub const WL_LATCH_SET: u32 = 1 << 0;
pub const WL_SOCKET_READABLE: u32 = 1 << 1;
pub const WL_SOCKET_WRITEABLE: u32 = 1 << 2;
/// Not for `WaitEventSetWait()`.
pub const WL_TIMEOUT: u32 = 1 << 3;
pub const WL_POSTMASTER_DEATH: u32 = 1 << 4;
pub const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;
/// Non-WIN32: alias of `WL_SOCKET_WRITEABLE`.
pub const WL_SOCKET_CONNECTED: u32 = WL_SOCKET_WRITEABLE;
pub const WL_SOCKET_CLOSED: u32 = 1 << 7;
/// Non-WIN32: alias of `WL_SOCKET_READABLE`.
pub const WL_SOCKET_ACCEPT: u32 = WL_SOCKET_READABLE;
pub const WL_SOCKET_MASK: u32 = WL_SOCKET_READABLE
    | WL_SOCKET_WRITEABLE
    | WL_SOCKET_CONNECTED
    | WL_SOCKET_ACCEPT
    | WL_SOCKET_CLOSED;
