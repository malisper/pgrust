//! Seam declarations for the `backend-storage-ipc-waiteventset` unit
//! (`storage/ipc/waiteventset.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! A `WaitEventSet` is carried across the seam as an opaque `u64` token
//! minted by the owner (`0` is never a valid set).

seam_core::seam!(
    /// `CreateWaitEventSet(ResourceOwner, nevents)` (`waiteventset.c`) —
    /// allocate a wait-event set sized for `nevents` events; allocation can
    /// `ereport(ERROR)` (OOM). The C `ResourceOwner` argument is `NULL` for
    /// the current consumers (process-lifetime sets); under the RAII resource
    /// model the owner decides how to track the set's kernel objects.
    pub fn create_wait_event_set(nevents: i32) -> types_error::PgResult<u64>
);

seam_core::seam!(
    /// `AddWaitEventToSet(set, events, fd, latch, user_data)`
    /// (`waiteventset.c`) — returns the event's position in the set; misuse
    /// (set full, bad event combination) is `ereport(ERROR)`. The C `latch`
    /// argument is either `NULL` or `MyLatch` for current consumers:
    /// `attach_my_latch` says which, and the owner resolves `MyLatch` itself
    /// (same pattern as `set_latch_my_latch`). `user_data` is not carried
    /// (always `NULL` here).
    pub fn add_wait_event_to_set(
        set: u64,
        events: u32,
        fd: i32,
        attach_my_latch: bool
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `WaitEventSetWait(set, timeout, occurred_events, nevents,
    /// wait_event_info)` (`waiteventset.c`) — wait for events; `timeout` in
    /// milliseconds, `-1` for no timeout. Returns the `events` masks of the
    /// occurred events (the C out-array `occurred_events[i].events`), empty
    /// on timeout. Internal failures are `ereport(ERROR)`.
    pub fn wait_event_set_wait(
        set: u64,
        timeout: i64,
        nevents: i32,
        wait_event_info: u32
    ) -> types_error::PgResult<::std::vec::Vec<u32>>
);
