//! Wait-event vocabulary (`utils/wait_event.h` and the generated
//! `wait_event_types.h`), trimmed to the items ports consume so far.
//!
//! A wait-event value is `class | id`; ids within a class come from the
//! case-insensitively sorted member list of that class's section in
//! `wait_event_names.txt`.

use types_core::uint32;

pub const PG_WAIT_TIMEOUT: uint32 = 0x09000000;

/// `WAIT_EVENT_SPIN_DELAY` — "Waiting while acquiring a contended spinlock."
/// 7th entry (0-based 6) of the `WaitEventTimeout` section, so the generated
/// enum value is `PG_WAIT_TIMEOUT | 6` (= 150994950, matching c2rust).
pub const WAIT_EVENT_SPIN_DELAY: uint32 = PG_WAIT_TIMEOUT | 6;
