//! Error levels and SQLSTATE codes (`utils/elog.h`, `utils/errcodes.h`),
//! trimmed to the items current ports consume.

/// `elog.h` error level. `#[repr(transparent)]` over the C int values so the
/// ordering comparisons (`level >= ERROR`) match elog.c.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ErrorLevel(pub i32);

pub const DEBUG5: ErrorLevel = ErrorLevel(10);
pub const DEBUG4: ErrorLevel = ErrorLevel(11);
pub const DEBUG3: ErrorLevel = ErrorLevel(12);
pub const DEBUG2: ErrorLevel = ErrorLevel(13);
pub const DEBUG1: ErrorLevel = ErrorLevel(14);
pub const LOG: ErrorLevel = ErrorLevel(15);
pub const LOG_SERVER_ONLY: ErrorLevel = ErrorLevel(16);
pub const COMMERROR: ErrorLevel = LOG_SERVER_ONLY;
pub const INFO: ErrorLevel = ErrorLevel(17);
pub const NOTICE: ErrorLevel = ErrorLevel(18);
pub const WARNING: ErrorLevel = ErrorLevel(19);
pub const PGWARNING: ErrorLevel = WARNING;
pub const WARNING_CLIENT_ONLY: ErrorLevel = ErrorLevel(20);
pub const ERROR: ErrorLevel = ErrorLevel(21);
pub const PGERROR: ErrorLevel = ERROR;
pub const FATAL: ErrorLevel = ErrorLevel(22);
pub const PANIC: ErrorLevel = ErrorLevel(23);

/// A packed SQLSTATE (`MAKE_SQLSTATE`, elog.h).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlState(pub i32);

/// `PGSIXBIT(ch)` (elog.h).
pub const fn pg_sixbit(ch: u8) -> i32 {
    ((ch as i32) - (b'0' as i32)) & 0x3f
}

/// `PGUNSIXBIT(val)` (elog.h).
pub const fn pg_unsixbit(value: i32) -> u8 {
    (((value & 0x3f) + (b'0' as i32)) & 0xff) as u8
}

/// `MAKE_SQLSTATE(ch1, ch2, ch3, ch4, ch5)` (elog.h).
pub const fn make_sqlstate(chars: [u8; 5]) -> SqlState {
    SqlState(
        pg_sixbit(chars[0])
            + (pg_sixbit(chars[1]) << 6)
            + (pg_sixbit(chars[2]) << 12)
            + (pg_sixbit(chars[3]) << 18)
            + (pg_sixbit(chars[4]) << 24),
    )
}

/// Unpack a [`SqlState`] back into its five SQLSTATE characters.
pub const fn unpack_sqlstate(sqlstate: SqlState) -> [u8; 5] {
    let value = sqlstate.0;
    [
        pg_unsixbit(value),
        pg_unsixbit(value >> 6),
        pg_unsixbit(value >> 12),
        pg_unsixbit(value >> 18),
        pg_unsixbit(value >> 24),
    ]
}

// SQLSTATEs (errcodes.txt), added as ports consume them.
pub const ERRCODE_SUCCESSFUL_COMPLETION: SqlState = make_sqlstate(*b"00000");
pub const ERRCODE_WARNING: SqlState = make_sqlstate(*b"01000");
pub const ERRCODE_INTERNAL_ERROR: SqlState = make_sqlstate(*b"XX000");
pub const ERRCODE_INVALID_OBJECT_DEFINITION: SqlState = make_sqlstate(*b"42P17");

/// `ERRCODE_TO_CATEGORY(ec)` (elog.h).
pub const fn errcode_to_category(sqlstate: SqlState) -> SqlState {
    SqlState(sqlstate.0 & ((1 << 12) - 1))
}

/// `ERRCODE_IS_CATEGORY(ec)` (elog.h).
pub const fn errcode_is_category(sqlstate: SqlState) -> bool {
    (sqlstate.0 & !((1 << 12) - 1)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_roundtrip() {
        assert_eq!(unpack_sqlstate(ERRCODE_WARNING), *b"01000");
        assert_eq!(unpack_sqlstate(ERRCODE_INTERNAL_ERROR), *b"XX000");
        assert_eq!(unpack_sqlstate(ERRCODE_SUCCESSFUL_COMPLETION), *b"00000");
    }
}
