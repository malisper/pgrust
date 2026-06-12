//! Error levels and SQLSTATE encoding (`utils/elog.h`, `utils/errcodes.h`).

/// `elog.h` error level. Ordered: comparisons like `level >= ERROR` mirror C.
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

/// A SQLSTATE packed with `MAKE_SQLSTATE`'s six-bit encoding (`elog.h`).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlState(pub i32);

pub const ERRCODE_SUCCESSFUL_COMPLETION: SqlState = make_sqlstate(*b"00000");
pub const ERRCODE_WARNING: SqlState = make_sqlstate(*b"01000");
pub const ERRCODE_INTERNAL_ERROR: SqlState = make_sqlstate(*b"XX000");

pub const fn pg_sixbit(ch: u8) -> i32 {
    ((ch as i32) - (b'0' as i32)) & 0x3f
}

pub const fn pg_unsixbit(value: i32) -> u8 {
    (((value & 0x3f) + (b'0' as i32)) & 0xff) as u8
}

pub const fn make_sqlstate(chars: [u8; 5]) -> SqlState {
    SqlState(
        pg_sixbit(chars[0])
            + (pg_sixbit(chars[1]) << 6)
            + (pg_sixbit(chars[2]) << 12)
            + (pg_sixbit(chars[3]) << 18)
            + (pg_sixbit(chars[4]) << 24),
    )
}

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

pub const fn errcode_to_category(sqlstate: SqlState) -> SqlState {
    SqlState(sqlstate.0 & ((1 << 12) - 1))
}

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
