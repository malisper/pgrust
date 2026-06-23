//! Shared helpers: fallible formatted appends into the caller's `PgString`
//! (the `appendStringInfo` analog) and bounds-checked native-endian scalar
//! reads for the records that carry bare values rather than an `xl_*` struct.

use core::fmt;

use mcx::PgString;
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};

/// `appendStringInfo(buf, fmt, ...)`: format into the caller's string,
/// surfacing an allocation failure as the context's OOM `PgError` (C's
/// `palloc` `ereport(ERROR)` path) instead of swallowing it in `fmt::Error`.
pub(crate) fn append_fmt(buf: &mut PgString<'_>, args: fmt::Arguments<'_>) -> PgResult<()> {
    struct Sink<'a, 'mcx> {
        buf: &'a mut PgString<'mcx>,
        err: Option<PgError>,
    }
    impl fmt::Write for Sink<'_, '_> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            self.buf.try_push_str(s).map_err(|e| {
                self.err = Some(e);
                fmt::Error
            })
        }
    }
    let mut sink = Sink { buf, err: None };
    match fmt::Write::write_fmt(&mut sink, args) {
        Ok(()) => Ok(()),
        // `write_fmt` only fails when our `write_str` failed, so `err` is set.
        Err(fmt::Error) => Err(sink
            .err
            .unwrap_or_else(|| PgError::error("could not format WAL record description"))),
    }
}

macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        $crate::util::append_fmt($buf, core::format_args!($($arg)*))
    };
}
pub(crate) use appendf;

/// `%s` over bytes that are not guaranteed UTF-8: stream them into `buf`
/// lossily (invalid sequences become U+FFFD) chunk by chunk through the
/// fallible API, never materializing an owned `String` through the
/// infallible global allocator.
pub(crate) fn append_lossy(buf: &mut PgString<'_>, bytes: &[u8]) -> PgResult<()> {
    for chunk in bytes.utf8_chunks() {
        buf.try_push_str(chunk.valid())?;
        if !chunk.invalid().is_empty() {
            buf.try_push(char::REPLACEMENT_CHARACTER)?;
        }
    }
    Ok(())
}

/// The record payload is shorter than the record being read. Unreachable for
/// well-formed WAL (the C reads whatever bytes follow the record); loud
/// `ERRCODE_DATA_CORRUPTED` beats reading garbage.
pub(crate) fn record_truncated(what: &'static str) -> PgError {
    PgError::error(format!("WAL record payload too short for {what}"))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

fn bytes_at<const N: usize>(
    data: &[u8],
    offset: usize,
    what: &'static str,
) -> PgResult<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| record_truncated(what))?;
    let bytes = data.get(offset..end).ok_or_else(|| record_truncated(what))?;
    Ok(bytes.try_into().expect("slice length is N"))
}

pub(crate) fn read_u16(data: &[u8], offset: usize, what: &'static str) -> PgResult<u16> {
    Ok(u16::from_ne_bytes(bytes_at(data, offset, what)?))
}

pub(crate) fn read_i64(data: &[u8], offset: usize, what: &'static str) -> PgResult<i64> {
    Ok(i64::from_ne_bytes(bytes_at(data, offset, what)?))
}
