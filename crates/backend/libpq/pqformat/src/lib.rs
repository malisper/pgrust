//! Port of `src/backend/libpq/pqformat.c` (PostgreSQL 18.3): routines for
//! formatting and parsing frontend/backend messages, plus the `pqformat.h`
//! inline send helpers.
//!
//! Outgoing messages are built up in a [`StringInfo`] buffer and then sent in
//! a single call to `pq_putmessage`. Incoming messages are read into a
//! `StringInfo` (cursor = read offset) and parsed with the `pq_getmsg*`
//! family. The same routines support external binary formats
//! (typsend/typreceive).
//!
//! C-to-Rust mapping notes:
//!
//! * The C functions allocate in `CurrentMemoryContext`; there is no ambient
//!   context here, so the initializers (`pq_beginmessage`, `pq_begintypsend`)
//!   and the converting readers take an explicit [`Mcx`]. Routines that grow
//!   an existing buffer allocate in that buffer's own context.
//! * Every `ereport(ERROR)`/`elog(ERROR)` path (protocol violations, the
//!   `enlargeStringInfo` 1GB cap, palloc OOM, encoding-conversion failures)
//!   becomes `Err(PgError)`.
//! * `pg_server_to_client` / `pg_client_to_server` live in unported
//!   `mbutils.c` and are reached through `backend-utils-mb-mbutils-seams`;
//!   `pq_putmessage` is reached through `backend-libpq-pqcomm-seams`.
//! * C's StringInfo trailing-NUL sentinel is not stored (see
//!   `types-stringinfo`); the NUL-terminated readers scan for an embedded NUL
//!   and apply C's identical `cursor + slen >= len` bound.

#![forbid(unsafe_code)]
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use ::datum::Bytea;
use types_error::{
    PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION,
};
use ::stringinfo::StringInfo;

use ::pqcomm_seams::pq_putmessage;
use mbutils_seams::{pg_client_to_server, pg_server_to_client};

/// `MaxAllocSize` (`utils/memutils.h`): `0x3fffffff` — the `enlargeStringInfo`
/// growth cap.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// Install this crate's seam implementations. This unit owns no seams:
/// message assembly is caller-side `StringInfo` building (callable directly
/// — no dependency cycle), and the completed message crosses to pqcomm via
/// its own `pq_putmessage` seam.
pub fn init_seams() {}

// ===========================================================================
// StringInfo growth (the stringinfo.c pieces pqformat depends on)
// ===========================================================================

/// `enlargeStringInfo(buf, needed)`: ensure room for `needed` more bytes,
/// enforcing the `MaxAllocSize` cap with stringinfo.c's exact error.
///
/// `pub`: this is `common/stringinfo.c` logic homed here until that unit is
/// ported; `pqcomm`'s `pq_getmessage` shares this single implementation.
pub fn enlarge_string_info(buf: &mut StringInfo<'_>, needed: usize) -> PgResult<()> {
    let len = buf.data.len();
    if needed >= MAX_ALLOC_SIZE.saturating_sub(len) {
        return Err(PgError::error(format!(
            "string buffer exceeds maximum allowed length ({MAX_ALLOC_SIZE} bytes)"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "Cannot enlarge string buffer containing {len} bytes by {needed} more bytes."
            )));
    }
    let mcx = buf.allocator();
    buf.data.try_reserve(needed).map_err(|_| mcx.oom(needed))
}

/// `appendBinaryStringInfo` / `appendBinaryStringInfoNT`: grow (fallibly) and
/// append. The NT/non-NT distinction is the trailing-NUL sentinel, which this
/// representation does not store, so both collapse here.
fn append_binary(buf: &mut StringInfo<'_>, data: &[u8]) -> PgResult<()> {
    enlarge_string_info(buf, data.len())?;
    buf.data.extend_from_slice(data);
    Ok(())
}

/// `initStringInfo`: a fresh buffer with the standard 1024-byte initial
/// allocation, charged to `mcx` (C: `palloc(1024)` in `CurrentMemoryContext`).
fn init_string_info(mcx: Mcx<'_>) -> PgResult<StringInfo<'_>> {
    let mut buf = StringInfo::new_in(mcx);
    buf.data.try_reserve(1024).map_err(|_| mcx.oom(1024))?;
    Ok(buf)
}

// ===========================================================================
// Message assembly and output
// ===========================================================================

/// `pq_beginmessage` — initialize for sending a message.
///
/// The message type is stashed into the buffer's `cursor` field, expecting
/// that the `pq_send*` routines won't touch it; `pq_endmessage` reads it back.
pub fn pq_beginmessage(mcx: Mcx<'_>, msgtype: u8) -> PgResult<StringInfo<'_>> {
    let mut buf = init_string_info(mcx)?;
    buf.cursor = msgtype as usize;
    Ok(buf)
}

/// `pq_beginmessage_reuse` — initialize for sending a message, reusing the
/// buffer (which must live in a sufficiently long-lived memory context).
pub fn pq_beginmessage_reuse(buf: &mut StringInfo<'_>, msgtype: u8) {
    buf.reset();
    buf.cursor = msgtype as usize;
}

/// `pq_sendbytes` — append raw data to the buffer.
pub fn pq_sendbytes(buf: &mut StringInfo<'_>, data: &[u8]) -> PgResult<()> {
    append_binary(buf, data)
}

/// `pq_sendcountedtext` — append a counted text string (with character-set
/// conversion): a 4-byte count (not including itself) followed by the string,
/// neither NUL-terminated.
pub fn pq_sendcountedtext(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    let mcx = buf.allocator();
    match pg_server_to_client::call(mcx, s)? {
        Some(p) => {
            // actual conversion has been done: slen = strlen(p)
            pq_sendint32(buf, p.len() as u32)?;
            append_binary(buf, &p)?;
            // pfree(p) on drop
        }
        None => {
            pq_sendint32(buf, s.len() as u32)?;
            append_binary(buf, s)?;
        }
    }
    Ok(())
}

/// `pq_sendtext` — append a text string (with conversion), no count and no
/// terminator. Useful for binary-format conversions rather than direct
/// frontend transmission.
pub fn pq_sendtext(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    let mcx = buf.allocator();
    match pg_server_to_client::call(mcx, s)? {
        Some(p) => append_binary(buf, &p),
        None => append_binary(buf, s),
    }
}

/// `pq_sendstring` — append a NUL-terminated text string (with conversion).
///
/// `s` is the string contents *without* the terminator (the Rust analog of a
/// `char *`); the data appended is the converted bytes plus a single NUL, as
/// in C's `appendBinaryStringInfoNT(buf, p, slen + 1)`.
pub fn pq_sendstring(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    let mcx = buf.allocator();
    match pg_server_to_client::call(mcx, s)? {
        Some(p) => {
            append_binary(buf, &p)?;
            append_binary(buf, &[0])
        }
        None => {
            append_binary(buf, s)?;
            append_binary(buf, &[0])
        }
    }
}

/// `pq_send_ascii_string` — append a NUL-terminated text string WITHOUT
/// conversion, silently replacing any non-7-bit-ASCII byte with `?`. Used only
/// when normal localization/encoding conversion of an error message is itself
/// in trouble.
pub fn pq_send_ascii_string(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    for &b in s {
        // IS_HIGHBIT_SET(ch) -> '?'
        let ch = if b & 0x80 != 0 { b'?' } else { b };
        append_binary(buf, &[ch])?;
    }
    append_binary(buf, &[0])
}

/// `pq_sendfloat4` — append a float4 in external binary representation
/// (byte-swapped like an int4; C bit-puns through a union, which is
/// `f32::to_bits`).
pub fn pq_sendfloat4(buf: &mut StringInfo<'_>, f: f32) -> PgResult<()> {
    pq_sendint32(buf, f.to_bits())
}

/// `pq_sendfloat8` — append a float8 in external binary representation
/// (byte-swapped like an int8).
pub fn pq_sendfloat8(buf: &mut StringInfo<'_>, f: f64) -> PgResult<()> {
    pq_sendint64(buf, f.to_bits())
}

/// `pq_endmessage` — send the completed message to the frontend and free the
/// buffer (consumed; C pfrees `buf->data` and NULLs it).
///
/// C discards only `pq_putmessage`'s `int` EOF result ("no need to complain
/// about any failure, since pqcomm.c already did" — `ereport(COMMERROR)`);
/// any `ereport(ERROR)` underneath the putmessage method still propagates,
/// so this returns `PgResult<()>` and discards only the `Ok` value.
pub fn pq_endmessage(buf: StringInfo<'_>) -> PgResult<()> {
    // msgtype was saved in the cursor field
    let _eof = pq_putmessage::call(buf.cursor as u8, &buf.data)?;
    // pfree(buf->data) on drop
    Ok(())
}

/// `pq_endmessage_reuse` — send the completed message but do *not* free the
/// buffer, allowing reuse with [`pq_beginmessage_reuse`]. Same failure
/// surface as [`pq_endmessage`].
pub fn pq_endmessage_reuse(buf: &StringInfo<'_>) -> PgResult<()> {
    let _eof = pq_putmessage::call(buf.cursor as u8, &buf.data)?;
    Ok(())
}

// ===========================================================================
// pqformat.h inline send helpers
// ===========================================================================

/// `pq_writeint8` (header inline) — append a `[u]int8` to a buffer with
/// preallocated space. The reserve/write split is a C micro-optimization;
/// over `PgVec` it collapses into the (fallible) append.
pub fn pq_writeint8(buf: &mut StringInfo<'_>, i: u8) -> PgResult<()> {
    append_binary(buf, &[i])
}

/// `pq_writeint16` (header inline) — append a network-order `[u]int16`.
pub fn pq_writeint16(buf: &mut StringInfo<'_>, i: u16) -> PgResult<()> {
    append_binary(buf, &i.to_be_bytes())
}

/// `pq_writeint32` (header inline) — append a network-order `[u]int32`.
pub fn pq_writeint32(buf: &mut StringInfo<'_>, i: u32) -> PgResult<()> {
    append_binary(buf, &i.to_be_bytes())
}

/// `pq_writeint64` (header inline) — append a network-order `[u]int64`.
pub fn pq_writeint64(buf: &mut StringInfo<'_>, i: u64) -> PgResult<()> {
    append_binary(buf, &i.to_be_bytes())
}

/// `pq_writestring` (header inline) — append a NUL-terminated text string
/// (with conversion) to a buffer with preallocated space. Same observable
/// behavior as [`pq_sendstring`] here.
pub fn pq_writestring(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    pq_sendstring(buf, s)
}

/// `pq_sendint8` (header inline) — `enlargeStringInfo` + `pq_writeint8`.
pub fn pq_sendint8(buf: &mut StringInfo<'_>, i: u8) -> PgResult<()> {
    pq_writeint8(buf, i)
}

/// `pq_sendint16` (header inline) — append a binary `[u]int16`.
pub fn pq_sendint16(buf: &mut StringInfo<'_>, i: u16) -> PgResult<()> {
    pq_writeint16(buf, i)
}

/// `pq_sendint32` (header inline) — append a binary `[u]int32`.
pub fn pq_sendint32(buf: &mut StringInfo<'_>, i: u32) -> PgResult<()> {
    pq_writeint32(buf, i)
}

/// `pq_sendint64` (header inline) — append a binary `[u]int64`.
pub fn pq_sendint64(buf: &mut StringInfo<'_>, i: u64) -> PgResult<()> {
    pq_writeint64(buf, i)
}

/// `pq_sendbyte` (header inline) — append a binary byte.
pub fn pq_sendbyte(buf: &mut StringInfo<'_>, byt: u8) -> PgResult<()> {
    pq_sendint8(buf, byt)
}

/// `pq_sendint` (header inline, deprecated in C) — append a binary integer of
/// width `b` ∈ {1, 2, 4}; any other width is `elog(ERROR, "unsupported
/// integer size %d", b)`.
pub fn pq_sendint(buf: &mut StringInfo<'_>, i: u32, b: i32) -> PgResult<()> {
    match b {
        1 => pq_sendint8(buf, i as u8),
        2 => pq_sendint16(buf, i as u16),
        4 => pq_sendint32(buf, i),
        _ => Err(unsupported_integer_size(b)),
    }
}

/// `elog(ERROR, "unsupported integer size %d", b)` — internal error (XX000).
fn unsupported_integer_size(b: i32) -> PgError {
    PgError::error(format!("unsupported integer size {b}"))
}

// ===========================================================================
// typsend support
// ===========================================================================

/// `pq_begintypsend` — initialize for constructing a bytea result: a fresh
/// buffer with four bytes reserved for the bytea length word.
pub fn pq_begintypsend(mcx: Mcx<'_>) -> PgResult<StringInfo<'_>> {
    let mut buf = init_string_info(mcx)?;
    // Reserve four bytes for the bytea length word
    append_binary(&mut buf, &[0u8; ::datum::VARHDRSZ])?;
    Ok(buf)
}

/// `pq_endtypsend` — finish constructing a bytea result.
///
/// C stamps the reserved length word (`SET_VARSIZE(result, buf->len)`, with
/// `Assert(buf->len >= VARHDRSZ)`) and returns `buf->data` itself as the
/// palloc'd `bytea *`; here that is [`Bytea::from_image`] over the buffer's
/// storage, which owns the varatt.h header encoding.
pub fn pq_endtypsend(buf: StringInfo<'_>) -> Bytea<'_> {
    Bytea::from_image(buf.data)
}

// ===========================================================================
// Special-case message output
// ===========================================================================

/// `pq_puttextmessage` — generate a character-set-converted message in one
/// step: like `pq_putmessage` but the body is a NUL-terminated string subject
/// to encoding conversion. `s` is the contents without the terminator; the
/// transmitted body includes it (C sends `slen + 1` bytes).
pub fn pq_puttextmessage(mcx: Mcx<'_>, msgtype: u8, s: &[u8]) -> PgResult<()> {
    match pg_server_to_client::call(mcx, s)? {
        Some(mut p) => {
            // (void) pq_putmessage(msgtype, p, strlen(p) + 1); pfree(p) —
            // C discards only the int EOF result.
            p.try_reserve(1).map_err(|_| mcx.oom(1))?;
            p.push(0);
            let _eof = pq_putmessage::call(msgtype, &p)?;
        }
        None => {
            // (void) pq_putmessage(msgtype, str, slen + 1) — the C body
            // includes the caller's NUL terminator, which a Rust slice does
            // not carry; materialize it.
            let mut body = ::mcx::vec_with_capacity_in::<u8>(mcx, s.len() + 1)?;
            body.extend_from_slice(s);
            body.push(0);
            let _eof = pq_putmessage::call(msgtype, &body)?;
        }
    }
    Ok(())
}

/// `pq_putemptymessage` — convenience routine for a message with empty body.
/// Same failure surface as [`pq_endmessage`] (C discards only the EOF
/// result).
pub fn pq_putemptymessage(msgtype: u8) -> PgResult<()> {
    let _eof = pq_putmessage::call(msgtype, &[])?;
    Ok(())
}

// ===========================================================================
// Message parsing after input
// ===========================================================================

/// The `ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg(...)))`
/// the readers raise on a malformed or truncated message.
fn protocol_violation(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
}

/// `pq_getmsgbyte` — get a raw byte from a message buffer.
///
/// C returns `(unsigned char) msg->data[msg->cursor++]` widened to `int`; the
/// `i32` is always in `0..=255`.
pub fn pq_getmsgbyte(msg: &mut StringInfo<'_>) -> PgResult<i32> {
    if msg.cursor >= msg.data.len() {
        return Err(protocol_violation("no data left in message"));
    }
    let b = msg.data[msg.cursor];
    msg.cursor += 1;
    Ok(b as i32)
}

/// `pq_getmsgint` — get a binary integer of width `b` ∈ {1, 2, 4} from a
/// message buffer; values are treated as unsigned. Any other width is
/// `elog(ERROR, "unsupported integer size %d", b)`.
pub fn pq_getmsgint(msg: &mut StringInfo<'_>, b: i32) -> PgResult<u32> {
    match b {
        1 => {
            let mut n8 = [0u8; 1];
            pq_copymsgbytes(msg, &mut n8)?;
            Ok(n8[0] as u32)
        }
        2 => {
            let mut n16 = [0u8; 2];
            pq_copymsgbytes(msg, &mut n16)?;
            Ok(u16::from_be_bytes(n16) as u32)
        }
        4 => {
            let mut n32 = [0u8; 4];
            pq_copymsgbytes(msg, &mut n32)?;
            Ok(u32::from_be_bytes(n32))
        }
        _ => Err(unsupported_integer_size(b)),
    }
}

/// `pq_getmsgint64` — get a binary 8-byte int (network order) from a message
/// buffer. (Kept separate from `pq_getmsgint` in C for performance.)
pub fn pq_getmsgint64(msg: &mut StringInfo<'_>) -> PgResult<i64> {
    let mut n64 = [0u8; 8];
    pq_copymsgbytes(msg, &mut n64)?;
    Ok(i64::from_be_bytes(n64))
}

/// `pq_getmsgfloat4` — get a float4 (see [`pq_sendfloat4`]); the C union pun
/// is `f32::from_bits`.
pub fn pq_getmsgfloat4(msg: &mut StringInfo<'_>) -> PgResult<f32> {
    Ok(f32::from_bits(pq_getmsgint(msg, 4)?))
}

/// `pq_getmsgfloat8` — get a float8 (see [`pq_sendfloat8`]).
pub fn pq_getmsgfloat8(msg: &mut StringInfo<'_>) -> PgResult<f64> {
    Ok(f64::from_bits(pq_getmsgint64(msg)? as u64))
}

/// `pq_getmsgbytes` — get raw data from a message buffer, returned as a
/// borrow directly into the buffer (C returns `&msg->data[cursor]`).
///
/// C also rejects `datalen < 0`, which `usize` makes unrepresentable; the
/// insufficient-data half of the check is preserved exactly.
pub fn pq_getmsgbytes<'a>(msg: &'a mut StringInfo<'_>, datalen: usize) -> PgResult<&'a [u8]> {
    if datalen > msg.data.len().saturating_sub(msg.cursor) {
        return Err(protocol_violation("insufficient data left in message"));
    }
    let start = msg.cursor;
    msg.cursor += datalen;
    Ok(&msg.data[start..start + datalen])
}

/// `pq_copymsgbytes` — copy raw data from a message buffer into the caller's
/// buffer. The copy length is `buf.len()` (every C call site passes
/// `sizeof(dest)`).
pub fn pq_copymsgbytes(msg: &mut StringInfo<'_>, buf: &mut [u8]) -> PgResult<()> {
    let datalen = buf.len();
    if datalen > msg.data.len().saturating_sub(msg.cursor) {
        return Err(protocol_violation("insufficient data left in message"));
    }
    let start = msg.cursor;
    buf.copy_from_slice(&msg.data[start..start + datalen]);
    msg.cursor += datalen;
    Ok(())
}

/// `pq_getmsgtext` — get a counted text string (with conversion).
///
/// Always returns freshly allocated bytes charged to `mcx` (C: "a freshly
/// palloc'd result" with a trailing NUL, plus `*nbytes = strlen`). The
/// returned vec holds the string contents without the terminator; its length
/// is C's `*nbytes` in both branches (converted: `strlen(p)`; unconverted:
/// `rawbytes`).
pub fn pq_getmsgtext<'mcx>(
    mcx: Mcx<'mcx>,
    msg: &mut StringInfo<'_>,
    rawbytes: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    if rawbytes > msg.data.len().saturating_sub(msg.cursor) {
        return Err(protocol_violation("insufficient data left in message"));
    }
    let start = msg.cursor;
    msg.cursor += rawbytes;
    let raw = &msg.data[start..start + rawbytes];

    match pg_client_to_server::call(mcx, raw)? {
        Some(p) => Ok(p),
        // No conversion: palloc(rawbytes + 1) + memcpy + NUL — i.e. a copy.
        None => ::mcx::slice_in(mcx, raw),
    }
}

/// The result of a converting string read ([`pq_getmsgstring`]): C returns
/// either a pointer directly into the message buffer (no conversion) or a
/// palloc'd conversion result.
pub enum PqString<'a, 'mcx> {
    /// No conversion was needed; borrowed directly from the message buffer.
    Borrowed(&'a [u8]),
    /// Conversion produced a fresh allocation in the caller's context.
    Converted(PgVec<'mcx, u8>),
}

impl core::fmt::Debug for PqString<'_, '_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match self {
            PqString::Borrowed(_) => "PqString::Borrowed",
            PqString::Converted(_) => "PqString::Converted",
        };
        f.debug_tuple(name).field(&self.as_bytes()).finish()
    }
}

impl PqString<'_, '_> {
    /// The string bytes (without any NUL terminator), whichever case applies.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PqString::Borrowed(b) => b,
            PqString::Converted(v) => v,
        }
    }
}

/// Shared scan behind the NUL-terminated readers: the length of the string at
/// the cursor, validated with C's `msg->cursor + slen >= msg->len` bound.
///
/// C's `strlen` is safe only because StringInfo guarantees a trailing NUL
/// sentinel; without the sentinel, "no NUL found" is exactly the case where
/// C's strlen would have run to the sentinel at index `len` and then failed
/// the same bound.
fn scan_cstring_len(msg: &StringInfo<'_>) -> PgResult<usize> {
    let len = msg.data.len();
    let slen = match msg.data[msg.cursor..].iter().position(|&b| b == 0) {
        Some(off) => off,
        None => len - msg.cursor,
    };
    if msg.cursor + slen >= len {
        return Err(protocol_violation("invalid string in message"));
    }
    Ok(slen)
}

/// `pq_getmsgstring` — get a NUL-terminated text string (with conversion).
/// May return a borrow directly into the message buffer, or the conversion
/// result allocated in `mcx`. The cursor advances past the terminator.
pub fn pq_getmsgstring<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    msg: &'a mut StringInfo<'_>,
) -> PgResult<PqString<'a, 'mcx>> {
    let slen = scan_cstring_len(msg)?;
    let start = msg.cursor;
    msg.cursor += slen + 1;
    let raw = &msg.data[start..start + slen];
    match pg_client_to_server::call(mcx, raw)? {
        Some(p) => Ok(PqString::Converted(p)),
        None => Ok(PqString::Borrowed(raw)),
    }
}

/// `pq_getmsgrawstring` — get a NUL-terminated text string with NO
/// conversion; returns a borrow directly into the message buffer. The cursor
/// advances past the terminator.
pub fn pq_getmsgrawstring<'a>(msg: &'a mut StringInfo<'_>) -> PgResult<&'a [u8]> {
    let slen = scan_cstring_len(msg)?;
    let start = msg.cursor;
    msg.cursor += slen + 1;
    Ok(&msg.data[start..start + slen])
}

/// `pq_getmsgend` — verify the message was fully consumed
/// (`msg->cursor == msg->len`).
pub fn pq_getmsgend(msg: &StringInfo<'_>) -> PgResult<()> {
    if msg.cursor != msg.data.len() {
        return Err(protocol_violation("invalid message format"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
