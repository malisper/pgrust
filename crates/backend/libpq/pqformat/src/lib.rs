#![forbid(unsafe_code)]

use datum::{Bytea, VARHDRSZ};
use mbutils::pg_client_to_server;
use mbutils_seams::pg_server_to_client;
use mcx::{Mcx, PgVec};
use pqcomm_seams::pq_putmessage;
use stringinfo::StringInfo;
use types_error::{PgError, PgResult, ERRCODE_PROTOCOL_VIOLATION};

pub fn init_seams() {}

pub fn pq_beginmessage(mcx: Mcx<'_>, msgtype: u8) -> PgResult<StringInfo<'_>> {
    let mut buf = StringInfo::new_in(mcx)?;
    // C stashes msgtype in the cursor field, which pq_send* never touch.
    buf.cursor = msgtype as usize;
    Ok(buf)
}

#[inline]
pub fn pq_beginmessage_reuse(buf: &mut StringInfo<'_>, msgtype: u8) {
    buf.reset();
    buf.cursor = msgtype as usize;
}

pub fn pq_sendbytes(buf: &mut StringInfo<'_>, data: &[u8]) -> PgResult<()> {
    buf.append_bytes(data)
}

pub fn pq_sendcountedtext(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    match pg_server_to_client::call(buf.allocator(), s)? {
        Some(p) => {
            pq_sendint32(buf, p.len() as u32)?;
            buf.append_bytes_nt(&p)
        }
        None => {
            pq_sendint32(buf, s.len() as u32)?;
            buf.append_bytes_nt(s)
        }
    }
}

pub fn pq_sendtext(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    match pg_server_to_client::call(buf.allocator(), s)? {
        Some(p) => buf.append_bytes(&p),
        None => buf.append_bytes(s),
    }
}

/// `s` excludes the NUL terminator (the Rust analog of a `char *`); the
/// appended bytes include one, as C's `appendBinaryStringInfoNT(buf, p, slen + 1)`.
pub fn pq_sendstring(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    match pg_server_to_client::call(buf.allocator(), s)? {
        Some(p) => buf.append_bytes_z(&p),
        None => buf.append_bytes_z(s),
    }
}

pub fn pq_send_ascii_string(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    for &b in s {
        let ch = if b & 0x80 != 0 { b'?' } else { b };
        buf.append_byte(ch)?;
    }
    buf.append_byte(0)
}

pub fn pq_sendfloat4(buf: &mut StringInfo<'_>, f: f32) -> PgResult<()> {
    pq_sendint32(buf, f.to_bits())
}

pub fn pq_sendfloat8(buf: &mut StringInfo<'_>, f: f64) -> PgResult<()> {
    pq_sendint64(buf, f.to_bits())
}

/// C discards only pq_putmessage's EOF result ("pqcomm.c already complained");
/// an ereport(ERROR) beneath the putmessage method still propagates.
pub fn pq_endmessage(buf: StringInfo<'_>) -> PgResult<()> {
    let _eof = pq_putmessage::call(buf.cursor as u8, buf.as_bytes())?;
    Ok(())
}

pub fn pq_endmessage_reuse(buf: &StringInfo<'_>) -> PgResult<()> {
    let _eof = pq_putmessage::call(buf.cursor as u8, buf.as_bytes())?;
    Ok(())
}

#[inline]
pub fn pq_writeint8(buf: &mut StringInfo<'_>, i: u8) {
    buf.write_fixed([i]);
}

#[inline]
pub fn pq_writeint16(buf: &mut StringInfo<'_>, i: u16) {
    buf.write_fixed(i.to_be_bytes());
}

#[inline]
pub fn pq_writeint32(buf: &mut StringInfo<'_>, i: u32) {
    buf.write_fixed(i.to_be_bytes());
}

#[inline]
pub fn pq_writeint64(buf: &mut StringInfo<'_>, i: u64) {
    buf.write_fixed(i.to_be_bytes());
}

/// Preallocated space must cover the string after conversion (C NB), plus its
/// NUL. Same observable behavior as [`pq_sendstring`] here; the enlarge check
/// C's prealloc contract skips is a no-op branch.
pub fn pq_writestring(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    pq_sendstring(buf, s)
}

#[inline]
pub fn pq_sendint8(buf: &mut StringInfo<'_>, i: u8) -> PgResult<()> {
    buf.append_bytes_nt(&[i])
}

#[inline]
pub fn pq_sendint16(buf: &mut StringInfo<'_>, i: u16) -> PgResult<()> {
    buf.append_bytes_nt(&i.to_be_bytes())
}

#[inline]
pub fn pq_sendint32(buf: &mut StringInfo<'_>, i: u32) -> PgResult<()> {
    buf.append_bytes_nt(&i.to_be_bytes())
}

#[inline]
pub fn pq_sendint64(buf: &mut StringInfo<'_>, i: u64) -> PgResult<()> {
    buf.append_bytes_nt(&i.to_be_bytes())
}

#[inline]
pub fn pq_sendbyte(buf: &mut StringInfo<'_>, byt: u8) -> PgResult<()> {
    pq_sendint8(buf, byt)
}

/// Deprecated in C; widths other than 1/2/4 are elog(ERROR).
pub fn pq_sendint(buf: &mut StringInfo<'_>, i: u32, b: i32) -> PgResult<()> {
    match b {
        1 => pq_sendint8(buf, i as u8),
        2 => pq_sendint16(buf, i as u16),
        4 => pq_sendint32(buf, i),
        _ => Err(unsupported_integer_size(b)),
    }
}

pub fn pq_begintypsend(mcx: Mcx<'_>) -> PgResult<StringInfo<'_>> {
    let mut buf = StringInfo::new_in(mcx)?;
    // Reserve four bytes for the bytea length word.
    buf.append_bytes(&[0u8; VARHDRSZ])?;
    Ok(buf)
}

/// C stamps `SET_VARSIZE(result, buf->len)` on the reserved word and returns
/// `buf->data` as the bytea; `Bytea::from_image` owns that header encoding.
pub fn pq_endtypsend(buf: StringInfo<'_>) -> Bytea<'_> {
    Bytea::from_image(buf.into_vec())
}

pub fn pq_puttextmessage(mcx: Mcx<'_>, msgtype: u8, s: &[u8]) -> PgResult<()> {
    match pg_server_to_client::call(mcx, s)? {
        Some(mut p) => {
            p.try_reserve(1).map_err(|_| mcx.oom(p.len() + 1))?;
            p.push(0);
            let _eof = pq_putmessage::call(msgtype, &p)?;
        }
        None => {
            // C transmits the caller's NUL terminator, which a slice does not
            // carry; materialize slen + 1 bytes.
            let mut body = mcx::vec_with_capacity_in::<u8>(mcx, s.len() + 1)?;
            mcx::vec_append_bytes(&mut body, s)?;
            body.push(0);
            let _eof = pq_putmessage::call(msgtype, &body)?;
        }
    }
    Ok(())
}

pub fn pq_putemptymessage(msgtype: u8) -> PgResult<()> {
    let _eof = pq_putmessage::call(msgtype, &[])?;
    Ok(())
}

#[inline]
pub fn pq_getmsgbyte(msg: &mut StringInfo<'_>) -> PgResult<i32> {
    let cursor = msg.cursor;
    match msg.as_bytes().get(cursor) {
        Some(&b) => {
            msg.cursor = cursor + 1;
            Ok(b as i32)
        }
        None => Err(protocol_violation("no data left in message")),
    }
}

#[inline]
fn getmsg_fixed<const N: usize>(msg: &mut StringInfo<'_>) -> PgResult<[u8; N]> {
    let cursor = msg.cursor;
    match msg
        .as_bytes()
        .get(cursor..)
        .and_then(|r| r.first_chunk::<N>())
        .copied()
    {
        Some(chunk) => {
            msg.cursor = cursor + N;
            Ok(chunk)
        }
        None => Err(protocol_violation("insufficient data left in message")),
    }
}

/// Values are treated as unsigned; widths other than 1/2/4 are elog(ERROR).
pub fn pq_getmsgint(msg: &mut StringInfo<'_>, b: i32) -> PgResult<u32> {
    match b {
        1 => Ok(getmsg_fixed::<1>(msg)?[0] as u32),
        2 => Ok(u16::from_be_bytes(getmsg_fixed(msg)?) as u32),
        4 => Ok(u32::from_be_bytes(getmsg_fixed(msg)?)),
        _ => Err(unsupported_integer_size(b)),
    }
}

pub fn pq_getmsgint64(msg: &mut StringInfo<'_>) -> PgResult<i64> {
    Ok(i64::from_be_bytes(getmsg_fixed(msg)?))
}

pub fn pq_getmsgfloat4(msg: &mut StringInfo<'_>) -> PgResult<f32> {
    Ok(f32::from_bits(pq_getmsgint(msg, 4)?))
}

pub fn pq_getmsgfloat8(msg: &mut StringInfo<'_>) -> PgResult<f64> {
    Ok(f64::from_bits(pq_getmsgint64(msg)? as u64))
}

/// Borrows directly into the message buffer (C returns `&msg->data[cursor]`);
/// C's `datalen < 0` half of the check is unrepresentable with usize.
#[inline]
pub fn pq_getmsgbytes<'a>(msg: &'a mut StringInfo<'_>, datalen: usize) -> PgResult<&'a [u8]> {
    let cursor = msg.cursor;
    if datalen > msg.len().saturating_sub(cursor) || cursor > msg.len() {
        return Err(protocol_violation("insufficient data left in message"));
    }
    msg.cursor = cursor + datalen;
    Ok(&msg.as_bytes()[cursor..cursor + datalen])
}

/// Copy length is `buf.len()` (every C call site passes `sizeof(dest)`).
#[inline]
pub fn pq_copymsgbytes(msg: &mut StringInfo<'_>, buf: &mut [u8]) -> PgResult<()> {
    let datalen = buf.len();
    let cursor = msg.cursor;
    match msg.as_bytes().get(cursor..).and_then(|r| r.get(..datalen)) {
        Some(src) => {
            buf.copy_from_slice(src);
            msg.cursor = cursor + datalen;
            Ok(())
        }
        None => Err(protocol_violation("insufficient data left in message")),
    }
}

/// Freshly allocated in `mcx` (C: palloc'd + trailing NUL); the vec's length
/// is C's `*nbytes` in both branches.
pub fn pq_getmsgtext<'mcx>(
    mcx: Mcx<'mcx>,
    msg: &mut StringInfo<'_>,
    rawbytes: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    let cursor = msg.cursor;
    if rawbytes > msg.len().saturating_sub(cursor) || cursor > msg.len() {
        return Err(protocol_violation("insufficient data left in message"));
    }
    msg.cursor = cursor + rawbytes;
    let raw = &msg.as_bytes()[cursor..cursor + rawbytes];
    match pg_client_to_server(mcx, raw)? {
        Some(p) => Ok(p),
        None => mcx::slice_in(mcx, raw),
    }
}

/// C returns either a pointer into the message buffer (no conversion) or a
/// palloc'd conversion result; pointer identity becomes the two variants.
#[derive(Debug)]
pub enum PqString<'a, 'mcx> {
    Borrowed(&'a [u8]),
    Converted(PgVec<'mcx, u8>),
}

impl PqString<'_, '_> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PqString::Borrowed(b) => b,
            PqString::Converted(v) => v,
        }
    }
}

// C strlen()s from the cursor, safe only because of StringInfo's NUL
// sentinel; "no NUL before len" fails C's cursor + slen >= len bound the same
// way (strlen stops at the sentinel at index len).
fn scan_cstring_len(msg: &StringInfo<'_>) -> PgResult<usize> {
    let bytes = msg.as_bytes();
    let cursor = msg.cursor;
    match bytes.get(cursor..).and_then(|r| r.iter().position(|&b| b == 0)) {
        Some(slen) => Ok(slen),
        None => Err(protocol_violation("invalid string in message")),
    }
}

pub fn pq_getmsgstring<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    msg: &'a mut StringInfo<'_>,
) -> PgResult<PqString<'a, 'mcx>> {
    let slen = scan_cstring_len(msg)?;
    let start = msg.cursor;
    msg.cursor = start + slen + 1;
    let raw = &msg.as_bytes()[start..start + slen];
    match pg_client_to_server(mcx, raw)? {
        Some(p) => Ok(PqString::Converted(p)),
        None => Ok(PqString::Borrowed(raw)),
    }
}

pub fn pq_getmsgrawstring<'a>(msg: &'a mut StringInfo<'_>) -> PgResult<&'a [u8]> {
    let slen = scan_cstring_len(msg)?;
    let start = msg.cursor;
    msg.cursor = start + slen + 1;
    Ok(&msg.as_bytes()[start..start + slen])
}

pub fn pq_getmsgend(msg: &StringInfo<'_>) -> PgResult<()> {
    if msg.cursor != msg.len() {
        return Err(protocol_violation("invalid message format"));
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn protocol_violation(msg: &'static str) -> Box<PgError> {
    PgError::error(msg)
        .with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
        .into()
}

#[track_caller]
#[cold]
#[inline(never)]
fn unsupported_integer_size(b: i32) -> Box<PgError> {
    PgError::error(format!("unsupported integer size {b}")).into()
}

#[cfg(test)]
mod tests;
