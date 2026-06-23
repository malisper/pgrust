//! Byte-exact v3 frontend/backend wire-protocol codec.
//!
//! This is the byte-level message codec of `fe-protocol3.c` (the `pqGetc` /
//! `pqGetInt` / `pqGets` / `pqGetnchar` readers and the `pqPutMsgStart` /
//! `pqPutInt` / `pqPutnchar` / `pqPutMsgEnd` writers from `fe-misc.c`), modelled
//! over owned buffers rather than the live `PGconn` in/out arena.
//!
//! All integers are network byte order (big-endian) exactly as the protocol
//! mandates (`pg_hton16`/`pg_hton32` on the wire; `pqGetInt` reads BE). A v3
//! backend message is `type_byte:u8`, `length:i32` (BE, includes the 4 length
//! bytes but not the type byte), then `length - 4` body bytes. The startup
//! message is the one exception: no type byte, `length:i32` includes itself.

use crate::transport::TransportError;

// ===========================================================================
// Backend message type codes (`src/include/libpq/protocol.h`, the PqMsg_*
// backend-to-frontend names). Transcribed 1:1.
// ===========================================================================

/// `PqMsg_AuthenticationRequest` ('R').
pub const B_AUTH: u8 = b'R';
/// `PqMsg_ParameterStatus` ('S').
pub const B_PARAMETER_STATUS: u8 = b'S';
/// `PqMsg_BackendKeyData` ('K').
pub const B_BACKEND_KEY_DATA: u8 = b'K';
/// `PqMsg_ReadyForQuery` ('Z').
pub const B_READY_FOR_QUERY: u8 = b'Z';
/// `PqMsg_RowDescription` ('T').
pub const B_ROW_DESCRIPTION: u8 = b'T';
/// `PqMsg_DataRow` ('D').
pub const B_DATA_ROW: u8 = b'D';
/// `PqMsg_CommandComplete` ('C').
pub const B_COMMAND_COMPLETE: u8 = b'C';
/// `PqMsg_EmptyQueryResponse` ('I').
pub const B_EMPTY_QUERY: u8 = b'I';
/// `PqMsg_ErrorResponse` ('E').
pub const B_ERROR_RESPONSE: u8 = b'E';
/// `PqMsg_NoticeResponse` ('N').
pub const B_NOTICE_RESPONSE: u8 = b'N';
/// `PqMsg_NotificationResponse` ('A').
pub const B_NOTIFICATION_RESPONSE: u8 = b'A';
/// `PqMsg_CopyInResponse` ('G').
pub const B_COPY_IN_RESPONSE: u8 = b'G';
/// `PqMsg_CopyOutResponse` ('H').
pub const B_COPY_OUT_RESPONSE: u8 = b'H';
/// `PqMsg_CopyBothResponse` ('W').
pub const B_COPY_BOTH_RESPONSE: u8 = b'W';
/// `PqMsg_CopyData` ('d').
pub const B_COPY_DATA: u8 = b'd';
/// `PqMsg_CopyDone` ('c').
pub const B_COPY_DONE: u8 = b'c';
/// `PqMsg_NegotiateProtocolVersion` ('v').
pub const B_NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';
/// `PqMsg_ParameterDescription` ('t').
pub const B_PARAMETER_DESCRIPTION: u8 = b't';
/// `PqMsg_NoData` ('n').
pub const B_NO_DATA: u8 = b'n';

// ===========================================================================
// Frontend message type codes (`protocol.h`, the frontend-to-backend names).
// ===========================================================================

/// `PqMsg_Query` ('Q').
pub const F_QUERY: u8 = b'Q';
/// `PqMsg_Terminate` ('X').
pub const F_TERMINATE: u8 = b'X';
/// `PqMsg_CopyData` ('d').
pub const F_COPY_DATA: u8 = b'd';
/// `PqMsg_CopyDone` ('c').
pub const F_COPY_DONE: u8 = b'c';
/// `PqMsg_PasswordMessage` ('p').
pub const F_PASSWORD_MESSAGE: u8 = b'p';

// ===========================================================================
// AuthenticationRequest sub-codes (`src/include/libpq/protocol.h`,
// AUTH_REQ_*). Only the ones the trust/cleartext path inspects.
// ===========================================================================

/// `AUTH_REQ_OK` — auth completed.
pub const AUTH_REQ_OK: i32 = 0;
/// `AUTH_REQ_KRB4` — obsolete.
pub const AUTH_REQ_KRB4: i32 = 1;
/// `AUTH_REQ_KRB5` — obsolete.
pub const AUTH_REQ_KRB5: i32 = 2;
/// `AUTH_REQ_PASSWORD` — cleartext password wanted.
pub const AUTH_REQ_PASSWORD: i32 = 3;
/// `AUTH_REQ_MD5` — MD5 password wanted.
pub const AUTH_REQ_MD5: i32 = 5;
/// `AUTH_REQ_GSS` — GSSAPI.
pub const AUTH_REQ_GSS: i32 = 7;
/// `AUTH_REQ_GSS_CONT` — GSSAPI continue.
pub const AUTH_REQ_GSS_CONT: i32 = 8;
/// `AUTH_REQ_SSPI` — SSPI.
pub const AUTH_REQ_SSPI: i32 = 9;
/// `AUTH_REQ_SASL` — SASL wanted.
pub const AUTH_REQ_SASL: i32 = 10;
/// `AUTH_REQ_SASL_CONT` — SASL continue.
pub const AUTH_REQ_SASL_CONT: i32 = 11;
/// `AUTH_REQ_SASL_FIN` — SASL final.
pub const AUTH_REQ_SASL_FIN: i32 = 12;

// ===========================================================================
// ErrorResponse / NoticeResponse field codes (`PG_DIAG_*`,
// `src/include/postgres_ext.h`). Only the ones consumers read.
// ===========================================================================

/// `PG_DIAG_SEVERITY` ('S').
pub const PG_DIAG_SEVERITY: u8 = b'S';
/// `PG_DIAG_SQLSTATE` ('C').
pub const PG_DIAG_SQLSTATE: u8 = b'C';
/// `PG_DIAG_MESSAGE_PRIMARY` ('M').
pub const PG_DIAG_MESSAGE_PRIMARY: u8 = b'M';

// ===========================================================================
// One fully-received backend message: its type byte plus its body
// (`length - 4` bytes). This is the unit `read_message` yields and the
// per-message readers below consume.
// ===========================================================================

/// A complete backend message read off the wire: the 1-byte type code and the
/// raw body (already length-stripped). `read_*` cursors below walk `body`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendMessage {
    /// The message type byte (`B_*` above).
    pub kind: u8,
    /// The message body — exactly `length - 4` bytes, no type byte, no length.
    pub body: Vec<u8>,
}

// ===========================================================================
// A cursor that reads big-endian integers / C strings / raw bytes out of a
// message body, mirroring `pqGetInt(n,2/4)` / `pqGets` / `pqGetnchar` exactly.
// A read past the end is the C "insufficient data" failure (the C readers
// return non-zero, the callers report a protocol error).
// ===========================================================================

/// Read cursor over a backend message body. The C readers advance `inCursor`;
/// here the cursor is `pos` into `body`. Every `get_*` returns `Err` on
/// underflow, the analog of the C "insufficient data in \"X\" message" path.
pub struct MsgReader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> MsgReader<'a> {
    /// Start reading a message body.
    pub fn new(body: &'a [u8]) -> Self {
        MsgReader { body, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.body.len() - self.pos
    }

    /// Whether the whole body has been consumed (the C "message fully read"
    /// invariant the parse loop asserts).
    pub fn at_end(&self) -> bool {
        self.pos >= self.body.len()
    }

    /// `pqGetc(&c, conn)` — read one byte.
    pub fn get_u8(&mut self) -> Result<u8, TransportError> {
        if self.pos >= self.body.len() {
            return Err(TransportError::ProtocolViolation);
        }
        let b = self.body[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// `pqGetInt(&v, 2, conn)` — read a 2-byte unsigned big-endian integer.
    /// (The C code coerces to `int16` at the call site where signedness matters;
    /// the raw read is unsigned, matching `pqGetInt`.)
    pub fn get_u16(&mut self) -> Result<u16, TransportError> {
        if self.remaining() < 2 {
            return Err(TransportError::ProtocolViolation);
        }
        let v = u16::from_be_bytes([self.body[self.pos], self.body[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    /// `pqGetInt(&v, 2, conn)` then `(int)(int16)v` — the signed-coerced 2-byte
    /// read used for `columnid` / `typlen` / `format`.
    pub fn get_i16(&mut self) -> Result<i32, TransportError> {
        Ok(self.get_u16()? as i16 as i32)
    }

    /// `pqGetInt(&v, 4, conn)` — read a 4-byte big-endian integer (signed, as
    /// the C `int` reads it; the protocol uses these as both signed lengths and
    /// unsigned OIDs depending on field).
    pub fn get_i32(&mut self) -> Result<i32, TransportError> {
        if self.remaining() < 4 {
            return Err(TransportError::ProtocolViolation);
        }
        let v = i32::from_be_bytes([
            self.body[self.pos],
            self.body[self.pos + 1],
            self.body[self.pos + 2],
            self.body[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// 4-byte big-endian read interpreted as an unsigned OID/u32 (the same wire
    /// bytes as `get_i32`, named where the field is an OID/`uint32`).
    pub fn get_u32(&mut self) -> Result<u32, TransportError> {
        Ok(self.get_i32()? as u32)
    }

    /// `pqGets(&buf, conn)` — read a NUL-terminated string, returning the bytes
    /// up to (not including) the NUL. The NUL is consumed.
    pub fn get_cstr(&mut self) -> Result<Vec<u8>, TransportError> {
        let start = self.pos;
        while self.pos < self.body.len() {
            if self.body[self.pos] == 0 {
                let mut out = Vec::new();
                out.try_reserve(self.pos - start)
                    .map_err(|_| TransportError::OutOfMemory)?;
                out.extend_from_slice(&self.body[start..self.pos]);
                self.pos += 1; // consume the NUL
                return Ok(out);
            }
            self.pos += 1;
        }
        // No terminator found before end-of-body: "insufficient data".
        Err(TransportError::ProtocolViolation)
    }

    /// `pqGetnchar(buf, n, conn)` — read exactly `n` raw bytes (copied out).
    pub fn get_nbytes(&mut self, n: usize) -> Result<Vec<u8>, TransportError> {
        if self.remaining() < n {
            return Err(TransportError::ProtocolViolation);
        }
        let mut out = Vec::new();
        out.try_reserve(n).map_err(|_| TransportError::OutOfMemory)?;
        out.extend_from_slice(&self.body[self.pos..self.pos + n]);
        self.pos += n;
        Ok(out)
    }
}

// ===========================================================================
// Message-frame writer: the frontend `pqPutMsgStart` / `pqPutInt` /
// `pqPutnchar` / `pqPutMsgEnd` sequence, building one complete framed message
// into an owned buffer. `pqPutMsgStart(type, conn)` writes the type byte then
// reserves 4 bytes for the length; `pqPutMsgEnd` backpatches the length
// (`pg_hton32(buffer_used - msgStart)`, i.e. length INCLUDES the 4 length bytes
// but NOT the type byte).
// ===========================================================================

/// Build one complete framed frontend message (`type` byte, then a 4-byte
/// big-endian length that covers the length bytes + body, then `body`).
/// 1:1 with `pqPutMsgStart`/`pqPutMsgEnd` for a non-zero message type.
pub fn build_message(msg_type: u8, body: &[u8]) -> Result<Vec<u8>, TransportError> {
    let mut out = Vec::new();
    // 1 type byte + 4 length bytes + body.
    out.try_reserve(1 + 4 + body.len())
        .map_err(|_| TransportError::OutOfMemory)?;
    out.push(msg_type);
    // length = 4 (the length field itself) + body length, big-endian.
    let len = (4 + body.len()) as i32;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// Build the startup message frame (`pqPacketSend(conn, 0, packet, packetlen)`):
/// type byte ZERO means NO type byte is emitted, only the 4-byte length (which
/// includes itself) followed by the packet body. `body` is the already-assembled
/// startup-packet bytes from `pqBuildStartupPacket3` (protocol-version word +
/// option pairs + trailing NUL).
pub fn build_startup_message(body: &[u8]) -> Result<Vec<u8>, TransportError> {
    let mut out = Vec::new();
    out.try_reserve(4 + body.len())
        .map_err(|_| TransportError::OutOfMemory)?;
    let len = (4 + body.len()) as i32;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_integers_big_endian() {
        let body = vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x2a, 0xff, 0xfe];
        let mut r = MsgReader::new(&body);
        assert_eq!(r.get_u16().unwrap(), 1);
        assert_eq!(r.get_i32().unwrap(), 42);
        // 0xfffe as int16 == -2
        assert_eq!(r.get_i16().unwrap(), -2);
        assert!(r.at_end());
    }

    #[test]
    fn reader_cstr_consumes_nul() {
        let body = b"hello\0world\0".to_vec();
        let mut r = MsgReader::new(&body);
        assert_eq!(r.get_cstr().unwrap(), b"hello");
        assert_eq!(r.get_cstr().unwrap(), b"world");
        assert!(r.at_end());
    }

    #[test]
    fn reader_underflow_is_protocol_violation() {
        let body = vec![0x00];
        let mut r = MsgReader::new(&body);
        assert!(matches!(r.get_u16(), Err(TransportError::ProtocolViolation)));
    }

    #[test]
    fn reader_cstr_without_nul_is_protocol_violation() {
        let body = b"nonul".to_vec();
        let mut r = MsgReader::new(&body);
        assert!(matches!(r.get_cstr(), Err(TransportError::ProtocolViolation)));
    }

    #[test]
    fn build_query_message_framing() {
        // Query message: 'Q', len (4 + "SELECT 1\0".len()), body.
        let body = b"SELECT 1\0";
        let msg = build_message(F_QUERY, body).unwrap();
        assert_eq!(msg[0], b'Q');
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(len as usize, 4 + body.len());
        assert_eq!(&msg[5..], body);
        // total on the wire = 1 type byte + length-field-value.
        assert_eq!(msg.len(), 1 + len as usize);
    }

    #[test]
    fn build_startup_message_no_type_byte() {
        // protocol 3.0 word + "user\0postgres\0\0"
        let mut packet = Vec::new();
        packet.extend_from_slice(&0x0003_0000_u32.to_be_bytes());
        packet.extend_from_slice(b"user\0postgres\0\0");
        let msg = build_startup_message(&packet).unwrap();
        let len = i32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(len as usize, 4 + packet.len());
        assert_eq!(&msg[4..], &packet[..]);
        // length INCLUDES itself: total bytes == len.
        assert_eq!(msg.len(), len as usize);
    }
}
