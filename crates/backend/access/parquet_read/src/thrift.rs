//! Hand-rolled thrift compact-protocol SKIP-PARSER for parquet metadata.
//!
//! Materializes only the fields the reader needs and skips everything else
//! (statistics, page indexes, key/value metadata) without decoding — the
//! convergent-law win every fast reader re-derived (generated thrift parsers
//! leave 3-9x on the table). No thrift crate dependency.
//!
//! Every read is bounds-checked and recursion is depth-capped: malformed
//! input yields a typed error, never a panic and never an OOM-sized
//! allocation (list/binary sizes are validated against remaining bytes
//! before any allocation).

use types_error::{PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT};

pub(crate) const T_BOOL_TRUE: u8 = 1;
pub(crate) const T_BOOL_FALSE: u8 = 2;
pub(crate) const T_BYTE: u8 = 3;
pub(crate) const T_I16: u8 = 4;
pub(crate) const T_I32: u8 = 5;
pub(crate) const T_I64: u8 = 6;
pub(crate) const T_DOUBLE: u8 = 7;
pub(crate) const T_BINARY: u8 = 8;
pub(crate) const T_LIST: u8 = 9;
pub(crate) const T_SET: u8 = 10;
pub(crate) const T_MAP: u8 = 11;
pub(crate) const T_STRUCT: u8 = 12;

/// Parquet metadata nests ~6 deep; unknown skipped structs get generous
/// headroom, but a crafted file cannot recurse unboundedly.
const MAX_DEPTH: u32 = 60;

#[cold]
#[inline(never)]
pub(crate) fn corrupt(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid parquet metadata: {what}"))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
    )
}

pub(crate) struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Cur { buf, pos: 0 }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    #[inline]
    pub fn u8(&mut self) -> PgResult<u8> {
        let Some(&b) = self.buf.get(self.pos) else {
            return Err(corrupt("unexpected end of metadata"));
        };
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    pub fn bytes(&mut self, n: usize) -> PgResult<&'a [u8]> {
        if n > self.remaining() {
            return Err(corrupt("unexpected end of metadata"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// ULEB128, capped at 10 bytes (u64).
    #[inline]
    pub fn varint(&mut self) -> PgResult<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = self.u8()?;
            if shift == 63 && b > 1 {
                return Err(corrupt("varint overflows u64"));
            }
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                return Err(corrupt("varint longer than 10 bytes"));
            }
        }
    }

    #[inline]
    pub fn zig_i64(&mut self) -> PgResult<i64> {
        let v = self.varint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }

    #[inline]
    pub fn zig_i32(&mut self) -> PgResult<i32> {
        let v = self.zig_i64()?;
        i32::try_from(v).map_err(|_| corrupt("i32 field out of range"))
    }

    #[inline]
    pub fn zig_i16(&mut self) -> PgResult<i16> {
        let v = self.zig_i64()?;
        i16::try_from(v).map_err(|_| corrupt("i16 field out of range"))
    }

    /// Compact-protocol binary/string: varint length + payload.
    pub fn binary(&mut self) -> PgResult<&'a [u8]> {
        let n = self.varint()?;
        let n = usize::try_from(n).map_err(|_| corrupt("binary length out of range"))?;
        if n > self.remaining() {
            return Err(corrupt("binary length exceeds metadata size"));
        }
        self.bytes(n)
    }

    /// Struct field header. `None` at STOP. Returns (compact type, field id).
    /// Bool fields carry their value in the type nibble (T_BOOL_TRUE/FALSE).
    pub fn field(&mut self, last_id: &mut i16) -> PgResult<Option<(u8, i16)>> {
        let b = self.u8()?;
        if b == 0 {
            return Ok(None);
        }
        let t = b & 0x0f;
        if t > T_STRUCT {
            return Err(corrupt("invalid compact field type"));
        }
        let delta = (b >> 4) as i16;
        let id = if delta == 0 {
            self.zig_i16()?
        } else {
            last_id.checked_add(delta).ok_or_else(|| corrupt("field id overflow"))?
        };
        *last_id = id;
        Ok(Some((t, id)))
    }

    /// List/set header: (element type, count). Count is validated against the
    /// remaining bytes (each element occupies at least one byte).
    pub fn list_header(&mut self) -> PgResult<(u8, usize)> {
        let b = self.u8()?;
        let et = b & 0x0f;
        let n = (b >> 4) as usize;
        let n = if n == 15 {
            usize::try_from(self.varint()?).map_err(|_| corrupt("list size out of range"))?
        } else {
            n
        };
        if et == 0 || et > T_STRUCT {
            return Err(corrupt("invalid list element type"));
        }
        if n > self.remaining() {
            return Err(corrupt("list size exceeds metadata size"));
        }
        Ok((et, n))
    }

    pub fn bool_value(&mut self, t: u8) -> PgResult<bool> {
        match t {
            T_BOOL_TRUE => Ok(true),
            T_BOOL_FALSE => Ok(false),
            _ => Err(corrupt("expected bool field")),
        }
    }

    pub fn i32_value(&mut self, t: u8) -> PgResult<i32> {
        if t != T_I32 {
            return Err(corrupt("expected i32 field"));
        }
        self.zig_i32()
    }

    pub fn i64_value(&mut self, t: u8) -> PgResult<i64> {
        if t != T_I64 {
            return Err(corrupt("expected i64 field"));
        }
        self.zig_i64()
    }

    pub fn binary_value(&mut self, t: u8) -> PgResult<&'a [u8]> {
        if t != T_BINARY {
            return Err(corrupt("expected binary field"));
        }
        self.binary()
    }

    /// Skip one value of compact type `t` as a struct-field value (bool
    /// fields carried the value in the type nibble: zero bytes follow).
    pub fn skip(&mut self, t: u8, depth: u32) -> PgResult<()> {
        if depth > MAX_DEPTH {
            return Err(corrupt("metadata nests too deeply"));
        }
        match t {
            T_BOOL_TRUE | T_BOOL_FALSE => Ok(()),
            T_BYTE => self.bytes(1).map(|_| ()),
            T_I16 | T_I32 | T_I64 => self.varint().map(|_| ()),
            T_DOUBLE => self.bytes(8).map(|_| ()),
            T_BINARY => self.binary().map(|_| ()),
            T_LIST | T_SET => {
                let (et, n) = self.list_header()?;
                self.skip_elems(et, n, depth)
            }
            T_MAP => {
                let n = self.varint()?;
                let n = usize::try_from(n).map_err(|_| corrupt("map size out of range"))?;
                if n == 0 {
                    return Ok(());
                }
                if n > self.remaining() {
                    return Err(corrupt("map size exceeds metadata size"));
                }
                let kv = self.u8()?;
                let (kt, vt) = (kv >> 4, kv & 0x0f);
                for _ in 0..n {
                    self.skip_elem(kt, depth + 1)?;
                    self.skip_elem(vt, depth + 1)?;
                }
                Ok(())
            }
            T_STRUCT => {
                let mut last_id = 0i16;
                while let Some((ft, _)) = self.field(&mut last_id)? {
                    self.skip(ft, depth + 1)?;
                }
                Ok(())
            }
            _ => Err(corrupt("invalid compact field type")),
        }
    }

    /// Skip `n` list/map elements of type `et` (bools occupy one byte here,
    /// unlike in field position).
    pub fn skip_elems(&mut self, et: u8, n: usize, depth: u32) -> PgResult<()> {
        if et == T_BOOL_TRUE || et == T_BOOL_FALSE {
            self.bytes(n).map(|_| ())
        } else {
            for _ in 0..n {
                self.skip_elem(et, depth + 1)?;
            }
            Ok(())
        }
    }

    fn skip_elem(&mut self, et: u8, depth: u32) -> PgResult<()> {
        if et == T_BOOL_TRUE || et == T_BOOL_FALSE {
            self.bytes(1).map(|_| ())
        } else {
            self.skip(et, depth)
        }
    }

    /// Position of the cursor inside the underlying buffer (for page-header
    /// parsing, where the caller resumes reading the raw stream after the
    /// thrift struct ends).
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }
}
