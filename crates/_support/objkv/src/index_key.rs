//! Index key encoding: an on-bucket format, so changing it means rewriting
//! every entry. One rule drives it -- the bytewise order of an encoded key
//! must equal the logical order of the values, since a range scan is a prefix
//! seek and a walk, and disagreement means wrong rows with no error.
//!
//! ```text
//! non-unique  {db:08x}/i/{index:08x}/{hex tuple}/{rowid:016x}/{version}
//! unique      {db:08x}/u/{index:08x}/{hex tuple}/{version}   -> rowid
//! ```
//!
//! `db` leads because oids repeat across databases; shared catalogs take
//! `00000000`. Left of the version suffix is an ordinary row key, so entries
//! get snapshot reads and tombstones free. Hex keeps bytewise order in an
//! S3-safe charset, at 2x the bytes.

use std::io;

/// Largest encoded tuple that fits an S3 key once hex-doubled and prefixed:
/// 1024 bytes less 54 of worst-case overhead. Over this, insert errors as
/// btree does -- never truncate, since a truncated key is a wrong key.
pub const MAX_ENCODED: usize = 400;

/// A column's tag byte says whether a value follows and how it is stored.
/// The four sort together as the options ask: a nulls-first NULL under
/// every value, a nulls-last NULL over every value, and a descending value
/// with each of its bytes inverted so that byte order is reversed value order.
const TAG_PRESENT: u8 = 0x01;
const TAG_PRESENT_DESC: u8 = 0xfe;
pub const TAG_NULL: u8 = 0xff;
pub const TAG_NULL_FIRST: u8 = 0x00;

/// How one column is ordered in the key. The default is what `ASC NULLS
/// LAST` means; DESC inverts the value's bytes and NULLS FIRST moves the
/// null tag below every value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColOpt {
    pub desc: bool,
    pub nulls_first: bool,
}

pub const ASC: ColOpt = ColOpt { desc: false, nulls_first: false };

pub fn is_null_tag(tag: u8) -> bool {
    tag == TAG_NULL || tag == TAG_NULL_FIRST
}

/// A column's body as the encoder wrote it before any inversion, so the
/// decoders below read one shape.
pub fn column_body(tag: u8, body: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if tag == TAG_PRESENT_DESC {
        std::borrow::Cow::Owned(body.iter().map(|b| !b).collect())
    } else {
        std::borrow::Cow::Borrowed(body)
    }
}

/// One column's value. Types the encoding refuses never reach here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Col<'a> {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Oid(u32),
    /// `"char"`: one byte, compared unsigned, so it is its own sort key. The
    /// catalogs key on it -- relkind, provolatile and friends.
    Char(u8),
    /// `name`: fixed 64 bytes, C collation, compared as a string. The trailing
    /// NUL padding is not part of the value, so it is trimmed.
    Name(&'a [u8]),
    /// `oidvector` / `int2vector`: element by element, then by length. pg_proc
    /// lookups key on these.
    Vector(&'a [u64]),
    Uuid(&'a [u8; 16]),
    Text(&'a [u8]),
    /// Both float widths, single precision widened -- exactly, and in order --
    /// so a `float8` bound can be compared against a `float4` column without
    /// rounding it first. Postgres sorts NaN above every number and treats the
    /// two zeros as one value, so both are normalised before encoding.
    Float8(f64),
}

impl Col<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Col::Null)
    }
}

/// Flipping the sign bit maps the signed range onto the unsigned one in order.
fn int_bytes(v: i64, width: usize) -> impl Iterator<Item = u8> {
    let flipped = (v as u64) ^ (1u64 << (width * 8 - 1));
    flipped.to_be_bytes().into_iter().skip(8 - width)
}

/// IEEE bits reordered so byte order is value order: flip the sign bit on a
/// positive number, invert every bit on a negative one. That puts -inf lowest
/// and NaN -- sign clear, exponent full -- above +inf, which is where Postgres
/// puts it.
fn float_bytes(v: f64) -> [u8; 8] {
    // Every NaN to one NaN and -0 to 0: Postgres compares each pair equal, and
    // two keys for one value would break a unique index.
    let v = if v.is_nan() { f64::NAN } else { v + 0.0 };
    let bits = v.to_bits();
    let top = 1u64 << 63;
    if bits & top != 0 { !bits } else { bits ^ top }.to_be_bytes()
}

fn encode_col(col: &Col<'_>, opt: ColOpt, out: &mut Vec<u8>) {
    if col.is_null() {
        out.push(if opt.nulls_first { TAG_NULL_FIRST } else { TAG_NULL });
        return;
    }
    out.push(if opt.desc { TAG_PRESENT_DESC } else { TAG_PRESENT });
    let body = out.len();
    encode_body(col, out);
    if opt.desc {
        for b in &mut out[body..] {
            *b = !*b;
        }
    }
}

fn encode_body(col: &Col<'_>, out: &mut Vec<u8>) {
    match *col {
        Col::Null => unreachable!("handled above"),
        Col::Bool(b) => out.push(b as u8),
        Col::Int2(v) => out.extend(int_bytes(v as i64, 2)),
        Col::Int4(v) => out.extend(int_bytes(v as i64, 4)),
        Col::Int8(v) => out.extend(int_bytes(v, 8)),
        Col::Oid(v) => out.extend_from_slice(&v.to_be_bytes()),
        Col::Char(c) => out.push(c),
        Col::Vector(v) => {
            // Tagged 0x01 with a 0x00 terminator, so a vector that is a prefix of
            // another sorts first.
            for e in v {
                out.push(0x01);
                out.extend_from_slice(&e.to_be_bytes());
            }
            out.push(0x00);
        }
        Col::Name(n) => {
            let end = n.iter().position(|&b| b == 0).unwrap_or(n.len());
            encode_string(&n[..end], out);
        }
        Col::Uuid(u) => out.extend_from_slice(u),
        Col::Text(s) => encode_string(s, out),
        Col::Float8(v) => out.extend_from_slice(&float_bytes(v)),
    }
}

/// 0x00 escapes to 0x00 0xff and the string ends 0x00 0x00, so the
/// terminator is always the lowest thing that can follow a 0x00.
fn encode_string(s: &[u8], out: &mut Vec<u8>) {
    for &b in s {
        out.push(b);
        if b == 0 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

// --- Reading a value back out of a key ---------------------------------------
//
// An index entry carries every key column, so a query that wants only indexed
// columns never has to fetch the row. Each of these is the exact inverse of
// the encoder above; the caller supplies the column's bytes without its tag.

/// Undoes the sign flip. `bytes` is the column's width, big-endian.
pub fn decode_int(bytes: &[u8]) -> i64 {
    let width = bytes.len();
    let mut raw = 0u64;
    for &b in bytes {
        raw = (raw << 8) | b as u64;
    }
    let flipped = raw ^ (1u64 << (width * 8 - 1));
    // Sign-extend from the column's width.
    let shift = 64 - width * 8;
    ((flipped << shift) as i64) >> shift
}

pub fn decode_uint(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0u64, |acc, &b| (acc << 8) | b as u64)
}

pub fn decode_float(bytes: &[u8; 8]) -> f64 {
    let ordered = u64::from_be_bytes(*bytes);
    let top = 1u64 << 63;
    f64::from_bits(if ordered & top != 0 { ordered ^ top } else { !ordered })
}

/// Undoes the 0x00 escape and drops the terminator.
pub fn decode_string(bytes: &[u8]) -> Option<Vec<u8>> {
    let body = bytes.strip_suffix(&[0x00, 0x00])?;
    let mut out = Vec::with_capacity(body.len());
    let mut i = 0;
    while i < body.len() {
        out.push(body[i]);
        // A real 0x00 is written 0x00 0xff; nothing else follows one.
        i += if body[i] == 0 { 2 } else { 1 };
    }
    Some(out)
}

/// How wide one column's encoding is, so an encoded tuple can be split back
/// into its columns without the values themselves.
///
/// A range on a column that is not the first still has to be checked, and the
/// entry key already carries every column -- reading it back out of the key is
/// what stops the check from fetching the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Width {
    Fixed(usize),
    Str,
    Vector,
}

/// Where each column's encoding starts and ends, tag included, so a span is
/// directly comparable with `encode(&[value])`.
pub fn column_spans(encoded: &[u8], widths: &[Width]) -> Option<Vec<(usize, usize)>> {
    let mut at = 0usize;
    let mut out = Vec::with_capacity(widths.len());
    for w in widths {
        let start = at;
        match *encoded.get(at)? {
            TAG_NULL | TAG_NULL_FIRST => at += 1,
            tag @ (TAG_PRESENT | TAG_PRESENT_DESC) => {
                at += 1;
                // A descending column's bytes are inverted, terminator and
                // escape included, so the walk looks for their inverses.
                let inv = tag == TAG_PRESENT_DESC;
                at = match w {
                    Width::Fixed(n) => at + n,
                    Width::Str => string_end(encoded, at, inv)?,
                    Width::Vector => vector_end(encoded, at, inv)?,
                };
            }
            _ => return None,
        }
        if at > encoded.len() {
            return None;
        }
        out.push((start, at));
    }
    Some(out)
}

/// Past the `00 00` that ends a string, skipping the `00 ff` escape.
fn string_end(b: &[u8], mut at: usize, inv: bool) -> Option<usize> {
    let (zero, esc) = if inv { (0xff, 0x00) } else { (0x00, 0xff) };
    loop {
        let (x, y) = (*b.get(at)?, b.get(at + 1).copied());
        if x == zero {
            if y == Some(zero) {
                return Some(at + 2);
            } else if y == Some(esc) {
                at += 2;
            } else {
                return None;
            }
        } else {
            at += 1;
        }
    }
}

/// Past the `00` that ends a vector; each element is `01` and eight bytes.
fn vector_end(b: &[u8], mut at: usize, inv: bool) -> Option<usize> {
    let (end, elem) = if inv { (0xff, 0xfe) } else { (0x00, 0x01) };
    loop {
        let x = *b.get(at)?;
        if x == end {
            return Some(at + 1);
        } else if x == elem {
            at += 9;
        } else {
            return None;
        }
    }
}

pub fn encode(cols: &[Col<'_>]) -> Vec<u8> {
    encode_with(cols, &[])
}

/// `opts` is per column; a column past its end is `ASC NULLS LAST`.
pub fn encode_with(cols: &[Col<'_>], opts: &[ColOpt]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, c) in cols.iter().enumerate() {
        encode_col(c, opts.get(i).copied().unwrap_or_default(), &mut out);
    }
    out
}

fn hex_into(bytes: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0xf) as usize]);
    }
}

fn too_long(len: usize) -> io::Error {
    io::Error::other(format!(
        "objkv: index row size {len} exceeds maximum {MAX_ENCODED} for an objkv index"
    ))
}

/// Whether a unique entry is keyed on the value alone: only with no NULL,
/// since Postgres allows any number of NULLs in a unique column.
fn unique_on_value_alone(unique: bool, cols: &[Col<'_>]) -> bool {
    unique && !cols.iter().any(Col::is_null)
}

/// The key an entry is stored under, which is also what conflict detection
/// matches on.
///
/// A unique index keys on the value alone, so two transactions inserting it
/// write the same key and collide -- that collision is the whole uniqueness
/// mechanism. A non-unique index adds the rowid, since sharing a value is
/// normal there.
pub fn entry_key(
    db: u32,
    index_id: u32,
    cols: &[Col<'_>],
    rowid: u64,
    unique: bool,
) -> io::Result<Vec<u8>> {
    entry_key_with(db, index_id, cols, &[], rowid, unique)
}

pub fn entry_key_with(
    db: u32,
    index_id: u32,
    cols: &[Col<'_>],
    opts: &[ColOpt],
    rowid: u64,
    unique: bool,
) -> io::Result<Vec<u8>> {
    let encoded = encode_with(cols, opts);
    if encoded.len() > MAX_ENCODED {
        return Err(too_long(encoded.len()));
    }
    let mut key = Vec::with_capacity(20 + encoded.len() * 2 + 17);
    key.extend_from_slice(index_prefix(db, index_id, unique).as_slice());
    hex_into(&encoded, &mut key);
    if !unique_on_value_alone(unique, cols) {
        key.extend_from_slice(format!("/{rowid:016x}").as_bytes());
    }
    Ok(key)
}

/// Every entry of one index, for a full scan or a drop. `db` scopes it: oids
/// repeat across databases, and without it two of them would overwrite each
/// other's entries with no error anywhere.
pub fn index_prefix(db: u32, index_id: u32, unique: bool) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(format!("{db:08x}/").as_bytes());
    p.push(if unique { b'u' } else { b'i' });
    p.extend_from_slice(format!("/{index_id:08x}/").as_bytes());
    p
}

/// Where a scan starts for an exact value or a leading-column prefix.
pub fn seek_prefix(
    db: u32,
    index_id: u32,
    cols: &[Col<'_>],
    unique: bool,
) -> io::Result<Vec<u8>> {
    seek_prefix_with(db, index_id, cols, &[], unique)
}

pub fn seek_prefix_with(
    db: u32,
    index_id: u32,
    cols: &[Col<'_>],
    opts: &[ColOpt],
    unique: bool,
) -> io::Result<Vec<u8>> {
    let encoded = encode_with(cols, opts);
    if encoded.len() > MAX_ENCODED {
        return Err(too_long(encoded.len()));
    }
    let mut p = index_prefix(db, index_id, unique);
    hex_into(&encoded, &mut p);
    Ok(p)
}

pub fn payload(rowid: u64) -> Vec<u8> {
    rowid.to_be_bytes().to_vec()
}

pub fn payload_rowid(payload: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(payload.try_into().ok()?))
}

/// The rowid an entry points at, from the key or from the payload.
pub fn rowid_of(key: &[u8], payload: &[u8]) -> Option<u64> {
    let fields = key.iter().filter(|&&b| b == b'/').count() + 1;
    if fields >= 5 {
        if let Some(id) = key
            .rsplit(|&b| b == b'/')
            .next()
            .filter(|tail| tail.len() == 16)
            .and_then(|tail| std::str::from_utf8(tail).ok())
            .and_then(|tail| u64::from_str_radix(tail, 16).ok())
        {
            return Some(id);
        }
    }
    payload_rowid(payload)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRef {
    pub db: u32,
    pub index: u32,
    pub rowid: u64,
}

/// Reads an entry key back, or `None` if it is not one. Deciding from the key
/// alone is what keeps the collector free of catalog lookups, which it must be
/// while holding the storage lock a catalog read would want.
pub fn entry_of(key: &[u8], payload: &[u8]) -> Option<EntryRef> {
    let mut fields = key.split(|&b| b == b'/');
    let db = hex32(fields.next()?)?;
    // `u` or `i` in the second field: a row key has a relation oid there.
    match fields.next()? {
        b"u" | b"i" => {}
        _ => return None,
    }
    let index = hex32(fields.next()?)?;
    Some(EntryRef { db, index, rowid: rowid_of(key, payload)? })
}

fn hex32(field: &[u8]) -> Option<u32> {
    if field.len() != 8 {
        return None;
    }
    u32::from_str_radix(std::str::from_utf8(field).ok()?, 16).ok()
}

pub fn row_key_of(e: &EntryRef, relid: u32) -> Vec<u8> {
    format!("{:08x}/{relid:08x}/{:016x}", e.db, e.rowid).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u32 = 0x2a;

    fn enc(cols: &[Col<'_>]) -> Vec<u8> {
        encode(cols)
    }

    fn assert_sorted(name: &str, values: &[Col<'_>]) {
        for w in values.windows(2) {
            assert!(
                enc(&[w[0]]) < enc(&[w[1]]),
                "{name}: {:?} must encode below {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn values_come_back_out_of_the_key() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let e = enc(&[Col::Int8(v)]);
            assert_eq!(decode_int(&e[1..]), v, "int8 {v}");
        }
        for v in [i32::MIN, -1, 0, 7, i32::MAX] {
            let e = enc(&[Col::Int4(v)]);
            assert_eq!(decode_int(&e[1..]) as i32, v, "int4 {v}");
        }
        for v in [0u32, 1, u32::MAX] {
            let e = enc(&[Col::Oid(v)]);
            assert_eq!(decode_uint(&e[1..]) as u32, v, "oid {v}");
        }
        for v in [f64::NEG_INFINITY, -1.5, 0.0, 1e308, f64::INFINITY] {
            let e = enc(&[Col::Float8(v)]);
            let b: [u8; 8] = e[1..].try_into().unwrap();
            assert_eq!(decode_float(&b), v, "float8 {v}");
        }
        assert!(decode_float(&enc(&[Col::Float8(f64::NAN)])[1..].try_into().unwrap()).is_nan());
        // A 0x00 inside a string is the case the escape exists for.
        for v in [&b""[..], b"a", b"hello", b"a\x00b", b"\x00\x00", b"\xff\x00"] {
            let e = enc(&[Col::Text(v)]);
            assert_eq!(decode_string(&e[1..]).as_deref(), Some(v), "text {v:?}");
        }
    }

    #[test]
    fn floats_sort_across_zero_and_infinity() {
        assert_sorted(
            "float8",
            &[
                Col::Float8(f64::NEG_INFINITY),
                Col::Float8(-1e308),
                Col::Float8(-1.5),
                Col::Float8(-f64::MIN_POSITIVE),
                Col::Float8(0.0),
                Col::Float8(f64::MIN_POSITIVE),
                Col::Float8(1.5),
                Col::Float8(1e308),
                Col::Float8(f64::INFINITY),
                Col::Float8(f64::NAN),
            ],
        );
        // Single precision rides the same key, widened exactly.
        assert_sorted(
            "float4 widened",
            &[
                Col::Float8(f32::NEG_INFINITY as f64),
                Col::Float8(-1.5f32 as f64),
                Col::Float8(0.0),
                Col::Float8(1.5f32 as f64),
                Col::Float8(f32::INFINITY as f64),
                Col::Float8(f32::NAN as f64),
            ],
        );
        // A NULL still sorts above everything, NaN included.
        assert!(enc(&[Col::Float8(f64::NAN)]) < enc(&[Col::Null]));
    }

    #[test]
    fn the_two_zeros_and_every_nan_are_one_key() {
        // Postgres compares -0 = 0 and NaN = NaN, so a unique index must not
        // accept both spellings of either.
        assert_eq!(enc(&[Col::Float8(-0.0)]), enc(&[Col::Float8(0.0)]));
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 0x3);
        assert!(other_nan.is_nan());
        assert_eq!(enc(&[Col::Float8(other_nan)]), enc(&[Col::Float8(f64::NAN)]));
        assert_eq!(enc(&[Col::Float8(-f64::NAN)]), enc(&[Col::Float8(f64::NAN)]));
    }

    #[test]
    fn floats_are_fixed_width() {
        assert_eq!(enc(&[Col::Float8(1.0)]).len(), 9, "tag + 8");
    }

    #[test]
    fn integers_sort_across_zero() {
        assert_sorted(
            "int4",
            &[
                Col::Int4(i32::MIN),
                Col::Int4(-2),
                Col::Int4(-1),
                Col::Int4(0),
                Col::Int4(1),
                Col::Int4(i32::MAX),
            ],
        );
        assert_sorted("int2", &[Col::Int2(i16::MIN), Col::Int2(-1), Col::Int2(0), Col::Int2(i16::MAX)]);
        assert_sorted("int8", &[Col::Int8(i64::MIN), Col::Int8(-1), Col::Int8(0), Col::Int8(i64::MAX)]);
    }

    #[test]
    fn integers_are_fixed_width() {
        for v in [i32::MIN, -1, 0, 7, i32::MAX] {
            assert_eq!(enc(&[Col::Int4(v)]).len(), 5, "tag + 4");
        }
        assert_eq!(enc(&[Col::Int2(0)]).len(), 3);
        assert_eq!(enc(&[Col::Int8(0)]).len(), 9);
    }

    #[test]
    fn text_sorts_bytewise_including_embedded_nuls() {
        assert_sorted(
            "text",
            &[
                Col::Text(b""),
                Col::Text(b"a"),
                Col::Text(b"a\x00"),
                Col::Text(b"a\x00b"),
                Col::Text(b"ab"),
                Col::Text(b"b"),
            ],
        );
    }

    #[test]
    fn a_prefix_sorts_before_what_extends_it() {
        // The escape scheme's job: the terminator must be lower than any
        // continuation, including one starting with 0x00.
        for (short, long) in [
            (&b""[..], &b"\x00"[..]),
            (&b"x"[..], &b"x\x00"[..]),
            (&b"x"[..], &b"xy"[..]),
            (&b"\x00"[..], &b"\x00\x00"[..]),
        ] {
            assert!(
                enc(&[Col::Text(short)]) < enc(&[Col::Text(long)]),
                "{short:?} must encode below {long:?}"
            );
        }
    }

    #[test]
    fn nulls_sort_last_and_bools_and_uuids_sort_right() {
        assert!(enc(&[Col::Int4(i32::MAX)]) < enc(&[Col::Null]));
        assert!(enc(&[Col::Text(&[0xff; 8])]) < enc(&[Col::Null]));
        assert_sorted("bool", &[Col::Bool(false), Col::Bool(true)]);

        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[15] = 1;
        assert!(enc(&[Col::Uuid(&lo)]) < enc(&[Col::Uuid(&hi)]));
    }

    #[test]
    fn the_first_column_decides() {
        // Lexicographic in the columns, which a leading-column seek depends on.
        let a = enc(&[Col::Int4(1), Col::Text(b"zzz")]);
        let b = enc(&[Col::Int4(2), Col::Text(b"aaa")]);
        assert!(a < b);

        let a = enc(&[Col::Int4(1), Col::Text(b"aaa")]);
        let b = enc(&[Col::Int4(1), Col::Text(b"aab")]);
        assert!(a < b);

        let p = seek_prefix(DB, 9, &[Col::Int4(1)], false).unwrap();
        let k = entry_key(DB, 9, &[Col::Int4(1), Col::Text(b"anything")], 3, false).unwrap();
        assert!(k.starts_with(&p));
    }

    #[test]
    fn a_unique_key_ignores_the_rowid_so_duplicates_collide() {
        // The uniqueness mechanism: if these two keys differ, two concurrent
        // inserts of one value both commit.
        let a = entry_key(DB, 1, &[Col::Text(b"bob")], 10, true).unwrap();
        let b = entry_key(DB, 1, &[Col::Text(b"bob")], 999, true).unwrap();
        assert_eq!(a, b, "a unique entry must not depend on which row it came from");

        let c = entry_key(DB, 1, &[Col::Text(b"carol")], 10, true).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn a_nonunique_key_keeps_the_rowid_so_duplicates_do_not_collide() {
        let a = entry_key(DB, 1, &[Col::Text(b"bob")], 10, false).unwrap();
        let b = entry_key(DB, 1, &[Col::Text(b"bob")], 999, false).unwrap();
        assert_ne!(a, b, "many rows share a value; that is not a conflict");
        assert_eq!(rowid_of(&a, &[]), Some(10));
    }

    #[test]
    fn null_entries_in_a_unique_index_keep_their_rowid() {
        let a = entry_key(DB, 1, &[Col::Null], 10, true).unwrap();
        let b = entry_key(DB, 1, &[Col::Null], 11, true).unwrap();
        assert_ne!(a, b);
        assert_eq!(rowid_of(&a, &[]), Some(10));

        let a = entry_key(DB, 1, &[Col::Int4(1), Col::Null], 10, true).unwrap();
        let b = entry_key(DB, 1, &[Col::Int4(1), Col::Null], 11, true).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn the_two_unique_shapes_cannot_be_mistaken_for_one_another() {
        // The two shapes could only collide if one tuple were both NULL-containing
        // and not, which the NULL tag rules out.
        let with_null = entry_key(DB, 1, &[Col::Null], 10, true).unwrap();
        let without = entry_key(DB, 1, &[Col::Text(b"bob")], 10, true).unwrap();
        assert!(!with_null.starts_with(&without));
        assert!(!without.starts_with(&with_null));
        assert_eq!(rowid_of(&without, &payload(42)), Some(42));
    }

    #[test]
    fn an_oversized_key_is_refused_rather_than_truncated() {
        // A truncated key collides with every value sharing its first 400 bytes.
        let big = vec![b'x'; MAX_ENCODED + 1];
        let err = entry_key(DB, 1, &[Col::Text(&big)], 1, false).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"), "{err}");
        assert!(entry_key(DB, 1, &[Col::Text(&big[..MAX_ENCODED - 3])], 1, false).is_ok());
    }

    #[test]
    fn oids_sort_as_unsigned() {
        // An oid is unsigned: 4000000000 is above 1. Flipping a sign bit, as the
        // signed integers need, would misorder half the catalog.
        assert_sorted(
            "oid",
            &[Col::Oid(0), Col::Oid(1), Col::Oid(16384), Col::Oid(2_147_483_648), Col::Oid(u32::MAX)],
        );
        assert_eq!(enc(&[Col::Oid(0)]).len(), 5, "tag plus a fixed four bytes");
    }

    #[test]
    fn names_sort_as_strings_and_ignore_their_padding() {
        // A `name` is NUL-padded to 64 bytes, and the padding is not part of the
        // value: "ab" padded differently is the same name.
        let mut padded = b"pg_class".to_vec();
        padded.resize(64, 0);
        assert_eq!(enc(&[Col::Name(&padded)]), enc(&[Col::Name(b"pg_class")]));

        assert_sorted(
            "name",
            &[Col::Name(b"pg_am"), Col::Name(b"pg_class"), Col::Name(b"pg_classes"), Col::Name(b"pg_proc")],
        );
        // Names and text order together, since the catalogs mix them freely.
        assert_eq!(enc(&[Col::Name(b"pg_class")]), enc(&[Col::Text(b"pg_class")]));
    }

    #[test]
    fn a_critical_index_key_round_trips() {
        // (oid, smallint), the shape most catalog lookups take.
        let a = entry_key(DB, 1, &[Col::Oid(1259), Col::Int2(1)], 0, true).unwrap();
        let b = entry_key(DB, 1, &[Col::Oid(1259), Col::Int2(2)], 0, true).unwrap();
        let c = entry_key(DB, 1, &[Col::Oid(1260), Col::Int2(1)], 0, true).unwrap();
        assert!(a < b && b < c);
        let p = seek_prefix(DB, 1, &[Col::Oid(1259)], true).unwrap();
        assert!(a.starts_with(&p) && b.starts_with(&p) && !c.starts_with(&p));
    }

    #[test]
    fn two_databases_never_share_a_key() {
        // Oids repeat across databases, so without it in the key one database
        // overwrites another's entries.
        for unique in [true, false] {
            let a = entry_key(1, 7, &[Col::Text(b"bob")], 10, unique).unwrap();
            let b = entry_key(2, 7, &[Col::Text(b"bob")], 10, unique).unwrap();
            assert_ne!(a, b, "same index oid in two databases must not collide");
            assert!(!a.starts_with(&index_prefix(2, 7, unique)));
            assert!(!b.starts_with(&index_prefix(1, 7, unique)));
            assert!(a.starts_with(&index_prefix(1, 7, unique)));
        }
    }

    #[test]
    fn a_scan_of_one_database_cannot_reach_another() {
        let mine = seek_prefix(1, 7, &[Col::Text(b"bob")], false).unwrap();
        let theirs = entry_key(2, 7, &[Col::Text(b"bob")], 10, false).unwrap();
        assert!(!theirs.starts_with(&mine));
    }

    #[test]
    fn keys_are_safe_ascii() {
        let k = entry_key(DB, 1, &[Col::Text(b"\x00\xff hi"), Col::Null], 7, false).unwrap();
        assert!(
            k.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'/'),
            "S3 keys stay in a safe charset: {}",
            String::from_utf8_lossy(&k)
        );
    }

    #[test]
    fn an_encoded_tuple_splits_back_into_its_columns() {
        let cols = [
            Col::Int4(-5),
            Col::Text(b"a\x00b"),
            Col::Null,
            Col::Oid(1259),
            Col::Vector(&[1, 2]),
            Col::Text(b""),
        ];
        let widths = [
            Width::Fixed(4),
            Width::Str,
            Width::Fixed(4),
            Width::Fixed(4),
            Width::Vector,
            Width::Str,
        ];
        let enc = encode(&cols);
        let spans = column_spans(&enc, &widths).expect("splits");
        assert_eq!(spans.len(), cols.len());
        for (i, (a, b)) in spans.iter().enumerate() {
            assert_eq!(&enc[*a..*b], encode(&[cols[i]]).as_slice(), "column {i}");
        }
        assert_eq!(spans.last().unwrap().1, enc.len(), "no bytes left over");
    }

    #[test]
    fn splitting_refuses_what_it_cannot_read() {
        let enc = encode(&[Col::Text(b"hi")]);
        assert_eq!(column_spans(&enc[..enc.len() - 1], &[Width::Str]), None);
        assert_eq!(column_spans(&enc, &[Width::Str, Width::Str]), None);
        assert_eq!(column_spans(&[0x7f], &[Width::Fixed(1)]), None);
    }

    #[test]
    fn a_column_split_off_a_key_compares_as_the_value_does() {
        // What a range on a trailing column needs: the bytes for that column
        // alone, ordered as the values are.
        let lo = encode(&[Col::Int4(1), Col::Text(b"m")]);
        let hi = encode(&[Col::Int4(1), Col::Text(b"z")]);
        let w = [Width::Fixed(4), Width::Str];
        let (a, b) = column_spans(&lo, &w).unwrap()[1];
        let (c, d) = column_spans(&hi, &w).unwrap()[1];
        assert!(lo[a..b] < hi[c..d]);
    }

    #[test]
    fn hex_preserves_order() {
        // What lands in the bucket is the hex, and the two orders must agree.
        let mut prev: Option<Vec<u8>> = None;
        for v in [i32::MIN, -9, -1, 0, 1, 9, i32::MAX] {
            let k = entry_key(DB, 1, &[Col::Int4(v)], 0, true).unwrap();
            if let Some(p) = prev {
                assert!(p < k, "hex order must match value order at {v}");
            }
            prev = Some(k);
        }
    }

    #[test]
    fn descending_and_nulls_first_columns_sort_as_asked() {
        let desc = ColOpt { desc: true, nulls_first: false };
        let desc_nf = ColOpt { desc: true, nulls_first: true };
        let asc_nf = ColOpt { desc: false, nulls_first: true };
        let e = |c: Col<'_>, o: ColOpt| encode_with(&[c], &[o]);

        // Integers reverse.
        assert!(e(Col::Int4(2), desc) < e(Col::Int4(1), desc));
        assert!(e(Col::Int4(-1), desc) > e(Col::Int4(0), desc));
        assert!(e(Col::Int8(i64::MAX), desc) < e(Col::Int8(i64::MIN), desc));
        // Floats too, and NaN, above everything ascending, is below it here.
        assert!(e(Col::Float8(1.5), desc) < e(Col::Float8(-1.5), desc));
        assert!(e(Col::Float8(f64::NAN), desc) < e(Col::Float8(f64::INFINITY), desc));
        // A prefix sorts after what extends it, the reverse of ascending.
        assert!(e(Col::Text(b"ab"), ASC) < e(Col::Text(b"abc"), ASC));
        assert!(e(Col::Text(b"ab"), desc) > e(Col::Text(b"abc"), desc));
        assert!(e(Col::Text(b"b"), desc) < e(Col::Text(b"a"), desc));
        // An escaped zero byte keeps its place.
        assert!(e(Col::Text(b"a\x00b"), desc) > e(Col::Text(b"a\x01"), desc));
        assert!(e(Col::Text(b"a\x00b"), desc) < e(Col::Text(b"a"), desc));
        let v1 = [1u64, 2];
        let v2 = [1u64, 2, 3];
        assert!(e(Col::Vector(&v1), desc) > e(Col::Vector(&v2), desc));

        // NULL goes where the option says, whichever way the values go.
        assert!(e(Col::Null, ASC) > e(Col::Int4(i32::MAX), ASC));
        assert!(e(Col::Null, desc) > e(Col::Int4(i32::MIN), desc));
        assert!(e(Col::Null, asc_nf) < e(Col::Int4(i32::MIN), asc_nf));
        assert!(e(Col::Null, desc_nf) < e(Col::Int4(i32::MAX), desc_nf));
    }

    #[test]
    fn spans_and_bodies_come_back_from_a_descending_key() {
        let opts = [ColOpt { desc: true, nulls_first: true }, ColOpt { desc: true, nulls_first: false }, ASC];
        let v = [7u64, 8];
        let cols = [Col::Text(b"a\x00z"), Col::Vector(&v), Col::Int4(5)];
        let enc = encode_with(&cols, &opts);
        let spans = column_spans(&enc, &[Width::Str, Width::Vector, Width::Fixed(4)]).unwrap();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[2].1, enc.len(), "the spans cover the key exactly");
        let (a, b) = spans[0];
        let body = column_body(enc[a], &enc[a + 1..b]);
        assert_eq!(decode_string(&body).unwrap(), b"a\x00z");
        let (a, b) = spans[2];
        assert_eq!(decode_int(&column_body(enc[a], &enc[a + 1..b])), 5);

        let nulls = encode_with(&[Col::Null, Col::Null], &[opts[0], ASC]);
        let spans = column_spans(&nulls, &[Width::Str, Width::Fixed(4)]).unwrap();
        assert!(is_null_tag(nulls[spans[0].0]) && is_null_tag(nulls[spans[1].0]));
        assert_ne!(nulls[0], nulls[1], "first and last are different tags");
    }
}
