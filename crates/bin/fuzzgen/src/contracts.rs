//! contracts: the sitediff data contracts (plan §3.2, §4.1-4.5, §6, §8).
//!
//! L0.0 of the sitediff fuzzer plan. Every M0 lane consumes these types
//! and writes/reads the files they describe; the lanes own disjoint source
//! files (L0.1 `client.rs`, L0.2 `diff.rs`+`canon.rs`, L0.3 `runner.rs`+
//! `supervisor.rs`+`logtail.rs`) and meet only here. The JSON Schema
//! twins live in `crates/bin/fuzzgen/schemas/*.schema.json`; realistic
//! fixtures in `crates/bin/fuzzgen/fixtures/contracts/`; the prose in
//! `docs/fuzzing/sitediff/CONTRACTS.md`.
//!
//! Serialization is hand-rolled on purpose: fuzzgen carries no serde
//! (the crate writes its JSONL by hand, `session::json_escape`), the
//! workspace has no `toml` crate at all, and the only serde users are
//! tool binaries (`tools/fnconf`, `tools/simharness`) plus an optional
//! sqe feature. The `json` submodule is a complete RFC 8259 value model
//! with a parser and a canonical writer (sorted keys, no insignificant
//! whitespace) — the same canonical form hashes into `cell_id`. The
//! `toml` submodule reads exactly the subset `rulings.toml` uses
//! (`[[ruling]]` array-of-tables, string/integer/boolean scalars, string
//! arrays) and refuses anything else loudly.
//!
//! Determinism: no type here reads a clock or entropy. Dates and ids are
//! explicit fields filled by the producer.

use std::collections::BTreeMap;
use std::fmt::Write as _;

// ---------------------------------------------------------------------
// JSON value model, parser, canonical writer
// ---------------------------------------------------------------------

pub mod json {
    //! Minimal JSON: a value tree, a strict parser, and two writers —
    //! `to_canonical` (compact, keys sorted, the hashing form) and
    //! `to_pretty` (2-space indent, keys sorted, the on-disk fixture and
    //! bank form). Objects keep insertion order in memory; both writers
    //! sort keys, so a parse→write cycle of a sorted document is byte-
    //! stable.

    use std::collections::BTreeMap;

    #[derive(Clone, Debug)]
    pub enum Value {
        Null,
        Bool(bool),
        /// Integers (the common case: counts, oids, ms) keep i64 exactly.
        Int(i64),
        /// Non-integral numbers. Written with Rust's shortest round-trip
        /// formatting; `NaN`/`inf` are not JSON and are refused on write.
        Float(f64),
        Str(String),
        Arr(Vec<Value>),
        Obj(Vec<(String, Value)>),
    }

    /// Equality is structural: object key order is insignificant (the
    /// on-disk form is sorted, builders insert in declaration order).
    impl PartialEq for Value {
        fn eq(&self, other: &Value) -> bool {
            match (self, other) {
                (Value::Null, Value::Null) => true,
                (Value::Bool(a), Value::Bool(b)) => a == b,
                (Value::Int(a), Value::Int(b)) => a == b,
                (Value::Float(a), Value::Float(b)) => a == b,
                (Value::Str(a), Value::Str(b)) => a == b,
                (Value::Arr(a), Value::Arr(b)) => a == b,
                (Value::Obj(a), Value::Obj(b)) => {
                    a.len() == b.len() && sorted_fields(a).iter().zip(sorted_fields(b).iter()).all(|(x, y)| x == y)
                }
                _ => false,
            }
        }
    }

    impl Value {
        pub fn obj() -> Value {
            Value::Obj(Vec::new())
        }

        /// Insert-or-replace a key on an object value (no-op otherwise).
        pub fn set(&mut self, key: &str, v: Value) {
            if let Value::Obj(fields) = self {
                if let Some(slot) = fields.iter_mut().find(|(k, _)| k == key) {
                    slot.1 = v;
                } else {
                    fields.push((key.to_string(), v));
                }
            }
        }

        /// Builder-style `set`.
        pub fn with(mut self, key: &str, v: Value) -> Value {
            self.set(key, v);
            self
        }

        pub fn get(&self, key: &str) -> Option<&Value> {
            match self {
                Value::Obj(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
                _ => None,
            }
        }

        pub fn as_str(&self) -> Option<&str> {
            match self {
                Value::Str(s) => Some(s),
                _ => None,
            }
        }

        pub fn as_i64(&self) -> Option<i64> {
            match self {
                Value::Int(i) => Some(*i),
                Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
                _ => None,
            }
        }

        pub fn as_f64(&self) -> Option<f64> {
            match self {
                Value::Int(i) => Some(*i as f64),
                Value::Float(f) => Some(*f),
                _ => None,
            }
        }

        pub fn as_bool(&self) -> Option<bool> {
            match self {
                Value::Bool(b) => Some(*b),
                _ => None,
            }
        }

        pub fn as_arr(&self) -> Option<&[Value]> {
            match self {
                Value::Arr(a) => Some(a),
                _ => None,
            }
        }

        pub fn as_obj(&self) -> Option<&[(String, Value)]> {
            match self {
                Value::Obj(o) => Some(o),
                _ => None,
            }
        }

        pub fn is_null(&self) -> bool {
            matches!(self, Value::Null)
        }

        /// Object keys, sorted (the contract's notion of "the key set").
        pub fn keys(&self) -> Vec<&str> {
            let mut ks: Vec<&str> =
                self.as_obj().map(|o| o.iter().map(|(k, _)| k.as_str()).collect()).unwrap_or_default();
            ks.sort_unstable();
            ks
        }
    }

    impl From<&str> for Value {
        fn from(s: &str) -> Value {
            Value::Str(s.to_string())
        }
    }
    impl From<String> for Value {
        fn from(s: String) -> Value {
            Value::Str(s)
        }
    }
    impl From<i64> for Value {
        fn from(i: i64) -> Value {
            Value::Int(i)
        }
    }
    impl From<u64> for Value {
        fn from(i: u64) -> Value {
            Value::Int(i as i64)
        }
    }
    impl From<u32> for Value {
        fn from(i: u32) -> Value {
            Value::Int(i as i64)
        }
    }
    impl From<i32> for Value {
        fn from(i: i32) -> Value {
            Value::Int(i as i64)
        }
    }
    impl From<i16> for Value {
        fn from(i: i16) -> Value {
            Value::Int(i as i64)
        }
    }
    impl From<bool> for Value {
        fn from(b: bool) -> Value {
            Value::Bool(b)
        }
    }
    impl From<Vec<Value>> for Value {
        fn from(a: Vec<Value>) -> Value {
            Value::Arr(a)
        }
    }

    /// String list → JSON array of strings.
    pub fn str_arr<S: AsRef<str>>(items: &[S]) -> Value {
        Value::Arr(items.iter().map(|s| Value::Str(s.as_ref().to_string())).collect())
    }

    /// String map → JSON object (sorted by key).
    pub fn str_map(m: &BTreeMap<String, String>) -> Value {
        Value::Obj(m.iter().map(|(k, v)| (k.clone(), Value::Str(v.clone()))).collect())
    }

    /// Optional value → the value or JSON null.
    pub fn opt<T: Into<Value>>(o: Option<T>) -> Value {
        o.map(Into::into).unwrap_or(Value::Null)
    }

    // ---- writer ----

    pub fn escape_into(out: &mut String, s: &str) {
        out.push('"');
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{08}' => out.push_str("\\b"),
                '\u{0c}' => out.push_str("\\f"),
                c if (c as u32) < 0x20 => {
                    out.push_str(&format!("\\u{:04x}", c as u32));
                }
                c => out.push(c),
            }
        }
        out.push('"');
    }

    fn write_number(out: &mut String, v: &Value) -> Result<(), String> {
        match v {
            Value::Int(i) => {
                out.push_str(&i.to_string());
                Ok(())
            }
            Value::Float(f) => {
                if !f.is_finite() {
                    return Err(format!("non-finite float {} is not JSON", f));
                }
                let s = format!("{}", f);
                out.push_str(&s);
                // Keep the float/integer distinction on the wire so a
                // Float(2.0) does not come back as Int(2).
                if !s.contains('.') && !s.contains('e') {
                    out.push_str(".0");
                }
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn sorted_fields(o: &[(String, Value)]) -> Vec<&(String, Value)> {
        let mut fs: Vec<&(String, Value)> = o.iter().collect();
        fs.sort_by(|a, b| a.0.cmp(&b.0));
        fs
    }

    fn write_canonical(out: &mut String, v: &Value) -> Result<(), String> {
        match v {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::Int(_) | Value::Float(_) => write_number(out, v)?,
            Value::Str(s) => escape_into(out, s),
            Value::Arr(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_canonical(out, x)?;
                }
                out.push(']');
            }
            Value::Obj(o) => {
                out.push('{');
                for (i, (k, x)) in sorted_fields(o).into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    escape_into(out, k);
                    out.push(':');
                    write_canonical(out, x)?;
                }
                out.push('}');
            }
        }
        Ok(())
    }

    fn write_pretty(out: &mut String, v: &Value, depth: usize) -> Result<(), String> {
        let pad = |out: &mut String, d: usize| {
            for _ in 0..d {
                out.push_str("  ");
            }
        };
        match v {
            Value::Arr(a) if !a.is_empty() => {
                out.push_str("[\n");
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push_str(",\n");
                    }
                    pad(out, depth + 1);
                    write_pretty(out, x, depth + 1)?;
                }
                out.push('\n');
                pad(out, depth);
                out.push(']');
            }
            Value::Obj(o) if !o.is_empty() => {
                out.push_str("{\n");
                for (i, (k, x)) in sorted_fields(o).into_iter().enumerate() {
                    if i > 0 {
                        out.push_str(",\n");
                    }
                    pad(out, depth + 1);
                    escape_into(out, k);
                    out.push_str(": ");
                    write_pretty(out, x, depth + 1)?;
                }
                out.push('\n');
                pad(out, depth);
                out.push('}');
            }
            other => write_canonical(out, other)?,
        }
        Ok(())
    }

    /// Compact canonical text: sorted keys, no whitespace. The hashing
    /// form (`Cell::cell_id`) and the JSONL line form.
    pub fn to_canonical(v: &Value) -> Result<String, String> {
        let mut out = String::new();
        write_canonical(&mut out, v)?;
        Ok(out)
    }

    /// Pretty text: sorted keys, 2-space indent, trailing newline. The
    /// on-disk form for fixtures, `findings/<id>.json` and `cell.json`.
    pub fn to_pretty(v: &Value) -> Result<String, String> {
        let mut out = String::new();
        write_pretty(&mut out, v, 0)?;
        out.push('\n');
        Ok(out)
    }

    // ---- parser ----

    struct Parser<'a> {
        s: &'a [u8],
        i: usize,
    }

    impl<'a> Parser<'a> {
        fn err<T>(&self, what: &str) -> Result<T, String> {
            Err(format!("json: {} at byte {}", what, self.i))
        }

        fn ws(&mut self) {
            while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\n' | b'\r' | b'\t') {
                self.i += 1;
            }
        }

        fn peek(&self) -> Option<u8> {
            self.s.get(self.i).copied()
        }

        fn expect(&mut self, b: u8) -> Result<(), String> {
            if self.peek() == Some(b) {
                self.i += 1;
                Ok(())
            } else {
                self.err(&format!("expected '{}'", b as char))
            }
        }

        fn lit(&mut self, word: &str, v: Value) -> Result<Value, String> {
            if self.s[self.i..].starts_with(word.as_bytes()) {
                self.i += word.len();
                Ok(v)
            } else {
                self.err("bad literal")
            }
        }

        fn value(&mut self) -> Result<Value, String> {
            self.ws();
            match self.peek() {
                None => self.err("unexpected end"),
                Some(b'{') => self.object(),
                Some(b'[') => self.array(),
                Some(b'"') => Ok(Value::Str(self.string()?)),
                Some(b't') => self.lit("true", Value::Bool(true)),
                Some(b'f') => self.lit("false", Value::Bool(false)),
                Some(b'n') => self.lit("null", Value::Null),
                Some(b'-') | Some(b'0'..=b'9') => self.number(),
                Some(_) => self.err("unexpected character"),
            }
        }

        fn object(&mut self) -> Result<Value, String> {
            self.expect(b'{')?;
            let mut fields = Vec::new();
            self.ws();
            if self.peek() == Some(b'}') {
                self.i += 1;
                return Ok(Value::Obj(fields));
            }
            loop {
                self.ws();
                if self.peek() != Some(b'"') {
                    return self.err("expected object key");
                }
                let k = self.string()?;
                self.ws();
                self.expect(b':')?;
                let v = self.value()?;
                if fields.iter().any(|(ek, _): &(String, Value)| *ek == k) {
                    return self.err(&format!("duplicate key {:?}", k));
                }
                fields.push((k, v));
                self.ws();
                match self.peek() {
                    Some(b',') => self.i += 1,
                    Some(b'}') => {
                        self.i += 1;
                        return Ok(Value::Obj(fields));
                    }
                    _ => return self.err("expected ',' or '}'"),
                }
            }
        }

        fn array(&mut self) -> Result<Value, String> {
            self.expect(b'[')?;
            let mut items = Vec::new();
            self.ws();
            if self.peek() == Some(b']') {
                self.i += 1;
                return Ok(Value::Arr(items));
            }
            loop {
                items.push(self.value()?);
                self.ws();
                match self.peek() {
                    Some(b',') => self.i += 1,
                    Some(b']') => {
                        self.i += 1;
                        return Ok(Value::Arr(items));
                    }
                    _ => return self.err("expected ',' or ']'"),
                }
            }
        }

        fn hex4(&mut self) -> Result<u32, String> {
            if self.i + 4 > self.s.len() {
                return self.err("short \\u escape");
            }
            let h = std::str::from_utf8(&self.s[self.i..self.i + 4]).map_err(|e| e.to_string())?;
            let v = u32::from_str_radix(h, 16).map_err(|e| format!("json: bad \\u escape: {}", e))?;
            self.i += 4;
            Ok(v)
        }

        fn string(&mut self) -> Result<String, String> {
            self.expect(b'"')?;
            let mut out: Vec<u8> = Vec::new();
            loop {
                let Some(b) = self.peek() else { return self.err("unterminated string") };
                self.i += 1;
                match b {
                    b'"' => break,
                    b'\\' => {
                        let Some(e) = self.peek() else { return self.err("unterminated escape") };
                        self.i += 1;
                        match e {
                            b'"' => out.push(b'"'),
                            b'\\' => out.push(b'\\'),
                            b'/' => out.push(b'/'),
                            b'b' => out.push(0x08),
                            b'f' => out.push(0x0c),
                            b'n' => out.push(b'\n'),
                            b'r' => out.push(b'\r'),
                            b't' => out.push(b'\t'),
                            b'u' => {
                                let mut cp = self.hex4()?;
                                if (0xD800..0xDC00).contains(&cp) {
                                    if !self.s[self.i..].starts_with(b"\\u") {
                                        return self.err("lone high surrogate");
                                    }
                                    self.i += 2;
                                    let lo = self.hex4()?;
                                    if !(0xDC00..0xE000).contains(&lo) {
                                        return self.err("bad low surrogate");
                                    }
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                }
                                let Some(c) = char::from_u32(cp) else { return self.err("bad code point") };
                                let mut buf = [0u8; 4];
                                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                            }
                            _ => return self.err("bad escape"),
                        }
                    }
                    b if b < 0x20 => return self.err("control character in string"),
                    b => out.push(b),
                }
            }
            String::from_utf8(out).map_err(|_| "json: invalid UTF-8 in string".to_string())
        }

        fn number(&mut self) -> Result<Value, String> {
            let start = self.i;
            if self.peek() == Some(b'-') {
                self.i += 1;
            }
            let mut is_float = false;
            while let Some(b) = self.peek() {
                match b {
                    b'0'..=b'9' => self.i += 1,
                    b'.' | b'e' | b'E' | b'+' | b'-' => {
                        is_float = true;
                        self.i += 1;
                    }
                    _ => break,
                }
            }
            let text = std::str::from_utf8(&self.s[start..self.i]).map_err(|e| e.to_string())?;
            if is_float {
                text.parse::<f64>().map(Value::Float).map_err(|e| format!("json: bad number {:?}: {}", text, e))
            } else {
                text.parse::<i64>().map(Value::Int).map_err(|e| format!("json: bad integer {:?}: {}", text, e))
            }
        }
    }

    /// Parse one JSON document (surrounding whitespace allowed, nothing
    /// else trailing).
    pub fn parse(text: &str) -> Result<Value, String> {
        let mut p = Parser { s: text.as_bytes(), i: 0 };
        let v = p.value()?;
        p.ws();
        if p.i != p.s.len() {
            return p.err("trailing data");
        }
        Ok(v)
    }
}

pub use json::Value;

/// Field-access helpers shared by every `from_json` below: a missing
/// required key or a wrong type is an error naming the key.
fn req<'a>(v: &'a Value, key: &str) -> Result<&'a Value, String> {
    v.get(key).ok_or_else(|| format!("missing required key {:?}", key))
}
fn req_str(v: &Value, key: &str) -> Result<String, String> {
    req(v, key)?.as_str().map(str::to_string).ok_or_else(|| format!("{:?} must be a string", key))
}
fn req_i64(v: &Value, key: &str) -> Result<i64, String> {
    req(v, key)?.as_i64().ok_or_else(|| format!("{:?} must be an integer", key))
}
fn req_u64(v: &Value, key: &str) -> Result<u64, String> {
    u64::try_from(req_i64(v, key)?).map_err(|_| format!("{:?} must be non-negative", key))
}
fn req_bool(v: &Value, key: &str) -> Result<bool, String> {
    req(v, key)?.as_bool().ok_or_else(|| format!("{:?} must be a boolean", key))
}
fn req_str_arr(v: &Value, key: &str) -> Result<Vec<String>, String> {
    str_arr_of(req(v, key)?, key)
}
fn str_arr_of(v: &Value, key: &str) -> Result<Vec<String>, String> {
    v.as_arr()
        .ok_or_else(|| format!("{:?} must be an array", key))?
        .iter()
        .map(|x| x.as_str().map(str::to_string).ok_or_else(|| format!("{:?} items must be strings", key)))
        .collect()
}
fn opt_str(v: &Value, key: &str) -> Result<Option<String>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Str(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("{:?} must be a string or null", key)),
    }
}
fn opt_u64(v: &Value, key: &str) -> Result<Option<u64>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => x
            .as_i64()
            .and_then(|i| u64::try_from(i).ok())
            .map(Some)
            .ok_or_else(|| format!("{:?} must be a non-negative integer or null", key)),
    }
}
fn opt_str_arr(v: &Value, key: &str) -> Result<Vec<String>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(x) => str_arr_of(x, key),
    }
}
fn str_map_of(v: &Value, key: &str) -> Result<BTreeMap<String, String>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(x) => x
            .as_obj()
            .ok_or_else(|| format!("{:?} must be an object", key))?
            .iter()
            .map(|(k, x)| {
                x.as_str().map(|s| (k.clone(), s.to_string())).ok_or_else(|| format!("{:?}.{} must be a string", key, k))
            })
            .collect(),
    }
}
fn reject_unknown(v: &Value, allowed: &[&str], what: &str) -> Result<(), String> {
    for k in v.keys() {
        if !allowed.contains(&k) {
            return Err(format!("{}: unknown key {:?}", what, k));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Raw bytes on the wire
// ---------------------------------------------------------------------

/// Raw bytes exactly as the server sent them (plan §4.1: "bytes stay
/// bytes, no from_utf8_lossy"). JSON form is a plain string when the
/// bytes are UTF-8 without control characters (readable fixtures), else
/// `{"hex": "..."}`. The choice is a pure function of the bytes, so the
/// round trip is byte-stable.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    pub fn text(s: &str) -> Bytes {
        Bytes(s.as_bytes().to_vec())
    }

    fn printable(&self) -> Option<&str> {
        let s = std::str::from_utf8(&self.0).ok()?;
        if s.chars().any(|c| (c as u32) < 0x20 && c != '\n' && c != '\t') {
            return None;
        }
        Some(s)
    }

    pub fn to_json(&self) -> Value {
        match self.printable() {
            Some(s) => Value::Str(s.to_string()),
            None => Value::obj().with("hex", Value::Str(hex(&self.0))),
        }
    }

    pub fn from_json(v: &Value) -> Result<Bytes, String> {
        match v {
            Value::Str(s) => Ok(Bytes::text(s)),
            Value::Obj(_) => {
                let h = req_str(v, "hex")?;
                unhex(&h).map(Bytes)
            }
            _ => Err("bytes must be a string or {\"hex\": ...}".to_string()),
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{:02x}", x);
    }
    s
}

pub fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("bad hex: {}", e)))
        .collect()
}

fn opt_bytes(v: &Value, key: &str) -> Result<Option<Bytes>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => Bytes::from_json(x).map(Some),
    }
}

// ---------------------------------------------------------------------
// Shared vocabulary: ordered, side
// ---------------------------------------------------------------------

/// How a step's result rows compare (plan §3.2 recipe header, §4.1):
/// `total` = ordered compare (an ORDER BY covering every output column,
/// or a pk-ordered probe); `partial` = multiset compare with the
/// tie-order ruling in play; `none` = multiset compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ordered {
    Total,
    Partial,
    None,
}

impl Ordered {
    pub fn as_str(self) -> &'static str {
        match self {
            Ordered::Total => "total",
            Ordered::Partial => "partial",
            Ordered::None => "none",
        }
    }
    pub fn parse(s: &str) -> Result<Ordered, String> {
        match s {
            "total" => Ok(Ordered::Total),
            "partial" => Ok(Ordered::Partial),
            "none" => Ok(Ordered::None),
            _ => Err(format!("ordered must be total|partial|none, got {:?}", s)),
        }
    }
}

/// Differential side: `a` = the C 18.6 oracle, `b` = pgrust.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::A => "a",
            Side::B => "b",
        }
    }
    pub fn parse(s: &str) -> Result<Side, String> {
        match s {
            "a" => Ok(Side::A),
            "b" => Ok(Side::B),
            _ => Err(format!("side must be a|b, got {:?}", s)),
        }
    }
}

// ---------------------------------------------------------------------
// StepRecord (stream JSONL)
// ---------------------------------------------------------------------

/// Step kinds (plan §3.2). `Env`, `Probe` carry their argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepKind {
    Sql,
    Xproto,
    CopyIn,
    Connect,
    Disconnect,
    Cancel,
    /// `env:<op>` — alter_system, reload, restart, ...
    Env(String),
    /// `probe:<deck>` — catalog, invariants, locks, physical, stats,
    /// artifacts, decode, amcheck, admission.
    Probe(String),
    Storm,
    Pressure,
    Settle,
    SleepUntilBlocked,
}

impl StepKind {
    pub fn to_string_key(&self) -> String {
        match self {
            StepKind::Sql => "sql".into(),
            StepKind::Xproto => "xproto".into(),
            StepKind::CopyIn => "copy_in".into(),
            StepKind::Connect => "connect".into(),
            StepKind::Disconnect => "disconnect".into(),
            StepKind::Cancel => "cancel".into(),
            StepKind::Env(op) => format!("env:{}", op),
            StepKind::Probe(deck) => format!("probe:{}", deck),
            StepKind::Storm => "storm".into(),
            StepKind::Pressure => "pressure".into(),
            StepKind::Settle => "settle".into(),
            StepKind::SleepUntilBlocked => "sleep_until_blocked".into(),
        }
    }

    pub fn parse(s: &str) -> Result<StepKind, String> {
        Ok(match s {
            "sql" => StepKind::Sql,
            "xproto" => StepKind::Xproto,
            "copy_in" => StepKind::CopyIn,
            "connect" => StepKind::Connect,
            "disconnect" => StepKind::Disconnect,
            "cancel" => StepKind::Cancel,
            "storm" => StepKind::Storm,
            "pressure" => StepKind::Pressure,
            "settle" => StepKind::Settle,
            "sleep_until_blocked" => StepKind::SleepUntilBlocked,
            _ => {
                if let Some(op) = s.strip_prefix("env:") {
                    if op.is_empty() {
                        return Err("env: step needs an operation".into());
                    }
                    StepKind::Env(op.to_string())
                } else if let Some(deck) = s.strip_prefix("probe:") {
                    if deck.is_empty() {
                        return Err("probe: step needs a deck".into());
                    }
                    StepKind::Probe(deck.to_string())
                } else {
                    return Err(format!("unknown step kind {:?}", s));
                }
            }
        })
    }
}

/// Extended-protocol shape of an `xproto` step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XProto {
    /// `parse_bind_execute` | `named_stmt` | `named_portal` | `pipeline` | `describe_only`.
    pub mode: String,
    /// Bind parameters (None = NULL); text-format unless the value carries a `hex` object.
    pub params: Vec<Option<Bytes>>,
    /// Named statement ("" = unnamed).
    pub stmt: String,
    /// Named portal ("" = unnamed).
    pub portal: String,
    /// Execute row limit (0 = no limit).
    pub limit: u32,
    /// Describe target: "S" | "P" | "" (none).
    pub describe: String,
    /// `pipeline` mode only: send the Parse/Bind/Execute group this many
    /// times before the single Sync (1 = once; the compose `pipeline-n`
    /// shell). Omitted from JSON when 1.
    pub repeat: u32,
}

impl XProto {
    /// The plain unnamed parse/bind/execute shape.
    pub fn simple(mode: &str) -> XProto {
        XProto { mode: mode.into(), params: Vec::new(), stmt: String::new(), portal: String::new(), limit: 0, describe: String::new(), repeat: 1 }
    }

    pub fn to_json(&self) -> Value {
        let o = Value::obj()
            .with("mode", Value::from(self.mode.as_str()))
            .with(
                "params",
                Value::Arr(self.params.iter().map(|p| p.as_ref().map(Bytes::to_json).unwrap_or(Value::Null)).collect()),
            )
            .with("stmt", Value::from(self.stmt.as_str()))
            .with("portal", Value::from(self.portal.as_str()))
            .with("limit", Value::from(self.limit))
            .with("describe", Value::from(self.describe.as_str()));
        if self.repeat > 1 {
            return o.with("repeat", Value::from(self.repeat));
        }
        o
    }

    pub fn from_json(v: &Value) -> Result<XProto, String> {
        reject_unknown(v, &["mode", "params", "stmt", "portal", "limit", "describe", "repeat"], "xproto")?;
        let params = req(v, "params")?
            .as_arr()
            .ok_or("xproto.params must be an array")?
            .iter()
            .map(|p| if p.is_null() { Ok(None) } else { Bytes::from_json(p).map(Some) })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(XProto {
            mode: req_str(v, "mode")?,
            params,
            stmt: req_str(v, "stmt")?,
            portal: req_str(v, "portal")?,
            limit: req_u64(v, "limit")? as u32,
            describe: req_str(v, "describe")?,
            repeat: opt_u64(v, "repeat")?.unwrap_or(1).max(1) as u32,
        })
    }
}

/// Mutation provenance of a mutated recipe step (plan §5.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mutant {
    pub op: String,
    pub seed: u64,
}

/// One generated step of a stream (plan §3.2 StepRecord). Streams are
/// regenerated from the witness, never stored, except when banked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepRecord {
    pub scenario: String,
    pub seq: u64,
    pub session: String,
    /// `superuser` | `owner` | `role=<name>`.
    pub role: String,
    pub kind: StepKind,
    pub sql: Option<String>,
    pub xproto: Option<XProto>,
    pub productions: Vec<String>,
    /// Universe unit ids this step targets (ereport sites, cfuncs, ...).
    pub targets: Vec<String>,
    pub ordered: Ordered,
    /// The C-18.6 expectation a recipe declares (SQLSTATE or message template).
    pub expect_c: Option<String>,
    /// Bracket id (transaction / env pair) the reducer keeps atomic.
    pub bracket: Option<String>,
    /// Recipe id when the step came from the bank.
    pub recipe: Option<String>,
    pub mutant: Option<Mutant>,
    /// Bound recipe slots (name -> rendered value).
    pub slots: BTreeMap<String, String>,
}

impl StepRecord {
    pub const KEYS: &'static [&'static str] = &[
        "scenario", "seq", "session", "role", "kind", "sql", "xproto", "productions", "targets", "ordered",
        "expect_c", "bracket", "recipe", "mutant", "slots",
    ];

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with("scenario", Value::from(self.scenario.as_str()))
            .with("seq", Value::from(self.seq))
            .with("session", Value::from(self.session.as_str()))
            .with("role", Value::from(self.role.as_str()))
            .with("kind", Value::Str(self.kind.to_string_key()))
            .with("sql", json::opt(self.sql.as_deref()))
            .with("xproto", self.xproto.as_ref().map(XProto::to_json).unwrap_or(Value::Null))
            .with("productions", json::str_arr(&self.productions))
            .with("targets", json::str_arr(&self.targets))
            .with("ordered", Value::from(self.ordered.as_str()))
            .with("expect_c", json::opt(self.expect_c.as_deref()))
            .with("bracket", json::opt(self.bracket.as_deref()))
            .with("recipe", json::opt(self.recipe.as_deref()))
            .with(
                "mutant",
                self.mutant
                    .as_ref()
                    .map(|m| Value::obj().with("op", Value::from(m.op.as_str())).with("seed", Value::from(m.seed)))
                    .unwrap_or(Value::Null),
            )
            .with("slots", json::str_map(&self.slots))
    }

    pub fn from_json(v: &Value) -> Result<StepRecord, String> {
        reject_unknown(v, Self::KEYS, "StepRecord")?;
        let xproto = match v.get("xproto") {
            None | Some(Value::Null) => None,
            Some(x) => Some(XProto::from_json(x)?),
        };
        let mutant = match v.get("mutant") {
            None | Some(Value::Null) => None,
            Some(m) => Some(Mutant { op: req_str(m, "op")?, seed: req_u64(m, "seed")? }),
        };
        Ok(StepRecord {
            scenario: req_str(v, "scenario")?,
            seq: req_u64(v, "seq")?,
            session: req_str(v, "session")?,
            role: req_str(v, "role")?,
            kind: StepKind::parse(&req_str(v, "kind")?)?,
            sql: opt_str(v, "sql")?,
            xproto,
            productions: opt_str_arr(v, "productions")?,
            targets: opt_str_arr(v, "targets")?,
            ordered: Ordered::parse(&req_str(v, "ordered")?)?,
            expect_c: opt_str(v, "expect_c")?,
            bracket: opt_str(v, "bracket")?,
            recipe: opt_str(v, "recipe")?,
            mutant,
            slots: str_map_of(v, "slots")?,
        })
    }

    /// One JSONL line (canonical, no newline).
    pub fn to_jsonl(&self) -> String {
        json::to_canonical(&self.to_json()).expect("StepRecord has no floats")
    }

    pub fn from_jsonl(line: &str) -> Result<StepRecord, String> {
        StepRecord::from_json(&json::parse(line)?)
    }
}

// ---------------------------------------------------------------------
// ObservationRecord: wire messages
// ---------------------------------------------------------------------

/// One RowDescription column, every field the backend sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColDesc {
    pub name: Bytes,
    pub tableoid: u32,
    pub attnum: i16,
    pub typoid: u32,
    pub typlen: i16,
    pub typmod: i32,
    /// 0 text, 1 binary.
    pub fmt: i16,
}

impl ColDesc {
    fn to_json(&self) -> Value {
        Value::obj()
            .with("name", self.name.to_json())
            .with("tableoid", Value::from(self.tableoid))
            .with("attnum", Value::from(self.attnum))
            .with("typoid", Value::from(self.typoid))
            .with("typlen", Value::from(self.typlen))
            .with("typmod", Value::from(self.typmod))
            .with("fmt", Value::from(self.fmt))
    }
    fn from_json(v: &Value) -> Result<ColDesc, String> {
        reject_unknown(v, &["name", "tableoid", "attnum", "typoid", "typlen", "typmod", "fmt"], "column")?;
        Ok(ColDesc {
            name: Bytes::from_json(req(v, "name")?)?,
            tableoid: req_i64(v, "tableoid")? as u32,
            attnum: req_i64(v, "attnum")? as i16,
            typoid: req_i64(v, "typoid")? as u32,
            typlen: req_i64(v, "typlen")? as i16,
            typmod: req_i64(v, "typmod")? as i32,
            fmt: req_i64(v, "fmt")? as i16,
        })
    }
}

/// ErrorResponse / NoticeResponse fields keyed by the one-byte field
/// code (S V C M D H P p q W s t c d n F L R), values raw.
pub type ErrFields = BTreeMap<char, Bytes>;

fn err_fields_to_json(f: &ErrFields) -> Value {
    Value::Obj(f.iter().map(|(k, v)| (k.to_string(), v.to_json())).collect())
}

fn err_fields_from_json(v: &Value) -> Result<ErrFields, String> {
    let mut out = ErrFields::new();
    for (k, x) in v.as_obj().ok_or("fields must be an object")? {
        let mut cs = k.chars();
        let (Some(c), None) = (cs.next(), cs.next()) else {
            return Err(format!("field code must be one character, got {:?}", k));
        };
        out.insert(c, Bytes::from_json(x)?);
    }
    Ok(out)
}

/// One backend message, decoded but lossless (plan §3.2 `wire[]`). The
/// `t` key in JSON is the protocol message type byte; anything the
/// client does not model structurally lands in `Raw`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMsg {
    /// 'E'
    ErrorResponse(ErrFields),
    /// 'N'
    NoticeResponse(ErrFields),
    /// 'T'
    RowDescription(Vec<ColDesc>),
    /// 'D' (None = NULL cell)
    DataRow(Vec<Option<Bytes>>),
    /// 'C' — the full tag text, e.g. "INSERT 0 3".
    CommandComplete(Bytes),
    /// 'S'
    ParameterStatus { name: Bytes, value: Bytes },
    /// 'A'
    NotificationResponse { pid: u32, channel: Bytes, payload: Bytes },
    /// 'K' — protocol 3.2 allows a variable-length key.
    BackendKeyData { pid: u32, key: Bytes },
    /// 'Z' — 'I' | 'T' | 'E'
    ReadyForQuery { status: char },
    /// 'I'
    EmptyQueryResponse,
    /// '1' / '2' / '3' / 'n' / 's' / 'c'
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,
    CopyDone,
    /// 't'
    ParameterDescription(Vec<u32>),
    /// 'G' / 'H' — overall format + per-column formats.
    CopyInResponse { fmt: i8, col_fmts: Vec<i16> },
    CopyOutResponse { fmt: i8, col_fmts: Vec<i16> },
    /// 'd'
    CopyData(Bytes),
    /// 'R' — auth kind + trailing bytes (SASL mechanisms, salt, ...).
    Authentication { kind: i32, data: Bytes },
    /// 'v'
    NegotiateProtocolVersion { minor: i32, unknown_options: Vec<Bytes> },
    /// Anything else, verbatim.
    Raw { code: char, data: Bytes },
}

fn one_char(v: &Value, key: &str) -> Result<char, String> {
    let s = req_str(v, key)?;
    let mut cs = s.chars();
    match (cs.next(), cs.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(format!("{:?} must be one character", key)),
    }
}

fn i16_arr(v: &Value, key: &str) -> Result<Vec<i16>, String> {
    req(v, key)?
        .as_arr()
        .ok_or_else(|| format!("{:?} must be an array", key))?
        .iter()
        .map(|x| x.as_i64().map(|i| i as i16).ok_or_else(|| format!("{:?} items must be integers", key)))
        .collect()
}

impl WireMsg {
    pub fn code(&self) -> char {
        match self {
            WireMsg::ErrorResponse(_) => 'E',
            WireMsg::NoticeResponse(_) => 'N',
            WireMsg::RowDescription(_) => 'T',
            WireMsg::DataRow(_) => 'D',
            WireMsg::CommandComplete(_) => 'C',
            WireMsg::ParameterStatus { .. } => 'S',
            WireMsg::NotificationResponse { .. } => 'A',
            WireMsg::BackendKeyData { .. } => 'K',
            WireMsg::ReadyForQuery { .. } => 'Z',
            WireMsg::EmptyQueryResponse => 'I',
            WireMsg::ParseComplete => '1',
            WireMsg::BindComplete => '2',
            WireMsg::CloseComplete => '3',
            WireMsg::NoData => 'n',
            WireMsg::PortalSuspended => 's',
            WireMsg::CopyDone => 'c',
            WireMsg::ParameterDescription(_) => 't',
            WireMsg::CopyInResponse { .. } => 'G',
            WireMsg::CopyOutResponse { .. } => 'H',
            WireMsg::CopyData(_) => 'd',
            WireMsg::Authentication { .. } => 'R',
            WireMsg::NegotiateProtocolVersion { .. } => 'v',
            WireMsg::Raw { code, .. } => *code,
        }
    }

    pub fn to_json(&self) -> Value {
        let base = Value::obj().with("t", Value::Str(self.code().to_string()));
        match self {
            WireMsg::ErrorResponse(f) | WireMsg::NoticeResponse(f) => base.with("fields", err_fields_to_json(f)),
            WireMsg::RowDescription(cols) => {
                base.with("columns", Value::Arr(cols.iter().map(ColDesc::to_json).collect()))
            }
            WireMsg::DataRow(cells) => base.with(
                "cells",
                Value::Arr(cells.iter().map(|c| c.as_ref().map(Bytes::to_json).unwrap_or(Value::Null)).collect()),
            ),
            WireMsg::CommandComplete(tag) => base.with("tag", tag.to_json()),
            WireMsg::ParameterStatus { name, value } => base.with("name", name.to_json()).with("value", value.to_json()),
            WireMsg::NotificationResponse { pid, channel, payload } => base
                .with("pid", Value::from(*pid))
                .with("channel", channel.to_json())
                .with("payload", payload.to_json()),
            WireMsg::BackendKeyData { pid, key } => {
                base.with("pid", Value::from(*pid)).with("key", Value::Str(hex(&key.0)))
            }
            WireMsg::ReadyForQuery { status } => base.with("status", Value::Str(status.to_string())),
            WireMsg::EmptyQueryResponse
            | WireMsg::ParseComplete
            | WireMsg::BindComplete
            | WireMsg::CloseComplete
            | WireMsg::NoData
            | WireMsg::PortalSuspended
            | WireMsg::CopyDone => base,
            WireMsg::ParameterDescription(oids) => {
                base.with("typoids", Value::Arr(oids.iter().map(|o| Value::from(*o)).collect()))
            }
            WireMsg::CopyInResponse { fmt, col_fmts } | WireMsg::CopyOutResponse { fmt, col_fmts } => base
                .with("fmt", Value::Int(*fmt as i64))
                .with("col_fmts", Value::Arr(col_fmts.iter().map(|f| Value::from(*f)).collect())),
            WireMsg::CopyData(d) => base.with("data", d.to_json()),
            WireMsg::Authentication { kind, data } => {
                base.with("kind", Value::from(*kind)).with("data", Value::Str(hex(&data.0)))
            }
            WireMsg::NegotiateProtocolVersion { minor, unknown_options } => base
                .with("minor", Value::from(*minor))
                .with("unknown_options", Value::Arr(unknown_options.iter().map(Bytes::to_json).collect())),
            WireMsg::Raw { data, .. } => base.with("data", Value::Str(hex(&data.0))),
        }
    }

    pub fn from_json(v: &Value) -> Result<WireMsg, String> {
        let t = one_char(v, "t")?;
        let hex_bytes = |key: &str| -> Result<Bytes, String> { unhex(&req_str(v, key)?).map(Bytes) };
        Ok(match t {
            'E' => WireMsg::ErrorResponse(err_fields_from_json(req(v, "fields")?)?),
            'N' => WireMsg::NoticeResponse(err_fields_from_json(req(v, "fields")?)?),
            'T' => WireMsg::RowDescription(
                req(v, "columns")?
                    .as_arr()
                    .ok_or("columns must be an array")?
                    .iter()
                    .map(ColDesc::from_json)
                    .collect::<Result<_, _>>()?,
            ),
            'D' => WireMsg::DataRow(
                req(v, "cells")?
                    .as_arr()
                    .ok_or("cells must be an array")?
                    .iter()
                    .map(|c| if c.is_null() { Ok(None) } else { Bytes::from_json(c).map(Some) })
                    .collect::<Result<_, _>>()?,
            ),
            'C' => WireMsg::CommandComplete(Bytes::from_json(req(v, "tag")?)?),
            'S' => WireMsg::ParameterStatus {
                name: Bytes::from_json(req(v, "name")?)?,
                value: Bytes::from_json(req(v, "value")?)?,
            },
            'A' => WireMsg::NotificationResponse {
                pid: req_i64(v, "pid")? as u32,
                channel: Bytes::from_json(req(v, "channel")?)?,
                payload: Bytes::from_json(req(v, "payload")?)?,
            },
            'K' => WireMsg::BackendKeyData { pid: req_i64(v, "pid")? as u32, key: hex_bytes("key")? },
            'Z' => WireMsg::ReadyForQuery { status: one_char(v, "status")? },
            'I' => WireMsg::EmptyQueryResponse,
            '1' => WireMsg::ParseComplete,
            '2' => WireMsg::BindComplete,
            '3' => WireMsg::CloseComplete,
            'n' => WireMsg::NoData,
            's' => WireMsg::PortalSuspended,
            'c' => WireMsg::CopyDone,
            't' => WireMsg::ParameterDescription(
                req(v, "typoids")?
                    .as_arr()
                    .ok_or("typoids must be an array")?
                    .iter()
                    .map(|x| x.as_i64().map(|i| i as u32).ok_or_else(|| "typoids items must be integers".to_string()))
                    .collect::<Result<_, _>>()?,
            ),
            'G' => WireMsg::CopyInResponse { fmt: req_i64(v, "fmt")? as i8, col_fmts: i16_arr(v, "col_fmts")? },
            'H' => WireMsg::CopyOutResponse { fmt: req_i64(v, "fmt")? as i8, col_fmts: i16_arr(v, "col_fmts")? },
            'd' => WireMsg::CopyData(Bytes::from_json(req(v, "data")?)?),
            'R' => WireMsg::Authentication { kind: req_i64(v, "kind")? as i32, data: hex_bytes("data")? },
            'v' => WireMsg::NegotiateProtocolVersion {
                minor: req_i64(v, "minor")? as i32,
                unknown_options: req(v, "unknown_options")?
                    .as_arr()
                    .ok_or("unknown_options must be an array")?
                    .iter()
                    .map(Bytes::from_json)
                    .collect::<Result<_, _>>()?,
            },
            code => WireMsg::Raw { code, data: hex_bytes("data")? },
        })
    }
}

// ---------------------------------------------------------------------
// ObservationRecord: log, panic, crash, hang, probes, counters
// ---------------------------------------------------------------------

/// One server-log line attributed to the step (plan §4.3). Parsed
/// against the cell's `log_line_prefix`; an unparseable line keeps only
/// `raw` (every parsed field None) and `source`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogLine {
    /// `stderr` | `csvlog` | `jsonlog`.
    pub source: String,
    pub raw: Bytes,
    /// `%m` text as printed, zone token kept.
    pub ts: Option<String>,
    /// `%b`.
    pub backend_type: Option<String>,
    /// `%p`.
    pub pid: Option<u32>,
    /// `%a`.
    pub app: Option<String>,
    /// LOG | INFO | NOTICE | WARNING | ERROR | FATAL | PANIC | DEBUGn | STATEMENT | DETAIL | HINT | LOCATION ...
    pub level: Option<String>,
    pub sqlstate: Option<String>,
    pub message: Option<Bytes>,
    /// `LOCATION:` line under log_error_verbosity=verbose (the A witness).
    pub location: Option<String>,
}

impl LogLine {
    const KEYS: &'static [&'static str] =
        &["source", "raw", "ts", "backend_type", "pid", "app", "level", "sqlstate", "message", "location"];

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with("source", Value::from(self.source.as_str()))
            .with("raw", self.raw.to_json())
            .with("ts", json::opt(self.ts.as_deref()))
            .with("backend_type", json::opt(self.backend_type.as_deref()))
            .with("pid", json::opt(self.pid))
            .with("app", json::opt(self.app.as_deref()))
            .with("level", json::opt(self.level.as_deref()))
            .with("sqlstate", json::opt(self.sqlstate.as_deref()))
            .with("message", self.message.as_ref().map(Bytes::to_json).unwrap_or(Value::Null))
            .with("location", json::opt(self.location.as_deref()))
    }

    pub fn from_json(v: &Value) -> Result<LogLine, String> {
        reject_unknown(v, Self::KEYS, "log line")?;
        Ok(LogLine {
            source: req_str(v, "source")?,
            raw: Bytes::from_json(req(v, "raw")?)?,
            ts: opt_str(v, "ts")?,
            backend_type: opt_str(v, "backend_type")?,
            pid: opt_u64(v, "pid")?.map(|p| p as u32),
            app: opt_str(v, "app")?,
            level: opt_str(v, "level")?,
            sqlstate: opt_str(v, "sqlstate")?,
            message: opt_bytes(v, "message")?,
            location: opt_str(v, "location")?,
        })
    }
}

/// A B-side panic marker pair from the stderr log (`panicked at` +
/// `panicking backend query:`), a crash even when the wire says XX000.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Panic {
    /// The `panicked at <site>:` text (Rust site).
    pub site: String,
    pub message: Bytes,
    /// The `panicking backend query:` payload.
    pub query: Option<Bytes>,
}

/// A server death observed by the symmetric supervisor (plan §4.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Crash {
    pub side: Side,
    /// Restart generation the stream resumed under (post-crash confidence).
    pub generation: u32,
    /// Signal name/number text when known (`SIGSEGV`, `6`, ...).
    pub signal: Option<String>,
    /// Last log lines banked before the restart.
    pub log_tail: Vec<Bytes>,
}

/// Per-step deadline exceeded; `ms` is the deadline hit, `ladder` the
/// escalation reached (`cancel` | `terminate` | `sigkill`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hang {
    pub ms: u64,
    pub ladder: Option<String>,
}

/// B-only self-oracle counters (plan §4.5). All from in-process
/// counters except `rss_kb`, which is reported and never gated.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ServerCounters {
    pub alloc_calls: Option<u64>,
    pub alloc_bytes: Option<u64>,
    pub live_bytes: Option<u64>,
    pub committed_bytes: Option<u64>,
    pub rss_kb: Option<u64>,
    /// `mcx::global_footprint::bytes()` via `pgrust: memctx`.
    pub footprint: Option<u64>,
}

impl ServerCounters {
    const KEYS: &'static [&'static str] =
        &["alloc_calls", "alloc_bytes", "live_bytes", "committed_bytes", "rss_kb", "footprint"];

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with("alloc_calls", json::opt(self.alloc_calls))
            .with("alloc_bytes", json::opt(self.alloc_bytes))
            .with("live_bytes", json::opt(self.live_bytes))
            .with("committed_bytes", json::opt(self.committed_bytes))
            .with("rss_kb", json::opt(self.rss_kb))
            .with("footprint", json::opt(self.footprint))
    }

    pub fn from_json(v: &Value) -> Result<ServerCounters, String> {
        reject_unknown(v, Self::KEYS, "server")?;
        Ok(ServerCounters {
            alloc_calls: opt_u64(v, "alloc_calls")?,
            alloc_bytes: opt_u64(v, "alloc_bytes")?,
            live_bytes: opt_u64(v, "live_bytes")?,
            committed_bytes: opt_u64(v, "committed_bytes")?,
            rss_kb: opt_u64(v, "rss_kb")?,
            footprint: opt_u64(v, "footprint")?,
        })
    }
}

/// What one side observed for one step (plan §3.2 ObservationRecord).
/// Hash line for matches; full record for divergences and 1% sampling.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservationRecord {
    pub scenario: String,
    pub seq: u64,
    pub session: String,
    pub side: Side,
    pub wire: Vec<WireMsg>,
    pub log: Vec<LogLine>,
    pub panic: Option<Panic>,
    pub crash: Option<Crash>,
    pub hang: Option<Hang>,
    /// Probe deck name → deck result (rows as the deck's own JSON shape,
    /// `catalog`, `invariants`, `locks`, `physical`, `stats`, `artifacts`,
    /// `decode`, `amcheck`, `admission`).
    pub probes: BTreeMap<String, Value>,
    pub server: Option<ServerCounters>,
    /// `ok` | `reconnected` | `dead` | `post-crash`.
    pub liveness: String,
    /// Wall time of the step on this side (reported, never compared).
    pub ms: u64,
    /// `SHOW server_version` text of this side (A must be 18.6).
    pub version: String,
}

impl ObservationRecord {
    pub const KEYS: &'static [&'static str] = &[
        "scenario", "seq", "session", "side", "wire", "log", "panic", "crash", "hang", "probes", "server",
        "liveness", "ms", "version",
    ];

    pub fn to_json(&self) -> Value {
        let panic = self
            .panic
            .as_ref()
            .map(|p| {
                Value::obj()
                    .with("site", Value::from(p.site.as_str()))
                    .with("message", p.message.to_json())
                    .with("query", p.query.as_ref().map(Bytes::to_json).unwrap_or(Value::Null))
            })
            .unwrap_or(Value::Null);
        let crash = self
            .crash
            .as_ref()
            .map(|c| {
                Value::obj()
                    .with("side", Value::from(c.side.as_str()))
                    .with("generation", Value::from(c.generation))
                    .with("signal", json::opt(c.signal.as_deref()))
                    .with("log_tail", Value::Arr(c.log_tail.iter().map(Bytes::to_json).collect()))
            })
            .unwrap_or(Value::Null);
        let hang = self
            .hang
            .as_ref()
            .map(|h| Value::obj().with("ms", Value::from(h.ms)).with("ladder", json::opt(h.ladder.as_deref())))
            .unwrap_or(Value::Null);
        Value::obj()
            .with("scenario", Value::from(self.scenario.as_str()))
            .with("seq", Value::from(self.seq))
            .with("session", Value::from(self.session.as_str()))
            .with("side", Value::from(self.side.as_str()))
            .with("wire", Value::Arr(self.wire.iter().map(WireMsg::to_json).collect()))
            .with("log", Value::Arr(self.log.iter().map(LogLine::to_json).collect()))
            .with("panic", panic)
            .with("crash", crash)
            .with("hang", hang)
            .with("probes", Value::Obj(self.probes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
            .with("server", self.server.as_ref().map(ServerCounters::to_json).unwrap_or(Value::Null))
            .with("liveness", Value::from(self.liveness.as_str()))
            .with("ms", Value::from(self.ms))
            .with("version", Value::from(self.version.as_str()))
    }

    pub fn from_json(v: &Value) -> Result<ObservationRecord, String> {
        reject_unknown(v, Self::KEYS, "ObservationRecord")?;
        let wire = req(v, "wire")?
            .as_arr()
            .ok_or("wire must be an array")?
            .iter()
            .map(WireMsg::from_json)
            .collect::<Result<Vec<_>, _>>()?;
        let log = match v.get("log") {
            None | Some(Value::Null) => Vec::new(),
            Some(l) => l
                .as_arr()
                .ok_or("log must be an array")?
                .iter()
                .map(LogLine::from_json)
                .collect::<Result<Vec<_>, _>>()?,
        };
        let panic = match v.get("panic") {
            None | Some(Value::Null) => None,
            Some(p) => {
                reject_unknown(p, &["site", "message", "query"], "panic")?;
                Some(Panic {
                    site: req_str(p, "site")?,
                    message: Bytes::from_json(req(p, "message")?)?,
                    query: opt_bytes(p, "query")?,
                })
            }
        };
        let crash = match v.get("crash") {
            None | Some(Value::Null) => None,
            Some(c) => {
                reject_unknown(c, &["side", "generation", "signal", "log_tail"], "crash")?;
                let log_tail = match c.get("log_tail") {
                    None | Some(Value::Null) => Vec::new(),
                    Some(t) => t
                        .as_arr()
                        .ok_or("log_tail must be an array")?
                        .iter()
                        .map(Bytes::from_json)
                        .collect::<Result<_, _>>()?,
                };
                Some(Crash {
                    side: Side::parse(&req_str(c, "side")?)?,
                    generation: req_u64(c, "generation")? as u32,
                    signal: opt_str(c, "signal")?,
                    log_tail,
                })
            }
        };
        let hang = match v.get("hang") {
            None | Some(Value::Null) => None,
            Some(h) => {
                reject_unknown(h, &["ms", "ladder"], "hang")?;
                Some(Hang { ms: req_u64(h, "ms")?, ladder: opt_str(h, "ladder")? })
            }
        };
        let probes = match v.get("probes") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(p) => p
                .as_obj()
                .ok_or("probes must be an object")?
                .iter()
                .map(|(k, x)| (k.clone(), x.clone()))
                .collect(),
        };
        let server = match v.get("server") {
            None | Some(Value::Null) => None,
            Some(s) => Some(ServerCounters::from_json(s)?),
        };
        Ok(ObservationRecord {
            scenario: req_str(v, "scenario")?,
            seq: req_u64(v, "seq")?,
            session: req_str(v, "session")?,
            side: Side::parse(&req_str(v, "side")?)?,
            wire,
            log,
            panic,
            crash,
            hang,
            probes,
            server,
            liveness: req_str(v, "liveness")?,
            ms: req_u64(v, "ms")?,
            version: req_str(v, "version")?,
        })
    }

    /// One JSONL line (canonical). Fails only if a probe result carries
    /// a non-finite float.
    pub fn to_jsonl(&self) -> Result<String, String> {
        json::to_canonical(&self.to_json())
    }

    pub fn from_jsonl(line: &str) -> Result<ObservationRecord, String> {
        ObservationRecord::from_json(&json::parse(line)?)
    }
}

// ---------------------------------------------------------------------
// Finding
// ---------------------------------------------------------------------

/// The audit classification vocabulary (docs/conformance/audit-18.6/
/// README.md "Classification vocabulary") plus the two proposed
/// extensions, `leak` and `alloc-policy`, which are PENDING the owner's
/// ruling (plan §12 Q3; docs/fuzzing/sitediff/VOCABULARY.md). A finding
/// in a pending class banks under `docs/fuzzing/sitediff/alloc.md`, not
/// BUG-LEDGER, until ruled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Crash,
    MissingError,
    SpuriousError,
    WrongResult,
    WrongSqlstate,
    WrongMessage,
    WrongPosition,
    PlanShape,
    MissingFeature,
    Hang,
    Cosmetic,
    /// Proposed extension (pending ruling).
    Leak,
    /// Proposed extension (pending ruling).
    AllocPolicy,
}

impl Class {
    pub const ALL: &'static [Class] = &[
        Class::Crash,
        Class::MissingError,
        Class::SpuriousError,
        Class::WrongResult,
        Class::WrongSqlstate,
        Class::WrongMessage,
        Class::WrongPosition,
        Class::PlanShape,
        Class::MissingFeature,
        Class::Hang,
        Class::Cosmetic,
        Class::Leak,
        Class::AllocPolicy,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Class::Crash => "crash",
            Class::MissingError => "missing-error",
            Class::SpuriousError => "spurious-error",
            Class::WrongResult => "wrong-result",
            Class::WrongSqlstate => "wrong-sqlstate",
            Class::WrongMessage => "wrong-message",
            Class::WrongPosition => "wrong-position",
            Class::PlanShape => "plan-shape",
            Class::MissingFeature => "missing-feature",
            Class::Hang => "hang",
            Class::Cosmetic => "cosmetic",
            Class::Leak => "leak",
            Class::AllocPolicy => "alloc-policy",
        }
    }

    pub fn parse(s: &str) -> Result<Class, String> {
        Class::ALL.iter().copied().find(|c| c.as_str() == s).ok_or_else(|| format!("unknown class {:?}", s))
    }

    /// True for the two classes outside the ratified vocabulary.
    pub fn is_pending_ruling(self) -> bool {
        matches!(self, Class::Leak | Class::AllocPolicy)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::High => "high",
            Severity::Medium => "medium",
            Severity::Low => "low",
        }
    }
    pub fn parse(s: &str) -> Result<Severity, String> {
        match s {
            "critical" => Ok(Severity::Critical),
            "high" => Ok(Severity::High),
            "medium" => Ok(Severity::Medium),
            "low" => Ok(Severity::Low),
            _ => Err(format!("unknown severity {:?}", s)),
        }
    }
}

/// Finding lifecycle (plan §3.2, §8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Classified, not yet reduced/verified.
    New,
    /// Reduced, 3/3 fresh, independent verdict matched; ledgered.
    Banked,
    /// Covered by a rulings.toml row (still recorded).
    Ruled,
    /// Same signature as an existing finding.
    Dup,
    /// Fresh re-verification below 3/3.
    Flaky,
    /// Known-unobservable tripwire (PR1603-1/PR1604-1 shape).
    Latent,
    /// Timing-class witness (N=5 CI cluster), ledgered in timing.md.
    Timing,
}

impl Status {
    pub const ALL: &'static [Status] =
        &[Status::New, Status::Banked, Status::Ruled, Status::Dup, Status::Flaky, Status::Latent, Status::Timing];

    pub fn as_str(self) -> &'static str {
        match self {
            Status::New => "NEW",
            Status::Banked => "BANKED",
            Status::Ruled => "RULED",
            Status::Dup => "DUP",
            Status::Flaky => "FLAKY",
            Status::Latent => "LATENT",
            Status::Timing => "TIMING",
        }
    }
    pub fn parse(s: &str) -> Result<Status, String> {
        Status::ALL.iter().copied().find(|c| c.as_str() == s).ok_or_else(|| format!("unknown status {:?}", s))
    }
}

/// `verify --independent` verdict (plan §8 step 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Independent {
    pub host_class: String,
    pub pgrust_sha: String,
    pub c_sha: String,
    /// The independent rig's own class (banks only when it equals `class`).
    pub verdict: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Verified {
    /// Fresh-datadir re-runs that reproduced (3/3 to bank).
    pub fresh_runs: u32,
    pub independent: Option<Independent>,
}

/// Timing-class witness (plan §8 step 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timing {
    pub settle_id: String,
    pub machine_class: String,
    /// N/N fresh CI cluster envs.
    pub n: u32,
}

/// Where the repro lives (plan §8 step 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Repro {
    /// Path of the reduced `cases.sql`, relative to the finding dir.
    pub cases_sql: String,
    /// Path of the `cell.json`, relative to the finding dir.
    pub cell_json: String,
    /// Optional isolation-style spec for multi-session repros.
    pub spec: Option<String>,
}

/// The audit `*.verified.json` finding fields carried verbatim, so a
/// Finding is a superset of the audit shape (plan §3.2).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AuditFields {
    /// C site(s), e.g. "analyze.c:316, analyze.c:321".
    pub site: Option<String>,
    pub reproduced: Option<bool>,
    /// Inline repro SQL of the audit finding.
    pub repro: Option<String>,
    pub c_output: Option<String>,
    pub pgrust_output: Option<String>,
    pub rust_site: Option<String>,
    pub already_tracked: Option<String>,
}

impl AuditFields {
    const KEYS: &'static [&'static str] =
        &["site", "reproduced", "repro", "c_output", "pgrust_output", "rust_site", "already_tracked"];

    fn to_json(&self) -> Value {
        Value::obj()
            .with("site", json::opt(self.site.as_deref()))
            .with("reproduced", json::opt(self.reproduced))
            .with("repro", json::opt(self.repro.as_deref()))
            .with("c_output", json::opt(self.c_output.as_deref()))
            .with("pgrust_output", json::opt(self.pgrust_output.as_deref()))
            .with("rust_site", json::opt(self.rust_site.as_deref()))
            .with("already_tracked", json::opt(self.already_tracked.as_deref()))
    }

    fn from_json(v: &Value) -> Result<AuditFields, String> {
        reject_unknown(v, Self::KEYS, "audit")?;
        let reproduced = match v.get("reproduced") {
            None | Some(Value::Null) => None,
            Some(b) => Some(b.as_bool().ok_or("audit.reproduced must be a boolean")?),
        };
        Ok(AuditFields {
            site: opt_str(v, "site")?,
            reproduced,
            repro: opt_str(v, "repro")?,
            c_output: opt_str(v, "c_output")?,
            pgrust_output: opt_str(v, "pgrust_output")?,
            rust_site: opt_str(v, "rust_site")?,
            already_tracked: opt_str(v, "already_tracked")?,
        })
    }
}

/// `findings/<id>.json` (plan §3.2 Finding).
#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
    pub id: String,
    /// `wire:E` | `wire:N` | `wire:T` | `wire:D` | `wire:C` | `wire:S` |
    /// `wire:A` | `log` | `panic` | `crash` | `hang` | `probe:<deck>` |
    /// `server:<counter>` | `explain`.
    pub plane: String,
    pub class: Class,
    pub severity: Severity,
    /// `plane | unit-or-message-template | sqlstate pair | field delta (+ Rust site)`.
    pub signature: String,
    /// Universe unit ids.
    pub units: Vec<String>,
    pub recipe: Option<String>,
    pub seed: Option<u64>,
    pub cell_id: String,
    /// Side A observation excerpt (the divergent plane, canonicalized).
    pub a: Value,
    /// Side B observation excerpt.
    pub b: Value,
    /// `@mark` ids / log slices bounding the finding.
    pub log_marks: Vec<String>,
    pub repro: Repro,
    pub verified: Verified,
    pub timing: Option<Timing>,
    /// rulings.toml id when status is RULED.
    pub rule: Option<String>,
    pub status: Status,
    pub audit: AuditFields,
}

impl Finding {
    pub const KEYS: &'static [&'static str] = &[
        "id", "plane", "class", "severity", "signature", "units", "recipe", "seed", "cell_id", "a", "b", "log_marks",
        "repro", "verified", "timing", "rule", "status", "audit",
    ];

    pub fn to_json(&self) -> Value {
        let independent = self
            .verified
            .independent
            .as_ref()
            .map(|i| {
                Value::obj()
                    .with("host_class", Value::from(i.host_class.as_str()))
                    .with("pgrust_sha", Value::from(i.pgrust_sha.as_str()))
                    .with("c_sha", Value::from(i.c_sha.as_str()))
                    .with("verdict", Value::from(i.verdict.as_str()))
            })
            .unwrap_or(Value::Null);
        let timing = self
            .timing
            .as_ref()
            .map(|t| {
                Value::obj()
                    .with("settle_id", Value::from(t.settle_id.as_str()))
                    .with("machine_class", Value::from(t.machine_class.as_str()))
                    .with("n", Value::from(t.n))
            })
            .unwrap_or(Value::Null);
        Value::obj()
            .with("id", Value::from(self.id.as_str()))
            .with("plane", Value::from(self.plane.as_str()))
            .with("class", Value::from(self.class.as_str()))
            .with("severity", Value::from(self.severity.as_str()))
            .with("signature", Value::from(self.signature.as_str()))
            .with("units", json::str_arr(&self.units))
            .with("recipe", json::opt(self.recipe.as_deref()))
            .with("seed", json::opt(self.seed))
            .with("cell_id", Value::from(self.cell_id.as_str()))
            .with("a", self.a.clone())
            .with("b", self.b.clone())
            .with("log_marks", json::str_arr(&self.log_marks))
            .with(
                "repro",
                Value::obj()
                    .with("cases_sql", Value::from(self.repro.cases_sql.as_str()))
                    .with("cell_json", Value::from(self.repro.cell_json.as_str()))
                    .with("spec", json::opt(self.repro.spec.as_deref())),
            )
            .with(
                "verified",
                Value::obj().with("fresh_runs", Value::from(self.verified.fresh_runs)).with("independent", independent),
            )
            .with("timing", timing)
            .with("rule", json::opt(self.rule.as_deref()))
            .with("status", Value::from(self.status.as_str()))
            .with("audit", self.audit.to_json())
    }

    pub fn from_json(v: &Value) -> Result<Finding, String> {
        reject_unknown(v, Self::KEYS, "Finding")?;
        let repro_v = req(v, "repro")?;
        reject_unknown(repro_v, &["cases_sql", "cell_json", "spec"], "repro")?;
        let verified_v = req(v, "verified")?;
        reject_unknown(verified_v, &["fresh_runs", "independent"], "verified")?;
        let independent = match verified_v.get("independent") {
            None | Some(Value::Null) => None,
            Some(i) => {
                reject_unknown(i, &["host_class", "pgrust_sha", "c_sha", "verdict"], "independent")?;
                Some(Independent {
                    host_class: req_str(i, "host_class")?,
                    pgrust_sha: req_str(i, "pgrust_sha")?,
                    c_sha: req_str(i, "c_sha")?,
                    verdict: req_str(i, "verdict")?,
                })
            }
        };
        let timing = match v.get("timing") {
            None | Some(Value::Null) => None,
            Some(t) => {
                reject_unknown(t, &["settle_id", "machine_class", "n"], "timing")?;
                Some(Timing {
                    settle_id: req_str(t, "settle_id")?,
                    machine_class: req_str(t, "machine_class")?,
                    n: req_u64(t, "n")? as u32,
                })
            }
        };
        let audit = match v.get("audit") {
            None | Some(Value::Null) => AuditFields::default(),
            Some(a) => AuditFields::from_json(a)?,
        };
        Ok(Finding {
            id: req_str(v, "id")?,
            plane: req_str(v, "plane")?,
            class: Class::parse(&req_str(v, "class")?)?,
            severity: Severity::parse(&req_str(v, "severity")?)?,
            signature: req_str(v, "signature")?,
            units: opt_str_arr(v, "units")?,
            recipe: opt_str(v, "recipe")?,
            seed: opt_u64(v, "seed")?,
            cell_id: req_str(v, "cell_id")?,
            a: v.get("a").cloned().unwrap_or(Value::Null),
            b: v.get("b").cloned().unwrap_or(Value::Null),
            log_marks: opt_str_arr(v, "log_marks")?,
            repro: Repro {
                cases_sql: req_str(repro_v, "cases_sql")?,
                cell_json: req_str(repro_v, "cell_json")?,
                spec: opt_str(repro_v, "spec")?,
            },
            verified: Verified { fresh_runs: req_u64(verified_v, "fresh_runs")? as u32, independent },
            timing,
            rule: opt_str(v, "rule")?,
            status: Status::parse(&req_str(v, "status")?)?,
            audit,
        })
    }

    /// The on-disk `findings/<id>.json` text.
    pub fn to_file(&self) -> Result<String, String> {
        json::to_pretty(&self.to_json())
    }

    pub fn from_file(text: &str) -> Result<Finding, String> {
        Finding::from_json(&json::parse(text)?)
    }
}

// ---------------------------------------------------------------------
// Cell (cell.json)
// ---------------------------------------------------------------------

/// A filesystem fixture the cell materializes on both sides (plan §6
/// "fs fixtures").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsFixture {
    /// `extension_control_path` | `libdir` | `tablespace` | `file` | `tsearch_data`.
    pub kind: String,
    /// Path relative to the cell's scratch root.
    pub path: String,
    /// Fixture flavour (e.g. `mode-000`, `file-as-dir`, `parquet`, `csv`).
    pub variant: Option<String>,
}

/// The environment cell (plan §6). `cell_id = sha256(canonical(cell))`
/// over the compact sorted JSON; the id is NOT a field of the cell so it
/// cannot go stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cell {
    /// C oracle: `version` (REL_18_6, refused otherwise) + build variants
    /// (`gcov`, `inj`, `ssl`, `xml`, `icu`, `openssl`).
    pub oracle_version: String,
    pub oracle_variants: Vec<String>,
    /// B build profile: `dev` | `dev-server` | `release`.
    pub b_build: String,
    /// B build features: `alloc-count`, `memstats`, ...
    pub b_features: Vec<String>,
    pub initdb_encoding: String,
    pub initdb_locale: String,
    pub initdb_checksums: bool,
    /// Named profile from `configprofiles.sh`, or None for sampled GUCs.
    pub conf_profile: Option<String>,
    /// Explicit GUC settings applied identically to both sides.
    pub conf_gucs: BTreeMap<String, String>,
    /// Keep the C-parity/datetime pins of runner.rs (false = `guc_pin: off`).
    pub conf_guc_pin: bool,
    /// Session `client_min_messages` co-draw (`notice`|`log`|`debug1`|`debug2`), if any.
    pub conf_cmm: Option<String>,
    /// `stderr` | `collector` (the `elog` cell) | `witness`.
    pub logging_mode: String,
    pub logging_prefix: String,
    pub logging_timezone: String,
    pub logging_min_messages: String,
    pub logging_csvlog: bool,
    pub logging_jsonlog: bool,
    /// `trust` | `password` | `md5` | `scram-sha-256` | `radius` | `ldap` | `reject`.
    pub hba_method: String,
    /// External responders booted for the cell (`radius-strict`, `ldap`, ...).
    pub hba_responders: Vec<String>,
    /// Extra pg_hba.conf lines, verbatim.
    pub hba_rules: Vec<String>,
    /// postgres server flags (`-b`).
    pub server_flags: Vec<String>,
    /// `single` | `fdw-loop` | `logical-live` | `standby` | `twosession` |
    /// `admission` | `memwatchdog` | `janitor` | `corrupt-page`.
    pub topology: String,
    pub fs_fixtures: Vec<FsFixture>,
    /// `linux` | `macos`.
    pub host: String,
    /// `none` | `antithesis`.
    pub faults: String,
}

impl Cell {
    pub const KEYS: &'static [&'static str] = &[
        "oracle", "b_build", "initdb", "conf", "logging", "hba", "server_flags", "topology", "fs_fixtures", "host",
        "faults",
    ];

    /// The base cell every lane's pre-PR smoke runs under.
    pub fn base() -> Cell {
        Cell {
            oracle_version: "REL_18_6".into(),
            oracle_variants: Vec::new(),
            b_build: "dev".into(),
            b_features: Vec::new(),
            initdb_encoding: "UTF8".into(),
            initdb_locale: "C".into(),
            initdb_checksums: true,
            conf_profile: Some("base".into()),
            conf_gucs: BTreeMap::new(),
            conf_guc_pin: true,
            conf_cmm: None,
            logging_mode: "stderr".into(),
            logging_prefix: "%m %b[%p] %q%a ".into(),
            logging_timezone: "America/Los_Angeles".into(),
            logging_min_messages: "warning".into(),
            logging_csvlog: false,
            logging_jsonlog: false,
            hba_method: "trust".into(),
            hba_responders: Vec::new(),
            hba_rules: Vec::new(),
            server_flags: Vec::new(),
            topology: "single".into(),
            fs_fixtures: Vec::new(),
            host: "linux".into(),
            faults: "none".into(),
        }
    }

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with(
                "oracle",
                Value::obj()
                    .with("version", Value::from(self.oracle_version.as_str()))
                    .with("variants", json::str_arr(&self.oracle_variants)),
            )
            .with(
                "b_build",
                Value::obj()
                    .with("profile", Value::from(self.b_build.as_str()))
                    .with("features", json::str_arr(&self.b_features)),
            )
            .with(
                "initdb",
                Value::obj()
                    .with("encoding", Value::from(self.initdb_encoding.as_str()))
                    .with("locale", Value::from(self.initdb_locale.as_str()))
                    .with("checksums", Value::from(self.initdb_checksums)),
            )
            .with(
                "conf",
                Value::obj()
                    .with("profile", json::opt(self.conf_profile.as_deref()))
                    .with("gucs", json::str_map(&self.conf_gucs))
                    .with("guc_pin", Value::from(self.conf_guc_pin))
                    .with("cmm", json::opt(self.conf_cmm.as_deref())),
            )
            .with(
                "logging",
                Value::obj()
                    .with("mode", Value::from(self.logging_mode.as_str()))
                    .with("prefix", Value::from(self.logging_prefix.as_str()))
                    .with("timezone", Value::from(self.logging_timezone.as_str()))
                    .with("min_messages", Value::from(self.logging_min_messages.as_str()))
                    .with("csvlog", Value::from(self.logging_csvlog))
                    .with("jsonlog", Value::from(self.logging_jsonlog)),
            )
            .with(
                "hba",
                Value::obj()
                    .with("method", Value::from(self.hba_method.as_str()))
                    .with("responders", json::str_arr(&self.hba_responders))
                    .with("rules", json::str_arr(&self.hba_rules)),
            )
            .with("server_flags", json::str_arr(&self.server_flags))
            .with("topology", Value::from(self.topology.as_str()))
            .with(
                "fs_fixtures",
                Value::Arr(
                    self.fs_fixtures
                        .iter()
                        .map(|f| {
                            Value::obj()
                                .with("kind", Value::from(f.kind.as_str()))
                                .with("path", Value::from(f.path.as_str()))
                                .with("variant", json::opt(f.variant.as_deref()))
                        })
                        .collect(),
                ),
            )
            .with("host", Value::from(self.host.as_str()))
            .with("faults", Value::from(self.faults.as_str()))
    }

    pub fn from_json(v: &Value) -> Result<Cell, String> {
        reject_unknown(v, Self::KEYS, "Cell")?;
        let oracle = req(v, "oracle")?;
        reject_unknown(oracle, &["version", "variants"], "oracle")?;
        let b_build = req(v, "b_build")?;
        reject_unknown(b_build, &["profile", "features"], "b_build")?;
        let initdb = req(v, "initdb")?;
        reject_unknown(initdb, &["encoding", "locale", "checksums"], "initdb")?;
        let conf = req(v, "conf")?;
        reject_unknown(conf, &["profile", "gucs", "guc_pin", "cmm"], "conf")?;
        let logging = req(v, "logging")?;
        reject_unknown(logging, &["mode", "prefix", "timezone", "min_messages", "csvlog", "jsonlog"], "logging")?;
        let hba = req(v, "hba")?;
        reject_unknown(hba, &["method", "responders", "rules"], "hba")?;
        let fs_fixtures = match v.get("fs_fixtures") {
            None | Some(Value::Null) => Vec::new(),
            Some(a) => a
                .as_arr()
                .ok_or("fs_fixtures must be an array")?
                .iter()
                .map(|f| {
                    reject_unknown(f, &["kind", "path", "variant"], "fs_fixture")?;
                    Ok(FsFixture { kind: req_str(f, "kind")?, path: req_str(f, "path")?, variant: opt_str(f, "variant")? })
                })
                .collect::<Result<Vec<_>, String>>()?,
        };
        Ok(Cell {
            oracle_version: req_str(oracle, "version")?,
            oracle_variants: opt_str_arr(oracle, "variants")?,
            b_build: req_str(b_build, "profile")?,
            b_features: opt_str_arr(b_build, "features")?,
            initdb_encoding: req_str(initdb, "encoding")?,
            initdb_locale: req_str(initdb, "locale")?,
            initdb_checksums: req_bool(initdb, "checksums")?,
            conf_profile: opt_str(conf, "profile")?,
            conf_gucs: str_map_of(conf, "gucs")?,
            conf_guc_pin: req_bool(conf, "guc_pin")?,
            conf_cmm: opt_str(conf, "cmm")?,
            logging_mode: req_str(logging, "mode")?,
            logging_prefix: req_str(logging, "prefix")?,
            logging_timezone: req_str(logging, "timezone")?,
            logging_min_messages: req_str(logging, "min_messages")?,
            logging_csvlog: req_bool(logging, "csvlog")?,
            logging_jsonlog: req_bool(logging, "jsonlog")?,
            hba_method: req_str(hba, "method")?,
            hba_responders: opt_str_arr(hba, "responders")?,
            hba_rules: opt_str_arr(hba, "rules")?,
            server_flags: opt_str_arr(v, "server_flags")?,
            topology: req_str(v, "topology")?,
            fs_fixtures,
            host: req_str(v, "host")?,
            faults: req_str(v, "faults")?,
        })
    }

    /// Compact sorted JSON — the hashing form.
    pub fn canonical_json(&self) -> String {
        json::to_canonical(&self.to_json()).expect("Cell has no floats")
    }

    /// `sha256(canonical_json)` as 64 lowercase hex chars.
    pub fn cell_id(&self) -> String {
        hex(&pg_sha2::sha256(self.canonical_json().as_bytes()))
    }

    /// The on-disk `cell.json` text.
    pub fn to_file(&self) -> String {
        json::to_pretty(&self.to_json()).expect("Cell has no floats")
    }

    pub fn from_file(text: &str) -> Result<Cell, String> {
        Cell::from_json(&json::parse(text)?)
    }
}

// ---------------------------------------------------------------------
// RecipeHeader (`-- key: value` block at the top of a recipe .sql)
// ---------------------------------------------------------------------

/// The header comment block of `recipes/<subsystem>/<cfile>/<id>.sql`
/// (plan §3.2 Recipe). Every line of the block is `-- key: value`; the
/// block ends at the first line that is not such a comment (blank lines
/// inside the block are allowed). List values are comma-separated;
/// `slots` is `name:type, name:type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipeHeader {
    pub id: String,
    /// Universe unit ids.
    pub targets: Vec<String>,
    /// Cell name (`base`, `astm`, `elog`, ...).
    pub env: String,
    /// `superuser` | `owner` | `role=<slot>`.
    pub session: String,
    /// `simple` | `extended` | `raw:<probe>`.
    pub protocol: String,
    /// Slot name → SQL type (or `opaque:<typname>`).
    pub slots: BTreeMap<String, String>,
    /// Catalog facts the recipe leaves behind.
    pub provides: Vec<String>,
    /// State predicates that must hold before the recipe runs.
    pub requires: Vec<String>,
    pub ordered: Ordered,
    /// SQLSTATE or message template A must emit for the recipe to be proven.
    pub expect_c: Option<String>,
    /// `transcript` | `self:no-panic` | `self:invariant:<n>` |
    /// `self:alloc-rows` | `self:mem-slope` | `xprofile:<pair>`.
    pub oracle: String,
    /// `audit` | `mined:<file>:<line>` | `author` | `propagate:<class>` | `catalog:<bug-id>`.
    pub origin: String,
}

impl RecipeHeader {
    pub const KEYS: &'static [&'static str] = &[
        "id", "targets", "env", "session", "protocol", "slots", "provides", "requires", "ordered", "expect_c",
        "oracle", "origin",
    ];
    const REQUIRED: &'static [&'static str] =
        &["id", "targets", "env", "session", "protocol", "ordered", "oracle", "origin"];

    fn split_list(v: &str) -> Vec<String> {
        v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect()
    }

    /// Parse the header block from the full recipe text. Returns the
    /// header and the byte offset where the SQL body starts.
    pub fn parse(text: &str) -> Result<(RecipeHeader, usize), String> {
        let mut kv: BTreeMap<String, String> = BTreeMap::new();
        let mut body_start = 0usize;
        let mut seen_any = false;
        for line in text.split_inclusive('\n') {
            let trimmed = line.trim_end_matches(['\n', '\r']);
            if trimmed.trim().is_empty() {
                if !seen_any {
                    body_start += line.len();
                    continue;
                }
                body_start += line.len();
                continue;
            }
            let Some(rest) = trimmed.strip_prefix("--") else { break };
            let rest = rest.trim();
            let Some((k, v)) = rest.split_once(':') else { break };
            let k = k.trim();
            if k.is_empty() || k.contains(char::is_whitespace) || !Self::KEYS.contains(&k) {
                break;
            }
            if kv.insert(k.to_string(), v.trim().to_string()).is_some() {
                return Err(format!("recipe header: duplicate key {:?}", k));
            }
            seen_any = true;
            body_start += line.len();
        }
        for k in Self::REQUIRED {
            if !kv.contains_key(*k) {
                return Err(format!("recipe header: missing required key {:?}", k));
            }
        }
        let mut slots = BTreeMap::new();
        if let Some(s) = kv.get("slots") {
            for item in Self::split_list(s) {
                let Some((name, ty)) = item.split_once(':') else {
                    return Err(format!("recipe header: slot {:?} needs name:type", item));
                };
                slots.insert(name.trim().to_string(), ty.trim().to_string());
            }
        }
        let expect_c = kv.get("expect_c").filter(|s| !s.is_empty()).cloned();
        let header = RecipeHeader {
            id: kv["id"].clone(),
            targets: Self::split_list(&kv["targets"]),
            env: kv["env"].clone(),
            session: kv["session"].clone(),
            protocol: kv["protocol"].clone(),
            slots,
            provides: kv.get("provides").map(|s| Self::split_list(s)).unwrap_or_default(),
            requires: kv.get("requires").map(|s| Self::split_list(s)).unwrap_or_default(),
            ordered: Ordered::parse(&kv["ordered"])?,
            expect_c,
            oracle: kv["oracle"].clone(),
            origin: kv["origin"].clone(),
        };
        Ok((header, body_start))
    }

    /// Render the header block (fixed key order, every key present so
    /// the block round-trips byte-stably through `parse`).
    pub fn render(&self) -> String {
        let mut out = String::new();
        let list = |v: &[String]| v.join(", ");
        let _ = writeln!(out, "-- id: {}", self.id);
        let _ = writeln!(out, "-- targets: {}", list(&self.targets));
        let _ = writeln!(out, "-- env: {}", self.env);
        let _ = writeln!(out, "-- session: {}", self.session);
        let _ = writeln!(out, "-- protocol: {}", self.protocol);
        let slots: Vec<String> = self.slots.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
        let _ = writeln!(out, "-- slots: {}", slots.join(", "));
        let _ = writeln!(out, "-- provides: {}", list(&self.provides));
        let _ = writeln!(out, "-- requires: {}", list(&self.requires));
        let _ = writeln!(out, "-- ordered: {}", self.ordered.as_str());
        let _ = writeln!(out, "-- expect_c: {}", self.expect_c.as_deref().unwrap_or(""));
        let _ = writeln!(out, "-- oracle: {}", self.oracle);
        let _ = writeln!(out, "-- origin: {}", self.origin);
        out
    }

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with("id", Value::from(self.id.as_str()))
            .with("targets", json::str_arr(&self.targets))
            .with("env", Value::from(self.env.as_str()))
            .with("session", Value::from(self.session.as_str()))
            .with("protocol", Value::from(self.protocol.as_str()))
            .with("slots", json::str_map(&self.slots))
            .with("provides", json::str_arr(&self.provides))
            .with("requires", json::str_arr(&self.requires))
            .with("ordered", Value::from(self.ordered.as_str()))
            .with("expect_c", json::opt(self.expect_c.as_deref()))
            .with("oracle", Value::from(self.oracle.as_str()))
            .with("origin", Value::from(self.origin.as_str()))
    }

    pub fn from_json(v: &Value) -> Result<RecipeHeader, String> {
        reject_unknown(v, Self::KEYS, "RecipeHeader")?;
        Ok(RecipeHeader {
            id: req_str(v, "id")?,
            targets: req_str_arr(v, "targets")?,
            env: req_str(v, "env")?,
            session: req_str(v, "session")?,
            protocol: req_str(v, "protocol")?,
            slots: str_map_of(v, "slots")?,
            provides: opt_str_arr(v, "provides")?,
            requires: opt_str_arr(v, "requires")?,
            ordered: Ordered::parse(&req_str(v, "ordered")?)?,
            expect_c: opt_str(v, "expect_c")?,
            oracle: req_str(v, "oracle")?,
            origin: req_str(v, "origin")?,
        })
    }
}

// ---------------------------------------------------------------------
// Minimal TOML (the rulings.toml subset)
// ---------------------------------------------------------------------

pub mod toml {
    //! Exactly the TOML subset `rulings.toml` uses: `#` comments,
    //! `[[name]]` array-of-tables headers, `key = "basic string"`,
    //! `key = 'literal string'`, `key = <integer>`, `key = true|false`,
    //! `key = ["a", "b"]`. No dotted keys, no inline tables, no
    //! multi-line strings, no floats/dates: anything else is an error
    //! naming the line. Values are returned as `json::Value` so the
    //! contracts share one accessor set.

    use super::json::Value;

    /// Parsed document: top-level scalars plus the array-of-tables
    /// entries in file order, each `(table name, fields)`.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Doc {
        pub top: Vec<(String, Value)>,
        pub tables: Vec<(String, Vec<(String, Value)>)>,
    }

    fn unescape_basic(s: &str, line: usize) -> Result<String, String> {
        let mut out = String::new();
        let mut cs = s.chars();
        while let Some(c) = cs.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match cs.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('u') => {
                    let h: String = cs.by_ref().take(4).collect();
                    let cp = u32::from_str_radix(&h, 16).map_err(|_| format!("toml line {}: bad \\u escape", line))?;
                    out.push(char::from_u32(cp).ok_or_else(|| format!("toml line {}: bad code point", line))?);
                }
                other => return Err(format!("toml line {}: bad escape \\{}", line, other.unwrap_or(' '))),
            }
        }
        Ok(out)
    }

    /// Scan one basic string starting at `s[0] == '"'`; returns (raw
    /// content, rest after the closing quote).
    fn take_basic(s: &str, line: usize) -> Result<(&str, &str), String> {
        let bytes = s.as_bytes();
        let mut i = 1;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' => i += 2,
                b'"' => return Ok((&s[1..i], &s[i + 1..])),
                _ => i += 1,
            }
        }
        Err(format!("toml line {}: unterminated string", line))
    }

    fn parse_scalar<'a>(s: &'a str, line: usize) -> Result<(Value, &'a str), String> {
        let s = s.trim_start();
        if s.starts_with('"') {
            let (raw, after) = take_basic(s, line)?;
            return Ok((Value::Str(unescape_basic(raw, line)?), after));
        }
        if let Some(rest) = s.strip_prefix('\'') {
            let end = rest.find('\'').ok_or_else(|| format!("toml line {}: unterminated literal string", line))?;
            return Ok((Value::Str(rest[..end].to_string()), &rest[end + 1..]));
        }
        if let Some(rest) = s.strip_prefix("true") {
            return Ok((Value::Bool(true), rest));
        }
        if let Some(rest) = s.strip_prefix("false") {
            return Ok((Value::Bool(false), rest));
        }
        let end = s.find(|c: char| !(c.is_ascii_digit() || c == '-' || c == '_')).unwrap_or(s.len());
        let num = &s[..end];
        if num.is_empty() {
            return Err(format!("toml line {}: unsupported value {:?}", line, s));
        }
        let i = num.replace('_', "").parse::<i64>().map_err(|e| format!("toml line {}: bad integer {:?}: {}", line, num, e))?;
        Ok((Value::Int(i), &s[end..]))
    }

    fn parse_value(s: &str, line: usize) -> Result<Value, String> {
        let s = s.trim();
        if let Some(inner) = s.strip_prefix('[') {
            let inner = inner.strip_suffix(']').ok_or_else(|| format!("toml line {}: unterminated array", line))?;
            let mut items = Vec::new();
            let mut rest = inner.trim();
            while !rest.is_empty() {
                let (v, after) = parse_scalar(rest, line)?;
                items.push(v);
                rest = after.trim_start();
                if let Some(r) = rest.strip_prefix(',') {
                    rest = r.trim_start();
                } else if !rest.is_empty() {
                    return Err(format!("toml line {}: expected ',' in array", line));
                }
            }
            return Ok(Value::Arr(items));
        }
        let (v, after) = parse_scalar(s, line)?;
        let after = after.trim();
        if !after.is_empty() && !after.starts_with('#') {
            return Err(format!("toml line {}: trailing data {:?}", line, after));
        }
        Ok(v)
    }

    /// Strip a trailing `# comment` that is not inside a string.
    fn strip_comment(line: &str) -> &str {
        let mut in_basic = false;
        let mut in_literal = false;
        let mut esc = false;
        for (i, c) in line.char_indices() {
            match c {
                '\\' if in_basic => esc = !esc,
                '"' if !in_literal && !esc => in_basic = !in_basic,
                '\'' if !in_basic => in_literal = !in_literal,
                '#' if !in_basic && !in_literal => return &line[..i],
                _ => esc = false,
            }
            if c != '\\' {
                esc = false;
            }
        }
        line
    }

    pub fn parse(text: &str) -> Result<Doc, String> {
        let mut doc = Doc { top: Vec::new(), tables: Vec::new() };
        let mut current: Option<usize> = None;
        for (idx, raw) in text.lines().enumerate() {
            let line_no = idx + 1;
            let line = strip_comment(raw).trim();
            if line.is_empty() {
                continue;
            }
            if let Some(h) = line.strip_prefix("[[") {
                let name = h.strip_suffix("]]").ok_or_else(|| format!("toml line {}: bad table header", line_no))?.trim();
                if name.is_empty() || name.contains(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-')) {
                    return Err(format!("toml line {}: bad table name {:?}", line_no, name));
                }
                doc.tables.push((name.to_string(), Vec::new()));
                current = Some(doc.tables.len() - 1);
                continue;
            }
            if line.starts_with('[') {
                return Err(format!("toml line {}: plain [table] headers are not supported", line_no));
            }
            let (k, v) = line.split_once('=').ok_or_else(|| format!("toml line {}: expected key = value", line_no))?;
            let k = k.trim();
            if k.is_empty() || k.contains(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-')) {
                return Err(format!("toml line {}: bad key {:?}", line_no, k));
            }
            let v = parse_value(v, line_no)?;
            let target = match current {
                Some(t) => &mut doc.tables[t].1,
                None => &mut doc.top,
            };
            if target.iter().any(|(ek, _)| ek == k) {
                return Err(format!("toml line {}: duplicate key {:?}", line_no, k));
            }
            target.push((k.to_string(), v));
        }
        Ok(doc)
    }

    /// Render a string as a TOML basic string.
    pub fn quote(s: &str) -> String {
        let mut out = String::from("\"");
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
        out
    }
}

// ---------------------------------------------------------------------
// Ruling (rulings.toml rows)
// ---------------------------------------------------------------------

/// One `[[ruling]]` row of `docs/fuzzing/rulings.toml` (plan §4.2).
/// First match wins; a ruling covers one plane; every ruled hit still
/// lands in findings.jsonl as `ruled:<id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ruling {
    pub id: String,
    /// The plane the ruling masks (`wire:E`, `wire:N`, `rows`, `explain`,
    /// `log`, `meta`, `tag`, `notify`, `probe:<deck>`, `server:<counter>`).
    pub plane: String,
    /// The field within the plane (`M`, `H`, `R`, `cell`, `count`, ...),
    /// None = whole plane.
    pub field: Option<String>,
    /// Regex the A-side value must match (None = any).
    pub a: Option<String>,
    /// Regex the B-side value must match (None = any).
    pub b: Option<String>,
    /// Ruling reference: doc path, note, or the ruling text.
    pub reference: String,
    pub owner: String,
    /// `YYYY-MM-DD`.
    pub created: String,
    /// `YYYY-MM-DD`; only cosmetic/HINT rulings expire.
    pub expires: Option<String>,
    /// CI-run hit counter (`sitediff rulings audit`).
    pub hits: u64,
    /// SQL statement-kind scope (`COPY`, `DROP TABLE`, ...), None = any.
    pub stmt: Option<String>,
}

impl Ruling {
    pub const KEYS: &'static [&'static str] =
        &["id", "plane", "field", "a", "b", "ref", "owner", "created", "expires", "hits", "stmt"];
    const REQUIRED: &'static [&'static str] = &["id", "plane", "ref", "owner", "created", "hits"];

    fn from_fields(fields: &[(String, Value)]) -> Result<Ruling, String> {
        let v = Value::Obj(fields.to_vec());
        reject_unknown(&v, Self::KEYS, "ruling")?;
        for k in Self::REQUIRED {
            if v.get(k).is_none() {
                return Err(format!("ruling: missing required key {:?}", k));
            }
        }
        let date_ok = |s: &str| s.len() == 10 && s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-';
        let created = req_str(&v, "created")?;
        if !date_ok(&created) {
            return Err(format!("ruling {:?}: created must be YYYY-MM-DD", req_str(&v, "id")?));
        }
        let expires = opt_str(&v, "expires")?;
        if let Some(e) = &expires {
            if !date_ok(e) {
                return Err(format!("ruling {:?}: expires must be YYYY-MM-DD", req_str(&v, "id")?));
            }
        }
        Ok(Ruling {
            id: req_str(&v, "id")?,
            plane: req_str(&v, "plane")?,
            field: opt_str(&v, "field")?,
            a: opt_str(&v, "a")?,
            b: opt_str(&v, "b")?,
            reference: req_str(&v, "ref")?,
            owner: req_str(&v, "owner")?,
            created,
            expires,
            hits: req_u64(&v, "hits")?,
            stmt: opt_str(&v, "stmt")?,
        })
    }

    pub fn to_json(&self) -> Value {
        Value::obj()
            .with("id", Value::from(self.id.as_str()))
            .with("plane", Value::from(self.plane.as_str()))
            .with("field", json::opt(self.field.as_deref()))
            .with("a", json::opt(self.a.as_deref()))
            .with("b", json::opt(self.b.as_deref()))
            .with("ref", Value::from(self.reference.as_str()))
            .with("owner", Value::from(self.owner.as_str()))
            .with("created", Value::from(self.created.as_str()))
            .with("expires", json::opt(self.expires.as_deref()))
            .with("hits", Value::from(self.hits))
            .with("stmt", json::opt(self.stmt.as_deref()))
    }

    /// Render one `[[ruling]]` block: fixed key order, optional keys
    /// omitted when None (the parse→render cycle is byte-stable for
    /// files written this way).
    pub fn render(&self) -> String {
        let mut out = String::from("[[ruling]]\n");
        let _ = writeln!(out, "id = {}", toml::quote(&self.id));
        let _ = writeln!(out, "plane = {}", toml::quote(&self.plane));
        if let Some(f) = &self.field {
            let _ = writeln!(out, "field = {}", toml::quote(f));
        }
        if let Some(a) = &self.a {
            let _ = writeln!(out, "a = {}", toml::quote(a));
        }
        if let Some(b) = &self.b {
            let _ = writeln!(out, "b = {}", toml::quote(b));
        }
        if let Some(s) = &self.stmt {
            let _ = writeln!(out, "stmt = {}", toml::quote(s));
        }
        let _ = writeln!(out, "ref = {}", toml::quote(&self.reference));
        let _ = writeln!(out, "owner = {}", toml::quote(&self.owner));
        let _ = writeln!(out, "created = {}", toml::quote(&self.created));
        if let Some(e) = &self.expires {
            let _ = writeln!(out, "expires = {}", toml::quote(e));
        }
        let _ = writeln!(out, "hits = {}", self.hits);
        out
    }
}

/// The whole ledger: rows in file order (first match wins).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Rulings {
    pub rulings: Vec<Ruling>,
}

impl Rulings {
    pub fn parse(text: &str) -> Result<Rulings, String> {
        let doc = toml::parse(text)?;
        if let Some((k, _)) = doc.top.first() {
            return Err(format!("rulings.toml: unexpected top-level key {:?}", k));
        }
        let mut rulings = Vec::new();
        for (name, fields) in &doc.tables {
            if name != "ruling" {
                return Err(format!("rulings.toml: unexpected table [[{}]]", name));
            }
            let r = Ruling::from_fields(fields)?;
            if rulings.iter().any(|x: &Ruling| x.id == r.id) {
                return Err(format!("rulings.toml: duplicate ruling id {:?}", r.id));
            }
            rulings.push(r);
        }
        Ok(Rulings { rulings })
    }

    /// Render the ledger (leading comment header + one block per row,
    /// blank-line separated).
    pub fn render(&self) -> String {
        let mut out = String::from(RULINGS_HEADER);
        for r in &self.rulings {
            out.push('\n');
            out.push_str(&r.render());
        }
        out
    }

    pub fn get(&self, id: &str) -> Option<&Ruling> {
        self.rulings.iter().find(|r| r.id == id)
    }

    pub fn to_json(&self) -> Value {
        Value::obj().with("ruling", Value::Arr(self.rulings.iter().map(Ruling::to_json).collect()))
    }
}

/// The fixed comment block at the top of every rendered rulings.toml.
pub const RULINGS_HEADER: &str = "\
# sitediff rulings ledger (plan §4.2). One [[ruling]] per row; first match
# wins; a ruling covers exactly one plane; every ruled hit is still recorded
# as ruled:<id>. Only cosmetic/HINT rulings carry `expires`. `hits` is
# maintained by `sitediff rulings audit`. Schema: crates/bin/fuzzgen/schemas/rulings.schema.json
";

// ---------------------------------------------------------------------
// Tests: fixtures parse, round-trip byte-stably, carry every schema-
// required key. `FUZZGEN_REGEN_FIXTURES=1 cargo test -p fuzzgen contracts`
// rewrites the fixtures from the builders below (the committed files
// ARE that output, so the round-trip tests double as a determinism pin).
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn crate_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }
    fn fixture_path(name: &str) -> PathBuf {
        crate_dir().join("fixtures/contracts").join(name)
    }
    fn schema_path(name: &str) -> PathBuf {
        crate_dir().join("schemas").join(name)
    }
    fn read(p: &PathBuf) -> String {
        std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {}", p.display(), e))
    }
    fn regen() -> bool {
        std::env::var_os("FUZZGEN_REGEN_FIXTURES").is_some()
    }
    /// Compare a rendered fixture against disk (or rewrite it under regen).
    fn pin(name: &str, rendered: &str) {
        let p = fixture_path(name);
        if regen() {
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, rendered).unwrap();
        }
        assert_eq!(read(&p), rendered, "fixture {} differs from its builder output", name);
    }

    /// Schema walk: every `required` key present, every present key
    /// known when `additionalProperties: false`, recursing through
    /// `properties` objects and `items` arrays. No full validator.
    fn check_schema(schema: &Value, v: &Value, path: &str) {
        if let Some(reqs) = schema.get("required").and_then(Value::as_arr) {
            for r in reqs {
                let k = r.as_str().unwrap();
                assert!(v.get(k).is_some(), "{}: missing required key {:?}", path, k);
            }
        }
        let props = schema.get("properties");
        if let (Some(props), Some(fields)) = (props, v.as_obj()) {
            let closed = schema.get("additionalProperties") == Some(&Value::Bool(false));
            for (k, x) in fields {
                match props.get(k) {
                    Some(sub) => {
                        if !x.is_null() {
                            check_schema(sub, x, &format!("{}.{}", path, k));
                        }
                    }
                    None => assert!(!closed, "{}: key {:?} not in schema properties", path, k),
                }
            }
        }
        if let (Some(items), Some(arr)) = (schema.get("items"), v.as_arr()) {
            for (i, x) in arr.iter().enumerate() {
                check_schema(items, x, &format!("{}[{}]", path, i));
            }
        }
    }

    fn schema(name: &str) -> Value {
        json::parse(&read(&schema_path(name))).unwrap()
    }

    // ---- builders (the fixtures) ----

    fn step_fixture() -> StepRecord {
        let mut slots = BTreeMap::new();
        slots.insert("tbl".into(), "fz_hp_3".into());
        slots.insert("n".into(), "17".into());
        StepRecord {
            scenario: "seed-3878502244648856050/cell-base".into(),
            seq: 42,
            session: "s1".into(),
            role: "superuser".into(),
            kind: StepKind::Sql,
            sql: Some("INSERT INTO fz_hp_3 (a, b) VALUES ((1, 2));".into()),
            xproto: None,
            productions: vec!["dml.insert".into(), "dml.insert.values".into(), "expr.row".into()],
            targets: vec!["ereport:parser_analyze.c:1123".into()],
            ordered: Ordered::None,
            expect_c: Some("42601".into()),
            bracket: Some("txn-7".into()),
            recipe: Some("parser/analyze/parser_analyze-2".into()),
            mutant: Some(Mutant { op: "arity".into(), seed: 91 }),
            slots,
        }
    }

    fn step_xproto_fixture() -> StepRecord {
        StepRecord {
            scenario: "seed-3878502244648856050/cell-base".into(),
            seq: 43,
            session: "s1".into(),
            role: "superuser".into(),
            kind: StepKind::Xproto,
            sql: Some("SELECT a FROM fz_hp_3 WHERE b = $1 ORDER BY a".into()),
            xproto: Some(XProto {
                mode: "named_portal".into(),
                params: vec![Some(Bytes::text("2")), None],
                stmt: "ps1".into(),
                portal: "p1".into(),
                limit: 5,
                describe: "P".into(),
                repeat: 1,
            }),
            productions: vec!["select.simple".into(), "orderby:total".into()],
            targets: vec![],
            ordered: Ordered::Total,
            expect_c: None,
            bracket: None,
            recipe: None,
            mutant: None,
            slots: BTreeMap::new(),
        }
    }

    fn fields(pairs: &[(char, &str)]) -> ErrFields {
        pairs.iter().map(|(c, s)| (*c, Bytes::text(s))).collect()
    }

    fn common_wire_head() -> Vec<WireMsg> {
        vec![
            WireMsg::ParameterStatus { name: Bytes::text("client_encoding"), value: Bytes::text("UTF8") },
            WireMsg::BackendKeyData { pid: 41233, key: Bytes(vec![0x1f, 0x9a, 0x00, 0x7c]) },
        ]
    }

    /// Side A of an errpath step: an ErrorResponse carrying every field
    /// code the protocol defines (S V C M D H P p q W s t c d n F L R) —
    /// a not-null violation raised from a plpgsql EXECUTE, verbose.
    fn obs_error_fixture() -> ObservationRecord {
        let err = fields(&[
            ('S', "ERROR"),
            ('V', "ERROR"),
            ('C', "23502"),
            ('M', "null value in column \"a\" of relation \"t\" violates not-null constraint"),
            ('D', "Failing row contains (null, 1)."),
            ('H', "Supply a value for column \"a\" or drop the constraint."),
            ('P', "1"),
            ('p', "13"),
            ('q', "INSERT INTO t VALUES (NULL, 1)"),
            ('W', "PL/pgSQL function f() line 3 at EXECUTE"),
            ('s', "public"),
            ('t', "t"),
            ('c', "t_a_not_null"),
            ('d', "integer"),
            ('n', "a"),
            ('F', "execMain.c"),
            ('L', "1975"),
            ('R', "ExecConstraints"),
        ]);
        let mut wire = Vec::new();
        wire.push(WireMsg::ErrorResponse(err));
        wire.push(WireMsg::ReadyForQuery { status: 'E' });
        ObservationRecord {
            scenario: "seed-3878502244648856050/cell-base".into(),
            seq: 42,
            session: "s1".into(),
            side: Side::A,
            wire,
            log: vec![LogLine {
                source: "stderr".into(),
                raw: Bytes::text(
                    "2026-09-02 10:14:03.117 PDT client backend[41233] fuzz ERROR:  23502: null value in column \"a\" of relation \"t\" violates not-null constraint",
                ),
                ts: Some("2026-09-02 10:14:03.117 PDT".into()),
                backend_type: Some("client backend".into()),
                pid: Some(41233),
                app: Some("fuzz".into()),
                level: Some("ERROR".into()),
                sqlstate: Some("23502".into()),
                message: Some(Bytes::text(
                    "null value in column \"a\" of relation \"t\" violates not-null constraint",
                )),
                location: Some("ExecConstraints, execMain.c:1975".into()),
            }],
            panic: None,
            crash: None,
            hang: None,
            probes: BTreeMap::new(),
            server: None,
            liveness: "ok".into(),
            ms: 3,
            version: "18.6".into(),
        }
    }

    /// Side A of analyze-1: ANALYZE VERBOSE's INFO notices plus the
    /// command tag and a rows exchange with every T column field.
    fn obs_notice_fixture() -> ObservationRecord {
        let info = |m: &str, f: &str, l: &str, r: &str| {
            WireMsg::NoticeResponse(fields(&[
                ('S', "INFO"),
                ('V', "INFO"),
                ('C', "00000"),
                ('M', m),
                ('F', f),
                ('L', l),
                ('R', r),
            ]))
        };
        let mut wire = common_wire_head();
        wire.push(info("analyzing \"public.t\"", "analyze.c", "319", "do_analyze_rel"));
        wire.push(info(
            "\"t\": scanned 1 of 1 pages, containing 10 live rows and 0 dead rows; 10 rows in sample, 10 estimated total rows",
            "analyze.c",
            "1352",
            "acquire_sample_rows",
        ));
        wire.push(WireMsg::CommandComplete(Bytes::text("ANALYZE")));
        wire.push(WireMsg::ReadyForQuery { status: 'I' });
        wire.push(WireMsg::RowDescription(vec![
            ColDesc { name: Bytes::text("a"), tableoid: 16401, attnum: 1, typoid: 23, typlen: 4, typmod: -1, fmt: 0 },
            ColDesc { name: Bytes::text("?column?"), tableoid: 0, attnum: 0, typoid: 25, typlen: -1, typmod: -1, fmt: 0 },
        ]));
        wire.push(WireMsg::DataRow(vec![Some(Bytes::text("1")), None]));
        wire.push(WireMsg::DataRow(vec![Some(Bytes::text("2")), Some(Bytes(vec![0xff, 0x00, 0x41]))]));
        wire.push(WireMsg::CommandComplete(Bytes::text("SELECT 2")));
        wire.push(WireMsg::NotificationResponse {
            pid: 41233,
            channel: Bytes::text("fz_chan"),
            payload: Bytes::text("hello"),
        });
        wire.push(WireMsg::ReadyForQuery { status: 'I' });
        let mut probes = BTreeMap::new();
        probes.insert(
            "stats".into(),
            Value::obj().with("pg_stat_user_tables.t.analyze_count", Value::Int(1)),
        );
        ObservationRecord {
            scenario: "seed-3878502244648856050/cell-base".into(),
            seq: 7,
            session: "s1".into(),
            side: Side::A,
            wire,
            log: vec![],
            panic: None,
            crash: None,
            hang: None,
            probes,
            server: None,
            liveness: "ok".into(),
            ms: 12,
            version: "18.6".into(),
        }
    }

    /// Side B of a catcorrupt step: the panic marker pair, a crash the
    /// supervisor absorbed, post-crash liveness, B-only counters.
    fn obs_crash_fixture() -> ObservationRecord {
        let mut probes = BTreeMap::new();
        probes.insert("invariants".into(), Value::Arr(vec![]));
        ObservationRecord {
            scenario: "seed-3878502244648856050/cell-astm".into(),
            seq: 118,
            session: "s2".into(),
            side: Side::B,
            wire: vec![WireMsg::ErrorResponse(fields(&[
                ('S', "FATAL"),
                ('V', "FATAL"),
                ('C', "XX000"),
                ('M', "index \"t_expr_idx\" has 2 expressions but indexprs lists 1"),
                ('F', "lib.rs"),
                ('L', "0"),
                ('R', "examine_attribute"),
            ]))],
            log: vec![
                LogLine {
                    source: "stderr".into(),
                    raw: Bytes::text("thread 'backend-19' panicked at crates/backend/commands/analyze/src/lib.rs:475:17:"),
                    ts: None,
                    backend_type: None,
                    pid: None,
                    app: None,
                    level: None,
                    sqlstate: None,
                    message: None,
                    location: None,
                },
                LogLine {
                    source: "stderr".into(),
                    raw: Bytes::text("panicking backend query: ANALYZE t"),
                    ts: None,
                    backend_type: None,
                    pid: None,
                    app: None,
                    level: None,
                    sqlstate: None,
                    message: None,
                    location: None,
                },
            ],
            panic: Some(Panic {
                site: "crates/backend/commands/analyze/src/lib.rs:475:17".into(),
                message: Bytes::text("index has 2 expressions but indexprs lists 1"),
                query: Some(Bytes::text("ANALYZE t")),
            }),
            crash: Some(Crash {
                side: Side::B,
                generation: 2,
                signal: Some("SIGABRT".into()),
                log_tail: vec![
                    Bytes::text("2026-09-02 10:20:41.902 PDT postmaster[41200]  LOG:  server process (PID 41233) was terminated by signal 6: Abort trap"),
                    Bytes::text("2026-09-02 10:20:41.903 PDT postmaster[41200]  LOG:  all server processes terminated; reinitializing"),
                ],
            }),
            hang: None,
            probes,
            server: Some(ServerCounters {
                alloc_calls: Some(18321),
                alloc_bytes: Some(2_211_840),
                live_bytes: Some(94_208),
                committed_bytes: Some(67_108_864),
                rss_kb: Some(184_320),
                footprint: Some(1_048_576),
            }),
            liveness: "post-crash".into(),
            ms: 20_000,
            version: "18.6 (pgrust b49a9debcb5)".into(),
        }
    }

    fn cell_fixture() -> Cell {
        Cell::base()
    }

    fn finding_fixture() -> Finding {
        Finding {
            id: "analyze-1".into(),
            plane: "wire:N".into(),
            class: Class::WrongMessage,
            severity: Severity::Medium,
            signature: "wire:N | analyzing \"%s\" | 00000/- | N.presence".into(),
            units: vec![
                "ereport:analyze.c:316".into(),
                "ereport:analyze.c:321".into(),
                "ereport:analyze.c:1345".into(),
                "ereport:analyze.c:1429".into(),
                "ereport:analyze.c:1527".into(),
            ],
            recipe: Some("commands/analyze/analyze-1".into()),
            seed: None,
            cell_id: cell_fixture().cell_id(),
            a: Value::obj()
                .with("count", Value::Int(3))
                .with("first", Value::obj().with("S", Value::from("INFO")).with("C", Value::from("00000")).with("M", Value::from("analyzing \"public.t\""))),
            b: Value::obj().with("count", Value::Int(0)).with("first", Value::Null),
            log_marks: vec!["m-analyze-1-pre".into(), "m-analyze-1-post".into()],
            repro: Repro { cases_sql: "cases.sql".into(), cell_json: "cell.json".into(), spec: None },
            verified: Verified {
                fresh_runs: 3,
                independent: Some(Independent {
                    host_class: "CI-box".into(),
                    pgrust_sha: "b49a9debcb5".into(),
                    c_sha: "724edf9b".into(),
                    verdict: "wrong-message".into(),
                }),
            },
            timing: None,
            rule: None,
            status: Status::Banked,
            audit: AuditFields {
                site: Some("analyze.c:316, analyze.c:321, analyze.c:1345, analyze.c:1429, analyze.c:1527 (C LOCATION lines report the ereport closing lines 319/324/1352/1432/1530)".into()),
                reproduced: Some(true),
                repro: Some("create table t(a int); insert into t select g from generate_series(1,10) g;\nanalyze verbose t;\ncreate table p(a int); create table c() inherits (p); insert into c values (1);\nanalyze verbose only p;\ncreate table p2(a int); create table c2() inherits (p2); drop table c2;\nanalyze verbose p2;\ncreate table pt(a int) partition by list (a); create table pt1 partition of pt for values in (1) partition by list (a);\nanalyze verbose pt;".into()),
                c_output: Some("INFO:  00000: analyzing \"public.t\"\nINFO:  00000: \"t\": scanned 1 of 1 pages, containing 10 live rows and 0 dead rows; 10 rows in sample, 10 estimated total rows\nINFO:  00000: finished analyzing table \"postgres.public.t\" (+ usage lines, see analyze-2)\nANALYZE".into()),
                pgrust_output: Some("ANALYZE   (no INFO line for any of the four statements, nor for (verbose true), (verbose), or the analyze half of VACUUM (VERBOSE, ANALYZE); catalog side effects identical: pg_statistic count 1 for t, relhassubclass f for p2 after the 1429 path)".into()),
                rust_site: Some("crates/backend/commands/analyze/src/lib.rs: do_analyze_rel computes elevel (lib.rs:745-761) but only forwards it to the FDW acquire fn / acquire_inherited_sample_rows and never emits the 'analyzing' messages".into()),
                already_tracked: Some("None. docs/fuzzing/BUG-LEDGER.md has no ANALYZE VERBOSE / analyzing / finished analyzing row".into()),
            },
        }
    }

    fn recipe_fixture() -> RecipeHeader {
        let mut slots = BTreeMap::new();
        slots.insert("t".into(), "table".into());
        slots.insert("n".into(), "int4".into());
        RecipeHeader {
            id: "commands/analyze/analyze-1".into(),
            targets: vec!["ereport:analyze.c:316".into(), "ereport:analyze.c:321".into(), "ereport:analyze.c:1345".into()],
            env: "base".into(),
            session: "superuser".into(),
            protocol: "simple".into(),
            slots,
            provides: vec!["table(t)".into(), "stats(t)".into()],
            requires: vec!["rows(t)>=1".into()],
            ordered: Ordered::None,
            expect_c: Some("N:INFO:analyzing \"public.%s\"".into()),
            oracle: "transcript".into(),
            origin: "audit".into(),
        }
    }

    const RECIPE_BODY: &str = "\
create table t(a int);
insert into t select g from generate_series(1, :n) g;
analyze verbose :t;
drop table t;
";

    fn rulings_fixture() -> Rulings {
        let owner = "fuzzgen ruled.rs (migrated by L0.0)";
        let created = "2026-09-02";
        let row = |id: &str, plane: &str, field: Option<&str>, a: Option<&str>, b: Option<&str>, stmt: Option<&str>, reference: &str| Ruling {
            id: id.into(),
            plane: plane.into(),
            field: field.map(Into::into),
            a: a.map(Into::into),
            b: b.map(Into::into),
            reference: reference.into(),
            owner: owner.into(),
            created: created.into(),
            expires: None,
            hits: 0,
            stmt: stmt.map(Into::into),
        };
        Rulings {
            rulings: vec![
                row("b1-float-ulp", "rows", Some("cell"), Some(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"), Some(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"), None,
                    "B1 float-reassociation ruling: float surfaces compare by ulp, not text (ruled.rs FloatUlp; ulp_tol)"),
                row("b1-float-agg-soft", "rows", Some("cell"), None, None, None,
                    "B1 float-reassociation ruling: order-sensitive float aggregate result columns are ruled-soft (ruled.rs FloatAggSoft; render::soft_float_cols mask)"),
                row("copy-order", "rows", Some("order"), None, None, Some("COPY"),
                    "COPY order ruling: COPY/heap row order is nondeterministic, non-surface (ruled.rs CopyOrder; multiset-equal only)"),
                row("explain-counter", "explain", Some("counter"), Some(r"(Sort Method|Memory|Buckets|Batches|Disk|Planning Time|Execution Time):"), None, Some("EXPLAIN"),
                    "EXPLAIN runtime resource counters are implementation state, never compared; plan structure under COSTS OFF still compares strictly (ruled.rs ExplainCounter)"),
                row("explain-timing", "explain", Some("timing"), Some(r"actual time=\d+\.\d+\.\.\d+\.\d+"), None, Some("EXPLAIN"),
                    "EXPLAIN ANALYZE wall-clock timing text is never comparable; opt-in lanes only (gramwalk, --mask-explain-timing); actual rows still compare (ruled.rs ExplainTiming)"),
                row("explain-planning-buffers", "explain", Some("planning-buffers"), Some(r"^Planning:$"), None, Some("EXPLAIN"),
                    "EXPLAIN TEXT Planning: buffer-usage block presence is session cache state; node-level Buffers and Planning Time still compare (ruled.rs ExplainPlanningBuffers)"),
                row("xml-config", "wire:E", Some("M"), Some(r"^unsupported XML feature$"), Some(r"^(?!.*panicked).*$"), None,
                    "xml build-config ruling (LD1-N1): the pinned C oracle is built without libxml while pgrust dlopens libxml2; a pgrust XX000 on the same statement still escalates (ruled.rs XmlConfig; delete only if the CI cluster oracle builds with libxml, plan Q4)"),
                row("guc-inventory", "rows", Some("count"), None, None, None,
                    "GUC-inventory ruling: pg_settings / SHOW ALL row-count shape only (extra pgrust.* GUCs, retuned defaults; docs/design/env-to-guc.md); GUC values still compare strictly (ruled.rs GucInventory)"),
                row("instance-config", "rows", None, None, None, None,
                    "round-9 FP-9/FP-9b/FP-10 instance-config ruling: pg_hba_file_rules / pg_ident_file_mappings / pg_file_settings / pg_shmem_allocations / pg_stat_progress_* / pg_stat_activity / pg_locks are instance state; rowset shape only, errors still compare (ruled.rs InstanceConfig)"),
                row("encoding-carve", "wire:E", Some("M"), None, Some(r"server encoding .* (is not supported|only UTF8)"), None,
                    "UTF-8-only server-encoding carve, RATIFIED 2026-08-18 (docs/design/carve-ratifications.md §11): a B-side 0A000 carrying the carve citation carries no oracle signal; exact-message scope (ruled.rs EncodingCarve)"),
                row("instance-lsn", "rows", Some("cell"), Some(r"^[0-9A-F]+/[0-9A-F]+$"), Some(r"^[0-9A-F]+/[0-9A-F]+$"), None,
                    "round-9 instance-LSN ruling: backup-control LSNs (pg_backup_start/stop, pg_switch_wal, pg_create_restore_point) are cluster-local, never comparable (ruled.rs InstanceLsn)"),
                row("lz4-config", "wire:E", Some("M"), Some(r"^compression method lz4 not supported$"), Some(r"^(?!.*panicked).*$"), None,
                    "round-9 lz4 build-config ruling (mirror of xml-config): a --without-lz4 oracle rejects 0A000 where pgrust is lz4-capable; a pgrust XX000 still escalates (ruled.rs Lz4Config)"),
                row("tid-input-upstream", "wire:E", Some("C"), Some(r"^$"), Some(r"^22P02$"), None,
                    "round-9 tid-input ruling (project owner): C 18.x tidin accepts malformed forms like '(0,)'; pgrust matches the upstream-fixed strict behavior; exact-signature scope, B-only 22P02 'invalid input syntax for type tid' (ruled.rs TidInputUpstream)"),
                row("shared-catalog-tcu", "wire:E", Some("M"), None, Some(r"^tuple concurrently (updated|deleted)$"), None,
                    "shared-catalog concurrency ruling (2026-08-21 soak): XX000 'tuple concurrently updated/deleted' on shared-catalog DDL is C's own simple_heap_update race under concurrent drivers; any other XX000 escalates (ruled.rs SharedCatalogTcu)"),
                row("autoconf-shared-race", "wire:E", Some("M"), None, Some(r#"^could not parse contents of file "postgresql\.auto\.conf""#), Some("ALTER SYSTEM"),
                    "round-10 RB-14 autoconf shared-state ruling: postgresql.auto.conf is instance-global; asymmetric F0000 under concurrent batches is write/read interleaving (notes/internal classification notes) (ruled.rs AutoconfSharedRace)"),
                row("oid-literal", "rows", Some("cell"), Some(r"'\d{5,}'::oid"), Some(r"'\d{5,}'::oid"), None,
                    "round-7 OID ruling: user-object OIDs (>= 16384) embedded as '<n>'::oid literals in deparse text are masked; all other text compares exactly (ruled.rs OidLiteral)"),
                row("toast-name", "rows", Some("cell"), Some(r"pg_toast_\d{5,}"), Some(r"pg_toast_\d{5,}"), None,
                    "round-8 OID ruling: pg_toast_<n> relation names embed the owning table's user-range OID; builtin catalog toast names still compare exactly (ruled.rs ToastName)"),
                row("binary-udt-oid", "rows", Some("cell"), None, None, None,
                    "round-7 OID ruling: embedded user-range type oids in record_send/array_send binary images are masked structurally; every other byte of the image still compares exactly (ruled.rs BinaryUdtOid)"),
                row("cmp-magnitude", "rows", Some("cell"), Some(r"^-?\d+$"), Some(r"^-?\d+$"), None,
                    "round-7 cmp-magnitude ruling: C's memcmp-convention *cmp() comparators return arbitrary magnitude; scope is direct *cmp() select-list calls with sign-equal int4 results (ruled.rs CmpMagnitude)"),
                row("parallel-worker-init", "wire:E", Some("M"), None, Some(r"^parallel worker failed to initialize$"), None,
                    "round-7 asymmetric-fault ruling: thread-pause faults hit only the instrumented SUT, so B-only 55000 'parallel worker failed to initialize' is injected scheduling; liveness owned by the parallel canary (ruled.rs ParallelWorkerInit)"),
                row("fault-stmt-timeout", "wire:E", Some("M"), None, Some(r"^canceling statement due to statement timeout$"), None,
                    "round-20 asymmetric-fault ruling: B-only 57014 statement-timeout under the decks' symmetric statement_timeout SETs is injected scheduling; exact-message scope (ruled.rs FaultStmtTimeout)"),
                row("drop-autovacuum-deadlock", "wire:E", Some("M"), None, Some(r"^deadlock detected$"), Some("DROP TABLE|VACUUM FULL"),
                    "round-20/r21 symmetric-race ruling: DROP TABLE / VACUUM FULL vs autovacuum-ANALYZE ancestor-stats propagation forms a hard lock cycle that 40P01s in C too; B-only visibility is fault-widened timing (ruled.rs DropAutovacuumDeadlock)"),
                row("role-setting-shared-race", "wire:E", Some("M"), None, Some(r"pg_db_role_setting_databaseid_rol_index"), Some("ALTER ROLE ALL"),
                    "r21 symmetric-race ruling: concurrent ALTER ROLE ALL SET batches race the (0, 0) pg_db_role_setting singleton; C's AlterSetting is scan-then-insert with no unique-violation recovery (ruled.rs RoleSettingSharedRace)"),
                row("scroll-materialize", "explain", Some("shape"), Some(r"^\s*->\s*Materialize$"), None, Some("EXPLAIN DECLARE"),
                    "SCROLL-Materialize ruling (round-9 RB-10, ratified Michael 2026-07-17, notes/se-wave10-integration.md §5 item 2): pgrust omits C's planner Materialize wrap for SCROLL cursors; the diff must vanish once the top-level wrap is stripped from A (ruled.rs ScrollMaterialize)"),
                row("tie-ordering", "rows", Some("order"), None, None, None,
                    "docs/conformance/tie-ordering.md: tie order under an underdetermined ORDER BY; multiset-equal only, never under ordered: total (ruled.rs TieOrder)"),
            ],
        }
    }

    // ---- json module ----

    #[test]
    fn json_parse_and_canonical() {
        let v = json::parse(r#" {"b": [1, -2, 3.5, "xé\n", null, true], "a": {"z": 1, "y": {}}} "#).unwrap();
        assert_eq!(
            json::to_canonical(&v).unwrap(),
            r#"{"a":{"y":{},"z":1},"b":[1,-2,3.5,"xé\n",null,true]}"#
        );
        let again = json::parse(&json::to_pretty(&v).unwrap()).unwrap();
        assert_eq!(json::to_canonical(&again).unwrap(), json::to_canonical(&v).unwrap());
        assert!(json::parse("{\"a\":1,}").is_err());
        assert!(json::parse("{\"a\":1} x").is_err());
        assert!(json::parse("{\"a\":1,\"a\":2}").is_err());
        assert!(json::parse("\"\\ud83d\\ude00\"").unwrap().as_str().unwrap().starts_with('\u{1F600}'));
        assert_eq!(json::to_canonical(&Value::Float(2.0)).unwrap(), "2.0");
        assert!(json::to_canonical(&Value::Float(f64::NAN)).is_err());
    }

    #[test]
    fn bytes_json_is_lossless() {
        for b in [b"plain".to_vec(), b"tab\tand\nnewline".to_vec(), vec![0xff, 0x00, 0x41], b"\x01ctl".to_vec()] {
            let v = Bytes(b.clone()).to_json();
            assert_eq!(Bytes::from_json(&v).unwrap(), Bytes(b));
        }
        assert_eq!(Bytes(vec![0xff, 0x00]).to_json(), Value::obj().with("hex", Value::from("ff00")));
    }

    // ---- StepRecord ----

    #[test]
    fn step_record_fixture() {
        for (name, rec) in [("step-record.json", step_fixture()), ("step-record-xproto.json", step_xproto_fixture())] {
            let rendered = json::to_pretty(&rec.to_json()).unwrap();
            pin(name, &rendered);
            let parsed = StepRecord::from_json(&json::parse(&read(&fixture_path(name))).unwrap()).unwrap();
            assert_eq!(parsed, rec);
            assert_eq!(json::to_pretty(&parsed.to_json()).unwrap(), rendered);
            assert_eq!(StepRecord::from_jsonl(&rec.to_jsonl()).unwrap(), rec);
            check_schema(&schema("step-record.schema.json"), &parsed.to_json(), name);
        }
        assert_eq!(StepKind::parse("probe:catalog").unwrap(), StepKind::Probe("catalog".into()));
        assert_eq!(StepKind::parse("env:reload").unwrap().to_string_key(), "env:reload");
        assert!(StepKind::parse("probe:").is_err());
        assert!(StepKind::parse("bogus").is_err());
    }

    // ---- ObservationRecord ----

    #[test]
    fn observation_record_fixtures() {
        let cases = [
            ("observation-error.json", obs_error_fixture()),
            ("observation-notice.json", obs_notice_fixture()),
            ("observation-crash.json", obs_crash_fixture()),
        ];
        let sch = schema("observation-record.schema.json");
        for (name, rec) in cases {
            let rendered = json::to_pretty(&rec.to_json()).unwrap();
            pin(name, &rendered);
            let parsed = ObservationRecord::from_json(&json::parse(&read(&fixture_path(name))).unwrap()).unwrap();
            assert_eq!(parsed, rec);
            assert_eq!(json::to_pretty(&parsed.to_json()).unwrap(), rendered);
            assert_eq!(ObservationRecord::from_jsonl(&rec.to_jsonl().unwrap()).unwrap(), rec);
            check_schema(&sch, &parsed.to_json(), name);
        }
        // Every ErrorResponse field code the protocol defines is present
        // in the error fixture.
        let WireMsg::ErrorResponse(f) = &obs_error_fixture().wire[0] else { panic!() };
        for c in "SVCMDHPpqWstcdnFLR".chars() {
            assert!(f.contains_key(&c), "error fixture lacks field {}", c);
        }
    }

    #[test]
    fn wire_msg_covers_every_code() {
        let msgs = vec![
            WireMsg::EmptyQueryResponse,
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::CloseComplete,
            WireMsg::NoData,
            WireMsg::PortalSuspended,
            WireMsg::CopyDone,
            WireMsg::ParameterDescription(vec![23, 25]),
            WireMsg::CopyInResponse { fmt: 0, col_fmts: vec![0, 0] },
            WireMsg::CopyOutResponse { fmt: 1, col_fmts: vec![1] },
            WireMsg::CopyData(Bytes(vec![b'a', b'\t', 0x00])),
            WireMsg::Authentication { kind: 10, data: Bytes::text("SCRAM-SHA-256\0\0") },
            WireMsg::NegotiateProtocolVersion { minor: 0, unknown_options: vec![Bytes::text("_pq_.foo")] },
            WireMsg::Raw { code: 'Q', data: Bytes(vec![1, 2, 3]) },
        ];
        for m in msgs {
            let v = m.to_json();
            assert_eq!(WireMsg::from_json(&v).unwrap(), m, "{:?}", v);
        }
    }

    // ---- Finding ----

    #[test]
    fn finding_fixture_banked_analyze_1() {
        let f = finding_fixture();
        let rendered = f.to_file().unwrap();
        pin("finding-analyze-1.json", &rendered);
        let parsed = Finding::from_file(&read(&fixture_path("finding-analyze-1.json"))).unwrap();
        assert_eq!(parsed, f);
        assert_eq!(parsed.to_file().unwrap(), rendered);
        check_schema(&schema("finding.schema.json"), &parsed.to_json(), "finding");
        assert_eq!(parsed.status, Status::Banked);
        assert_eq!(parsed.cell_id.len(), 64);
        for s in Status::ALL {
            assert_eq!(Status::parse(s.as_str()).unwrap(), *s);
        }
        for c in Class::ALL {
            assert_eq!(Class::parse(c.as_str()).unwrap(), *c);
        }
        assert!(Class::Leak.is_pending_ruling() && Class::AllocPolicy.is_pending_ruling());
        assert!(!Class::WrongMessage.is_pending_ruling());
    }

    // ---- Cell ----

    #[test]
    fn cell_fixture_and_id() {
        let c = cell_fixture();
        let rendered = c.to_file();
        pin("cell-base.json", &rendered);
        let parsed = Cell::from_file(&read(&fixture_path("cell-base.json"))).unwrap();
        assert_eq!(parsed, c);
        assert_eq!(parsed.to_file(), rendered);
        check_schema(&schema("cell.schema.json"), &parsed.to_json(), "cell");
        // Content-addressed: the id is a pure function of the canonical
        // form, key order on disk does not matter, any axis change does.
        assert_eq!(c.cell_id(), parsed.cell_id());
        assert_eq!(c.cell_id(), CELL_BASE_ID, "base cell id drifted: id={} canonical={}", c.cell_id(), c.canonical_json());
        let mut other = c.clone();
        other.conf_cmm = Some("debug1".into());
        assert_ne!(other.cell_id(), c.cell_id());
        assert!(!c.canonical_json().contains('\n'));
    }

    /// sha256 of `Cell::base().canonical_json()`; recomputed by the test,
    /// pinned here so an accidental axis/default change is loud.
    const CELL_BASE_ID: &str = "570fe9f2f4618c9e5b693d955cfe040e02dded5edb2309ddb18a4d7ea2d7b96e";

    // ---- RecipeHeader ----

    #[test]
    fn recipe_header_fixture() {
        let h = recipe_fixture();
        let rendered = format!("{}{}", h.render(), RECIPE_BODY);
        pin("recipe-header.sql", &rendered);
        let text = read(&fixture_path("recipe-header.sql"));
        let (parsed, body_start) = RecipeHeader::parse(&text).unwrap();
        assert_eq!(parsed, h);
        assert_eq!(&text[body_start..], RECIPE_BODY);
        assert_eq!(parsed.render(), h.render());
        check_schema(&schema("recipe-header.schema.json"), &parsed.to_json(), "recipe-header");
        assert_eq!(RecipeHeader::from_json(&parsed.to_json()).unwrap(), h);
        // Header ends at the first non-header line; missing keys are errors.
        assert!(RecipeHeader::parse("-- id: x\nselect 1;\n").unwrap_err().contains("missing required key"));
        assert!(RecipeHeader::parse(&format!("{}-- id: dup\n", h.render())).unwrap_err().contains("duplicate"));
        let (_, off) = RecipeHeader::parse(&format!("\n{}\n-- a plain comment\nselect 1;\n", h.render())).unwrap();
        assert_eq!(off, 1 + h.render().len() + 1);
    }

    // ---- Rulings ----

    #[test]
    fn rulings_fixture_matches_ruled_rs() {
        let r = rulings_fixture();
        let rendered = r.render();
        pin("rulings.toml", &rendered);
        let parsed = Rulings::parse(&read(&fixture_path("rulings.toml"))).unwrap();
        assert_eq!(parsed, r);
        assert_eq!(parsed.render(), rendered);
        check_schema(&schema("rulings.schema.json"), &parsed.to_json(), "rulings");
        // Every ruled.rs table entry has exactly one row, same order.
        let code_ids: Vec<&str> = crate::ruled::default_table().iter().map(|e| e.id).collect();
        let row_ids: Vec<&str> = parsed.rulings.iter().map(|x| x.id.as_str()).collect();
        assert_eq!(row_ids, code_ids);
        assert!(parsed.get("tie-ordering").is_some());
        // Nothing expires by default (only cosmetic/HINT rulings may).
        assert!(parsed.rulings.iter().all(|x| x.expires.is_none()));
    }

    #[test]
    fn toml_subset_errors_are_loud() {
        assert!(Rulings::parse("[[ruling]]\nid = \"x\"\n").unwrap_err().contains("missing required key"));
        assert!(Rulings::parse("[table]\n").unwrap_err().contains("not supported"));
        assert!(Rulings::parse("x = 1\n").unwrap_err().contains("top-level"));
        assert!(toml::parse("a = 1.5\n").is_err());
        assert!(toml::parse("a = \"unterminated\n").is_err());
        assert!(toml::parse("a = 1\na = 2\n").unwrap_err().contains("duplicate"));
        let d = toml::parse("# c\n[[r]]\nk = 'lit # not comment' # comment\nn = -4_2\nb = true\nl = [\"a\", 'b']\n").unwrap();
        assert_eq!(
            d.tables[0].1,
            vec![
                ("k".to_string(), Value::from("lit # not comment")),
                ("n".to_string(), Value::Int(-42)),
                ("b".to_string(), Value::Bool(true)),
                ("l".to_string(), Value::Arr(vec![Value::from("a"), Value::from("b")])),
            ]
        );
        assert_eq!(toml::quote("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
    }
}
