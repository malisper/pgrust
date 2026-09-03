//! Canonicalization of one side's `ObservationRecord` into comparable
//! planes (plan §4.1, obsdiff §5 adopted). Exactly these masks and no
//! others:
//!
//! - `F` / `L` are dropped from every E/N tuple (the witness consumes them
//!   first); `R` stays and is compared under the ledger's `r-field` ruling.
//! - User OIDs (>= 16384) in RowDescription / ParameterDescription become
//!   `u:<typname>` (`u:?` when the side's type-name map does not know the
//!   oid). Digit runs in `M` / `D` / `H` are masked ONLY pairwise, at the
//!   same template position on both sides and only when both are in the
//!   user-OID range (`oid_pair`); an OID where C prints a name is a
//!   finding (catalog_namespace-2, PLPGSQL-F3).
//! - Paths under the side's libdir / pgdata become `<LIBDIR>/...` /
//!   `<PGDATA>/...`, keeping the file name. The DLSUFFIX is never
//!   rewritten: it is a per-host expectation (`CanonCtx::dlsuffix`), so
//!   `pg_trgm.dylib` vs `pg_trgm.so` and `<LIBDIR>/pg_trgm.dylib` vs
//!   `$libdir/pg_trgm` stay findings (dfmgr-5, dfmgr-9).
//! - pids (`PID n`, `process n`, `backend n`, the `[n]` prefix slot),
//!   LSNs (`X/XXXXXXXX`) and durations (`n.n ms`, `n.n s`, `n MB/s`,
//!   `h:mm:ss.frac`) are tokenized.
//! - `%m` / `%t` timestamps are masked to `<ts>` keeping the zone token,
//!   so `PDT` vs `GMT` is compared (ISO-OBS-1b, postgres-7).
//! - Float ulp / geometry tolerance is not a mask here: the comparator
//!   keeps it as compare semantics under the retained B1 rulings.
//! - Nothing here reads bytes as lossy text: cells stay bytes; text
//!   fields are rendered losslessly (`\x<hex>` when not UTF-8) so regexes
//!   can run on them.

use std::collections::BTreeMap;

use crate::contracts::{
    json, Bytes, ColDesc, Crash, ErrFields, Hang, LogLine, ObservationRecord, Panic, Side, WireMsg,
};

/// C's FirstNormalObjectId.
pub const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

/// Shared-library suffixes a path can carry; only the host's is expected.
pub const DLSUFFIXES: [&str; 3] = [".dylib", ".so", ".dll"];

/// The DLSUFFIX of the host this binary runs on.
pub fn host_dlsuffix() -> &'static str {
    if cfg!(target_os = "macos") {
        ".dylib"
    } else if cfg!(target_os = "windows") {
        ".dll"
    } else {
        ".so"
    }
}

/// E/N field codes in classification order: `C` first (wrong-sqlstate),
/// then the wrong-message group, then the position pair.
pub const ERR_FIELD_ORDER: &[char] = &['C', 'S', 'V', 'M', 'D', 'H', 'W', 'q', 's', 't', 'c', 'd', 'n', 'R', 'P', 'p'];

/// Fields dropped before comparison.
pub const DROPPED_FIELDS: &[char] = &['F', 'L'];

/// Per-side canonicalization inputs: where this side's libdir / pgdata
/// live, the host DLSUFFIX, and the user-range oid -> typname map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonCtx {
    pub libdir: Option<String>,
    pub pgdata: Option<String>,
    pub dlsuffix: String,
    pub type_names: BTreeMap<u32, String>,
}

impl Default for CanonCtx {
    fn default() -> CanonCtx {
        CanonCtx { libdir: None, pgdata: None, dlsuffix: host_dlsuffix().to_string(), type_names: BTreeMap::new() }
    }
}

/// Lossless text rendering of raw bytes: the UTF-8 text when it is one,
/// else `\x<hex>` (a pure function of the bytes, so equality is preserved).
pub fn bytes_text(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => {
            let mut s = String::with_capacity(2 + 2 * b.len());
            s.push_str("\\x");
            for x in b {
                s.push_str(&format!("{:02x}", x));
            }
            s
        }
    }
}

// ---------------------------------------------------------------------
// Text masks
// ---------------------------------------------------------------------

fn is_hex_upper(c: u8) -> bool {
    c.is_ascii_digit() || (b'A'..=b'F').contains(&c)
}

/// LSN `X/XXXXXXXX` (pg's `%X/%08X`): 1-8 hex digits, slash, exactly 8.
fn lsn_len(b: &[u8], i: usize) -> Option<usize> {
    if i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_') {
        return None;
    }
    let mut j = i;
    while j < b.len() && j - i < 8 && is_hex_upper(b[j]) {
        j += 1;
    }
    if j == i || j >= b.len() || b[j] != b'/' {
        return None;
    }
    let start2 = j + 1;
    let mut k = start2;
    while k < b.len() && k - start2 < 8 && is_hex_upper(b[k]) {
        k += 1;
    }
    if k - start2 != 8 {
        return None;
    }
    if k < b.len() && (b[k].is_ascii_alphanumeric() || b[k] == b'_') {
        return None;
    }
    Some(k - i)
}

/// `h:mm:ss[.frac]` session/elapsed time.
fn clock_len(b: &[u8], i: usize) -> Option<usize> {
    let mut j = i;
    while j < b.len() && b[j].is_ascii_digit() {
        j += 1;
    }
    if j == i || j + 6 > b.len() || b[j] != b':' {
        return None;
    }
    let ok = b[j + 1].is_ascii_digit()
        && b[j + 2].is_ascii_digit()
        && b[j + 3] == b':'
        && b[j + 4].is_ascii_digit()
        && b[j + 5].is_ascii_digit();
    if !ok {
        return None;
    }
    let mut k = j + 6;
    if k < b.len() && b[k] == b'.' {
        k += 1;
        while k < b.len() && b[k].is_ascii_digit() {
            k += 1;
        }
    }
    if k < b.len() && b[k].is_ascii_digit() {
        return None;
    }
    Some(k - i)
}

/// `<num> ms` | `<num.num> s` | `<num> MB/s` | `<num> kB/s`.
fn duration_len(b: &[u8], i: usize) -> Option<usize> {
    if i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'.' || b[i - 1] == b'_') {
        return None;
    }
    let mut j = i;
    while j < b.len() && b[j].is_ascii_digit() {
        j += 1;
    }
    if j == i {
        return None;
    }
    let mut frac = false;
    if j < b.len() && b[j] == b'.' {
        let mut k = j + 1;
        while k < b.len() && b[k].is_ascii_digit() {
            k += 1;
        }
        if k > j + 1 {
            frac = true;
            j = k;
        }
    }
    if j >= b.len() || b[j] != b' ' {
        return None;
    }
    let rest = &b[j + 1..];
    let unit_ok = |u: &[u8]| rest.starts_with(u) && !rest.get(u.len()).is_some_and(|c| c.is_ascii_alphanumeric());
    if unit_ok(b"ms") {
        return Some(j + 1 + 2 - i);
    }
    if unit_ok(b"MB/s") || unit_ok(b"kB/s") {
        return Some(j + 1 + 4 - i);
    }
    if frac && unit_ok(b"s") {
        return Some(j + 1 + 1 - i);
    }
    None
}

/// `PID n` | `pid n` | `process n` | `backend n` (the digits only).
fn pid_prefix_len(b: &[u8], i: usize) -> Option<usize> {
    for kw in [&b"PID "[..], b"pid ", b"process ", b"backend "] {
        if b[i..].starts_with(kw) {
            let left_ok = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            let after = i + kw.len();
            let mut j = after;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if left_ok && j > after && !b.get(j).is_some_and(|c| c.is_ascii_alphanumeric()) {
                return Some(kw.len());
            }
        }
    }
    None
}

/// Replace every occurrence of a directory `root` (trailing slash
/// trimmed) followed by `/` or end with `token`.
fn mask_root(s: &str, root: &str, token: &str) -> String {
    let root = root.trim_end_matches('/');
    if root.is_empty() || !s.contains(root) {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find(root) {
        let after = &rest[pos + root.len()..];
        let boundary = after.is_empty() || after.starts_with('/') || after.starts_with('"') || after.starts_with('\'');
        out.push_str(&rest[..pos]);
        if boundary {
            out.push_str(token);
        } else {
            out.push_str(root);
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// The §4.1 tokenization of free text: paths, pids, LSNs, durations.
/// Digit runs that might be OIDs are NOT touched here (see `oid_pair`).
pub fn mask_text(s: &str, ctx: &CanonCtx) -> String {
    let mut s = s.to_string();
    if let Some(p) = &ctx.pgdata {
        s = mask_root(&s, p, "<PGDATA>");
    }
    if let Some(l) = &ctx.libdir {
        s = mask_root(&s, l, "<LIBDIR>");
    }
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if let Some(n) = lsn_len(b, i) {
            out.push_str("<lsn>");
            i += n;
            continue;
        }
        if let Some(n) = clock_len(b, i) {
            out.push_str("<dur>");
            i += n;
            continue;
        }
        if let Some(n) = duration_len(b, i) {
            out.push_str("<dur>");
            i += n;
            continue;
        }
        if let Some(n) = pid_prefix_len(b, i) {
            out.push_str(&s[i..i + n]);
            i += n;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            out.push_str("<pid>");
            continue;
        }
        let c = s[i..].chars().next().unwrap();
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// Split into alternating non-digit text and digit runs.
fn digit_split(s: &str) -> (Vec<String>, Vec<String>) {
    let mut text = Vec::new();
    let mut digits = Vec::new();
    let mut cur = String::new();
    let mut run = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            run.push(c);
        } else {
            if !run.is_empty() {
                text.push(std::mem::take(&mut cur));
                digits.push(std::mem::take(&mut run));
            }
            cur.push(c);
        }
    }
    if !run.is_empty() {
        text.push(std::mem::take(&mut cur));
        digits.push(run);
        text.push(String::new());
    } else {
        text.push(cur);
    }
    (text, digits)
}

/// Pairwise user-OID mask for M/D/H: when both sides share the same
/// template (identical non-digit text, same number of digit runs), a
/// digit run is replaced by `<oid>` on both sides iff both values are
/// >= FirstNormalObjectId at that position. Any other digit stays.
pub fn oid_pair(a: &str, b: &str) -> (String, String) {
    let (ta, da) = digit_split(a);
    let (tb, db) = digit_split(b);
    if ta != tb || da.len() != db.len() {
        return (a.to_string(), b.to_string());
    }
    let user = |d: &str| d.parse::<u64>().is_ok_and(|v| v >= u64::from(FIRST_NORMAL_OBJECT_ID));
    let mut oa = String::with_capacity(a.len());
    let mut ob = String::with_capacity(b.len());
    for i in 0..da.len() {
        oa.push_str(&ta[i]);
        ob.push_str(&tb[i]);
        if user(&da[i]) && user(&db[i]) {
            oa.push_str("<oid>");
            ob.push_str("<oid>");
        } else {
            oa.push_str(&da[i]);
            ob.push_str(&db[i]);
        }
    }
    oa.push_str(&ta[da.len()]);
    ob.push_str(&tb[db.len()]);
    (oa, ob)
}

/// Signature template of a message: double-quoted spans become `"%s"`,
/// digit runs outside quotes become `%d` (`analyzing "%s"`).
pub fn message_template(m: &str) -> String {
    let mut out = String::with_capacity(m.len());
    let mut in_quote = false;
    let mut in_digits = false;
    for c in m.chars() {
        if in_quote {
            if c == '"' {
                out.push_str("%s\"");
                in_quote = false;
            }
            continue;
        }
        if c == '"' {
            in_quote = true;
            in_digits = false;
            out.push('"');
            continue;
        }
        if c.is_ascii_digit() {
            if !in_digits {
                out.push_str("%d");
                in_digits = true;
            }
            continue;
        }
        in_digits = false;
        out.push(c);
    }
    if in_quote {
        out.push_str("%s");
    }
    out
}

/// True when `s` names a shared object with a suffix other than the host's.
pub fn foreign_dlsuffix(s: &str, ctx: &CanonCtx) -> Option<&'static str> {
    let trimmed = s.trim_end_matches(['"', '\'', '.', ':', ')']);
    DLSUFFIXES.iter().copied().find(|suf| trimmed.ends_with(suf) && *suf != ctx.dlsuffix)
}

/// Mask a raw log line: `%m`/`%t` timestamp -> `<ts>` keeping the zone
/// token, `[pid]` -> `[<pid>]`, then the free-text masks.
pub fn mask_log_raw(raw: &str, ctx: &CanonCtx) -> String {
    let b = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < b.len() {
        if let Some(n) = timestamp_len(b, i) {
            // `<ts>` plus the zone token that follows (`PDT`, `GMT`, `+02`):
            // the digits are masked, the zone label is compared.
            out.push_str("<ts>");
            i += n;
            if let Some(z) = raw[i..].strip_prefix(' ') {
                let ze = z.find(|c: char| !(c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == ':')).unwrap_or(z.len());
                if ze > 0 {
                    out.push(' ');
                    out.push_str(&z[..ze]);
                    i += 1 + ze;
                }
            }
            continue;
        }
        if b[i] == b'[' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j < b.len() && b[j] == b']' {
                out.push_str("[<pid>]");
                i = j + 1;
                continue;
            }
        }
        let c = raw[i..].chars().next().unwrap();
        out.push(c);
        i += c.len_utf8();
    }
    mask_text(&out, ctx)
}

/// `YYYY-MM-DD HH:MM:SS[.frac]` (the digits; the zone is left to the caller).
fn timestamp_len(b: &[u8], i: usize) -> Option<usize> {
    if i + 19 > b.len() {
        return None;
    }
    let s = &b[i..i + 19];
    let d = |k: usize| s[k].is_ascii_digit();
    let ok = (0..4).all(d)
        && s[4] == b'-'
        && d(5)
        && d(6)
        && s[7] == b'-'
        && d(8)
        && d(9)
        && s[10] == b' '
        && d(11)
        && d(12)
        && s[13] == b':'
        && d(14)
        && d(15)
        && s[16] == b':'
        && d(17)
        && d(18);
    if !ok {
        return None;
    }
    let mut j = i + 19;
    if j < b.len() && b[j] == b'.' {
        j += 1;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
    }
    Some(j - i)
}

/// Zone token of a `%m`/`%t` text (`2026-09-02 08:25:09.562 PDT` -> `PDT`).
pub fn zone_of(ts: &str) -> Option<String> {
    let b = ts.as_bytes();
    let n = timestamp_len(b, 0)?;
    let rest = ts[n..].trim_start();
    let end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == ':')).unwrap_or(rest.len());
    let z = &rest[..end];
    (!z.is_empty()).then(|| z.to_string())
}

// ---------------------------------------------------------------------
// Canonical planes
// ---------------------------------------------------------------------

/// One E/N tuple after canonicalization (F/L dropped, text masks applied).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ErrTuple {
    pub fields: BTreeMap<char, String>,
}

impl ErrTuple {
    pub fn from_fields(f: &ErrFields, ctx: &CanonCtx) -> ErrTuple {
        let mut fields = BTreeMap::new();
        for (&code, raw) in f {
            if DROPPED_FIELDS.contains(&code) {
                continue;
            }
            let text = bytes_text(&raw.0);
            let text = match code {
                'M' | 'D' | 'H' | 'W' => mask_text(&text, ctx),
                _ => text,
            };
            fields.insert(code, text);
        }
        ErrTuple { fields }
    }

    pub fn get(&self, code: char) -> Option<&str> {
        self.fields.get(&code).map(String::as_str)
    }

    /// `<C>: <M>` — the composite the ledger's build-config rulings test.
    pub fn cm(&self) -> String {
        format!("{}: {}", self.get('C').unwrap_or(""), self.get('M').unwrap_or(""))
    }

    /// Ledger field accessor: a single field code or `CM`.
    pub fn value(&self, field: &str) -> Option<String> {
        if field == "CM" {
            return Some(self.cm());
        }
        let mut cs = field.chars();
        match (cs.next(), cs.next()) {
            (Some(c), None) => self.get(c).map(str::to_string),
            _ => None,
        }
    }

    /// The (a, b) values of one field with the pairwise OID mask on M/D/H.
    pub fn pair(a: &ErrTuple, b: &ErrTuple, code: char) -> (Option<String>, Option<String>) {
        match (a.get(code), b.get(code)) {
            (Some(x), Some(y)) if matches!(code, 'M' | 'D' | 'H') => {
                let (x, y) = oid_pair(x, y);
                (Some(x), Some(y))
            }
            (x, y) => (x.map(str::to_string), y.map(str::to_string)),
        }
    }
}

/// One RowDescription column, oid canonicalized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColMeta {
    pub name: String,
    /// Builtin: the oid as decimal text; user range: `u:<typname>` / `u:?`.
    pub typ: String,
    pub typoid: u32,
    pub typlen: i16,
    pub typmod: i32,
    pub fmt: i16,
}

/// Canonical type token for an oid.
pub fn canon_typ(oid: u32, ctx: &CanonCtx) -> String {
    if oid >= FIRST_NORMAL_OBJECT_ID {
        match ctx.type_names.get(&oid) {
            Some(n) => format!("u:{}", n),
            None => "u:?".to_string(),
        }
    } else {
        oid.to_string()
    }
}

impl ColMeta {
    fn from_desc(c: &ColDesc, ctx: &CanonCtx) -> ColMeta {
        ColMeta {
            name: bytes_text(&c.name.0),
            typ: canon_typ(c.typoid, ctx),
            typoid: c.typoid,
            typlen: c.typlen,
            typmod: c.typmod,
            fmt: c.fmt,
        }
    }

    pub fn render(&self) -> String {
        format!("{} {} len={} mod={} fmt={}", self.name, self.typ, self.typlen, self.typmod, self.fmt)
    }
}

/// One result group of a step: an optional RowDescription, its DataRows,
/// and the CommandComplete tag that closed it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ResultGroup {
    pub columns: Option<Vec<ColMeta>>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub tag: Option<String>,
}

impl ResultGroup {
    pub fn col_oids(&self) -> Vec<u32> {
        self.columns.as_ref().map(|c| c.iter().map(|x| x.typoid).collect()).unwrap_or_default()
    }

    /// Rows as text cells (lossless rendering) for the text-shaped masks.
    pub fn text_rows(&self) -> Vec<Vec<Option<String>>> {
        self.rows.iter().map(|r| r.iter().map(|c| c.as_ref().map(|b| bytes_text(b))).collect()).collect()
    }

    /// EXPLAIN output lines when this group is one text column.
    pub fn explain_lines(&self) -> Option<Vec<String>> {
        let cols = self.columns.as_ref()?;
        if cols.len() != 1 {
            return None;
        }
        self.rows
            .iter()
            .map(|r| match r.as_slice() {
                [Some(b)] => std::str::from_utf8(b).ok().map(str::to_string),
                _ => None,
            })
            .collect()
    }
}

/// One canonical server-log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogCanon {
    pub level: Option<String>,
    pub sqlstate: Option<String>,
    /// Zone token of `%m`/`%t`, compared; the timestamp digits are not.
    pub zone: Option<String>,
    pub backend_type: Option<String>,
    pub app: Option<String>,
    pub message: Option<String>,
    /// The raw line with every mask applied.
    pub raw: String,
}

impl LogCanon {
    fn from_line(l: &LogLine, ctx: &CanonCtx) -> LogCanon {
        LogCanon {
            level: l.level.clone(),
            sqlstate: l.sqlstate.clone(),
            zone: l.ts.as_deref().and_then(zone_of),
            backend_type: l.backend_type.clone(),
            app: l.app.clone(),
            message: l.message.as_ref().map(|m| mask_text(&bytes_text(&m.0), ctx)),
            raw: mask_log_raw(&bytes_text(&l.raw.0), ctx),
        }
    }

    pub fn value(&self, field: &str) -> Option<String> {
        match field {
            "level" => self.level.clone(),
            "sqlstate" => self.sqlstate.clone(),
            "zone" => self.zone.clone(),
            "backend_type" => self.backend_type.clone(),
            "app" => self.app.clone(),
            "message" => self.message.clone(),
            "raw" | "prefix" => Some(self.raw.clone()),
            _ => None,
        }
    }
}

/// Everything the comparator looks at for one side of one step.
#[derive(Clone, Debug, PartialEq)]
pub struct CanonSide {
    pub side: Side,
    /// The first ErrorResponse (a step has at most one that matters).
    pub error: Option<ErrTuple>,
    pub notices: Vec<ErrTuple>,
    pub groups: Vec<ResultGroup>,
    /// ParameterStatus name -> value.
    pub params: BTreeMap<String, String>,
    /// ParameterDescription type tokens, per `t` message.
    pub param_types: Vec<Vec<String>>,
    /// (channel, payload) -> count; pid masked by construction.
    pub notify: BTreeMap<(String, String), u32>,
    /// CopyData payload concatenated (COPY TO STDOUT).
    pub copy_out: Vec<u8>,
    pub copy_in_fmt: Option<(i8, Vec<i16>)>,
    pub copy_out_fmt: Option<(i8, Vec<i16>)>,
    /// ReadyForQuery statuses in order.
    pub ready: Vec<char>,
    /// Message-type sequence (every message, in order) for shape diffs.
    pub shape: String,
    pub log: Vec<LogCanon>,
    pub panic: Option<Panic>,
    pub crash: Option<Crash>,
    pub hang: Option<Hang>,
    pub liveness: String,
    /// Probe deck -> canonical JSON.
    pub probes: BTreeMap<String, String>,
}

/// Canonicalize one side's record.
pub fn canonicalize(rec: &ObservationRecord, ctx: &CanonCtx) -> CanonSide {
    let mut out = CanonSide {
        side: rec.side,
        error: None,
        notices: Vec::new(),
        groups: Vec::new(),
        params: BTreeMap::new(),
        param_types: Vec::new(),
        notify: BTreeMap::new(),
        copy_out: Vec::new(),
        copy_in_fmt: None,
        copy_out_fmt: None,
        ready: Vec::new(),
        shape: String::new(),
        log: Vec::new(),
        panic: rec.panic.clone(),
        crash: rec.crash.clone(),
        hang: rec.hang.clone(),
        liveness: rec.liveness.clone(),
        probes: BTreeMap::new(),
    };
    let mut open: Option<ResultGroup> = None;
    for m in &rec.wire {
        out.shape.push(m.code());
        match m {
            WireMsg::ErrorResponse(f) => {
                if out.error.is_none() {
                    out.error = Some(ErrTuple::from_fields(f, ctx));
                }
            }
            WireMsg::NoticeResponse(f) => out.notices.push(ErrTuple::from_fields(f, ctx)),
            WireMsg::RowDescription(cols) => {
                if let Some(g) = open.take() {
                    out.groups.push(g);
                }
                open = Some(ResultGroup {
                    columns: Some(cols.iter().map(|c| ColMeta::from_desc(c, ctx)).collect()),
                    rows: Vec::new(),
                    tag: None,
                });
            }
            WireMsg::DataRow(cells) => {
                let g = open.get_or_insert_with(ResultGroup::default);
                g.rows.push(cells.iter().map(|c| c.as_ref().map(|b| b.0.clone())).collect());
            }
            WireMsg::CommandComplete(tag) => {
                let mut g = open.take().unwrap_or_default();
                g.tag = Some(bytes_text(&tag.0));
                out.groups.push(g);
            }
            WireMsg::ParameterStatus { name, value } => {
                out.params.insert(bytes_text(&name.0), bytes_text(&value.0));
            }
            WireMsg::NotificationResponse { channel, payload, .. } => {
                *out.notify.entry((bytes_text(&channel.0), bytes_text(&payload.0))).or_insert(0) += 1;
            }
            WireMsg::ParameterDescription(oids) => {
                out.param_types.push(oids.iter().map(|&o| canon_typ(o, ctx)).collect());
            }
            WireMsg::CopyInResponse { fmt, col_fmts } => out.copy_in_fmt = Some((*fmt, col_fmts.clone())),
            WireMsg::CopyOutResponse { fmt, col_fmts } => out.copy_out_fmt = Some((*fmt, col_fmts.clone())),
            WireMsg::CopyData(d) => out.copy_out.extend_from_slice(&d.0),
            WireMsg::ReadyForQuery { status } => out.ready.push(*status),
            WireMsg::BackendKeyData { .. }
            | WireMsg::EmptyQueryResponse
            | WireMsg::ParseComplete
            | WireMsg::BindComplete
            | WireMsg::CloseComplete
            | WireMsg::NoData
            | WireMsg::PortalSuspended
            | WireMsg::CopyDone
            | WireMsg::Authentication { .. }
            | WireMsg::NegotiateProtocolVersion { .. }
            | WireMsg::Raw { .. } => {}
        }
    }
    if let Some(g) = open.take() {
        out.groups.push(g);
    }
    for l in &rec.log {
        // LOCATION lines are the A-side witness (log_error_verbosity=verbose), never compared.
        if l.level.as_deref() == Some("LOCATION") {
            continue;
        }
        out.log.push(LogCanon::from_line(l, ctx));
    }
    for (deck, v) in &rec.probes {
        out.probes.insert(deck.clone(), json::to_canonical(v).unwrap_or_else(|e| format!("<unrenderable: {}>", e)));
    }
    out
}

/// Convenience: text -> Bytes for builders and tests.
pub fn b(s: &str) -> Bytes {
    Bytes::text(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> CanonCtx {
        CanonCtx {
            libdir: Some("/home/dev/dev/pg186-install/lib/postgresql".to_string()),
            pgdata: Some("/tmp/u186-audit/dfmgr-verify/data/".to_string()),
            dlsuffix: ".dylib".to_string(),
            type_names: BTreeMap::from([(28290, "fz_udt".to_string())]),
        }
    }

    #[test]
    fn paths_keep_file_name_and_suffix() {
        let m = "could not find function \"no_such_symbol\" in file \"/home/dev/dev/pg186-install/lib/postgresql/pg_trgm.dylib\"";
        assert_eq!(
            mask_text(m, &ctx()),
            "could not find function \"no_such_symbol\" in file \"<LIBDIR>/pg_trgm.dylib\""
        );
        assert_eq!(mask_text("in \"/tmp/u186-audit/dfmgr-verify/data/base/5\"", &ctx()), "in \"<PGDATA>/base/5\"");
        // A prefix that is not at a path boundary is left alone.
        assert_eq!(mask_text("/tmp/u186-audit/dfmgr-verify/data2/x", &ctx()), "/tmp/u186-audit/dfmgr-verify/data2/x");
        // $libdir is the user's spelling, not a path: untouched.
        assert_eq!(mask_text("in file \"$libdir/pg_trgm\"", &ctx()), "in file \"$libdir/pg_trgm\"");
        assert_eq!(foreign_dlsuffix("pg_trgm.so", &ctx()), Some(".so"));
        assert_eq!(foreign_dlsuffix("pg_trgm.dylib", &ctx()), None);
    }

    #[test]
    fn pids_lsns_durations_tokenized() {
        let c = CanonCtx::default();
        assert_eq!(mask_text("terminating connection due to administrator command (PID 41233)", &c),
            "terminating connection due to administrator command (PID <pid>)");
        assert_eq!(mask_text("process 4121 acquired ShareLock on transaction 733", &c),
            "process <pid> acquired ShareLock on transaction 733");
        assert_eq!(mask_text("pg_backup_start returned 0/16000028", &c), "pg_backup_start returned <lsn>");
        assert_eq!(mask_text("session time: 0:00:00.001 user=postgres", &c), "session time: <dur> user=postgres");
        assert_eq!(mask_text("duration: 12.345 ms  statement: SELECT 1", &c), "duration: <dur>  statement: SELECT 1");
        assert_eq!(mask_text("avg read rate: 0.000 MB/s, elapsed: 0.00 s", &c), "avg read rate: <dur>, elapsed: <dur>");
        // Row counts are not durations and not pids.
        assert_eq!(mask_text("scanned 1 of 1 pages, containing 10 live rows", &c), "scanned 1 of 1 pages, containing 10 live rows");
        // A fraction is not an LSN (second half must be 8 hex digits).
        assert_eq!(mask_text("ratio 1/2", &c), "ratio 1/2");
    }

    #[test]
    fn oid_pair_masks_only_same_template_user_range() {
        // Same template, both user range: masked.
        assert_eq!(
            oid_pair("type with OID 16401 does not exist", "type with OID 16455 does not exist"),
            ("type with OID <oid> does not exist".to_string(), "type with OID <oid> does not exist".to_string())
        );
        // Builtin oid on one side: kept (a real divergence).
        let (a, b) = oid_pair("type with OID 23 does not exist", "type with OID 16455 does not exist");
        assert_ne!(a, b);
        // Different templates: nothing masked (catalog_namespace-2 shape).
        let (a, b) = oid_pair("improper qualified name (too many dotted names): a.b.c.d.+", "improper qualified name (too many dotted names): a.b.c.d");
        assert_eq!(a, "improper qualified name (too many dotted names): a.b.c.d.+");
        assert_eq!(b, "improper qualified name (too many dotted names): a.b.c.d");
        // Small counts at the same position stay compared.
        let (a, b) = oid_pair("10 live rows", "9 live rows");
        assert_eq!((a.as_str(), b.as_str()), ("10 live rows", "9 live rows"));
    }

    #[test]
    fn log_raw_keeps_zone_token() {
        let c = CanonCtx::default();
        let a = "2026-09-02 08:25:09.562 PDT [41233] LOG:  disconnection: session time: 0:00:00.001 user=postgres database=postgres host=[local]";
        let b = "2026-09-02 15:25:09.569 GMT [41240] LOG:  disconnection: session time: 0:00:00.002 user=postgres database=postgres host=[local]";
        let ma = mask_log_raw(a, &c);
        let mb = mask_log_raw(b, &c);
        assert_eq!(ma, "<ts> PDT [<pid>] LOG:  disconnection: session time: <dur> user=postgres database=postgres host=[local]");
        assert_eq!(mb, "<ts> GMT [<pid>] LOG:  disconnection: session time: <dur> user=postgres database=postgres host=[local]");
        assert_ne!(ma, mb, "the zone token survives the mask");
        assert_eq!(zone_of("2026-09-02 08:25:09.562 PDT").as_deref(), Some("PDT"));
        assert_eq!(zone_of("2026-09-02 08:25:09 +02"), Some("+02".to_string()));
        assert_eq!(zone_of("nope"), None);
    }

    #[test]
    fn message_templates() {
        assert_eq!(message_template("analyzing \"public.t\""), "analyzing \"%s\"");
        assert_eq!(message_template("relation \"t\" does not exist at character 15"), "relation \"%s\" does not exist at character %d");
        assert_eq!(message_template("no digits"), "no digits");
    }

    #[test]
    fn err_tuple_drops_f_l_and_canonicalizes() {
        let mut f = ErrFields::new();
        f.insert('S', b("ERROR"));
        f.insert('C', b("42883"));
        f.insert('M', b("could not find function \"x\" in file \"/home/dev/dev/pg186-install/lib/postgresql/pg_trgm.dylib\""));
        f.insert('F', b("dfmgr.c"));
        f.insert('L', b("131"));
        f.insert('R', b("lookup_external_function"));
        let t = ErrTuple::from_fields(&f, &ctx());
        assert!(t.get('F').is_none() && t.get('L').is_none());
        assert_eq!(t.get('R'), Some("lookup_external_function"));
        assert_eq!(t.get('M'), Some("could not find function \"x\" in file \"<LIBDIR>/pg_trgm.dylib\""));
        assert_eq!(t.value("CM").unwrap(), format!("42883: {}", t.get('M').unwrap()));
        assert_eq!(t.value("MM"), None);
    }

    #[test]
    fn typ_tokens_and_groups() {
        let c = ctx();
        assert_eq!(canon_typ(23, &c), "23");
        assert_eq!(canon_typ(28290, &c), "u:fz_udt");
        assert_eq!(canon_typ(28284, &c), "u:?");
        let rec = ObservationRecord {
            scenario: "s".into(),
            seq: 1,
            session: "s1".into(),
            side: Side::A,
            wire: vec![
                WireMsg::RowDescription(vec![ColDesc { name: b("a"), tableoid: 0, attnum: 0, typoid: 23, typlen: 4, typmod: -1, fmt: 0 }]),
                WireMsg::DataRow(vec![Some(b("1"))]),
                WireMsg::DataRow(vec![None]),
                WireMsg::CommandComplete(b("SELECT 2")),
                WireMsg::CommandComplete(b("BEGIN")),
                WireMsg::NotificationResponse { pid: 5, channel: b("ch"), payload: b("p") },
                WireMsg::NotificationResponse { pid: 6, channel: b("ch"), payload: b("p") },
                WireMsg::ReadyForQuery { status: 'I' },
            ],
            log: vec![],
            panic: None,
            crash: None,
            hang: None,
            probes: BTreeMap::new(),
            server: None,
            liveness: "ok".into(),
            ms: 0,
            version: "18.6".into(),
        };
        let cs = canonicalize(&rec, &c);
        assert_eq!(cs.groups.len(), 2);
        assert_eq!(cs.groups[0].rows.len(), 2);
        assert_eq!(cs.groups[0].tag.as_deref(), Some("SELECT 2"));
        assert_eq!(cs.groups[1].tag.as_deref(), Some("BEGIN"));
        assert!(cs.groups[1].columns.is_none());
        assert_eq!(cs.notify.get(&("ch".to_string(), "p".to_string())), Some(&2));
        assert_eq!(cs.shape, "TDDCCAAZ");
        assert_eq!(cs.groups[0].explain_lines(), None, "a NULL cell is not an EXPLAIN line");
    }
}
