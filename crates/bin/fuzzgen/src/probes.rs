//! Probe decks (sitediff plan §4.4, lane L0.3): the catalog deck,
//! `invariants.sql`, the in-transaction `pg_locks` probe, the
//! per-statement auto-name (`conname`) probe, the exact-integer stats
//! deltas and the physical column probes.
//!
//! The SQL lives under `crates/bin/fuzzgen/probes/*.sql` (one file per
//! deck, `include_str!`'d) with a tiny marker grammar: `-- name: <key>`
//! starts a statement, `-- when: <flag>` renders it only when the flag is
//! set in the `RenderCtx`, `-- each: <list>` renders it once per element
//! of that list (sequences, columns, GUC names). Placeholders are
//! `{{table}}`, `{{pk}}`, `{{column}}`, `{{seq}}`, `{{guc}}`,
//! `{{class_oid_col}}`.
//!
//! `schedule` decides which decks fire after a step (plan §4.4: the
//! catalog deck after every DDL bracket, restart and stream end; conname
//! after every DDL statement; locks after lock-taking DDL inside an open
//! transaction, bound to the issuing session; stats deltas and physical
//! probes after DML brackets). `deck_result` folds the wire messages of
//! a deck into the `probes{deck: result}` JSON of the ObservationRecord,
//! rows sorted so the result is order-independent; `stats_delta` turns
//! two stats snapshots into integer deltas.
//!
//! No live server here: rendering and folding are pure and snapshot
//! tested; the runner executes the rendered statements.

use std::collections::BTreeMap;

use crate::contracts::json::Value;
use crate::contracts::{Bytes, WireMsg};

/// The decks this lane ships. `probe:<deck>` step kinds name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Deck {
    Catalog,
    Invariants,
    Locks,
    Conname,
    Stats,
    Physical,
}

impl Deck {
    pub const ALL: &'static [Deck] =
        &[Deck::Catalog, Deck::Invariants, Deck::Locks, Deck::Conname, Deck::Stats, Deck::Physical];

    pub fn name(self) -> &'static str {
        match self {
            Deck::Catalog => "catalog",
            Deck::Invariants => "invariants",
            Deck::Locks => "locks",
            Deck::Conname => "conname",
            Deck::Stats => "stats",
            Deck::Physical => "physical",
        }
    }

    pub fn parse(s: &str) -> Option<Deck> {
        Deck::ALL.iter().copied().find(|d| d.name() == s)
    }

    /// The deck's SQL resource.
    pub fn source(self) -> &'static str {
        match self {
            Deck::Catalog => include_str!("../probes/catalog-deck.sql"),
            Deck::Invariants => include_str!("../probes/invariants.sql"),
            Deck::Locks => include_str!("../probes/locks.sql"),
            Deck::Conname => include_str!("../probes/conname.sql"),
            Deck::Stats => include_str!("../probes/stats.sql"),
            Deck::Physical => include_str!("../probes/physical.sql"),
        }
    }

    /// True for decks that must run on the issuing session (in-txn
    /// locks, per-backend loaded modules through the catalog deck's
    /// `loaded_modules`, conname inside an open transaction).
    pub fn session_bound(self) -> bool {
        matches!(self, Deck::Locks | Deck::Conname | Deck::Catalog)
    }

    /// A self-oracle deck: no A/B compare, any row on B is a finding.
    pub fn self_oracle(self) -> bool {
        matches!(self, Deck::Invariants)
    }
}

/// One statement of a deck, as written in the resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeckStmt {
    pub key: String,
    pub when: Option<String>,
    pub each: Option<String>,
    pub sql: String,
}

/// Split a deck resource on its `-- name:` markers.
pub fn split_deck(src: &str) -> Vec<DeckStmt> {
    let mut out: Vec<DeckStmt> = Vec::new();
    let mut cur: Option<DeckStmt> = None;
    for line in src.lines() {
        let t = line.trim();
        if let Some(name) = t.strip_prefix("-- name:") {
            if let Some(c) = cur.take() {
                out.push(finish_stmt(c));
            }
            cur = Some(DeckStmt { key: name.trim().to_string(), when: None, each: None, sql: String::new() });
            continue;
        }
        let Some(c) = cur.as_mut() else { continue };
        if let Some(w) = t.strip_prefix("-- when:") {
            c.when = Some(w.trim().to_string());
        } else if let Some(e) = t.strip_prefix("-- each:") {
            c.each = Some(e.trim().to_string());
        } else if t.starts_with("--") || t.is_empty() {
            // comments and blank lines between statements
        } else {
            c.sql.push_str(line);
            c.sql.push('\n');
        }
    }
    if let Some(c) = cur.take() {
        out.push(finish_stmt(c));
    }
    out
}

fn finish_stmt(mut s: DeckStmt) -> DeckStmt {
    s.sql = s.sql.trim().to_string();
    s
}

/// What a deck renders against.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenderCtx {
    /// The DDL/DML target table (schema-qualified or not, as the stream
    /// spelled it) — `{{table}}`; enables `when: table`.
    pub table: Option<String>,
    /// Its pk column — `{{pk}}` (physical probes need a total order).
    pub pk: Option<String>,
    /// Its columns — `each: column`.
    pub columns: Vec<String>,
    /// Generated sequences — `each: seq`.
    pub sequences: Vec<String>,
    /// GUCs the recipe SET — `each: guc`; enables `when: gucs`.
    pub gucs: Vec<String>,
    /// binupgrade cell: raw OIDs in the class deck (`{{class_oid_col}}`).
    pub raw_oids: bool,
}

/// One rendered, executable probe statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeStmt {
    /// `<stmt key>` or `<stmt key>:<element>` for `each:` expansions.
    pub key: String,
    pub sql: String,
}

fn quote_lit(s: &str) -> String {
    s.replace('\'', "''")
}

fn subst(sql: &str, ctx: &RenderCtx, elem: Option<(&str, &str)>) -> String {
    let mut out = sql.to_string();
    if let Some(t) = &ctx.table {
        out = out.replace("{{table}}", t);
    }
    if let Some(pk) = &ctx.pk {
        out = out.replace("{{pk}}", pk);
    }
    out = out.replace("{{class_oid_col}}", if ctx.raw_oids { "c.oid, " } else { "" });
    if let Some((list, value)) = elem {
        let ph = match list {
            "seq" => "{{seq}}",
            "column" => "{{column}}",
            "guc" => "{{guc}}",
            _ => "",
        };
        if !ph.is_empty() {
            let v = if list == "column" { value.to_string() } else { quote_lit(value) };
            out = out.replace(ph, &v);
        }
    }
    out
}

fn flag_set(flag: &str, ctx: &RenderCtx) -> bool {
    match flag {
        "table" => ctx.table.is_some(),
        "gucs" => !ctx.gucs.is_empty(),
        "raw_oids" => ctx.raw_oids,
        "pk" => ctx.pk.is_some(),
        _ => false,
    }
}

/// Render a deck for a context. Statements whose `when:` flag is unset
/// are skipped; `each:` statements expand per element (none = skipped).
pub fn render(deck: Deck, ctx: &RenderCtx) -> Vec<ProbeStmt> {
    let mut out = Vec::new();
    for s in split_deck(deck.source()) {
        if let Some(w) = &s.when {
            if !flag_set(w, ctx) {
                continue;
            }
        }
        match s.each.as_deref() {
            None => out.push(ProbeStmt { key: s.key.clone(), sql: subst(&s.sql, ctx, None) }),
            Some(list) => {
                let items: &[String] = match list {
                    "seq" => &ctx.sequences,
                    "column" => &ctx.columns,
                    "guc" => &ctx.gucs,
                    _ => &[],
                };
                for item in items {
                    if list == "column" && s.sql.contains("{{pk}}") && ctx.pk.is_none() {
                        continue;
                    }
                    out.push(ProbeStmt {
                        key: format!("{}:{}", s.key, item),
                        sql: subst(&s.sql, ctx, Some((list, item))),
                    });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------

/// Why a probe round is being considered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// A DDL statement just applied (target = the table it addressed).
    DdlStatement { in_txn: bool, takes_lock: bool },
    /// A DDL bracket (transaction of DDL, or a standalone DDL) closed.
    DdlBracketEnd,
    /// A DML bracket closed (writes visible).
    DmlBracketEnd,
    /// A side recovered from a crash / an `env:restart` completed.
    Restart,
    /// The stream ended.
    StreamEnd,
    /// An explicit `probe:<deck>` step.
    Explicit(Deck),
}

/// The decks to run for a trigger, in run order (plan §4.4).
pub fn schedule(trigger: &Trigger) -> Vec<Deck> {
    match trigger {
        Trigger::DdlStatement { in_txn, takes_lock } => {
            let mut v = vec![Deck::Conname];
            if *in_txn && *takes_lock {
                v.push(Deck::Locks);
            }
            v
        }
        Trigger::DdlBracketEnd => vec![Deck::Catalog, Deck::Invariants],
        Trigger::DmlBracketEnd => vec![Deck::Stats, Deck::Physical],
        Trigger::Restart => vec![Deck::Catalog, Deck::Invariants, Deck::Stats],
        Trigger::StreamEnd => vec![Deck::Catalog, Deck::Invariants, Deck::Stats, Deck::Physical],
        Trigger::Explicit(d) => vec![*d],
    }
}

// ---------------------------------------------------------------------
// Statement classifiers (leading-keyword, dollar-quote unaware: the
// scheduler needs the statement class, not a parse)
// ---------------------------------------------------------------------

fn leading_words(sql: &str, n: usize) -> Vec<String> {
    let mut s = sql.trim_start();
    // Skip leading comments.
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split_once('\n').map(|x| x.1).unwrap_or("").trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.split_once("*/").map(|x| x.1).unwrap_or("").trim_start();
        } else {
            break;
        }
    }
    s.split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .filter(|w| !w.is_empty())
        .take(n)
        .map(|w| w.to_ascii_uppercase())
        .collect()
}

/// DDL: CREATE / ALTER / DROP / TRUNCATE / COMMENT / GRANT / REVOKE /
/// REINDEX / CLUSTER / REFRESH / SECURITY LABEL / IMPORT.
pub fn is_ddl(sql: &str) -> bool {
    let w = leading_words(sql, 2);
    matches!(
        w.first().map(String::as_str),
        Some("CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "COMMENT" | "GRANT" | "REVOKE" | "REINDEX" | "CLUSTER" | "REFRESH" | "SECURITY" | "IMPORT")
    )
}

/// DML: INSERT / UPDATE / DELETE / MERGE / COPY ... FROM / (WITH ... DML).
pub fn is_dml(sql: &str) -> bool {
    let w = leading_words(sql, 1);
    matches!(w.first().map(String::as_str), Some("INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY"))
}

/// Transaction control.
pub fn txn_control(sql: &str) -> Option<&'static str> {
    let w = leading_words(sql, 2);
    match (w.first().map(String::as_str), w.get(1).map(String::as_str)) {
        (Some("BEGIN"), _) | (Some("START"), Some("TRANSACTION")) => Some("begin"),
        (Some("COMMIT"), Some("PREPARED")) | (Some("ROLLBACK"), Some("PREPARED")) => Some("end"),
        (Some("COMMIT"), _) | (Some("END"), _) | (Some("ABORT"), _) => Some("end"),
        (Some("ROLLBACK"), Some("TO")) => None,
        (Some("ROLLBACK"), _) => Some("end"),
        (Some("PREPARE"), Some("TRANSACTION")) => Some("end"),
        _ => None,
    }
}

/// DDL that takes a relation lock the pg_locks probe can see.
pub fn takes_relation_lock(sql: &str) -> bool {
    let w = leading_words(sql, 3);
    match (w.first().map(String::as_str), w.get(1).map(String::as_str)) {
        (Some("ALTER"), Some("TABLE" | "INDEX" | "MATERIALIZED" | "VIEW" | "SEQUENCE")) => true,
        (Some("CREATE"), Some("INDEX" | "UNIQUE" | "TRIGGER" | "RULE" | "POLICY")) => true,
        (Some("DROP"), Some("TABLE" | "INDEX" | "VIEW" | "MATERIALIZED" | "SEQUENCE" | "TRIGGER" | "RULE" | "POLICY")) => true,
        (Some("TRUNCATE"), _) | (Some("REINDEX"), _) | (Some("CLUSTER"), _) | (Some("LOCK"), _) => true,
        (Some("CREATE"), Some("TABLE")) => true,
        (Some("REFRESH"), _) => true,
        _ => false,
    }
}

/// The table a DDL/DML statement addresses, for the conname / physical
/// probes: the identifier after the object keyword (ALTER TABLE [IF
/// EXISTS] [ONLY] t, CREATE [TEMP|UNLOGGED] TABLE [IF NOT EXISTS] t,
/// CREATE INDEX ... ON t, INSERT INTO t, UPDATE t, DELETE FROM t, COPY t,
/// TRUNCATE t, MERGE INTO t). None when the statement drops its target
/// (there is nothing left to probe) or is not table-addressed.
pub fn target_table(sql: &str) -> Option<String> {
    let w = leading_words(sql, 12);
    let ws: Vec<&str> = w.iter().map(String::as_str).collect();
    let skip = |i: usize, words: &[&str]| -> usize {
        let mut i = i;
        while i < ws.len() && words.contains(&ws[i]) {
            i += 1;
        }
        i
    };
    let ident_at = |i: usize| -> Option<String> {
        let raw = raw_word_at(sql, i)?;
        Some(raw.trim_end_matches(',').to_string())
    };
    match ws.first().copied() {
        Some("ALTER") if ws.get(1) == Some(&"TABLE") => {
            let i = skip(2, &["IF", "EXISTS", "ONLY"]);
            ident_at(i)
        }
        Some("CREATE") => {
            let i = skip(1, &["TEMP", "TEMPORARY", "UNLOGGED", "LOCAL", "GLOBAL"]);
            match ws.get(i).copied() {
                Some("TABLE") => {
                    let j = skip(i + 1, &["IF", "NOT", "EXISTS"]);
                    ident_at(j)
                }
                Some("UNIQUE") | Some("INDEX") => {
                    let on = ws.iter().position(|x| *x == "ON")?;
                    let j = skip(on + 1, &["ONLY"]);
                    ident_at(j)
                }
                Some("TRIGGER") | Some("RULE") | Some("POLICY") => {
                    let on = ws.iter().position(|x| *x == "ON")?;
                    ident_at(on + 1)
                }
                _ => None,
            }
        }
        Some("INSERT") if ws.get(1) == Some(&"INTO") => ident_at(2),
        Some("MERGE") if ws.get(1) == Some(&"INTO") => ident_at(2),
        Some("UPDATE") => {
            let i = skip(1, &["ONLY"]);
            ident_at(i)
        }
        Some("DELETE") if ws.get(1) == Some(&"FROM") => {
            let i = skip(2, &["ONLY"]);
            ident_at(i)
        }
        Some("TRUNCATE") => {
            let i = skip(1, &["TABLE", "ONLY"]);
            ident_at(i)
        }
        Some("COPY") => {
            if ws.get(1) == Some(&"BINARY") { ident_at(2) } else { ident_at(1) }
        }
        _ => None,
    }
}

/// The i-th whitespace/paren-delimited word of `sql` in its original
/// spelling (identifiers keep their case and quotes).
fn raw_word_at(sql: &str, i: usize) -> Option<&str> {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split_once('\n').map(|x| x.1).unwrap_or("").trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.split_once("*/").map(|x| x.1).unwrap_or("").trim_start();
        } else {
            break;
        }
    }
    let w = s
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .filter(|w| !w.is_empty())
        .nth(i)?;
    // A bare "(" split can leave a word like `t(a` handled above; also
    // reject keywords that mean "no identifier here".
    const NOT_IDENT: &[&str] = &["SELECT", "VALUES", "AS", "WITH", "ON", "FROM", "INTO", "SET", "USING", "TABLE"];
    if NOT_IDENT.iter().any(|k| w.eq_ignore_ascii_case(k)) || w.starts_with('(') {
        return None;
    }
    Some(w)
}

// ---------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------

/// Fold the wire messages of one executed probe statement into its JSON
/// result: `{"columns": [...], "rows": [[cell|null, ...], ...]}` with
/// rows sorted (the decks ORDER BY anyway; sorting makes the fold
/// order-independent under a NULLS/collation disagreement), or
/// `{"error": {"C": sqlstate, "M": message}}` when the server errored,
/// or `{"tag": "..."}` for a command result.
pub fn stmt_result(wire: &[WireMsg]) -> Value {
    let mut columns: Vec<Value> = Vec::new();
    let mut rows: Vec<Vec<Option<Bytes>>> = Vec::new();
    let mut tag: Option<Bytes> = None;
    for m in wire {
        match m {
            WireMsg::ErrorResponse(f) => {
                let mut e = Value::obj();
                if let Some(c) = f.get(&'C') {
                    e.set("C", c.to_json());
                }
                if let Some(msg) = f.get(&'M') {
                    e.set("M", msg.to_json());
                }
                return Value::obj().with("error", e);
            }
            WireMsg::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name.to_json()).collect();
            }
            WireMsg::DataRow(cells) => rows.push(cells.clone()),
            WireMsg::CommandComplete(t) => tag = Some(t.clone()),
            _ => {}
        }
    }
    if columns.is_empty() && rows.is_empty() {
        return Value::obj().with("tag", tag.map(|t| t.to_json()).unwrap_or(Value::Null));
    }
    rows.sort_by_key(|r| r.iter().map(|c| c.as_ref().map(|b| b.0.clone())).collect::<Vec<_>>());
    Value::obj().with("columns", Value::Arr(columns)).with(
        "rows",
        Value::Arr(
            rows.into_iter()
                .map(|r| Value::Arr(r.into_iter().map(|c| c.map(|b| b.to_json()).unwrap_or(Value::Null)).collect()))
                .collect(),
        ),
    )
}

/// A whole deck's result: statement key → `stmt_result`.
pub fn deck_result(results: &[(String, Vec<WireMsg>)]) -> Value {
    let mut m: BTreeMap<String, Value> = BTreeMap::new();
    for (k, wire) in results {
        m.insert(k.clone(), stmt_result(wire));
    }
    Value::Obj(m.into_iter().collect())
}

/// Rows of a self-oracle deck result (invariants): every row of every
/// statement, keyed by statement — non-empty means a finding on B.
pub fn self_oracle_rows(deck: &Value) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    if let Some(obj) = deck.as_obj() {
        for (k, v) in obj {
            if let Some(rows) = v.get("rows").and_then(|r| r.as_arr()) {
                for r in rows {
                    out.push((k.clone(), r.clone()));
                }
            }
            if v.get("error").is_some() {
                out.push((k.clone(), v.clone()));
            }
        }
    }
    out
}

fn cell_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Str(s) => s.parse().ok(),
        Value::Int(i) => Some(*i),
        _ => None,
    }
}

/// Integer deltas between two stats deck results: per statement, rows
/// keyed by their leading non-integer columns; integer columns are
/// subtracted (`cur - prev`), rows only in `cur` count from zero, rows
/// only in `prev` are reported with negative values. The `flush`
/// statement (a function call) is dropped.
pub fn stats_delta(prev: &Value, cur: &Value) -> Value {
    let mut out: Vec<(String, Value)> = Vec::new();
    let Some(cur_obj) = cur.as_obj() else { return Value::obj() };
    for (k, cv) in cur_obj {
        if k == "flush" {
            continue;
        }
        let pv = prev.get(k);
        let columns = cv.get("columns").cloned().unwrap_or(Value::Arr(Vec::new()));
        let key_of = |row: &Value| -> (Vec<String>, Vec<Option<i64>>) {
            let mut keys = Vec::new();
            let mut nums = Vec::new();
            if let Some(cells) = row.as_arr() {
                for c in cells {
                    match cell_i64(c) {
                        Some(i) => nums.push(Some(i)),
                        None => {
                            if nums.is_empty() {
                                keys.push(c.as_str().unwrap_or("").to_string());
                            } else {
                                nums.push(None);
                            }
                        }
                    }
                }
            }
            (keys, nums)
        };
        let mut table: BTreeMap<Vec<String>, (Vec<Option<i64>>, Vec<Option<i64>>)> = BTreeMap::new();
        if let Some(rows) = pv.and_then(|p| p.get("rows")).and_then(|r| r.as_arr()) {
            for r in rows {
                let (k, n) = key_of(r);
                table.entry(k).or_default().0 = n;
            }
        }
        if let Some(rows) = cv.get("rows").and_then(|r| r.as_arr()) {
            for r in rows {
                let (k, n) = key_of(r);
                table.entry(k).or_default().1 = n;
            }
        }
        let rows: Vec<Value> = table
            .into_iter()
            .map(|(keys, (p, c))| {
                let n = p.len().max(c.len());
                let mut cells: Vec<Value> = keys.iter().map(|k| Value::Str(k.clone())).collect();
                for i in 0..n {
                    let a = p.get(i).copied().flatten().unwrap_or(0);
                    let b = c.get(i).copied().flatten().unwrap_or(0);
                    cells.push(Value::Int(b - a));
                }
                Value::Arr(cells)
            })
            .collect();
        out.push((k.clone(), Value::obj().with("columns", columns).with("rows", Value::Arr(rows))));
    }
    Value::Obj(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{ColDesc, ErrFields};

    #[test]
    fn every_deck_splits_into_named_statements() {
        for d in Deck::ALL {
            let stmts = split_deck(d.source());
            assert!(!stmts.is_empty(), "{}", d.name());
            for s in &stmts {
                assert!(!s.key.is_empty());
                assert!(s.sql.ends_with(';'), "{}:{} must end with ';'", d.name(), s.key);
                assert!(!s.sql.contains("-- name:"));
            }
            let mut keys: Vec<&str> = stmts.iter().map(|s| s.key.as_str()).collect();
            let n = keys.len();
            keys.sort();
            keys.dedup();
            assert_eq!(keys.len(), n, "duplicate keys in {}", d.name());
            assert_eq!(Deck::parse(d.name()), Some(*d));
        }
    }

    #[test]
    fn catalog_deck_snapshot() {
        let keys: Vec<String> = render(Deck::Catalog, &RenderCtx::default()).into_iter().map(|s| s.key).collect();
        assert_eq!(
            keys,
            [
                "depend", "init_privs", "description_builtin", "description_user", "constraint", "class", "type",
                "proc", "subscription", "replication_origin_status", "event_trigger", "extension",
                "extension_members", "db_role_setting", "largeobject_metadata", "loaded_modules", "matviews",
            ]
        );
        // Every catalog statement is sorted or a single-row digest.
        for s in render(Deck::Catalog, &RenderCtx::default()) {
            assert!(
                s.sql.contains("ORDER BY") || s.key == "description_builtin",
                "{} is not totally ordered",
                s.key
            );
        }
        // No raw OIDs by default; the binupgrade cell adds c.oid to class.
        let class = render(Deck::Catalog, &RenderCtx::default()).into_iter().find(|s| s.key == "class").unwrap();
        assert!(class.sql.starts_with("SELECT n.nspname, c.relname"), "{}", class.sql);
        let raw = render(Deck::Catalog, &RenderCtx { raw_oids: true, ..Default::default() })
            .into_iter()
            .find(|s| s.key == "class")
            .unwrap();
        assert!(raw.sql.starts_with("SELECT c.oid, n.nspname"), "{}", raw.sql);
        // GUCs the recipe SET: one SHOW per GUC plus the file/settings decks.
        let ctx = RenderCtx { gucs: vec!["work_mem".into(), "search_path".into()], ..Default::default() };
        let r = render(Deck::Catalog, &ctx);
        let show: Vec<&ProbeStmt> = r.iter().filter(|s| s.key.starts_with("show_guc:")).collect();
        assert_eq!(show.len(), 2);
        assert_eq!(show[0].sql, "SELECT 'work_mem' AS name, current_setting('work_mem', true) AS value;");
        assert!(r.iter().any(|s| s.key == "file_settings"));
        assert!(r.iter().any(|s| s.key == "settings_source"));
    }

    #[test]
    fn invariants_render_per_sequence() {
        let ctx = RenderCtx { sequences: vec!["s1".into(), "public.s2".into()], ..Default::default() };
        let r = render(Deck::Invariants, &ctx);
        let seqs: Vec<&ProbeStmt> = r.iter().filter(|s| s.key.starts_with("sequence_monotone:")).collect();
        assert_eq!(seqs.len(), 2);
        assert_eq!(
            seqs[1].sql,
            "SELECT 'public.s2' AS seq\n WHERE NOT (nextval('public.s2') < nextval('public.s2'));"
        );
        assert_eq!(r.len(), 15 + 2);
        assert!(render(Deck::Invariants, &RenderCtx::default()).iter().all(|s| !s.key.starts_with("sequence_monotone")));
    }

    #[test]
    fn locks_probe_is_bound_to_the_issuing_backend() {
        let r = render(Deck::Locks, &RenderCtx::default());
        assert_eq!(r.len(), 2);
        for s in &r {
            assert!(s.sql.contains("l.pid = pg_backend_pid()"), "{}", s.key);
        }
        assert!(r[0].sql.contains("n.nspname NOT IN ('pg_catalog', 'information_schema')"));
        assert!(Deck::Locks.session_bound());
    }

    #[test]
    fn conname_and_physical_render_only_with_a_table() {
        assert!(render(Deck::Conname, &RenderCtx::default()).is_empty());
        let ctx = RenderCtx {
            table: Some("fz_hp_3".into()),
            pk: Some("pk".into()),
            columns: vec!["pk".into(), "b".into()],
            ..Default::default()
        };
        let c = render(Deck::Conname, &ctx);
        assert_eq!(c.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(), ["conname", "indexname", "attname"]);
        assert!(c[0].sql.contains("c.conrelid = 'fz_hp_3'::regclass"));
        let p = render(Deck::Physical, &ctx);
        assert_eq!(
            p.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(),
            ["column_shape:pk", "column_shape:b", "relation_size_bucket"]
        );
        assert_eq!(
            p[1].sql,
            "SELECT pk AS pk, 'b' AS col,\n       pg_column_size(b) AS size,\n       pg_column_compression(b) AS compression,\n       (pg_column_toast_chunk_id(b) IS NOT NULL) AS toasted\n  FROM fz_hp_3\n ORDER BY pk;"
        );
        // Without a pk there is no total order: column shapes are skipped.
        let nopk = RenderCtx { pk: None, ..ctx.clone() };
        assert_eq!(render(Deck::Physical, &nopk).len(), 1);
    }

    #[test]
    fn stats_deck_has_no_timings() {
        for s in render(Deck::Stats, &RenderCtx::default()) {
            let up = s.sql.to_ascii_uppercase();
            assert!(!up.contains("_TIME") && !up.contains("BLK_READ") && !up.contains("LAST_"), "{}", s.key);
        }
    }

    #[test]
    fn schedule_matches_the_plan() {
        assert_eq!(schedule(&Trigger::DdlBracketEnd), vec![Deck::Catalog, Deck::Invariants]);
        assert_eq!(schedule(&Trigger::Restart), vec![Deck::Catalog, Deck::Invariants, Deck::Stats]);
        assert_eq!(schedule(&Trigger::StreamEnd), vec![Deck::Catalog, Deck::Invariants, Deck::Stats, Deck::Physical]);
        assert_eq!(schedule(&Trigger::DdlStatement { in_txn: true, takes_lock: true }), vec![Deck::Conname, Deck::Locks]);
        assert_eq!(schedule(&Trigger::DdlStatement { in_txn: false, takes_lock: true }), vec![Deck::Conname]);
        assert_eq!(schedule(&Trigger::DmlBracketEnd), vec![Deck::Stats, Deck::Physical]);
        assert_eq!(schedule(&Trigger::Explicit(Deck::Locks)), vec![Deck::Locks]);
    }

    #[test]
    fn statement_classifiers() {
        assert!(is_ddl("CREATE TABLE t (a int);"));
        assert!(is_ddl("  -- c\n ALTER TABLE t ADD COLUMN b int"));
        assert!(!is_ddl("SELECT 1"));
        assert!(is_dml("INSERT INTO t VALUES (1)"));
        assert!(!is_dml("CREATE TABLE t (a int)"));
        assert_eq!(txn_control("BEGIN;"), Some("begin"));
        assert_eq!(txn_control("START TRANSACTION ISOLATION LEVEL SERIALIZABLE"), Some("begin"));
        assert_eq!(txn_control("COMMIT"), Some("end"));
        assert_eq!(txn_control("ROLLBACK TO SAVEPOINT s"), None);
        assert_eq!(txn_control("ROLLBACK"), Some("end"));
        assert_eq!(txn_control("SELECT 1"), None);
        assert!(takes_relation_lock("ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)"));
        assert!(takes_relation_lock("CREATE INDEX i ON t (a)"));
        assert!(!takes_relation_lock("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'"));
        assert_eq!(target_table("ALTER TABLE IF EXISTS ONLY fz_hp_3 ADD COLUMN x int"), Some("fz_hp_3".into()));
        assert_eq!(target_table("CREATE TEMP TABLE IF NOT EXISTS \"Weird\"(a int)"), Some("\"Weird\"".into()));
        assert_eq!(target_table("CREATE UNIQUE INDEX CONCURRENTLY i ON ONLY public.t (a)"), Some("public.t".into()));
        assert_eq!(target_table("INSERT INTO t (a) VALUES (1)"), Some("t".into()));
        assert_eq!(target_table("UPDATE ONLY t SET a = 1"), Some("t".into()));
        assert_eq!(target_table("DELETE FROM t WHERE a = 1"), Some("t".into()));
        assert_eq!(target_table("TRUNCATE TABLE t, u"), Some("t".into()));
        assert_eq!(target_table("COPY t FROM STDIN"), Some("t".into()));
        assert_eq!(target_table("MERGE INTO t USING u ON true WHEN MATCHED THEN DELETE"), Some("t".into()));
        assert_eq!(target_table("DROP TABLE t"), None);
        assert_eq!(target_table("CREATE TABLE AS SELECT 1"), None);
        assert_eq!(target_table("SELECT * FROM t"), None);
    }

    fn col(name: &str) -> ColDesc {
        ColDesc { name: Bytes::text(name), tableoid: 0, attnum: 0, typoid: 25, typlen: -1, typmod: -1, fmt: 0 }
    }

    fn row(cells: &[Option<&str>]) -> WireMsg {
        WireMsg::DataRow(cells.iter().map(|c| c.map(Bytes::text)).collect())
    }

    #[test]
    fn deck_result_sorts_rows_and_keeps_errors() {
        let ok = vec![
            WireMsg::RowDescription(vec![col("relname"), col("n")]),
            row(&[Some("t2"), Some("3")]),
            row(&[Some("t1"), None]),
            WireMsg::CommandComplete(Bytes::text("SELECT 2")),
            WireMsg::ReadyForQuery { status: 'I' },
        ];
        let mut ef = ErrFields::new();
        ef.insert('C', Bytes::text("42P01"));
        ef.insert('M', Bytes::text("relation \"x\" does not exist"));
        let err = vec![WireMsg::ErrorResponse(ef), WireMsg::ReadyForQuery { status: 'I' }];
        let cmd = vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }];
        let v = deck_result(&[("class".into(), ok), ("bad".into(), err), ("flush".into(), cmd)]);
        let s = crate::contracts::json::to_canonical(&v).unwrap();
        assert_eq!(
            s,
            r#"{"bad":{"error":{"C":"42P01","M":"relation \"x\" does not exist"}},"class":{"columns":["relname","n"],"rows":[["t1",null],["t2","3"]]},"flush":{"tag":"SELECT 1"}}"#
        );
        let rows = self_oracle_rows(&v);
        assert_eq!(rows.len(), 3, "two rows + one error");
    }

    #[test]
    fn stats_delta_subtracts_integers_per_key() {
        let prev = deck_result(&[
            ("user_tables".into(), vec![
                WireMsg::RowDescription(vec![col("schemaname"), col("relname"), col("n_tup_ins"), col("n_tup_del")]),
                row(&[Some("public"), Some("t"), Some("10"), Some("1")]),
                row(&[Some("public"), Some("gone"), Some("5"), Some("0")]),
            ]),
            ("flush".into(), vec![WireMsg::CommandComplete(Bytes::text("SELECT 1"))]),
        ]);
        let cur = deck_result(&[
            ("user_tables".into(), vec![
                WireMsg::RowDescription(vec![col("schemaname"), col("relname"), col("n_tup_ins"), col("n_tup_del")]),
                row(&[Some("public"), Some("t"), Some("13"), Some("1")]),
                row(&[Some("public"), Some("new"), Some("2"), Some("0")]),
            ]),
            ("flush".into(), vec![WireMsg::CommandComplete(Bytes::text("SELECT 1"))]),
        ]);
        let d = stats_delta(&prev, &cur);
        let s = crate::contracts::json::to_canonical(&d).unwrap();
        assert_eq!(
            s,
            r#"{"user_tables":{"columns":["schemaname","relname","n_tup_ins","n_tup_del"],"rows":[["public","gone",-5,0],["public","new",2,0],["public","t",3,0]]}}"#
        );
        assert_eq!(crate::contracts::json::to_canonical(&stats_delta(&Value::obj(), &Value::obj())).unwrap(), "{}");
    }
}
