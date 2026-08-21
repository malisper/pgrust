//! [sqe-m4] SQL parser + lowerer for the hits workload's full 43-query
//! grammar. The parser produces the AUTHORING shape (planner::APlan) and
//! feeds plan_from_ap — ONE pipeline for SQL- and hand-lowered plans;
//! the FAMILY is elected over query SHAPE + bank stats; no query
//! identity anywhere (the M4 gate: no `qN` in this file). The ORACLE
//! consumes the parsed AST directly; shapes the lowering does not speak
//! return a typed "gap" refusal while still parsing.

use crate::bank::{Bank, ColMeta};
use crate::engine::SqeCtx;
use crate::family::{elect_family_entry, EntryFacts, ShapeFacts};
use crate::ir::PlanNode;
use crate::planner::{
    plan_from_ap, AAgg, ACmpOp, AFamily, AHaving, AKeyExpr, AOrderBy, AOrderSpec, APlan, APred,
    AValExpr,
};
use crate::stencils::col_width;

// ---- tokens ----

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Num(String),
    Str(String),
    Punct(char),
}

fn tokenize(s: &str) -> Result<Vec<Tok>, String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i] as char;
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == '_' {
            let st = i;
            while i < b.len() && ((b[i] as char).is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push(Tok::Ident(s[st..i].to_string()));
        } else if c.is_ascii_digit()
            || (c == '-' && i + 1 < b.len() && (b[i + 1] as char).is_ascii_digit())
        {
            let st = i;
            i += 1;
            while i < b.len() && (b[i] as char).is_ascii_digit() {
                i += 1;
            }
            out.push(Tok::Num(s[st..i].to_string()));
        } else if c == '\'' {
            i += 1;
            let st = i;
            while i < b.len() && b[i] != b'\'' {
                i += 1;
            }
            if i >= b.len() {
                return Err("unterminated string literal".into());
            }
            out.push(Tok::Str(s[st..i].to_string()));
            i += 1;
        } else if "(),*=<>;+-".contains(c) {
            out.push(Tok::Punct(c));
            i += 1;
        } else if c == '!' {
            out.push(Tok::Punct('!'));
            i += 1;
        } else {
            return Err(format!("unexpected char {c:?}"));
        }
    }
    Ok(out)
}

// ---- AST ----

/// Scalar expression (select items, group keys, aggregate inputs).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlExpr {
    Col(u32),
    Int(i64),
    Str(String),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Sub(Box<SqlExpr>, Box<SqlExpr>),
    Mul(Box<SqlExpr>, Box<SqlExpr>),
    ExtractMinute { col: u32 },
    TruncMinute { col: u32 },
    OctetLength { col: u32 },
    /// Evaluated by the PORTED catalog fn (adt_regexp).
    RegexpReplace { col: u32, pat: String, rep: String },
    Case { cond: Vec<SqlPred>, then: Box<SqlExpr>, els: Box<SqlExpr> },
}

impl SqlExpr {
    pub fn cols_into(&self, out: &mut Vec<u32>) {
        match self {
            SqlExpr::Col(c)
            | SqlExpr::ExtractMinute { col: c }
            | SqlExpr::TruncMinute { col: c }
            | SqlExpr::OctetLength { col: c }
            | SqlExpr::RegexpReplace { col: c, .. } => {
                if !out.contains(c) {
                    out.push(*c);
                }
            }
            SqlExpr::Int(_) | SqlExpr::Str(_) => {}
            SqlExpr::Add(a, b) | SqlExpr::Sub(a, b) | SqlExpr::Mul(a, b) => {
                a.cols_into(out);
                b.cols_into(out);
            }
            SqlExpr::Case { cond, then, els } => {
                for p in cond {
                    let c = p.col();
                    if !out.contains(&c) {
                        out.push(c);
                    }
                }
                then.cols_into(out);
                els.cols_into(out);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlAgg {
    CountStar,
    CountDistinct(SqlExpr),
    Sum(SqlExpr),
    Avg(SqlExpr),
    Min(SqlExpr),
    Max(SqlExpr),
}

impl SqlAgg {
    pub fn input(&self) -> Option<&SqlExpr> {
        match self {
            SqlAgg::CountStar => None,
            SqlAgg::CountDistinct(e)
            | SqlAgg::Sum(e)
            | SqlAgg::Avg(e)
            | SqlAgg::Min(e)
            | SqlAgg::Max(e) => Some(e),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SelExpr {
    Expr(SqlExpr),
    Agg(SqlAgg),
}

#[derive(Debug, Clone, PartialEq)]
pub enum COp {
    Eq,
    Ne,
    Ge,
    Le,
    Gt,
    Lt,
}

/// Literal with its ORIGINAL text (the canonical fingerprint constant).
#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Num(String),
    Str(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlPred {
    Cmp { col: u32, op: COp, val: Lit },
    /// `pattern` is the FULL LIKE pattern text (wildcards included);
    /// classification (`%x%` → Contains vs the general matcher) happens
    /// at lowering, never in the parser.
    Like { col: u32, pattern: String, not: bool },
    In { col: u32, vals: Vec<String> },
}

impl SqlPred {
    pub fn col(&self) -> u32 {
        match self {
            SqlPred::Cmp { col, .. } | SqlPred::Like { col, .. } | SqlPred::In { col, .. } => *col,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OKey {
    Col(u32),
    /// ORDER BY <agg>: SELECT-list index of the Agg item.
    AggRef(usize),
    /// ORDER BY a group-key expression: index into `group`.
    Key(usize),
}

#[derive(Debug)]
pub struct SqlQuery {
    pub select: Vec<(SelExpr, Option<String>)>,
    pub preds: Vec<SqlPred>,
    pub group: Vec<SqlExpr>,
    pub order: Vec<(OKey, bool)>, // (key, desc)
    pub limit: Option<usize>,
    pub offset: usize,
    pub having_count_gt: Option<u64>,
}

// ---- parser ----

struct P<'a> {
    t: &'a [Tok],
    i: usize,
    schema: &'a [ColMeta],
}

impl<'a> P<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        if t.is_some() {
            self.i += 1;
        }
        t
    }
    fn kw(&mut self, w: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s.eq_ignore_ascii_case(w) {
                self.i += 1;
                return true;
            }
        }
        false
    }
    fn expect_kw(&mut self, w: &str) -> Result<(), String> {
        if self.kw(w) {
            Ok(())
        } else {
            Err(format!("expected {w} at token {:?}", self.peek()))
        }
    }
    fn punct(&mut self, c: char) -> bool {
        if self.peek() == Some(&Tok::Punct(c)) {
            self.i += 1;
            return true;
        }
        false
    }
    fn expect_punct(&mut self, c: char) -> Result<(), String> {
        if self.punct(c) {
            Ok(())
        } else {
            Err(format!("expected {c:?} at token {:?}", self.peek()))
        }
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            t => Err(format!("expected identifier, got {t:?}")),
        }
    }
    fn attno(&mut self) -> Result<u32, String> {
        let name = self.ident()?;
        resolve_col(self.schema, &name)
    }
    fn lit(&mut self) -> Result<Lit, String> {
        match self.next() {
            Some(Tok::Num(n)) => Ok(Lit::Num(n)),
            Some(Tok::Str(s)) => Ok(Lit::Str(s)),
            t => Err(format!("expected literal, got {t:?}")),
        }
    }
}

fn resolve_col(schema: &[ColMeta], name: &str) -> Result<u32, String> {
    let lname = name.to_ascii_lowercase();
    schema
        .iter()
        .find(|c| c.name == lname)
        .map(|c| c.attno)
        .ok_or_else(|| format!("unknown column {name}"))
}

const AGG_FNS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

fn is_agg_call(p: &P) -> bool {
    matches!(p.peek(), Some(Tok::Ident(name))
        if AGG_FNS.iter().any(|f| name.eq_ignore_ascii_case(f))
            && p.t.get(p.i + 1) == Some(&Tok::Punct('(')))
}

fn parse_agg(p: &mut P, fname: &str) -> Result<SqlAgg, String> {
    p.expect_punct('(')?;
    let a = match fname.to_ascii_uppercase().as_str() {
        "COUNT" => {
            if p.punct('*') {
                SqlAgg::CountStar
            } else if p.kw("DISTINCT") {
                SqlAgg::CountDistinct(parse_expr(p)?)
            } else {
                return Err("COUNT(col) unsupported (only COUNT(*)/COUNT(DISTINCT c))".into());
            }
        }
        "SUM" => SqlAgg::Sum(parse_expr(p)?),
        "AVG" => SqlAgg::Avg(parse_expr(p)?),
        "MIN" => SqlAgg::Min(parse_expr(p)?),
        "MAX" => SqlAgg::Max(parse_expr(p)?),
        other => return Err(format!("unsupported function {other}")),
    };
    p.expect_punct(')')?;
    Ok(a)
}

/// Scalar expression: term (('+'|'-') term)*; term: primary ('*' primary)*.
fn parse_expr(p: &mut P) -> Result<SqlExpr, String> {
    let mut e = parse_term(p)?;
    loop {
        if p.punct('+') {
            e = SqlExpr::Add(Box::new(e), Box::new(parse_term(p)?));
        } else if p.punct('-') {
            e = SqlExpr::Sub(Box::new(e), Box::new(parse_term(p)?));
        } else {
            return Ok(e);
        }
    }
}

fn parse_term(p: &mut P) -> Result<SqlExpr, String> {
    let mut e = parse_primary(p)?;
    while p.punct('*') {
        e = SqlExpr::Mul(Box::new(e), Box::new(parse_primary(p)?));
    }
    Ok(e)
}

fn parse_primary(p: &mut P) -> Result<SqlExpr, String> {
    if p.punct('(') {
        let e = parse_expr(p)?;
        p.expect_punct(')')?;
        return Ok(e);
    }
    match p.peek().cloned() {
        Some(Tok::Num(n)) => {
            p.i += 1;
            Ok(SqlExpr::Int(n.parse().map_err(|_| format!("bad int literal {n}"))?))
        }
        Some(Tok::Str(s)) => {
            p.i += 1;
            Ok(SqlExpr::Str(s))
        }
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("CASE") => {
            p.i += 1;
            p.expect_kw("WHEN")?;
            let paren = p.punct('(');
            let mut cond = vec![parse_pred(p)?];
            while p.kw("AND") {
                cond.push(parse_pred(p)?);
            }
            if paren {
                p.expect_punct(')')?;
            }
            p.expect_kw("THEN")?;
            let then = parse_expr(p)?;
            p.expect_kw("ELSE")?;
            let els = parse_expr(p)?;
            p.expect_kw("END")?;
            Ok(SqlExpr::Case { cond, then: Box::new(then), els: Box::new(els) })
        }
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("EXTRACT") => {
            p.i += 1;
            p.expect_punct('(')?;
            let unit = p.ident()?;
            if !unit.eq_ignore_ascii_case("minute") {
                return Err(format!("EXTRACT unit {unit} unsupported (minute only)"));
            }
            p.expect_kw("FROM")?;
            let col = p.attno()?;
            p.expect_punct(')')?;
            Ok(SqlExpr::ExtractMinute { col })
        }
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("DATE_TRUNC") => {
            p.i += 1;
            p.expect_punct('(')?;
            let unit = match p.next() {
                Some(Tok::Str(s)) => s,
                t => return Err(format!("expected DATE_TRUNC unit string, got {t:?}")),
            };
            if !unit.eq_ignore_ascii_case("minute") {
                return Err(format!("DATE_TRUNC unit {unit} unsupported (minute only)"));
            }
            p.expect_punct(',')?;
            let col = p.attno()?;
            p.expect_punct(')')?;
            Ok(SqlExpr::TruncMinute { col })
        }
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("OCTET_LENGTH") => {
            p.i += 1;
            p.expect_punct('(')?;
            let col = p.attno()?;
            p.expect_punct(')')?;
            Ok(SqlExpr::OctetLength { col })
        }
        Some(Tok::Ident(name)) if name.eq_ignore_ascii_case("REGEXP_REPLACE") => {
            p.i += 1;
            p.expect_punct('(')?;
            let col = p.attno()?;
            p.expect_punct(',')?;
            let pat = match p.next() {
                Some(Tok::Str(s)) => s,
                t => return Err(format!("expected pattern string, got {t:?}")),
            };
            p.expect_punct(',')?;
            let rep = match p.next() {
                Some(Tok::Str(s)) => s,
                t => return Err(format!("expected replacement string, got {t:?}")),
            };
            p.expect_punct(')')?;
            Ok(SqlExpr::RegexpReplace { col, pat, rep })
        }
        Some(Tok::Ident(_)) => Ok(SqlExpr::Col(p.attno()?)),
        t => Err(format!("unsupported expression at {t:?}")),
    }
}

fn parse_pred(p: &mut P) -> Result<SqlPred, String> {
    let col = p.attno()?;
    if p.kw("NOT") {
        if p.kw("ILIKE") {
            return Err("ILIKE unsupported (C-collation byte matcher only)".into());
        }
        p.expect_kw("LIKE")?;
        let pattern = like_pattern(p)?;
        return Ok(SqlPred::Like { col, pattern, not: true });
    }
    if p.kw("ILIKE") {
        return Err("ILIKE unsupported (C-collation byte matcher only)".into());
    }
    if p.kw("LIKE") {
        let pattern = like_pattern(p)?;
        return Ok(SqlPred::Like { col, pattern, not: false });
    }
    if p.kw("IN") {
        p.expect_punct('(')?;
        let mut vals = Vec::new();
        loop {
            match p.lit()? {
                Lit::Num(n) => vals.push(n),
                Lit::Str(s) => vals.push(s),
            }
            if !p.punct(',') {
                break;
            }
        }
        p.expect_punct(')')?;
        return Ok(SqlPred::In { col, vals });
    }
    let op = if p.punct('=') {
        COp::Eq
    } else if p.punct('<') {
        if p.punct('>') {
            COp::Ne
        } else if p.punct('=') {
            COp::Le
        } else {
            COp::Lt
        }
    } else if p.punct('>') {
        if p.punct('=') {
            COp::Ge
        } else {
            COp::Gt
        }
    } else if p.punct('!') {
        p.expect_punct('=')?;
        COp::Ne
    } else {
        return Err(format!("expected comparison at {:?}", p.peek()));
    };
    let val = p.lit()?;
    Ok(SqlPred::Cmp { col, op, val })
}

/// [R6d] Any LIKE pattern parses; validity (trailing escape) is checked
/// here so the parser refuses with a readable cause, and again at
/// lowering (the engine's typed refusal).
fn like_pattern(p: &mut P) -> Result<String, String> {
    match p.next() {
        Some(Tok::Str(s)) => {
            crate::like::like_valid(s.as_bytes())
                .map_err(|e| format!("invalid LIKE pattern: {e}"))?;
            Ok(s)
        }
        t => Err(format!("expected LIKE pattern, got {t:?}")),
    }
}

pub fn parse_sql(bank: &Bank, text: &str) -> Result<SqlQuery, String> {
    parse_sql_schema(&bank.schema, text)
}

pub fn parse_sql_schema(schema: &[ColMeta], text: &str) -> Result<SqlQuery, String> {
    let toks = tokenize(text)?;
    let mut p = P { t: &toks, i: 0, schema };
    p.expect_kw("SELECT")?;
    let mut select: Vec<(SelExpr, Option<String>)> = Vec::new();
    if p.punct('*') {
        for c in schema {
            select.push((SelExpr::Expr(SqlExpr::Col(c.attno)), None));
        }
    } else {
        loop {
            let expr = if is_agg_call(&p) {
                let name = match p.next() {
                    Some(Tok::Ident(n)) => n,
                    _ => unreachable!(),
                };
                SelExpr::Agg(parse_agg(&mut p, &name)?)
            } else {
                SelExpr::Expr(parse_expr(&mut p)?)
            };
            let alias = if p.kw("AS") { Some(p.ident()?.to_ascii_lowercase()) } else { None };
            select.push((expr, alias));
            if !p.punct(',') {
                break;
            }
        }
    }
    p.expect_kw("FROM")?;
    let tbl = p.ident()?;
    if !tbl.eq_ignore_ascii_case("hits") {
        return Err(format!("unknown table {tbl}"));
    }
    let mut preds = Vec::new();
    if p.kw("WHERE") {
        loop {
            preds.push(parse_pred(&mut p)?);
            if !p.kw("AND") {
                break;
            }
        }
    }
    let by_alias = |select: &[(SelExpr, Option<String>)], name: &str| -> Option<usize> {
        let lname = name.to_ascii_lowercase();
        select.iter().position(|(_, al)| al.as_deref() == Some(&lname))
    };
    let mut group: Vec<SqlExpr> = Vec::new();
    if p.kw("GROUP") {
        p.expect_kw("BY")?;
        loop {
            let e = if let Some(Tok::Num(n)) = p.peek() {
                let idx: usize = n.parse().map_err(|_| "bad GROUP BY ordinal")?;
                let item = select
                    .get(idx.wrapping_sub(1))
                    .ok_or_else(|| format!("GROUP BY ordinal {idx} out of range"))?;
                p.i += 1;
                match &item.0 {
                    SelExpr::Expr(e) => e.clone(),
                    SelExpr::Agg(_) => return Err("GROUP BY ordinal names an aggregate".into()),
                }
            } else if let Some(Tok::Ident(name)) = p.peek().cloned() {
                if let Some(idx) = by_alias(&select, &name) {
                    p.i += 1;
                    match &select[idx].0 {
                        SelExpr::Expr(e) => e.clone(),
                        SelExpr::Agg(_) => return Err("GROUP BY alias names an aggregate".into()),
                    }
                } else {
                    parse_expr(&mut p)?
                }
            } else {
                parse_expr(&mut p)?
            };
            group.push(e);
            if !p.punct(',') {
                break;
            }
        }
    }
    let mut having_count_gt = None;
    if p.kw("HAVING") {
        p.expect_kw("COUNT")?;
        p.expect_punct('(')?;
        p.expect_punct('*')?;
        p.expect_punct(')')?;
        p.expect_punct('>')?;
        match p.next() {
            Some(Tok::Num(n)) => {
                having_count_gt = Some(n.parse().map_err(|_| "bad HAVING count")?)
            }
            t => return Err(format!("expected HAVING count, got {t:?}")),
        }
    }
    let mut order: Vec<(OKey, bool)> = Vec::new();
    if p.kw("ORDER") {
        p.expect_kw("BY")?;
        loop {
            let key = if is_agg_call(&p) {
                let name = match p.next() {
                    Some(Tok::Ident(n)) => n,
                    _ => unreachable!(),
                };
                let a = parse_agg(&mut p, &name)?;
                let idx = select
                    .iter()
                    .position(|(e, _)| matches!(e, SelExpr::Agg(x) if *x == a))
                    .ok_or("ORDER BY aggregate not in select list")?;
                OKey::AggRef(idx)
            } else if let Some(Tok::Ident(name)) = p.peek().cloned() {
                let is_expr_kw = ["DATE_TRUNC", "EXTRACT", "CASE", "OCTET_LENGTH",
                    "REGEXP_REPLACE"]
                    .iter()
                    .any(|k| name.eq_ignore_ascii_case(k));
                if let Some(idx) = by_alias(&select, &name) {
                    p.i += 1;
                    match &select[idx].0 {
                        SelExpr::Agg(_) => OKey::AggRef(idx),
                        SelExpr::Expr(SqlExpr::Col(c)) => OKey::Col(*c),
                        SelExpr::Expr(e) => {
                            let k = group
                                .iter()
                                .position(|g| g == e)
                                .ok_or("ORDER BY alias expr not a group key")?;
                            OKey::Key(k)
                        }
                    }
                } else if !is_expr_kw {
                    OKey::Col(p.attno()?)
                } else {
                    let e = parse_expr(&mut p)?;
                    let k = group
                        .iter()
                        .position(|g| g == &e)
                        .ok_or("ORDER BY expression not a group key")?;
                    OKey::Key(k)
                }
            } else {
                return Err(format!("unsupported order key at {:?}", p.peek()));
            };
            let desc = if p.kw("DESC") {
                true
            } else {
                p.kw("ASC");
                false
            };
            order.push((key, desc));
            if !p.punct(',') {
                break;
            }
        }
    }
    let mut limit = None;
    let mut offset = 0usize;
    if p.kw("LIMIT") {
        match p.next() {
            Some(Tok::Num(n)) => limit = Some(n.parse().map_err(|_| "bad LIMIT")?),
            t => return Err(format!("expected LIMIT count, got {t:?}")),
        }
    }
    if p.kw("OFFSET") {
        match p.next() {
            Some(Tok::Num(n)) => offset = n.parse().map_err(|_| "bad OFFSET")?,
            t => return Err(format!("expected OFFSET count, got {t:?}")),
        }
    }
    p.punct(';');
    if p.i != p.t.len() {
        return Err(format!("trailing tokens at {:?}", p.peek()));
    }
    Ok(SqlQuery { select, preds, group, order, limit, offset, having_count_gt })
}

// ---- lowering: SqlQuery -> APlan (authoring shape) -> plan_from_ap ----

fn lit_text(l: &Lit) -> String {
    match l {
        Lit::Num(n) => n.clone(),
        Lit::Str(s) => s.clone(),
    }
}

/// WHERE conjuncts -> authoring predicates; `>=`/`<=` pairs merge into
/// Range keeping the literals' ORIGINAL text (canonical constants).
fn lower_preds(preds: &[SqlPred]) -> Result<Vec<APred>, String> {
    let mut out: Vec<APred> = Vec::new();
    let mut pend_lo: Vec<(u32, String, usize)> = Vec::new();
    let mut pend_hi: Vec<(u32, String, usize)> = Vec::new();
    let mut slots: Vec<Option<APred>> = Vec::new();
    for p in preds {
        let slot = slots.len();
        match p {
            SqlPred::Cmp { col, op: COp::Eq, val } => {
                let t = lit_text(val);
                slots.push(Some(if t.is_empty() {
                    return Err("= '' unsupported".into());
                } else if t == "0" {
                    APred::EqZero { col: *col, fp: None }
                } else {
                    APred::Eq { col: *col, val: t, fp: None }
                }));
            }
            SqlPred::Cmp { col, op: COp::Ne, val } => {
                let t = lit_text(val);
                slots.push(Some(if matches!(val, Lit::Str(s) if s.is_empty()) {
                    APred::NeEmpty { col: *col, fp: None }
                } else if t == "0" {
                    APred::NeZero { col: *col, fp: None }
                } else {
                    return Err("<> non-zero constant unsupported (gap)".into());
                }));
            }
            SqlPred::Cmp { col, op: COp::Ge, val } => {
                pend_lo.push((*col, lit_text(val), slot));
                slots.push(None);
            }
            SqlPred::Cmp { col, op: COp::Le, val } => {
                pend_hi.push((*col, lit_text(val), slot));
                slots.push(None);
            }
            SqlPred::Cmp { .. } => {
                return Err("strict </> bounds unsupported (gap: only >=/<= pairs)".into())
            }
            SqlPred::Like { col, pattern, not } => {
                // [R6d] pattern-class normalization: the `%x%` class keeps
                // the shipped Contains lowering (and its fingerprints);
                // every other pattern rides the general entry-decidable
                // APred::Like.
                match crate::like::classify(pattern.as_bytes()) {
                    crate::like::LikeClass::Contains(inner) => {
                        let inner = APred::Contains {
                            col: *col,
                            needle: String::from_utf8_lossy(&inner).into_owned(),
                            fp: None,
                        };
                        slots.push(Some(if *not {
                            APred::Not { leg: Box::new(inner) }
                        } else {
                            inner
                        }));
                    }
                    crate::like::LikeClass::General => slots.push(Some(APred::Like {
                        col: *col,
                        pattern: pattern.clone(),
                        not: *not,
                        fp: None,
                    })),
                }
            }
            SqlPred::In { col, vals } => {
                slots.push(Some(APred::In { col: *col, vals: vals.clone(), fp: None }));
            }
        }
    }
    for (col, lo, slot) in &pend_lo {
        let hi = pend_hi
            .iter()
            .find(|(c, _, _)| c == col)
            .ok_or("single-sided range unsupported (gap)")?;
        slots[*slot] =
            Some(APred::Range { col: *col, lo: lo.clone(), hi: hi.1.clone(), fp: None });
    }
    for (col, _, _) in &pend_hi {
        if !pend_lo.iter().any(|(c, _, _)| c == col) {
            return Err("single-sided range unsupported (gap)".into());
        }
    }
    for s in slots {
        if let Some(pr) = s {
            out.push(pr);
        }
    }
    Ok(out)
}

fn lower_val_expr(e: &SqlExpr) -> Result<AValExpr, String> {
    match e {
        SqlExpr::Col(c) => Ok(AValExpr::Col(*c)),
        SqlExpr::OctetLength { col } => Ok(AValExpr::OctetLength { col: *col }),
        other => Err(format!("aggregate input {other:?} unsupported by lowering (gap)")),
    }
}

/// PG literal typing: int4 when the value fits, else int8 — the op
/// result-width driver for the arithmetic vocabulary.
fn lit_w(k: i64) -> u8 {
    if i32::try_from(k).is_ok() {
        4
    } else {
        8
    }
}

/// [P4-1] the closed fused-arithmetic recognition over SUM/AVG inputs:
/// `col ± k`, `a * b`, `a * (k - b)` (commuted forms included). `w`/`wi`
/// carry PG's op result widths (max of the operand widths — the int op
/// family's typing rule); the planner's no-overflow witness consumes
/// them. Anything else stays a lowering gap (typed refusal upstream).
fn arith_val_expr(bank: &Bank, e: &SqlExpr) -> Option<AValExpr> {
    let w2 = |wa: u8, wb: u8| wa.max(wb);
    let cw = |c: u32| col_width(bank, c);
    match e {
        SqlExpr::Add(x, y) => match (&**x, &**y) {
            (SqlExpr::Col(c), SqlExpr::Int(k)) | (SqlExpr::Int(k), SqlExpr::Col(c)) => {
                Some(AValExpr::AddK { col: *c, k: *k, w: w2(cw(*c), lit_w(*k)) })
            }
            _ => None,
        },
        SqlExpr::Sub(x, y) => match (&**x, &**y) {
            (SqlExpr::Col(c), SqlExpr::Int(k)) if *k != i64::MIN => {
                Some(AValExpr::AddK { col: *c, k: -*k, w: w2(cw(*c), lit_w(*k)) })
            }
            _ => None,
        },
        SqlExpr::Mul(x, y) => {
            let ksub = |e: &SqlExpr| match e {
                SqlExpr::Sub(k, b) => match (&**k, &**b) {
                    (SqlExpr::Int(k), SqlExpr::Col(b)) => Some((*k, *b)),
                    _ => None,
                },
                _ => None,
            };
            match (&**x, &**y) {
                (SqlExpr::Col(a), SqlExpr::Col(b)) => {
                    Some(AValExpr::MulCC { a: *a, b: *b, w: w2(cw(*a), cw(*b)) })
                }
                (SqlExpr::Col(a), inner) | (inner, SqlExpr::Col(a)) => {
                    let (k, b) = ksub(inner)?;
                    let wi = w2(lit_w(k), cw(b));
                    Some(AValExpr::MulKSub { a: *a, k, b, w: w2(cw(*a), wi), wi })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn lower_agg(bank: &Bank, a: &SqlAgg) -> Result<AAgg, String> {
    Ok(match a {
        SqlAgg::CountStar => AAgg::CountStar,
        SqlAgg::CountDistinct(e) => AAgg::CountDistinct { e: lower_val_expr(e)? },
        SqlAgg::Sum(e) => AAgg::Sum {
            e: lower_val_expr(e).or_else(|gap| arith_val_expr(bank, e).ok_or(gap))?,
        },
        SqlAgg::Avg(e) => AAgg::Avg {
            e: lower_val_expr(e).or_else(|gap| {
                // AVG admits the mul shapes only (AVG(col+k) has no
                // engine spelling yet — the planner refuses it typed).
                arith_val_expr(bank, e)
                    .filter(|v| matches!(v, AValExpr::MulCC { .. } | AValExpr::MulKSub { .. }))
                    .ok_or(gap)
            })?,
        },
        SqlAgg::Min(e) => {
            if let SqlExpr::Col(c) = e {
                if col_width(bank, *c) == 0 {
                    return Ok(AAgg::MinBytes { e: AValExpr::Col(*c) });
                }
            }
            AAgg::Min { e: lower_val_expr(e)? }
        }
        SqlAgg::Max(e) => AAgg::Max { e: lower_val_expr(e)? },
    })
}

/// AST + lowered predicates -> ShapeFacts (the elect_family currency)
/// + EntryFacts (the R6d general-arm currency).
struct Shape {
    facts: ShapeFacts,
    entry: EntryFacts,
    aggs: Vec<SqlAgg>,
    proj_cols: Vec<u32>,
}

fn shape_of(bank: &Bank, q: &SqlQuery, aps: &[APred], group_cols: &[u32]) -> Shape {
    let mut has_contains = false;
    let mut frame_eq = false;
    let mut frame_rng = false;
    let mut pred_cols = Vec::new();
    let mut ne_empty_cols = Vec::new();
    let mut entry_var_cols = Vec::new();
    for p in aps {
        match p {
            APred::Contains { col, .. } => {
                has_contains = true;
                pred_cols.push(*col);
            }
            // [R6d] general LIKE class: entry-decidable varlena conjunct
            // outside the Contains fact (lower_preds already normalized
            // `%x%` into Contains).
            APred::Like { col, .. } => {
                entry_var_cols.push(*col);
                pred_cols.push(*col);
            }
            APred::Not { leg } => match &**leg {
                APred::Contains { col, .. } => {
                    has_contains = true;
                    pred_cols.push(*col);
                }
                APred::Like { col, .. } => {
                    entry_var_cols.push(*col);
                    pred_cols.push(*col);
                }
                _ => {}
            },
            APred::Eq { col, val, .. } => {
                if val != "0" {
                    frame_eq = true;
                }
                pred_cols.push(*col);
            }
            APred::Range { col, .. } => {
                frame_rng = true;
                pred_cols.push(*col);
            }
            // [packedpred] mantissa word conjunct over a PackedNumeric
            // lane (seam-authored; SQL lowering here never mints it —
            // arm kept exhaustive like CmpBytes). eq/ne classify with
            // the word Eq/Ne facts, range with the frame-range fact.
            APred::Packed { col, op, .. } => {
                if op == "range" {
                    frame_rng = true;
                } else if op == "eq" {
                    frame_eq = true;
                }
                pred_cols.push(*col);
            }
            APred::NeEmpty { col, .. } => ne_empty_cols.push(*col),
            APred::NeZero { col, .. } | APred::EqZero { col, .. } => pred_cols.push(*col),
            APred::Ne { col, .. } => pred_cols.push(*col),
            APred::In { col, .. } => pred_cols.push(*col),
            // [type-vocab] byte-order compare: a varlena/fixed-face conjunct
            // (planner lowers it to a VarPredTerm — the Like class here);
            // SQL lowering never authors it, arm kept exhaustive for the
            // seam-authored vocabulary.
            APred::CmpBytes { col, .. } => {
                entry_var_cols.push(*col);
                pred_cols.push(*col);
            }
            // [sqe-bpchar] byte-equality IN: same entry-decidable class
            // as CmpBytes (seam-authored only; SQL lowering never mints it).
            APred::InBytes { col, .. } => {
                entry_var_cols.push(*col);
                pred_cols.push(*col);
            }
            // [tpch-expr] byte-prefix IN: same class (seam-authored only).
            APred::InPrefixBytes { col, .. } => {
                entry_var_cols.push(*col);
                pred_cols.push(*col);
            }
            // [colcmp] seam-authored join-side vocabulary only.
            APred::ColCmp { a, b, .. } => {
                pred_cols.push(*a);
                pred_cols.push(*b);
            }
            APred::And { .. } => {}
        }
    }
    let mut aggs = Vec::new();
    let mut proj_cols = Vec::new();
    for (e, _) in &q.select {
        match e {
            SelExpr::Agg(a) => aggs.push(a.clone()),
            SelExpr::Expr(SqlExpr::Col(c)) => proj_cols.push(*c),
            SelExpr::Expr(_) => {}
        }
    }
    // Under ANY predicate only count(*) is stats-answerable (C8 rule).
    let has_preds = !q.preds.is_empty();
    let stats_answerable = |a: &SqlAgg| match a {
        SqlAgg::CountStar => true,
        SqlAgg::Sum(SqlExpr::Col(c))
        | SqlAgg::Avg(SqlExpr::Col(c))
        | SqlAgg::Min(SqlExpr::Col(c))
        | SqlAgg::Max(SqlExpr::Col(c)) => !has_preds && col_width(bank, *c) > 0,
        // [P4-1] SUM(col ± k) rides the metadata answer algebra
        // (Σ(col+k) = Σcol + k·nonnull — both stats-plane facts).
        SqlAgg::Sum(e) => matches!(arith_val_expr(bank, e), Some(AValExpr::AddK { col, .. })
            if !has_preds && col_width(bank, col) > 0),
        _ => false,
    };
    let avglen_ne_fused = {
        let mut srcs = aggs.iter().filter_map(|a| match a {
            SqlAgg::Avg(SqlExpr::OctetLength { col }) => Some(*col),
            _ => None,
        });
        match (srcs.next(), srcs.next()) {
            (Some(x), None) => {
                aggs.iter().all(|a| {
                    matches!(a, SqlAgg::CountStar | SqlAgg::Avg(SqlExpr::OctetLength { .. }))
                }) && q.preds.len() == 1
                    && ne_empty_cols.as_slice() == [x]
            }
            _ => false,
        }
    };
    let facts = ShapeFacts {
        n_order_aggs: 0,
        // [aggqual] the rig's SQL vocabulary carries no FILTER or
        // general-distinct spellings.
        n_distinct_general: 0,
        has_agg_filters: false,
        has_contains,
        has_count_distinct: aggs.iter().any(|a| matches!(a, SqlAgg::CountDistinct(_))),
        n_count_distinct: aggs.iter().filter(|a| matches!(a, SqlAgg::CountDistinct(_))).count(),
        n_aggs: aggs.len(),
        aggs_all_stats_answerable: aggs.iter().all(stats_answerable),
        aggs_all_count_star: aggs.iter().all(|a| *a == SqlAgg::CountStar),
        proj_cols: proj_cols.clone(),
        pred_cols,
        ne_empty_cols,
        frame_pair: frame_eq && frame_rng,
        group: group_cols.to_vec(),
        // The rig's curated RON plans carry their family explicitly; its
        // SQL-lowered facts keep the pre-expr-key election byte-identical
        // (the seam derives key_exprs from real plan trees).
        key_exprs: Vec::new(),
        npreds: q.preds.len(),
        avglen_ne_fused,
        preds_all_zero_witness: q.preds.iter().all(|p| {
            matches!(p, SqlPred::Cmp { op: COp::Eq | COp::Ne, val: Lit::Num(n), .. } if n == "0")
        }),
        has_order: !q.order.is_empty(),
        n_order_keys: q.order.len(),
        has_limit: q.limit.is_some(),
    };
    // [R6d] the general-arm currency, derived alongside ShapeFacts.
    let entry = EntryFacts {
        entry_var_cols,
        // var-lane grouped consumer vocabulary witness:
        // COUNT(*) / COUNT(DISTINCT c) / MIN(text col) (→ MinBytes).
        aggs_all_var_grouped: aggs.iter().all(|a| match a {
            SqlAgg::CountStar | SqlAgg::CountDistinct(_) => true,
            SqlAgg::Min(SqlExpr::Col(c)) => col_width(bank, *c) == 0,
            _ => false,
        }),
        aggs_all_var_fold: aggs.iter().all(|a| match a {
            SqlAgg::CountStar => true,
            SqlAgg::Sum(SqlExpr::Col(c))
            | SqlAgg::Avg(SqlExpr::Col(c))
            | SqlAgg::Min(SqlExpr::Col(c))
            | SqlAgg::Max(SqlExpr::Col(c)) => {
                col_width(bank, *c) > 0
                    || matches!(bank.face(*c), crate::bank::Face::PackedNumeric { .. })
            }
            _ => false,
        }),
    };
    Shape { facts, entry, aggs, proj_cols }
}

/// Parse, elect the family (shape + bank stats), build the APlan, and
/// run the shared lowering pipeline.
pub fn lower(ctx: &SqeCtx, q: u32, text: &str, tag: &str) -> Result<PlanNode, String> {
    let bank = ctx.bank;
    let ast = parse_sql(bank, text)?;
    let aps = lower_preds(&ast.preds)?;
    // plain-column group keys only (derived-key lowering: gap list)
    let mut group_cols: Vec<u32> = Vec::new();
    for g in &ast.group {
        match g {
            SqlExpr::Col(c) => group_cols.push(*c),
            other => return Err(format!("GROUP BY expr {other:?} unsupported by lowering (gap)")),
        }
    }
    let shape = shape_of(bank, &ast, &aps, &group_cols);
    let family = elect_family_entry(bank, ctx.faces, &shape.facts, &shape.entry, tag)
        .map_err(|r| r.to_string())?;

    // group keys must be the leading select items in group order
    let leading: Vec<u32> = shape.proj_cols.clone();
    if !group_cols.is_empty() && leading != group_cols {
        return Err(format!(
            "select list keys {:?} must equal GROUP BY {:?} in order (gap)",
            leading, group_cols
        ));
    }

    let agg: Vec<AAgg> =
        shape.aggs.iter().map(|a| lower_agg(bank, a)).collect::<Result<_, _>>()?;

    let agg_slot = |i: usize| -> usize {
        ast.select[..i].iter().filter(|(e, _)| matches!(e, SelExpr::Agg(_))).count()
    };

    let order: Option<AOrderSpec> = if ast.order.is_empty() {
        None
    } else {
        let by = match ast.order.as_slice() {
            [(OKey::AggRef(i), true)] => match &ast.select[*i].0 {
                SelExpr::Agg(SqlAgg::CountStar) => AOrderBy::CountDesc,
                SelExpr::Agg(SqlAgg::CountDistinct(_)) => AOrderBy::CountDesc,
                SelExpr::Agg(_) => AOrderBy::AggDesc { idx: agg_slot(*i) as u32 },
                other => return Err(format!("ORDER BY {other:?} DESC unsupported (gap)")),
            },
            [(OKey::Col(c), false)] => AOrderBy::ColAsc { col: *c },
            [(OKey::Col(a), false), (OKey::Col(b), false)] => {
                AOrderBy::ColThenColAsc { a: *a, b: *b }
            }
            [(OKey::Key(0), false)] => AOrderBy::KeyAsc,
            other => return Err(format!("ORDER BY shape {other:?} unsupported (gap)")),
        };
        Some(AOrderSpec { by, limit: ast.limit.map(|l| l as u32), offset: ast.offset as u32 })
    };
    if ast.order.is_empty() && (ast.limit.is_some() || ast.offset != 0) {
        return Err("LIMIT without ORDER BY unsupported (gap)".into());
    }
    let having = ast
        .having_count_gt
        .map(|v| AHaving { agg: AAgg::CountStar, op: ACmpOp::Gt, val: v });

    // hot-path column list: projection, predicates, agg inputs, order
    // cols; ZoneOrderWalk contract: cols[0] is the emitted VALUE column.
    let mut cols: Vec<u32> = Vec::new();
    let push = |cols: &mut Vec<u32>, c: u32| {
        if !cols.contains(&c) {
            cols.push(c);
        }
    };
    let order_cols: Vec<u32> = match &order {
        Some(AOrderSpec { by: AOrderBy::ColAsc { col }, .. }) => vec![*col],
        Some(AOrderSpec { by: AOrderBy::ColThenColAsc { a, b }, .. }) => vec![*a, *b],
        _ => Vec::new(),
    };
    if family == AFamily::ZoneOrderWalk {
        for &c in &shape.proj_cols {
            if !order_cols.contains(&c) || shape.proj_cols.len() == 1 {
                push(&mut cols, c);
            }
        }
    } else {
        for &c in &shape.proj_cols {
            push(&mut cols, c);
        }
    }
    for p in &ast.preds {
        push(&mut cols, p.col());
    }
    for a in &shape.aggs {
        if let Some(e) = a.input() {
            let mut ac = Vec::new();
            e.cols_into(&mut ac);
            for c in ac {
                push(&mut cols, c);
            }
        }
    }
    for &c in &order_cols {
        push(&mut cols, c);
    }

    let pred = if aps.is_empty() {
        None
    } else if aps.len() == 1 {
        Some(aps.into_iter().next().unwrap())
    } else {
        Some(APred::And { legs: aps })
    };

    let ap = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        agg_filters: Vec::new(),
        q,
        family,
        tags: Vec::new(),
        cols,
        pred,
        group: group_cols.iter().map(|&c| AKeyExpr::Col(c)).collect(),
        agg,
        order,
        having,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: format!("sql-lowered [{tag}]"),
        flags: Vec::new(),
    };
    plan_from_ap(bank, ctx.faces, &ap).map_err(|r| format!("lowering refused: {r}"))
}

// ---- shipped SQL sources ----

/// Verbatim ClickBench SQL; line q+1 is query q (0-based).
const CLICKBENCH_SQL: &str = include_str!("../../rig/plans/queries-clickbench.sql");

/// The five NON-ClickBench probe queries.
const UNSEEN_SQL: &str = include_str!("../../rig/plans/unseen.sql");

pub fn clickbench_text(q: u32) -> Option<&'static str> {
    CLICKBENCH_SQL.lines().nth(q as usize).filter(|l| !l.trim().is_empty())
}

pub fn unseen_text(i: u32) -> Option<&'static str> {
    UNSEEN_SQL.lines().nth(i as usize).filter(|l| !l.trim().is_empty())
}

pub fn plan_sql(ctx: &SqeCtx, q: u32) -> Result<PlanNode, String> {
    let text = clickbench_text(q).ok_or("no such ClickBench query")?;
    lower(ctx, q, text, &format!("q={q}"))
}

/// Parse + plan for unseen query i (0-based); node.q = 100 + i.
pub fn plan_unseen(ctx: &SqeCtx, i: u32) -> Result<(SqlQuery, PlanNode), String> {
    let text = unseen_text(i).ok_or("no such unseen query")?;
    let ast = parse_sql(ctx.bank, text)?;
    let node = lower(ctx, 100 + i, text, &format!("u={i}"))?;
    Ok((ast, node))
}

// ---- structural plan diff (RON vs SQL) — the M4 gate instrument ----

fn fp_sets(n: &PlanNode) -> (Vec<String>, Vec<String>, Vec<String>) {
    match &n.pred {
        None => (Vec::new(), Vec::new(), Vec::new()),
        Some(p) => {
            let mut f: Vec<String> = p.frame().iter().map(|t| t.fp.to_string()).collect();
            let mut r: Vec<String> = p.residues().iter().map(|t| t.fp.to_string()).collect();
            let mut v: Vec<String> = p.var_terms.iter().map(|t| t.fp.to_string()).collect();
            f.sort();
            r.sort();
            v.sort();
            (f, r, v)
        }
    }
}

/// Structural plan diff; `cols` compares as a SET (order load-bearing
/// only for the walk families' cols[0], compared separately).
pub fn plan_diff(a: &PlanNode, b: &PlanNode) -> Vec<String> {
    let mut d = Vec::new();
    let mut chk = |name: &str, x: String, y: String| {
        if x != y {
            d.push(format!("{name}: ron={x} sql={y}"));
        }
    };
    chk("family", format!("{:?}", a.family), format!("{:?}", b.family));
    chk("group_cols", format!("{:?}", a.params.group_cols), format!("{:?}", b.params.group_cols));
    chk("key_exprs", format!("{:?}", a.params.key_exprs), format!("{:?}", b.params.key_exprs));
    chk("agg", format!("{:?}", a.agg), format!("{:?}", b.agg));
    chk("order", format!("{:?}", a.params.order), format!("{:?}", b.params.order));
    chk("offset", a.params.offset.to_string(), b.params.offset.to_string());
    chk("limit", a.params.limit.to_string(), b.params.limit.to_string());
    chk("flags", a.params.flags.to_string(), b.params.flags.to_string());
    {
        let mut x = a.params.ne_empty_cols.clone();
        let mut y = b.params.ne_empty_cols.clone();
        x.sort();
        y.sort();
        chk("ne_empty_cols", format!("{x:?}"), format!("{y:?}"));
    }
    chk(
        "having_min_count",
        a.params.having_min_count.to_string(),
        b.params.having_min_count.to_string(),
    );
    let (fa, ra, va) = fp_sets(a);
    let (fb, rb, vb) = fp_sets(b);
    chk("frame_fps", format!("{fa:?}"), format!("{fb:?}"));
    chk("residue_fps", format!("{ra:?}"), format!("{rb:?}"));
    chk("var_fps", format!("{va:?}"), format!("{vb:?}"));
    chk(
        "goal_fps",
        format!(
            "{:?}",
            a.params.goal.fingerprints.iter().map(|f| f.to_string()).collect::<Vec<_>>()
        ),
        format!(
            "{:?}",
            b.params.goal.fingerprints.iter().map(|f| f.to_string()).collect::<Vec<_>>()
        ),
    );
    chk(
        "claim_class",
        format!("{:?}", a.params.goal.claim_class),
        format!("{:?}", b.params.goal.claim_class),
    );
    {
        let mut x = a.cols.clone();
        let mut y = b.cols.clone();
        x.sort();
        y.sort();
        chk("cols_set", format!("{x:?}"), format!("{y:?}"));
    }
    if matches!(a.family, crate::ir::Family::ZoneOrderWalk) {
        chk("cols[0]", format!("{:?}", a.cols.first()), format!("{:?}", b.cols.first()));
    }
    chk("slot_bytes", a.params.slot_bytes.to_string(), b.params.slot_bytes.to_string());
    chk("l2_bytes", a.params.l2_bytes.to_string(), b.params.l2_bytes.to_string());
    d
}


#[cfg(test)]
mod tests {
    use super::*;

    const M4_SET: [u32; 10] = [1, 4, 7, 12, 19, 22, 25, 32, 37, 40];

    #[test]
    fn m4_clickbench_subset_parses() {
        let schema = crate::rig::hits_schema();
        for q in M4_SET {
            let text = clickbench_text(q).expect("query text");
            let ast = parse_sql_schema(&schema, text)
                .unwrap_or_else(|e| panic!("q{q} parse: {e}"));
            assert!(!ast.select.is_empty(), "q{q}");
            let aps = lower_preds(&ast.preds).unwrap_or_else(|e| panic!("q{q} preds: {e}"));
            let _ = aps;
        }
    }

    /// Parser-competence gate: EVERY ClickBench query parses.
    #[test]
    fn all_43_parse() {
        let schema = crate::rig::hits_schema();
        for q in 0..43u32 {
            let text = clickbench_text(q).expect("query text");
            parse_sql_schema(&schema, text).unwrap_or_else(|e| panic!("q{q} parse: {e}"));
        }
    }

    #[test]
    fn unseen_parse() {
        let schema = crate::rig::hits_schema();
        for i in 0..5u32 {
            let text = unseen_text(i).expect("unseen text");
            let ast = parse_sql_schema(&schema, text)
                .unwrap_or_else(|e| panic!("u{i} parse: {e}"));
            let aps = lower_preds(&ast.preds).unwrap_or_else(|e| panic!("u{i} preds: {e}"));
            let _ = aps;
        }
    }

    #[test]
    fn range_merge_and_fps() {
        let schema = crate::rig::hits_schema();
        let ast = parse_sql_schema(
            &schema,
            "SELECT COUNT(*) FROM hits WHERE EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND CounterID = 62;",
        )
        .unwrap();
        let aps = lower_preds(&ast.preds).unwrap();
        assert_eq!(aps.len(), 2);
        match &aps[0] {
            APred::Range { col, lo, hi, .. } => {
                assert_eq!(*col, 6);
                assert_eq!(lo, "2013-07-01");
                assert_eq!(hi, "2013-07-31");
            }
            other => panic!("expected Range, got {other:?}"),
        }
    }

    #[test]
    fn negative_in_list_tokenizes() {
        let schema = crate::rig::hits_schema();
        let ast = parse_sql_schema(
            &schema,
            "SELECT COUNT(*) FROM hits WHERE TraficSourceID IN (-1, 6);",
        )
        .unwrap();
        match &ast.preds[0] {
            SqlPred::In { col, vals } => {
                assert_eq!(*col, 38);
                assert_eq!(vals, &vec!["-1".to_string(), "6".to_string()]);
            }
            other => panic!("expected In, got {other:?}"),
        }
    }

    #[test]
    fn parser_gap_shapes() {
        let schema = crate::rig::hits_schema();
        // q18-class: extract minute + alias group
        let a = parse_sql_schema(
            &schema,
            "SELECT UserID, extract(minute FROM EventTime) AS m, COUNT(*) FROM hits GROUP BY UserID, m ORDER BY COUNT(*) DESC LIMIT 10;",
        )
        .unwrap();
        assert!(matches!(a.group[1], SqlExpr::ExtractMinute { .. }));
        // q23-class: SELECT *
        let a = parse_sql_schema(&schema, "SELECT * FROM hits ORDER BY EventTime LIMIT 10;")
            .unwrap();
        assert_eq!(a.select.len(), 105);
        // q29-class: + arithmetic in an aggregate input
        let a = parse_sql_schema(
            &schema,
            "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1) FROM hits;",
        )
        .unwrap();
        assert!(matches!(
            a.select[1].0,
            SelExpr::Agg(SqlAgg::Sum(SqlExpr::Add(_, _)))
        ));
        // q34-class: literal select item + GROUP BY ordinal
        let a = parse_sql_schema(
            &schema,
            "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;",
        )
        .unwrap();
        assert!(matches!(a.group[0], SqlExpr::Int(1)));
        // q35-class: - arithmetic keys
        let a = parse_sql_schema(
            &schema,
            "SELECT ClientIP, ClientIP - 1, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1 ORDER BY c DESC LIMIT 10;",
        )
        .unwrap();
        assert!(matches!(a.group[1], SqlExpr::Sub(_, _)));
        // q39-class: CASE WHEN conjunction key by alias
        let a = parse_sql_schema(
            &schema,
            "SELECT SearchEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, COUNT(*) AS PageViews FROM hits GROUP BY SearchEngineID, Src ORDER BY PageViews DESC LIMIT 10;",
        )
        .unwrap();
        assert!(matches!(a.group[1], SqlExpr::Case { .. }));
        // q42-class: DATE_TRUNC group + ORDER BY the same expression
        let a = parse_sql_schema(
            &schema,
            "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;",
        )
        .unwrap();
        assert!(matches!(a.order[0], (OKey::Key(0), false)));
        // q27-class: HAVING + octet_length + ORDER BY avg alias
        let a = parse_sql_schema(
            &schema,
            "SELECT CounterID, AVG(octet_length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
        )
        .unwrap();
        assert_eq!(a.having_count_gt, Some(100000));
        assert!(matches!(a.order[0], (OKey::AggRef(1), true)));
        // q17-class: LIMIT without ORDER BY parses
        let a = parse_sql_schema(
            &schema,
            "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID LIMIT 10;",
        )
        .unwrap();
        assert!(a.order.is_empty() && a.limit == Some(10));
        // q28-class: REGEXP_REPLACE key
        let a = parse_sql_schema(
            &schema,
            "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, COUNT(*) AS c FROM hits WHERE Referer <> '' GROUP BY k ORDER BY c DESC LIMIT 25;",
        )
        .unwrap();
        assert!(matches!(a.group[0], SqlExpr::RegexpReplace { .. }));
    }
}
