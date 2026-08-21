//! The NAIVE ORACLE: a row-at-a-time scalar interpreter over the parsed
//! SQL AST, independent of the stencil/planner code paths (shared: the
//! format reader, the answer/render currency, and — deliberately — the
//! PORTED regexp_replace catalog fn as a differential vs the engine's
//! hand-rolled host walk). No elections, no parallelism, no caches.
//!
//! Laws (shared spec with the engine, independently implemented):
//! 3VL WHERE (NULL operand never passes, negative ops included); NULL is
//! its own group, sorting greatest; strict aggregates (NULL folds
//! nothing; SUM/AVG/MIN/MAX over zero non-null rows are NULL); unsigned
//! words zero-extend; float folds use PG float_cmp order; text/fixed
//! MIN/MAX fold by memcmp (C collation). Canonical order: grouped =
//! (ORDER BY's ACTUAL aggregate DESC, key tuple ASC with width-masked
//! unsigned int elements); projection top-k = (key ASC, row ordinal ASC).
//! Zero referenced columns answer from the bank's row total; projection
//! without ORDER BY collects matching rows in bank scan order.

use super::lower::{COp, Lit, OKey, SelExpr, SqlAgg, SqlExpr, SqlPred, SqlQuery};
use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::{Bank, Face};
use crate::scan::{open_cursor, varlena_payload, GranValid, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::{oids, TypMeta, COLLATION_C};
use std::collections::{BTreeMap, HashMap, HashSet};


/// PG float8_cmp "less than" (NaN greatest, NaNs equal).
fn f64_pg_lt(a: f64, b: f64) -> bool {
    if a.is_nan() {
        false
    } else {
        b.is_nan() || a < b
    }
}

/// Literal -> typed datum, decided by the COLUMN's type.
fn lit_i64(bank: &Bank, col: u32, l: &Lit) -> i64 {
    match l {
        Lit::Num(n) => n.parse().expect("int literal"),
        Lit::Str(s) => {
            assert_eq!(
                bank.typ(col).oid,
                crate::typmeta::oids::DATE,
                "string literal {s} on a non-date int column"
            );
            let b: Vec<&str> = s.split('-').collect();
            assert_eq!(b.len(), 3, "unsupported string literal {s} in int compare");
            crate::kernels_f6::pg_date(
                b[0].parse().unwrap(),
                b[1].parse().unwrap(),
                b[2].parse().unwrap(),
            )
        }
    }
}

/// Datum -> i64: signed sx, unsigned zero-extend, bool 0/1.
fn word_val(face: Face, w: u8, x: u64) -> i64 {
    match face {
        Face::UnsignedWord(_) => match w {
            1 => x as u8 as i64,
            2 => x as u16 as i64,
            4 => x as u32 as i64,
            _ => panic!("oracle: unsigned width-8 refused at lowering"),
        },
        Face::Bool => (x != 0) as i64,
        _ => sx(x, w),
    }
}

/// Granule-resident column accessors (valid None = AllValid).
struct GCols<'a> {
    ints: &'a [(u32, Vec<i64>)],
    floats: &'a [(u32, Vec<f64>)],
    fixed: &'a [(u32, u32, Vec<u64>)],
    texts: &'a [(u32, Vec<u64>)],
    valids: &'a [(u32, Option<Vec<bool>>)],
}

impl<'a> GCols<'a> {
    #[inline]
    fn int(&self, c: u32, r: usize) -> i64 {
        self.ints.iter().find(|(a, _)| *a == c).expect("int col").1[r]
    }
    #[inline]
    fn float(&self, c: u32, r: usize) -> f64 {
        self.floats.iter().find(|(a, _)| *a == c).expect("float col").1[r]
    }
    #[inline]
    fn fixed_bytes(&self, c: u32, r: usize) -> &[u8] {
        let (_, len, v) = self.fixed.iter().find(|(a, _, _)| *a == c).expect("fixed col");
        unsafe { std::slice::from_raw_parts(v[r] as *const u8, *len as usize) }
    }
    #[inline]
    fn text(&self, c: u32, r: usize) -> &[u8] {
        let v = &self.texts.iter().find(|(a, _)| *a == c).expect("text col").1;
        unsafe { varlena_payload(v[r]) }
    }
    #[inline]
    fn is_valid(&self, c: u32, r: usize) -> bool {
        match &self.valids.iter().find(|(a, _)| *a == c).expect("col validity").1 {
            None => true,
            Some(m) => m[r],
        }
    }
}

fn pred_pass(bank: &Bank, preds: &[SqlPred], g: &GCols, r: usize) -> bool {
    for p in preds {
        let col = p.col();
        // 3VL: NULL operand => conjunct fails, whatever the polarity.
        if !g.is_valid(col, r) {
            return false;
        }
        let ok = match p {
            SqlPred::Cmp { col, op, val } => {
                if col_width(bank, *col) == 0 {
                    let t = g.text(*col, r);
                    match (op, val) {
                        (COp::Ne, Lit::Str(s)) if s.is_empty() => !t.is_empty(),
                        (COp::Eq, Lit::Str(s)) => t == s.as_bytes(),
                        _ => panic!("oracle: unsupported text compare"),
                    }
                } else {
                    let v = g.int(*col, r);
                    let k = lit_i64(bank, *col, val);
                    match op {
                        COp::Eq => v == k,
                        COp::Ne => v != k,
                        COp::Ge => v >= k,
                        COp::Le => v <= k,
                        COp::Gt => v > k,
                        COp::Lt => v < k,
                    }
                }
            }
            SqlPred::Like { col, pattern, not } => {
                // [R6d] full LIKE semantics (the parser now carries the
                // whole pattern); like_match reduces to contains for the
                // `%x%` class, so pre-R6d oracle verdicts are unchanged.
                let c = crate::like::like_match(g.text(*col, r), pattern.as_bytes());
                if *not {
                    !c
                } else {
                    c
                }
            }
            SqlPred::In { col, vals } => {
                let v = g.int(*col, r);
                vals.iter().any(|s| s.parse::<i64>().map(|k| k == v).unwrap_or(false))
            }
        };
        if !ok {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// scalar expression evaluation (the oracle's own 3VL interpreter)
// ---------------------------------------------------------------------------

enum Val {
    I(i64),
    B(Vec<u8>),
    Null,
}

/// REGEXP_REPLACE via the ported catalog fn; context reset per call
/// (the RE cache lives in adt_regexp's own static context).
struct RegexEval {
    cx: mcx::MemoryContext,
}

impl RegexEval {
    fn new() -> RegexEval {
        use std::sync::Once;
        static ENC: Once = Once::new();
        ENC.call_once(|| {
            fn cfi_ok() -> types_error::PgResult<()> {
                Ok(())
            }
            regex_core::init_seams();
            adt_regexp::init_seams();
            postgres_seams::check_for_interrupts::set(cfi_ok);
            let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
        });
        RegexEval { cx: mcx::MemoryContext::new("rig-oracle-regexp") }
    }

    fn replace(&mut self, s: &[u8], pat: &[u8], rep: &[u8]) -> Vec<u8> {
        let out = adt_regexp::textregexreplace_noopt(
            self.cx.mcx(),
            s,
            pat,
            rep,
            types_core::C_COLLATION_OID,
        )
        .unwrap_or_else(|e| panic!("oracle regexp_replace: {e:?}"))
        .as_slice()
        .to_vec();
        self.cx.reset();
        out
    }
}

struct Ev<'b> {
    bank: &'b Bank,
    regex: Option<RegexEval>,
}

impl<'b> Ev<'b> {
    fn new(bank: &'b Bank) -> Ev<'b> {
        Ev { bank, regex: None }
    }

    fn eval(&mut self, e: &SqlExpr, g: &GCols, r: usize) -> Val {
        match e {
            SqlExpr::Col(c) => {
                if !g.is_valid(*c, r) {
                    return Val::Null;
                }
                match self.bank.face(*c) {
                    Face::Varlena => Val::B(g.text(*c, r).to_vec()),
                    Face::Fixed(_) => Val::B(g.fixed_bytes(*c, r).to_vec()),
                    Face::F32 | Face::F64 => panic!("oracle: float column in scalar expr"),
                    _ => Val::I(g.int(*c, r)),
                }
            }
            SqlExpr::Int(v) => Val::I(*v),
            SqlExpr::Str(s) => Val::B(s.as_bytes().to_vec()),
            SqlExpr::Add(a, b) => match (self.eval(a, g, r), self.eval(b, g, r)) {
                (Val::I(x), Val::I(y)) => Val::I(x.wrapping_add(y)),
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                _ => panic!("oracle: '+' over non-int operands"),
            },
            SqlExpr::Sub(a, b) => match (self.eval(a, g, r), self.eval(b, g, r)) {
                (Val::I(x), Val::I(y)) => Val::I(x.wrapping_sub(y)),
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                _ => panic!("oracle: '-' over non-int operands"),
            },
            SqlExpr::Mul(a, b) => match (self.eval(a, g, r), self.eval(b, g, r)) {
                (Val::I(x), Val::I(y)) => Val::I(x.wrapping_mul(y)),
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                _ => panic!("oracle: '*' over non-int operands"),
            },
            SqlExpr::ExtractMinute { col } => {
                if !g.is_valid(*col, r) {
                    return Val::Null;
                }
                let us = g.int(*col, r);
                Val::I(us.rem_euclid(3_600_000_000) / 60_000_000)
            }
            SqlExpr::TruncMinute { col } => {
                if !g.is_valid(*col, r) {
                    return Val::Null;
                }
                let us = g.int(*col, r);
                Val::I(us - us.rem_euclid(60_000_000))
            }
            SqlExpr::OctetLength { col } => {
                if !g.is_valid(*col, r) {
                    return Val::Null;
                }
                Val::I(g.text(*col, r).len() as i64)
            }
            SqlExpr::RegexpReplace { col, pat, rep } => {
                if !g.is_valid(*col, r) {
                    return Val::Null;
                }
                let s = g.text(*col, r).to_vec();
                let re = self.regex.get_or_insert_with(RegexEval::new);
                Val::B(re.replace(&s, pat.as_bytes(), rep.as_bytes()))
            }
            SqlExpr::Case { cond, then, els } => {
                if pred_pass(self.bank, cond, g, r) {
                    self.eval(then, g, r)
                } else {
                    self.eval(els, g, r)
                }
            }
        }
    }
}

/// Rendered TYPE of a key/projection expression (oid elects the form).
fn expr_ty(bank: &Bank, e: &SqlExpr) -> TypMeta {
    match e {
        SqlExpr::Col(c) | SqlExpr::TruncMinute { col: c } => bank.typ(*c),
        SqlExpr::ExtractMinute { .. } => TypMeta::INT8,
        SqlExpr::Int(_) => TypMeta::INT4,
        SqlExpr::OctetLength { .. } => TypMeta::INT4,
        SqlExpr::Str(_) => TypMeta::varlena(oids::TEXT, COLLATION_C),
        SqlExpr::RegexpReplace { col, .. } => bank.typ(*col),
        SqlExpr::Add(a, _) | SqlExpr::Sub(a, _) | SqlExpr::Mul(a, _) => expr_ty(bank, a),
        SqlExpr::Case { then, els, .. } => {
            if let SqlExpr::Col(c) = &**then {
                bank.typ(*c)
            } else if let SqlExpr::Col(c) = &**els {
                bank.typ(*c)
            } else {
                TypMeta::varlena(oids::TEXT, COLLATION_C)
            }
        }
    }
}

/// Whether the expression evaluates to bytes (text lane).
fn expr_is_bytes(bank: &Bank, e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Col(c) => matches!(bank.face(*c), Face::Varlena | Face::Fixed(_)),
        SqlExpr::Str(_) | SqlExpr::RegexpReplace { .. } | SqlExpr::Case { .. } => true,
        _ => false,
    }
}

/// Key-ASC image: plain columns width-masked unsigned (the packed-key
/// order); derived int keys full unsigned.
fn key_ord(bank: &Bank, e: &SqlExpr, v: i64) -> u64 {
    if let SqlExpr::Col(c) = e {
        let w = col_width(bank, *c);
        let mask = if w == 8 { u64::MAX } else { (1u64 << (8 * w)) - 1 };
        (v as u64) & mask
    } else {
        v as u64
    }
}

/// Key element; Int compares by `ord`, Null (last variant) sorts greatest.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum KElem {
    Int { ord: u64, val: i64 },
    Text(Vec<u8>),
    Null,
}

#[derive(Clone)]
enum TVal {
    I(i64),
    B(Vec<u8>),
    Null,
}

#[derive(Default)]
struct Acc {
    count: u64,
    sums: Vec<i128>,
    cnts: Vec<u64>,
    distinct: Vec<HashSet<Vec<u8>>>,
    imins: Vec<Option<i64>>,
    imaxs: Vec<Option<i64>>,
    fmins: Vec<Option<f64>>,
    fmaxs: Vec<Option<f64>>,
    bmins: Vec<Option<Vec<u8>>>,
    bmaxs: Vec<Option<Vec<u8>>>,
}

impl Acc {
    fn sized(n: usize) -> Acc {
        Acc {
            count: 0,
            sums: vec![0; n],
            cnts: vec![0; n],
            distinct: vec![HashSet::new(); n],
            imins: vec![None; n],
            imaxs: vec![None; n],
            fmins: vec![None; n],
            fmaxs: vec![None; n],
            bmins: vec![None; n],
            bmaxs: vec![None; n],
        }
    }
}

/// Windowed answer + full result + witness counts (the rig's law inputs).
pub struct OracleOut {
    pub ans: AnswerSet,
    pub full: Option<AnswerSet>,
    pub order_field: Option<usize>,
    pub order_desc: bool,
    pub groups_total: Option<u64>,
    pub rows_folded: Option<u64>,
    pub groups_prehaving: Option<u64>,
    pub rows_prehaving: Option<u64>,
    pub matches: u64,
}

pub fn run_oracle(bank: &Bank, q: &SqlQuery) -> AnswerSet {
    run_oracle_full(bank, q).ans
}

pub fn run_oracle_full(bank: &Bank, q: &SqlQuery) -> OracleOut {
    let mut cols: Vec<u32> = Vec::new();
    for (e, _) in &q.select {
        match e {
            SelExpr::Expr(x) => x.cols_into(&mut cols),
            SelExpr::Agg(a) => {
                if let Some(x) = a.input() {
                    x.cols_into(&mut cols);
                }
            }
        }
    }
    for p in &q.preds {
        let c = p.col();
        if !cols.contains(&c) {
            cols.push(c);
        }
    }
    for g in &q.group {
        g.cols_into(&mut cols);
    }
    for (k, _) in &q.order {
        if let OKey::Col(c) = k {
            if !cols.contains(c) {
                cols.push(*c);
            }
        }
    }

    let aggs: Vec<SqlAgg> = q
        .select
        .iter()
        .filter_map(|(e, _)| match e {
            SelExpr::Agg(a) => Some(a.clone()),
            _ => None,
        })
        .collect();
    let grouped = !q.group.is_empty();
    let topk_proj = aggs.is_empty() && !q.order.is_empty() && !grouped;
    let rowfetch = aggs.is_empty() && q.order.is_empty() && !grouped;

    if cols.is_empty() {
        assert!(q.preds.is_empty() && !grouped && !topk_proj && !rowfetch);
        let n = bank.rows_total();
        let mut scalar = Acc::sized(aggs.len());
        scalar.count = n;
        assert!(
            aggs.iter().all(|a| matches!(a, SqlAgg::CountStar)),
            "oracle: zero-column scan admits COUNT(*) only"
        );
        return OracleOut {
            ans: agg_answer(bank, &aggs, std::iter::once(&scalar)),
            full: None,
            order_field: None,
            order_desc: false,
            groups_total: None,
            rows_folded: None,
            groups_prehaving: None,
            rows_prehaving: None,
            matches: n,
        };
    }

    let mut ev = Ev::new(bank);
    let mut groups: HashMap<Vec<KElem>, Acc> = HashMap::new();
    let mut scalar = Acc::sized(aggs.len());
    let mut matches: u64 = 0;

    let order_cols: Vec<u32> = if topk_proj {
        q.order
            .iter()
            .map(|(k, _)| match k {
                OKey::Col(c) => *c,
                other => panic!("oracle: order key {other:?} on a projection query"),
            })
            .collect()
    } else {
        Vec::new()
    };
    let k_lim = q.offset + q.limit.unwrap_or(usize::MAX / 2);
    let mut top: BTreeMap<(Vec<KElem>, u64), Vec<TVal>> = BTreeMap::new();
    let mut fetched: Vec<Vec<TVal>> = Vec::new();

    let faces: Vec<Face> = cols.iter().map(|&a| bank.face(a)).collect();
    let mut base: u64 = 0;
    let mut scr: Vec<Scratch> = cols.iter().map(|_| Scratch::new()).collect();
    for pi in 0..bank.parts.len() {
        let mut curs: Vec<_> = cols.iter().map(|&a| open_cursor(bank, pi, a)).collect();
        let ng = curs.first().map(|c| c.granule_count()).unwrap_or(0);
        for g in 0..ng {
            let rows = curs[0].rows_in_granule(g) as usize;
            let mut ints: Vec<(u32, Vec<i64>)> = Vec::new();
            let mut floats: Vec<(u32, Vec<f64>)> = Vec::new();
            let mut fixed: Vec<(u32, u32, Vec<u64>)> = Vec::new();
            let mut texts: Vec<(u32, Vec<u64>)> = Vec::new();
            let mut valids: Vec<(u32, Option<Vec<bool>>)> = Vec::new();
            for (ci, &a) in cols.iter().enumerate() {
                let gv = scr[ci].validity(&mut curs[ci], g, rows);
                let d = scr[ci].decode_full(&mut curs[ci], g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                match faces[ci] {
                    Face::Varlena => texts.push((a, d.to_vec())),
                    Face::F32 => floats.push((
                        a,
                        d.iter().map(|&x| f32::from_bits(x as u32) as f64).collect(),
                    )),
                    Face::F64 => floats.push((a, d.iter().map(|&x| f64::from_bits(x)).collect())),
                    Face::Fixed(len) => fixed.push((a, len, d.to_vec())),
                    face => {
                        let w = col_width(bank, a);
                        ints.push((a, d.iter().map(|&x| word_val(face, w, x)).collect()));
                    }
                }
                valids.push((
                    a,
                    match gv {
                        GranValid::AllValid => None,
                        GranValid::Mixed { .. } => {
                            Some((0..rows).map(|r| scr[ci].row_valid(r)).collect())
                        }
                    },
                ));
            }
            let gc = GCols {
                ints: &ints,
                floats: &floats,
                fixed: &fixed,
                texts: &texts,
                valids: &valids,
            };
            for r in 0..rows {
                if !pred_pass(bank, &q.preds, &gc, r) {
                    continue;
                }
                matches += 1;
                if topk_proj {
                    let key: Vec<KElem> = order_cols
                        .iter()
                        .map(|&c| {
                            if !gc.is_valid(c, r) {
                                KElem::Null
                            } else if col_width(bank, c) == 0 {
                                KElem::Text(gc.text(c, r).to_vec())
                            } else {
                                let v = gc.int(c, r);
                                KElem::Int { ord: (v as u64) ^ (1 << 63), val: v }
                            }
                        })
                        .collect();
                    let full = (key, base + r as u64);
                    let admit = top.len() < k_lim
                        || full < *top.keys().next_back().expect("nonempty top");
                    if admit {
                        let vals: Vec<TVal> = q
                            .select
                            .iter()
                            .map(|(e, _)| match e {
                                SelExpr::Expr(x) => match ev.eval(x, &gc, r) {
                                    Val::I(v) => TVal::I(v),
                                    Val::B(b) => TVal::B(b),
                                    Val::Null => TVal::Null,
                                },
                                SelExpr::Agg(_) => unreachable!(),
                            })
                            .collect();
                        top.insert(full, vals);
                        while top.len() > k_lim {
                            let last = top.keys().next_back().unwrap().clone();
                            top.remove(&last);
                        }
                    }
                    continue;
                }
                if rowfetch {
                    let vals: Vec<TVal> = q
                        .select
                        .iter()
                        .map(|(e, _)| match e {
                            SelExpr::Expr(x) => match ev.eval(x, &gc, r) {
                                Val::I(v) => TVal::I(v),
                                Val::B(b) => TVal::B(b),
                                Val::Null => TVal::Null,
                            },
                            SelExpr::Agg(_) => unreachable!(),
                        })
                        .collect();
                    fetched.push(vals);
                    continue;
                }
                let acc: &mut Acc = if grouped {
                    let key: Vec<KElem> = q
                        .group
                        .iter()
                        .map(|ke| match ev.eval(ke, &gc, r) {
                            Val::Null => KElem::Null,
                            Val::B(b) => KElem::Text(b),
                            Val::I(v) => KElem::Int { ord: key_ord(bank, ke, v), val: v },
                        })
                        .collect();
                    groups.entry(key).or_insert_with(|| Acc::sized(aggs.len()))
                } else {
                    &mut scalar
                };
                acc.count += 1;
                for (ai, a) in aggs.iter().enumerate() {
                    match a {
                        SqlAgg::CountStar => {}
                        SqlAgg::Sum(e) | SqlAgg::Avg(e) => match ev.eval(e, &gc, r) {
                            Val::Null => {}
                            Val::I(v) => {
                                acc.sums[ai] += v as i128;
                                acc.cnts[ai] += 1;
                            }
                            Val::B(_) => panic!("oracle: SUM/AVG over bytes"),
                        },
                        SqlAgg::CountDistinct(e) => match ev.eval(e, &gc, r) {
                            Val::Null => {}
                            Val::I(v) => {
                                acc.distinct[ai].insert(v.to_le_bytes().to_vec());
                            }
                            Val::B(b) => {
                                acc.distinct[ai].insert(b);
                            }
                        },
                        SqlAgg::Min(e) => {
                            if let SqlExpr::Col(c) = e {
                                if matches!(bank.face(*c), Face::F32 | Face::F64) {
                                    if gc.is_valid(*c, r) {
                                        let v = gc.float(*c, r);
                                        if acc.fmins[ai]
                                            .map(|m| f64_pg_lt(v, m))
                                            .unwrap_or(true)
                                        {
                                            acc.fmins[ai] = Some(v);
                                        }
                                    }
                                    continue;
                                }
                            }
                            match ev.eval(e, &gc, r) {
                                Val::Null => {}
                                Val::I(v) => {
                                    if acc.imins[ai].map(|m| v < m).unwrap_or(true) {
                                        acc.imins[ai] = Some(v);
                                    }
                                }
                                Val::B(b) => {
                                    if acc.bmins[ai]
                                        .as_deref()
                                        .map(|m| b.as_slice() < m)
                                        .unwrap_or(true)
                                    {
                                        acc.bmins[ai] = Some(b);
                                    }
                                }
                            }
                        }
                        SqlAgg::Max(e) => {
                            if let SqlExpr::Col(c) = e {
                                if matches!(bank.face(*c), Face::F32 | Face::F64) {
                                    if gc.is_valid(*c, r) {
                                        let v = gc.float(*c, r);
                                        if acc.fmaxs[ai]
                                            .map(|m| f64_pg_lt(m, v))
                                            .unwrap_or(true)
                                        {
                                            acc.fmaxs[ai] = Some(v);
                                        }
                                    }
                                    continue;
                                }
                            }
                            match ev.eval(e, &gc, r) {
                                Val::Null => {}
                                Val::I(v) => {
                                    if acc.imaxs[ai].map(|m| v > m).unwrap_or(true) {
                                        acc.imaxs[ai] = Some(v);
                                    }
                                }
                                Val::B(b) => {
                                    if acc.bmaxs[ai]
                                        .as_deref()
                                        .map(|m| b.as_slice() > m)
                                        .unwrap_or(true)
                                    {
                                        acc.bmaxs[ai] = Some(b);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            base += rows as u64;
        }
    }

    if topk_proj {
        let rows: Vec<Vec<TVal>> = top.into_iter().skip(q.offset).map(|(_, v)| v).collect();
        return OracleOut {
            ans: proj_answer(bank, q, rows),
            full: None,
            order_field: None,
            order_desc: false,
            groups_total: None,
            rows_folded: None,
            groups_prehaving: None,
            rows_prehaving: None,
            matches,
        };
    }
    if rowfetch {
        let end = k_lim.min(fetched.len());
        let window: Vec<Vec<TVal>> =
            fetched.drain(..).skip(q.offset.min(end)).take(end.saturating_sub(q.offset)).collect();
        return OracleOut {
            ans: proj_answer(bank, q, window),
            full: None,
            order_field: None,
            order_desc: false,
            groups_total: None,
            rows_folded: None,
            groups_prehaving: None,
            rows_prehaving: None,
            matches,
        };
    }
    if grouped {
        render_grouped(bank, q, &aggs, groups, matches)
    } else {
        OracleOut {
            ans: agg_answer(bank, &aggs, std::iter::once(&scalar)),
            full: None,
            order_field: None,
            order_desc: false,
            groups_total: None,
            rows_folded: None,
            groups_prehaving: None,
            rows_prehaving: None,
            matches,
        }
    }
}

/// Typed projection answer, select order, NULL via the validity leg.
fn proj_answer(bank: &Bank, q: &SqlQuery, rows: Vec<Vec<TVal>>) -> AnswerSet {
    enum PB {
        I(TypMeta, Vec<i64>, Vec<bool>),
        B(TypMeta, BytesBuild, Vec<bool>),
    }
    let mut builders: Vec<PB> = q
        .select
        .iter()
        .map(|(e, _)| match e {
            SelExpr::Expr(x) => {
                let ty = expr_ty(bank, x);
                if expr_is_bytes(bank, x) {
                    PB::B(ty, BytesBuild::new(), Vec::new())
                } else {
                    PB::I(ty, Vec::new(), Vec::new())
                }
            }
            SelExpr::Agg(_) => unreachable!(),
        })
        .collect();
    for vals in rows {
        for (bi, v) in vals.into_iter().enumerate() {
            match (&mut builders[bi], v) {
                (PB::I(_, out, m), TVal::I(x)) => {
                    out.push(x);
                    m.push(true);
                }
                (PB::I(_, out, m), TVal::Null) => {
                    out.push(0);
                    m.push(false);
                }
                (PB::B(_, out, m), TVal::B(b)) => {
                    out.push(&b);
                    m.push(true);
                }
                (PB::B(_, out, m), TVal::Null) => {
                    out.push(b"");
                    m.push(false);
                }
                _ => unreachable!(),
            }
        }
    }
    AnswerSet::from_cols(
        builders
            .into_iter()
            .map(|b| match b {
                PB::I(ty, v, m) => {
                    let mut c = AnswerCol::i64s(ty, v);
                    if !m.iter().all(|&x| x) {
                        c.validity = Validity::Mask(m);
                    }
                    c
                }
                PB::B(ty, bb, m) => {
                    let mut c = bb.finish(ty);
                    if !m.iter().all(|&x| x) {
                        c.validity = Validity::Mask(m);
                    }
                    c
                }
            })
            .collect(),
    )
}

/// Aggregate columns per Acc; empty/all-NULL folds render NULL (never 0).
fn agg_answer<'a>(
    bank: &Bank,
    aggs: &[SqlAgg],
    accs: impl Iterator<Item = &'a Acc> + Clone,
) -> AnswerSet {
    let mut cols: Vec<AnswerCol> = Vec::new();
    for (ai, ag) in aggs.iter().enumerate() {
        let col = agg_col(bank, ag, ai, accs.clone());
        cols.push(col);
    }
    AnswerSet::from_cols(cols)
}

fn agg_col<'a>(
    bank: &Bank,
    ag: &SqlAgg,
    ai: usize,
    accs: impl Iterator<Item = &'a Acc> + Clone,
) -> AnswerCol {
    match ag {
        SqlAgg::CountStar => {
            AnswerCol::i64s(TypMeta::INT8, accs.map(|a| a.count as i64).collect())
        }
        SqlAgg::Sum(_) => {
            let vals: Vec<i128> = accs.clone().map(|a| a.sums[ai]).collect();
            let mask: Vec<bool> = accs.map(|a| a.cnts[ai] > 0).collect();
            let mut c = AnswerCol::i128s(TypMeta::NUMERIC, vals);
            if !mask.iter().all(|&b| b) {
                c.validity = Validity::Mask(mask);
            }
            c
        }
        SqlAgg::Avg(e) => {
            let exact = matches!(e, SqlExpr::Col(c) if col_width(bank, *c) == 8);
            AnswerCol::ratios(
                TypMeta::NUMERIC,
                accs.map(|a| (a.sums[ai], a.cnts[ai] as i64)).collect(),
                exact,
            )
        }
        SqlAgg::CountDistinct(_) => {
            AnswerCol::i64s(TypMeta::INT8, accs.map(|a| a.distinct[ai].len() as i64).collect())
        }
        SqlAgg::Min(e) => minmax_col(bank, e, accs, ai, true),
        SqlAgg::Max(e) => minmax_col(bank, e, accs, ai, false),
    }
}

/// MIN/MAX column: int word / float / memcmp-bytes lanes.
fn minmax_col<'a>(
    bank: &Bank,
    e: &SqlExpr,
    accs: impl Iterator<Item = &'a Acc> + Clone,
    ai: usize,
    is_min: bool,
) -> AnswerCol {
    if let SqlExpr::Col(c) = e {
        if matches!(bank.face(*c), Face::F32 | Face::F64) {
            let opts: Vec<Option<f64>> = accs
                .map(|a| if is_min { a.fmins[ai] } else { a.fmaxs[ai] })
                .collect();
            let mask: Vec<bool> = opts.iter().map(|o| o.is_some()).collect();
            return AnswerCol {
                ty: bank.typ(*c),
                data: ColData::F64(opts.into_iter().map(|o| o.unwrap_or(0.0)).collect()),
                validity: if mask.iter().all(|&b| b) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                },
            };
        }
    }
    let ty = expr_ty(bank, e);
    if expr_is_bytes(bank, e) {
        let mut bb = BytesBuild::new();
        let mut mask: Vec<bool> = Vec::new();
        for a in accs {
            let o = if is_min { &a.bmins[ai] } else { &a.bmaxs[ai] };
            bb.push(o.as_deref().unwrap_or(b""));
            mask.push(o.is_some());
        }
        let mut col = bb.finish(ty);
        if !mask.iter().all(|&b| b) {
            col.validity = Validity::Mask(mask);
        }
        col
    } else {
        AnswerCol::i64s_opt(
            ty,
            accs.map(|a| if is_min { a.imins[ai] } else { a.imaxs[ai] }).collect(),
        )
    }
}

/// Aggregate order value; AVG compares as an exact cross-multiplied ratio.
enum AggOrd {
    I(i128),
    R { n: i128, d: i128 },
}

fn agg_ord(a: &SqlAgg, ai: usize, acc: &Acc) -> AggOrd {
    match a {
        SqlAgg::CountStar => AggOrd::I(acc.count as i128),
        SqlAgg::CountDistinct(_) => AggOrd::I(acc.distinct[ai].len() as i128),
        SqlAgg::Sum(_) => AggOrd::I(acc.sums[ai]),
        SqlAgg::Avg(_) => AggOrd::R { n: acc.sums[ai], d: (acc.cnts[ai] as i128).max(1) },
        SqlAgg::Min(_) => AggOrd::I(acc.imins[ai].unwrap_or(i64::MIN) as i128),
        SqlAgg::Max(_) => AggOrd::I(acc.imaxs[ai].unwrap_or(i64::MIN) as i128),
    }
}

fn agg_ord_cmp(a: &AggOrd, b: &AggOrd) -> std::cmp::Ordering {
    match (a, b) {
        (AggOrd::I(x), AggOrd::I(y)) => x.cmp(y),
        (AggOrd::R { n: xn, d: xd }, AggOrd::R { n: yn, d: yd }) => (xn * yd).cmp(&(yn * xd)),
        _ => panic!("oracle: mixed aggregate order values"),
    }
}

fn render_grouped(
    bank: &Bank,
    q: &SqlQuery,
    aggs: &[SqlAgg],
    groups: HashMap<Vec<KElem>, Acc>,
    matches: u64,
) -> OracleOut {
    let groups_prehaving = groups.len() as u64;
    let rows_prehaving: u64 = groups.values().map(|a| a.count).sum();
    let mut rows: Vec<(Vec<KElem>, Acc)> = groups.into_iter().collect();
    if let Some(n) = q.having_count_gt {
        rows.retain(|(_, a)| a.count > n);
    }
    let groups_total = rows.len() as u64;
    let rows_folded: u64 = rows.iter().map(|(_, a)| a.count).sum();

    let agg_slot = |i: usize| -> usize {
        q.select[..i].iter().filter(|(e, _)| matches!(e, SelExpr::Agg(_))).count()
    };

    let order_desc: bool;
    match q.order.as_slice() {
        [] => {
            rows.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(&b.0)));
            order_desc = true;
        }
        [(OKey::AggRef(i), desc)] => {
            let ai = agg_slot(*i);
            let a = &aggs[ai];
            order_desc = *desc;
            rows.sort_by(|x, y| {
                let c = agg_ord_cmp(&agg_ord(a, ai, &x.1), &agg_ord(a, ai, &y.1));
                let c = if *desc { c.reverse() } else { c };
                c.then_with(|| x.0.cmp(&y.0))
            });
        }
        [(OKey::Key(k), desc)] => {
            let k = *k;
            order_desc = *desc;
            rows.sort_by(|x, y| {
                let c = x.0[k].cmp(&y.0[k]);
                let c = if *desc { c.reverse() } else { c };
                c.then_with(|| x.0.cmp(&y.0))
            });
        }
        [(OKey::Col(c), desc)] => {
            let k = q
                .group
                .iter()
                .position(|g| g == &SqlExpr::Col(*c))
                .expect("oracle: order col not a group key");
            order_desc = *desc;
            rows.sort_by(|x, y| {
                let cmp = x.0[k].cmp(&y.0[k]);
                let cmp = if *desc { cmp.reverse() } else { cmp };
                cmp.then_with(|| x.0.cmp(&y.0))
            });
        }
        other => panic!("oracle: unsupported grouped ORDER BY {other:?}"),
    }

    let order_field: Option<usize> = match q.order.as_slice() {
        [] => None,
        [(OKey::AggRef(i), _)] => Some(*i),
        [(OKey::Key(k), _)] => q.select.iter().position(|(e, _)| match e {
            SelExpr::Expr(x) => x == &q.group[*k],
            _ => false,
        }),
        [(OKey::Col(c), _)] => q.select.iter().position(|(e, _)| match e {
            SelExpr::Expr(x) => x == &SqlExpr::Col(*c),
            _ => false,
        }),
        _ => None,
    };

    let lim = q.offset + q.limit.unwrap_or(usize::MAX / 2);
    let end = lim.min(rows.len());
    let start = q.offset.min(end);
    let render = |slice: &[(Vec<KElem>, Acc)]| -> AnswerSet {
        let window: Vec<&(Vec<KElem>, Acc)> = slice.iter().collect();
        let mut cols: Vec<AnswerCol> = Vec::new();
        for (i, (item, _)) in q.select.iter().enumerate() {
            match item {
                SelExpr::Expr(e) => {
                    let ki = q
                        .group
                        .iter()
                        .position(|g| g == e)
                        .unwrap_or_else(|| panic!("oracle: select expr {e:?} not a group key"));
                    let ty = expr_ty(bank, e);
                    if expr_is_bytes(bank, e) {
                        let mut b = BytesBuild::new();
                        let mut mask: Vec<bool> = Vec::new();
                        for (key, _) in window.iter() {
                            match &key[ki] {
                                KElem::Text(t) => {
                                    b.push(t);
                                    mask.push(true);
                                }
                                KElem::Null => {
                                    b.push(b"");
                                    mask.push(false);
                                }
                                KElem::Int { .. } => unreachable!(),
                            }
                        }
                        let mut col = b.finish(ty);
                        if !mask.iter().all(|&v| v) {
                            col.validity = Validity::Mask(mask);
                        }
                        cols.push(col);
                    } else {
                        let vals: Vec<Option<i64>> = window
                            .iter()
                            .map(|(key, _)| match &key[ki] {
                                KElem::Int { val, .. } => Some(*val),
                                KElem::Null => None,
                                KElem::Text(_) => unreachable!(),
                            })
                            .collect();
                        cols.push(AnswerCol::i64s_opt(ty, vals));
                    }
                }
                SelExpr::Agg(a) => {
                    let ai = agg_slot(i);
                    let accs: Vec<&Acc> = window.iter().map(|(_, a)| a).collect();
                    cols.push(agg_col(bank, a, ai, accs.into_iter()));
                }
            }
        }
        AnswerSet::from_cols(cols)
    };

    let ans = render(&rows[start..end]);
    let full = render(&rows);
    OracleOut {
        ans,
        full: Some(full),
        order_field,
        order_desc,
        groups_total: Some(groups_total),
        rows_folded: Some(rows_folded),
        groups_prehaving: Some(groups_prehaving),
        rows_prehaving: Some(rows_prehaving),
        matches,
    }
}

#[cfg(test)]
mod tests {
    use super::RegexEval;

    /// Ported regexp_replace vs the engine's referer_key walk,
    /// backtracking edge shapes included.
    #[test]
    fn regexp_replace_matches_the_engine_host_walk() {
        let pat = br"^https?://(?:www\.)?([^/]+)/.*$";
        let mut re = RegexEval::new();
        for s in [
            &b"https://www.example.com/path/x?y=1"[..],
            b"http://foo.bar/x",
            b"https://foo.bar/",
            b"http://www.a/b",
            b"https://www./x",
            b"nohttp://x/y",
            b"http://noslash",
            b"",
            b"http://host/a/b/c",
        ] {
            let got = re.replace(s, pat, b"\\1");
            let want = crate::kernels::referer_key(s);
            assert_eq!(
                got.as_slice(),
                want,
                "host walk diverges on {:?}",
                String::from_utf8_lossy(s)
            );
        }
    }
}
