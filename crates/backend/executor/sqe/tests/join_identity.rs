//! P4-2 phase-1 join gate: synthetic multi-part banks sealed through the
//! real writer, the hash-join stencil vs an INDEPENDENT nested-loop
//! oracle (row-at-a-time, byte-compared text keys — no fps, no hashing,
//! no partitions), byte identity through render::to_lines. Joins carry no
//! ORDER BY, so identity is over the sorted rendered-line multiset.

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{BytesCmp, CmpOp, PredSpec, PredTerm, VarOp, VarPredTerm};
use sqe::joins::{
    attach_build_fold, join_agg_node, join_node, nest_loop_agg_node, nest_loop_node,
    run_hash_join, run_hash_join_agg, BuildFold, JoinAggNode, JoinAggOp, JoinAggReq, JoinCmp,
    JoinKey, JoinNode, JoinOut, JoinQual, JoinRefuse, JoinSide, JoinType, KeyXf,
    NL_PRODUCT_BUDGET_PAIRS,
};
use sqe::render::to_lines;
use sqe::scan::{open_cursor, varlena_payload, GranValid, Scratch};
use sqe::typmeta::TypMeta;

// ---------------------------------------------------------------------------
// fixtures: schema [1:k int8, 2:g int4, 3:t text, 4:v int8, 5:s text]
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum RV {
    Null,
    I(i64),
    T(Vec<u8>),
}

impl RV {
    fn i(&self) -> Option<i64> {
        match self {
            RV::I(v) => Some(*v),
            _ => None,
        }
    }
    fn t(&self) -> Option<&[u8]> {
        match self {
            RV::T(b) => Some(b),
            _ => None,
        }
    }
}

type Row = [RV; 5];

fn wcol(attno: u32, width: u8, typlen: i16) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width, signed: true },
        typlen,
        typbyval: true,
        typalign: if width == 8 { b'd' } else { b'i' },
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

fn tcol(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

fn seal(dir: &str, rows: &[Row]) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3), wcol(4, 8, 8), tcol(5)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        777,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 2048, max_bytes: u64::MAX, cut_granule_rows: 512 },
    )
    .expect("open writer");
    for row in rows {
        let imgs: Vec<Option<Vec<u8>>> = row
            .iter()
            .map(|c| c.t().map(img_4b_u))
            .collect();
        let datums: Vec<RawDatum> = row
            .iter()
            .zip(imgs.iter())
            .map(|(c, img)| match c {
                RV::Null => RawDatum::Null,
                RV::I(v) => RawDatum::Word(*v as u64),
                RV::T(_) => RawDatum::Bytes(img.as_ref().unwrap()),
            })
            .collect();
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::Committed);
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &probe).expect("publish");
}

fn schema_meta() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "t", TypMeta::TEXT_C),
        ColMeta::new(4, "v", TypMeta::INT8),
        ColMeta::new(5, "s", TypMeta::TEXT_C),
    ]
}

fn open_engine(dir: &str) -> Engine {
    let bank = Bank::open(dir, schema_meta(), &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 3, ..SqeConfig::default() })
}

fn empty_engine(tag: &str) -> Engine {
    Engine::new(
        Bank::empty(format!("/nonexistent-{tag}"), schema_meta()),
        SqeConfig { threads: 3, ..SqeConfig::default() },
    )
}

/// Build side: dup-heavy + skewed int key, NULLs in k/t/v, shared-dict
/// text key `t` in a null-free twin column style (t NULL only when
/// `nullable_t`).
fn build_rows(n: u64, nullable_t: bool) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let k = if i % 13 == 0 {
                RV::Null
            } else if i % 10 < 3 {
                RV::I(7)
            } else {
                RV::I((i % 97) as i64)
            };
            let g = RV::I((i % 16) as i64);
            let t = if nullable_t && i % 17 == 0 {
                RV::Null
            } else {
                RV::T(format!("t{:02}", i % 23).into_bytes())
            };
            let v = if i % 11 == 0 { RV::Null } else { RV::I((i % 50) as i64) };
            let s = RV::T(format!("s{}", i % 7).into_bytes());
            [k, g, t, v, s]
        })
        .collect()
}

/// Probe side: different distribution, part-disjoint dict ranges for `t`
/// (each 2048-row part carries its own string set), NULLs both sides.
fn probe_rows(n: u64) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let k = if i % 19 == 0 {
                RV::Null
            } else if i % 7 == 0 {
                RV::I(7)
            } else {
                RV::I((i % 140) as i64)
            };
            let g = RV::I((i % 24) as i64);
            let part = i / 2048;
            let t = if part % 2 == 0 {
                RV::T(format!("t{:02}", i % 23).into_bytes())
            } else {
                RV::T(format!("x{}_{}", part, i % 29).into_bytes())
            };
            let v = if i % 9 == 0 { RV::Null } else { RV::I((i % 60) as i64) };
            let s = RV::T(format!("p{}", i % 5).into_bytes());
            [k, g, t, v, s]
        })
        .collect()
}

// ---------------------------------------------------------------------------
// independent nested-loop oracle
// ---------------------------------------------------------------------------

fn load_rows(bank: &Bank) -> Vec<Row> {
    let n = bank.rows_total() as usize;
    let mut out: Vec<Row> =
        vec![[RV::Null, RV::Null, RV::Null, RV::Null, RV::Null].clone(); n];
    for attno in 1..=5u32 {
        let text = bank.typ(attno).is_varlena();
        let face = bank.face(attno);
        let mut scr = Scratch::new();
        let mut at = 0usize;
        for pi in 0..bank.parts.len() {
            let mut cur = open_cursor(bank, pi, attno);
            for g in 0..cur.granule_count() {
                let rows = cur.rows_in_granule(g) as usize;
                let gv = scr.validity(&mut cur, g, rows);
                let d = scr.decode_full(&mut cur, g, rows).to_vec();
                for (r, &w) in d.iter().enumerate() {
                    let ok = matches!(gv, GranValid::AllValid) || scr.row_valid(r);
                    out[at + r][(attno - 1) as usize] = if !ok {
                        RV::Null
                    } else if text {
                        RV::T(unsafe { varlena_payload(w) }.to_vec())
                    } else {
                        RV::I(face.word_key(w))
                    };
                }
                at += rows;
            }
        }
    }
    out
}

fn o_pred(pred: &Option<PredSpec>, row: &Row) -> bool {
    let Some(p) = pred else { return true };
    p.terms.iter().all(|t| {
        let Some(v) = row[(t.col - 1) as usize].i() else { return false };
        match t.op {
            CmpOp::Eq => v == t.lo,
            CmpOp::Ne => v != t.lo,
            CmpOp::Between => v >= t.lo && v <= t.hi,
            CmpOp::In2 => v == t.lo || v == t.hi,
        }
    }) && p.col_terms.iter().all(|t| {
        // [colcmp] 3VL: a NULL operand never passes.
        let (Some(a), Some(b)) = (row[(t.a - 1) as usize].i(), row[(t.b - 1) as usize].i())
        else {
            return false;
        };
        t.op.eval(a, b)
    }) && p.var_terms.iter().all(|t| {
        // Varlena conjunct over the row's own payload bytes (3VL); the
        // matcher is the term's (like.rs carries its own oracle tests) —
        // decode, staging, and NULL law stay independently checked here.
        let Some(b) = row[(t.col - 1) as usize].t() else { return false };
        t.eval(b)
    })
}

fn o_keys_eq(node: &JoinNode, b: &Row, p: &Row) -> bool {
    node.keys.iter().all(|k| {
        let (bc, pc) = ((k.build_col - 1) as usize, (k.probe_col - 1) as usize);
        match (&b[bc], &p[pc]) {
            (RV::I(x), RV::I(y)) => x == y,
            (RV::T(x), RV::T(y)) => x == y,
            _ => false,
        }
    })
}

fn o_quals(node: &JoinNode, b: &Row, p: &Row) -> bool {
    node.quals.iter().all(|q| {
        let (Some(bv), Some(pv)) =
            (b[(q.build_col - 1) as usize].i(), p[(q.probe_col - 1) as usize].i())
        else {
            return false;
        };
        match q.op {
            JoinCmp::Lt => pv < bv,
            JoinCmp::Le => pv <= bv,
            JoinCmp::Gt => pv > bv,
            JoinCmp::Ge => pv >= bv,
            JoinCmp::Ne => pv != bv,
            JoinCmp::Eq => pv == bv,
        }
    })
}

fn oracle_join(node: &JoinNode, brows: &[Row], prows: &[Row]) -> Vec<Vec<RV>> {
    let bs: Vec<&Row> = brows.iter().filter(|r| o_pred(&node.build_pred, r)).collect();
    let mut out: Vec<Vec<RV>> = Vec::new();
    // [sqe-semi-anti] right classes: iterate BUILD rows; each emits once
    // iff some/no post-pred probe row matches (NULL keys never match, so
    // a NULL-keyed build row emits under RightAnti and never RightSemi).
    if matches!(node.join_type, JoinType::RightSemi | JoinType::RightAnti) {
        let ps: Vec<&Row> = prows.iter().filter(|r| o_pred(&node.probe_pred, r)).collect();
        let want = node.join_type == JoinType::RightSemi;
        for b in &bs {
            let matched = ps.iter().any(|p| o_keys_eq(node, b, p) && o_quals(node, b, p));
            if matched == want {
                out.push(
                    node.out
                        .iter()
                        .map(|o| match o.side {
                            JoinSide::Build => b[(o.col - 1) as usize].clone(),
                            _ => unreachable!("right classes project build only"),
                        })
                        .collect(),
                );
            }
        }
        return out;
    }
    if node.join_type == JoinType::Right {
        let ps: Vec<&Row> = prows.iter().filter(|r| o_pred(&node.probe_pred, r)).collect();
        for b in &bs {
            let mut matched = false;
            for p in &ps {
                if o_keys_eq(node, b, p) && o_quals(node, b, p) {
                    matched = true;
                    out.push(
                        node.out
                            .iter()
                            .map(|o| match o.side {
                                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                                JoinSide::Build => b[(o.col - 1) as usize].clone(),
                                JoinSide::Dim(_) => unreachable!("2-way oracle sees no dim outs"),
                            })
                            .collect(),
                    );
                }
            }
            if !matched {
                out.push(
                    node.out
                        .iter()
                        .map(|o| match o.side {
                            JoinSide::Build => b[(o.col - 1) as usize].clone(),
                            _ => RV::Null,
                        })
                        .collect(),
                );
            }
        }
        return out;
    }
    let project = |p: &Row, b: Option<&Row>| -> Vec<RV> {
        node.out
            .iter()
            .map(|o| match o.side {
                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                JoinSide::Build => match b {
                    Some(b) => b[(o.col - 1) as usize].clone(),
                    None => RV::Null,
                },
                JoinSide::Dim(_) => unreachable!("2-way oracle sees no dim outs"),
            })
            .collect()
    };
    for p in prows.iter().filter(|r| o_pred(&node.probe_pred, r)) {
        let mut matched = false;
        for b in &bs {
            if o_keys_eq(node, b, p) && o_quals(node, b, p) {
                matched = true;
                match node.join_type {
                    JoinType::Inner | JoinType::Left => out.push(project(p, Some(b))),
                    JoinType::Semi => {
                        out.push(project(p, None));
                        break;
                    }
                    JoinType::Anti => break,
                    JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                        unreachable!("right classes returned above")
                    }
                }
            }
        }
        if !matched && matches!(node.join_type, JoinType::Left | JoinType::Anti) {
            out.push(project(p, None));
        }
    }
    out
}

fn oracle_answer(node: &JoinNode, rows: Vec<Vec<RV>>) -> AnswerSet {
    let mut cols: Vec<AnswerCol> = Vec::new();
    for (oi, ty) in node.out_tys.iter().enumerate() {
        if ty.is_varlena() {
            let mut bb = BytesBuild::new();
            let mut mask = Vec::new();
            for r in &rows {
                match &r[oi] {
                    RV::T(b) => {
                        bb.push(b);
                        mask.push(true);
                    }
                    _ => {
                        bb.push(b"");
                        mask.push(false);
                    }
                }
            }
            let mut c = bb.finish(*ty);
            if !mask.iter().all(|&x| x) {
                c.validity = Validity::Mask(mask);
            }
            cols.push(c);
        } else {
            let mut v = Vec::new();
            let mut mask = Vec::new();
            for r in &rows {
                match &r[oi] {
                    RV::I(x) => {
                        v.push(*x);
                        mask.push(true);
                    }
                    _ => {
                        v.push(0);
                        mask.push(false);
                    }
                }
            }
            let validity = if mask.iter().all(|&x| x) {
                Validity::AllValid
            } else {
                Validity::Mask(mask)
            };
            cols.push(AnswerCol { ty: *ty, data: ColData::I64(v), validity });
        }
    }
    AnswerSet::from_cols(cols)
}

// ---------------------------------------------------------------------------
// the gate
// ---------------------------------------------------------------------------

struct Fix {
    be: Engine,
    pe: Engine,
    brows: Vec<Row>,
    prows: Vec<Row>,
}

fn fixture(tag: &str, nullable_t: bool, nb: u64, np: u64) -> Fix {
    let base = std::env::temp_dir().join(format!("sqe_join_{tag}_{}", std::process::id()));
    let bdir = base.join("b");
    let pdir = base.join("p");
    let _ = std::fs::remove_dir_all(&base);
    let brows = build_rows(nb, nullable_t);
    let prows = probe_rows(np);
    seal(bdir.to_str().unwrap(), &brows);
    seal(pdir.to_str().unwrap(), &prows);
    Fix {
        be: open_engine(bdir.to_str().unwrap()),
        pe: open_engine(pdir.to_str().unwrap()),
        brows,
        prows,
    }
}

/// The fixture with an explicit spill switch (the set-plane spill tests
/// force it on; the freeze-then-refuse pin forces it off so it stays
/// deterministic whatever sibling tests registered the std store).
fn fixture_sp(tag: &str, nullable_t: bool, nb: u64, np: u64, spill: bool) -> Fix {
    let mut fix = fixture(tag, nullable_t, nb, np);
    let re = |e: &Engine| {
        let bank = Bank::open(&e.bank.dir, schema_meta(), &OpenOpts { bankstats: false, threads: 1 });
        Engine::new(bank, SqeConfig { threads: 3, spill, ..SqeConfig::default() })
    };
    fix.be = re(&fix.be);
    fix.pe = re(&fix.pe);
    fix
}

fn check(fix: &Fix, node: &JoinNode, what: &str) {
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], node)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let want = oracle_answer(node, oracle_join(node, &fix.brows, &fix.prows));
    let mut gl = to_lines(&got);
    let mut wl = to_lines(&want);
    gl.sort();
    wl.sort();
    assert_eq!(gl.len(), wl.len(), "{what}: row count {} vs oracle {}", got.nrows, want.nrows);
    assert_eq!(gl, wl, "{what}: rendered identity");
}

fn base_node(fix: &Fix, jt: JoinType, keys: Vec<JoinKey>, out: Vec<JoinOut>) -> JoinNode {
    join_node(
        &fix.be.bank,
        &fix.pe.bank, &[],
        0,
        jt,
        keys,
        Vec::new(),
        None,
        None, Vec::new(),
        out,
        fix.be.bank.rows_total(), false)
    .expect("construct")
}

fn out_pb(p: &[u32], b: &[u32]) -> Vec<JoinOut> {
    p.iter()
        .map(|&c| JoinOut { side: JoinSide::Probe, col: c })
        .chain(b.iter().map(|&c| JoinOut { side: JoinSide::Build, col: c }))
        .collect()
}

const ALL: [JoinType; 4] = [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti];

#[test]
fn join_int_single_key_all_types() {
    let fix = fixture("int1", true, 6000, 9000);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2, 5], &[])
        } else {
            out_pb(&[1, 2], &[4, 5])
        };
        let node = base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
        check(&fix, &node, &format!("int-single {jt:?}"));
    }
}

#[test]
fn join_multi_int_key() {
    let fix = fixture("int2", true, 6000, 9000);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2], &[])
        } else {
            out_pb(&[1, 2], &[4])
        };
        let node = base_node(
            &fix,
            jt,
            vec![
                JoinKey { build_col: 1, probe_col: 1 },
                JoinKey { build_col: 2, probe_col: 2 },
            ],
            out,
        );
        check(&fix, &node, &format!("int-multi {jt:?}"));
    }
}

#[test]
fn join_text_key_all_types() {
    // null-free build `t` exercises the dict entry-fp path; probe parts
    // alternate shared/disjoint dictionaries.
    let fix = fixture("txt", false, 6000, 9000);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[3, 1], &[])
        } else {
            out_pb(&[3, 1], &[4, 5])
        };
        let node = base_node(&fix, jt, vec![JoinKey { build_col: 3, probe_col: 3 }], out);
        check(&fix, &node, &format!("text {jt:?}"));
    }
}

#[test]
fn join_text_key_nullable_build() {
    let fix = fixture("txtn", true, 4000, 6000);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[3], &[])
        } else {
            out_pb(&[3], &[5])
        };
        let node = base_node(&fix, jt, vec![JoinKey { build_col: 3, probe_col: 3 }], out);
        check(&fix, &node, &format!("text-nullable {jt:?}"));
    }
}

#[test]
fn join_mixed_int_text_key() {
    let fix = fixture("mix", false, 6000, 9000);
    for jt in [JoinType::Inner, JoinType::Left] {
        let node = base_node(
            &fix,
            jt,
            vec![
                JoinKey { build_col: 2, probe_col: 2 },
                JoinKey { build_col: 3, probe_col: 3 },
            ],
            out_pb(&[2, 3], &[4]),
        );
        check(&fix, &node, &format!("mixed {jt:?}"));
    }
}

#[test]
fn join_nonequi_quals() {
    let fix = fixture("qual", true, 5000, 7000);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 4], &[])
        } else {
            out_pb(&[1, 4], &[4])
        };
        let mut node = base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
        node.quals = vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }];
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            node.keys.clone(),
            node.quals.clone(),
            None,
            None, Vec::new(),
            node.out.clone(),
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("qual {jt:?}"));
    }
}

#[test]
fn join_side_preds() {
    let fix = fixture("pred", true, 5000, 7000);
    let bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 5, 40, TypMeta::INT8)]);
    let pp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 3, 18, TypMeta::INT4)]);
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Anti] {
        let out = if matches!(jt, JoinType::Anti) {
            out_pb(&[1, 2], &[])
        } else {
            out_pb(&[1, 2], &[4])
        };
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(),
            out,
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("side-pred {jt:?}"));
    }
}

/// Varlena side conjuncts on BOTH sides (LIKE / NOT LIKE / byte-eq /
/// NeEmpty / Contains / InBytes) composed with word terms — every join
/// class, oracle 3VL over a NULLABLE text lane.
#[test]
fn join_side_var_preds() {
    let fix = fixture("varpred", true, 5000, 7000);
    let text_b = fix.be.bank.typ(3);
    let text_p = fix.pe.bank.typ(3);
    let mut bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 0, 45, TypMeta::INT8)]);
    bp.var_terms = vec![VarPredTerm::new(3, VarOp::Like, b"t1%".to_vec(), text_b)];
    let mut pp = PredSpec::all(Vec::new());
    pp.var_terms = vec![
        VarPredTerm::new(3, VarOp::NotLike, b"x1%".to_vec(), text_p),
        VarPredTerm::new(5, VarOp::CmpBytes(BytesCmp::Eq), b"p3".to_vec(), fix.pe.bank.typ(5)),
    ];
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti] {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2, 3], &[])
        } else {
            out_pb(&[1, 2, 3], &[3, 4])
        };
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(),
            out,
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("side-var-pred {jt:?}"));
    }
    // One op per leg over the nullable build text lane, word-free spec.
    let legs: Vec<(&str, VarPredTerm)> = vec![
        ("contains", VarPredTerm::new(3, VarOp::Contains, b"1".to_vec(), text_b)),
        ("not-contains", VarPredTerm::new(3, VarOp::NotContains, b"t0".to_vec(), text_b)),
        ("ne-empty", VarPredTerm::new(3, VarOp::NeEmpty, Vec::new(), text_b)),
        ("like-under", VarPredTerm::new(3, VarOp::Like, b"t_2".to_vec(), text_b)),
        ("bytes-ne", VarPredTerm::new(3, VarOp::CmpBytes(BytesCmp::Ne), b"t07".to_vec(), text_b)),
        (
            "in-bytes",
            VarPredTerm::new(
                3,
                VarOp::InBytes,
                sqe::ir::encode_in_needles(vec![b"t03".to_vec(), b"t11".to_vec()]),
                text_b,
            ),
        ),
    ];
    for (tag, vt) in legs {
        let mut bp = PredSpec::all(Vec::new());
        bp.var_terms = vec![vt];
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            Some(bp),
            None, Vec::new(),
            out_pb(&[1, 2], &[3]),
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("side-var-{tag}"));
    }
}

/// [colcmp] column-vs-column side conjuncts: build `v < k` (both
/// nullable), probe `g >= v` (v nullable), composed with a const
/// conjunct — every join class, oracle 3VL.
#[test]
fn join_side_colcmp_preds() {
    let fix = fixture("colcmp", true, 5000, 7000);
    let mut bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 0, 40, TypMeta::INT8)]);
    bp.col_terms = vec![sqe::ir::ColCmpTerm::new(4, 1, sqe::ir::ColCmpOp::Lt, TypMeta::INT8)];
    let mut pp = PredSpec::all(Vec::new());
    pp.col_terms = vec![sqe::ir::ColCmpTerm::new(2, 4, sqe::ir::ColCmpOp::Ge, TypMeta::INT4)];
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti] {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2, 4], &[])
        } else {
            out_pb(&[1, 2, 4], &[4])
        };
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(),
            out,
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("side-colcmp {jt:?}"));
    }
    // Every comparator on one probe pair, ungrouped count over the join.
    for op in [
        sqe::ir::ColCmpOp::Lt,
        sqe::ir::ColCmpOp::Le,
        sqe::ir::ColCmpOp::Gt,
        sqe::ir::ColCmpOp::Ge,
        sqe::ir::ColCmpOp::Eq,
        sqe::ir::ColCmpOp::Ne,
    ] {
        let mut pp = PredSpec::all(Vec::new());
        pp.col_terms = vec![sqe::ir::ColCmpTerm::new(2, 4, op, TypMeta::INT4)];
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            Some(pp), Vec::new(),
            out_pb(&[1, 2, 4], &[4]),
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("side-colcmp-{op:?}"));
    }
}

#[test]
fn join_dup_heavy_keys() {
    let fix = {
        let base =
            std::env::temp_dir().join(format!("sqe_join_dup_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let brows: Vec<Row> = (0..3000u64)
            .map(|i| {
                [
                    RV::I((i % 3) as i64),
                    RV::I((i % 4) as i64),
                    RV::T(b"d".to_vec()),
                    RV::I(i as i64),
                    RV::T(b"z".to_vec()),
                ]
            })
            .collect();
        let prows: Vec<Row> = (0..500u64)
            .map(|i| {
                [
                    RV::I((i % 5) as i64),
                    RV::I((i % 4) as i64),
                    RV::T(b"d".to_vec()),
                    RV::I(i as i64),
                    RV::T(b"y".to_vec()),
                ]
            })
            .collect();
        let (bd, pd) = (base.join("b"), base.join("p"));
        seal(bd.to_str().unwrap(), &brows);
        seal(pd.to_str().unwrap(), &prows);
        Fix {
            be: open_engine(bd.to_str().unwrap()),
            pe: open_engine(pd.to_str().unwrap()),
            brows,
            prows,
        }
    };
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 4], &[])
        } else {
            out_pb(&[1, 4], &[4])
        };
        let node = base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
        check(&fix, &node, &format!("dup-heavy {jt:?}"));
    }
}

#[test]
fn join_empty_build_and_probe() {
    let fix = fixture("empt", true, 2000, 2000);
    let eb = empty_engine("b");
    let ep = empty_engine("p");
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1], &[])
        } else {
            out_pb(&[1], &[4])
        };
        let node = join_node(
            &eb.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(),
            out.clone(),
            0, false)
        .expect("construct");
        let efix = Fix {
            be: empty_engine("b2"),
            pe: open_engine(&fix.pe.bank.dir),
            brows: Vec::new(),
            prows: fix.prows.clone(),
        };
        let got = run_hash_join(&eb.ctx(), &fix.pe.ctx(), &[], &node).expect("run");
        let want = oracle_answer(&node, oracle_join(&node, &efix.brows, &efix.prows));
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "empty-build {jt:?}");
        // empty probe: zero rows for every join type
        let node2 = join_node(
            &fix.be.bank,
            &ep.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(),
            out,
            0, false)
        .expect("construct");
        let got2 = run_hash_join(&fix.be.ctx(), &ep.ctx(), &[], &node2).expect("run");
        assert_eq!(got2.nrows, 0, "empty-probe {jt:?}");
    }
}

// ---------------------------------------------------------------------------
// [sqe-semi-anti] right semi / right anti: build-side match flags, the
// post-probe build-emit sweep, RightAnti NULL-key retention.
// ---------------------------------------------------------------------------

const RIGHTS: [JoinType; 2] = [JoinType::RightSemi, JoinType::RightAnti];

#[test]
fn join_right_int_keys() {
    let fix = fixture("rint", true, 6000, 9000);
    for jt in RIGHTS {
        // single key (NULLs both sides; dup-heavy at k=7)
        let node =
            base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out_pb(&[], &[1, 2, 5]));
        check(&fix, &node, &format!("right-int-single {jt:?}"));
        // multi key
        let node = base_node(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }, JoinKey { build_col: 2, probe_col: 2 }],
            out_pb(&[], &[1, 2, 4]),
        );
        check(&fix, &node, &format!("right-int-multi {jt:?}"));
    }
}

#[test]
fn join_right_text_keys() {
    // null-free build text key (dict entry-fp path), then a NULLABLE
    // build text key — the RightAnti retained rows ride guarded fp lanes.
    let nf = fixture("rtxt", false, 6000, 9000);
    let nn = fixture("rtxtn", true, 4000, 6000);
    for jt in RIGHTS {
        let node = base_node(&nf, jt, vec![JoinKey { build_col: 3, probe_col: 3 }], out_pb(&[], &[3, 1]));
        check(&nf, &node, &format!("right-text {jt:?}"));
        let node = base_node(&nn, jt, vec![JoinKey { build_col: 3, probe_col: 3 }], out_pb(&[], &[3, 4]));
        check(&nn, &node, &format!("right-text-nullable {jt:?}"));
    }
}

#[test]
fn join_right_quals_and_preds() {
    let fix = fixture("rqual", true, 5000, 7000);
    let bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 5, 40, TypMeta::INT8)]);
    let pp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 3, 18, TypMeta::INT4)]);
    for jt in RIGHTS {
        let node = join_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(),
            out_pb(&[], &[1, 4]),
            0, false)
        .expect("construct");
        check(&fix, &node, &format!("right-qual-pred {jt:?}"));
    }
}

#[test]
fn join_right_empty_sides() {
    let fix = fixture("rempt", true, 2000, 2000);
    for jt in RIGHTS {
        // empty build: zero entries -> zero emitted rows.
        let eb = empty_engine("rb");
        let node = join_node(
            &eb.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(),
            out_pb(&[], &[1]),
            0, false)
        .expect("construct");
        let got = run_hash_join(&eb.ctx(), &fix.pe.ctx(), &[], &node).expect("run");
        assert_eq!(got.nrows, 0, "right-empty-build {jt:?}");
        // empty probe: RightSemi emits nothing; RightAnti emits EVERY
        // build row (the punits-empty emit path).
        let ep = empty_engine("rp");
        let node2 = join_node(
            &fix.be.bank,
            &ep.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(),
            out_pb(&[], &[1, 4]),
            0, false)
        .expect("construct");
        let got2 = run_hash_join(&fix.be.ctx(), &ep.ctx(), &[], &node2).expect("run");
        let want2 = oracle_answer(&node2, oracle_join(&node2, &fix.brows, &[]));
        let (mut gl, mut wl) = (to_lines(&got2), to_lines(&want2));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "right-empty-probe {jt:?}");
        if jt == JoinType::RightSemi {
            assert_eq!(got2.nrows, 0, "right-semi over empty probe emits nothing");
        } else {
            assert_eq!(got2.nrows as usize, fix.brows.len(), "right-anti over empty probe emits all");
        }
    }
}

#[test]
fn join_right_agg_ungrouped_and_grouped() {
    let fix = fixture("ragg", true, 6000, 9000);
    for jt in RIGHTS {
        // ungrouped folds over build-side inputs (NULLs in v and t).
        let anode = agg_node(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::CountCol, bin(3)),
                (JoinAggOp::Sum, bin(4)),
                (JoinAggOp::Min, bin(1)),
                (JoinAggOp::Max, bin(2)),
            ],
        );
        check_agg(&fix, &anode, &format!("right-agg-ungrouped {jt:?}"));
        // pure count(*): the synthesized BUILD-key out.
        let anode = agg_node(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![(JoinAggOp::CountStar, None)],
        );
        check_agg(&fix, &anode, &format!("right-agg-count-star {jt:?}"));
        // grouped by BUILD keys (col 1 carries NULLs -> one NULL group;
        // col 2 null-free) — the non-probe-side composite sink arm.
        for gcol in [1u32, 2] {
            let anode = join_agg_node(
                &fix.be.bank,
                &fix.pe.bank, &[],
                0,
                jt,
                vec![JoinKey { build_col: 1, probe_col: 1 }],
                Vec::new(),
                None,
                None, Vec::new(),
                vec![
                    (JoinAggOp::CountStar, None),
                    (JoinAggOp::Sum, bin(4)),
                    (JoinAggOp::Min, bin(4)),
                ]
                .into_iter()
                .map(|(op, input)| JoinAggReq::col(op, input))
                .collect(),
                Vec::from_iter(bin(gcol)),
                0)
            .expect("construct grouped right");
            check_group_fold(&fix, &anode, &format!("right-grouped {jt:?} g{gcol}"));
        }
    }
}

#[test]
fn join_right_constructor_refusals_typed() {
    let fix = fixture("rctor", true, 100, 100);
    let (b, p) = (&fix.be.bank, &fix.pe.bank);
    let k = || vec![JoinKey { build_col: 1, probe_col: 1 }];
    for jt in RIGHTS {
        // probe-side out refuses (right classes emit build rows only).
        assert!(matches!(
            join_node(b, p, &[], 0, jt, k(), Vec::new(), None, None, Vec::new(), out_pb(&[1], &[]), 0, false),
            Err(JoinRefuse::Unsupported { what: "right-semi-anti-probe-out" })
        ));
        // keyless right classes are outside the vocabulary.
        assert!(matches!(
            nest_loop_node(b, p, 0, jt, vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }], None, None, out_pb(&[], &[1]), 0),
            Err(JoinRefuse::Unsupported { what: "right-join-keyless" })
        ));
        // probe-side group key refuses.
        assert!(matches!(
            join_agg_node(
                b, p, &[], 0, jt, k(), Vec::new(), None, None, Vec::new(),
                vec![JoinAggReq::col(JoinAggOp::CountStar, None)],
                Vec::from_iter(pin(1)), 0
            ),
            Err(JoinRefuse::Unsupported { what: "agg-group-side" })
        ));
    }
}

#[test]
fn join_budget_refusal_typed() {
    let fix = fixture("budg", true, 4000, 1000);
    let mut node = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1], &[4]),
    );
    node.build_budget_bytes = 1024;
    match run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node) {
        Err(JoinRefuse::BuildExceedsBudget { budget: 1024, .. }) => {}
        other => panic!("expected budget refusal, got {other:?}"),
    }
    // with a build pred the pre-check is skipped; the scatter accounting
    // must still refuse (typed), never OOM.
    let bp = PredSpec::all(vec![PredTerm::new(1, CmpOp::Between, 0, 1 << 40, TypMeta::INT8)]);
    let mut node2 = join_node(
        &fix.be.bank,
        &fix.pe.bank, &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        Some(bp),
        None, Vec::new(),
        out_pb(&[1], &[4]),
        0, false)
    .expect("construct");
    node2.build_budget_bytes = 1024;
    match run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node2) {
        Err(JoinRefuse::BuildExceedsBudget { .. }) => {}
        other => panic!("expected scatter-time budget refusal, got {other:?}"),
    }
}

#[test]
fn join_constructor_refusals_typed() {
    let fix = fixture("ctor", true, 100, 100);
    let (b, p) = (&fix.be.bank, &fix.pe.bank);
    let k = |bc, pc| vec![JoinKey { build_col: bc, probe_col: pc }];
    let out = out_pb(&[1], &[]);
    assert!(matches!(
        join_node(b, p, &[], 0, JoinType::Semi, Vec::new(), Vec::new(), None, None, Vec::new(), out.clone(), 0, false),
        Err(JoinRefuse::Unsupported { what: "no-equi-key" })
    ));
    assert!(matches!(
        join_node(b, p, &[], 0, JoinType::Semi, k(1, 3), Vec::new(), None, None, Vec::new(), out.clone(), 0, false),
        Err(JoinRefuse::Unsupported { what: "key-type-mix" })
    ));
    assert!(matches!(
        join_node(
            b,
            p, &[],
            0,
            JoinType::Semi,
            k(1, 1),
            Vec::new(),
            None,
            None, Vec::new(),
            vec![JoinOut { side: JoinSide::Build, col: 4 }],
            0,
            false
        ),
        Err(JoinRefuse::Unsupported { what: "semi-anti-build-out" })
    ));
    assert!(matches!(
        join_node(
            b,
            p, &[],
            0,
            JoinType::Inner,
            k(1, 1),
            vec![JoinQual { probe_col: 3, build_col: 4, op: JoinCmp::Lt }],
            None,
            None, Vec::new(),
            out,
            0,
            false
        ),
        Err(JoinRefuse::Unsupported { what: "qual-face-no-word-embed" })
    ));
}

// ---------------------------------------------------------------------------
// fused agg-over-join gate: same fixtures, same independent nested-loop
// oracle, aggregates computed row-by-row over the oracle's joined rows.
// ---------------------------------------------------------------------------

/// [sqe-q9arith] One agg leg's per-joined-row fold input: the plain
/// column word, or the fused-arithmetic evaluation (NULL in EITHER
/// operand makes the row's input NULL — PG's strict transition over a
/// NULL-propagating expression).
/// [caseleg] The CASE test over the row's test-column value (3VL).
fn o_case_test(t: &sqe::joins::CaseTest, v: &RV) -> bool {
    use sqe::joins::CaseTest;
    match t {
        CaseTest::Word(p) => v.i().is_some_and(|w| p.eval(w)),
        CaseTest::Packed(p, _) => v.i().is_some_and(|w| p.eval(w)),
        CaseTest::InWords(ws) => v.i().is_some_and(|w| ws.binary_search(&w).is_ok()),
        CaseTest::Bytes(p) => matches!(v, RV::T(b) if p.eval(b)),
        CaseTest::And(ts) => ts.iter().all(|t| o_case_test(t, v)),
    }
}

/// [caseleg] CountCol validity at one joined row (the case gate, then
/// the input's non-NULL-ness; the lane-less count shape counts passes).
fn o_count_ok(anode: &JoinAggNode, ai: usize, r: &[RV]) -> bool {
    if let Some(c) = &anode.aggs[ai].case {
        if !o_case_test(&c.test, &r[anode.agg_oic[ai]]) {
            return c.else_zero;
        }
        if anode.agg_oi[ai] == usize::MAX {
            return true;
        }
    }
    r[anode.agg_oi[ai]] != RV::Null
}

fn o_arith_val(anode: &JoinAggNode, ai: usize, r: &[RV]) -> Option<i64> {
    if let Some(c) = &anode.aggs[ai].case {
        if !o_case_test(&c.test, &r[anode.agg_oic[ai]]) {
            return c.else_zero.then_some(0);
        }
        if anode.agg_oi[ai] == usize::MAX {
            return Some(1);
        }
    }
    let x = r[anode.agg_oi[ai]].i();
    match anode.aggs[ai].arith {
        None => x,
        Some(sqe::joins::JoinArith::AddK { k, .. }) => x.map(|x| x + k),
        Some(sqe::joins::JoinArith::MulCC { .. }) => {
            x.zip(r[anode.agg_oi2[ai]].i()).map(|(x, y)| x * y)
        }
        Some(sqe::joins::JoinArith::MulKSub { k, .. }) => {
            x.zip(r[anode.agg_oi2[ai]].i()).map(|(x, y)| x * (k - y))
        }
        Some(sqe::joins::JoinArith::PackedMulK { k, sub, .. }) => x
            .zip(r[anode.agg_oi2[ai]].i())
            .map(|(x, y)| x * if sub { k - y } else { k + y }),
        Some(sqe::joins::JoinArith::PackedMulKSubCC { k, sub, sa, sb, c, sc, d, sd }) => {
            let pos = |io| anode.join.out.iter().position(|o| *o == io).unwrap();
            let s = (sa + sb).max(sc + sd);
            let (ma, mb) = (10i64.pow((s - sa - sb) as u32), 10i64.pow((s - sc - sd) as u32));
            let y = r[anode.agg_oi2[ai]].i();
            let cd = r[pos(c)].i().zip(r[pos(d)].i());
            x.zip(y).zip(cd).map(|((x, y), (cv, dv))| {
                x * (if sub { k - y } else { k + y }) * ma - cv * dv * mb
            })
        }
    }
}

fn oracle_agg_answer(anode: &JoinAggNode, rows: &[Vec<RV>]) -> AnswerSet {
    let cols: Vec<AnswerCol> = anode
        .aggs
        .iter()
        .enumerate()
        .map(|(ai, a)| {
            let oi = anode.agg_oi[ai];
            match a.op {
                JoinAggOp::CountStar => AnswerCol::i64s(a.out, vec![rows.len() as i64]),
                JoinAggOp::CountCol => {
                    let n = rows.iter().filter(|r| o_count_ok(anode, ai, r)).count();
                    AnswerCol::i64s(a.out, vec![n as i64])
                }
                JoinAggOp::CountDistinct => {
                    let mut set: std::collections::BTreeSet<i64> = Default::default();
                    for r in rows.iter() {
                        if let Some(w) = r[oi].i() {
                            set.insert(w);
                        }
                    }
                    AnswerCol::i64s(a.out, vec![set.len() as i64])
                }
                JoinAggOp::Sum => {
                    let (mut sum, mut n) = (0i128, 0u64);
                    for r in rows {
                        if let Some(v) = o_arith_val(anode, ai, r) {
                            sum += v as i128;
                            n += 1;
                        }
                    }
                    let mut c = AnswerCol::i128s(a.out, vec![sum]);
                    if n == 0 {
                        c.validity = Validity::Mask(vec![false]);
                    }
                    c
                }
                JoinAggOp::Min => {
                    let m = rows.iter().filter_map(|r| r[oi].i()).min();
                    AnswerCol::i64s_opt(a.out, vec![m])
                }
                JoinAggOp::Max => {
                    let m = rows.iter().filter_map(|r| r[oi].i()).max();
                    AnswerCol::i64s_opt(a.out, vec![m])
                }
            }
        })
        .collect();
    AnswerSet::from_cols(cols)
}

fn check_agg(fix: &Fix, anode: &JoinAggNode, what: &str) {
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], anode)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let want = oracle_agg_answer(anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
    assert_eq!(to_lines(&got), to_lines(&want), "{what}: rendered identity");
}

fn agg_node(
    fix: &Fix,
    jt: JoinType,
    keys: Vec<JoinKey>,
    aggs: Vec<(JoinAggOp, Option<JoinOut>)>,
) -> JoinAggNode {
    join_agg_node(
        &fix.be.bank,
        &fix.pe.bank, &[],
        0,
        jt,
        keys,
        Vec::new(),
        None,
        None, Vec::new(), aggs.into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(None),
        0)
    .expect("construct agg")
}

fn pin(col: u32) -> Option<JoinOut> {
    Some(JoinOut { side: JoinSide::Probe, col })
}

fn bin(col: u32) -> Option<JoinOut> {
    Some(JoinOut { side: JoinSide::Build, col })
}

#[test]
fn join_agg_ungrouped_all_types() {
    let fix = fixture("agg1", true, 6000, 9000);
    for jt in ALL {
        let aggs: Vec<(JoinAggOp, Option<JoinOut>)> =
            if matches!(jt, JoinType::Semi | JoinType::Anti) {
                vec![
                    (JoinAggOp::CountStar, None),
                    (JoinAggOp::CountCol, pin(4)),
                    (JoinAggOp::Sum, pin(4)),
                    (JoinAggOp::Min, pin(1)),
                    (JoinAggOp::Max, pin(2)),
                ]
            } else {
                vec![
                    (JoinAggOp::CountStar, None),
                    (JoinAggOp::CountCol, pin(4)),
                    (JoinAggOp::CountCol, bin(3)),
                    (JoinAggOp::Sum, pin(4)),
                    (JoinAggOp::Sum, bin(4)),
                    (JoinAggOp::Min, pin(1)),
                    (JoinAggOp::Max, bin(4)),
                ]
            };
        let anode = agg_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], aggs);
        check_agg(&fix, &anode, &format!("agg-ungrouped {jt:?}"));
    }
}

#[test]
fn join_agg_text_key_count_star() {
    // pure count(*): the synthesized probe-key out; dup-heavy text key on
    // shared/disjoint probe dictionaries.
    let fix = fixture("aggt", false, 6000, 9000);
    for jt in ALL {
        let anode = agg_node(
            &fix,
            jt,
            vec![JoinKey { build_col: 3, probe_col: 3 }],
            vec![(JoinAggOp::CountStar, None)],
        );
        check_agg(&fix, &anode, &format!("agg-text-count {jt:?}"));
    }
}

#[test]
fn join_agg_quals_and_preds() {
    let fix = fixture("aggq", true, 5000, 7000);
    let bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 5, 40, TypMeta::INT8)]);
    let pp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 3, 18, TypMeta::INT4)]);
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Anti] {
        let aggs: Vec<(JoinAggOp, Option<JoinOut>)> = if matches!(jt, JoinType::Anti) {
            vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::Sum, pin(4)),
                (JoinAggOp::Min, pin(4)),
            ]
        } else {
            vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::Sum, pin(4)),
                (JoinAggOp::Sum, bin(4)),
                (JoinAggOp::Max, bin(4)),
            ]
        };
        let anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(), aggs.into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(None),
            0)
        .expect("construct agg");
        check_agg(&fix, &anode, &format!("agg-qual-pred {jt:?}"));
    }
}

#[test]
fn join_agg_empty_sides() {
    let fix = fixture("agge", true, 2000, 2000);
    let eb = empty_engine("ab");
    let ep = empty_engine("ap");
    let aggs = || {
        vec![
            (JoinAggOp::CountStar, None),
            (JoinAggOp::CountCol, pin(4)),
            (JoinAggOp::Sum, pin(4)),
            (JoinAggOp::Min, pin(1)),
        ]
    };
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti] {
        let anode = join_agg_node(
            &eb.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), aggs().into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(None),
            0)
        .expect("construct");
        let got = run_hash_join_agg(&eb.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
        let want = oracle_agg_answer(&anode, &oracle_join(&anode.join, &[], &fix.prows));
        assert_eq!(to_lines(&got), to_lines(&want), "agg-empty-build {jt:?}");
        let anode2 = join_agg_node(
            &fix.be.bank,
            &ep.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), aggs().into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(None),
            0)
        .expect("construct");
        let got2 = run_hash_join_agg(&fix.be.ctx(), &ep.ctx(), &[], &anode2).expect("run");
        let want2 = oracle_agg_answer(&anode2, &[]);
        assert_eq!(to_lines(&got2), to_lines(&want2), "agg-empty-probe {jt:?}");
    }
}

fn oracle_group_count(anode: &JoinAggNode, rows: &[Vec<RV>]) -> AnswerSet {
    let goi = anode.group_oi[0];
    let mut m: std::collections::BTreeMap<Option<i64>, i64> = std::collections::BTreeMap::new();
    for r in rows {
        *m.entry(r[goi].i()).or_insert(0) += 1;
    }
    let (mut keys, mut cnts, mut mask) = (Vec::new(), Vec::new(), Vec::new());
    for (k, c) in m.iter().filter(|(k, _)| k.is_some()) {
        keys.push(k.unwrap());
        cnts.push(*c);
        mask.push(true);
    }
    if let Some(c) = m.get(&None) {
        keys.push(0);
        cnts.push(*c);
        mask.push(false);
    }
    let kvalid = if mask.iter().all(|&x| x) {
        Validity::AllValid
    } else {
        Validity::Mask(mask)
    };
    AnswerSet::from_cols(vec![
        AnswerCol {
            ty: anode.join.out_tys[goi],
            data: ColData::I64(keys),
            validity: kvalid,
        },
        AnswerCol::i64s(anode.aggs[0].out, cnts),
    ])
}

#[test]
fn join_agg_grouped_count_probe_key() {
    let fix = fixture("aggg", true, 6000, 9000);
    for jt in ALL {
        // group by probe k (col 1, NULLs -> one NULL group) and by probe
        // g (col 2, null-free dense-ish domain).
        for gcol in [1u32, 2] {
            let anode = join_agg_node(
                &fix.be.bank,
                &fix.pe.bank, &[],
                0,
                jt,
                vec![JoinKey { build_col: 1, probe_col: 1 }],
                Vec::new(),
                None,
                None, Vec::new(), vec![(JoinAggOp::CountStar, None)].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(gcol)),
                0)
            .expect("construct grouped");
            let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode)
                .unwrap_or_else(|e| panic!("grouped {jt:?} g{gcol}: refused: {e}"));
            let want =
                oracle_group_count(&anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
            let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
            gl.sort();
            wl.sort();
            assert_eq!(gl, wl, "grouped-count {jt:?} g{gcol}");
        }
    }
}

/// Grouped-fold oracle: group the oracle's joined rows by the group-key
/// column, fold each agg row-at-a-time (independent control flow — no
/// cells, no combine); keys ascending, NULL group last. A fused HAVING
/// filters groups by the aggregate value recomputed row-at-a-time.
fn oracle_group_fold(anode: &JoinAggNode, rows: &[Vec<RV>]) -> AnswerSet {
    use sqe::ir::HvOp;
    let goi = anode.group_oi[0];
    let mut m: std::collections::BTreeMap<Option<i64>, Vec<&Vec<RV>>> =
        std::collections::BTreeMap::new();
    for r in rows {
        m.entry(r[goi].i()).or_default().push(r);
    }
    let mut order: Vec<Option<i64>> = m.keys().filter(|k| k.is_some()).cloned().collect();
    if m.contains_key(&None) {
        order.push(None);
    }
    if let Some(h) = &anode.having {
        let ai = h.agg as usize;
        let oi = anode.agg_oi[ai];
        order.retain(|k| {
            let rs = &m[k];
            let v: Option<i128> = match anode.aggs[ai].op {
                JoinAggOp::CountStar => Some(rs.len() as i128),
                JoinAggOp::CountCol => {
                    Some(rs.iter().filter(|r| r[oi] != RV::Null).count() as i128)
                }
                JoinAggOp::CountDistinct => Some(
                    rs.iter().filter_map(|r| r[oi].i()).collect::<std::collections::BTreeSet<_>>().len()
                        as i128,
                ),
                JoinAggOp::Sum => {
                    let (mut s, mut n) = (0i128, 0u64);
                    for r in rs {
                        if let Some(x) = r[oi].i() {
                            s += x as i128;
                            n += 1;
                        }
                    }
                    (n > 0).then_some(s)
                }
                JoinAggOp::Min => rs.iter().filter_map(|r| r[oi].i()).min().map(|x| x as i128),
                JoinAggOp::Max => rs.iter().filter_map(|r| r[oi].i()).max().map(|x| x as i128),
            };
            let Some(v) = v else { return false };
            let rhs = h.rhs as i128;
            match h.op {
                HvOp::Gt => v > rhs,
                HvOp::Ge => v >= rhs,
                HvOp::Lt => v < rhs,
                HvOp::Le => v <= rhs,
                HvOp::Eq => v == rhs,
                HvOp::Ne => v != rhs,
            }
        });
    }
    let keys: Vec<i64> = order.iter().map(|k| k.unwrap_or(0)).collect();
    let kmask: Vec<bool> = order.iter().map(|k| k.is_some()).collect();
    let kvalid = if kmask.iter().all(|&x| x) {
        Validity::AllValid
    } else {
        Validity::Mask(kmask)
    };
    let mut cols = vec![AnswerCol {
        ty: anode.join.out_tys[goi],
        data: ColData::I64(keys),
        validity: kvalid,
    }];
    for (ai, a) in anode.aggs.iter().enumerate() {
        let oi = anode.agg_oi[ai];
        cols.push(match a.op {
            JoinAggOp::CountStar => {
                AnswerCol::i64s(a.out, order.iter().map(|k| m[k].len() as i64).collect())
            }
            JoinAggOp::CountCol => AnswerCol::i64s(
                a.out,
                order
                    .iter()
                    .map(|k| m[k].iter().filter(|r| r[oi] != RV::Null).count() as i64)
                    .collect(),
            ),
            JoinAggOp::CountDistinct => AnswerCol::i64s(
                a.out,
                order
                    .iter()
                    .map(|k| {
                        m[k].iter()
                            .filter_map(|r| r[oi].i())
                            .collect::<std::collections::BTreeSet<_>>()
                            .len() as i64
                    })
                    .collect(),
            ),
            JoinAggOp::Sum => {
                let mut v: Vec<i128> = Vec::new();
                let mut mask: Vec<bool> = Vec::new();
                for k in &order {
                    let (mut s, mut n) = (0i128, 0u64);
                    for r in &m[k] {
                        if let Some(x) = r[oi].i() {
                            s += x as i128;
                            n += 1;
                        }
                    }
                    v.push(s);
                    mask.push(n > 0);
                }
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol { ty: a.out, data: ColData::I128(v), validity }
            }
            JoinAggOp::Min => AnswerCol::i64s_opt(
                a.out,
                order.iter().map(|k| m[k].iter().filter_map(|r| r[oi].i()).min()).collect(),
            ),
            JoinAggOp::Max => AnswerCol::i64s_opt(
                a.out,
                order.iter().map(|k| m[k].iter().filter_map(|r| r[oi].i()).max()).collect(),
            ),
        });
    }
    AnswerSet::from_cols(cols)
}

fn check_group_fold(fix: &Fix, anode: &JoinAggNode, what: &str) {
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], anode)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let want = oracle_group_fold(anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl, wl, "{what}: rendered identity");
}

#[test]
fn join_agg_grouped_folds_probe_key() {
    let fix = fixture("aggf", true, 6000, 9000);
    for jt in ALL {
        // group by probe k (col 1, NULL keys -> one NULL group) and by
        // probe g (col 2, null-free); fold inputs carry NULLs both sides
        // (probe v, build v, build t for count(col)).
        for gcol in [1u32, 2] {
            let aggs: Vec<(JoinAggOp, Option<JoinOut>)> =
                if matches!(jt, JoinType::Semi | JoinType::Anti) {
                    vec![
                        (JoinAggOp::CountStar, None),
                        (JoinAggOp::CountCol, pin(4)),
                        (JoinAggOp::Sum, pin(4)),
                        (JoinAggOp::Min, pin(4)),
                        (JoinAggOp::Max, pin(1)),
                    ]
                } else {
                    vec![
                        (JoinAggOp::CountStar, None),
                        (JoinAggOp::CountCol, bin(3)),
                        (JoinAggOp::Sum, pin(4)),
                        (JoinAggOp::Sum, bin(4)),
                        (JoinAggOp::Min, pin(4)),
                        (JoinAggOp::Max, bin(4)),
                    ]
                };
            let anode = join_agg_node(
                &fix.be.bank,
                &fix.pe.bank, &[],
                0,
                jt,
                vec![JoinKey { build_col: 1, probe_col: 1 }],
                Vec::new(),
                None,
                None, Vec::new(), aggs.into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(gcol)),
                0)
            .expect("construct grouped fold");
            check_group_fold(&fix, &anode, &format!("grouped-fold {jt:?} g{gcol}"));
        }
    }
}

#[test]
fn join_agg_grouped_fold_quals_and_preds() {
    let fix = fixture("aggfq", true, 5000, 7000);
    let bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 5, 40, TypMeta::INT8)]);
    let pp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 3, 18, TypMeta::INT4)]);
    for jt in [JoinType::Inner, JoinType::Left] {
        let anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
            Some(bp.clone()),
            Some(pp.clone()), Vec::new(), vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::Sum, bin(4)),
                (JoinAggOp::Min, pin(4)),
                (JoinAggOp::Max, bin(4)),
            ].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
            0)
        .expect("construct grouped fold");
        check_group_fold(&fix, &anode, &format!("grouped-fold-qual-pred {jt:?}"));
    }
}

#[test]
fn join_agg_grouped_fold_empty_sides() {
    let fix = fixture("aggfe", true, 2000, 2000);
    let eb = empty_engine("gfb");
    let ep = empty_engine("gfp");
    let aggs = || {
        vec![
            (JoinAggOp::CountStar, None),
            (JoinAggOp::Sum, pin(4)),
            (JoinAggOp::Min, pin(4)),
        ]
    };
    for jt in ALL {
        let anode = join_agg_node(
            &eb.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), aggs().into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
            0)
        .expect("construct");
        let got = run_hash_join_agg(&eb.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
        let want = oracle_group_fold(&anode, &oracle_join(&anode.join, &[], &fix.prows));
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "grouped-fold-empty-build {jt:?}");
        let anode2 = join_agg_node(
            &fix.be.bank,
            &ep.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), aggs().into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
            0)
        .expect("construct");
        let got2 = run_hash_join_agg(&fix.be.ctx(), &ep.ctx(), &[], &anode2).expect("run");
        assert_eq!(got2.nrows, 0, "grouped-fold-empty-probe {jt:?}");
    }
}

#[test]
fn join_agg_grouped_fold_overflow_adjacent() {
    // i64-overflow-adjacent fold inputs: per-group sums leave the i64
    // domain (exact i128 cells); min/max at the extremes.
    let base = std::env::temp_dir().join(format!("sqe_join_ovf_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let big = i64::MAX - 3;
    let brows: Vec<Row> = (0..3000u64)
        .map(|i| {
            let v = if i % 13 == 0 {
                RV::Null
            } else if i % 2 == 0 {
                RV::I(big - (i % 7) as i64)
            } else {
                RV::I(-big + (i % 5) as i64)
            };
            [
                RV::I((i % 3) as i64),
                RV::I((i % 4) as i64),
                RV::T(b"d".to_vec()),
                v,
                RV::T(b"z".to_vec()),
            ]
        })
        .collect();
    let prows: Vec<Row> = (0..800u64)
        .map(|i| {
            let k = if i % 11 == 0 { RV::Null } else { RV::I((i % 5) as i64) };
            let v = if i % 9 == 0 { RV::Null } else { RV::I(big - (i % 3) as i64) };
            [k, RV::I((i % 4) as i64), RV::T(b"d".to_vec()), v, RV::T(b"y".to_vec())]
        })
        .collect();
    let (bd, pd) = (base.join("b"), base.join("p"));
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    let fix = Fix {
        be: open_engine(bd.to_str().unwrap()),
        pe: open_engine(pd.to_str().unwrap()),
        brows,
        prows,
    };
    for jt in [JoinType::Inner, JoinType::Left] {
        let anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::Sum, pin(4)),
                (JoinAggOp::Sum, bin(4)),
                (JoinAggOp::Min, bin(4)),
                (JoinAggOp::Max, pin(4)),
            ].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
            0)
        .expect("construct grouped fold");
        check_group_fold(&fix, &anode, &format!("grouped-fold-overflow {jt:?}"));
    }
}

#[test]
fn join_agg_refusals_typed() {
    let fix = fixture("aggr", true, 100, 100);
    let (b, p) = (&fix.be.bank, &fix.pe.bank);
    let k = || vec![JoinKey { build_col: 1, probe_col: 1 }];
    let mk = |jt, keys, aggs: Vec<(JoinAggOp, Option<JoinOut>)>, group: Option<JoinOut>| {
        join_agg_node(b, p, &[], 0, jt, keys, Vec::new(), None, None, Vec::new(), aggs.into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(group), 0)
    };
    assert!(matches!(
        mk(JoinType::Inner, k(), Vec::new(), None),
        Err(JoinRefuse::Unsupported { what: "agg-none" })
    ));
    // [sqe-mech3] build-side and text group keys now CONSTRUCT (the
    // composite grouped sink serves them); semi/anti still pin the
    // probe side (the build side null-extends/never materializes).
    assert!(mk(JoinType::Inner, k(), vec![(JoinAggOp::CountStar, None)], bin(2)).is_ok());
    assert!(mk(JoinType::Inner, k(), vec![(JoinAggOp::CountStar, None)], pin(3)).is_ok());
    assert!(matches!(
        mk(JoinType::Semi, k(), vec![(JoinAggOp::CountStar, None)], bin(2)),
        Err(JoinRefuse::Unsupported { what: "agg-group-side" })
    ));
    // grouped folds construct now (sqe-grpfold); grouped input-face
    // refusals stay typed (float/varlena fold inputs never word-embed).
    assert!(mk(JoinType::Inner, k(), vec![(JoinAggOp::Sum, pin(4))], pin(2)).is_ok());
    assert!(matches!(
        mk(JoinType::Inner, k(), vec![(JoinAggOp::Min, pin(3))], pin(2)),
        Err(JoinRefuse::Unsupported { what: "agg-input-face" })
    ));
    assert!(matches!(
        mk(JoinType::Inner, k(), vec![(JoinAggOp::CountStar, pin(4))], None),
        Err(JoinRefuse::Unsupported { what: "agg-count-star-input" })
    ));
    assert!(matches!(
        mk(JoinType::Inner, k(), vec![(JoinAggOp::Sum, None)], None),
        Err(JoinRefuse::Unsupported { what: "agg-missing-input" })
    ));
    assert!(matches!(
        mk(JoinType::Inner, k(), vec![(JoinAggOp::Min, pin(3))], None),
        Err(JoinRefuse::Unsupported { what: "agg-input-face" })
    ));
    assert!(matches!(
        mk(JoinType::Semi, k(), vec![(JoinAggOp::Sum, bin(4))], None),
        Err(JoinRefuse::Unsupported { what: "semi-anti-build-out" })
    ));
    assert!(matches!(
        mk(JoinType::Inner, Vec::new(), vec![(JoinAggOp::CountStar, None)], None),
        Err(JoinRefuse::Unsupported { what: "no-equi-key" })
    ));
}

/// Directional throughput witness, not a gate (CI cluster numbers are the
/// reportable ones). `--ignored` to run.
#[test]
#[ignore]
fn join_throughput_directional() {
    let fix = fixture("perf", false, 100_000, 400_000);
    let node = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1, 2], &[4]),
    );
    for _ in 0..2 {
        run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("warm");
    }
    let t0 = std::time::Instant::now();
    let reps = 5;
    let mut rows_out = 0usize;
    for _ in 0..reps {
        rows_out = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("run").nrows;
    }
    let dt = t0.elapsed().as_secs_f64() / reps as f64;
    println!(
        "join_throughput: build=100000 probe=400000 out={} {:.1} Mprobe-rows/s ({:.2} ms/rep)",
        rows_out,
        400_000.0 / dt / 1e6,
        dt * 1e3
    );
}

// ---------------------------------------------------------------------------
// [sqe-tpch-mech] mechanism gates: direct-array grouped fold (A/B vs the
// hash arm + oracle), fused HAVING over the join, the overflow-audit
// fallback, and the agg-result-as-set consumption hook.
// ---------------------------------------------------------------------------

use sqe::ir::{HavingCmp, HvOp};
use sqe::joins::InSetFilter;
use std::sync::Arc as StdArc;

fn open_engine_arm(dir: &str, direct: bool) -> Engine {
    let bank = Bank::open(dir, schema_meta(), &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 3, direct_array: direct, ..SqeConfig::default() })
}

/// Direct-array vs hash GroupFold identity (nullable + dense group keys,
/// fold mixes both sides), with and without a fused HAVING, plus the
/// independent oracle.
#[test]
fn join_agg_grouped_fold_direct_array_ab() {
    let fix = fixture("dnab", true, 6000, 9000);
    let pe_hash = open_engine_arm(&fix.pe.bank.dir, false);
    assert!(fix.pe.faces.cfg.direct_array, "arm A must elect the direct array");
    for gcol in [1u32, 2] {
        for having in [None, Some(HavingCmp { agg: 1, op: HvOp::Gt, rhs: 800 })] {
            let mut anode = join_agg_node(
                &fix.be.bank,
                &fix.pe.bank, &[],
                0,
                JoinType::Inner,
                vec![JoinKey { build_col: 1, probe_col: 1 }],
                Vec::new(),
                None,
                None, Vec::new(), vec![
                    (JoinAggOp::CountStar, None),
                    (JoinAggOp::Sum, pin(4)),
                    (JoinAggOp::Min, pin(4)),
                    (JoinAggOp::Max, bin(4)),
                ].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(gcol)),
                0)
            .expect("construct grouped fold");
            anode.having = having;
            let a = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("dense arm");
            let b = run_hash_join_agg(&fix.be.ctx(), &pe_hash.ctx(), &[], &anode).expect("hash arm");
            let (mut al, mut bl) = (to_lines(&a), to_lines(&b));
            al.sort();
            bl.sort();
            assert_eq!(al, bl, "direct-array vs hash arm g{gcol} having={having:?}");
            let want = oracle_group_fold(&anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
            let mut wl = to_lines(&want);
            wl.sort();
            assert_eq!(al, wl, "direct-array vs oracle g{gcol} having={having:?}");
        }
    }
}

/// Fused HAVING over the join grouped fold: boundary constants, count(*)
/// legs, hidden-agg-free empty survivor sets — vs the oracle's row-law
/// filter on both arms.
#[test]
fn join_agg_having_legs() {
    let fix = fixture("jhav", true, 5000, 7000);
    let pe_hash = open_engine_arm(&fix.pe.bank.dir, false);
    let cases: Vec<HavingCmp> = vec![
        HavingCmp { agg: 0, op: HvOp::Gt, rhs: 50 },
        HavingCmp { agg: 0, op: HvOp::Le, rhs: 3 },
        HavingCmp { agg: 1, op: HvOp::Ge, rhs: 500 },
        HavingCmp { agg: 2, op: HvOp::Lt, rhs: 5 },
        HavingCmp { agg: 1, op: HvOp::Gt, rhs: i64::MAX / 2 }, // empty survivors
    ];
    for h in cases {
        let mut anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(), vec![
                (JoinAggOp::CountStar, None),
                (JoinAggOp::Sum, pin(4)),
                (JoinAggOp::Min, bin(4)),
            ].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
            0)
        .expect("construct");
        anode.having = Some(h);
        let a = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("dense arm");
        let b = run_hash_join_agg(&fix.be.ctx(), &pe_hash.ctx(), &[], &anode).expect("hash arm");
        let (mut al, mut bl) = (to_lines(&a), to_lines(&b));
        al.sort();
        bl.sort();
        assert_eq!(al, bl, "having arms identity {h:?}");
        let want = oracle_group_fold(&anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
        let mut wl = to_lines(&want);
        wl.sort();
        assert_eq!(al, wl, "having vs oracle {h:?}");
    }
}

/// Overflow-adjacent Sum inputs over a dense-witnessable group key: the
/// direct arm's post-run audit must fall back to the hash arm — answers
/// identical to the oracle either way.
#[test]
fn join_agg_dense_overflow_audit_falls_back() {
    let base = std::env::temp_dir().join(format!("sqe_join_dovf_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let big = i64::MAX - 3;
    let brows: Vec<Row> = (0..3000u64)
        .map(|i| {
            let v = if i % 2 == 0 { RV::I(big - (i % 7) as i64) } else { RV::I(-big + (i % 5) as i64) };
            [RV::I((i % 3) as i64), RV::I((i % 4) as i64), RV::T(b"d".to_vec()), v, RV::T(b"z".to_vec())]
        })
        .collect();
    let prows: Vec<Row> = (0..800u64)
        .map(|i| {
            let k = if i % 11 == 0 { RV::Null } else { RV::I((i % 5) as i64) };
            [k, RV::I((i % 4) as i64), RV::T(b"d".to_vec()), RV::I(i as i64), RV::T(b"y".to_vec())]
        })
        .collect();
    let (bd, pd) = (base.join("b"), base.join("p"));
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    let fix = Fix {
        be: open_engine(bd.to_str().unwrap()),
        pe: open_engine(pd.to_str().unwrap()),
        brows,
        prows,
    };
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank, &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None, Vec::new(), vec![(JoinAggOp::CountStar, None), (JoinAggOp::Sum, bin(4)), (JoinAggOp::Max, pin(4))].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(pin(1)),
        0)
    .expect("construct");
    check_group_fold(&fix, &anode, "dense-overflow-audit-fallback");
}

/// [sqe-hugedom] Huge witnessed probe-key domain (20M-wide, ~2k keys
/// occupied) under a fused HAVING: the answer is BOUNDED, so the dense
/// election prices under the bounded (occupancy) budget — over the
/// unbounded budget, inside the bounded one, verified through the
/// planner's own election fn — and the dense arm answers byte-identical
/// to the hash arm and the oracle (sparse-occupancy sweep edge, empty
/// survivor set included).
#[test]
fn join_agg_hugedom_having_dense_ab() {
    let base = std::env::temp_dir().join(format!("sqe_join_hugedom_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    const DOM: i64 = 20_000_000;
    let brows: Vec<Row> = (0..3000u64)
        .map(|i| {
            let k = (i as i64 * 10_007) % DOM; // sparse witnessed domain
            [RV::I(k), RV::I((i % 16) as i64), RV::T(b"d".to_vec()), RV::I((i % 40) as i64), RV::T(b"z".to_vec())]
        })
        .collect();
    let prows: Vec<Row> = (0..9000u64)
        .map(|i| {
            // Overlapping sparse keys incl. domain endpoints (witness =
            // exact part min/max spans the full 20M range).
            let k = if i == 0 { 0 } else if i == 1 { DOM - 1 } else { (i as i64 % 2500) * 10_007 % DOM };
            let v = if i % 9 == 0 { RV::Null } else { RV::I((i % 60) as i64) };
            [RV::I(k), RV::I((i % 24) as i64), RV::T(b"d".to_vec()), v, RV::T(b"y".to_vec())]
        })
        .collect();
    let (bd, pd) = (base.join("b"), base.join("p"));
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    let fix = Fix {
        be: open_engine(bd.to_str().unwrap()),
        pe: open_engine(pd.to_str().unwrap()),
        brows,
        prows,
    };
    let pe_hash = open_engine_arm(&fix.pe.bank.dir, false);
    // The election inputs: the 20M-wide witnessed domain at one Sum
    // fold lane (24B/slot = 480MB) is OVER the unbounded budget and
    // INSIDE the bounded band.
    use sqe::planner::{direct_array_bytes_cap, direct_array_domain_capped};
    assert!(
        direct_array_domain_capped(&fix.pe.bank, &fix.pe.faces, 1, 1, direct_array_bytes_cap(false))
            .is_none(),
        "20M domain must exceed the unbounded direct budget"
    );
    let dom = direct_array_domain_capped(
        &fix.pe.bank,
        &fix.pe.faces,
        1,
        1,
        direct_array_bytes_cap(true),
    )
    .expect("20M domain must sit inside the bounded budget");
    assert!(dom.1 as i64 >= DOM, "witnessed domain spans the endpoints");
    for (rhs, tag) in [(2i64, "survivors"), (i64::MAX / 2, "empty-survivors")] {
        let mut anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank, &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None, Vec::new(),
            vec![(JoinAggOp::CountStar, None), (JoinAggOp::Sum, pin(4))]
                .into_iter()
                .map(|(op, input)| JoinAggReq::col(op, input))
                .collect(),
            Vec::from_iter(pin(1)),
            0)
        .expect("construct hugedom grouped fold");
        anode.having = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs });
        let a = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("dense arm");
        let b = run_hash_join_agg(&fix.be.ctx(), &pe_hash.ctx(), &[], &anode).expect("hash arm");
        let (mut al, mut bl) = (to_lines(&a), to_lines(&b));
        al.sort();
        bl.sort();
        assert_eq!(al, bl, "hugedom having {tag}: dense vs hash identity");
        let want = oracle_group_fold(&anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
        let mut wl = to_lines(&want);
        wl.sort();
        assert_eq!(al, wl, "hugedom having {tag}: vs oracle");
        if tag == "empty-survivors" {
            assert!(al.is_empty(), "boundary rhs must empty the survivor set");
        } else {
            assert!(!al.is_empty(), "fixture must keep survivors");
        }
    }
}

/// The agg-result-as-set consumption hook: a survivor key set filters
/// one side's rows ahead of the join (probe and build), byte-identical
/// to the oracle's IN filter; a non-key set column refuses typed.
#[test]
fn join_right_outer_row_goal() {
    let fix = fixture("rightout", true, 6000, 9000);
    let node = base_node(
        &fix,
        JoinType::Right,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1, 2], &[4, 5]),
    );
    check(&fix, &node, "right-outer row goal");
}

#[test]
fn join_right_outer_sparse_probe_side() {
    let fix = fixture("rightoute", true, 500, 3);
    let node = base_node(
        &fix,
        JoinType::Right,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[2], &[1, 4]),
    );
    check(&fix, &node, "right-outer empty probe");
}

/// [subset] the negated filter's 3VL laws on a non-key nullable lane.
#[test]
fn join_set_filter_negate_laws() {
    let fix = fixture("negset", true, 5000, 8000);
    let keys: StdArc<Vec<i64>> = StdArc::new(vec![3, 7, 21, 40]);
    let jk = || vec![JoinKey { build_col: 1, probe_col: 1 }];
    let filt = |negate: bool, unknown_false: bool, set_null: bool, ks: &StdArc<Vec<i64>>| {
        InSetFilter {
            col: 4,
            keys: StdArc::clone(ks),
            negate,
            unknown_false,
            set_null,
        }
    };
    let want_for = |keep: &dyn Fn(&Row) -> bool, node: &JoinNode| {
        let pfilt: Vec<Row> = fix.prows.iter().filter(|r| keep(r)).cloned().collect();
        oracle_answer(node, oracle_join(node, &fix.brows, &pfilt))
    };
    let run_law = |f: InSetFilter, keep: &dyn Fn(&Row) -> bool, what: &str| {
        let mut node = base_node(&fix, JoinType::Inner, jk(), out_pb(&[1, 4], &[4]));
        node.probe_in = Some(f);
        let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node)
            .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
        let want = want_for(keep, &node);
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "{what}");
    };
    let member = |r: &Row| r[3].i().is_some_and(|v| keys.binary_search(&v).is_ok());
    let valid = |r: &Row| r[3].i().is_some();
    run_law(filt(true, false, false, &keys), &|r| valid(r) && !member(r), "negate general");
    run_law(filt(true, false, true, &keys), &|_| false, "negate set-null");
    let empty: StdArc<Vec<i64>> = StdArc::new(Vec::new());
    run_law(filt(true, false, false, &empty), &|_| true, "negate empty set");
    run_law(filt(true, true, false, &keys), &|r| !(valid(r) && member(r)), "negate unknown-false");
    run_law(filt(false, false, false, &keys), &|r| member(r), "member unchanged");
}

#[test]
fn join_in_set_consumption() {
    let fix = fixture("inset", true, 5000, 8000);
    let keys: StdArc<Vec<i64>> = StdArc::new(vec![3, 7, 21, 40, 97, 120]);
    // probe-side consumption
    let mut node = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1, 2], &[4]),
    );
    node.probe_in = Some(InSetFilter::member(1, StdArc::clone(&keys)));
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("probe in-set");
    let pfilt: Vec<Row> = fix
        .prows
        .iter()
        .filter(|r| r[0].i().map(|k| keys.binary_search(&k).is_ok()).unwrap_or(false))
        .cloned()
        .collect();
    let want = oracle_answer(&node, oracle_join(&node, &fix.brows, &pfilt));
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl, wl, "probe-side in-set consumption");
    // build-side consumption
    let mut node2 = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1, 2], &[4]),
    );
    node2.build_in = Some(InSetFilter::member(1, StdArc::clone(&keys)));
    let got2 = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node2).expect("build in-set");
    let bfilt: Vec<Row> = fix
        .brows
        .iter()
        .filter(|r| r[0].i().map(|k| keys.binary_search(&k).is_ok()).unwrap_or(false))
        .cloned()
        .collect();
    let want2 = oracle_answer(&node2, oracle_join(&node2, &bfilt, &fix.prows));
    let (mut gl2, mut wl2) = (to_lines(&got2), to_lines(&want2));
    gl2.sort();
    wl2.sort();
    assert_eq!(gl2, wl2, "build-side in-set consumption");
    // [subset] wave: an in-set over any PLANNED word column serves (the
    // pextra lane plans it); col 4 (`v`) is planned via the out list.
    let mut node3 = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1], &[4]),
    );
    node3.probe_in = Some(InSetFilter::member(4, StdArc::clone(&keys)));
    let got3 = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node3).expect("planned-col in-set");
    let pfilt3: Vec<Row> = fix
        .prows
        .iter()
        .filter(|r| r[3].i().map(|k| keys.binary_search(&k).is_ok()).unwrap_or(false))
        .cloned()
        .collect();
    let want3 = oracle_answer(&node3, oracle_join(&node3, &fix.brows, &pfilt3));
    let (mut gl3, mut wl3) = (to_lines(&got3), to_lines(&want3));
    gl3.sort();
    wl3.sort();
    assert_eq!(gl3, wl3, "planned-column in-set consumption");
    // the SURVIVING typed refusal: an in-set over a TEXT column (col 3, `t`)
    let mut node4 = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1], &[3]),
    );
    node4.probe_in = Some(InSetFilter::member(3, keys));
    match run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node4) {
        Err(JoinRefuse::Unsupported { what: "in-set-col-text" }) => {}
        other => panic!("expected in-set-col-text refusal, got {other:?}"),
    }
}

/// End-to-end result-as-set composition (engine side): a fused-HAVING
/// grouped fold over the probe bank produces the survivor key set, the
/// set feeds the join's probe filter — vs the oracle's HAVING-derived IN
/// filter. (The SQL-level 3-level Q18 composition stays a typed seam
/// refusal; this gate proves the engine-side pieces compose.)
#[test]
fn join_having_set_composition() {
    use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr};
    let fix = fixture("comp", true, 4000, 8000);
    // producer: sum(v) grouped by k over the PROBE bank, HAVING sum > K
    let ap = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols: vec![1, 4],
        pred: None,
        group: vec![AKeyExpr::Col(1)],
        agg: vec![AAgg::Sum { e: AValExpr::Col(4) }],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: String::new(),
        flags: Vec::new(),
    };
    let mut gnode = plan_from_ap(&fix.pe.bank, &fix.pe.faces, &ap).expect("lower producer");
    gnode.params.having = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs: 1200 });
    let ganswer = fix.pe.run(&gnode);
    let set = StdArc::new(sqe::answer::survivor_keyset(&ganswer));
    // oracle producer: same set from the row law
    let mut om: std::collections::BTreeMap<i64, i128> = Default::default();
    for r in &fix.prows {
        if let (Some(k), Some(v)) = (r[0].i(), r[3].i()) {
            *om.entry(k).or_insert(0) += v as i128;
        }
    }
    let oset: Vec<i64> = om.iter().filter(|(_, &s)| s > 1200).map(|(k, _)| *k).collect();
    assert_eq!(*set, oset, "survivor keyset vs oracle producer");
    assert!(!set.is_empty(), "fixture must keep survivors");
    // consumer: the join filtered by the set
    let mut node = base_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        out_pb(&[1, 4], &[4]),
    );
    node.probe_in = Some(InSetFilter::member(1, StdArc::clone(&set)));
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("composition");
    let pfilt: Vec<Row> = fix
        .prows
        .iter()
        .filter(|r| r[0].i().map(|k| set.binary_search(&k).is_ok()).unwrap_or(false))
        .cloned()
        .collect();
    let want = oracle_answer(&node, oracle_join(&node, &fix.brows, &pfilt));
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl, wl, "having-set composition vs oracle");
}

// ---------------------------------------------------------------------------
// [sqe-mech3] 3-way dim composition + composite grouped folds + arith
// ---------------------------------------------------------------------------

use sqe::joins::{DimKey, DimSrc, DimStage, JoinArith, TextEqTerm};

/// Dim rows: overlapping small int domain in col1 (dense-electable),
/// a second key col2, word payload col4 with NULLs, text payload col5,
/// NULLs in the key.
fn dim_rows(n: u64) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let k = if i % 11 == 0 { RV::Null } else { RV::I((i % 23) as i64) };
            let g = RV::I((i % 8) as i64);
            let t = RV::T(format!("d{}", i % 5).into_bytes());
            let v = if i % 7 == 0 { RV::Null } else { RV::I((i % 40) as i64) };
            let s = RV::T(format!("s{}", i % 7).into_bytes());
            [k, g, t, v, s]
        })
        .collect()
}

struct Fix3 {
    be: Engine,
    pe: Engine,
    de: Engine,
    brows: Vec<Row>,
    prows: Vec<Row>,
    drows: Vec<Row>,
}

fn fixture3(tag: &str, nb: u64, np: u64, nd: u64) -> Fix3 {
    let base = std::env::temp_dir().join(format!("sqe_join3_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let (bd, pd, dd) = (base.join("b"), base.join("p"), base.join("d"));
    let brows = build_rows(nb, true);
    let prows = probe_rows(np);
    let drows = dim_rows(nd);
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    seal(dd.to_str().unwrap(), &drows);
    Fix3 {
        be: open_engine(bd.to_str().unwrap()),
        pe: open_engine(pd.to_str().unwrap()),
        de: open_engine(dd.to_str().unwrap()),
        brows,
        prows,
        drows,
    }
}

fn o_dim_pass(stage: &DimStage, row: &Row) -> bool {
    o_pred(&stage.pred, row)
        && stage
            .text_eqs
            .iter()
            .all(|t| row[(t.col - 1) as usize].t() == Some(&t.bytes[..]))
}

/// Oracle for the dim-composed join: expand each surviving build row by
/// its dim matches (inner semantics, NULL keys never match), then the
/// plain nested-loop probe; Dim outs project from the matched dim rows.
fn oracle_join3(node: &JoinNode, brows: &[Row], prows: &[Row], dims: &[&[Row]]) -> Vec<Vec<RV>> {
    // (build row, one dim row index per stage)
    let mut ext: Vec<(&Row, Vec<&Row>)> = Vec::new();
    'b: for b in brows.iter().filter(|r| o_pred(&node.build_pred, r)) {
        // [sqe-join-depth] per-combination stage matching: a chained key
        // (src = Dim(j)) compares against the combo's stage-j row, so
        // branches prune independently; the build row survives iff at
        // least one FULL combination exists.
        let mut combos: Vec<Vec<&Row>> = vec![Vec::new()];
        for (di, stage) in node.dims.iter().enumerate() {
            let mut next: Vec<Vec<&Row>> = Vec::new();
            for c in &combos {
                for d in dims[di].iter().filter(|d| o_dim_pass(stage, d)) {
                    let all = stage.keys.iter().all(|k| {
                        let src_row: &Row = match k.src {
                            DimSrc::Build => b,
                            DimSrc::Dim(j) => c[j as usize],
                        };
                        match (&src_row[(k.build_col - 1) as usize], &d[(k.dim_col - 1) as usize])
                        {
                            (RV::I(x), RV::I(y)) => x == y,
                            _ => false,
                        }
                    });
                    if all {
                        let mut c2 = c.clone();
                        c2.push(d);
                        next.push(c2);
                    }
                }
            }
            combos = next;
            if combos.is_empty() {
                continue 'b;
            }
        }
        for c in combos {
            ext.push((b, c));
        }
    }
    let project = |p: &Row, bd: Option<&(&Row, Vec<&Row>)>| -> Vec<RV> {
        node.out
            .iter()
            .map(|o| match o.side {
                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                JoinSide::Build => match bd {
                    Some((b, _)) => b[(o.col - 1) as usize].clone(),
                    None => RV::Null,
                },
                JoinSide::Dim(di) => match bd {
                    Some((_, ds)) => ds[di as usize][(o.col - 1) as usize].clone(),
                    None => RV::Null,
                },
            })
            .collect()
    };
    let mut out: Vec<Vec<RV>> = Vec::new();
    for p in prows.iter().filter(|r| o_pred(&node.probe_pred, r)) {
        let mut matched = false;
        for e in &ext {
            if o_keys_eq(node, e.0, p) && o_quals(node, e.0, p) {
                matched = true;
                match node.join_type {
                    JoinType::Inner | JoinType::Left => out.push(project(p, Some(e))),
                    JoinType::Semi => {
                        out.push(project(p, None));
                        break;
                    }
                    JoinType::Anti => break,
                    JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                        unreachable!("3-way oracle sees no right classes")
                    }
                }
            }
        }
        if !matched && matches!(node.join_type, JoinType::Left | JoinType::Anti) {
            out.push(project(p, None));
        }
    }
    out
}

fn check3(fix: &Fix3, node: &JoinNode, what: &str) {
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[&fix.de.ctx()], node)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let want =
        oracle_answer(node, oracle_join3(node, &fix.brows, &fix.prows, &[&fix.drows]));
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl.len(), wl.len(), "{what}: row count {} vs oracle {}", got.nrows, want.nrows);
    assert_eq!(gl, wl, "{what}: rendered identity");
}

fn dim_stage(keys: Vec<DimKey>, pred: Option<PredSpec>, text_eqs: Vec<TextEqTerm>) -> DimStage {
    DimStage { keys, pred, text_eqs, rows_hint: 0 }
}

fn node3(
    fix: &Fix3,
    jt: JoinType,
    keys: Vec<JoinKey>,
    dims: Vec<DimStage>,
    out: Vec<JoinOut>,
) -> JoinNode {
    join_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[&fix.de.bank],
        0,
        jt,
        keys,
        Vec::new(),
        None,
        None,
        dims,
        out,
        fix.be.bank.rows_total(),
        false,
    )
    .expect("construct 3-way")
}

fn dout(col: u32) -> JoinOut {
    JoinOut { side: JoinSide::Dim(0), col }
}

#[test]
fn join3_dim_composition_row_goal() {
    let fix = fixture3("row", 4000, 6000, 400);
    // dense arm: single word key col2(build g) = col1(dim k), payload
    // word col4 + text col5, dim word pred + text byte-eq.
    let pred = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 1, 6, TypMeta::INT4)]);
    for jt in [JoinType::Inner, JoinType::Left] {
        let node = node3(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![dim_stage(
                vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }],
                Some(pred.clone()),
                vec![TextEqTerm { col: 5, bytes: b"s3".to_vec() }],
            )],
            vec![
                JoinOut { side: JoinSide::Probe, col: 1 },
                JoinOut { side: JoinSide::Probe, col: 5 },
                JoinOut { side: JoinSide::Build, col: 4 },
                dout(4),
                dout(5),
            ],
        );
        check3(&fix, &node, &format!("dim-row {jt:?}"));
    }
    // semi/anti: probe outs only (dim outs refuse by family law).
    for jt in [JoinType::Semi, JoinType::Anti] {
        let node = node3(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new())],
            vec![JoinOut { side: JoinSide::Probe, col: 1 }],
        );
        check3(&fix, &node, &format!("dim-row {jt:?}"));
    }
}

/// Dim-stage varlena conjunct (LIKE over the dim text payload) composed
/// with a word pred + text byte-eq — the same per-side law at dim grain.
#[test]
fn join3_dim_var_pred() {
    let fix = fixture3("dimvar", 4000, 6000, 400);
    let mut pred = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 1, 6, TypMeta::INT4)]);
    pred.var_terms = vec![VarPredTerm::new(3, VarOp::NotLike, b"d3%".to_vec(), fix.de.bank.typ(3))];
    for jt in [JoinType::Inner, JoinType::Left] {
        let node = node3(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![dim_stage(
                vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }],
                Some(pred.clone()),
                vec![TextEqTerm { col: 5, bytes: b"s3".to_vec() }],
            )],
            vec![
                JoinOut { side: JoinSide::Probe, col: 1 },
                JoinOut { side: JoinSide::Build, col: 4 },
                dout(4),
                dout(3),
            ],
        );
        check3(&fix, &node, &format!("dim-var-pred {jt:?}"));
    }
}

#[test]
fn join3_dim_map_arm_multikey() {
    // two dim keys -> the OA-map arm (no single-key dense election).
    let fix = fixture3("map", 3000, 5000, 300);
    let node = node3(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![dim_stage(
            vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }, DimKey { dim_col: 2, build_col: 2, src: DimSrc::Build }],
            None,
            Vec::new(),
        )],
        vec![JoinOut { side: JoinSide::Probe, col: 1 }, dout(5), dout(4)],
    );
    check3(&fix, &node, "dim-map-multikey");
}

#[test]
fn join3_empty_dim() {
    let fix = fixture3("edim", 500, 500, 200);
    let ed = empty_engine("dim");
    let node = join_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[&ed.bank],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        vec![dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new())],
        vec![JoinOut { side: JoinSide::Probe, col: 1 }, dout(4)],
        0,
        false,
    )
    .expect("construct");
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[&ed.ctx()], &node).expect("run");
    assert_eq!(got.nrows, 0, "empty dim answers the empty inner join");
}

// ---------------------------------------------------------------------------
// [sqe-join-depth] N staged builds + chained dim keys
// ---------------------------------------------------------------------------

struct FixN {
    be: Engine,
    pe: Engine,
    des: Vec<Engine>,
    brows: Vec<Row>,
    prows: Vec<Row>,
    drows: Vec<Vec<Row>>,
}

fn fixture_n(tag: &str, nb: u64, np: u64, nds: &[u64]) -> FixN {
    let base = std::env::temp_dir().join(format!("sqe_joinN_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let (bd, pd) = (base.join("b"), base.join("p"));
    let brows = build_rows(nb, true);
    let prows = probe_rows(np);
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    let mut des = Vec::new();
    let mut drows = Vec::new();
    for (i, &nd) in nds.iter().enumerate() {
        let dd = base.join(format!("d{i}"));
        let rows = dim_rows(nd);
        seal(dd.to_str().unwrap(), &rows);
        des.push(open_engine(dd.to_str().unwrap()));
        drows.push(rows);
    }
    FixN { be: open_engine(bd.to_str().unwrap()), pe: open_engine(pd.to_str().unwrap()), des, drows, brows, prows }
}

fn check_n(fix: &FixN, node: &JoinNode, what: &str) {
    let dctxs: Vec<_> = fix.des.iter().map(|e| e.ctx()).collect();
    let drefs: Vec<&_> = dctxs.iter().collect();
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &drefs, node)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let dslices: Vec<&[Row]> = fix.drows.iter().map(|v| &v[..]).collect();
    let want = oracle_answer(node, oracle_join3(node, &fix.brows, &fix.prows, &dslices));
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl.len(), wl.len(), "{what}: row count {} vs oracle {}", got.nrows, want.nrows);
    assert_eq!(gl, wl, "{what}: rendered identity");
}

fn node_n(
    fix: &FixN,
    jt: JoinType,
    keys: Vec<JoinKey>,
    dims: Vec<DimStage>,
    out: Vec<JoinOut>,
) -> Result<JoinNode, sqe::joins::JoinRefuse> {
    let dbanks: Vec<&_> = fix.des.iter().map(|e| &e.bank).collect();
    join_node(
        &fix.be.bank,
        &fix.pe.bank,
        &dbanks,
        0,
        jt,
        keys,
        Vec::new(),
        None,
        None,
        dims,
        out,
        fix.be.bank.rows_total(),
        false,
    )
}

fn dko(di: u8, col: u32) -> JoinOut {
    JoinOut { side: JoinSide::Dim(di), col }
}

/// 5-way tree: probe ⋈ build, dim0 flat (build.g), dim1 CHAINED off
/// dim0.g, dim2 CHAINED off dim1's NULLABLE v (a NULL parent payload
/// prunes the branch, never the row) — row goal, Inner + Left.
#[test]
fn join_depth_chained_row_goal() {
    let fix = fixture_n("chain", 1500, 2500, &[200, 120, 90]);
    for jt in [JoinType::Inner, JoinType::Left] {
        let node = node_n(
            &fix,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![
                dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
                dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Dim(0) }], None, Vec::new()),
                dim_stage(vec![DimKey { dim_col: 1, build_col: 4, src: DimSrc::Dim(1) }], None, Vec::new()),
            ],
            vec![
                JoinOut { side: JoinSide::Probe, col: 1 },
                JoinOut { side: JoinSide::Build, col: 4 },
                dko(0, 4),
                dko(1, 2),
                dko(2, 5),
            ],
        )
        .expect("construct chained");
        check_n(&fix, &node, &format!("chained-row {jt:?}"));
    }
}

/// Four stages (two flat, two chained), mixed multi-key stage, text
/// byte-eq + word pred on chained stages, dim text payload outs.
#[test]
fn join_depth_four_stage_product() {
    let fix = fixture_n("four", 900, 1400, &[150, 100, 80, 60]);
    let pred = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 1, 6, TypMeta::INT4)]);
    let node = node_n(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
            dim_stage(
                vec![
                    DimKey { dim_col: 1, build_col: 2, src: DimSrc::Dim(0) },
                    DimKey { dim_col: 2, build_col: 2, src: DimSrc::Dim(0) },
                ],
                None,
                Vec::new(),
            ),
            dim_stage(
                vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }],
                Some(pred),
                vec![TextEqTerm { col: 5, bytes: b"s3".to_vec() }],
            ),
            dim_stage(vec![DimKey { dim_col: 1, build_col: 4, src: DimSrc::Dim(2) }], None, Vec::new()),
        ],
        vec![
            JoinOut { side: JoinSide::Probe, col: 1 },
            dko(0, 4),
            dko(1, 5),
            dko(2, 4),
            dko(3, 3),
        ],
    )
    .expect("construct four-stage");
    check_n(&fix, &node, "four-stage-product");
}

/// An EMPTY chained stage answers the empty inner join (no panic in the
/// generalized walk).
#[test]
fn join_depth_empty_chained_dim() {
    let mut fix = fixture_n("echain", 400, 400, &[120]);
    fix.des.push(empty_engine("chained-dim"));
    fix.drows.push(Vec::new());
    let node = node_n(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Dim(0) }], None, Vec::new()),
        ],
        vec![JoinOut { side: JoinSide::Probe, col: 1 }, dko(0, 4)],
    )
    .expect("construct");
    let dctxs: Vec<_> = fix.des.iter().map(|e| e.ctx()).collect();
    let drefs: Vec<&_> = dctxs.iter().collect();
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &drefs, &node).expect("run");
    assert_eq!(got.nrows, 0, "empty chained dim answers the empty inner join");
}

/// Grouped fold over the chained tree: group keys on probe + the DEEP
/// dim; count(*) + sum(probe) fold per full match combination.
#[test]
fn join_depth_grouped_agg_chained() {
    let fix = fixture_n("gchain", 1200, 2000, &[150, 100]);
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountStar, None),
        JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![
        JoinOut { side: JoinSide::Probe, col: 2 },
        JoinOut { side: JoinSide::Dim(1), col: 2 },
    ];
    let dbanks: Vec<&_> = fix.des.iter().map(|e| &e.bank).collect();
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &dbanks,
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        vec![
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Dim(0) }], None, Vec::new()),
        ],
        aggs,
        groups,
        0,
    )
    .expect("construct");
    let dctxs: Vec<_> = fix.des.iter().map(|e| e.ctx()).collect();
    let drefs: Vec<&_> = dctxs.iter().collect();
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &drefs, &anode).expect("run");
    let dslices: Vec<&[Row]> = fix.drows.iter().map(|v| &v[..]).collect();
    let joined = oracle_join3(&anode.join, &fix.brows, &fix.prows, &dslices);
    check_agg_multi(got, &anode, &joined, "grouped-chained");
}

/// Typed refusals: a forward/self chain reference, over-cap stage count,
/// and a text-face chained source.
#[test]
fn join_depth_refusals() {
    let fix = fixture_n("refuse", 200, 200, &[50, 50]);
    // self/forward chain: stage 0 sourcing Dim(0) refuses.
    let r = node_n(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Dim(0) }], None, Vec::new()),
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
        ],
        vec![JoinOut { side: JoinSide::Probe, col: 1 }],
    );
    assert!(
        matches!(r, Err(sqe::joins::JoinRefuse::Unsupported { what: "dim-chain-order" })),
        "forward chain refuses typed: {r:?}"
    );
    // text-face chained source (dim0 col 5 is text) refuses dim-key-face.
    let r = node_n(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()),
            dim_stage(vec![DimKey { dim_col: 1, build_col: 5, src: DimSrc::Dim(0) }], None, Vec::new()),
        ],
        vec![JoinOut { side: JoinSide::Probe, col: 1 }],
    );
    assert!(
        matches!(r, Err(sqe::joins::JoinRefuse::Unsupported { what: "dim-key-face" })),
        "text chained source refuses typed: {r:?}"
    );
    // over-cap stage count refuses dim-stage-depth.
    let fix7 = fixture_n("refuse7", 100, 100, &[20, 20, 20, 20, 20, 20, 20]);
    let stages: Vec<DimStage> = (0..7)
        .map(|_| dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new()))
        .collect();
    let r = node_n(
        &fix7,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        stages,
        vec![JoinOut { side: JoinSide::Probe, col: 1 }],
    );
    assert!(
        matches!(r, Err(sqe::joins::JoinRefuse::Unsupported { what: "dim-stage-depth" })),
        "7 stages refuse typed: {r:?}"
    );
}

// --- composite grouped folds ------------------------------------------------

/// Oracle for the multi-key grouped fold: group joined rows by the FULL
/// key tuple (NULLs group equal), fold, HAVING, render [keys…, aggs…].
fn oracle_multi_grouped(anode: &JoinAggNode, rows: &[Vec<RV>]) -> AnswerSet {
    use std::collections::BTreeMap;
    #[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
    enum KC {
        Null,
        I(i64),
        T(Vec<u8>),
    }
    let key_of = |r: &Vec<RV>| -> Vec<KC> {
        anode
            .group_oi
            .iter()
            .enumerate()
            .map(|(j, &goi)| match &r[goi] {
                RV::Null => KC::Null,
                RV::I(v) => KC::I(*v),
                RV::T(b) => KC::T(match anode.group_xf.get(j) {
                    Some(Some(xf)) => xf.apply_bytes(b).to_vec(),
                    _ => b.clone(),
                }),
            })
            .collect()
    };
    let mut groups: BTreeMap<Vec<KC>, Vec<usize>> = BTreeMap::new();
    for (ri, r) in rows.iter().enumerate() {
        groups.entry(key_of(r)).or_default().push(ri);
    }
    struct G {
        keys: Vec<KC>,
        rows: u64,
        cells: Vec<(i128, i64, Option<i64>)>, // (sum, nonnull count, minmax)
        dsets: Vec<std::collections::BTreeSet<i64>>,
    }
    let mut gs: Vec<G> = Vec::new();
    for (k, ris) in groups {
        let mut cells: Vec<(i128, i64, Option<i64>)> = vec![(0, 0, None); anode.aggs.len()];
        let mut dsets: Vec<std::collections::BTreeSet<i64>> =
            (0..anode.aggs.len()).map(|_| Default::default()).collect();
        for &ri in &ris {
            for (ai, a) in anode.aggs.iter().enumerate() {
                if a.op == JoinAggOp::CountStar {
                    continue;
                }
                let oi = anode.agg_oi[ai];
                if a.op == JoinAggOp::CountDistinct {
                    if let Some(v) = rows[ri][oi].i() {
                        dsets[ai].insert(v);
                    }
                    continue;
                }
                let val: Option<i64> = match a.arith {
                    None => rows[ri][oi].i(),
                    Some(ar) => {
                        let x = rows[ri][oi].i();
                        match ar {
                            JoinArith::AddK { k, .. } => x.map(|x| x + k),
                            JoinArith::MulCC { .. } => {
                                let y = rows[ri][anode.agg_oi2[ai]].i();
                                x.zip(y).map(|(x, y)| x * y)
                            }
                            JoinArith::MulKSub { k, .. } => {
                                let y = rows[ri][anode.agg_oi2[ai]].i();
                                x.zip(y).map(|(x, y)| x * (k - y))
                            }
                            JoinArith::PackedMulK { k, sub, .. } => {
                                let y = rows[ri][anode.agg_oi2[ai]].i();
                                x.zip(y).map(|(x, y)| x * if sub { k - y } else { k + y })
                            }
                            JoinArith::PackedMulKSubCC { k, sub, sa, sb, c, sc, d, sd } => {
                                let pos = |io| {
                                    anode.join.out.iter().position(|o| *o == io).unwrap()
                                };
                                let s = (sa + sb).max(sc + sd);
                                let (ma, mb) = (
                                    10i64.pow((s - sa - sb) as u32),
                                    10i64.pow((s - sc - sd) as u32),
                                );
                                let y = rows[ri][anode.agg_oi2[ai]].i();
                                let cd = rows[ri][pos(c)].i().zip(rows[ri][pos(d)].i());
                                x.zip(y).zip(cd).map(|((x, y), (cv, dv))| {
                                    x * (if sub { k - y } else { k + y }) * ma - cv * dv * mb
                                })
                            }
                        }
                    }
                };
                if a.op == JoinAggOp::CountCol {
                    // count(col) over TEXT inputs counts non-null too.
                    let ok = match &rows[ri][oi] {
                        RV::Null => false,
                        _ => true,
                    };
                    if ok {
                        cells[ai].1 += 1;
                    }
                    continue;
                }
                if let Some(v) = val {
                    cells[ai].0 += v as i128;
                    cells[ai].1 += 1;
                    cells[ai].2 = Some(match (a.op, cells[ai].2) {
                        (JoinAggOp::Min, Some(m)) => m.min(v),
                        (JoinAggOp::Max, Some(m)) => m.max(v),
                        (_, None) => v,
                        (_, Some(m)) => m,
                    });
                }
            }
        }
        gs.push(G { keys: k, rows: ris.len() as u64, cells, dsets });
    }
    if let Some(h) = &anode.having {
        gs.retain(|g| {
            let ai = h.agg as usize;
            let v: Option<i128> = match anode.aggs[ai].op {
                JoinAggOp::CountStar => Some(g.rows as i128),
                JoinAggOp::CountCol => Some(g.cells[ai].1 as i128),
                JoinAggOp::CountDistinct => Some(g.dsets[ai].len() as i128),
                JoinAggOp::Sum => (g.cells[ai].1 > 0).then_some(g.cells[ai].0),
                JoinAggOp::Min | JoinAggOp::Max => g.cells[ai].2.map(|x| x as i128),
            };
            h.keep(v)
        });
    }
    let nk = anode.groups.len();
    let mut cols: Vec<AnswerCol> = Vec::new();
    for j in 0..nk {
        let ty = match anode.group_xf.get(j) {
            Some(Some(xf)) => xf.out_ty(),
            _ => anode.join.out_tys[anode.group_oi[j]],
        };
        if ty.is_varlena() {
            let mut bb = BytesBuild::new();
            let mut mask = Vec::new();
            for g in &gs {
                match &g.keys[j] {
                    KC::T(b) => {
                        bb.push(b);
                        mask.push(true);
                    }
                    _ => {
                        bb.push(b"");
                        mask.push(false);
                    }
                }
            }
            let mut c = bb.finish(ty);
            if !mask.iter().all(|&x| x) {
                c.validity = Validity::Mask(mask);
            }
            cols.push(c);
        } else {
            let mut v = Vec::new();
            let mut mask = Vec::new();
            for g in &gs {
                match &g.keys[j] {
                    KC::I(x) => {
                        v.push(*x);
                        mask.push(true);
                    }
                    _ => {
                        v.push(0);
                        mask.push(false);
                    }
                }
            }
            let validity = if mask.iter().all(|&x| x) {
                Validity::AllValid
            } else {
                Validity::Mask(mask)
            };
            cols.push(AnswerCol { ty, data: ColData::I64(v), validity });
        }
    }
    for (ai, a) in anode.aggs.iter().enumerate() {
        cols.push(match a.op {
            JoinAggOp::CountStar => {
                AnswerCol::i64s(a.out, gs.iter().map(|g| g.rows as i64).collect())
            }
            JoinAggOp::CountCol => {
                AnswerCol::i64s(a.out, gs.iter().map(|g| g.cells[ai].1).collect())
            }
            JoinAggOp::CountDistinct => {
                AnswerCol::i64s(a.out, gs.iter().map(|g| g.dsets[ai].len() as i64).collect())
            }
            JoinAggOp::Sum => {
                let mask: Vec<bool> = gs.iter().map(|g| g.cells[ai].1 > 0).collect();
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol {
                    ty: a.out,
                    data: ColData::I128(gs.iter().map(|g| g.cells[ai].0).collect()),
                    validity,
                }
            }
            JoinAggOp::Min | JoinAggOp::Max => {
                AnswerCol::i64s_opt(a.out, gs.iter().map(|g| g.cells[ai].2).collect())
            }
        });
    }
    AnswerSet::from_cols(cols)
}

/// Joined-row projection for the AGG oracle: `join.out` order (the agg
/// input/out set), one row per joined row.
fn check_agg_multi(
    got: AnswerSet,
    anode: &JoinAggNode,
    joined: &[Vec<RV>],
    what: &str,
) {
    let want = oracle_multi_grouped(anode, joined);
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert_eq!(gl, wl, "{what}: rendered identity");
}

#[test]
fn join_grouped_multi_key_2way() {
    let fix = fixture("gm2", true, 5000, 8000);
    // keys: probe g (word), probe t (text, NULLs), build v (word, NULLs)
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountStar, None),
        JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        JoinAggReq::col(JoinAggOp::Min, Some(JoinOut { side: JoinSide::Build, col: 4 })),
    ];
    let groups = vec![
        JoinOut { side: JoinSide::Probe, col: 2 },
        JoinOut { side: JoinSide::Probe, col: 3 },
        JoinOut { side: JoinSide::Build, col: 4 },
    ];
    for having in [None, Some(sqe::ir::HavingCmp { agg: 0, op: sqe::ir::HvOp::Gt, rhs: 3 })] {
        let mut anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank,
            &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None,
            Vec::new(),
            aggs.clone(),
            groups.clone(),
            0,
        )
        .expect("construct");
        anode.having = having;
        let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
        let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
        check_agg_multi(got, &anode, &joined, "grouped-multi-2way");
    }
}

#[test]
fn join_grouped_multi_distinct() {
    let fix = fixture("gmd", true, 5000, 8000);
    // distinct over nullable word lanes both sides; text + word keys.
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountStar, None),
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Build, col: 4 })),
        JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![
        JoinOut { side: JoinSide::Probe, col: 2 },
        JoinOut { side: JoinSide::Build, col: 3 },
    ];
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
    .expect("construct");
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    check_agg_multi(got, &anode, &joined, "grouped-multi-distinct");
}

#[test]
fn join_grouped_text_slice_key() {
    // [textslice] derived byte-prefix group keys: alone, clamped (len
    // past short images), NULL-bearing build lane mixed with a word
    // key, and the anti-join grain (the q22 shape at the oracle grain).
    let fix = fixture("gts", true, 5000, 8000);
    let aggs = || {
        vec![
            JoinAggReq::col(JoinAggOp::CountStar, None),
            JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        ]
    };
    let legs: Vec<(JoinType, Vec<JoinOut>, Vec<Option<KeyXf>>, &str)> = vec![
        (
            JoinType::Inner,
            vec![JoinOut { side: JoinSide::Probe, col: 3 }],
            vec![Some(KeyXf::TextSlice { from: 1, len: 2 })],
            "slice-probe",
        ),
        (
            JoinType::Inner,
            vec![JoinOut { side: JoinSide::Probe, col: 3 }],
            vec![Some(KeyXf::TextSlice { from: 1, len: 4 })],
            "slice-clamp",
        ),
        (
            JoinType::Inner,
            vec![
                JoinOut { side: JoinSide::Build, col: 3 },
                JoinOut { side: JoinSide::Probe, col: 2 },
            ],
            vec![Some(KeyXf::TextSlice { from: 1, len: 1 }), None],
            "slice-null-mixed",
        ),
        (
            JoinType::Anti,
            vec![JoinOut { side: JoinSide::Probe, col: 3 }],
            vec![Some(KeyXf::TextSlice { from: 1, len: 2 })],
            "slice-anti",
        ),
    ];
    for (jt, groups, gxf, what) in legs {
        let mut anode = join_agg_node(
            &fix.be.bank,
            &fix.pe.bank,
            &[],
            0,
            jt,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None,
            Vec::new(),
            aggs(),
            groups,
            0,
        )
        .expect("construct");
        anode.group_xf = gxf;
        let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode)
            .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
        let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
        check_agg_multi(got, &anode, &joined, what);
    }
}

#[test]
fn join_text_slice_admission_typed() {
    // Multibyte lane (no single-byte-chars witness), word lane, off-shape
    // from/len — every arm the typed refusal, never a wrong answer.
    let base = std::env::temp_dir().join(format!("sqe_join_gtsa_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let mb = |i: u64| -> Row {
        [
            RV::I(i as i64 % 5),
            RV::I(i as i64 % 3),
            RV::T(if i % 2 == 0 { "é7-mb".as_bytes().to_vec() } else { b"a7".to_vec() }),
            RV::I(i as i64),
            RV::T(b"s".to_vec()),
        ]
    };
    let brows: Vec<Row> = (0..40).map(mb).collect();
    let prows: Vec<Row> = (0..80).map(mb).collect();
    let (bdir, pdir) = (base.join("b"), base.join("p"));
    seal(bdir.to_str().unwrap(), &brows);
    seal(pdir.to_str().unwrap(), &prows);
    let be = open_engine(bdir.to_str().unwrap());
    let pe = open_engine(pdir.to_str().unwrap());
    let mk = |groups: Vec<JoinOut>, gxf: Vec<Option<KeyXf>>| {
        let mut a = join_agg_node(
            &be.bank,
            &pe.bank,
            &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None,
            Vec::new(),
            vec![JoinAggReq::col(JoinAggOp::CountStar, None)],
            groups,
            0,
        )
        .expect("construct");
        a.group_xf = gxf;
        run_hash_join_agg(&be.ctx(), &pe.ctx(), &[], &a)
    };
    let t3 = || vec![JoinOut { side: JoinSide::Probe, col: 3 }];
    let sl = |from: u16, len: u16| vec![Some(KeyXf::TextSlice { from, len })];
    match mk(t3(), sl(1, 2)) {
        Err(JoinRefuse::Face { what: "text-slice-char-witness", .. }) => {}
        other => panic!("multibyte lane: {other:?}"),
    }
    match mk(vec![JoinOut { side: JoinSide::Probe, col: 2 }], sl(1, 2)) {
        Err(JoinRefuse::Face { what: "text-slice-source", .. }) => {}
        other => panic!("word lane: {other:?}"),
    }
    match mk(t3(), sl(2, 2)) {
        Err(JoinRefuse::Unsupported { what: "text-slice-shape" }) => {}
        other => panic!("from!=1: {other:?}"),
    }
    match mk(t3(), sl(1, 0)) {
        Err(JoinRefuse::Unsupported { what: "text-slice-shape" }) => {}
        other => panic!("len 0: {other:?}"),
    }
}

#[test]
fn join_grouped_distinct_single_word_key() {
    // A single probe word key would elect the Cells64 arms; a distinct
    // leg must route to the composite sink and stay exact.
    let fix = fixture("gmdw", false, 3000, 6000);
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        JoinAggReq::col(JoinAggOp::CountCol, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![JoinOut { side: JoinSide::Probe, col: 2 }];
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
    .expect("construct");
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    check_agg_multi(got, &anode, &joined, "grouped-distinct-single-key");
}

#[test]
fn join_distinct_admission_typed() {
    let fix = fixture("gmda", false, 200, 400);
    let mk = |aggs: Vec<JoinAggReq>, groups: Vec<JoinOut>| {
        join_agg_node(
            &fix.be.bank,
            &fix.pe.bank,
            &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            Vec::new(),
            None,
            None,
            Vec::new(),
            aggs,
            groups,
            0,
        )
    };
    let d = |io: JoinOut| JoinAggReq::col(JoinAggOp::CountDistinct, Some(io));
    let gk = vec![JoinOut { side: JoinSide::Probe, col: 2 }];
    match mk(vec![d(JoinOut { side: JoinSide::Probe, col: 4 })], Vec::new()) {
        Err(JoinRefuse::Unsupported { what: "agg-distinct-ungrouped" }) => {}
        other => panic!("ungrouped distinct: {other:?}"),
    }
    match mk(vec![d(JoinOut { side: JoinSide::Probe, col: 3 })], gk.clone()) {
        Err(JoinRefuse::Unsupported { what: "agg-distinct-input" }) => {}
        other => panic!("varlena distinct: {other:?}"),
    }
    let shaped = JoinAggReq {
        op: JoinAggOp::CountDistinct,
        input: Some(JoinOut { side: JoinSide::Probe, col: 4 }),
        input2: None,
        arith: Some(sqe::joins::JoinArith::AddK { k: 1, w: 8 }),
        case: None,
    };
    match mk(vec![shaped], gk) {
        Err(JoinRefuse::Unsupported { what: "agg-distinct-shape" }) => {}
        other => panic!("arith distinct: {other:?}"),
    }
}

#[test]
fn join_distinct_budget_refusal_typed() {
    // Small build (the table itself fits), wide probe distinct plane:
    // crossing the shared budget is the typed refusal, never an OOM.
    // Kill switch forced: the freeze-then-refuse law must hold verbatim
    // whatever spill substrate sibling tests registered.
    let fix = fixture_sp("gmdb", false, 40, 6000, false);
    let aggs =
        vec![JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Probe, col: 4 }))];
    let groups = vec![JoinOut { side: JoinSide::Probe, col: 2 }];
    let mut anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
    .expect("construct");
    // merged-tree meter charges 1920 bytes on this fixture (the campaign's
    // set-plane accounting refinement); the forced budget pins below it.
    anode.join.build_budget_bytes = 1792;
    match run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode) {
        Err(JoinRefuse::DistinctExceedsBudget { bytes, budget }) => {
            assert!(bytes > budget, "meter past the budget: {bytes} vs {budget}");
            assert_eq!(budget, 1792);
        }
        other => panic!("distinct budget: {:?}", other.map(|_| ())),
    }
}

/// Set-plane spill: forced-small budgets serve the exact answer (identity
/// vs the resident twin AND the oracle), the engagement counters prove
/// the drains/merges ran, and reruns are byte-stable.
#[test]
#[cfg(feature = "rig")]
fn join_distinct_spill_identity() {
    use sqe::joins::hash_join::{JDSPILL_DRAINS, JDSPILL_MERGES, JDSPILL_SCATTERS};
    use std::sync::atomic::Ordering::Relaxed;
    sqe::spill::register_std_store();
    // Small build (the build table itself fits the forced budget); the
    // probe side drives the distinct plane far past it.
    let fix = fixture_sp("gmds", true, 40, 8000, true);
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountStar, None),
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Build, col: 4 })),
        JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![
        JoinOut { side: JoinSide::Probe, col: 2 },
        JoinOut { side: JoinSide::Build, col: 3 },
    ];
    let mut anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
    .expect("construct");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    let resident = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("resident");
    let mut rl = to_lines(&resident);
    rl.sort();
    for budget in [1792usize, 1 << 16] {
        anode.join.build_budget_bytes = budget;
        let (s0, d0, m0) = (
            JDSPILL_SCATTERS.load(Relaxed),
            JDSPILL_DRAINS.load(Relaxed),
            JDSPILL_MERGES.load(Relaxed),
        );
        let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode)
            .expect("armed set-plane spill serves over budget");
        if budget == 1792 {
            assert!(JDSPILL_SCATTERS.load(Relaxed) > s0, "worker set drains engaged");
            assert!(JDSPILL_DRAINS.load(Relaxed) > d0, "finalize table drains engaged");
            assert!(JDSPILL_MERGES.load(Relaxed) > m0, "finalize dedupe merge engaged");
        }
        let mut gl = to_lines(&got);
        gl.sort();
        assert_eq!(gl, rl, "spill vs resident identity: budget={budget}");
        let again = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("rerun");
        let mut al = to_lines(&again);
        al.sort();
        assert_eq!(al, gl, "spill rerun determinism: budget={budget}");
        check_agg_multi(got, &anode, &joined, &format!("distinct-spill b={budget}"));
    }
}

/// The single-word-key distinct shape (forced onto the composite sink)
/// under a forced-small budget: served, exact, oracle-checked.
#[test]
#[cfg(feature = "rig")]
fn join_distinct_spill_single_word_key() {
    sqe::spill::register_std_store();
    let fix = fixture_sp("gmdsw", false, 40, 6000, true);
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountDistinct, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
        JoinAggReq::col(JoinAggOp::CountCol, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![JoinOut { side: JoinSide::Probe, col: 2 }];
    let mut anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
    .expect("construct");
    anode.join.build_budget_bytes = 4096;
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode)
        .expect("armed set-plane spill serves over budget");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    check_agg_multi(got, &anode, &joined, "distinct-spill-single-key");
}

#[test]
fn join3_grouped_multi_key_with_dim() {
    let fix = fixture3("gm3", 4000, 6000, 300);
    let aggs = vec![
        JoinAggReq::col(JoinAggOp::CountStar, None),
        JoinAggReq::col(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 4 })),
    ];
    let groups = vec![
        JoinOut { side: JoinSide::Probe, col: 2 },
        JoinOut { side: JoinSide::Dim(0), col: 5 },
        JoinOut { side: JoinSide::Dim(0), col: 4 },
    ];
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[&fix.de.bank],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        vec![dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new())],
        aggs,
        groups,
        0,
    )
    .expect("construct");
    let got =
        run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[&fix.de.ctx()], &anode).expect("run");
    let joined = oracle_join3(&anode.join, &fix.brows, &fix.prows, &[&fix.drows]);
    check_agg_multi(got, &anode, &joined, "grouped-multi-3way");
}

#[test]
fn join_agg_arith_folds() {
    let fix = fixture("arith", true, 4000, 6000);
    // grouped sum(a*(k-b)), sum(a*b), sum(a+k) over probe cols 4 and 2.
    let a4 = JoinOut { side: JoinSide::Probe, col: 4 };
    let g2 = JoinOut { side: JoinSide::Probe, col: 2 };
    let aggs = vec![
        JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(a4),
            input2: Some(g2),
            arith: Some(JoinArith::MulKSub { k: 100, w: 8, wi: 8 }),
            case: None,
        },
        JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(a4),
            input2: Some(g2),
            arith: Some(JoinArith::MulCC { w: 8 }),
            case: None,
        },
        JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(a4),
            input2: None,
            arith: Some(JoinArith::AddK { k: 7, w: 8 }),
            case: None,
        },
    ];
    // multi-key grouped (goes through the composite sink)
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs.clone(),
        vec![g2, JoinOut { side: JoinSide::Build, col: 2 }],
        0,
    )
    .expect("construct");
    let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    check_agg_multi(got, &anode, &joined, "arith-grouped");
    // single-key path (Cells64 arm) with an arith leg
    let anode1 = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        vec![g2],
        0,
    )
    .expect("construct");
    let got1 = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode1).expect("run");
    let joined1 = oracle_join(&anode1.join, &fix.brows, &fix.prows);
    check_agg_multi(got1, &anode1, &joined1, "arith-single-key");
    // overflow-unwitnessed refusal: k too large for the AddK result width
    let bad = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        Vec::new(),
        vec![JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(a4),
            input2: None,
            arith: Some(JoinArith::AddK { k: i64::MAX, w: 8 }),
            case: None,
        }],
        vec![g2],
        0,
    )
    .expect("construct");
    match run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &bad) {
        Err(JoinRefuse::Unsupported { what: "agg-arith-overflow-unwitnessed" }) => {}
        other => panic!("expected overflow refusal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// [sqe-q9arith] fused-arithmetic join folds — the Q9-arith gate lane:
// both-sides operands, NULL mixes, all four join types, ungrouped +
// grouped, boundary products at the i64 corner, typed shape refusals.
// Oracle law: the same independent nested-loop join, aggregates re-derived
// row-at-a-time with the NULL-propagating expression evaluation.
// ---------------------------------------------------------------------------

/// [caseleg] Predicated fold legs over the joined stream: word / byte-eq
/// / IN-images / LIKE / AND tests on probe and build columns (nullable
/// test columns exercise 3VL), ELSE 0 vs no ELSE, the lane-less count
/// shape, a fused-arith arm, and the typed face/shape refusals.
#[test]
fn join_agg_case_legs() {
    use sqe::ir::{BytesCmp, VarOp, VarPredTerm};
    use sqe::joins::{CaseTest, JoinCaseLeg};
    let fix = fixture("caseleg", true, 4000, 6000);
    let keys = || vec![JoinKey { build_col: 1, probe_col: 1 }];
    let po = |c: u32| JoinOut { side: JoinSide::Probe, col: c };
    let bo = |c: u32| JoinOut { side: JoinSide::Build, col: c };
    let leg = |op: JoinAggOp, input: Option<JoinOut>, input2: Option<JoinOut>, arith: Option<JoinArith>, case: JoinCaseLeg| JoinAggReq {
        op,
        input,
        input2,
        arith,
        case: Some(case),
    };
    let word = |col: u32, lo: i64, hi: i64| CaseTest::Word(PredTerm::new(col, CmpOp::Between, lo, hi, TypMeta::INT4));
    let beq = |col: u32, img: &[u8]| CaseTest::Bytes(VarPredTerm::new(col, VarOp::CmpBytes(BytesCmp::Eq), img.to_vec(), TypMeta::TEXT_C));
    let aggs = vec![
        // word test on the probe (else 0)
        leg(JoinAggOp::Sum, pin(4), None, None, JoinCaseLeg { col: po(2), test: word(2, 3, 12), else_zero: true }),
        // byte-eq test on a build text column (no else -> NULL when none pass)
        leg(JoinAggOp::Sum, bin(4), None, None, JoinCaseLeg { col: bo(5), test: beq(5, b"s3"), else_zero: false }),
        // the lane-less count shape: IN images on the build text column
        leg(
            JoinAggOp::CountCol,
            None,
            None,
            None,
            JoinCaseLeg {
                col: bo(5),
                test: CaseTest::Bytes(VarPredTerm::new(
                    5,
                    VarOp::InBytes,
                    sqe::ir::encode_in_needles(vec![b"s1".to_vec(), b"s4".to_vec()]),
                    TypMeta::TEXT_C,
                )),
                else_zero: false,
            },
        ),
        // fused arm under an AND of word tests
        leg(
            JoinAggOp::Sum,
            pin(4),
            bin(4),
            Some(JoinArith::MulCC { w: 8 }),
            JoinCaseLeg { col: po(2), test: CaseTest::And(vec![word(2, 5, 100), word(2, 0, 20)]), else_zero: true },
        ),
        // LIKE on the NULLABLE build text column (NULL test -> else arm)
        leg(
            JoinAggOp::Sum,
            pin(4),
            None,
            None,
            JoinCaseLeg {
                col: bo(3),
                test: CaseTest::Bytes(VarPredTerm::new(3, VarOp::Like, b"t0%".to_vec(), TypMeta::TEXT_C)),
                else_zero: true,
            },
        ),
        // count(col) under a case: pass AND non-NULL input
        leg(JoinAggOp::CountCol, pin(4), None, None, JoinCaseLeg { col: po(2), test: word(2, 0, 7), else_zero: false }),
        JoinAggReq::col(JoinAggOp::CountStar, None),
    ];
    let anode = arith_agg_node(&fix, JoinType::Inner, keys(), aggs.clone(), Vec::new()).expect("construct");
    check_agg(&fix, &anode, "case-legs-inner");
    let semi: Vec<JoinAggReq> = aggs.iter().filter(|a| a.case.as_ref().is_none_or(|c| c.col.side == JoinSide::Probe) && a.input.is_none_or(|i| i.side == JoinSide::Probe) && a.input2.is_none()).cloned().collect();
    let anode = arith_agg_node(&fix, JoinType::Semi, keys(), semi, Vec::new()).expect("construct");
    check_agg(&fix, &anode, "case-legs-semi");
    // typed refusals: a byte test on a word column, a word test on a text
    // column, a build-side test under Semi, count(*) carrying a case.
    let bad = [
        (leg(JoinAggOp::Sum, pin(4), None, None, JoinCaseLeg { col: po(2), test: beq(2, b"x"), else_zero: true }), JoinType::Inner, "agg-case-test-face"),
        (leg(JoinAggOp::Sum, pin(4), None, None, JoinCaseLeg { col: bo(5), test: word(5, 0, 1), else_zero: true }), JoinType::Inner, "agg-case-test-face"),
        (leg(JoinAggOp::Sum, pin(4), None, None, JoinCaseLeg { col: bo(5), test: beq(5, b"s3"), else_zero: true }), JoinType::Semi, "agg-case-side"),
        (leg(JoinAggOp::CountStar, None, None, None, JoinCaseLeg { col: po(2), test: word(2, 0, 1), else_zero: true }), JoinType::Inner, "agg-count-star-input"),
    ];
    for (req, jt, want) in bad {
        match arith_agg_node(&fix, jt, keys(), vec![req], Vec::new()) {
            Err(JoinRefuse::Unsupported { what }) if what == want => {}
            other => panic!("expected {want}, got {other:?}"),
        }
    }
}

fn areq(op: JoinAggOp, input: Option<JoinOut>, input2: Option<JoinOut>, arith: Option<JoinArith>) -> JoinAggReq {
    JoinAggReq { op, input, input2, arith, case: None }
}

fn arith_agg_node(fix: &Fix, jt: JoinType, keys: Vec<JoinKey>, aggs: Vec<JoinAggReq>, groups: Vec<JoinOut>) -> Result<JoinAggNode, JoinRefuse> {
    join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[],
        0,
        jt,
        keys,
        Vec::new(),
        None,
        None,
        Vec::new(),
        aggs,
        groups,
        0,
    )
}

/// Ungrouped arith folds through the AggFold sink, all four join types.
/// Operands cross sides where the join delivers them (Inner/Left); the
/// semi/anti shapes keep probe-only operands (family law). NULL mixes
/// ride the fixtures' nullable v columns; LEFT's null-extended build
/// operands must fold nothing (either-NULL law).
#[test]
fn join_agg_arith_ungrouped_all_types() {
    let fix = fixture("q9au", true, 6000, 9000);
    for jt in ALL {
        let aggs: Vec<JoinAggReq> = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            vec![
                areq(JoinAggOp::CountStar, None, None, None),
                // probe v * probe g
                areq(JoinAggOp::Sum, pin(4), pin(2), Some(JoinArith::MulCC { w: 8 })),
                // probe v * (60 - probe g) — the revenue shape, one side
                areq(JoinAggOp::Sum, pin(4), pin(2), Some(JoinArith::MulKSub { k: 60, w: 8, wi: 8 })),
                // probe v + k
                areq(JoinAggOp::Sum, pin(4), None, Some(JoinArith::AddK { k: -3, w: 8 })),
                // probe v squared: agg_oi2 == agg_oi (dedup path)
                areq(JoinAggOp::Sum, pin(4), pin(4), Some(JoinArith::MulCC { w: 8 })),
            ]
        } else {
            vec![
                areq(JoinAggOp::CountStar, None, None, None),
                // CROSS-SIDE: probe v * build v (the Q9 cost-term shape)
                areq(JoinAggOp::Sum, pin(4), bin(4), Some(JoinArith::MulCC { w: 8 })),
                // CROSS-SIDE, build-primary: build v * probe v
                areq(JoinAggOp::Sum, bin(4), pin(4), Some(JoinArith::MulCC { w: 8 })),
                // CROSS-SIDE revenue: probe v * (100 - build v)
                areq(JoinAggOp::Sum, pin(4), bin(4), Some(JoinArith::MulKSub { k: 100, w: 8, wi: 8 })),
                // build-side AddK (null-extended under LEFT)
                areq(JoinAggOp::Sum, bin(4), None, Some(JoinArith::AddK { k: 7, w: 8 })),
                // same-side square on the build stream
                areq(JoinAggOp::Sum, bin(4), bin(4), Some(JoinArith::MulCC { w: 8 })),
            ]
        };
        let anode = arith_agg_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], aggs, Vec::new())
            .expect("construct arith agg");
        check_agg(&fix, &anode, &format!("arith-ungrouped {jt:?}"));
    }
}

/// Grouped arith folds with CROSS-SIDE operands over a two-key equi join
/// — the Q9 2-way core shape: sum(build.cost * probe.qty) + the revenue
/// form, grouped by a probe word key. Runs Inner AND Left (grouped LEFT:
/// null-extended build operands fold nothing while the probe rows still
/// count).
#[test]
fn join_agg_arith_q9_core_shape() {
    let fix = fixture("q9core", true, 5000, 8000);
    let keys = vec![
        JoinKey { build_col: 1, probe_col: 1 },
        JoinKey { build_col: 2, probe_col: 2 },
    ];
    let aggs = vec![
        // cost term: build v * probe v across the two-key join
        areq(JoinAggOp::Sum, bin(4), pin(4), Some(JoinArith::MulCC { w: 8 })),
        // revenue term: probe v * (100 - probe g), probe-side pair
        areq(JoinAggOp::Sum, pin(4), pin(2), Some(JoinArith::MulKSub { k: 100, w: 8, wi: 8 })),
        areq(JoinAggOp::CountStar, None, None, None),
    ];
    for jt in [JoinType::Inner, JoinType::Left] {
        // single-key grouped (Cells64 arm)
        let anode = arith_agg_node(&fix, jt, keys.clone(), aggs.clone(), vec![JoinOut { side: JoinSide::Probe, col: 2 }])
            .expect("construct q9 core");
        let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("run");
        let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
        check_agg_multi(got, &anode, &joined, &format!("q9-core single-key {jt:?}"));
        // multi-key grouped (composite sink), group keys on both sides
        let groups = vec![
            JoinOut { side: JoinSide::Probe, col: 2 },
            JoinOut { side: JoinSide::Build, col: 2 },
        ];
        if jt == JoinType::Inner {
            let anode2 = arith_agg_node(&fix, jt, keys.clone(), aggs.clone(), groups).expect("construct");
            let got2 = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode2).expect("run");
            let joined2 = oracle_join(&anode2.join, &fix.brows, &fix.prows);
            check_agg_multi(got2, &anode2, &joined2, "q9-core multi-key");
        }
    }
}

/// The product-bound law at the i64 corner: witnessed domains of
/// ±3.0e9 give |a|max·|b|max = 9.0e18 < 2^63−1 — the shape ENGAGES and
/// the i128 cell sum is exact; ±3.1e9 corners (9.61e18 > 2^63−1)
/// REFUSE typed (agg-arith-overflow-unwitnessed), never a wrapped fold.
#[test]
fn join_agg_arith_boundary_products() {
    let base = std::env::temp_dir().join(format!("sqe_q9a_bnd_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let mk_rows = |n: u64, big: i64| -> Vec<Row> {
        (0..n)
            .map(|i| {
                let k = if i % 13 == 0 { RV::Null } else { RV::I((i % 7) as i64) };
                let v = match i % 5 {
                    0 => RV::Null,
                    1 => RV::I(big),
                    2 => RV::I(-big),
                    _ => RV::I((i % 100) as i64 - 50),
                };
                [k, RV::I((i % 4) as i64), RV::T(b"c".to_vec()), v, RV::T(b"d".to_vec())]
            })
            .collect()
    };
    let fits = 3_000_000_000i64; // 9.0e18 ≤ i64::MAX
    let (bd, pd) = (base.join("b"), base.join("p"));
    let brows = mk_rows(2000, fits);
    let prows = mk_rows(3000, fits);
    seal(bd.to_str().unwrap(), &brows);
    seal(pd.to_str().unwrap(), &prows);
    let fix = Fix {
        be: open_engine(bd.to_str().unwrap()),
        pe: open_engine(pd.to_str().unwrap()),
        brows,
        prows,
    };
    let anode = arith_agg_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            areq(JoinAggOp::Sum, pin(4), bin(4), Some(JoinArith::MulCC { w: 8 })),
            areq(JoinAggOp::CountStar, None, None, None),
        ],
        Vec::new(),
    )
    .expect("construct boundary");
    check_agg(&fix, &anode, "arith-boundary-fits");
    // over the corner: same shape, domains ±3.1e9 — typed refusal.
    let over = 3_100_000_000i64; // 9.61e18 > i64::MAX
    let (bd2, pd2) = (base.join("b2"), base.join("p2"));
    let brows2 = mk_rows(1000, over);
    let prows2 = mk_rows(1000, over);
    seal(bd2.to_str().unwrap(), &brows2);
    seal(pd2.to_str().unwrap(), &prows2);
    let fix2 = Fix {
        be: open_engine(bd2.to_str().unwrap()),
        pe: open_engine(pd2.to_str().unwrap()),
        brows: brows2,
        prows: prows2,
    };
    let bad = arith_agg_node(
        &fix2,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![areq(JoinAggOp::Sum, pin(4), bin(4), Some(JoinArith::MulCC { w: 8 }))],
        Vec::new(),
    )
    .expect("construct");
    match run_hash_join_agg(&fix2.be.ctx(), &fix2.pe.ctx(), &[], &bad) {
        Err(JoinRefuse::Unsupported { what: "agg-arith-overflow-unwitnessed" }) => {}
        other => panic!("expected MulCC product-bound refusal, got {other:?}"),
    }
    // MulKSub INNER-op width: (k − b) must fit ITS result type — wi=4
    // with k = 3e9 puts the inner subtraction past i32 (typed refusal);
    // wi=8 admits, and the product bound then gates the outer op.
    let ksub_bad = arith_agg_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![areq(JoinAggOp::Sum, pin(2), bin(2), Some(JoinArith::MulKSub { k: 3_000_000_000, w: 8, wi: 4 }))],
        Vec::new(),
    )
    .expect("construct");
    match run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &ksub_bad) {
        Err(JoinRefuse::Unsupported { what: "agg-arith-overflow-unwitnessed" }) => {}
        other => panic!("expected MulKSub inner-width refusal, got {other:?}"),
    }
    let ksub_ok = arith_agg_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            areq(JoinAggOp::Sum, pin(2), bin(2), Some(JoinArith::MulKSub { k: 3_000_000_000, w: 8, wi: 8 })),
            areq(JoinAggOp::CountStar, None, None, None),
        ],
        Vec::new(),
    )
    .expect("construct");
    check_agg(&fix, &ksub_ok, "arith-ksub-wide-inner");
}

/// Family-law shape refusals: arith on a non-Sum op, mul without its
/// second operand, a second operand without a shape, AddK carrying one,
/// count(*) carrying arith — all typed, never panics or wrapped folds.
#[test]
fn join_agg_arith_shape_refusals() {
    let fix = fixture("q9aref", true, 500, 500);
    let keys = || vec![JoinKey { build_col: 1, probe_col: 1 }];
    let cases: Vec<(JoinAggReq, &str)> = vec![
        (areq(JoinAggOp::Min, pin(4), pin(2), Some(JoinArith::MulCC { w: 8 })), "agg-arith-op"),
        (areq(JoinAggOp::Max, pin(4), None, Some(JoinArith::AddK { k: 1, w: 8 })), "agg-arith-op"),
        (areq(JoinAggOp::Sum, pin(4), None, Some(JoinArith::MulCC { w: 8 })), "agg-arith-shape"),
        (areq(JoinAggOp::Sum, pin(4), pin(2), None), "agg-arith-shape"),
        (areq(JoinAggOp::Sum, pin(4), pin(2), Some(JoinArith::AddK { k: 1, w: 8 })), "agg-arith-shape"),
        (areq(JoinAggOp::CountStar, None, pin(2), Some(JoinArith::MulCC { w: 8 })), "agg-count-star-input"),
    ];
    for (req, want) in cases {
        match arith_agg_node(&fix, JoinType::Inner, keys(), vec![req.clone()], Vec::new()) {
            Err(JoinRefuse::Unsupported { what }) if what == want => {}
            other => panic!("shape {req:?}: expected {want}, got {other:?}"),
        }
    }
    // text second operand: word-embed refusal (agg-input-face).
    match arith_agg_node(
        &fix,
        JoinType::Inner,
        keys(),
        vec![areq(JoinAggOp::Sum, pin(4), pin(3), Some(JoinArith::MulCC { w: 8 }))],
        Vec::new(),
    ) {
        Err(JoinRefuse::Unsupported { what: "agg-input-face" }) => {}
        other => panic!("expected face refusal, got {other:?}"),
    }
}

/// Dense-electable single-key grouped fold WITH an arith leg: the direct
/// arm's atomic lanes carry no single-column |x| bound for arith inputs,
/// so election must keep the exact-i128 hash arm — answers identical to
/// the oracle on both engine configs.
#[test]
fn join_agg_arith_dense_arm_keeps_hash() {
    let fix = fixture("q9adn", true, 5000, 8000);
    let pe_hash = open_engine_arm(&fix.pe.bank.dir, false);
    assert!(fix.pe.faces.cfg.direct_array, "arm A must have the direct array enabled");
    let anode = arith_agg_node(
        &fix,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![
            areq(JoinAggOp::CountStar, None, None, None),
            areq(JoinAggOp::Sum, pin(4), bin(4), Some(JoinArith::MulCC { w: 8 })),
        ],
        vec![JoinOut { side: JoinSide::Probe, col: 2 }],
    )
    .expect("construct");
    let a = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode).expect("direct cfg");
    let b = run_hash_join_agg(&fix.be.ctx(), &pe_hash.ctx(), &[], &anode).expect("hash cfg");
    let (mut al, mut bl) = (to_lines(&a), to_lines(&b));
    al.sort();
    bl.sort();
    assert_eq!(al, bl, "arith grouped fold: direct-cfg vs hash-cfg identity");
    let joined = oracle_join(&anode.join, &fix.brows, &fix.prows);
    check_agg_multi(a, &anode, &joined, "arith-dense-arm-fallback");
}

/// Dim-side arith operands through the 3-way composition: the fused
/// fold reads Dim payload lanes as either operand (dim payload has
/// NULLs — the either-NULL law crosses the dim membrane too).
#[test]
fn join3_agg_arith_dim_operand() {
    let fix = fixture3("q9adim", 4000, 6000, 300);
    let aggs = vec![
        areq(JoinAggOp::CountStar, None, None, None),
        // dim payload * probe v
        areq(
            JoinAggOp::Sum,
            Some(JoinOut { side: JoinSide::Dim(0), col: 4 }),
            Some(JoinOut { side: JoinSide::Probe, col: 4 }),
            Some(JoinArith::MulCC { w: 8 }),
        ),
        // probe v * (40 − dim payload)
        areq(
            JoinAggOp::Sum,
            Some(JoinOut { side: JoinSide::Probe, col: 4 }),
            Some(JoinOut { side: JoinSide::Dim(0), col: 4 }),
            Some(JoinArith::MulKSub { k: 40, w: 8, wi: 8 }),
        ),
        // dim payload + k
        areq(
            JoinAggOp::Sum,
            Some(JoinOut { side: JoinSide::Dim(0), col: 4 }),
            None,
            Some(JoinArith::AddK { k: 5, w: 8 }),
        ),
    ];
    let anode = join_agg_node(
        &fix.be.bank,
        &fix.pe.bank,
        &[&fix.de.bank],
        0,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        vec![dim_stage(vec![DimKey { dim_col: 1, build_col: 2, src: DimSrc::Build }], None, Vec::new())],
        aggs,
        vec![JoinOut { side: JoinSide::Probe, col: 2 }],
        0,
    )
    .expect("construct 3-way arith");
    let got =
        run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[&fix.de.ctx()], &anode).expect("run");
    let joined = oracle_join3(&anode.join, &fix.brows, &fix.prows, &[&fix.drows]);
    check_agg_multi(got, &anode, &joined, "arith-dim-operand");
}
// ---------------------------------------------------------------------------
// P4-5: keyless (nest-loop) gate — same fixtures, same independent
// row-at-a-time oracle (o_keys_eq over zero keys is vacuously true, so
// the oracle IS the cross-with-quals semantics).
// ---------------------------------------------------------------------------

fn nl_node(fix: &Fix, jt: JoinType, quals: Vec<JoinQual>, out: Vec<JoinOut>) -> JoinNode {
    nest_loop_node(&fix.be.bank, &fix.pe.bank, 0, jt, quals, None, None, out, 0)
        .expect("construct nl")
}

#[test]
fn nl_nonequi_quals_all_types() {
    // NULLs in both qual columns (probe v: col4, build v: col4) — a NULL
    // operand never matches (3VL through the pair loop).
    let fix = fixture("nlq", true, 600, 900);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 4], &[])
        } else {
            out_pb(&[1, 4], &[4])
        };
        let node = nl_node(
            &fix,
            jt,
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
            out,
        );
        check(&fix, &node, &format!("nl-qual {jt:?}"));
    }
}

#[test]
fn nl_eq_qual_all_types() {
    // Equality carried as a per-pair qual (no key lanes) — the tiny-side
    // equi-join shape PG plans as a nest loop.
    let fix = fixture("nleq", true, 500, 800);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2], &[])
        } else {
            out_pb(&[1, 2], &[4])
        };
        let node = nl_node(
            &fix,
            jt,
            vec![
                JoinQual { probe_col: 2, build_col: 2, op: JoinCmp::Eq },
                JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Ge },
            ],
            out,
        );
        check(&fix, &node, &format!("nl-eq {jt:?}"));
    }
}

#[test]
fn nl_between_composed() {
    // BETWEEN over the pair = a Ge/Le conjunction (range-overlap class);
    // build cols 1 and 4 both carry NULLs.
    let fix = fixture("nlbt", true, 500, 700);
    for jt in [JoinType::Inner, JoinType::Left, JoinType::Anti] {
        let out = if matches!(jt, JoinType::Anti) {
            out_pb(&[1, 4], &[])
        } else {
            out_pb(&[1, 4], &[1, 4])
        };
        let node = nl_node(
            &fix,
            jt,
            vec![
                JoinQual { probe_col: 4, build_col: 1, op: JoinCmp::Ge },
                JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Le },
            ],
            out,
        );
        check(&fix, &node, &format!("nl-between {jt:?}"));
    }
}

#[test]
fn nl_cross_product() {
    // No quals at all: the witnessed cross product.
    let fix = fixture("nlx", true, 60, 90);
    for jt in [JoinType::Inner, JoinType::Left] {
        let node = nl_node(&fix, jt, Vec::new(), out_pb(&[1], &[4]));
        check(&fix, &node, &format!("nl-cross {jt:?}"));
    }
}

#[test]
fn nl_side_preds() {
    let fix = fixture("nlp", true, 800, 1200);
    let bp = PredSpec::all(vec![PredTerm::new(4, CmpOp::Between, 5, 40, TypMeta::INT8)]);
    let pp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Between, 3, 18, TypMeta::INT4)]);
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1, 2], &[])
        } else {
            out_pb(&[1, 2], &[4])
        };
        let node = nest_loop_node(
            &fix.be.bank,
            &fix.pe.bank,
            0,
            jt,
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Gt }],
            Some(bp.clone()),
            Some(pp.clone()),
            out,
            0,
        )
        .expect("construct nl");
        check(&fix, &node, &format!("nl-side-pred {jt:?}"));
    }
}

#[test]
fn nl_text_out_columns() {
    // Text payload columns ride the build arena / probe raw lanes exactly
    // as in the keyed stencil.
    let fix = fixture("nlt", true, 400, 600);
    let node = nl_node(
        &fix,
        JoinType::Inner,
        vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Eq }],
        out_pb(&[1, 5], &[3, 5]),
    );
    check(&fix, &node, "nl-text-out");
}

#[test]
fn nl_empty_sides() {
    let fix = fixture("nle", true, 300, 300);
    let eb = empty_engine("nlb");
    let ep = empty_engine("nlp2");
    for jt in ALL {
        let out = if matches!(jt, JoinType::Semi | JoinType::Anti) {
            out_pb(&[1], &[])
        } else {
            out_pb(&[1], &[4])
        };
        let quals = vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }];
        // empty build: Inner/Semi answer empty; Left/Anti keep every
        // probe row (null-extended / all-unmatched).
        let node =
            nest_loop_node(&eb.bank, &fix.pe.bank, 0, jt, quals.clone(), None, None, out.clone(), 0)
                .expect("construct");
        let got = run_hash_join(&eb.ctx(), &fix.pe.ctx(), &[], &node).expect("run");
        let want = oracle_answer(&node, oracle_join(&node, &[], &fix.prows));
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "nl-empty-build {jt:?}");
        // empty probe: zero rows for every join type.
        let node2 = nest_loop_node(&fix.be.bank, &ep.bank, 0, jt, quals, None, None, out, 0)
            .expect("construct");
        let got2 = run_hash_join(&fix.be.ctx(), &ep.ctx(), &[], &node2).expect("run");
        assert_eq!(got2.nrows, 0, "nl-empty-probe {jt:?}");
    }
}

#[test]
fn nl_product_budget_refusal_typed() {
    // 9000 x 9000 = 81M pairs > the 2^26 budget: typed refusal, no run.
    let fix = fixture("nlbig", true, 9000, 9000);
    let node = nl_node(
        &fix,
        JoinType::Inner,
        vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
        out_pb(&[1], &[4]),
    );
    match run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node) {
        Err(JoinRefuse::ProductExceedsBudget { est_pairs, budget }) => {
            assert_eq!(budget, NL_PRODUCT_BUDGET_PAIRS);
            assert!(est_pairs > budget, "est {est_pairs} vs budget {budget}");
        }
        other => panic!("expected product refusal, got {other:?}"),
    }
    // A selective build pred brings the witnessed product back under
    // budget: the same shape then serves.
    let bp = PredSpec::all(vec![PredTerm::new(2, CmpOp::Eq, 3, 0, TypMeta::INT4)]);
    let node2 = nest_loop_node(
        &fix.be.bank,
        &fix.pe.bank,
        0,
        JoinType::Inner,
        vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
        Some(bp),
        None,
        out_pb(&[1], &[4]),
        0,
    )
    .expect("construct");
    check(&fix, &node2, "nl-product-under-budget-after-pred");
}

#[test]
fn nl_constructor_refusals_typed() {
    let fix = fixture("nlc", true, 100, 100);
    let (b, p) = (&fix.be.bank, &fix.pe.bank);
    // A residual Eq qual on the KEYED constructor stays refused.
    assert!(matches!(
        join_node(
            b,
            p,
            &[],
            0,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            vec![JoinQual { probe_col: 2, build_col: 2, op: JoinCmp::Eq }],
            None,
            None,
            Vec::new(),
            out_pb(&[1], &[4]),
            0,
            false
        ),
        Err(JoinRefuse::Unsupported { what: "keyed-eq-qual" })
    ));
    // A keyless side with no planned column has no decode lane: refused.
    assert!(matches!(
        nest_loop_node(b, p, 0, JoinType::Inner, Vec::new(), None, None, out_pb(&[1], &[]), 0),
        Err(JoinRefuse::Unsupported { what: "nl-side-no-cols" })
    ));
    // Varlena qual faces stay refused in the keyless shape too.
    assert!(matches!(
        nest_loop_node(
            b,
            p,
            0,
            JoinType::Inner,
            vec![JoinQual { probe_col: 3, build_col: 3, op: JoinCmp::Eq }],
            None,
            None,
            out_pb(&[1], &[4]),
            0
        ),
        Err(JoinRefuse::Unsupported { what: "qual-face-no-word-embed" })
    ));
}

#[test]
fn nl_agg_ungrouped_all_types() {
    let fix = fixture("nlag", true, 600, 900);
    for jt in ALL {
        let aggs: Vec<JoinAggReq> =
            if matches!(jt, JoinType::Semi | JoinType::Anti) {
                vec![
                    JoinAggReq::col(JoinAggOp::CountStar, None),
                    JoinAggReq::col(JoinAggOp::CountCol, pin(4)),
                    JoinAggReq::col(JoinAggOp::Sum, pin(4)),
                    JoinAggReq::col(JoinAggOp::Min, pin(1)),
                ]
            } else {
                vec![
                    JoinAggReq::col(JoinAggOp::CountStar, None),
                    JoinAggReq::col(JoinAggOp::CountCol, bin(3)),
                    JoinAggReq::col(JoinAggOp::Sum, bin(4)),
                    JoinAggReq::col(JoinAggOp::Min, pin(1)),
                    JoinAggReq::col(JoinAggOp::Max, bin(4)),
                ]
            };
        let anode = nest_loop_agg_node(
            &fix.be.bank,
            &fix.pe.bank,
            0,
            jt,
            vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Lt }],
            None,
            None,
            aggs,
            Vec::new(),
            0,
        )
        .expect("construct nl agg");
        check_agg(&fix, &anode, &format!("nl-agg-ungrouped {jt:?}"));
    }
}

#[test]
fn nl_agg_grouped() {
    let fix = fixture("nlgg", true, 500, 800);
    for jt in ALL {
        // grouped count(*) by probe g (col 2, null-free) and probe k
        // (col 1, NULLs -> one NULL group).
        for gcol in [1u32, 2] {
            let anode = nest_loop_agg_node(
                &fix.be.bank,
                &fix.pe.bank,
                0,
                jt,
                vec![JoinQual { probe_col: 4, build_col: 4, op: JoinCmp::Le }],
                None,
                None,
                vec![JoinAggReq::col(JoinAggOp::CountStar, None)],
                vec![JoinOut { side: JoinSide::Probe, col: gcol }],
                0,
            )
            .expect("construct nl grouped");
            let got = run_hash_join_agg(&fix.be.ctx(), &fix.pe.ctx(), &[], &anode)
                .unwrap_or_else(|e| panic!("nl grouped {jt:?} g{gcol}: refused: {e}"));
            let want =
                oracle_group_count(&anode, &oracle_join(&anode.join, &fix.brows, &fix.prows));
            let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
            gl.sort();
            wl.sort();
            assert_eq!(gl, wl, "nl-grouped-count {jt:?} g{gcol}");
        }
    }
}

// ---------------------------------------------------------------------------
// [corrsubq] rung 2: the grouped build (aggregate-result-as-map)
// ---------------------------------------------------------------------------

/// Oracle map over the build rows: key -> (count(*), count(v), sum, min,
/// max) of the lane column (strict folds; NULL keys form no group).
type FoldCell = (i64, i64, Option<i64>, Option<i64>, Option<i64>);

fn fold_map(rows: &[Row], lane: u32) -> std::collections::HashMap<i64, FoldCell> {
    let mut m = std::collections::HashMap::new();
    for b in rows {
        let Some(k) = b[0].i() else { continue };
        let e: &mut FoldCell = m.entry(k).or_insert((0, 0, None, None, None));
        e.0 += 1;
        if let Some(v) = b[(lane - 1) as usize].i() {
            e.1 += 1;
            e.2 = Some(e.2.unwrap_or(0) + v);
            e.3 = Some(e.3.map_or(v, |x| x.min(v)));
            e.4 = Some(e.4.map_or(v, |x| x.max(v)));
        }
    }
    m
}

fn fold_cell(c: Option<&FoldCell>, op: JoinAggOp, missing: Option<i64>) -> Option<i64> {
    match c {
        None => missing,
        Some(e) => match op {
            JoinAggOp::CountStar => Some(e.0),
            JoinAggOp::CountCol | JoinAggOp::CountDistinct => Some(e.1),
            JoinAggOp::Sum => e.2,
            JoinAggOp::Min => e.3,
            JoinAggOp::Max => e.4,
        },
    }
}

fn check_rows(fix: &Fix, node: &JoinNode, rows: Vec<Vec<RV>>, what: &str) {
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], node)
        .unwrap_or_else(|e| panic!("{what}: refused: {e}"));
    let want = oracle_answer(node, rows);
    let mut gl = to_lines(&got);
    let mut wl = to_lines(&want);
    gl.sort();
    wl.sort();
    assert_eq!(gl.len(), wl.len(), "{what}: row count {} vs oracle {}", got.nrows, want.nrows);
    assert_eq!(gl, wl, "{what}: rendered identity");
}

#[test]
fn join_build_fold_semi_map_law() {
    let fix = fixture("fold1", true, 6000, 9000);
    let cases = [
        (JoinAggOp::Max, JoinCmp::Ge, 4u32),
        (JoinAggOp::Max, JoinCmp::Lt, 4),
        (JoinAggOp::Min, JoinCmp::Le, 4),
        (JoinAggOp::Min, JoinCmp::Ne, 4),
        (JoinAggOp::Sum, JoinCmp::Lt, 2),
        (JoinAggOp::Sum, JoinCmp::Ge, 2),
        (JoinAggOp::CountStar, JoinCmp::Gt, 4),
        (JoinAggOp::CountStar, JoinCmp::Ge, 4),
        (JoinAggOp::CountCol, JoinCmp::Le, 4),
        (JoinAggOp::Max, JoinCmp::Eq, 4),
        (JoinAggOp::CountStar, JoinCmp::Eq, 2),
        (JoinAggOp::CountCol, JoinCmp::Eq, 4),
    ];
    for (op, cmp, lane) in cases {
        let m = fold_map(&fix.brows, lane);
        let missing = matches!(op, JoinAggOp::CountStar | JoinAggOp::CountCol).then_some(0);
        let q = JoinQual { probe_col: lane, build_col: lane, op: cmp };
        let (quals, eq) = if cmp == JoinCmp::Eq { (Vec::new(), Some(q)) } else { (vec![q], None) };
        let mut node = join_node(
            &fix.be.bank,
            &fix.pe.bank,
            &[],
            0,
            JoinType::Semi,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            quals,
            None,
            None,
            Vec::new(),
            out_pb(&[1, lane], &[]),
            fix.be.bank.rows_total(),
            false,
        )
        .expect("construct");
        attach_build_fold(
            &mut node,
            &fix.be.bank,
            &fix.pe.bank,
            BuildFold { col: lane, op, missing },
            eq,
        )
        .expect("fold admission");
        let rows: Vec<Vec<RV>> = fix
            .prows
            .iter()
            .filter(|p| {
                let pv = p[(lane - 1) as usize].i();
                let cell = fold_cell(p[0].i().and_then(|k| m.get(&k)), op, missing);
                match (pv, cell) {
                    (Some(pv), Some(c)) => q.eval_v(pv, true, c, true),
                    _ => false,
                }
            })
            .map(|p| vec![p[0].clone(), p[(lane - 1) as usize].clone()])
            .collect();
        check_rows(&fix, &node, rows, &format!("semi fold {op:?} {cmp:?} lane {lane}"));
    }
}

#[test]
fn join_build_fold_left_projection() {
    let fix = fixture("fold2", true, 6000, 9000);
    for (op, lane) in [
        (JoinAggOp::Max, 4u32),
        (JoinAggOp::Min, 4),
        (JoinAggOp::Sum, 2),
        (JoinAggOp::CountStar, 4),
        (JoinAggOp::CountCol, 4),
    ] {
        let m = fold_map(&fix.brows, lane);
        let missing = matches!(op, JoinAggOp::CountStar | JoinAggOp::CountCol).then_some(0);
        let mut node = base_node(
            &fix,
            JoinType::Left,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out_pb(&[1, 2], &[lane]),
        );
        attach_build_fold(
            &mut node,
            &fix.be.bank,
            &fix.pe.bank,
            BuildFold { col: lane, op, missing },
            None,
        )
        .expect("fold admission");
        // The engine's Left law: an absent group is NULL (the count 0
        // fill is the consumer's); every probe row emits exactly once.
        let rows: Vec<Vec<RV>> = fix
            .prows
            .iter()
            .map(|p| {
                let cell = fold_cell(p[0].i().and_then(|k| m.get(&k)), op, None);
                vec![p[0].clone(), p[1].clone(), cell.map_or(RV::Null, RV::I)]
            })
            .collect();
        check_rows(&fix, &node, rows, &format!("left fold {op:?} lane {lane}"));
    }
}

#[test]
fn join_build_fold_admission_typed() {
    let fix = fixture("fold3", false, 500, 500);
    let mk = |jt: JoinType, out: Vec<JoinOut>| {
        base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out)
    };
    let f = |col: u32, op: JoinAggOp, missing: Option<i64>| BuildFold { col, op, missing };
    // the lane must be a qual/out payload column
    let mut n = mk(JoinType::Semi, out_pb(&[1], &[]));
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(4, JoinAggOp::Max, None), None),
        Err(JoinRefuse::Unsupported { what: "build-fold-col" })
    ));
    // Sum over an int8 lane refuses (width law); int4 admits
    let mut n = mk(JoinType::Left, out_pb(&[1], &[4]));
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(4, JoinAggOp::Sum, None), None),
        Err(JoinRefuse::Unsupported { what: "build-fold-sum-width" })
    ));
    let mut n = mk(JoinType::Left, out_pb(&[1], &[2]));
    let ok = attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(2, JoinAggOp::Sum, None), None);
    assert!(ok.is_ok());
    // missing is the count ops' law only
    let mut n = mk(JoinType::Left, out_pb(&[1], &[4]));
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(4, JoinAggOp::Max, Some(0)), None),
        Err(JoinRefuse::Unsupported { what: "build-fold-missing" })
    ));
    // join type: Inner/Anti refuse
    let mut n = mk(JoinType::Inner, out_pb(&[1], &[4]));
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(4, JoinAggOp::Max, None), None),
        Err(JoinRefuse::Unsupported { what: "build-fold-join-type" })
    ));
    // the eq qual must read the fold lane
    let mut n = mk(JoinType::Semi, out_pb(&[1], &[]));
    let eq = JoinQual { probe_col: 2, build_col: 2, op: JoinCmp::Eq };
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(4, JoinAggOp::Max, None), Some(eq)),
        Err(JoinRefuse::Unsupported { what: "build-fold-eq-qual" })
    ));
    // a text lane never folds
    let mut n = mk(JoinType::Left, out_pb(&[1], &[3]));
    assert!(matches!(
        attach_build_fold(&mut n, &fix.be.bank, &fix.pe.bank, f(3, JoinAggOp::Max, None), None),
        Err(JoinRefuse::Unsupported { what: "build-fold-col" })
    ));
}

// ---------------------------------------------------------------------------
// [crossdim-or] staged disjunction / [semianti-flt] membership stages
// ---------------------------------------------------------------------------

fn or_row(t: &sqe::joins::CaseTest, r: &Row, col: u32) -> bool {
    use sqe::joins::CaseTest;
    let c = &r[(col - 1) as usize];
    match t {
        CaseTest::Word(pt) => match c.i() {
            Some(v) => pt.eval_v(v, true),
            None => false,
        },
        CaseTest::Bytes(vt) => match c.t() {
            Some(b) => vt.eval(b),
            None => false,
        },
        CaseTest::Packed(pt, _) => match c.i() {
            Some(v) => pt.eval_v(v, true),
            None => false,
        },
        CaseTest::InWords(ws) => match c.i() {
            Some(v) => ws.binary_search(&v).is_ok(),
            None => false,
        },
        CaseTest::And(ts) => ts.iter().all(|t| or_row(t, r, col)),
    }
}

fn o_staged_or(so: &sqe::joins::StagedOr, b: &Row, p: &Row) -> bool {
    (0..so.narms).any(|a| {
        so.terms.iter().filter(|t| t.arm == a).all(|t| {
            let r = match t.site {
                JoinSide::Probe => p,
                JoinSide::Build => b,
                JoinSide::Dim(_) => unreachable!("2-way oracle"),
            };
            or_row(&t.test, r, t.col)
        })
    })
}

#[test]
fn join_staged_or_word_text_arms() {
    use sqe::ir::{BytesCmp, VarOp, VarPredTerm};
    use sqe::joins::{attach_staged_or, CaseTest, OrTerm, StagedOr};
    let fix = fixture("stor", true, 4000, 9000);
    let out = out_pb(&[1, 2], &[4]);
    let mut node =
        base_node(&fix, JoinType::Inner, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
    let ty5 = TypMeta::TEXT_C;
    let so = StagedOr {
        narms: 2,
        terms: vec![
            OrTerm {
                site: JoinSide::Probe,
                col: 2,
                arm: 0,
                test: CaseTest::Word(PredTerm::new(2, CmpOp::Between, 0, 11, TypMeta::INT4)),
            },
            OrTerm {
                site: JoinSide::Build,
                col: 5,
                arm: 0,
                test: CaseTest::Bytes(VarPredTerm::new(
                    5,
                    VarOp::CmpBytes(BytesCmp::Eq),
                    b"s1".to_vec(),
                    ty5,
                )),
            },
            OrTerm {
                site: JoinSide::Probe,
                col: 2,
                arm: 1,
                test: CaseTest::Word(PredTerm::new(2, CmpOp::Eq, 13, 0, TypMeta::INT4)),
            },
        ],
    };
    attach_staged_or(&mut node, &fix.be.bank, &fix.pe.bank, &[], so.clone()).expect("attach");
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("run");
    // Independent oracle: inner pairs where some arm holds (3VL).
    let mut rows: Vec<Vec<RV>> = Vec::new();
    for p in &fix.prows {
        for b in &fix.brows {
            if o_keys_eq(&node, b, p) && o_staged_or(&so, b, p) {
                rows.push(
                    node.out
                        .iter()
                        .map(|o| match o.side {
                            JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                            JoinSide::Build => b[(o.col - 1) as usize].clone(),
                            JoinSide::Dim(_) => unreachable!(),
                        })
                        .collect(),
                );
            }
        }
    }
    let want = oracle_answer(&node, rows);
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert!(!gl.is_empty(), "disjunction selects rows");
    assert_eq!(gl, wl, "staged-or identity");
}

#[test]
fn join_staged_or_admission() {
    use sqe::joins::{attach_staged_or, CaseTest, OrTerm, StagedOr};
    let fix = fixture("storadm", false, 600, 900);
    let term = |site, arm| OrTerm {
        site,
        col: 2,
        arm,
        test: CaseTest::Word(PredTerm::new(2, CmpOp::Eq, 1, 0, TypMeta::INT4)),
    };
    let mk = |jt| {
        base_node(&fix, jt, vec![JoinKey { build_col: 1, probe_col: 1 }], out_pb(&[1], &[]))
    };
    // non-Inner refuses
    let mut n = mk(JoinType::Semi);
    let so = StagedOr { narms: 2, terms: vec![term(JoinSide::Probe, 0), term(JoinSide::Build, 1)] };
    assert!(matches!(
        attach_staged_or(&mut n, &fix.be.bank, &fix.pe.bank, &[], so.clone()),
        Err(JoinRefuse::Unsupported { what: "staged-or-join-type" })
    ));
    // an untermed arm is vacuous — refused
    let mut n = mk(JoinType::Inner);
    let so2 = StagedOr { narms: 2, terms: vec![term(JoinSide::Probe, 0)] };
    assert!(matches!(
        attach_staged_or(&mut n, &fix.be.bank, &fix.pe.bank, &[], so2),
        Err(JoinRefuse::Unsupported { what: "staged-or-shape" })
    ));
    // a text column under a WORD test refuses the face
    let mut n = mk(JoinType::Inner);
    let bad = OrTerm {
        site: JoinSide::Probe,
        col: 3,
        arm: 0,
        test: CaseTest::Word(PredTerm::new(3, CmpOp::Eq, 1, 0, TypMeta::INT4)),
    };
    let so3 = StagedOr { narms: 2, terms: vec![bad, term(JoinSide::Build, 1)] };
    assert!(matches!(
        attach_staged_or(&mut n, &fix.be.bank, &fix.pe.bank, &[], so3),
        Err(JoinRefuse::Unsupported { what: "staged-or-face" })
    ));
}

#[test]
fn join_staged_or_range_in_arms() {
    use sqe::ir::{encode_in_needles, VarOp, VarPredTerm};
    use sqe::joins::{attach_staged_or, CaseTest, OrTerm, StagedOr};
    let fix = fixture("storin", true, 4000, 9000);
    let out = out_pb(&[1, 4], &[5]);
    let mut node =
        base_node(&fix, JoinType::Inner, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
    let in_bytes = |col: u32, imgs: &[&str]| {
        CaseTest::Bytes(VarPredTerm::new(
            col,
            VarOp::InBytes,
            encode_in_needles(imgs.iter().map(|s| s.as_bytes().to_vec()).collect()),
            TypMeta::TEXT_C,
        ))
    };
    // Arm 0: a both-inclusive range over a NULLABLE word lane (boundary
    // rows at 10 and 20) plus a byte IN image set; arm 1: a >2-element
    // sorted word IN-list (domain boundaries included) plus a 2-element
    // In2 over a nullable build lane; arm 2: an EMPTY word IN-list (the
    // arm can never fire) plus a word eq.
    let so = StagedOr {
        narms: 3,
        terms: vec![
            OrTerm {
                site: JoinSide::Probe,
                col: 4,
                arm: 0,
                test: CaseTest::Word(PredTerm::new(4, CmpOp::Between, 10, 20, TypMeta::INT8)),
            },
            OrTerm { site: JoinSide::Build, col: 5, arm: 0, test: in_bytes(5, &["s1", "s3"]) },
            OrTerm {
                site: JoinSide::Probe,
                col: 2,
                arm: 1,
                test: CaseTest::InWords(vec![0, 1, 5, 9, 23]),
            },
            OrTerm {
                site: JoinSide::Build,
                col: 4,
                arm: 1,
                test: CaseTest::Word(PredTerm::new(4, CmpOp::In2, 7, 13, TypMeta::INT8)),
            },
            OrTerm { site: JoinSide::Probe, col: 2, arm: 2, test: CaseTest::InWords(Vec::new()) },
            OrTerm {
                site: JoinSide::Build,
                col: 2,
                arm: 2,
                test: CaseTest::Word(PredTerm::new(2, CmpOp::Eq, 3, 0, TypMeta::INT4)),
            },
        ],
    };
    attach_staged_or(&mut node, &fix.be.bank, &fix.pe.bank, &[], so.clone()).expect("attach");
    let got = run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &node).expect("run");
    let mut rows: Vec<Vec<RV>> = Vec::new();
    for p in &fix.prows {
        for b in &fix.brows {
            if o_keys_eq(&node, b, p) && o_staged_or(&so, b, p) {
                rows.push(
                    node.out
                        .iter()
                        .map(|o| match o.side {
                            JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                            JoinSide::Build => b[(o.col - 1) as usize].clone(),
                            JoinSide::Dim(_) => unreachable!(),
                        })
                        .collect(),
                );
            }
        }
    }
    let want = oracle_answer(&node, rows);
    let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
    gl.sort();
    wl.sort();
    assert!(!gl.is_empty(), "range/IN disjunction selects rows");
    assert_eq!(gl, wl, "staged-or range/IN identity");
}

#[test]
fn join_staged_or_range_in_admission() {
    use sqe::joins::{attach_staged_or, CaseTest, OrTerm, StagedOr};
    let fix = fixture("storinadm", false, 600, 900);
    let word = |site, arm| OrTerm {
        site,
        col: 2,
        arm,
        test: CaseTest::Word(PredTerm::new(2, CmpOp::Eq, 1, 0, TypMeta::INT4)),
    };
    let mk = || {
        base_node(
            &fix,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out_pb(&[1], &[]),
        )
    };
    // a word-face lane under a Packed (mantissa-grid) test refuses: the
    // authored scale can never match a non-packed face.
    let mut n = mk();
    let bad = OrTerm {
        site: JoinSide::Probe,
        col: 2,
        arm: 0,
        test: CaseTest::Packed(PredTerm::new(2, CmpOp::Between, 100, 1100, TypMeta::NUMERIC), 2),
    };
    let so = StagedOr { narms: 2, terms: vec![bad, word(JoinSide::Build, 1)] };
    assert!(matches!(
        attach_staged_or(&mut n, &fix.be.bank, &fix.pe.bank, &[], so),
        Err(JoinRefuse::Unsupported { what: "staged-or-face" })
    ));
    // a text lane under a word IN-list refuses the face.
    let mut n = mk();
    let bad = OrTerm {
        site: JoinSide::Probe,
        col: 3,
        arm: 0,
        test: CaseTest::InWords(vec![1, 2, 3]),
    };
    let so = StagedOr { narms: 2, terms: vec![bad, word(JoinSide::Build, 1)] };
    assert!(matches!(
        attach_staged_or(&mut n, &fix.be.bank, &fix.pe.bank, &[], so),
        Err(JoinRefuse::Unsupported { what: "staged-or-face" })
    ));
}

fn o_filter_pass(f: &sqe::joins::FilterStage, host: &Row, stage_rows: &[Row]) -> bool {
    let hit = stage_rows.iter().any(|s| {
        f.keys.iter().all(|k| {
            match (host[(k.host_col - 1) as usize].i(), s[(k.stage_col - 1) as usize].i()) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }) && f.quals.iter().all(|q| {
            match (host[(q.host_col - 1) as usize].i(), s[(q.stage_col - 1) as usize].i()) {
                (Some(a), Some(b)) => JoinQual { probe_col: 0, build_col: 0, op: q.op }
                    .eval_v(a, true, b, true),
                _ => false,
            }
        })
    });
    hit != f.anti
}

#[test]
fn join_filter_stage_semi_anti() {
    use sqe::joins::{
        attach_filter_stages, run_hash_join_flt, FilterKey, FilterQual, FilterStage,
    };
    let fix = fixture("fltstg", true, 4000, 9000);
    // The stage scans the BUILD bank a second time (the two-scan shape):
    // host = probe, keys k = k, residual v <> v (3VL, NULL never matches).
    for anti in [false, true] {
        let out = out_pb(&[1, 4], &[2]);
        let mut node =
            base_node(&fix, JoinType::Inner, vec![JoinKey { build_col: 1, probe_col: 1 }], out);
        let stage = FilterStage {
            anti,
            host: JoinSide::Probe,
            keys: vec![FilterKey { host_col: 1, stage_col: 1 }],
            quals: vec![FilterQual { host_col: 4, stage_col: 4, op: JoinCmp::Ne }],
            pred: Some(PredSpec::all(vec![PredTerm::new(
                1,
                CmpOp::Between,
                0,
                49,
                TypMeta::INT8,
            )])),
            text_eqs: Vec::new(),
            fold: None,
            src: sqe::joins::StageSrc::Bank,
            rows_hint: 0,
        };
        attach_filter_stages(&mut node, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![
            stage.clone(),
        ])
        .expect("attach");
        let got = run_hash_join_flt(&fix.be.ctx(), &fix.pe.ctx(), &[], &[&fix.be.ctx()], &node)
            .expect("run");
        // Independent oracle: the stage filters PROBE rows first (its
        // own scan pred applies to the stage rows), then the inner join.
        let srows: Vec<Row> = fix
            .brows
            .iter()
            .filter(|r| o_pred(&stage.pred, r))
            .cloned()
            .collect();
        let mut rows: Vec<Vec<RV>> = Vec::new();
        for p in fix.prows.iter().filter(|p| o_filter_pass(&stage, p, &srows)) {
            for b in &fix.brows {
                if o_keys_eq(&node, b, p) {
                    rows.push(
                        node.out
                            .iter()
                            .map(|o| match o.side {
                                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                                JoinSide::Build => b[(o.col - 1) as usize].clone(),
                                JoinSide::Dim(_) => unreachable!(),
                            })
                            .collect(),
                    );
                }
            }
        }
        let want = oracle_answer(&node, rows);
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert!(!gl.is_empty(), "anti={anti}: filter keeps rows");
        assert_eq!(gl, wl, "anti={anti}: filter-stage identity");
    }
}

#[test]
fn join_filter_stage_admission() {
    use sqe::joins::{attach_filter_stages, FilterKey, FilterQual, FilterStage};
    let fix = fixture("fltadm", false, 600, 900);
    let mk = || {
        base_node(
            &fix,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out_pb(&[1], &[]),
        )
    };
    let stage = |quals: Vec<FilterQual>, keys: Vec<FilterKey>| FilterStage {
        anti: false,
        host: JoinSide::Probe,
        keys,
        quals,
        pred: None,
        text_eqs: Vec::new(),
        fold: None,
        src: sqe::joins::StageSrc::Bank,
        rows_hint: 0,
    };
    let k11 = FilterKey { host_col: 1, stage_col: 1 };
    // an Eq residual belongs in the key lanes
    let mut n = mk();
    let bad = stage(
        vec![FilterQual { host_col: 4, stage_col: 4, op: JoinCmp::Eq }],
        vec![k11],
    );
    assert!(matches!(
        attach_filter_stages(&mut n, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![bad]),
        Err(JoinRefuse::Unsupported { what: "filter-eq-qual" })
    ));
    // keys are mandatory
    let mut n = mk();
    let nokey = stage(Vec::new(), Vec::new());
    assert!(matches!(
        attach_filter_stages(&mut n, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![nokey]),
        Err(JoinRefuse::Unsupported { what: "filter-no-equi-key" })
    ));
    // a text key face refuses
    let mut n = mk();
    let badk = stage(Vec::new(), vec![FilterKey { host_col: 3, stage_col: 1 }]);
    assert!(matches!(
        attach_filter_stages(&mut n, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![badk]),
        Err(JoinRefuse::Unsupported { what: "filter-key-face" })
    ));
    // a misaligned run refuses typed (no ctxs for the attached stage)
    let mut n = mk();
    attach_filter_stages(&mut n, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![stage(
        Vec::new(),
        vec![k11],
    )])
    .expect("attach");
    assert!(matches!(
        run_hash_join(&fix.be.ctx(), &fix.pe.ctx(), &[], &n),
        Err(JoinRefuse::Unsupported { what: "filter-ctx-align" })
    ));
}

/// [mapstage] Independent oracle for one host row against a WORD map
/// stage: group the stage rows by the stage keys, fold, apply the quals
/// to the cell (missing key = the empty-group answer; NULL never true).
fn o_map_pass(f: &sqe::joins::FilterStage, host: &Row, stage_rows: &[Row]) -> bool {
    use sqe::joins::StageFold;
    let Some(StageFold::Word(bf)) = &f.fold else { unreachable!("word maps only") };
    let keyed: Vec<&Row> = stage_rows
        .iter()
        .filter(|s| {
            f.keys.iter().all(|k| {
                match (host[(k.host_col - 1) as usize].i(), s[(k.stage_col - 1) as usize].i()) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                }
            })
        })
        .collect();
    let cell: Option<i64> = if keyed.is_empty() {
        bf.missing
    } else {
        let vals: Vec<i64> =
            keyed.iter().filter_map(|s| s[(bf.col - 1) as usize].i()).collect();
        match bf.op {
            JoinAggOp::CountStar => Some(keyed.len() as i64),
            JoinAggOp::CountCol => Some(vals.len() as i64),
            JoinAggOp::CountDistinct => {
                Some(vals.iter().copied().collect::<std::collections::BTreeSet<_>>().len() as i64)
            }
            JoinAggOp::Sum => (!vals.is_empty()).then(|| vals.iter().sum()),
            JoinAggOp::Min => vals.iter().copied().min(),
            JoinAggOp::Max => vals.iter().copied().max(),
        }
    };
    let Some(bv) = cell else { return false };
    f.quals.iter().all(|q| match host[(q.host_col - 1) as usize].i() {
        Some(a) => {
            JoinQual { probe_col: 0, build_col: 0, op: q.op }.eval_v(a, true, bv, true)
        }
        None => false,
    })
}

#[test]
fn join_filter_stage_fold_word() {
    use sqe::joins::{
        attach_filter_stages, run_hash_join_flt, BuildFold, FilterKey, FilterQual, FilterStage,
        StageFold,
    };
    let fix = fixture("fltmap", false, 4000, 9000);
    // (fold col, op, missing, stage key col, qual host col, cmp): the
    // count legs key on a column whose probe domain exceeds the build's,
    // so the empty-group answer is observable post-join; sum keys the
    // narrow lane (the width law); Eq reads the collapsed cell.
    let legs: Vec<(u32, JoinAggOp, Option<i64>, u32, u32, JoinCmp)> = vec![
        (4, JoinAggOp::Max, None, 1, 4, JoinCmp::Lt),
        (4, JoinAggOp::Min, None, 1, 4, JoinCmp::Ge),
        (2, JoinAggOp::Sum, None, 1, 4, JoinCmp::Le),
        (2, JoinAggOp::CountStar, Some(0), 2, 4, JoinCmp::Gt),
        (4, JoinAggOp::CountCol, Some(0), 2, 4, JoinCmp::Eq),
        (4, JoinAggOp::CountCol, None, 2, 4, JoinCmp::Gt),
    ];
    for (col, op, missing, keyc, hostq, cmp) in legs {
        let out = out_pb(&[1, 4], &[2]);
        let mut node = base_node(
            &fix,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out,
        );
        let stage = FilterStage {
            anti: false,
            host: JoinSide::Probe,
            keys: vec![FilterKey { host_col: keyc, stage_col: keyc }],
            quals: vec![FilterQual { host_col: hostq, stage_col: col, op: cmp }],
            pred: None,
            text_eqs: Vec::new(),
            fold: Some(StageFold::Word(BuildFold { col, op, missing })),
            src: sqe::joins::StageSrc::Bank,
            rows_hint: 0,
        };
        attach_filter_stages(&mut node, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![
            stage.clone(),
        ])
        .expect("attach");
        let got = run_hash_join_flt(&fix.be.ctx(), &fix.pe.ctx(), &[], &[&fix.be.ctx()], &node)
            .expect("run");
        let mut rows: Vec<Vec<RV>> = Vec::new();
        for p in fix.prows.iter().filter(|p| o_map_pass(&stage, p, &fix.brows)) {
            for b in &fix.brows {
                if o_keys_eq(&node, b, p) {
                    rows.push(
                        node.out
                            .iter()
                            .map(|o| match o.side {
                                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                                JoinSide::Build => b[(o.col - 1) as usize].clone(),
                                JoinSide::Dim(_) => unreachable!(),
                            })
                            .collect(),
                    );
                }
            }
        }
        let want = oracle_answer(&node, rows);
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "map leg {op:?} {cmp:?} identity");
    }
}

/// [mapstage] Independent oracle for one host row against a NUM map
/// stage at scale 0 (word lanes): group, fold by the op, gate on a
/// non-empty valid cell, compare the host operand against k * cell.
fn o_num_pass(f: &sqe::joins::FilterStage, host: &Row, stage_rows: &[Row]) -> bool {
    use sqe::joins::{NumCellOp, StageFold};
    let Some(StageFold::Num(nf)) = &f.fold else { unreachable!("num maps only") };
    let vals: Vec<i128> = stage_rows
        .iter()
        .filter(|s| {
            f.keys.iter().all(|k| {
                match (host[(k.host_col - 1) as usize].i(), s[(k.stage_col - 1) as usize].i()) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                }
            })
        })
        .filter_map(|s| s[(nf.col - 1) as usize].i().map(|v| v as i128))
        .collect();
    if vals.is_empty() {
        return false;
    }
    let cell = match nf.op {
        NumCellOp::Sum | NumCellOp::Avg => vals.iter().sum::<i128>(),
        NumCellOp::Min => *vals.iter().min().unwrap(),
        NumCellOp::Max => *vals.iter().max().unwrap(),
    };
    let rhs = nf.qual.k_m as i128 * cell;
    match host[(nf.qual.probe_col - 1) as usize].i() {
        Some(o) => {
            let o = o as i128;
            match nf.qual.op {
                JoinCmp::Lt => o < rhs,
                JoinCmp::Le => o <= rhs,
                JoinCmp::Gt => o > rhs,
                JoinCmp::Ge => o >= rhs,
                JoinCmp::Ne => o != rhs,
                JoinCmp::Eq => o == rhs,
            }
        }
        None => false,
    }
}

#[test]
fn join_filter_stage_fold_num_order() {
    use sqe::joins::{
        attach_filter_stages, run_hash_join_flt, FilterKey, FilterStage, NumCellOp,
        NumCellQual, NumFold, StageFold,
    };
    // Dup-heavy stage keys (col 2): groups collapse over many entries,
    // so an order fold and an accumulate fold answer DIFFERENT cells —
    // the identity is only satisfiable by the op-honest arm.
    let fix = fixture("fltnum", false, 4000, 9000);
    let legs: Vec<(NumCellOp, JoinCmp, i64)> = vec![
        (NumCellOp::Min, JoinCmp::Eq, 1),
        (NumCellOp::Min, JoinCmp::Ge, 1),
        (NumCellOp::Max, JoinCmp::Le, 1),
        (NumCellOp::Max, JoinCmp::Eq, 2),
        (NumCellOp::Sum, JoinCmp::Gt, 1),
    ];
    for (op, cmp, k_m) in legs {
        let out = out_pb(&[1, 4], &[2]);
        let mut node = base_node(
            &fix,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out,
        );
        let stage = FilterStage {
            anti: false,
            host: JoinSide::Probe,
            keys: vec![FilterKey { host_col: 2, stage_col: 2 }],
            quals: Vec::new(),
            pred: None,
            text_eqs: Vec::new(),
            fold: Some(StageFold::Num(NumFold {
                col: 4,
                op,
                scale: 0,
                qual: NumCellQual { probe_col: 4, op: cmp, k_m, k_scale: 0, probe_scale: 0 },
            })),
            src: sqe::joins::StageSrc::Bank,
            rows_hint: 0,
        };
        attach_filter_stages(&mut node, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![
            stage.clone(),
        ])
        .expect("attach");
        let got = run_hash_join_flt(&fix.be.ctx(), &fix.pe.ctx(), &[], &[&fix.be.ctx()], &node)
            .expect("run");
        let mut rows: Vec<Vec<RV>> = Vec::new();
        for p in fix.prows.iter().filter(|p| o_num_pass(&stage, p, &fix.brows)) {
            for b in &fix.brows {
                if o_keys_eq(&node, b, p) {
                    rows.push(
                        node.out
                            .iter()
                            .map(|o| match o.side {
                                JoinSide::Probe => p[(o.col - 1) as usize].clone(),
                                JoinSide::Build => b[(o.col - 1) as usize].clone(),
                                JoinSide::Dim(_) => unreachable!(),
                            })
                            .collect(),
                    );
                }
            }
        }
        let want = oracle_answer(&node, rows);
        let (mut gl, mut wl) = (to_lines(&got), to_lines(&want));
        gl.sort();
        wl.sort();
        assert_eq!(gl, wl, "num map leg {op:?} {cmp:?} identity");
    }
}

#[test]
fn join_filter_stage_fold_admission() {
    use sqe::joins::{
        attach_filter_stages, BuildFold, FilterKey, FilterQual, FilterStage, StageFold,
    };
    let fix = fixture("fltmadm", false, 600, 900);
    let mk = || {
        base_node(
            &fix,
            JoinType::Inner,
            vec![JoinKey { build_col: 1, probe_col: 1 }],
            out_pb(&[1], &[]),
        )
    };
    let stage = |anti: bool, quals: Vec<FilterQual>, fold: Option<StageFold>| FilterStage {
        anti,
        host: JoinSide::Probe,
        keys: vec![FilterKey { host_col: 1, stage_col: 1 }],
        quals,
        pred: None,
        text_eqs: Vec::new(),
        fold,
        src: sqe::joins::StageSrc::Bank,
        rows_hint: 0,
    };
    let q = |sc: u32, op: JoinCmp| FilterQual { host_col: 4, stage_col: sc, op };
    let wf = |col: u32, op: JoinAggOp, missing: Option<i64>| {
        Some(StageFold::Word(BuildFold { col, op, missing }))
    };
    let att = |n: &mut JoinNode, s: FilterStage| {
        attach_filter_stages(n, &fix.be.bank, &fix.pe.bank, &[&fix.be.bank], vec![s])
    };
    // anti has no cell law
    let mut n = mk();
    assert!(matches!(
        att(&mut n, stage(true, vec![q(4, JoinCmp::Lt)], wf(4, JoinAggOp::Max, None))),
        Err(JoinRefuse::Unsupported { what: "filter-fold-anti" })
    ));
    // residual against a non-cell stage column
    let mut n = mk();
    assert!(matches!(
        att(&mut n, stage(false, vec![q(2, JoinCmp::Lt)], wf(4, JoinAggOp::Max, None))),
        Err(JoinRefuse::Unsupported { what: "filter-fold-residual" })
    ));
    // a word fold with no qual reads nothing
    let mut n = mk();
    assert!(matches!(
        att(&mut n, stage(false, Vec::new(), wf(4, JoinAggOp::Max, None))),
        Err(JoinRefuse::Unsupported { what: "filter-fold-residual" })
    ));
    // sum keeps the narrow-width law at stage grain
    let mut n = mk();
    assert!(matches!(
        att(&mut n, stage(false, vec![q(4, JoinCmp::Lt)], wf(4, JoinAggOp::Sum, None))),
        Err(JoinRefuse::Unsupported { what: "filter-fold-sum-width" })
    ));
    // missing is the count ops' law only
    let mut n = mk();
    assert!(matches!(
        att(&mut n, stage(false, vec![q(4, JoinCmp::Lt)], wf(4, JoinAggOp::Max, Some(0)))),
        Err(JoinRefuse::Unsupported { what: "filter-fold-missing" })
    ));
    // Eq against the collapsed cell admits (the keyed-Eq law's counterpart)
    let mut n = mk();
    assert!(att(&mut n, stage(false, vec![q(4, JoinCmp::Eq)], wf(4, JoinAggOp::Max, None)))
        .is_ok());
}
