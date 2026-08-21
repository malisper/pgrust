//! The bench-grain identity rig (feature "rig") — byte-identity
//! instrumentation, never engine code: catalog stand-in, RON plan table,
//! SQL front end + scalar oracle, env reads, stdout drivers. Identity
//! legs: honest arm vs elected arm vs the NAIVE ORACLE through the ONE
//! render seam.

pub mod lower;
pub mod oracle;

use crate::answer::AnswerSet;
use crate::bank::{Bank, ColMeta, OpenOpts};
use crate::engine::{Engine, SqeConfig};
use crate::ir::{PlanNode, F_HONEST};
use crate::planner::{plan_from_ap, APlans};
use crate::render::to_lines;
use crate::typmeta::{oids, TypMeta, COLLATION_C};
use std::sync::OnceLock;

/// The 105-column hits schema — the rig's catalog stand-in; type OIDs
/// are decided HERE, at the catalog boundary.
pub fn hits_schema() -> Vec<ColMeta> {
    const RAW: &str = include_str!("../../rig/plans/hits_cols.tsv");
    RAW.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut it = l.split_whitespace();
            let attno: u32 = it.next().unwrap().parse().unwrap();
            let name = it.next().unwrap();
            let typlen: i32 = it.next().unwrap().parse().unwrap();
            let byval = it.next().unwrap() == "t";
            let typ = match (typlen, byval) {
                (-1, false) => TypMeta::varlena(oids::TEXT, COLLATION_C),
                (2, true) => TypMeta::INT2,
                (4, true) if name == "eventdate" => TypMeta::DATE,
                (4, true) => TypMeta::INT4,
                // The catalog's THREE timestamp columns (attnos 5/46/65).
                // clienteventtime/localeventtime falling to INT8 rendered
                // raw micros-since-2000 in the native q23 SELECT * dump —
                // a byte-law divergence against the (correct) server arm.
                (8, true)
                    if matches!(name, "eventtime" | "clienteventtime" | "localeventtime") =>
                {
                    TypMeta::byval(oids::TIMESTAMP, 8)
                }
                (8, true) => TypMeta::INT8,
                other => panic!("unhandled column shape {other:?} for {name}"),
            };
            ColMeta::new(attno, name, typ)
        })
        .collect()
}

pub fn thread_count() -> usize {
    std::env::var("PGRCBENCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
}

pub fn plans() -> &'static APlans {
    static P: OnceLock<APlans> = OnceLock::new();
    P.get_or_init(|| {
        let p: APlans =
            ron::from_str(include_str!("../../rig/plans/clickbench.ron")).expect("clickbench.ron");
        assert_eq!(p.version, 1);
        p
    })
}

pub fn open_engine(dir: &str) -> Engine {
    open_engine_cfg(dir, SqeConfig { threads: thread_count(), ..SqeConfig::default() })
}

pub fn open_engine_cfg(dir: &str, cfg: SqeConfig) -> Engine {
    let bank = Bank::open(dir, hits_schema(), &OpenOpts { bankstats: true, threads: cfg.threads });
    Engine::new(bank, cfg)
}

pub fn plan_ron(engine: &Engine, q: u32) -> Result<PlanNode, String> {
    let ap = plans()
        .plans
        .iter()
        .find(|p| p.q == q)
        .ok_or_else(|| format!("no RON plan for q{q}"))?;
    plan_from_ap(&engine.bank, &engine.faces, ap).map_err(|r| format!("lowering refused: {r}"))
}

/// [sqe-stmt-constant] diagnosis lever: re-encode an authored CountDesc/
/// limit bound in the SEAM's idiom (order=None, limit=MAX, topk native) so
/// the two bound encodings can be A/B'd inside one rig process
/// (PGRUST_SQE_BOUND_AS_TOPK=1).
pub fn maybe_server_bound(mut node: PlanNode) -> PlanNode {
    if std::env::var("PGRUST_SQE_BOUND_AS_TOPK").is_err() {
        return node;
    }
    if node.params.order == crate::ir::OrderBy::CountDesc && node.params.limit != usize::MAX {
        let count_slot = if node.params.key_exprs.is_empty() {
            node.params.group_cols.len() as u32
        } else {
            node.params.key_exprs.len() as u32
        };
        node.params.topk = Some(crate::ir::TopK {
            keys: vec![crate::ir::TopKKey { col: count_slot, desc: true, nulls_first: true, lo: None, trim: false }],
            n: node.params.offset + node.params.limit,
            native: true,
        });
        node.params.order = crate::ir::OrderBy::None;
        node.params.limit = usize::MAX;
        node.params.offset = 0;
        println!("BOUNDXFORM|q={}|topk_native n={}", node.q, node.params.topk.as_ref().unwrap().n);
    }
    node
}

/// Honest then elected once; arms asserted; (answer, honest ms, elected ms).
pub fn run_arms(engine: &Engine, node: &PlanNode) -> (AnswerSet, f64, f64) {
    let mut honest = node.clone();
    honest.params.flags |= F_HONEST;
    let t0 = std::time::Instant::now();
    let a_honest = engine.run(&honest);
    let ms_h = t0.elapsed().as_secs_f64() * 1e3;
    let t0 = std::time::Instant::now();
    let a_elect = engine.run(node);
    let ms_e = t0.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        a_honest, a_elect,
        "q={}: honest and elected arms disagree (typed)",
        node.q
    );
    assert_eq!(
        to_lines(&a_honest),
        to_lines(&a_elect),
        "q={}: honest and elected arms disagree (rendered)",
        node.q
    );
    (a_elect, ms_h, ms_e)
}

/// [sqe-park-knobs, measurement branch] matched-cadence law (Michael's
/// ruling, 2026-08-19): the rig must be able to record BOTH hot-cadence
/// (default, back-to-back reps) and gapped-cadence numbers so allocator
/// cells compose with v6.2 scoring. `SQE_RIG_REP_GAP_MS=<ms>` sleeps
/// before every timed rep (outside the timed window — parked-arrival /
/// purge-delay cadence); `SQE_RIG_REP_DUMP=1` prints per-rep `REPMS|`
/// lines so min AND median score off the same run.
fn rig_rep_gap_ms() -> u64 {
    static G: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *G.get_or_init(|| {
        std::env::var("SQE_RIG_REP_GAP_MS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    })
}

fn rig_rep_pause() {
    let g = rig_rep_gap_ms();
    if g > 0 {
        std::thread::sleep(std::time::Duration::from_millis(g));
    }
}

fn rig_rep_dump(q: u32, arm: &str, rep: usize, ms: f64) {
    static D: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *D.get_or_init(|| std::env::var("SQE_RIG_REP_DUMP").as_deref() == Ok("1")) {
        println!("REPMS|q={q}|arm={arm}|rep={rep}|gap_ms={}|ms={ms:.3}", rig_rep_gap_ms());
    }
}

/// Arm-GROUPED rep loops: the honest arm's whole rep loop first, then
/// the elected arm's (the PoC timing recipe); rep stability + cross-arm
/// identity asserted outside the timed windows.
pub fn run_arms_grouped(engine: &Engine, node: &PlanNode, reps: usize) -> (AnswerSet, f64, f64) {
    let mut honest = node.clone();
    honest.params.flags |= F_HONEST;
    let mut best_h = f64::MAX;
    let mut a_honest: Option<AnswerSet> = None;
    for i in 0..reps.max(1) {
        rig_rep_pause();
        let t0 = std::time::Instant::now();
        let a = engine.run(&honest);
        let ms = t0.elapsed().as_secs_f64() * 1e3;
        best_h = best_h.min(ms);
        rig_rep_dump(node.q, "honest", i, ms);
        if let Some(prev) = &a_honest {
            assert_eq!(prev, &a, "q={}: honest arm unstable across reps (typed)", node.q);
        }
        a_honest = Some(a);
    }
    let mut best_e = f64::MAX;
    let mut a_elect: Option<AnswerSet> = None;
    // [sqe-q39q10] PARKED-ARRIVAL twin: sleeping between elected reps
    // forces every rep to start from PARKED pool workers — the served
    // statement's arrival pattern (a server statement always follows a
    // client round-trip gap; the back-to-back loop below keeps workers
    // hot on the condvar fast path instead). The v6.2 kit's M8 module
    // prices the delta (GAP| line): if parked-native ~= served on the
    // box of record, the shallow violator band is comparator-basis, not
    // engine. Rig-only env; 0/unset = the historical back-to-back loop.
    let gap_ms: u64 = std::env::var("PGRCBENCH_REP_GAP_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    for i in 0..reps.max(1) {
        if gap_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(gap_ms));
        }
        let t0 = std::time::Instant::now();
        let a = engine.run(node);
        let ms = t0.elapsed().as_secs_f64() * 1e3;
        best_e = best_e.min(ms);
        rig_rep_dump(node.q, "elected", i, ms);
        if let Some(prev) = &a_elect {
            assert_eq!(prev, &a, "q={}: elected arm unstable across reps (typed)", node.q);
        }
        a_elect = Some(a);
    }
    let (a_honest, a_elect) = (a_honest.unwrap(), a_elect.unwrap());
    assert_eq!(
        a_honest, a_elect,
        "q={}: honest and elected arms disagree (typed)",
        node.q
    );
    assert_eq!(
        to_lines(&a_honest),
        to_lines(&a_elect),
        "q={}: honest and elected arms disagree (rendered)",
        node.q
    );
    (a_elect, best_h, best_e)
}

// Oracle identity laws: BYTE default; "set" = tie tiers straddling a
// LIMIT/OFFSET edge (agg-field sequence byte-checked, per-tier
// sub-multiset vs the FULL result); "membership" = LIMIT without ORDER
// BY; "proj" = compare on the trailing select-arity columns. Witness
// notes verify against the oracle's counts via the render-seam
// constructors.

fn data_lines(a: &AnswerSet) -> Vec<String> {
    let mut c = a.clone();
    c.note = None;
    c.head_note = None;
    to_lines(&c)
}

pub struct OracleVerdict {
    pub ok: bool,
    pub law: String,
    pub note: String,
    pub detail: Vec<String>,
}

fn fail(law: &str, note: &str, detail: Vec<String>) -> OracleVerdict {
    OracleVerdict { ok: false, law: law.into(), note: note.into(), detail }
}

/// The rig's oracle identity check under the canonicalization laws.
pub fn oracle_check(
    ast: &lower::SqlQuery,
    out: &oracle::OracleOut,
    answer: &AnswerSet,
) -> OracleVerdict {
    use crate::render::{field_at, footer_groups, footer_rows, head_matches};
    let n_s = ast.select.len();
    let mut proj = answer.clone();
    let mut law_proj = false;
    if proj.cols.len() > n_s {
        proj.cols.drain(..proj.cols.len() - n_s);
        law_proj = true;
    }
    if proj.cols.len() != n_s {
        return fail(
            "byte",
            "colcount",
            vec![format!("engine cols {} vs select arity {n_s}", proj.cols.len())],
        );
    }
    let note_tag = match &proj.note {
        None => "none".to_string(),
        Some(s) => {
            let mut tag = None;
            if *s == footer_rows(out.matches) {
                tag = Some("rows".to_string());
            }
            if tag.is_none() {
                if let (Some(g), Some(r)) = (out.groups_total, out.rows_folded) {
                    if *s == footer_groups(g, r) {
                        tag = Some("groups".to_string());
                    }
                }
            }
            if tag.is_none() {
                if let (Some(g), Some(r)) = (out.groups_prehaving, out.rows_prehaving) {
                    if *s == footer_groups(g, r) {
                        tag = Some("groups_prehaving".to_string());
                    }
                }
            }
            match tag {
                Some(t) => t,
                None => {
                    return fail(
                        "byte",
                        "footer",
                        vec![format!(
                            "engine footer {s:?} matches no oracle witness (matches={} groups={:?} rows={:?} pre={:?}/{:?})",
                            out.matches,
                            out.groups_total,
                            out.rows_folded,
                            out.groups_prehaving,
                            out.rows_prehaving
                        )],
                    )
                }
            }
        }
    };
    let note_tag = match &proj.head_note {
        None => note_tag,
        Some(h) => {
            if *h == head_matches(out.matches) {
                format!("{note_tag}+matches")
            } else {
                return fail(
                    "byte",
                    "head",
                    vec![format!(
                        "engine head note {h:?} != oracle matches {}",
                        out.matches
                    )],
                );
            }
        }
    };
    let eng = data_lines(&proj);
    let ora = data_lines(&out.ans);
    let base_law = if law_proj { "proj+" } else { "" };
    let byte_verdict = |law: &str| -> OracleVerdict {
        if eng == ora {
            OracleVerdict {
                ok: true,
                law: format!("{base_law}{law}"),
                note: note_tag.clone(),
                detail: Vec::new(),
            }
        } else {
            let mut d = Vec::new();
            for i in 0..eng.len().max(ora.len()) {
                if eng.get(i) != ora.get(i) {
                    d.push(format!(
                        "row {i}: oracle={:?} engine={:?}",
                        ora.get(i),
                        eng.get(i)
                    ));
                    if d.len() >= 4 {
                        break;
                    }
                }
            }
            fail(&format!("{base_law}{law}"), &note_tag, d)
        }
    };
    let full = match &out.full {
        None => return byte_verdict("byte"),
        Some(f) => f,
    };
    if ast.order.is_empty() && ast.limit.is_some() {
        let mut bag: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        for l in data_lines(full) {
            *bag.entry(l).or_insert(0) += 1;
        }
        let mut d = Vec::new();
        for l in &eng {
            let e = bag.entry(l.clone()).or_insert(0);
            *e -= 1;
            if *e < 0 && d.len() < 4 {
                d.push(format!("engine row not in full result (or duplicated): {l:?}"));
            }
        }
        if eng.len() != ora.len() {
            d.push(format!("cardinality: engine {} vs oracle window {}", eng.len(), ora.len()));
        }
        return if d.is_empty() {
            OracleVerdict {
                ok: true,
                law: format!("{base_law}membership"),
                note: note_tag,
                detail: Vec::new(),
            }
        } else {
            fail(&format!("{base_law}membership"), &note_tag, d)
        };
    }
    let of = match out.order_field {
        None => return byte_verdict("byte"),
        Some(of) => of,
    };
    let n_full = full.nrows;
    let start = ast.offset.min(n_full);
    let end = (ast.offset + ast.limit.unwrap_or(usize::MAX / 2)).min(n_full);
    let straddle_low =
        start > 0 && start < n_full && field_at(full, of, start - 1) == field_at(full, of, start);
    let straddle_high =
        end > 0 && end < n_full && field_at(full, of, end - 1) == field_at(full, of, end);
    if !straddle_low && !straddle_high {
        return byte_verdict("byte");
    }
    let mut d = Vec::new();
    if eng.len() != ora.len() {
        d.push(format!("cardinality: engine {} vs oracle window {}", eng.len(), ora.len()));
    }
    let win_fields: Vec<String> = (0..out.ans.nrows).map(|r| field_at(&out.ans, of, r)).collect();
    let eng_fields: Vec<String> = (0..proj.nrows).map(|r| field_at(&proj, of, r)).collect();
    if win_fields != eng_fields {
        d.push(format!(
            "order-field sequence differs: oracle {:?} engine {:?}",
            &win_fields[..win_fields.len().min(6)],
            &eng_fields[..eng_fields.len().min(6)]
        ));
    }
    if d.is_empty() {
        let tiers: std::collections::HashSet<String> = win_fields.iter().cloned().collect();
        let full_lines = data_lines(full);
        let mut bag: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for (r, l) in full_lines.iter().enumerate() {
            if tiers.contains(&field_at(full, of, r)) {
                *bag.entry(l.as_str()).or_insert(0) += 1;
            }
        }
        for l in &eng {
            match bag.get_mut(l.as_str()) {
                Some(e) if *e > 0 => *e -= 1,
                _ => {
                    if d.len() < 4 {
                        d.push(format!("engine row not in its tie tier (or duplicated): {l:?}"));
                    }
                }
            }
        }
    }
    if d.is_empty() {
        OracleVerdict {
            ok: true,
            law: format!("{base_law}set"),
            note: note_tag,
            detail: Vec::new(),
        }
    } else {
        fail(&format!("{base_law}set"), &note_tag, d)
    }
}

fn oracle_leg(engine: &Engine, label: &str, ast: &lower::SqlQuery, answer: &AnswerSet) -> bool {
    let out = oracle::run_oracle_full(&engine.bank, ast);
    let v = oracle_check(ast, &out, answer);
    println!(
        "VERIFY|q={label}|variants=oracle,sqe_honest,sqe|answers_identical={}|law={}|note={}",
        v.ok, v.law, v.note
    );
    for line in &v.detail {
        println!("ORACLEDIFF|q={label}|{line}");
    }
    v.ok
}

/// One ClickBench query: plans, arms, and the oracle identity legs;
/// false on an oracle identity failure.
pub fn run_query(engine: &Engine, q: u32, sql: bool, oracle: bool, reps: usize) -> bool {
    let ron = match plan_ron(engine, q) {
        Ok(n) => n,
        Err(e) => {
            println!("SQE|q={q}|FAIL|{e}");
            return true;
        }
    };
    let node = if sql {
        match lower::plan_sql(&engine.ctx(), q) {
            Ok(n) => {
                let diffs = lower::plan_diff(&ron, &n);
                if diffs.is_empty() {
                    println!("PLANDIFF|q={q}|equal=true");
                } else {
                    for d in &diffs {
                        println!("PLANDIFF|q={q}|equal=false|{d}");
                    }
                }
                n
            }
            Err(e) => {
                println!("SQLLOWER|q={q}|FAIL|{e}");
                return true;
            }
        }
    } else {
        ron
    };
    let node = maybe_server_bound(node);
    if std::env::var("PGRUST_SQE_DUMPNODE").is_ok() {
        println!("SQENODE|side=rig|fam={:?}|{node:#?}", node.family);
    }
    println!(
        "PLAN|q={q}|family={:?}|cols={:?}|goal_fps={:?}",
        node.family,
        node.cols,
        node.params
            .goal
            .fingerprints
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
    );
    let (answer, best_h, best_e) = run_arms_grouped(engine, &node, reps);
    let mut ok = true;
    if oracle {
        let text = lower::clickbench_text(q).expect("query text");
        match lower::parse_sql(&engine.bank, text) {
            Ok(ast) => ok = oracle_leg(engine, &q.to_string(), &ast, &answer),
            Err(e) => println!("ORACLE|q={q}|SKIP|{e}"),
        }
    }
    println!(
        "SQE|q={q}|sqe_ms={best_e:.3}|sqe_honest_ms={best_h:.3}|lines={}",
        to_lines(&answer).len()
    );
    for line in crate::coldledger::drain_lines(q, "sqe") {
        println!("{line}");
    }
    ok
}

/// ELECTED-COLD mode: the elected arm alone, first, so a cold child
/// measures elected-cold (no honest arm absorbing the I/O). No identity
/// legs; identity runs keep honest-then-elected.
pub fn run_query_elected(engine: &Engine, q: u32, reps: usize) {
    let node = match plan_ron(engine, q) {
        Ok(n) => n,
        Err(e) => {
            println!("SQE|q={q}|FAIL|{e}");
            return;
        }
    };
    println!("PLAN|q={q}|family={:?}|cols={:?}", node.family, node.cols);
    let mut best_e = f64::MAX;
    let mut answer: Option<AnswerSet> = None;
    for i in 0..reps.max(1) {
        rig_rep_pause();
        let t0 = std::time::Instant::now();
        let a = engine.run(&node);
        let ms = t0.elapsed().as_secs_f64() * 1e3;
        best_e = best_e.min(ms);
        rig_rep_dump(q, "elected", i, ms);
        if let Some(prev) = &answer {
            assert_eq!(prev, &a, "q={q}: elected arm unstable across reps (typed)");
        }
        answer = Some(a);
    }
    let answer = answer.unwrap();
    println!(
        "SQE|q={q}|sqe_ms={best_e:.3}|arms=elected_only|lines={}",
        to_lines(&answer).len()
    );
    for line in crate::coldledger::drain_lines(q, "sqe") {
        println!("{line}");
    }
}

/// One unseen query: SQL -> plan, arms vs oracle under the same laws.
pub fn run_unseen(engine: &Engine, uq: u32, reps: usize) -> bool {
    let qid = 100 + uq;
    let text = lower::unseen_text(uq).expect("unseen query text");
    println!("UNSEEN|u={uq}|sql={}", text.trim());
    let (ast, node) = match lower::plan_unseen(&engine.ctx(), uq) {
        Ok(x) => x,
        Err(e) => {
            println!("SQLLOWER|u={uq}|FAIL|{e}");
            return false;
        }
    };
    println!("PLAN|q={qid}|family={:?}|cols={:?}", node.family, node.cols);
    let (answer, best_h, best_e) = run_arms_grouped(engine, &node, reps);
    let ok = oracle_leg(engine, &qid.to_string(), &ast, &answer);
    println!("SQE|q={qid}|src=unseen|sqe_ms={best_e:.3}|sqe_honest_ms={best_h:.3}");
    ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_ron_parses_all_43() {
        let p = plans();
        assert_eq!(p.plans.len(), 43);
        for q in [0u32, 7, 19, 32, 37] {
            assert!(p.plans.iter().any(|pl| pl.q == q), "q{q} missing");
        }
    }

    #[test]
    fn hits_schema_types() {
        let s = hits_schema();
        assert_eq!(s.len(), 105);
        // The catalog boundary owns the date/timestamp facts.
        assert_eq!(s.iter().find(|c| c.name == "eventdate").unwrap().typ, TypMeta::DATE);
        // ALL THREE timestamp columns — clienteventtime/localeventtime as
        // INT8 is the q23 native-dump raw-micros divergence (v6.2 tax cell
        // srvrig-perfcheck-v6-20260820T190812Z); this pin is red at the
        // pre-fix schema.
        for tscol in ["eventtime", "clienteventtime", "localeventtime"] {
            assert_eq!(
                s.iter().find(|c| c.name == tscol).unwrap().typ.oid,
                oids::TIMESTAMP,
                "{tscol} must carry the TIMESTAMP render law"
            );
        }
        assert!(s.iter().find(|c| c.name == "url").unwrap().typ.is_varlena());
    }
}
