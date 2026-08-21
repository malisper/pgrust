//! [R6d] LIKE-lane identity gate: the dict-predicate survivor law's first
//! proving predicate class. Engine answers vs the scalar oracle over real
//! pgrc2_write banks, on BOTH postures (dict-published and verbatim text),
//! high and low selectivity, plus the guard-failure row-grain lowering
//! (same answer via the row plane) and the typed refusal edges.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{PlanNode, VarOp, F_ROW_GRAIN};
use sqe::planner::{entry_grain_law, EntryGrain, R6dGuards};
use sqe::render::to_lines;
use sqe::rig::lower::{lower, parse_sql};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 12_000;

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

/// Deterministic null-free rows. `t` is the pattern-target column: empty
/// strings, prefix/suffix/infix families, literal `%`/`_` bytes (escape
/// coverage), multibyte UTF-8 families (2- and 3-byte chars — the
/// character-grain `_` lane), and a high-frequency tail class
/// (selectivity spread).
fn row(i: u64) -> (i64, i64, String, String) {
    let k = i as i64; // unique (row top-k tie-freedom)
    let g = (i % 16) as i64;
    let s = format!("s{:02}", i % 20);
    let t = match i % 10 {
        0 => String::new(),
        1 => format!("alpha{:03}", i % 50),
        2 => format!("beta{:03}xyz", i % 30),
        3 => "100%".to_string(),
        4 => "a_b".to_string(),
        5 => format!("x{}z", i % 7),
        6 => "google".to_string(),
        7 => format!("pre{}suf", "-".repeat((i % 4) as usize)),
        8 => {
            if i % 2 == 0 {
                format!("日本{:02}語", i % 9) // 3-byte chars around digits
            } else {
                format!("é{:02}é", i % 25) // 2-byte chars at both edges
            }
        }
        _ => format!("tail{:02}end", i % 25),
    };
    (k, g, s, t)
}

/// Seal via the real writer; `dict` arms the dict posture on both text
/// columns (the entry-grain plane), else verbatim (the row plane).
fn seal(dir: &str, dict: bool) {
    use pgrc2_write::dict::TextSemantics;
    use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let dpol = || ColumnPosture {
        dict: Some(DictPolicy { ndv_cap: 1 << 16, exec_ok: true, sem: TextSemantics::Utf8Chars }),
        ..Default::default()
    };
    let cands = if dict {
        CodecCandidates::new(ColumnPosture::default())
            .with_column(3, 0, dpol())
            .with_column(4, 0, dpol())
    } else {
        CodecCandidates::new(ColumnPosture::default())
    };
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3), tcol(4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        790,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, g, s, t) = row(i);
        let s_img = img_4b_u(s.as_bytes());
        let t_img = img_4b_u(t.as_bytes());
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Word(g as u64),
            RawDatum::Bytes(&s_img),
            RawDatum::Bytes(&t_img),
        ];
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");
}

fn open(dir: &str) -> Engine {
    let schema = vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
        ColMeta::new(4, "t", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 2, ..SqeConfig::default() })
}

/// Lower one SQL text, run it, and gate byte-identity vs the oracle.
/// Returns the plan for structural asserts.
fn check(engine: &Engine, q: u32, sql: &str, tag: &str) -> PlanNode {
    let node = lower(&engine.ctx(), q, sql, tag).unwrap_or_else(|e| panic!("{tag}: lower: {e}"));
    let answer = engine.run(&node);
    let ast = parse_sql(&engine.bank, sql).unwrap_or_else(|e| panic!("{tag}: parse: {e}"));
    let want = run_oracle(&engine.bank, &ast);
    assert_eq!(to_lines(&want), to_lines(&answer), "{tag}: engine vs oracle\nsql: {sql}");
    node
}

fn var_op0(node: &PlanNode) -> VarOp {
    node.pred.as_ref().expect("pred").var_terms.first().expect("var term").op
}

fn row_grain(node: &PlanNode) -> bool {
    node.params.flags & F_ROW_GRAIN != 0
}

// ---------------------------------------------------------------------------
// pure guard law (no bank)
// ---------------------------------------------------------------------------

#[test]
fn entry_grain_law_guards() {
    // [ruling 2] the law takes Witness<u64> — the test-only door mints
    // the fixture values; production callers go through the membrane.
    let w = sqe::witness::Witness::assume_for_test;
    let g = R6dGuards::default();
    // no dict anywhere: only the row plane exists.
    assert_eq!(entry_grain_law(VarOp::Like, w(0), w(1_000_000), false, &g), EntryGrain::Row);
    // guard 1 (build cost): near-unique dict (entries ~= rows) fails the
    // reuse test; a well-shared dict passes.
    assert_eq!(entry_grain_law(VarOp::Like, w(900_000), w(1_000_000), false, &g), EntryGrain::Row);
    assert_eq!(entry_grain_law(VarOp::Like, w(10_000), w(1_000_000), false, &g), EntryGrain::Entry);
    // boundary: entries == g_build * rows passes; one more fails.
    assert_eq!(entry_grain_law(VarOp::Like, w(500_000), w(1_000_000), false, &g), EntryGrain::Entry);
    assert_eq!(entry_grain_law(VarOp::Like, w(500_001), w(1_000_000), false, &g), EntryGrain::Row);
    // guard 2 (survivor selectivity) arms on GROUPED consumers only:
    // complements estimate near-exhaustive and lower to the row plane.
    assert_eq!(entry_grain_law(VarOp::NotLike, w(10_000), w(1_000_000), false, &g), EntryGrain::Entry);
    assert_eq!(entry_grain_law(VarOp::NotLike, w(10_000), w(1_000_000), true, &g), EntryGrain::Row);
    assert_eq!(entry_grain_law(VarOp::Like, w(10_000), w(1_000_000), true, &g), EntryGrain::Entry);
    assert_eq!(entry_grain_law(VarOp::NeEmpty, w(10_000), w(1_000_000), true, &g), EntryGrain::Row);
    // parameterization is live, not hardcoded: a permissive g_sel flips
    // the grouped complement back to entry grain.
    let loose = R6dGuards { g_sel: 0.95, ..g };
    assert_eq!(
        entry_grain_law(VarOp::NotLike, w(10_000), w(1_000_000), true, &loose),
        EntryGrain::Entry
    );
}

// ---------------------------------------------------------------------------
// dict bank: entry-grain lane
// ---------------------------------------------------------------------------

#[test]
fn like_identity_dict_bank() {
    let dir = std::env::temp_dir().join(format!("sqe_like_dict_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, true);
    let engine = open(&dir);
    assert!(
        (0..engine.bank.parts.len()).any(|pi| sqe::scan::is_dict(&engine.bank, pi, 4)),
        "dict posture required on t"
    );

    // (pattern, negated, expected driving op) — vocabulary sweep: prefix,
    // suffix, underscore, escape, exact, match-all, zero-match, and the
    // `%x%` Contains normalization.
    let cases: &[(&str, bool, VarOp)] = &[
        ("alpha%", false, VarOp::Like),
        ("alpha%", true, VarOp::NotLike),
        ("%xyz", false, VarOp::Like),
        ("x_z", false, VarOp::Like),
        ("100\\%", false, VarOp::Like),
        ("a\\_b", false, VarOp::Like),
        ("google", false, VarOp::Like),
        ("google", true, VarOp::NotLike),
        ("%", false, VarOp::Like),
        ("nosuch-%", false, VarOp::Like),
        ("%goog%", false, VarOp::Contains),
        ("%goog%", true, VarOp::NotContains),
        ("pre%suf", false, VarOp::Like),
        ("%00%", false, VarOp::Contains), // high-selectivity infix
        // character-grain `_` over multibyte entries (the MB lane):
        // 3-byte chars, 2-byte chars, `_` before a multibyte literal
        // (byte-grain diverges), mixed %_, and the complement.
        ("日本__語", false, VarOp::Like),
        ("日本__語", true, VarOp::NotLike),
        ("_本__語", false, VarOp::Like),
        ("é__é", false, VarOp::Like),
        ("%_語", false, VarOp::Like),
        ("__0_語", false, VarOp::Like),
        ("é0_é", false, VarOp::Like), // low-selectivity mb `_`
    ];
    for (i, (pat, not, op)) in cases.iter().enumerate() {
        let sql = format!(
            "SELECT COUNT(*) FROM hits WHERE t {} '{}';",
            if *not { "NOT LIKE" } else { "LIKE" },
            pat
        );
        let node = check(&engine, 900 + i as u32, &sql, &format!("dict[{pat}] not={not}"));
        assert_eq!(node.family, sqe::ir::Family::WindowReplay, "family for {pat}");
        assert_eq!(var_op0(&node), *op, "driving op for {pat}");
        // ungrouped consumer + shared small dict: entry grain elected.
        assert!(!row_grain(&node), "dict bank ungrouped {pat} must stay entry-grain");
        // fingerprint identity: general patterns carry the like/not_like
        // tag; the %x% class carries the SHIPPED contains identity.
        let fp = node.pred.as_ref().unwrap().var_terms[0].fp.to_string();
        match op {
            VarOp::Like => assert!(fp.contains(":like:"), "fp {fp}"),
            VarOp::NotLike => assert!(fp.contains(":not_like:"), "fp {fp}"),
            VarOp::Contains => assert!(fp.contains(":contains:"), "fp {fp}"),
            VarOp::NotContains => assert!(fp.contains(":not_contains:"), "fp {fp}"),
            _ => unreachable!(),
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn like_grouped_identity_dict_bank() {
    let dir = std::env::temp_dir().join(format!("sqe_like_grp_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, true);
    let engine = open(&dir);

    // grouped consumer (q21-class shape, LIKE instead of Contains).
    let node = check(
        &engine,
        920,
        "SELECT s, COUNT(*) FROM hits WHERE t LIKE 'alpha%' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "grouped-like",
    );
    assert_eq!(node.family, sqe::ir::Family::WindowReplay);
    assert_eq!(var_op0(&node), VarOp::Like);
    assert!(!row_grain(&node), "selective grouped LIKE stays entry-grain");

    // grouped + MinBytes (the MIN(text) leg of the var-lane vocabulary).
    let node = check(
        &engine,
        921,
        "SELECT s, MIN(t), COUNT(*) FROM hits WHERE t LIKE 'beta%' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "grouped-like-minbytes",
    );
    assert_eq!(node.family, sqe::ir::Family::WindowReplay);

    // grouped complement: the selectivity guard lowers to the row plane
    // (touched est near-exhaustive) — flag set, answer identical.
    let node = check(
        &engine,
        922,
        "SELECT s, COUNT(*) FROM hits WHERE t NOT LIKE 'alpha%' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "grouped-notlike-rowgrain",
    );
    assert_eq!(var_op0(&node), VarOp::NotLike);
    assert!(row_grain(&node), "grouped complement must guard-lower to row grain");

    // grouped multibyte `_` (character-grain on the entry plane).
    let node = check(
        &engine,
        924,
        "SELECT s, COUNT(*) FROM hits WHERE t LIKE '日本__語' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "grouped-like-mb-underscore",
    );
    assert_eq!(var_op0(&node), VarOp::Like);
    assert!(!row_grain(&node), "selective grouped mb-underscore LIKE stays entry-grain");

    // grouped mb complement: guard-lowers to the row plane — the `_`
    // char-grain law must hold there too (same matcher, row grain).
    let node = check(
        &engine,
        925,
        "SELECT s, COUNT(*) FROM hits WHERE t NOT LIKE '%__語' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "grouped-notlike-mb-rowgrain",
    );
    assert_eq!(var_op0(&node), VarOp::NotLike);
    assert!(row_grain(&node), "grouped mb complement must guard-lower to row grain");

    // LIKE composed with an int conjunct (zone x verdict interplay).
    let node = check(
        &engine,
        923,
        "SELECT COUNT(*) FROM hits WHERE t LIKE 'tail%' AND g = 3;",
        "like-plus-int",
    );
    assert_eq!(node.family, sqe::ir::Family::WindowReplay);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// verbatim bank: no dict anywhere => the row plane IS the plan
// ---------------------------------------------------------------------------

#[test]
fn like_identity_verbatim_bank() {
    let dir = std::env::temp_dir().join(format!("sqe_like_verb_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, false);
    let engine = open(&dir);
    assert!(
        (0..engine.bank.parts.len()).all(|pi| !sqe::scan::is_dict(&engine.bank, pi, 4)),
        "verbatim posture required on t"
    );
    for (i, (pat, not)) in [
        ("alpha%", false),
        ("alpha%", true),
        ("%xyz", false),
        ("x_z", false),
        ("100\\%", false),
        ("%goog%", false),
        ("%", false),
        // character-grain `_` on the row plane (no dict anywhere).
        ("日本__語", false),
        ("_本%", false),
        ("é__é", true),
    ]
    .iter()
    .enumerate()
    {
        let sql = format!(
            "SELECT COUNT(*) FROM hits WHERE t {} '{}';",
            if *not { "NOT LIKE" } else { "LIKE" },
            pat
        );
        let node = check(&engine, 930 + i as u32, &sql, &format!("verb[{pat}] not={not}"));
        // entries == 0: the grain election lowers to the row plane.
        assert!(row_grain(&node), "dictless bank must elect the row plane for {pat}");
    }
    // grouped on the verbatim bank too.
    let node = check(
        &engine,
        938,
        "SELECT s, COUNT(*) FROM hits WHERE t LIKE 'x_z' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "verb-grouped",
    );
    assert!(row_grain(&node));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// guard-failure lowering correctness: forced row grain == entry grain
// ---------------------------------------------------------------------------

#[test]
fn forced_row_grain_same_answer() {
    let dir = std::env::temp_dir().join(format!("sqe_like_grain_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, true);

    let sqls = [
        "SELECT COUNT(*) FROM hits WHERE t LIKE 'alpha%';",
        "SELECT s, COUNT(*) FROM hits WHERE t LIKE '%xyz' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
        "SELECT COUNT(*) FROM hits WHERE t NOT LIKE 'x_z';",
        "SELECT COUNT(*) FROM hits WHERE t LIKE '日本__語';",
        "SELECT s, COUNT(*) FROM hits WHERE t LIKE '_本__語' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;",
    ];
    for (i, sql) in sqls.iter().enumerate() {
        // Fresh engines per arm: no condcache cross-talk between grains.
        let ea = open(&dir);
        let na = lower(&ea.ctx(), 940 + i as u32, sql, "entry-arm").expect("lower a");
        let mut nb = na.clone();
        let a = to_lines(&ea.run(&na));
        let eb = open(&dir);
        nb.params.flags |= F_ROW_GRAIN;
        let b = to_lines(&eb.run(&nb));
        assert_eq!(a, b, "entry vs forced-row-grain answers must agree\nsql: {sql}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// condcache: the LIKE survivor plane replays under its fingerprint
// ---------------------------------------------------------------------------

#[test]
fn like_plane_replays_identically() {
    let dir = std::env::temp_dir().join(format!("sqe_like_replay_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, true);
    let engine = open(&dir);
    let sql = "SELECT s, COUNT(*) FROM hits WHERE t LIKE 'tail%end' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 10;";
    let node = lower(&engine.ctx(), 950, sql, "replay").expect("lower");
    assert!(
        !node.params.goal.fingerprints.is_empty(),
        "var-term plans must carry a consumable goal fingerprint"
    );
    let ast = parse_sql(&engine.bank, sql).expect("parse");
    let want = to_lines(&run_oracle(&engine.bank, &ast));
    // populate policy Second: compute / populate / replay — identical.
    for rep in 0..3 {
        let got = to_lines(&engine.run(&node));
        assert_eq!(want, got, "rep {rep}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// typed refusal edges
// ---------------------------------------------------------------------------

#[test]
fn refusal_edges() {
    let dir = std::env::temp_dir().join(format!("sqe_like_refuse_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, true);
    let engine = open(&dir);

    // ILIKE: typed parser refusal (no case-folded matcher under C).
    let e = lower(&engine.ctx(), 960, "SELECT COUNT(*) FROM hits WHERE t ILIKE 'a%';", "ilike")
        .unwrap_err();
    assert!(e.contains("ILIKE"), "{e}");
    let e = lower(
        &engine.ctx(),
        961,
        "SELECT COUNT(*) FROM hits WHERE t NOT ILIKE 'a%';",
        "not-ilike",
    )
    .unwrap_err();
    assert!(e.contains("ILIKE"), "{e}");

    // trailing escape: invalid pattern, refused at parse AND at lowering.
    let e = lower(&engine.ctx(), 962, "SELECT COUNT(*) FROM hits WHERE t LIKE 'abc\\';", "esc")
        .unwrap_err();
    assert!(e.contains("trailing-escape"), "{e}");
    {
        use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred};
        let ap = APlan {
            sortagg_keys: Vec::new(),
        win: None,
            q: 963,
            family: AFamily::WindowReplay,
            tags: Vec::new(),
            cols: vec![4],
            pred: Some(APred::Like {
                col: 4,
                pattern: "abc\\".into(),
                not: false,
                fp: None,
            }),
            group: Vec::new(),
            agg: vec![AAgg::CountStar],
            order: None,
            agg_filters: Vec::new(),
            having: None,
            params: Vec::new(),
            fingerprints: Vec::new(),
            kernel_oracle: String::new(),
            notes: String::new(),
            flags: Vec::new(),
        };
        match plan_from_ap(&engine.bank, &engine.faces, &ap) {
            Err(sqe::refuse::Refuse::PredUnsupported { what }) => {
                assert_eq!(what, "like-pattern-trailing-escape")
            }
            other => panic!("expected typed trailing-escape refusal, got {other:?}"),
        }
    }

    // consumer shape without a landed body: typed, never a wrong answer.
    let e = lower(
        &engine.ctx(),
        964,
        "SELECT SUM(k) FROM hits WHERE t LIKE 'alpha%';",
        "sum-like",
    )
    .unwrap_err();
    assert!(e.contains("r6d-consumer-shape"), "{e}");
    let _ = std::fs::remove_dir_all(&dir);
}
