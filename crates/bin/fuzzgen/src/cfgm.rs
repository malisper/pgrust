//! CFG config-machinery module (line-drain CONFIG-GATED bucket, lane
//! fuzz-cfgprofile): the GUC set/reset/show machinery
//! (backend/utils/misc/guc.c `set_config_with_handle`, `AtEOXact_GUC`,
//! `push_old_value`, guc_funcs.c `ExecSetVariableStmt`/`ShowGUCOption`),
//! custom placeholder classes, the timezone-abbreviation file loader
//! (utils/misc/tzparser.c via `SET timezone_abbreviations`), and the
//! encoding-conversion tables (backend/utils/mb/conversion_procs — the
//! deep code-region branches the pre-verified mbconv cell table never
//! reaches) via exception-swallowed DO-block byte sweeps.
//!
//! Differential-soundness rules (the module runs on BOTH engines of a
//! diff pair):
//!
//!   - every SET names a GUC from the curated pool below, hand-verified
//!     present on BOTH engines (deck-cfg-guc.sql executes every pool
//!     entry once; the hand-verify leg diffs it before mass generation);
//!   - every bracket restores state (RESET / transaction rollback /
//!     RESET ALL at group end), so later statements see identical GUC
//!     state on both sides;
//!   - the DO-block sweeps (cfgm:sweep, cfgm:conv) swallow every error in
//!     a subtransaction and project NOTHING — client-visible output is
//!     byte-identical even where the engines' GUC inventories or
//!     conversion verdicts differ (any such difference is invisible to
//!     the differential bar and is drained for C-side coverage only);
//!   - client_encoding brackets emit ASCII-only projections plus
//!     conversion expressions whose outputs were hand-verified
//!     byte-identical (deck-cfg-enc.sql) — the encoding hop itself is
//!     exercised by the non-ASCII payloads living INSIDE convert()/
//!     convert_from() calls, never in raw literals;
//!   - no ALTER SYSTEM: it persists across the stream (postgresql.auto.conf)
//!     and would leak config into later legs; the deck owns that surface
//!     with an explicit RESET ALL + reload bracket.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "cfgm:set",
    "cfgm:local",
    "cfgm:setcfg",
    "cfgm:custom",
    "cfgm:sweep",
    "cfgm:tz",
    "cfgm:txniso",
    "cfgm:conv",
    "cfgm:clienc",
];

/// Curated (guc, [values]) pool: every entry SET-able by an ordinary
/// superuser session on BOTH engines (hand-verified via deck-cfg-guc.sql).
/// Values include unit spellings and boundary values; the err arm draws
/// from BAD_SETS instead.
const GUC_POOL: &[(&str, &[&str])] = &[
    ("work_mem", &["'64kB'", "'4MB'", "'1GB'", "64", "DEFAULT"]),
    ("maintenance_work_mem", &["'1MB'", "'64MB'", "1024"]),
    ("enable_seqscan", &["on", "off", "true", "false", "1", "0"]),
    ("enable_hashjoin", &["on", "off"]),
    ("enable_material", &["on", "off"]),
    ("seq_page_cost", &["1.0", "0.1", "100.0"]),
    ("random_page_cost", &["4.0", "1.1"]),
    ("cpu_tuple_cost", &["0.01", "0.5"]),
    ("cursor_tuple_fraction", &["0.1", "1.0"]),
    ("default_statistics_target", &["10", "100", "10000"]),
    ("join_collapse_limit", &["1", "8", "20"]),
    ("from_collapse_limit", &["1", "8"]),
    ("geqo", &["on", "off"]),
    ("geqo_threshold", &["2", "12"]),
    ("application_name", &["'fz_cfgm'", "'fz cfgm two words'", "''"]),
    ("client_min_messages", &["debug1", "log", "notice", "warning", "error"]),
    ("search_path", &["public", "'public, pg_catalog'", "'\"$user\", public'"]),
    ("statement_timeout", &["0", "'10s'", "'2min'"]),
    ("lock_timeout", &["0", "'5s'"]),
    ("extra_float_digits", &["-15", "0", "1", "3"]),
    ("bytea_output", &["hex", "escape"]),
    ("IntervalStyle", &["postgres", "postgres_verbose", "sql_standard", "iso_8601"]),
    ("DateStyle", &["'ISO, MDY'", "'German'", "'Postgres, DMY'", "'SQL, YMD'"]),
    ("gin_fuzzy_search_limit", &["0", "100"]),
    ("vacuum_cost_delay", &["0", "1"]),
    ("track_functions", &["none", "pl", "all"]),
    ("backslash_quote", &["safe_encoding", "on", "off"]),
    ("check_function_bodies", &["on", "off"]),
    ("default_transaction_isolation", &["'read committed'", "'repeatable read'", "serializable"]),
    ("default_transaction_read_only", &["on", "off"]),
    ("transform_null_equals", &["on", "off"]),
    ("array_nulls", &["on", "off"]),
    ("row_security", &["on", "off"]),
    ("temp_buffers", &["'16MB'", "100"]),
];

/// Matched-error SET arms (identical SQLSTATE on both engines,
/// hand-verified in deck-cfg-guc.sql): bad enum value, bad unit, out of
/// range, bad bool, PGC_POSTMASTER at runtime, unknown parameter.
const BAD_SETS: &[&str] = &[
    "SET bytea_output TO 'octal';",
    "SET work_mem TO '64pettabytes';",
    "SET extra_float_digits TO 9999;",
    "SET enable_seqscan TO 'maybe';",
    "SET shared_buffers TO '128MB';",
    "SET no_such_parameter_fzcfgm TO 1;",
    "SET vacuum_cost_delay TO 200;",
];

/// Encoding-pair byte-sweep segments for cfgm:conv: (src, dst, lead lo,
/// lead hi). The DO block iterates lead x trail (0x21..0xFE step 3) and
/// swallows every error — the point is the C conversion-table branches
/// (sjis2mic, big52euc_tw, UtfToLocal ...), not the verdicts.
const CONV_SWEEPS: &[(&str, &str, u16, u16)] = &[
    ("SJIS", "UTF8", 0x81, 0x9f),
    ("SJIS", "UTF8", 0xe0, 0xfc),
    ("SJIS", "EUC_JP", 0x81, 0xef),
    ("SJIS", "MULE_INTERNAL", 0x81, 0xfc),
    ("EUC_JP", "SJIS", 0xa1, 0xfe),
    ("EUC_JP", "UTF8", 0xa1, 0xfe),
    ("EUC_JP", "MULE_INTERNAL", 0x8e, 0xfe),
    ("EUC_KR", "UTF8", 0xa1, 0xfe),
    ("EUC_TW", "BIG5", 0xa1, 0xfe),
    ("EUC_TW", "MULE_INTERNAL", 0x8e, 0xfe),
    ("BIG5", "EUC_TW", 0xa1, 0xf9),
    ("BIG5", "MULE_INTERNAL", 0xa1, 0xf9),
    ("GBK", "UTF8", 0x81, 0xfe),
    ("UHC", "UTF8", 0x81, 0xfe),
    ("GB18030", "UTF8", 0x81, 0xfe),
    ("JOHAB", "UTF8", 0x84, 0xf9),
    ("SHIFT_JIS_2004", "UTF8", 0x81, 0xfc),
    ("SHIFT_JIS_2004", "EUC_JIS_2004", 0x81, 0xfc),
    ("EUC_JIS_2004", "SHIFT_JIS_2004", 0xa1, 0xfe),
    ("MULE_INTERNAL", "EUC_JP", 0x90, 0x92),
    ("MULE_INTERNAL", "SJIS", 0x90, 0x92),
    ("MULE_INTERNAL", "EUC_TW", 0x93, 0x96),
    ("MULE_INTERNAL", "BIG5", 0x93, 0x96),
    ("MULE_INTERNAL", "EUC_KR", 0x93, 0x94),
];

/// Unicode codepoint bands for the UTF8 -> target sweeps (LocalToUtf /
/// UtfToLocal combined-map and segment-boundary branches).
const UTF8_BANDS: &[(u32, u32)] = &[
    (0x00a1, 0x0500),  // latin-1 sup .. cyrillic
    (0x2010, 0x2600),  // punctuation, arrows, math, box drawing
    (0x3041, 0x3400),  // kana + CJK punctuation + compat jamo
    (0x4e00, 0x5200),  // CJK unified band
    (0xac00, 0xb000),  // hangul syllables band
    (0xff01, 0xfff0),  // full/half-width forms
    (0x20000, 0x20200), // SIP band (4-byte UTF8, gb18030/2004 planes)
];

const UTF8_TARGETS: &[&str] = &[
    "SJIS", "EUC_JP", "EUC_KR", "EUC_TW", "BIG5", "GBK", "UHC", "GB18030",
    "JOHAB", "SHIFT_JIS_2004", "EUC_JIS_2004", "LATIN1", "LATIN2", "LATIN5",
    "WIN1251", "WIN1252", "WIN1256", "KOI8R", "KOI8U", "ISO_8859_5",
];

/// client_encoding round-trip brackets: ASCII-safe projections only; the
/// non-ASCII payload rides inside convert_to()/encode() so the client
/// bytes stay ASCII hex on every client_encoding (hand-verified).
const CLIENT_ENCODINGS: &[&str] = &[
    "SJIS", "EUC_JP", "LATIN1", "WIN1252", "GB18030", "UHC", "KOI8R",
    "BIG5", "ISO_2022_JP", "UTF8",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_cfgm_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm");
    match g.weights.pick(g.rng, SHAPES) {
        "cfgm:set" => gen_set(g),
        "cfgm:local" => gen_local(g),
        "cfgm:setcfg" => gen_setcfg(g),
        "cfgm:custom" => gen_custom(g),
        "cfgm:sweep" => gen_sweep(g),
        "cfgm:tz" => gen_tz(g),
        "cfgm:txniso" => gen_txniso(g),
        "cfgm:conv" => gen_conv(g),
        "cfgm:clienc" => gen_clienc(g),
        other => unreachable!("unknown cfgm shape {other}"),
    }
}

fn pick_guc(g: &mut Gen) -> (&'static str, &'static str) {
    let (name, vals) = GUC_POOL[g.rng.below_usize(GUC_POOL.len())];
    (name, vals[g.rng.below_usize(vals.len())])
}

/// SET / SHOW / RESET bracket, occasionally the matched-error arm.
fn gen_set(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:set");
    if g.weights.pick(g.rng, &["cfgm:ok", "cfgm:err"]) == "cfgm:err" {
        g.fire("cfgm:err");
        return vec![raw(BAD_SETS[g.rng.below_usize(BAD_SETS.len())])];
    }
    let (name, val) = pick_guc(g);
    let mut out = vec![raw(format!("SET {name} TO {val};"))];
    if g.rng.chance(1, 2) {
        out.push(raw(format!("SHOW {name};")));
    }
    if g.rng.chance(1, 3) {
        // Re-SET before the RESET: push_old_value's already-stacked arm.
        let (_, vals) = GUC_POOL.iter().find(|(n, _)| *n == name).unwrap();
        out.push(raw(format!("SET {name} TO {};", vals[0])));
    }
    out.push(raw(format!("RESET {name};")));
    out
}

/// Transaction bracket: SET LOCAL + savepoint rollback (push_old_value /
/// AtEOSubXact_GUC / AtEOXact_GUC restore arms).
fn gen_local(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:local");
    let (n1, v1) = pick_guc(g);
    let (n2, v2) = pick_guc(g);
    let mut out = vec![
        raw("BEGIN;"),
        raw(format!("SET LOCAL {n1} TO {v1};")),
        raw("SAVEPOINT fz_cfgm_sp;"),
        raw(format!("SET LOCAL {n2} TO {v2};")),
    ];
    if g.rng.chance(2, 3) {
        out.push(raw("ROLLBACK TO SAVEPOINT fz_cfgm_sp;"));
    } else {
        out.push(raw("RELEASE SAVEPOINT fz_cfgm_sp;"));
    }
    out.push(raw(format!("SELECT current_setting('{n1}') IS NOT NULL;")));
    out.push(raw(if g.rng.chance(1, 2) { "COMMIT;" } else { "ROLLBACK;" }));
    out
}

/// set_config()/current_setting() function forms, incl. the missing_ok
/// arm and the is_local-outside-transaction arm.
fn gen_setcfg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:setcfg");
    let (name, val) = pick_guc(g);
    let v = val.trim_matches('\'');
    match g.rng.below(4) {
        0 => vec![
            raw(format!("SELECT set_config('{name}', '{v}', false);")),
            raw(format!("RESET {name};")),
        ],
        // is_local=true outside a transaction block: takes effect and is
        // discarded at statement end — identical on both engines.
        1 => vec![raw(format!("SELECT set_config('{name}', '{v}', true);"))],
        2 => vec![
            raw("SELECT current_setting('fzcfgm.not_defined', true) IS NULL;"),
            raw(format!("SELECT current_setting('{name}', false) IS NOT NULL;")),
        ],
        _ => vec![
            raw("BEGIN;"),
            raw(format!("SELECT set_config('{name}', '{v}', true);")),
            raw("ROLLBACK;"),
            raw(format!("SELECT current_setting('{name}') IS NOT NULL;")),
        ],
    }
}

/// Custom placeholder classes (define_custom_variable-adjacent placeholder
/// paths + the reserved-class matched error).
fn gen_custom(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:custom");
    let knob = g.rng.below(4);
    let mut out = vec![
        raw(format!("SET fzcfgm.knob{knob} TO 'v{knob}';")),
        raw(format!("SHOW fzcfgm.knob{knob};")),
        raw(format!(
            "SELECT set_config('fzcfgm.dyn{knob}', 'w{knob}', {});",
            if g.rng.chance(1, 2) { "true" } else { "false" }
        )),
        raw(format!("SELECT current_setting('fzcfgm.dyn{knob}', true);")),
        raw(format!("RESET fzcfgm.knob{knob};")),
    ];
    if g.rng.chance(1, 3) {
        g.fire("cfgm:err");
        // Reserved class prefix: matched 42501 on both engines.
        out.push(raw("SET pg_catalog.fzcfgm_bad TO 1;"));
    }
    out
}

/// Exception-swallowed EXECUTE-SET sweep over the pool (no output; the
/// C-side arms are the payload — parse_and_validate_value across types,
/// unit parsing, check hooks).
fn gen_sweep(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:sweep");
    let mut body = String::from("DO $fzcfgm$ BEGIN ");
    let n = 3 + g.rng.below_usize(5);
    for _ in 0..n {
        let (name, val) = pick_guc(g);
        let v = val.replace('\'', "''");
        body.push_str(&format!(
            "BEGIN EXECUTE 'SET {name} TO {v}'; EXCEPTION WHEN others THEN NULL; END; "
        ));
    }
    body.push_str("END $fzcfgm$;");
    vec![raw(body), raw("RESET ALL;")]
}

/// Timezone machinery: SET TIME ZONE grammar variants + the tzparser
/// abbreviation-file loader (ParseTzFile/splitTzLine/validateTzEntry via
/// the shipped Australia/India files).
fn gen_tz(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:tz");
    match g.rng.below(6) {
        0 => vec![
            raw("SET timezone_abbreviations TO 'Australia';"),
            raw("SELECT '2026-01-15 12:00:00 EST'::timestamptz AT TIME ZONE 'UTC';"),
            raw("RESET timezone_abbreviations;"),
        ],
        1 => vec![
            raw("SET timezone_abbreviations TO 'India';"),
            raw("SELECT '2026-01-15 12:00:00 IST'::timestamptz AT TIME ZONE 'UTC';"),
            raw("SET timezone_abbreviations TO 'Default';"),
        ],
        2 => vec![
            raw("SET TIME ZONE 'UTC';"),
            raw("SELECT '2026-06-01 00:00:00'::timestamptz;"),
            raw("SET TIME ZONE LOCAL;"),
        ],
        3 => vec![
            raw("SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE;"),
            raw("SHOW TimeZone;"),
            raw("SET TIME ZONE DEFAULT;"),
        ],
        4 => vec![
            raw("SET TIME ZONE 'America/New_York';"),
            raw("SELECT extract(timezone FROM '2026-01-15 12:00:00'::timestamptz);"),
            raw("RESET TimeZone;"),
        ],
        _ => vec![
            raw("BEGIN;"),
            raw("SET LOCAL TIME ZONE 'Asia/Tokyo';"),
            raw("SELECT '2026-01-15 12:00:00+00'::timestamptz;"),
            raw("ROLLBACK;"),
        ],
    }
}

/// SET TRANSACTION / SESSION CHARACTERISTICS grammar arms
/// (ExecSetVariableStmt VAR_SET_MULTI paths).
fn gen_txniso(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:txniso");
    match g.rng.below(5) {
        0 => vec![
            raw("BEGIN;"),
            raw("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"),
            raw("SELECT 1;"),
            raw("COMMIT;"),
        ],
        1 => vec![
            raw("BEGIN;"),
            raw("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE;"),
            raw("SELECT 1;"),
            raw("ROLLBACK;"),
        ],
        2 => vec![
            raw("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;"),
            raw("SELECT current_setting('default_transaction_read_only');"),
            raw("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;"),
        ],
        3 => vec![
            raw("BEGIN;"),
            raw("SET TRANSACTION READ ONLY;"),
            // Matched 25006: cannot execute in a read-only transaction.
            raw("CREATE TABLE fz_cfgm_ro_probe (a int);"),
            raw("ROLLBACK;"),
        ],
        _ => vec![
            raw("BEGIN ISOLATION LEVEL REPEATABLE READ;"),
            // Matched 22023: invalid snapshot identifier.
            raw("SET TRANSACTION SNAPSHOT 'FFFFFFFF-FFFFFFFF-1';"),
            raw("ROLLBACK;"),
        ],
    }
}

/// Encoding-conversion byte sweep: one CONV_SWEEPS segment, exception-
/// swallowed per cell, zero output (C-table drain; verdict differences
/// between engines are invisible by construction).
fn gen_conv(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:conv");
    if g.rng.chance(1, 3) {
        // UTF8 -> target band sweep through chr()/convert_to.
        let (lo, hi) = UTF8_BANDS[g.rng.below_usize(UTF8_BANDS.len())];
        let dst = UTF8_TARGETS[g.rng.below_usize(UTF8_TARGETS.len())];
        let step = 3 + g.rng.below(4); // 3..6
        return vec![raw(format!(
            "DO $fzconv$ DECLARE cp int; BEGIN cp := {lo}; WHILE cp < {hi} LOOP \
             BEGIN PERFORM convert_to(chr(cp), '{dst}'); \
             EXCEPTION WHEN others THEN NULL; END; \
             cp := cp + {step}; END LOOP; END $fzconv$;"
        ))];
    }
    let (src, dst, lo, hi) = CONV_SWEEPS[g.rng.below_usize(CONV_SWEEPS.len())];
    // Pick a lead-byte subrange so one statement stays cheap; trail bytes
    // walk 0x21..0xFE step 3 (covers 0x40..0x7E and 0xA1..0xFE rows).
    let lead = lo + (g.rng.below(u64::from(hi - lo + 1)) as u16);
    let lead_hi = (lead + 4).min(hi);
    vec![raw(format!(
        "DO $fzconv$ DECLARE l int; t int; b bytea; BEGIN \
         FOR l IN {lead}..{lead_hi} LOOP t := 33; WHILE t <= 254 LOOP \
         b := set_byte(set_byte('\\x2020'::bytea, 0, l), 1, t); \
         BEGIN PERFORM convert(b, '{src}', '{dst}'); \
         EXCEPTION WHEN others THEN NULL; END; \
         t := t + 3; END LOOP; END LOOP; END $fzconv$;"
    ))]
}

/// client_encoding bracket: switch, run ASCII-projected conversion
/// probes (PrepareClientEncoding + the conversion-proc dispatch), reset.
fn gen_clienc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cfgm:clienc");
    let enc = CLIENT_ENCODINGS[g.rng.below_usize(CLIENT_ENCODINGS.len())];
    vec![
        raw(format!("SET client_encoding TO '{enc}';")),
        raw("SHOW client_encoding;"),
        // ASCII-only projection: the conversion hop happens on this
        // ASCII result set itself (trivial path) — the deep tables are
        // cfgm:conv's job; this bracket exists for the mbutils
        // set/prepare/reset machinery.
        raw("SELECT 'fz_cfgm_ascii_probe', length('abc');"),
        raw(format!(
            "SELECT encode(convert_to('abc123', '{enc}'), 'hex');"
        )),
        raw("RESET client_encoding;"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape fires; statements are single-line, terminated, and
    /// dollar-quote balanced; brackets restore state (every SET of a
    /// non-custom GUC outside a txn bracket has a later RESET in-group,
    /// or the group is a BEGIN..COMMIT/ROLLBACK bracket).
    #[test]
    fn cfgm_shapes_fire_and_hold_invariants() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xCF61);
        let mut prods_all = Vec::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_cfgm_module(&mut g);
            assert!(!stmts.is_empty());
            let sqls: Vec<String> = stmts.iter().map(|s| s.to_sql()).collect();
            for sql in &sqls {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert_eq!(
                    sql.matches("$fzconv$").count() % 2,
                    0,
                    "unbalanced dollar quote: {sql}"
                );
            }
            // State restoration: a group that opens SET <pool guc> either
            // RESETs it, is a transaction bracket, or ends with RESET ALL.
            // (BAD_SETS singles are exempt: the SET itself errors, so no
            // state was changed.)
            let first = &sqls[0];
            let is_bad_single = sqls.len() == 1 && BAD_SETS.contains(&first.as_str());
            if let Some(name) = first.strip_prefix("SET ").and_then(|r| r.split(' ').next())
            {
                if !is_bad_single && GUC_POOL.iter().any(|(n, _)| *n == name) {
                    let restored = sqls.iter().any(|s| {
                        s == &format!("RESET {name};")
                            || s == "RESET ALL;"
                            || s.starts_with("SET ") && s.contains("TO DEFAULT")
                    });
                    assert!(restored, "unrestored SET in group: {sqls:?}");
                }
            }
            prods_all.extend(prods);
        }
        for p in SHAPES {
            assert!(prods_all.iter().any(|q| q == p), "{p} never fired");
        }
        assert!(prods_all.iter().any(|q| q == "cfgm:err"), "err arm never fired");
    }

    /// Same seed -> byte-identical statements.
    #[test]
    fn cfgm_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(0xCF62);
            let mut out = Vec::new();
            for _ in 0..400 {
                let mut prods = Vec::new();
                let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                out.extend(gen_cfgm_module(&mut g).iter().map(|s| s.to_sql()));
            }
            out
        };
        assert_eq!(run(), run());
    }

    /// Pool hygiene: names are bare identifiers (no quoting surprises),
    /// every BAD_SETS entry is a single SET statement, sweep segments
    /// have sane byte ranges.
    #[test]
    fn cfgm_tables_are_consistent() {
        for (n, vals) in GUC_POOL {
            assert!(
                n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "{n}"
            );
            assert!(!vals.is_empty());
        }
        for b in BAD_SETS {
            assert!(b.starts_with("SET ") && b.ends_with(';'), "{b}");
        }
        for (src, dst, lo, hi) in CONV_SWEEPS {
            assert!(lo <= hi && *lo >= 0x80 && *hi <= 0xff, "{src}->{dst}");
            assert!(src != dst);
        }
        for (lo, hi) in UTF8_BANDS {
            assert!(lo < hi && *lo >= 0xa1 && *hi <= 0x110000);
        }
    }
}
