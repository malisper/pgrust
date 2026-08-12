//! COPY BINARY differential suite (lane X2).
//!
//! COPY ... TO STDOUT (FORMAT binary) is copyto.c's binary emit: an 11-byte
//! signature + flags + extension header, then per-tuple field counts and
//! per-field *_send payloads. C's output IS the spec — the suite captures
//! the payload through the wire client on both engines and compares
//! byte-for-byte (any byte diff = finding). COPY ... FROM STDIN (FORMAT
//! binary) is copyfromparse.c's binary arm (CopyReadBinaryData /
//! CopyReadBinaryAttribute + per-type *_recv): the suite writes the
//! REFERENCE side's bytes into BOTH engines, then compares the resulting
//! table state (pk-ordered probe) and each engine's re-emitted COPY TO
//! payload — a full send -> recv -> send loop.
//!
//! The case deck is deterministic (no seeds): every fixture table, a family
//! of breadth tables covering the *_send/*_recv residuals from the gap
//! reports (network, bit/uuid/bytea/money, datetime, geometric, ranges +
//! multiranges, json/jsonb/jsonpath, enum/domain/composite/misc, arrays),
//! column-list projections, and COPY (SELECT ...) query forms reaching
//! record_send and array_send over constructed values. Breadth families
//! live in separate small tables so one engine lacking a type surfaces as
//! one ERROR_DIFF finding without sinking the rest of the deck.

use crate::diff::{classify, DiffClass, DiffInput, StmtOutcome};
use crate::ruled::{apply_ruled, RuledEntry};
use crate::runner::{Executor, Record};

/// One breadth table: name + DDL + seed rows (deterministic, identical on
/// both sides by construction). `pk` orders the round-trip state probe.
struct BinTable {
    name: &'static str,
    ddl: &'static str,
    rows: &'static [&'static str],
}

/// Type-prelude DDL: enum, domain, composite used by fz_bx_misc.
const TYPE_DDL: &[&str] = &[
    "CREATE TYPE fz_mood AS ENUM ('sad', 'ok', 'happy');",
    "CREATE DOMAIN fz_posint AS int4 CHECK (VALUE > 0);",
    "CREATE TYPE fz_pair AS (x int4, y text);",
];

const BIN_TABLES: &[BinTable] = &[
    BinTable {
        name: "fz_bx_net",
        ddl: "CREATE TABLE fz_bx_net (pk int4 PRIMARY KEY, c_inet inet, c_cidr cidr, \
              c_mac macaddr, c_mac8 macaddr8);",
        rows: &[
            "(1, '192.168.0.1', '10.1.0.0/16', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05')",
            "(2, '::1', '2001:db8::/32', 'ff:ff:ff:ff:ff:ff', 'ff:ff:ff:ff:ff:ff:ff:ff')",
            "(3, '192.168.0.0/24', '0.0.0.0/0', '00:00:00:00:00:00', '00:00:00:00:00:00:00:00')",
            "(4, NULL, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_bits",
        ddl: "CREATE TABLE fz_bx_bits (pk int4 PRIMARY KEY, c_bit bit(6), c_varbit varbit, \
              c_uuid uuid, c_bytea bytea, c_money money);",
        rows: &[
            "(1, B'101010', B'1', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\xdeadbeef', '12.34')",
            "(2, B'000000', B'', '00000000-0000-0000-0000-000000000000', '\\x', '-0.01')",
            "(3, B'111111', B'01010101010101010101', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '\\x00ff00', '92233720368547758.07')",
            "(4, NULL, NULL, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_time",
        ddl: "CREATE TABLE fz_bx_time (pk int4 PRIMARY KEY, c_date date, c_time time, \
              c_timetz timetz, c_ts timestamp, c_tstz timestamptz, c_interval interval);",
        rows: &[
            "(1, '2020-02-29', '00:00:00', '00:00:00+00', '2000-01-01 00:00:00', '2000-01-01 00:00:00+00', '0')",
            "(2, '1999-12-31', '23:59:59.999999', '23:59:59-08', '1999-12-31 23:59:59.999999', '1999-12-31 23:59:59+05:45', '1 year 2 mons 3 days 04:05:06.000007')",
            "(3, '0001-01-01', '24:00:00', '12:00:00+14', '4713-01-01 00:00:00 BC', '2024-06-15 12:00:00-11', '-178000000 years')",
            "(4, 'infinity', NULL, NULL, 'infinity', '-infinity', NULL)",
            "(5, NULL, NULL, NULL, NULL, NULL, '-3 days +02:03:04')",
        ],
    },
    BinTable {
        name: "fz_bx_geo",
        ddl: "CREATE TABLE fz_bx_geo (pk int4 PRIMARY KEY, c_point point, c_line line, \
              c_lseg lseg, c_box box, c_path path, c_poly polygon, c_circle circle);",
        rows: &[
            "(1, '(0,0)', '{1,-1,0}', '[(0,0),(1,1)]', '(1,1),(0,0)', '[(0,0),(1,1),(2,0)]', '((0,0),(1,1),(2,0))', '<(0,0),1>')",
            "(2, '(-1.5,2.25)', '{0.5,1,-2}', '[(-1,-1),(-2,-2)]', '(10,10),(-10,-10)', '((3,4),(5,6))', '((0,0),(0,3),(3,3),(3,0))', '<(-1.5,2.5),0.125>')",
            "(3, '(1e10,-1e-10)', '{1,0,-5}', '[(0.1,0.2),(0.3,0.4)]', '(0.5,0.5),(0.25,0.25)', '[(9,9)]', '((1,1),(2,2),(3,1))', '<(0,0),0>')",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_range",
        ddl: "CREATE TABLE fz_bx_range (pk int4 PRIMARY KEY, c_i4r int4range, c_i8r int8range, \
              c_numr numrange, c_dater daterange, c_tsr tsrange, c_tstzr tstzrange, \
              c_i4mr int4multirange, c_nummr nummultirange);",
        rows: &[
            "(1, '[1,10)', '[-9000000000,9000000000)', '[1.5,2.5]', '[2020-01-01,2020-12-31)', '[2020-01-01 00:00:00,2020-06-01 00:00:00)', '[2020-01-01 00:00:00+00,)', '{[1,3),[5,9)}', '{[0.1,0.2],[0.5,0.9)}')",
            "(2, 'empty', '(,)', '(,2.5)', 'empty', '(,)', 'empty', '{}', '{}')",
            "(3, '[-5,-1)', '[0,1)', '[0,0]', '[0001-01-01,infinity)', '(,2020-01-01 00:00:00]', '[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00]', '{[-10,-5),[-3,-1),[0,100)}', '{[-1.5,1.5)}')",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_json",
        ddl: "CREATE TABLE fz_bx_json (pk int4 PRIMARY KEY, c_json json, c_jsonb jsonb, \
              c_jsonpath jsonpath);",
        rows: &[
            "(1, '{\"a\":1,\"a\":2}', '{\"a\":1,\"a\":2}', '$.a[*] ? (@ > 2)')",
            "(2, '[1,2,[3,[4]],null,\"x\"]', '[1,2,[3,[4]],null,\"x\"]', 'strict $.**{2}')",
            "(3, '\"scalar\"', 'true', '$')",
            "(4, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_misc",
        ddl: "CREATE TABLE fz_bx_misc (pk int4 PRIMARY KEY, c_char \"char\", c_bpchar char(3), \
              c_name name, c_oid oid, c_lsn pg_lsn, c_enum fz_mood, c_dom fz_posint, \
              c_pair fz_pair, c_tsv tsvector, c_tsq tsquery);",
        rows: &[
            "(1, 'x', 'abc', 'some_name', 12345, '0/1A2B3C4D', 'happy', 7, ROW(1,'one')::fz_pair, 'a:1 b:2', 'a & !b')",
            "(2, '!', 'a', '', 0, '0/0', 'sad', 1, ROW(-1,NULL)::fz_pair, '', 'x')",
            "(3, 'Z', '  x', 'MixedCase', 4294967295, 'FFFFFFFF/FFFFFFFF', 'ok', 2147483647, ROW(0,'')::fz_pair, '''it''''s'':3', 'a | (b & c)')",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    },
    BinTable {
        name: "fz_bx_arr",
        ddl: "CREATE TABLE fz_bx_arr (pk int4 PRIMARY KEY, a_text text[], a_int4 int4[], \
              a_int8 int8[], a_float8 float8[], a_num numeric[], a_bool bool[], a_date date[], \
              a_ts timestamp[], a_uuid uuid[], a_jsonb jsonb[], a_point point[], \
              a_interval interval[], a_i4_2d int4[][]);",
        rows: &[
            "(1, '{x,\"\",NULL}', '{1,NULL,-2147483648}', '{9000000000,NULL}', '{1.5,NaN,Infinity,-Infinity,NULL}', '{1.005,-0.00001,NULL}', '{t,f,NULL}', '{2020-01-01,NULL,infinity}', '{2020-01-01 00:00:00,NULL}', '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL}', '{\"{\\\"k\\\":1}\",NULL}', '{\"(0,0)\",NULL}', '{\"1 day\",NULL}', '{{1,2},{3,4}}')",
            "(2, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            "(3, '{it''s,\"q\\\"z\",caf\u{e9}}', '{0}', '{-9223372036854775808,9223372036854775807}', '{0,-0}', '{0}', '{t}', '{1999-12-31}', '{infinity,-infinity}', '{00000000-0000-0000-0000-000000000000}', '{\"[1,2,3]\"}', '{\"(1.5,-2.5)\",\"(0,0)\"}', '{\"-3 days\",\"178000000 years\"}', '{{-1,-2},{-3,-4}}')",
            "(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    },
];

/// Fixture tables riding the suite whole (name, pk column).
const FIXTURE_TABLES: &[(&str, &str)] = &[
    ("fz_scalar", "pk"),
    ("fz_mixed", "id"),
    ("fz_wide", "pk"),
    ("fz_rich", "pk"),
    ("fz_one", "k_int"),
    ("fz_empty", "pk"),
];

/// The full suite DDL (types, breadth tables, seed rows), deterministic.
pub fn suite_ddl() -> Vec<String> {
    let mut out: Vec<String> = TYPE_DDL.iter().map(|s| s.to_string()).collect();
    for t in BIN_TABLES {
        out.push(t.ddl.to_string());
        for r in t.rows {
            out.push(format!("INSERT INTO {} VALUES {};", t.name, r));
        }
    }
    out
}

/// Standalone COPY TO STDOUT (FORMAT binary) cases beyond the whole-table
/// emits the round-trips already perform: column lists and query forms
/// (record_send / array_send / range constructors over computed values).
pub fn copyto_extra_cases() -> Vec<String> {
    vec![
        // Column-list projections (per-attribute emit paths).
        "COPY fz_scalar (c_text, c_int4, c_ts) TO STDOUT (FORMAT binary);".to_string(),
        "COPY fz_rich (r_jsonb, r_intarr, r_uuid) TO STDOUT (FORMAT binary);".to_string(),
        "COPY fz_bx_geo (c_poly, c_circle) TO STDOUT (FORMAT binary);".to_string(),
        // Query forms: anonymous records, constructed arrays/ranges,
        // nested composites — ORDER BY pins the tuple order so the byte
        // compare is total.
        "COPY (SELECT pk, ROW(pk, k_text), ARRAY[pk, pk + 1, NULL] FROM fz_scalar \
         ORDER BY pk) TO STDOUT (FORMAT binary);"
            .to_string(),
        "COPY (SELECT int4range(k_int, k_int + 5), int8range(NULL, c_int8), \
         ARRAY[ROW(pk, c_bool)] FROM fz_scalar ORDER BY pk) TO STDOUT (FORMAT binary);"
            .to_string(),
        "COPY (SELECT ROW(ROW(1, 'x'), ARRAY['a', NULL], '{\"k\":[1,2]}'::jsonb)) \
         TO STDOUT (FORMAT binary);"
            .to_string(),
        // NOTE: no ARRAY[c_enum] / ROW(c_enum) here — array_send embeds
        // its ELEMENT TYPE OID in the payload, and a user-defined type's
        // oid is engine-local counter state (observed live: 28290 vs 28284
        // after four DDL-heavy streams), so an enum-element array's bytes
        // are environment, not conformance. Bare enum columns are fine:
        // enum_send emits the label text only.
        "COPY (SELECT c_enum, c_pair FROM fz_bx_misc ORDER BY pk) \
         TO STDOUT (FORMAT binary);"
            .to_string(),
        // Text-format COPY of a breadth table: the copyto.c text arm over
        // the same values (cheap contrast case, still byte-compared).
        "COPY fz_bx_time TO STDOUT;".to_string(),
    ]
}

/// Suite outcome counters.
#[derive(Clone, Debug, Default)]
pub struct CopyBinStats {
    pub cases: u32,
    pub matches: u32,
    pub ruled: u32,
    pub findings: u32,
}

/// First non-builtin oid (FirstNormalObjectId): everything at or above it
/// is engine-local counter state, not a stable type identity.
const FIRST_NORMAL_OID: u32 = 16384;

/// Collapse user-defined column type oids to a sentinel before compare:
/// two engines that ran the same DDL assign user types from their own oid
/// counters, so the raw values are environment. Builtin-vs-user and shape
/// mismatches still diff.
fn normalize_user_oids(o: &StmtOutcome) -> StmtOutcome {
    match o {
        StmtOutcome::Rows { col_oids, rows } => StmtOutcome::Rows {
            col_oids: col_oids
                .iter()
                .map(|&oid| if oid >= FIRST_NORMAL_OID { FIRST_NORMAL_OID } else { oid })
                .collect(),
            rows: rows.clone(),
        },
        other => other.clone(),
    }
}

/// Classify one dual-engine exchange and record it if it diverged.
#[allow(clippy::too_many_arguments)]
fn record_case(
    sql: &str,
    oa: &StmtOutcome,
    ob: &StmtOutcome,
    table: &[RuledEntry],
    ulp_tol: u64,
    case_index: u32,
    records: &mut Vec<Record>,
    stats: &mut CopyBinStats,
) -> DiffClass {
    let (na, nb) = (normalize_user_oids(oa), normalize_user_oids(ob));
    let raw = classify(&DiffInput { sql, a: &na, b: &nb, ulp_tol, soft_cols: &[] });
    let c = apply_ruled(table, sql, raw);
    stats.cases += 1;
    match &c.class {
        DiffClass::Match => stats.matches += 1,
        DiffClass::Ruled(_) => {
            stats.ruled += 1;
            records.push(Record {
                stmt_index: case_index,
                sql: sql.to_string(),
                class: c.class.clone(),
                detail: c.detail.clone(),
                probe: false,
            });
        }
        _ => {
            stats.findings += 1;
            records.push(Record {
                stmt_index: case_index,
                sql: sql.to_string(),
                class: c.class.clone(),
                detail: c.detail.clone(),
                probe: false,
            });
        }
    }
    c.class
}

/// Did this outcome succeed (no error, connection alive)?
fn is_ok(o: &StmtOutcome) -> bool {
    !matches!(o, StmtOutcome::Error { .. } | StmtOutcome::ConnLost { .. })
}

/// Run the COPY BINARY suite over a connected pair. `a` is the reference
/// engine (its COPY TO bytes are the spec fed into both sides' COPY FROM).
/// The fixture schema must already be set up on both sides.
pub fn run_suite(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    table: &[RuledEntry],
    ulp_tol: u64,
) -> (Vec<Record>, CopyBinStats) {
    let mut records = Vec::new();
    let mut stats = CopyBinStats::default();
    let mut idx = 0u32;
    let mut next = || {
        idx += 1;
        idx - 1
    };

    // Suite DDL + seed rows (divergences here are findings themselves).
    let mut table_ok: std::collections::BTreeMap<String, bool> =
        std::collections::BTreeMap::new();
    for (name, _) in FIXTURE_TABLES {
        table_ok.insert(name.to_string(), true);
    }
    for sql in suite_ddl() {
        let oa = a.apply(&sql);
        let ob = b.apply(&sql);
        let both_ok = is_ok(&oa) && is_ok(&ob);
        record_case(&sql, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
        // Track per-breadth-table health so a failed CREATE/INSERT skips
        // that table's cases instead of cascading spurious diffs.
        for t in BIN_TABLES {
            if sql.contains(t.name) && !both_ok {
                table_ok.insert(t.name.to_string(), false);
            }
        }
    }
    for t in BIN_TABLES {
        table_ok.entry(t.name.to_string()).or_insert(true);
    }

    // Round-trips: emit (byte compare), feed A's bytes into both, probe
    // state, re-emit (byte compare), drop the clone.
    let roundtrip: Vec<(String, String)> = FIXTURE_TABLES
        .iter()
        .map(|(n, pk)| (n.to_string(), pk.to_string()))
        .chain(BIN_TABLES.iter().map(|t| (t.name.to_string(), "pk".to_string())))
        .collect();
    for (src, pk) in &roundtrip {
        if table_ok.get(src) != Some(&true) {
            continue;
        }
        let copy_to = format!("COPY {src} TO STDOUT (FORMAT binary);");
        let oa = a.apply(&copy_to);
        let ob = b.apply(&copy_to);
        record_case(&copy_to, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
        let StmtOutcome::CopyOut { bytes, .. } = &oa else { continue };
        let spec_bytes = bytes.clone();

        let clone = format!("{src}_rt");
        let clone_ddl = format!("CREATE TABLE {clone} (LIKE {src});");
        let ca = a.apply(&clone_ddl);
        let cb = b.apply(&clone_ddl);
        record_case(&clone_ddl, &ca, &cb, table, ulp_tol, next(), &mut records, &mut stats);
        if !(is_ok(&ca) && is_ok(&cb)) {
            continue;
        }

        // Feed the REFERENCE bytes into both engines: copyfromparse.c's
        // binary arm + per-type *_recv on each side, against C's own emit.
        let copy_from = format!("COPY {clone} FROM STDIN (FORMAT binary);");
        let fa = a.apply_copy_in(&copy_from, &spec_bytes);
        let fb = b.apply_copy_in(&copy_from, &spec_bytes);
        record_case(&copy_from, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);

        if is_ok(&fa) && is_ok(&fb) {
            // Semantic state probe (pk-ordered, strict compare).
            let probe = format!("SELECT * FROM {clone} ORDER BY {pk};");
            let pa = a.apply(&probe);
            let pb = b.apply(&probe);
            record_case(&probe, &pa, &pb, table, ulp_tol, next(), &mut records, &mut stats);

            // Re-emit: the recv -> send loop must reproduce the payload.
            let re_emit = format!("COPY {clone} TO STDOUT (FORMAT binary);");
            let ra = a.apply(&re_emit);
            let rb = b.apply(&re_emit);
            record_case(&re_emit, &ra, &rb, table, ulp_tol, next(), &mut records, &mut stats);
            // And the reference engine's re-emit must equal its original
            // emit (loop closure on the spec side; a mismatch means the
            // suite's byte capture itself is unsound).
            if let StmtOutcome::CopyOut { bytes: rb_a, .. } = &ra {
                if *rb_a != spec_bytes {
                    stats.findings += 1;
                    records.push(Record {
                        stmt_index: next(),
                        sql: re_emit.clone(),
                        class: DiffClass::RowsetDiff,
                        detail: format!(
                            "A-side recv->send loop not byte-stable for {src} \
                             (len {} vs original {})",
                            rb_a.len(),
                            spec_bytes.len()
                        ),
                        probe: false,
                    });
                }
            }
        }

        let drop = format!("DROP TABLE {clone};");
        let da = a.apply(&drop);
        let db = b.apply(&drop);
        record_case(&drop, &da, &db, table, ulp_tol, next(), &mut records, &mut stats);
    }

    // Extra COPY TO shapes (column lists, query forms, one text-format
    // contrast case).
    for sql in copyto_extra_cases() {
        if let Some(t) = BIN_TABLES.iter().find(|t| sql.contains(t.name)) {
            if table_ok.get(t.name) != Some(&true) {
                continue;
            }
        }
        let oa = a.apply(&sql);
        let ob = b.apply(&sql);
        record_case(&sql, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
    }

    (records, stats)
}

/// Single-engine exercise of the same deck (coverage runs: drive the
/// instrumented C server through every COPY TO / COPY FROM binary arm
/// without a peer). The engine's own COPY TO bytes feed its COPY FROM.
pub fn run_single(engine: &mut dyn Executor) -> (u32, u32) {
    fn run(
        ex: &mut dyn Executor,
        sql: &str,
        applied: &mut u32,
        errors: &mut u32,
    ) -> StmtOutcome {
        *applied += 1;
        let o = ex.apply(sql);
        if !is_ok(&o) {
            *errors += 1;
        }
        o
    }
    let (mut applied, mut errors) = (0u32, 0u32);
    for sql in suite_ddl() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    let roundtrip: Vec<(String, String)> = FIXTURE_TABLES
        .iter()
        .map(|(n, pk)| (n.to_string(), pk.to_string()))
        .chain(BIN_TABLES.iter().map(|t| (t.name.to_string(), "pk".to_string())))
        .collect();
    for (src, pk) in &roundtrip {
        let copy_to = format!("COPY {src} TO STDOUT (FORMAT binary);");
        let oa = run(engine, &copy_to, &mut applied, &mut errors);
        let StmtOutcome::CopyOut { bytes, .. } = oa else { continue };
        let clone = format!("{src}_rt");
        run(engine, &format!("CREATE TABLE {clone} (LIKE {src});"), &mut applied, &mut errors);
        applied += 1;
        let fa = engine
            .apply_copy_in(&format!("COPY {clone} FROM STDIN (FORMAT binary);"), &bytes);
        if !is_ok(&fa) {
            errors += 1;
        }
        run(engine, &format!("SELECT * FROM {clone} ORDER BY {pk};"), &mut applied, &mut errors);
        run(engine, &format!("COPY {clone} TO STDOUT (FORMAT binary);"), &mut applied, &mut errors);
        run(engine, &format!("DROP TABLE {clone};"), &mut applied, &mut errors);
    }
    for sql in copyto_extra_cases() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    (applied, errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::StmtOutcome;

    /// Scripted pair executor: answers COPY TO with a canned payload,
    /// COPY FROM by remembering what it was fed, probes with fixed rows.
    struct Fake {
        copy_payload: Vec<u8>,
        fed: Vec<Vec<u8>>,
    }

    impl Executor for Fake {
        fn apply(&mut self, sql: &str) -> StmtOutcome {
            if sql.starts_with("COPY ") && sql.contains(" TO STDOUT") {
                StmtOutcome::CopyOut {
                    bytes: self.copy_payload.clone(),
                    tag: "COPY 1".to_string(),
                }
            } else if sql.starts_with("SELECT ") {
                StmtOutcome::Rows {
                    col_oids: vec![23],
                    rows: vec![vec![Some("1".to_string())]],
                }
            } else {
                StmtOutcome::Command { tag: "OK".to_string(), affected: None }
            }
        }

        fn apply_copy_in(&mut self, _sql: &str, data: &[u8]) -> StmtOutcome {
            self.fed.push(data.to_vec());
            StmtOutcome::Command { tag: "COPY 1".to_string(), affected: Some(1) }
        }
    }

    #[test]
    fn suite_ddl_is_deterministic_and_complete() {
        assert_eq!(suite_ddl(), suite_ddl());
        let ddl = suite_ddl();
        for t in BIN_TABLES {
            assert!(ddl.iter().any(|s| s.starts_with("CREATE TABLE ") && s.contains(t.name)));
            assert!(
                ddl.iter().filter(|s| s.starts_with(&format!("INSERT INTO {} ", t.name))).count()
                    >= 4,
                "{} has too few seed rows",
                t.name
            );
        }
        // Every INSERT arity matches its CREATE's column count is enforced
        // live (the servers reject arity mismatches at suite runtime).
        assert!(ddl.iter().any(|s| s.contains("CREATE TYPE fz_mood")));
        assert!(ddl.iter().any(|s| s.contains("CREATE DOMAIN fz_posint")));
    }

    #[test]
    fn identical_engines_produce_zero_findings() {
        let mut a = Fake { copy_payload: b"PGCOPY-BYTES".to_vec(), fed: Vec::new() };
        let mut b = Fake { copy_payload: b"PGCOPY-BYTES".to_vec(), fed: Vec::new() };
        let table = crate::ruled::default_table();
        let (records, stats) = run_suite(&mut a, &mut b, &table, 4);
        assert_eq!(stats.findings, 0, "records: {records:?}");
        assert_eq!(stats.matches, stats.cases);
        // Both engines were fed the REFERENCE payload for every round-trip.
        assert_eq!(a.fed.len(), b.fed.len());
        assert!(a.fed.iter().all(|f| f == b"PGCOPY-BYTES"));
        assert!(b.fed.iter().all(|f| f == b"PGCOPY-BYTES"));
        assert_eq!(
            a.fed.len(),
            FIXTURE_TABLES.len() + BIN_TABLES.len(),
            "one round-trip feed per table"
        );
    }

    #[test]
    fn payload_divergence_is_one_finding_per_copy_case() {
        let mut a = Fake { copy_payload: b"PGCOPY-AAAA".to_vec(), fed: Vec::new() };
        let mut b = Fake { copy_payload: b"PGCOPY-BBBB".to_vec(), fed: Vec::new() };
        let table = crate::ruled::default_table();
        let (records, stats) = run_suite(&mut a, &mut b, &table, 4);
        assert!(stats.findings > 0);
        assert!(records
            .iter()
            .filter(|r| r.is_finding())
            .all(|r| r.class == DiffClass::RowsetDiff && r.sql.contains("COPY ")));
        // B is still fed A's (reference) bytes even while diverging.
        assert!(b.fed.iter().all(|f| f == b"PGCOPY-AAAA"));
    }

    #[test]
    fn user_type_oids_normalize_before_compare() {
        // Same query, drifted user-type oids (engine-local counters):
        // normalized compare matches; builtin-vs-user still diffs.
        let rows = vec![vec![Some("happy".to_string())]];
        let ua = StmtOutcome::Rows { col_oids: vec![23, 28290], rows: rows.clone() };
        let ub = StmtOutcome::Rows { col_oids: vec![23, 28284], rows: rows.clone() };
        let table = crate::ruled::default_table();
        let mut records = Vec::new();
        let mut stats = CopyBinStats::default();
        let c = record_case("SELECT ...;", &ua, &ub, &table, 4, 0, &mut records, &mut stats);
        assert_eq!(c, DiffClass::Match);
        let builtin = StmtOutcome::Rows { col_oids: vec![23, 25], rows };
        let c = record_case("SELECT ...;", &ua, &builtin, &table, 4, 1, &mut records, &mut stats);
        assert_eq!(c, DiffClass::RowsetDiff);
    }

    #[test]
    fn single_engine_pass_applies_whole_deck() {
        let mut e = Fake { copy_payload: b"PGCOPY-X".to_vec(), fed: Vec::new() };
        let (applied, errors) = run_single(&mut e);
        assert_eq!(errors, 0);
        assert!(applied as usize > suite_ddl().len());
        assert_eq!(e.fed.len(), FIXTURE_TABLES.len() + BIN_TABLES.len());
    }
}
