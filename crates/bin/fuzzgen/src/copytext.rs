//! COPY text/csv option-breadth differential suite (lane Q8, copy-variants
//! chunk of docs/fuzzing/sql-reachable-queue.tsv).
//!
//! X2 (crate::copybin) covered FORMAT binary; this suite drains the rest
//! of the copy.c/copyto.c/copyfromparse.c option surface:
//!
//! - COPY TO STDOUT over the text/csv option matrix (DELIMITER, NULL,
//!   HEADER, QUOTE, ESCAPE, FORCE_QUOTE cols/*, ENCODING, column lists,
//!   COPY (query) TO) — CopyOut payloads byte-compared A-vs-B.
//! - COPY FROM STDIN feeds (well-formed and malformed: wrong arity,
//!   unterminated quotes, bad escapes, bad encoding) — outcome-compared,
//!   then pk-ordered state probe + re-emit. Malformed feeds are matched
//!   error fuel (CopyFromErrorCallback / CopyLimitPrintoutLength).
//! - FROM-side options: NULL/DEFAULT markers, HEADER true/MATCH (and
//!   mismatched-header error), FORCE_NOT_NULL / FORCE_NULL (cols and *),
//!   ON_ERROR stop/ignore (+ REJECT_LIMIT, LOG_VERBOSITY), COPY ... WHERE,
//!   \xNN hex + octal escapes, multi-partition and BEFORE-trigger targets.
//! - COPY ... TO/FROM PROGRAM (superuser rig): success, failing and
//!   signaled child commands (common/wait_error.c decoding). Gated: if the
//!   first PROGRAM case diverges (engine lacks the feature) the one record
//!   is banked and the rest of the family is skipped, so an inventory gap
//!   costs one finding, not a flood.
//! - The chunk's expression tail: reg* casts + to_reg*() breadth +
//!   xid/xid8/cid ops + int2vector/oidvector literals, plus COPY BINARY
//!   round-trips of a reg*/xid/cid/int2vector/oidvector table (the *_recv
//!   / *_send rows). Values reference BUILTIN objects only (pinned oids)
//!   so binary payloads are cross-engine deterministic; xid_age is
//!   projected to a boolean (current-xid-dependent, never comparable).
//!
//! Determinism disciplines: deck tables are insert-only (heap order is
//! identical by construction — the COPY-order ruling makes re-ordered
//! heaps a non-surface, so the suite never updates/deletes deck rows);
//! every COPY (query) TO carries ORDER BY; the seeded arm draws all
//! randomness from the session PRNG (same seed = byte-identical case
//! list, options and payloads included).

use crate::diff::{classify, DiffClass, DiffInput, StmtOutcome};
use crate::rng::Rng;
use crate::ruled::{apply_ruled, RuledEntry};
use crate::runner::{Executor, Record};

/// One suite exchange: a plain statement or a COPY FROM STDIN feed.
pub enum CopyCase {
    Stmt(String),
    CopyIn { sql: String, data: Vec<u8> },
}

#[derive(Default, Debug)]
pub struct CopyTextStats {
    pub cases: u32,
    pub matches: u32,
    pub ruled: u32,
    pub findings: u32,
}

// ------------------------------------------------------------- fixtures ----

/// Hostile text corpus: delimiters, quotes, backslashes, embedded
/// newlines/tabs, NULL-lookalike strings, unicode, empty string.
const HOSTILE: &[&str] = &[
    "plain",
    "",
    "NA",
    "NULL",
    "has\ttab",
    "has\nnewline",
    "has\\backslash",
    "has\"quote",
    "has'apos",
    "comma,pipe|semi;",
    "\\N",
    "\\D",
    "caf\u{00e9} \u{4e2d}\u{6587} \u{1f4a9}",
    "  padded  ",
    "trailing\\",
];

/// Deck DDL + seed rows. Deterministic; insert-only ever after.
pub fn suite_ddl() -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    v.push(
        "CREATE TABLE fz_cx_main (pk int4 PRIMARY KEY, c_txt text, c_int int4, \
         c_num numeric, c_date date, c_bool bool, c_bytea bytea, c_def int4 DEFAULT 42);"
            .to_string(),
    );
    for (i, s) in HOSTILE.iter().enumerate() {
        let lit = s.replace('\'', "''").replace('\\', "\\\\");
        v.push(format!(
            "INSERT INTO fz_cx_main VALUES ({pk}, E'{lit}', {int}, {num}, \
             '2020-01-{day:02}', {b}, '\\x0{i:x}ff', {def});",
            pk = i + 1,
            lit = lit,
            int = (i as i64) * 7 - 3,
            num = format!("{}.{:03}", i, i * 37),
            day = (i % 28) + 1,
            b = if i % 2 == 0 { "true" } else { "false" },
            i = i % 16,
            def = i * 11,
        ));
    }
    v.push("INSERT INTO fz_cx_main VALUES (100, NULL, NULL, NULL, NULL, NULL, NULL, NULL);".into());
    // Multi-partition COPY FROM target (bulk-insert state across parts).
    v.push(
        "CREATE TABLE fz_cx_part (pk int4, v text) PARTITION BY RANGE (pk);".to_string(),
    );
    v.push("CREATE TABLE fz_cx_part_1 PARTITION OF fz_cx_part FOR VALUES FROM (0) TO (100);".into());
    v.push(
        "CREATE TABLE fz_cx_part_2 PARTITION OF fz_cx_part FOR VALUES FROM (100) TO (200);".into(),
    );
    v.push(
        "CREATE TABLE fz_cx_part_3 PARTITION OF fz_cx_part FOR VALUES FROM (200) TO (300);".into(),
    );
    // BEFORE ROW trigger target returning a modified tuple.
    v.push("CREATE TABLE fz_cx_trig (pk int4 PRIMARY KEY, v text);".to_string());
    v.push(
        "CREATE FUNCTION fz_cx_trigfn() RETURNS trigger LANGUAGE plpgsql AS \
         $$BEGIN NEW.v := NEW.v || '!'; RETURN NEW; END$$;"
            .to_string(),
    );
    v.push(
        "CREATE TRIGGER fz_cx_btrig BEFORE INSERT ON fz_cx_trig \
         FOR EACH ROW EXECUTE FUNCTION fz_cx_trigfn();"
            .to_string(),
    );
    // reg*/xid/cid/int2vector/oidvector table: builtin-object values only,
    // so text AND binary payloads are cross-engine deterministic.
    v.push(
        "CREATE TABLE fz_cx_reg (pk int4 PRIMARY KEY, c_regclass regclass, \
         c_regproc regproc, c_regprocedure regprocedure, c_regoper regoper, \
         c_regoperator regoperator, c_regtype regtype, c_regconfig regconfig, \
         c_regdictionary regdictionary, c_regnamespace regnamespace, \
         c_regcollation regcollation, c_regrole regrole, c_xid xid, c_xid8 xid8, \
         c_cid cid, c_i2v int2vector, c_oidv oidvector);"
            .to_string(),
    );
    v.push(
        "INSERT INTO fz_cx_reg VALUES (1, 'pg_class', 'boolin', 'lower(text)', \
         '|/', '+(int4,int4)', 'int4', 'english', 'simple', 'pg_catalog', \
         '\"C\"', 'postgres', '123', '9876543210', '7', '1 2 3', '10 20 30');"
            .to_string(),
    );
    v.push(
        "INSERT INTO fz_cx_reg VALUES (2, 'pg_type', 'textin', 'upper(text)', \
         '@-@', '-(int8,int8)', 'numeric', 'simple', 'english_stem', \
         'information_schema', '\"default\"', 'postgres', '4', '4', '0', '', '0');"
            .to_string(),
    );
    v.push(
        "INSERT INTO fz_cx_reg VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
            .to_string(),
    );
    // Qualified-format fuel: op + function living OUTSIDE the search path.
    v.push("CREATE SCHEMA fz_cx_s;".to_string());
    v.push(
        "CREATE FUNCTION fz_cx_s.qfun(int4, text) RETURNS int4 LANGUAGE sql \
         AS 'SELECT $1';"
            .to_string(),
    );
    v.push(
        "CREATE OPERATOR fz_cx_s.+ (leftarg = int4, rightarg = int4, function = int4pl);"
            .to_string(),
    );
    // Small target for PROGRAM feeds.
    v.push("CREATE TABLE fz_cx_p (a int4, b text);".to_string());
    v
}

// ------------------------------------------------------- COPY TO matrix ----

/// COPY TO STDOUT deck: byte-compared CopyOut payloads plus matched
/// option-error fuel (both sides must reject identically).
pub fn copyto_cases() -> Vec<String> {
    let mut v: Vec<String> = vec![
        // text arm.
        "COPY fz_cx_main TO STDOUT;".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, DELIMITER '|');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, DELIMITER '|', NULL 'NA');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, NULL '');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, HEADER);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, ENCODING 'UTF8');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, ENCODING 'SQL_ASCII');".into(),
        // csv arm.
        "COPY fz_cx_main TO STDOUT (FORMAT csv);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, HEADER);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, HEADER true);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, DELIMITER ';', QUOTE '''');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, QUOTE '\"', ESCAPE '\\');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, FORCE_QUOTE (c_txt));".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, FORCE_QUOTE (c_txt, c_num, c_date));".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, FORCE_QUOTE *);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, NULL 'NULL');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, DELIMITER E'\\t');".into(),
        // Column lists + query forms (ORDER BY pins the emit order).
        "COPY fz_cx_main (c_txt, pk) TO STDOUT (FORMAT csv);".into(),
        "COPY fz_cx_main (pk, c_bytea, c_bool) TO STDOUT;".into(),
        "COPY (SELECT pk, c_txt FROM fz_cx_main ORDER BY pk) TO STDOUT (FORMAT csv, HEADER);"
            .into(),
        "COPY (SELECT pk, c_int * 2 AS d FROM fz_cx_main ORDER BY pk DESC) TO STDOUT;".into(),
        "COPY (SELECT count(*) FROM fz_cx_main) TO STDOUT;".into(),
        "COPY (VALUES (1, 'a'), (2, 'b|c'), (3, NULL)) TO STDOUT (FORMAT csv);".into(),
        "COPY fz_cx_reg TO STDOUT;".into(),
        "COPY fz_cx_reg TO STDOUT (FORMAT csv);".into(),
        // Option-error fuel (matched rejects).
        "COPY fz_cx_main TO STDOUT (FORMAT csv, HEADER match);".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, FORCE_QUOTE (c_txt));".into(),
        "COPY fz_cx_main TO STDOUT (FORCE_NOT_NULL (c_txt));".into(),
        "COPY fz_cx_main TO STDOUT (FORCE_NULL (c_txt));".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, DELIMITER 'xy');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT csv, DELIMITER '\"', QUOTE '\"');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT text, QUOTE '\"');".into(),
        "COPY fz_cx_main TO STDOUT (ENCODING 'no_such_encoding');".into(),
        "COPY fz_cx_main TO STDOUT (FORMAT binary, HEADER);".into(),
        "COPY fz_cx_main TO STDOUT (ON_ERROR ignore);".into(),
        "COPY fz_cx_main TO STDOUT (LOG_VERBOSITY verbose);".into(),
        "COPY fz_cx_main TO STDOUT (DEFAULT 'x');".into(),
        "COPY fz_cx_main TO STDOUT (FREEZE);".into(),
        "COPY fz_cx_main (no_such_col) TO STDOUT;".into(),
        "COPY no_such_table TO STDOUT;".into(),
        "COPY (SELECT 1) FROM STDIN;".into(),
        // Server-side file errors (path identical on both sides).
        "COPY fz_cx_main TO '/nonexistent_fz_dir/out.txt';".into(),
        "COPY fz_cx_main TO 'relative_path.txt';".into(),
    ];
    // to_regtypemod / typemod-carrying render breadth rides here too.
    v.push("COPY (SELECT to_regtypemod('varchar(17)'), to_regtypemod('numeric(10,2)')) TO STDOUT;".into());
    v
}

// ----------------------------------------------------- COPY FROM matrix ----

/// One COPY FROM STDIN case: the statement, the payload, and whether the
/// clone table state should be probed after (skip for pure error fuel).
pub struct FromCase {
    pub sql: String,
    pub data: &'static str,
    pub label: &'static str,
}

/// Deterministic COPY FROM feeds into per-case scratch tables. The runner
/// TRUNCATEs the target between cases, so each case's probe is isolated.
pub fn copyfrom_cases() -> Vec<FromCase> {
    let t = "COPY fz_cx_scratch FROM STDIN";
    vec![
        FromCase {
            sql: format!("{t};"),
            data: "1\talpha\t10\t1.5\t2020-01-01\tt\t\\x00ff\t7\n2\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n",
            label: "text-wellformed",
        },
        FromCase {
            sql: format!("{t};"),
            data: "3\th\\x41x\t1\t0\t2020-01-01\tf\t\\x01\t1\n4\to\\101ct\t2\t0\t2020-01-02\tt\t\\x02\t2\n5\tt\\tb\\nn\\\\b\t3\t0\t2020-01-03\tf\t\\x03\t3\n",
            label: "text-escapes-hex-octal",
        },
        FromCase {
            sql: format!("{t} (DELIMITER '|', NULL 'NA');"),
            data: "6|pipe,y|4|1.25|2020-02-01|t|\\x04|4\n7|NA|NA|NA|NA|NA|NA|NA\n",
            label: "text-delim-null",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv);"),
            data: "8,\"quoted,comma\",5,2.5,2020-03-01,t,\\x05,5\n9,\"emb\"\"quote\",6,3.5,2020-03-02,f,\\x06,6\n10,\"multi\nline\",7,4.5,2020-03-03,t,\\x07,7\n",
            label: "csv-quoting",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, HEADER);"),
            data: "pk,c_txt,c_int,c_num,c_date,c_bool,c_bytea,c_def\n11,hdr,8,5.5,2020-04-01,t,\\x08,8\n",
            label: "csv-header-skip",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, HEADER match);"),
            data: "pk,c_txt,c_int,c_num,c_date,c_bool,c_bytea,c_def\n12,hm,9,6.5,2020-05-01,f,\\x09,9\n",
            label: "csv-header-match-ok",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, HEADER match);"),
            data: "pk,c_txt,WRONG,c_num,c_date,c_bool,c_bytea,c_def\n13,hх,1,1,2020-05-01,f,\\x0a,1\n",
            label: "csv-header-match-mismatch(error)",
        },
        FromCase {
            sql: format!("{t} (HEADER match);"),
            data: "pk\tc_txt\tc_int\tc_num\tc_date\tc_bool\tc_bytea\tc_def\n14\tthm\t1\t1\t2020-05-02\tt\t\\x0b\t2\n",
            label: "text-header-match-ok",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\');"),
            data: "15;'semi;colon';10;7.5;2020-06-01;t;\\x0c;10\n16;'esc\\'aped';11;8.5;2020-06-02;f;\\x0d;11\n",
            label: "csv-custom-quote-escape",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, NULL 'NULL', FORCE_NOT_NULL (c_txt));"),
            data: "17,NULL,12,9.5,2020-07-01,t,\\x0e,12\n",
            label: "csv-force-not-null",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, NULL 'NA', FORCE_NOT_NULL *);"),
            data: "18,NA,13,1.5,2020-07-02,f,\\x0f,13\n",
            label: "csv-force-not-null-star",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, QUOTE '\"', FORCE_NULL (c_txt));"),
            data: "19,\"\",14,2.5,2020-07-03,t,\\x10,14\n",
            label: "csv-force-null",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, FORCE_NULL *);"),
            data: "20,\"\",\"15\",\"3.5\",\"2020-07-04\",\"f\",\"\\x11\",\"15\"\n",
            label: "csv-force-null-star",
        },
        FromCase {
            sql: "COPY fz_cx_scratch (pk, c_txt, c_def) FROM STDIN (DEFAULT '\\D');".to_string(),
            data: "21\tdef\t\\D\n22\t\\D\t5\n",
            label: "default-marker",
        },
        FromCase {
            sql: "COPY fz_cx_scratch (pk, c_int) FROM STDIN (FORMAT csv, DEFAULT 'D');".to_string(),
            data: "23,D\n24,9\n",
            label: "csv-default-marker",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR stop);"),
            data: "25\tok\t1\t1\t2020-08-01\tt\t\\x12\t1\n26\tbad\tnot_an_int\t1\t2020-08-02\tt\t\\x13\t1\n",
            label: "on-error-stop(error)",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR ignore);"),
            data: "27\tok\t1\t1\t2020-08-03\tt\t\\x14\t1\n28\tbad\tnope\t1\t2020-08-04\tt\t\\x15\t1\n29\tok2\t2\t2\t2020-08-05\tf\t\\x16\t2\nbadrow_too_few\n",
            label: "on-error-ignore",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR ignore, LOG_VERBOSITY verbose);"),
            data: "30\tok\t1\t1\t2020-08-06\tt\t\\x17\t1\n31\tbad\tx\t1\t2020-08-07\tt\t\\x18\t1\n",
            label: "on-error-ignore-verbose",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR ignore, LOG_VERBOSITY silent);"),
            data: "32\tok\t1\t1\t2020-08-08\tt\t\\x19\t1\n33\tbad\ty\t1\t2020-08-09\tt\t\\x1a\t1\n",
            label: "on-error-ignore-silent",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR ignore, REJECT_LIMIT 2);"),
            data: "34\tok\t1\t1\t2020-09-01\tt\t\\x1b\t1\n35\tb1\tx\t1\t2020-09-02\tt\t\\x1c\t1\n36\tb2\ty\t1\t2020-09-03\tt\t\\x1d\t1\n",
            label: "reject-limit-under",
        },
        FromCase {
            sql: format!("{t} (ON_ERROR ignore, REJECT_LIMIT 2);"),
            data: "37\tok\t1\t1\t2020-09-04\tt\t\\x1e\t1\n38\tb1\tx\t1\t2020-09-05\tt\t\\x1f\t1\n39\tb2\ty\t1\t2020-09-06\tt\t\\x20\t1\n40\tb3\tz\t1\t2020-09-07\tt\t\\x21\t1\n",
            label: "reject-limit-exceeded(error)",
        },
        FromCase {
            sql: format!("{t} WHERE c_int > 5;"),
            data: "41\tlow\t1\t1\t2020-10-01\tt\t\\x22\t1\n42\thigh\t10\t1\t2020-10-02\tt\t\\x23\t1\n",
            label: "from-where",
        },
        FromCase {
            sql: format!("{t} WHERE (SELECT true);"),
            data: "43\tsub\t1\t1\t2020-10-03\tt\t\\x24\t1\n",
            label: "from-where-subquery(error)",
        },
        // Malformed-data error fuel (matched errors; scratch stays empty).
        FromCase {
            sql: format!("{t};"),
            data: "44\ttoo_few_columns\n",
            label: "text-missing-cols(error)",
        },
        FromCase {
            sql: format!("{t};"),
            data: "45\ta\t1\t1\t2020-01-01\tt\t\\x25\t1\textra_column\n",
            label: "text-extra-cols(error)",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv);"),
            data: "46,\"unterminated quote\n",
            label: "csv-unterminated-quote(error)",
        },
        FromCase {
            sql: format!("{t};"),
            data: "47\tbad_date\t1\t1\tnot-a-date\tt\t\\x26\t1\n",
            label: "text-bad-date(error)",
        },
        FromCase {
            sql: format!("{t};"),
            data: "48\tlong_field_error_context_padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_end\tnot_int\t1\t2020-01-01\tt\t\\x27\t1\n",
            label: "text-long-field-error-context(error)",
        },
        FromCase {
            sql: format!("{t};"),
            data: "49\tbad hex \\xZZ tail\t1\t1\t2020-01-01\tt\t\\x28\t1\n",
            label: "text-bare-backslash-x",
        },
        FromCase {
            sql: format!("{t} (FORMAT csv, ENCODING 'LATIN1');"),
            data: "50,caf\u{00e9}-as-latin1-\u{00ff},1,1,2020-01-01,t,\\x29,1\n",
            label: "csv-encoding-latin1",
        },
        FromCase {
            sql: format!("{t} (ENCODING 'UTF8');"),
            data: "51\t\u{fffd}ok\t1\t1\t2020-01-01\tt\t\\x2a\t1\n",
            label: "text-encoding-utf8",
        },
        // Partitioned + trigger targets.
        FromCase {
            sql: "COPY fz_cx_part FROM STDIN;".to_string(),
            data: "5\tp1\n150\tp2\n250\tp3\n15\tp1b\n160\tp2b\n260\tp3b\n",
            label: "partitioned-spread",
        },
        FromCase {
            sql: "COPY fz_cx_trig FROM STDIN (FORMAT csv);".to_string(),
            data: "1,alpha\n2,beta\n",
            label: "before-trigger-target",
        },
        // FREEZE outside the creating transaction: matched error.
        FromCase {
            sql: format!("{t} (FREEZE);"),
            data: "52\tfrozen\t1\t1\t2020-01-01\tt\t\\x2b\t1\n",
            label: "freeze-outside-txn(error)",
        },
    ]
}

/// Per-FromCase probe target: everything feeds fz_cx_scratch except the
/// partition/trigger cases.
fn from_case_target(sql: &str) -> (&'static str, &'static str) {
    if sql.contains("fz_cx_part") {
        ("fz_cx_part", "pk")
    } else if sql.contains("fz_cx_trig") {
        ("fz_cx_trig", "pk")
    } else {
        ("fz_cx_scratch", "pk")
    }
}

// -------------------------------------------------------- scalar decks ----

/// reg* / xid / cid / int2vector expression deck (deterministic SELECTs;
/// every query is totally determined by builtin catalog state).
pub fn scalar_cases() -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    // out-funcs via ::text round-trips, unqualified and qualified.
    for (lit, ty) in [
        ("pg_class", "regclass"),
        ("boolin", "regproc"),
        ("lower(text)", "regprocedure"),
        ("|/", "regoper"),
        ("+(int4,int4)", "regoperator"),
        ("int4", "regtype"),
        ("english", "regconfig"),
        ("simple", "regdictionary"),
        ("pg_catalog", "regnamespace"),
        ("\"C\"", "regcollation"),
        ("postgres", "regrole"),
    ] {
        let l = lit.replace('"', "\"\"");
        v.push(format!("SELECT '{l}'::{ty}, '{l}'::{ty}::text;", l = l.replace('"', "\""), ty = ty));
    }
    // Qualified renders: objects outside the search path.
    v.push("SELECT 'fz_cx_s.qfun(int4,text)'::regprocedure::text;".into());
    v.push("SELECT 'fz_cx_s.+(int4,int4)'::regoperator::text;".into());
    v.push("SELECT 'fz_cx_s.qfun'::regproc::text;".into());
    // Force-qualified identity via pg_identify_object (builtin oids only).
    v.push(
        "SELECT * FROM pg_identify_object('pg_operator'::regclass, '|/'::regoper::oid, 0);".into(),
    );
    v.push(
        "SELECT * FROM pg_identify_object('pg_proc'::regclass, 'lower(text)'::regprocedure::oid, 0);"
            .into(),
    );
    v.push(
        "SELECT pg_describe_object('pg_operator'::regclass, '+(int4,int4)'::regoperator::oid, 0);"
            .into(),
    );
    // to_reg* breadth: hit, miss (NULL), and syntax-error fuel.
    for f in [
        "to_regclass", "to_regproc", "to_regprocedure", "to_regoper", "to_regoperator",
        "to_regtype", "to_regnamespace", "to_regrole", "to_regcollation",
    ] {
        let hit = match f {
            "to_regclass" => "pg_class",
            "to_regproc" => "boolin",
            "to_regprocedure" => "lower(text)",
            "to_regoper" => "|/",
            "to_regoperator" => "+(int4,int4)",
            "to_regtype" => "integer",
            "to_regnamespace" => "pg_catalog",
            "to_regrole" => "postgres",
            _ => "\"C\"",
        };
        v.push(format!(
            "SELECT {f}('{hit}') IS NOT NULL, {f}('no_such_object_fz'), {f}('pg_catalog.no_such_fz');",
            f = f,
            hit = hit.replace('"', "\"\"")
        ));
    }
    v.push("SELECT to_regtype('varchar(10)'), to_regtypemod('varchar(10)'), to_regtypemod('numeric(12,3)'), to_regtypemod('integer');".into());
    v.push("SELECT to_regclass('one.two.three.four');".into());
    v.push("SELECT 'mytext'::text::regclass;".into()); // text_regclass error path
    // xid / xid8 ops.
    v.push(
        "SELECT '123'::xid = '123'::xid, '123'::xid <> '124'::xid, '17'::xid::text;".into(),
    );
    v.push("SELECT hashxid('123'::xid), hashxidextended('123'::xid, 42);".into());
    v.push(
        "SELECT a = b, a <> b, a < b, a <= b, a > b, a >= b FROM \
         (VALUES ('5'::xid8, '9'::xid8)) t(a, b);"
            .into(),
    );
    v.push("SELECT max(c_xid8), min(c_xid8) FROM fz_cx_reg;".into());
    v.push("SELECT c_xid8 FROM fz_cx_reg WHERE c_xid8 IS NOT NULL ORDER BY c_xid8;".into());
    v.push("SELECT '9876543210'::xid8::xid, '18446744073709551615'::xid8::text;".into());
    v.push("SELECT hashxid8('77'::xid8), hashxid8extended('77'::xid8, 7);".into());
    v.push("SELECT age('123'::xid) IS NOT NULL;".into()); // current-xid-dependent: projected
    // cid ops.
    v.push("SELECT '7'::cid = '7'::cid, '7'::cid::text, hashcid('7'::cid), hashcidextended('7'::cid, 3);".into());
    // int2vector / oidvector io + validity.
    v.push("SELECT '1 2 3'::int2vector, '1 2 3'::int2vector::text, ''::int2vector;".into());
    v.push("SELECT '99999999'::int2vector;".into()); // out-of-range error
    v.push("SELECT '10 20 30'::oidvector::text;".into());
    v
}

// ------------------------------------------------------- PROGRAM cases ----

/// COPY TO/FROM PROGRAM deck (superuser rig). Deterministic commands only.
pub fn program_cases() -> Vec<CopyCase> {
    vec![
        CopyCase::Stmt("COPY (SELECT 1, 'x' ORDER BY 1) TO PROGRAM 'cat > /dev/null';".into()),
        CopyCase::Stmt("COPY fz_cx_p FROM PROGRAM 'printf ''1\\tone\\n2\\ttwo\\n''';".into()),
        CopyCase::Stmt("SELECT * FROM fz_cx_p ORDER BY a;".into()),
        CopyCase::Stmt("COPY (SELECT 1) TO PROGRAM 'exit 3';".into()),
        CopyCase::Stmt("COPY fz_cx_p FROM PROGRAM 'exit 5';".into()),
        CopyCase::Stmt("COPY fz_cx_p FROM PROGRAM 'kill -TERM $$';".into()),
        CopyCase::Stmt("COPY (SELECT generate_series(1,100000)) TO PROGRAM 'head -c 10 > /dev/null';".into()),
    ]
}

// --------------------------------------------------------- seeded arm ----

/// Seeded random COPY cases: option combos + payload fuzz, all randomness
/// from the PRNG (same seed = identical case list). Errors are expected to
/// MATCH between the engines; state probes catch silent divergence.
pub fn seeded_cases(seed: u64, n: u32) -> Vec<CopyCase> {
    let mut rng = Rng::new(seed ^ 0xc0f7_7e57);
    let mut v: Vec<CopyCase> = Vec::new();
    let delims = ['\t', '|', ',', ';'];
    let null_strs = ["", "NA", "NULL", "\\N", "nil"];
    for _ in 0..n {
        let csv = rng.chance(1, 2);
        let delim = *rng.pick(&delims);
        let nullstr = null_strs[rng.below_usize(null_strs.len())];
        // Options (valid combos only; option errors live in the
        // deterministic deck where they are hand-verified).
        let mut opts: Vec<String> =
            vec![format!("FORMAT {}", if csv { "csv" } else { "text" })];
        if delim != '\t' || rng.chance(1, 4) {
            opts.push(format!("DELIMITER E'{}'", esc_ch(delim)));
        }
        if !nullstr.is_empty() || rng.chance(1, 4) {
            // NULL string must not contain the delimiter.
            if !nullstr.contains(delim) {
                opts.push(format!("NULL E'{}'", esc_str(nullstr)));
            }
        }
        let on_error_ignore = rng.chance(1, 3);
        if on_error_ignore {
            opts.push("ON_ERROR ignore".to_string());
            if rng.chance(1, 3) {
                opts.push(format!("REJECT_LIMIT {}", 1 + rng.below(4)));
            }
            if rng.chance(1, 3) {
                opts.push(
                    if rng.chance(1, 2) { "LOG_VERBOSITY verbose" } else { "LOG_VERBOSITY silent" }
                        .to_string(),
                );
            }
        }
        if csv && rng.chance(1, 4) {
            opts.push("FORCE_NOT_NULL (c_txt)".to_string());
        }
        if csv && rng.chance(1, 5) {
            opts.push("FORCE_NULL (c_txt)".to_string());
        }
        let sql = format!("COPY fz_cx_scratch FROM STDIN ({});", opts.join(", "));
        // Payload: 1-6 rows, mostly well-formed, occasional arity error /
        // hostile field content.
        let rows = 1 + rng.below(6);
        let mut data = String::new();
        for r in 0..rows {
            let pk = 1000 + (v.len() as u64 * 10) + r;
            let arity_bad = rng.chance(1, 12);
            let mut fields: Vec<String> = vec![pk.to_string()];
            let txt = seeded_field(&mut rng, csv, delim, nullstr);
            fields.push(txt);
            fields.push(if rng.chance(1, 10) { "oops".into() } else { rng.below(1000).to_string() });
            fields.push(format!("{}.{}", rng.below(100), rng.below(1000)));
            fields.push(format!("2021-{:02}-{:02}", 1 + rng.below(12), 1 + rng.below(28)));
            fields.push(if rng.chance(1, 2) { "t" } else { "f" }.to_string());
            fields.push(format!("\\x{:02x}", rng.below(256)));
            fields.push(rng.below(100).to_string());
            if arity_bad {
                fields.truncate(1 + rng.below_usize(7));
            }
            data.push_str(&fields.join(&delim.to_string()));
            data.push('\n');
        }
        v.push(CopyCase::CopyIn { sql, data: data.into_bytes() });
        // Occasionally re-emit with a random TO option set.
        if rng.chance(1, 3) {
            let hdr = if csv && rng.chance(1, 2) { ", HEADER" } else { "" };
            v.push(CopyCase::Stmt(format!(
                "COPY fz_cx_scratch TO STDOUT (FORMAT {}, DELIMITER E'{}'{});",
                if csv { "csv" } else { "text" },
                esc_ch(delim),
                hdr
            )));
        }
    }
    v
}

fn esc_ch(c: char) -> String {
    match c {
        '\t' => "\\t".to_string(),
        '\'' => "''".to_string(),
        '\\' => "\\\\".to_string(),
        c => c.to_string(),
    }
}

fn esc_str(s: &str) -> String {
    s.chars().map(esc_ch).collect()
}

/// One random text/csv field: hostile alphabet, escapes valid for the arm.
fn seeded_field(rng: &mut Rng, csv: bool, delim: char, nullstr: &str) -> String {
    match rng.below(10) {
        0 => nullstr.to_string(),
        1 if csv => format!("\"emb{delim}edded\""),
        1 => "plain".to_string(),
        2 if csv => "\"q\"\"q\"".to_string(),
        2 => "\\x41hex".to_string(),
        3 if !csv => "\\101octal".to_string(),
        3 => "\"wrapped\"".to_string(),
        4 if !csv => "tab\\there".to_string(),
        4 => "comma,val".to_string(),
        5 => "caf\u{00e9}\u{4e2d}".to_string(),
        6 => "x".repeat(1 + rng.below_usize(40)),
        7 if !csv => "back\\\\slash".to_string(),
        7 => "back\\slash".to_string(),
        8 => String::new(),
        _ => format!("v{}", rng.below(100_000)),
    }
}

// ------------------------------------------------------------- running ----

fn is_ok(o: &StmtOutcome) -> bool {
    !matches!(o, StmtOutcome::Error { .. } | StmtOutcome::ConnLost { .. })
}

#[allow(clippy::too_many_arguments)]
fn record_case(
    sql: &str,
    oa: &StmtOutcome,
    ob: &StmtOutcome,
    table: &[RuledEntry],
    ulp_tol: u64,
    case_index: u32,
    records: &mut Vec<Record>,
    stats: &mut CopyTextStats,
) -> DiffClass {
    let raw = classify(&DiffInput { sql, a: oa, b: ob, ulp_tol, soft_cols: &[] });
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

/// The scratch table every COPY FROM case feeds (LIKE the main deck table
/// including its DEFAULT — the DEFAULT-marker cases need it).
const SCRATCH_DDL: &str =
    "CREATE TABLE fz_cx_scratch (LIKE fz_cx_main INCLUDING DEFAULTS);";

/// Matched-option round-trips: (TO options, FROM options) pairs whose
/// emitted payload must reload losslessly.
const ROUNDTRIP_OPTS: &[(&str, &str)] = &[
    ("", ""),
    ("(FORMAT text, DELIMITER '|', NULL 'NA')", "(FORMAT text, DELIMITER '|', NULL 'NA')"),
    ("(FORMAT csv)", "(FORMAT csv)"),
    ("(FORMAT csv, HEADER)", "(FORMAT csv, HEADER match)"),
    (
        "(FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\')",
        "(FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\')",
    ),
    ("(FORMAT csv, FORCE_QUOTE *)", "(FORMAT csv)"),
];

/// Run the full differential suite. `a` is the reference engine; its COPY
/// TO payloads are the spec fed into both sides on round-trips.
pub fn run_suite(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    table: &[RuledEntry],
    ulp_tol: u64,
    seed: u64,
    seeded_n: u32,
) -> (Vec<Record>, CopyTextStats) {
    let mut records = Vec::new();
    let mut stats = CopyTextStats::default();
    let mut idx = 0u32;
    let mut next = || {
        idx += 1;
        idx - 1
    };

    let duo = |sql: &str,
                   a: &mut dyn Executor,
                   b: &mut dyn Executor,
                   records: &mut Vec<Record>,
                   stats: &mut CopyTextStats,
                   next: &mut dyn FnMut() -> u32| {
        let oa = a.apply(sql);
        let ob = b.apply(sql);
        record_case(sql, &oa, &ob, table, ulp_tol, next(), records, stats)
    };

    for sql in suite_ddl() {
        duo(&sql, a, b, &mut records, &mut stats, &mut next);
    }
    duo(SCRATCH_DDL, a, b, &mut records, &mut stats, &mut next);

    // 1. COPY TO matrix (payload byte compare + matched option errors).
    for sql in copyto_cases() {
        duo(&sql, a, b, &mut records, &mut stats, &mut next);
    }

    // 2. COPY FROM matrix: truncate, feed, probe, re-emit.
    for case in copyfrom_cases() {
        let (target, pk) = from_case_target(&case.sql);
        let trunc = format!("TRUNCATE {target};");
        duo(&trunc, a, b, &mut records, &mut stats, &mut next);
        let fa = a.apply_copy_in(&case.sql, case.data.as_bytes());
        let fb = b.apply_copy_in(&case.sql, case.data.as_bytes());
        record_case(&case.sql, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);
        let probe = format!("SELECT * FROM {target} ORDER BY {pk};");
        duo(&probe, a, b, &mut records, &mut stats, &mut next);
    }

    // 3. Matched-option round-trips of A's own emitted payload.
    for (to_opts, from_opts) in ROUNDTRIP_OPTS {
        let sep_to = if to_opts.is_empty() { "" } else { " " };
        let copy_to = format!("COPY fz_cx_main TO STDOUT{sep_to}{to_opts};");
        let oa = a.apply(&copy_to);
        let ob = b.apply(&copy_to);
        record_case(&copy_to, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
        let StmtOutcome::CopyOut { bytes, .. } = &oa else { continue };
        let spec = bytes.clone();
        duo("TRUNCATE fz_cx_scratch;", a, b, &mut records, &mut stats, &mut next);
        let sep_from = if from_opts.is_empty() { "" } else { " " };
        let copy_from = format!("COPY fz_cx_scratch FROM STDIN{sep_from}{from_opts};");
        let fa = a.apply_copy_in(&copy_from, &spec);
        let fb = b.apply_copy_in(&copy_from, &spec);
        record_case(&copy_from, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);
        if is_ok(&fa) && is_ok(&fb) {
            duo(
                "SELECT * FROM fz_cx_scratch ORDER BY pk;",
                a,
                b,
                &mut records,
                &mut stats,
                &mut next,
            );
            let sep = if to_opts.is_empty() { "" } else { " " };
            let re_emit = format!("COPY fz_cx_scratch TO STDOUT{sep}{to_opts};");
            duo(&re_emit, a, b, &mut records, &mut stats, &mut next);
        }
    }

    // 4. COPY BINARY of the reg*/xid/cid/vector table (*_send / *_recv).
    {
        let copy_to = "COPY fz_cx_reg TO STDOUT (FORMAT binary);";
        let oa = a.apply(copy_to);
        let ob = b.apply(copy_to);
        record_case(copy_to, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
        if let StmtOutcome::CopyOut { bytes, .. } = &oa {
            let spec = bytes.clone();
            duo(
                "CREATE TABLE fz_cx_reg_rt (LIKE fz_cx_reg);",
                a,
                b,
                &mut records,
                &mut stats,
                &mut next,
            );
            let copy_from = "COPY fz_cx_reg_rt FROM STDIN (FORMAT binary);";
            let fa = a.apply_copy_in(copy_from, &spec);
            let fb = b.apply_copy_in(copy_from, &spec);
            record_case(copy_from, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);
            duo(
                "SELECT * FROM fz_cx_reg_rt ORDER BY pk;",
                a,
                b,
                &mut records,
                &mut stats,
                &mut next,
            );
            duo(
                "COPY fz_cx_reg_rt TO STDOUT (FORMAT binary);",
                a,
                b,
                &mut records,
                &mut stats,
                &mut next,
            );
            duo("DROP TABLE fz_cx_reg_rt;", a, b, &mut records, &mut stats, &mut next);
        }
    }

    // 5. Scalar deck (reg*/xid/cid breadth).
    for sql in scalar_cases() {
        duo(&sql, a, b, &mut records, &mut stats, &mut next);
    }

    // 6. FREEZE inside the creating transaction (positive arm).
    {
        for sql in
            ["BEGIN;", "CREATE TABLE fz_cx_frz (a int4, b text);"]
        {
            duo(sql, a, b, &mut records, &mut stats, &mut next);
        }
        let sql = "COPY fz_cx_frz FROM STDIN (FREEZE);";
        let fa = a.apply_copy_in(sql, b"1\tone\n2\ttwo\n");
        let fb = b.apply_copy_in(sql, b"1\tone\n2\ttwo\n");
        record_case(sql, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);
        for sql in ["COMMIT;", "SELECT * FROM fz_cx_frz ORDER BY a;", "DROP TABLE fz_cx_frz;"] {
            duo(sql, a, b, &mut records, &mut stats, &mut next);
        }
    }

    // 7. PROGRAM family, gated on the first case matching.
    let progs = program_cases();
    let mut program_supported = true;
    for (i, case) in progs.iter().enumerate() {
        let sql = match case {
            CopyCase::Stmt(s) => s.clone(),
            CopyCase::CopyIn { sql, .. } => sql.clone(),
        };
        let oa = a.apply(&sql);
        let ob = b.apply(&sql);
        let class =
            record_case(&sql, &oa, &ob, table, ulp_tol, next(), &mut records, &mut stats);
        if i == 0 && class != DiffClass::Match {
            program_supported = false;
        }
        if !program_supported {
            break; // one inventory record, not a flood
        }
    }

    // 8. Seeded arm.
    for case in seeded_cases(seed, seeded_n) {
        match case {
            CopyCase::Stmt(sql) => {
                duo(&sql, a, b, &mut records, &mut stats, &mut next);
            }
            CopyCase::CopyIn { sql, data } => {
                let trunc = "TRUNCATE fz_cx_scratch;";
                duo(trunc, a, b, &mut records, &mut stats, &mut next);
                let fa = a.apply_copy_in(&sql, &data);
                let fb = b.apply_copy_in(&sql, &data);
                record_case(&sql, &fa, &fb, table, ulp_tol, next(), &mut records, &mut stats);
                duo(
                    "SELECT * FROM fz_cx_scratch ORDER BY pk;",
                    a,
                    b,
                    &mut records,
                    &mut stats,
                    &mut next,
                );
            }
        }
    }

    (records, stats)
}

/// Single-engine deck (coverage runs): same cases, engine's own payloads.
pub fn run_single(engine: &mut dyn Executor, seed: u64, seeded_n: u32) -> (u32, u32) {
    let (mut applied, mut errors) = (0u32, 0u32);
    let run = |ex: &mut dyn Executor, sql: &str, applied: &mut u32, errors: &mut u32| {
        *applied += 1;
        let o = ex.apply(sql);
        if !is_ok(&o) {
            *errors += 1;
        }
        o
    };
    for sql in suite_ddl() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    run(engine, SCRATCH_DDL, &mut applied, &mut errors);
    for sql in copyto_cases() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    for case in copyfrom_cases() {
        let (target, pk) = from_case_target(&case.sql);
        run(engine, &format!("TRUNCATE {target};"), &mut applied, &mut errors);
        applied += 1;
        if !is_ok(&engine.apply_copy_in(&case.sql, case.data.as_bytes())) {
            errors += 1;
        }
        run(engine, &format!("SELECT * FROM {target} ORDER BY {pk};"), &mut applied, &mut errors);
    }
    for (to_opts, from_opts) in ROUNDTRIP_OPTS {
        let sep_to = if to_opts.is_empty() { "" } else { " " };
        let o = run(
            engine,
            &format!("COPY fz_cx_main TO STDOUT{sep_to}{to_opts};"),
            &mut applied,
            &mut errors,
        );
        let StmtOutcome::CopyOut { bytes, .. } = o else { continue };
        run(engine, "TRUNCATE fz_cx_scratch;", &mut applied, &mut errors);
        applied += 1;
        let sep_from = if from_opts.is_empty() { "" } else { " " };
        if !is_ok(&engine.apply_copy_in(
            &format!("COPY fz_cx_scratch FROM STDIN{sep_from}{from_opts};"),
            &bytes,
        )) {
            errors += 1;
        }
    }
    {
        let o = run(engine, "COPY fz_cx_reg TO STDOUT (FORMAT binary);", &mut applied, &mut errors);
        if let StmtOutcome::CopyOut { bytes, .. } = o {
            run(engine, "CREATE TABLE fz_cx_reg_rt (LIKE fz_cx_reg);", &mut applied, &mut errors);
            applied += 1;
            if !is_ok(
                &engine.apply_copy_in("COPY fz_cx_reg_rt FROM STDIN (FORMAT binary);", &bytes),
            ) {
                errors += 1;
            }
            run(engine, "DROP TABLE fz_cx_reg_rt;", &mut applied, &mut errors);
        }
    }
    for sql in scalar_cases() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    for sql in ["BEGIN;", "CREATE TABLE fz_cx_frz (a int4, b text);"] {
        run(engine, sql, &mut applied, &mut errors);
    }
    applied += 1;
    if !is_ok(&engine.apply_copy_in("COPY fz_cx_frz FROM STDIN (FREEZE);", b"1\tone\n")) {
        errors += 1;
    }
    for sql in ["COMMIT;", "DROP TABLE fz_cx_frz;"] {
        run(engine, sql, &mut applied, &mut errors);
    }
    for case in program_cases() {
        if let CopyCase::Stmt(sql) = case {
            run(engine, &sql, &mut applied, &mut errors);
        }
    }
    for case in seeded_cases(seed, seeded_n) {
        match case {
            CopyCase::Stmt(sql) => {
                run(engine, &sql, &mut applied, &mut errors);
            }
            CopyCase::CopyIn { sql, data } => {
                run(engine, "TRUNCATE fz_cx_scratch;", &mut applied, &mut errors);
                applied += 1;
                if !is_ok(&engine.apply_copy_in(&sql, &data)) {
                    errors += 1;
                }
            }
        }
    }
    (applied, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decks_are_deterministic() {
        assert_eq!(suite_ddl(), suite_ddl());
        assert_eq!(copyto_cases(), copyto_cases());
        let a = copyfrom_cases();
        let b = copyfrom_cases();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.sql, y.sql);
            assert_eq!(x.data, y.data);
        }
        assert_eq!(scalar_cases(), scalar_cases());
    }

    #[test]
    fn seeded_arm_is_seed_stable_and_seed_sensitive() {
        let render = |seed: u64| {
            seeded_cases(seed, 50)
                .iter()
                .map(|c| match c {
                    CopyCase::Stmt(s) => s.clone(),
                    CopyCase::CopyIn { sql, data } => {
                        format!("{sql}<<{}>>", String::from_utf8_lossy(data))
                    }
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(render(7), render(7));
        assert_ne!(render(7), render(8));
    }

    #[test]
    fn seeded_null_string_never_contains_delimiter() {
        for seed in 0..8u64 {
            for c in seeded_cases(seed, 100) {
                if let CopyCase::CopyIn { sql, .. } = c {
                    if let Some(d) = sql.find("DELIMITER E'") {
                        let delim = &sql[d + 12..sql[d + 12..].find('\'').unwrap() + d + 12];
                        if let Some(n) = sql.find("NULL E'") {
                            let nul = &sql[n + 7..sql[n + 7..].find('\'').unwrap() + n + 7];
                            assert!(
                                !nul.contains(delim),
                                "NULL string {nul:?} contains delimiter {delim:?}: {sql}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Every COPY (query) TO in the deck orders its rows (COPY-order
    /// ruling: only table-heap order is exempt, query output is not).
    #[test]
    fn query_copies_carry_order_by() {
        for sql in copyto_cases() {
            if sql.starts_with("COPY (SELECT")
                && sql.contains("TO STDOUT")
                && !sql.contains("count(*)")
                && !sql.contains("to_regtypemod")
            {
                assert!(sql.contains("ORDER BY"), "unordered query COPY: {sql}");
            }
        }
    }
}
