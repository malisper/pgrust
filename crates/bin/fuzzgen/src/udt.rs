//! UDT: user-defined types differential module (enum / composite / domain).
//!
//! Drains the SQL-reachable user-defined-type surface — the enum machinery
//! (enum.c / pg_enum), the composite/row-type machinery (rowtypes.c +
//! typecmds.c ALTER TYPE attribute paths), and the domain machinery
//! (domains.c + typecmds.c ALTER DOMAIN paths). Each group is self-contained
//! in the plpg/coll style: create fresh type(s), exercise every reachable
//! operation, probe result identity, then DROP everything (CASCADE on the
//! final type/domain drop so a missed dependency can never leak into later
//! groups). Names carry session-persistent counters (`UdtState`) so they
//! stay session-unique even across a group whose drop failed.
//!
//! What the differential pair checks (three distinct identities):
//!   - RESULT identity: every table probe is total-ordered (`ORDER BY v, pk`)
//!     and every scalar probe is a deterministic literal-driven value, so a
//!     byte-difference in enum sort order, composite I/O text, or a
//!     value-returning cast is a finding.
//!   - ERROR identity: the deliberately-invalid casts / INSERTs (domain CHECK
//!     and NOT NULL violations, out-of-range enum labels) must fail with the
//!     SAME SQLSTATE + message on both engines (autocommit standalone
//!     statements — a failing statement rolls back only itself, so the group
//!     continues; groups are never inside a txn bracket).
//!   - CONSTRAINT-DECISION identity (the HIGH-severity target): whether a
//!     domain ACCEPTS or REJECTS a given value — on cast, on assignment, on
//!     INSERT, and on ALTER DOMAIN ... VALIDATE CONSTRAINT — must match C
//!     exactly. Every domain probe here is a controlled accept/reject pair so
//!     any decision divergence surfaces immediately.
//!
//! Determinism disciplines (same seed + same weights = byte-identical
//! stream): every label / value / base type is drawn from a fixed constant
//! pool through the session PRNG; enum label slices are chosen so the base
//! set and the ALTER-ADDed labels can never collide; domain accept/reject
//! literals are fixed per base type so the accept/reject decision is fixed.
//!
//! Trap notes:
//!   - ALTER TYPE ... ADD VALUE followed by USE of the new value is only
//!     legal across a commit boundary; because module groups run as
//!     standalone autocommit statements (never inside a txn bracket) each
//!     ADD VALUE commits before the next statement reads it.
//!   - ALTER TYPE ATTRIBUTE surgery runs after the dependent table is
//!     dropped, so no CASCADE-into-column recursion is needed and the
//!     attribute-path coverage stays a clean success.

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counters (the objects themselves are group-local
/// and always dropped in-group; only the counters survive so names stay
/// session-unique even after a failed drop).
#[derive(Clone, Debug, Default)]
pub struct UdtState {
    next_enum: u32,
    next_comp: u32,
    next_domain: u32,
    next_table: u32,
}

impl UdtState {
    pub fn new() -> UdtState {
        UdtState::default()
    }
}

/// Enum label pool. Every entry is a bare identifier-safe, SQL-literal-safe
/// token (no quotes/commas/parens). A group takes a non-wrapping window so
/// the base set and the ALTER-ADD labels are guaranteed distinct.
const ENUM_LABELS: &[&str] = &[
    "red", "green", "blue", "cyan", "magenta", "yellow", "black", "white",
];

fn lit(s: &str) -> String {
    format!("'{}'", s)
}

/// UDT group entry point: pick a family and emit its self-contained group.
pub fn gen_udt_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("udt");
    let form = g.weights.pick(g.rng, &["udt:enum", "udt:composite", "udt:domain"]);
    g.fire(form);
    match form {
        "udt:enum" => gen_enum_group(g),
        "udt:composite" => gen_composite_group(g),
        _ => gen_domain_group(g),
    }
}

// ---------------------------------------------------------------------------
// ENUM
// ---------------------------------------------------------------------------

/// CREATE TYPE AS ENUM + comparison/ordering, enum_range/first/last, enum in
/// a table (total-ordered), enum arrays, ALTER TYPE ADD VALUE BEFORE/AFTER,
/// RENAME VALUE — then drop.
fn gen_enum_group(g: &mut Gen) -> Vec<StmtKind> {
    let name = format!("fz_udt_e_{}", g.udt.next_enum);
    g.udt.next_enum += 1;
    // Non-wrapping window of three base labels plus two spare labels for
    // ADD VALUE, all distinct (offset in 0..=3 keeps offset+4 < 8).
    let off = g.rng.below_usize(4);
    let base = [ENUM_LABELS[off], ENUM_LABELS[off + 1], ENUM_LABELS[off + 2]];
    let add_before = ENUM_LABELS[off + 3];
    let add_after = ENUM_LABELS[off + 4];
    let renamed = format!("{}_r", base[1]);

    let mut out = vec![StmtKind::Raw(format!(
        "CREATE TYPE {} AS ENUM ({}, {}, {});",
        name,
        lit(base[0]),
        lit(base[1]),
        lit(base[2])
    ))];

    // Comparison / ordering operators over the enum.
    g.fire("udt:enum:cmp");
    let op = *g.rng.pick(&["<", "<=", "=", ">=", ">", "<>"]);
    out.push(StmtKind::Raw(format!(
        "SELECT {}::{} {} {}::{};",
        lit(base[0]),
        name,
        op,
        lit(base[2]),
        name
    )));
    // Ordinal built-ins (deterministic array/scalar text).
    g.fire("udt:enum:range");
    out.push(StmtKind::Raw(format!(
        "SELECT enum_range(NULL::{}), enum_first(NULL::{}), enum_last(NULL::{});",
        name, name, name
    )));

    // Table with an enum column, total-ordered probe (enum sort order is the
    // core surface; the pk tiebreak makes the order total even on ties).
    let t = format!("fz_udt_t_{}", g.udt.next_table);
    g.udt.next_table += 1;
    g.fire("udt:enum:table");
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, v {});",
        t, name
    )));
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (1, {}), (2, {}), (3, {});",
        t,
        lit(base[1]),
        lit(base[2]),
        lit(base[0])
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT pk, v FROM {} ORDER BY v, pk;",
        t
    )));
    // enum array + unnest(enum_range) ordered.
    g.fire("udt:enum:array");
    out.push(StmtKind::Raw(format!(
        "SELECT ARRAY[{}, {}]::{}[];",
        lit(base[0]),
        lit(base[2]),
        name
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT x FROM unnest(enum_range(NULL::{})) AS x ORDER BY x;",
        name
    )));

    // ALTER TYPE ADD VALUE BEFORE/AFTER (each commits standalone; safe to
    // use immediately after).
    g.fire("udt:enum:addval");
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} ADD VALUE {} BEFORE {};",
        name,
        lit(add_before),
        lit(base[2])
    )));
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} ADD VALUE {} AFTER {};",
        name,
        lit(add_after),
        lit(base[0])
    )));
    // RENAME VALUE (old exists, new does not — guaranteed by the "_r" suffix).
    g.fire("udt:enum:rename");
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} RENAME VALUE {} TO {};",
        name,
        lit(base[1]),
        lit(&renamed)
    )));
    // Re-probe the (now larger, reordered) range for the post-ALTER order.
    out.push(StmtKind::Raw(format!(
        "SELECT enum_range(NULL::{});",
        name
    )));

    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));
    out.push(StmtKind::Raw(format!("DROP TYPE {} CASCADE;", name)));
    out
}

// ---------------------------------------------------------------------------
// COMPOSITE
// ---------------------------------------------------------------------------

/// Composite-value labels (paren/comma/quote-free so the (a,b) text I/O form
/// is unambiguous).
const COMP_LABELS: &[&str] = &["alpha", "beta", "gamma", "delta", "hello", "world"];

/// CREATE TYPE AS (...) + ROW() field access, composite I/O ::text round
/// trip, composite comparison, composite in a table (total-ordered) and in
/// arrays, ALTER TYPE ADD/ALTER/DROP ATTRIBUTE — then drop.
fn gen_composite_group(g: &mut Gen) -> Vec<StmtKind> {
    let name = format!("fz_udt_c_{}", g.udt.next_comp);
    g.udt.next_comp += 1;
    let s1 = *g.rng.pick(COMP_LABELS);
    let s2 = *g.rng.pick(COMP_LABELS);

    let mut out = vec![StmtKind::Raw(format!(
        "CREATE TYPE {} AS (a int4, b text);",
        name
    ))];

    // ROW() construction + field access.
    g.fire("udt:comp:row");
    out.push(StmtKind::Raw(format!(
        "SELECT (ROW(1, {})::{}).a, (ROW(1, {})::{}).b;",
        lit(s1), name, lit(s1), name
    )));
    // Text I/O round trip (input parser -> output formatter).
    g.fire("udt:comp:io");
    out.push(StmtKind::Raw(format!(
        "SELECT '(1,{})'::{}::text;",
        s1, name
    )));
    // Composite comparison (record_cmp, field-by-field).
    g.fire("udt:comp:cmp");
    let op = *g.rng.pick(&["<", "<=", "=", ">=", ">", "<>"]);
    out.push(StmtKind::Raw(format!(
        "SELECT ROW(1, {})::{} {} ROW(2, {})::{};",
        lit(s1), name, op, lit(s2), name
    )));
    // Composite array.
    g.fire("udt:comp:array");
    out.push(StmtKind::Raw(format!(
        "SELECT ARRAY[ROW(1, {})::{}, ROW(2, {})::{}];",
        lit(s1), name, lit(s2), name
    )));

    // Composite column in a table, total-ordered probe + field projection.
    let t = format!("fz_udt_t_{}", g.udt.next_table);
    g.udt.next_table += 1;
    g.fire("udt:comp:table");
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, v {});",
        t, name
    )));
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (1, ROW(2, {})), (2, ROW(1, {})), (3, ROW(2, {}));",
        t,
        lit(s2),
        lit(s1),
        lit(s1)
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT pk, (v).a, (v).b FROM {} ORDER BY v, pk;",
        t
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));

    // ALTER TYPE ATTRIBUTE surgery (table dropped -> no cascade needed).
    g.fire("udt:comp:attr");
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} ADD ATTRIBUTE c numeric;",
        name
    )));
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} ALTER ATTRIBUTE b TYPE varchar(32);",
        name
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT (ROW(1, {}, 2.5)::{}).c;",
        lit(s1), name
    )));
    out.push(StmtKind::Raw(format!(
        "ALTER TYPE {} DROP ATTRIBUTE c;",
        name
    )));

    out.push(StmtKind::Raw(format!("DROP TYPE {} CASCADE;", name)));
    out
}

// ---------------------------------------------------------------------------
// DOMAIN
// ---------------------------------------------------------------------------

/// Per-base-type domain fuel: (base type, CHECK1 expr, CHECK2 expr, accepted
/// literal, CHECK1-violating literal, CHECK2-violating literal). Every
/// accept/reject decision is fixed so the constraint-decision identity is
/// deterministic.
struct DomFuel {
    base: &'static str,
    check1: &'static str,
    check2: &'static str,
    accept: &'static str,
    reject_check: &'static str,
    reject_check2: &'static str,
}

fn domain_fuel(g: &mut Gen) -> DomFuel {
    match g.weights.pick(g.rng, &["udt:dom:int", "udt:dom:text", "udt:dom:num"]) {
        "udt:dom:int" => DomFuel {
            base: "int4",
            check1: "VALUE > 0",
            check2: "VALUE < 100",
            accept: "5",
            // Parenthesized: `::` binds tighter than unary minus, so a bare
            // `-1::dom` would cast 1 (which PASSES the check) then negate,
            // never exercising the reject decision.
            reject_check: "(-1)",
            reject_check2: "200",
        },
        "udt:dom:text" => DomFuel {
            base: "text",
            check1: "char_length(VALUE) >= 2",
            check2: "VALUE <> 'zzz'",
            accept: "'ab'",
            reject_check: "'a'",
            reject_check2: "'zzz'",
        },
        _ => DomFuel {
            base: "numeric",
            check1: "VALUE > 0.0",
            check2: "VALUE < 100.0",
            accept: "1.5",
            // Parenthesized (see the int4 note on `::` vs unary-minus).
            reject_check: "(-0.5)",
            reject_check2: "200.0",
        },
    }
}

/// CREATE DOMAIN with CHECK + NOT NULL, validation on cast/assignment/INSERT
/// (accept/reject decision identity + constraint error identity), ALTER
/// DOMAIN ADD/DROP/VALIDATE CONSTRAINT and SET/DROP NOT NULL/DEFAULT, domain
/// over domain, domain in array/composite — then drop.
fn gen_domain_group(g: &mut Gen) -> Vec<StmtKind> {
    let f = domain_fuel(g);
    let d = format!("fz_udt_d_{}", g.udt.next_domain);
    g.udt.next_domain += 1;

    let mut out = vec![StmtKind::Raw(format!(
        "CREATE DOMAIN {} AS {} NOT NULL CHECK ({});",
        d, f.base, f.check1
    ))];

    // Cast-time decision identity: accept, CHECK reject (23514), NULL reject
    // (23502).
    g.fire("udt:dom:cast");
    out.push(StmtKind::Raw(format!("SELECT {}::{};", f.accept, d)));
    out.push(StmtKind::Raw(format!("SELECT {}::{};", f.reject_check, d)));
    out.push(StmtKind::Raw(format!("SELECT NULL::{};", d)));

    // DEFAULT lifecycle.
    g.fire("udt:dom:default");
    out.push(StmtKind::Raw(format!(
        "ALTER DOMAIN {} SET DEFAULT {};",
        d, f.accept
    )));
    out.push(StmtKind::Raw(format!("ALTER DOMAIN {} DROP DEFAULT;", d)));

    // ADD CONSTRAINT ... NOT VALID + VALIDATE (no stored values yet -> the
    // validate is a clean success; still walks ATExecValidateConstraint /
    // validateDomainConstraint), then a decision probe for the new
    // constraint.
    g.fire("udt:dom:addcon");
    out.push(StmtKind::Raw(format!(
        "ALTER DOMAIN {} ADD CONSTRAINT k2 CHECK ({}) NOT VALID;",
        d, f.check2
    )));
    out.push(StmtKind::Raw(format!(
        "ALTER DOMAIN {} VALIDATE CONSTRAINT k2;",
        d
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT {}::{};",
        f.reject_check2, d
    )));

    // Assignment/INSERT decision identity through a table column.
    let t = format!("fz_udt_t_{}", g.udt.next_table);
    g.udt.next_table += 1;
    g.fire("udt:dom:table");
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, v {});",
        t, d
    )));
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (1, {}), (2, {});",
        t, f.accept, f.accept
    )));
    // Rejected INSERTs (each rolls back only itself).
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (3, {});",
        t, f.reject_check
    )));
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (4, NULL);",
        t
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT pk, v FROM {} ORDER BY v, pk;",
        t
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));

    // Constraint drop + NOT NULL lifecycle (table gone -> SET NOT NULL scans
    // no dependent nulls and succeeds).
    g.fire("udt:dom:notnull");
    out.push(StmtKind::Raw(format!(
        "ALTER DOMAIN {} DROP CONSTRAINT k2;",
        d
    )));
    out.push(StmtKind::Raw(format!("ALTER DOMAIN {} DROP NOT NULL;", d)));
    out.push(StmtKind::Raw(format!("SELECT NULL::{};", d)));
    out.push(StmtKind::Raw(format!("ALTER DOMAIN {} SET NOT NULL;", d)));

    // Domain over domain: inner + outer CHECK both decide.
    if g.weights.pick(g.rng, &["udt:dom:nest", "udt:dom:flat"]) == "udt:dom:nest" {
        g.fire("udt:dom:nest");
        let d2 = format!("fz_udt_d_{}", g.udt.next_domain);
        g.udt.next_domain += 1;
        out.push(StmtKind::Raw(format!(
            "CREATE DOMAIN {} AS {} CHECK ({});",
            d2, d, f.check2
        )));
        out.push(StmtKind::Raw(format!("SELECT {}::{};", f.accept, d2)));
        out.push(StmtKind::Raw(format!(
            "SELECT {}::{};",
            f.reject_check2, d2
        )));
        out.push(StmtKind::Raw(format!(
            "SELECT {}::{};",
            f.reject_check, d2
        )));
        out.push(StmtKind::Raw(format!("DROP DOMAIN {} CASCADE;", d2)));
    }

    // Domain in array (one element violates -> whole cast rejects) and in a
    // composite type.
    g.fire("udt:dom:array");
    out.push(StmtKind::Raw(format!(
        "SELECT ARRAY[{}, {}]::{}[];",
        f.accept, f.accept, d
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT ARRAY[{}, {}]::{}[];",
        f.accept, f.reject_check, d
    )));
    g.fire("udt:dom:composite");
    let c = format!("fz_udt_c_{}", g.udt.next_comp);
    g.udt.next_comp += 1;
    out.push(StmtKind::Raw(format!(
        "CREATE TYPE {} AS (x {}, y text);",
        c, d
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT ROW({}, 'ok')::{};",
        f.accept, c
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT ROW({}, 'ok')::{};",
        f.reject_check, c
    )));
    out.push(StmtKind::Raw(format!("DROP TYPE {} CASCADE;", c)));

    out.push(StmtKind::Raw(format!("DROP DOMAIN {} CASCADE;", d)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = UdtState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.udt, &mut state);
            let kinds = gen_udt_module(&mut g);
            std::mem::swap(&mut g.udt, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn udt_is_deterministic() {
        let (a, _) = gen_groups(31, 400, "");
        let (b, _) = gen_groups(31, 400, "");
        assert_eq!(a, b);
        let (c, _) = gen_groups(32, 400, "");
        assert_ne!(a, c);
    }

    #[test]
    fn udt_variety_and_shape() {
        let (groups, prods) = gen_groups(0x0D7, 900, "");
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let hay = all.join("\n");
        for frag in [
            "CREATE TYPE fz_udt_e_",
            "AS ENUM (",
            "enum_range(NULL::",
            "enum_first(NULL::",
            "ALTER TYPE fz_udt_e_",
            " ADD VALUE ",
            " BEFORE ",
            " AFTER ",
            " RENAME VALUE ",
            "CREATE TYPE fz_udt_c_",
            " AS (a int4, b text)",
            "ROW(1, ",
            "::text;",
            "ADD ATTRIBUTE c numeric",
            "ALTER ATTRIBUTE b TYPE varchar(32)",
            "DROP ATTRIBUTE c",
            "CREATE DOMAIN fz_udt_d_",
            "NOT NULL CHECK (",
            "SELECT NULL::fz_udt_d_",
            "SET DEFAULT ",
            "DROP DEFAULT;",
            "ADD CONSTRAINT k2 CHECK (",
            "NOT VALID;",
            "VALIDATE CONSTRAINT k2;",
            "DROP CONSTRAINT k2;",
            "DROP NOT NULL;",
            "SET NOT NULL;",
            "CREATE DOMAIN fz_udt_d_", // nested reuse (domain over domain)
            "DROP DOMAIN fz_udt_d_",
            "DROP TYPE fz_udt_e_",
            "DROP TYPE fz_udt_c_",
        ] {
            assert!(hay.contains(frag), "udt flavor {frag:?} never generated");
        }
        for p in [
            "udt:enum",
            "udt:composite",
            "udt:domain",
            "udt:enum:cmp",
            "udt:enum:range",
            "udt:enum:table",
            "udt:enum:array",
            "udt:enum:addval",
            "udt:enum:rename",
            "udt:comp:row",
            "udt:comp:io",
            "udt:comp:cmp",
            "udt:comp:array",
            "udt:comp:table",
            "udt:comp:attr",
            "udt:dom:cast",
            "udt:dom:default",
            "udt:dom:addcon",
            "udt:dom:table",
            "udt:dom:notnull",
            "udt:dom:nest",
            "udt:dom:array",
            "udt:dom:composite",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        // Every base type surfaces.
        for base in ["AS int4 NOT NULL", "AS text NOT NULL", "AS numeric NOT NULL"] {
            assert!(hay.contains(base), "domain base {base:?} never generated");
        }
    }

    /// Statement shape invariants (mirrors the registry-wide gate) plus group
    /// self-containment: every created object is dropped within its group.
    #[test]
    fn udt_groups_are_self_contained() {
        let (groups, _) = gen_groups(0x517, 600, "");
        for group in &groups {
            for sql in group {
                assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            let hay = group.join("\n");
            // Every CREATE has a matching DROP in-group (DROP ... CASCADE
            // counts as a DROP of that object kind).
            for (create, drop) in [
                ("CREATE TYPE fz_udt_e_", "DROP TYPE fz_udt_e_"),
                ("CREATE TYPE fz_udt_c_", "DROP TYPE fz_udt_c_"),
                ("CREATE DOMAIN ", "DROP DOMAIN "),
                ("CREATE TABLE ", "DROP TABLE "),
            ] {
                assert_eq!(
                    hay.matches(create).count(),
                    hay.matches(drop).count(),
                    "unbalanced {create}/{drop} in group:\n{hay}"
                );
            }
        }
    }

    /// Enum ADD-VALUE labels can never collide with the base labels (the
    /// non-wrapping window discipline): the CREATE and the two ADD VALUEs use
    /// five distinct labels.
    #[test]
    fn enum_add_labels_never_collide() {
        let (groups, _) = gen_groups(0xE17, 800, "");
        for group in &groups {
            let create = group.iter().find(|s| s.contains("AS ENUM ("));
            let Some(create) = create else { continue };
            // Extract the three base labels from the CREATE.
            let base: Vec<&str> = create
                .split('\'')
                .skip(1)
                .step_by(2)
                .take(3)
                .collect();
            for add in group.iter().filter(|s| s.contains(" ADD VALUE ")) {
                let lbl = add.split('\'').nth(1).unwrap();
                assert!(
                    !base.contains(&lbl),
                    "ADD VALUE label {lbl:?} collides with base {base:?}"
                );
            }
        }
    }
}
