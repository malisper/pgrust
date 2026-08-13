//! CASTCOERCE: the cast/coercion system drain — the parser coercion core
//! (backend/parser/parse_coerce.c, parse_target.c, parse_type.c), the
//! CREATE CAST / domain / type DDL commands (backend/commands/
//! {functioncmds,typecmds}.c, backend/catalog/pg_cast.c), and the cast
//! execution paths (length coercion / typmod-in functions, array_coerce,
//! domain_check_input).
//!
//! Every operand is a literal drawn from the deterministic pools below;
//! results are cast `::text` and compared byte-exact, error identities
//! compared by SQLSTATE, and — the load-bearing surface of this module —
//! the coercion DECISION itself is compared: whether a given explicit /
//! implicit / assignment cast SUCCEEDS is the same on both engines. A
//! divergence in what casts are *allowed* (one side accepts, the other
//! raises 42846/42804/22P02) is a HIGH finding, distinct from a value or
//! error-text difference.
//!
//! What the twelve productions target (weights in weights.rs, `castcoerce:`
//! prefixed; the differ classes noted are the intended comparison surface):
//!
//!   explicit    `::type` / CAST(x AS type) across the built-in numeric,
//!               boolean, string, network and datetime type graph; result
//!               ::text. Drives coerce_type / find_coercion_pathway /
//!               coerce_to_target_type over COERCION_EXPLICIT.
//!   typmod      length coercion / typmod application under EXPLICIT cast:
//!               varchar(n) silent truncation, char(n)/bpchar blank-pad,
//!               numeric(p,s) rounding + field overflow (22003), bit(n)/
//!               varbit(n), timestamp(p)/time(p) fractional rounding,
//!               interval field + (p) qualifiers. Drives the *typmodin
//!               parsers and the length-coercion cast functions.
//!   unify       select_common_type over CASE/COALESCE/GREATEST/LEAST/
//!               NULLIF/array constructor/UNION — plus deliberate
//!               unresolvable unifications (decision surface: 42804
//!               "could not determine ... type").
//!   unknown     `unknown`-literal resolution: bare quoted literals in
//!               contexts that force a type (operator, IN list, function
//!               arg, ORDER BY) — the coerce_type UNKNOWNOID arm.
//!   array       array element coercion + array_coerce_common: ARRAY[...]
//!               and '{...}'-literal casts across element types incl.
//!               per-element rounding, multidim, text[]<->varchar[].
//!   bincoerce   binary-coercible (WITHOUT-FUNCTION) pathways:
//!               varchar<->text, cidr->inet, int4<->oid family,
//!               IsBinaryCoercibleWithCast / hide_coercion_node.
//!   failed      failed-cast error identity: 22P02 invalid input syntax,
//!               22003 out of range, 42846 cannot cast type A to type B,
//!               22P02 invalid input value for enum — deliberate fuel that
//!               probes the *rejection* half of the decision surface.
//!   assign      assignment coercion via a self-contained TEMP table:
//!               INSERT/UPDATE of a differently-typed literal into a typed
//!               column — the isExplicit=false length-coercion arm
//!               (varchar over-length ERROR vs trailing-space truncation,
//!               numeric assignment rounding + overflow, numeric->int
//!               assignment rounding). transformAssignedExpr surface.
//!   domain      CREATE DOMAIN (CHECK / NOT NULL / DEFAULT, base with
//!               typmod), casts to/from the domain, constraint recheck on
//!               cast (23514), ALTER DOMAIN ADD CONSTRAINT — DefineDomain /
//!               AlterDomainAddConstraint / domain_check_input.
//!   enum        CREATE TYPE AS ENUM, text<->enum casts, out-of-set input
//!               (22P02), ordering + enum_range.
//!   createcast  CREATE CAST WITH FUNCTION / WITHOUT FUNCTION, AS
//!               ASSIGNMENT / AS IMPLICIT, then exercise the freshly
//!               created cast — CreateCast / CastCreate.
//!   composite   composite/record coercion: ROW()::comptype and record
//!               text input, per-field coercion incl. unknown + rounding —
//!               coerce_record_to_complex.
//!
//! Determinism: literal-only, no now()/random()/nextval; every DDL group is
//! self-contained (leading DROP ... IF EXISTS, CREATE, exercise, trailing
//! DROP) with a fixed `cc_`-prefixed name, so groups never collide with each
//! other or with sibling modules and leave no residue. Same seed + toggles
//! => byte-identical stream.

use crate::stmt::{Gen, StmtKind};

/// Scalar literal pool spanning the type graph, spelled as bare quoted
/// literals (unknown-typed until a cast pins them) plus a few numeric bare
/// literals. Each entry pairs a source SQL fragment with a set of target
/// types that the explicit-cast matrix will draw from.
const NUM_LITS: &[&str] = &[
    "0", "1", "-1", "127", "-128", "32767", "-32768", "2147483647",
    "-2147483648", "9223372036854775807", "-9223372036854775808",
    "42", "255", "65536", "100000",
];

/// Numeric literals with a fractional part (rounding / truncation fuel).
const FRAC_LITS: &[&str] = &[
    "0.5", "-0.5", "1.5", "2.5", "-2.5", "123.456", "0.001", "9999.999",
    "1.0000005", "-0.0000005", "3.14159265358979", "0.15", "2.675",
];

/// String-shaped bare literals (unknown-typed) that are valid input for
/// several target types, plus deliberately-invalid ones (used only by the
/// `failed` production).
const STR_OK: &[&str] = &[
    "'123'", "'-45'", "'0'", "'  17  '", "'3.5'", "'1e3'", "'t'", "'f'",
    "'true'", "'yes'", "'on'", "'0'", "'1'",
];

/// Built-in scalar target types for the explicit-cast matrix. Kept to
/// types whose ::text output is locale- and float-digit-independent given
/// the runner's session pin (no money/lc_monetary; float uses shortest
/// round-trip which is bit-deterministic for an exact cast value).
const SCALAR_TYPES: &[&str] = &[
    "int2", "int4", "int8", "float4", "float8", "numeric", "bool", "text",
    "varchar", "bpchar", "oid",
];

/// Types reachable from a numeric source (excludes string-only spellings so
/// the matrix stays mostly-legal; the illegal corners are the `failed`
/// production's job).
const NUM_TARGETS: &[&str] =
    &["int2", "int4", "int8", "float4", "float8", "numeric", "bool", "oid", "text", "money"];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_castcoerce_module(g: &mut Gen) -> Vec<StmtKind> {
    let pick = g.weights.pick(
        g.rng,
        &[
            "castcoerce:explicit",
            "castcoerce:typmod",
            "castcoerce:unify",
            "castcoerce:unknown",
            "castcoerce:array",
            "castcoerce:bincoerce",
            "castcoerce:failed",
            "castcoerce:assign",
            "castcoerce:domain",
            "castcoerce:enum",
            "castcoerce:createcast",
            "castcoerce:composite",
        ],
    );
    match pick {
        "castcoerce:explicit" => vec![one(gen_explicit(g))],
        "castcoerce:typmod" => vec![one(gen_typmod(g))],
        "castcoerce:unify" => vec![one(gen_unify(g))],
        "castcoerce:unknown" => vec![one(gen_unknown(g))],
        "castcoerce:array" => vec![one(gen_array(g))],
        "castcoerce:bincoerce" => vec![one(gen_bincoerce(g))],
        "castcoerce:failed" => vec![one(gen_failed(g))],
        "castcoerce:assign" => gen_assign_group(g),
        "castcoerce:domain" => gen_domain_group(g),
        "castcoerce:enum" => gen_enum_group(g),
        "castcoerce:createcast" => gen_createcast_group(g),
        _ => gen_composite_group(g),
    }
}

/// Wrap a scalar cast expression as a single `SELECT (expr)::text;` so every
/// scalar production compares byte-exact and its parens stay balanced.
fn one(expr: String) -> StmtKind {
    StmtKind::Raw(format!("SELECT ({})::text;", expr))
}

// ---------------------------------------------------------------------------
// explicit: `::type` / CAST(x AS type) across the type graph.
// ---------------------------------------------------------------------------

fn gen_explicit(g: &mut Gen) -> String {
    g.fire("castcoerce:explicit");
    // Source: bare numeric, fractional numeric, or a quoted string literal.
    let (src, targets): (String, &[&str]) = match g.rng.below(3) {
        0 => (g.rng.pick(NUM_LITS).to_string(), NUM_TARGETS),
        1 => (g.rng.pick(FRAC_LITS).to_string(), NUM_TARGETS),
        _ => (g.rng.pick(STR_OK).to_string(), SCALAR_TYPES),
    };
    let to = *g.rng.pick(targets);
    // Two spellings of the same explicit cast (postfix `::` vs CAST()), plus
    // a double cast (source -> intermediate -> final) to walk two pathway
    // resolutions in one statement.
    match g.rng.below(3) {
        0 => format!("({}) :: {}", src, to),
        1 => format!("CAST(({}) AS {})", src, to),
        _ => {
            let mid = *g.rng.pick(SCALAR_TYPES);
            format!("CAST(CAST(({}) AS {}) AS {})", src, mid, to)
        }
    }
}

// ---------------------------------------------------------------------------
// typmod: length coercion / typmod application under an EXPLICIT cast.
// ---------------------------------------------------------------------------

fn gen_typmod(g: &mut Gen) -> String {
    g.fire("castcoerce:typmod");
    match g.rng.below(8) {
        // varchar(n): explicit cast truncates silently (isExplicit=true).
        0 => {
            let s = *g.rng.pick(&["'abcdef'", "'ab'", "'  x  '", "'héllo'", "''", "'ABCDE'"]);
            let n = 1 + g.rng.below(6);
            format!("({}) :: varchar({})", s, n)
        }
        // char(n)/bpchar: blank-padded to n; over-length explicit truncates.
        1 => {
            let s = *g.rng.pick(&["'ab'", "'abcdef'", "'x '", "'  '", "'Z'"]);
            let n = 1 + g.rng.below(6);
            let ty = if g.rng.chance(1, 2) { "char" } else { "bpchar" };
            format!("({}) :: {}({})", s, ty, n)
        }
        // numeric(p,s): rounding at the scale boundary + field overflow fuel.
        2 => {
            let v = *g.rng.pick(&[
                "123.456", "0.5", "2.5", "-2.5", "9.995", "0.001", "999.9",
                "12345.678", "0.15", "2.675", "-0.005",
            ]);
            let p = 1 + g.rng.below(6);
            let s = g.rng.below(p + 1);
            format!("({}) :: numeric({},{})", v, p, s)
        }
        // numeric(p) — no scale (scale defaults 0, rounds to integer).
        3 => {
            let v = *g.rng.pick(&["123.456", "0.5", "-0.5", "2.5", "9999.99"]);
            let p = 1 + g.rng.below(6);
            format!("({}) :: numeric({})", v, p)
        }
        // bit(n)/varbit(n): pad/truncate + length-mismatch fuel.
        4 => {
            let s = *g.rng.pick(&["'1'", "'101'", "'0000'", "'11110000'", "'1010101'"]);
            let n = 1 + g.rng.below(8);
            let ty = if g.rng.chance(1, 2) { "bit" } else { "varbit" };
            format!("({}) :: {}({})", s, ty, n)
        }
        // timestamp(p) / time(p): fractional-second precision rounding.
        5 => {
            let (ty, v) = if g.rng.chance(1, 2) {
                ("timestamp", "'2001-02-03 04:05:06.987654'")
            } else {
                ("time", "'04:05:06.987654'")
            };
            let p = g.rng.below(7);
            format!("({}) :: {}({})", v, ty, p)
        }
        // interval field qualifiers + (p).
        6 => {
            let v = *g.rng.pick(&[
                "'1 year 2 months 3 days 04:05:06.7891'",
                "'1.5 days'",
                "'26:03:04.7654'",
                "'100 months'",
            ]);
            let q = *g.rng.pick(&[
                "year", "month", "day", "hour", "minute", "second",
                "year to month", "day to hour", "day to second", "hour to minute",
                "hour to second", "minute to second", "second(2)", "day to second(3)",
            ]);
            format!("(interval {} {}) :: text", v, q)
        }
        // typmod removal: cast a typmod'd value back to the unqualified type
        // (hide_coercion_node / the no-op relabel path).
        _ => {
            let s = *g.rng.pick(&["'abcdef'", "'12'", "'x'"]);
            let n = 1 + g.rng.below(5);
            format!("(({}) :: varchar({})) :: varchar", s, n)
        }
    }
}

// ---------------------------------------------------------------------------
// unify: select_common_type over the polymorphic constructs.
// ---------------------------------------------------------------------------

fn gen_unify(g: &mut Gen) -> String {
    g.fire("castcoerce:unify");
    // Deterministic operand pairs across the numeric/string boundary; the
    // last two are deliberately unresolvable (42804 decision fuel).
    let good: &[(&str, &str)] = &[
        ("1::int4", "2.5::numeric"),
        ("1::int2", "2::int8"),
        ("1::float4", "2::float8"),
        ("1::int4", "2::float8"),
        ("'a'::text", "'b'::varchar"),
        ("1::numeric", "2::int4"),
        ("'2001-01-01'::date", "'2001-01-02 03:04'::timestamp"),
        ("1::int4", "NULL"),
    ];
    let bad: &[(&str, &str)] = &[
        ("1::int4", "'abc'::bytea"),
        ("'x'::point", "1::int4"),
        ("'a'::json", "'b'::text"),
    ];
    let unresolvable = g.rng.chance(1, 5);
    let (a, b) = if unresolvable {
        *g.rng.pick(bad)
    } else {
        *g.rng.pick(good)
    };
    match g.rng.below(6) {
        0 => format!("CASE WHEN true THEN {} ELSE {} END", a, b),
        1 => format!("COALESCE({}, {})", a, b),
        2 => format!("GREATEST({}, {})", a, b),
        3 => format!("LEAST({}, {})", a, b),
        4 => format!("NULLIF({}, {})", a, b),
        // UNION unify, ordered so the row set + order is deterministic.
        _ => format!(
            "(SELECT c::text FROM (SELECT {} AS c UNION ALL SELECT {}) s ORDER BY 1)",
            a, b
        ),
    }
}

// ---------------------------------------------------------------------------
// unknown: `unknown`-literal resolution in a type-forcing context.
// ---------------------------------------------------------------------------

fn gen_unknown(g: &mut Gen) -> String {
    g.fire("castcoerce:unknown");
    match g.rng.below(7) {
        // Bare literal -> text (the default resolution).
        0 => format!("{}", g.rng.pick(&["'hello'", "'42'", "' '", "''"])),
        // Operator forces the unknown to the other operand's type.
        1 => {
            let n = *g.rng.pick(&["1", "2", "100"]);
            let op = *g.rng.pick(&["+", "-", "*"]);
            format!("'{}' {} {}", g.rng.below(50), op, n)
        }
        // Comparison against a typed value.
        2 => format!("('5' = {}::int4)", g.rng.pick(&["5", "6"])),
        // IN list: unknown resolved from the list's common type.
        3 => "('2' IN (1, 2, 3))".to_string(),
        // Function argument coercion (length forces text).
        4 => format!("length({})", g.rng.pick(&["'abcde'", "'  x'", "''"])),
        // CASE with all-unknown results resolves to text.
        5 => "CASE WHEN true THEN 'a' ELSE 'bb' END".to_string(),
        // Concatenation: unknown || typed.
        _ => format!("('x' || {}::int4)", g.rng.pick(&["7", "8"])),
    }
}

// ---------------------------------------------------------------------------
// array: element coercion + array_coerce.
// ---------------------------------------------------------------------------

fn gen_array(g: &mut Gen) -> String {
    g.fire("castcoerce:array");
    match g.rng.below(7) {
        // ARRAY constructor element unification then cast.
        0 => "ARRAY[1, 2, 3]::numeric[]".to_string(),
        // Fractional elements rounded per element by the element cast.
        1 => "ARRAY[1.4, 2.5, 3.6]::int4[]".to_string(),
        // String-literal array input then element cast.
        2 => format!("('{{1,2,3}}')::int4[]::{}", g.rng.pick(&["text[]", "numeric[]", "float8[]"])),
        // text[] <-> varchar[] (binary-coercible element).
        3 => "ARRAY['a','b','c']::varchar[]::text[]".to_string(),
        // Mixed unknown + typed element unification.
        4 => "ARRAY['1', 2, 3.5]::numeric[]".to_string(),
        // Multidimensional array cast.
        5 => "ARRAY[ARRAY[1,2],ARRAY[3,4]]::int8[]".to_string(),
        // Element cast that overflows on one element (decision/error fuel).
        _ => format!("(ARRAY[1, {}, 3])::int2[]", g.rng.pick(&["2", "40000"])),
    }
}

// ---------------------------------------------------------------------------
// bincoerce: WITHOUT-FUNCTION binary-coercible pathways.
// ---------------------------------------------------------------------------

fn gen_bincoerce(g: &mut Gen) -> String {
    g.fire("castcoerce:bincoerce");
    let s = match g.rng.below(6) {
        // varchar <-> text (binary coercible both directions).
        0 => "('abc'::varchar)::text".to_string(),
        1 => "('abc'::text)::varchar".to_string(),
        // cidr -> inet (binary coercible), inet -> cidr (with function).
        2 => format!("('{}'::cidr)::inet", g.rng.pick(&["10.0.0.0/8", "192.168.1.0/24", "::/0"])),
        3 => format!("('{}'::inet)::cidr", g.rng.pick(&["10.1.2.3/8", "192.168.1.5/24"])),
        // int4 <-> oid (binary coercible), oid -> regtype (io).
        4 => format!("({}::int4)::oid::int4", g.rng.pick(&["16", "2200", "0"])),
        // xml <-> text (with function); pg_lsn text round-trip.
        _ => format!("('{}'::pg_lsn)::text", g.rng.pick(&["0/0", "16/B374D848", "FFFFFFFF/FFFFFFFF"])),
    };
    s
}

// ---------------------------------------------------------------------------
// failed: deliberate rejection fuel — the decision surface's negative half.
// ---------------------------------------------------------------------------

fn gen_failed(g: &mut Gen) -> String {
    g.fire("castcoerce:failed");
    match g.rng.below(8) {
        // 22P02 invalid input syntax for integer.
        0 => format!("({})::int4", g.rng.pick(&["'abc'", "''", "'1.5'", "'true'", "'0x10'"])),
        // 22P02 invalid input for numeric.
        1 => format!("({})::numeric", g.rng.pick(&["'abc'", "'1,000'", "''", "'NaNx'"])),
        // 22003 out of range (integer overflow on narrowing cast).
        2 => format!("({})::int2", g.rng.pick(&["100000", "-40000", "32768"])),
        // 22003 numeric field overflow (typmod).
        3 => format!("({})::numeric(3,1)", g.rng.pick(&["12345.6", "999.99", "-1000"])),
        // 42846 cannot cast type A to type B (no pathway).
        4 => {
            let (a, b) = *g.rng.pick(&[
                ("1::int4", "point"),
                ("'x'::text", "point"),
                ("'{1,2}'::int4[]", "int4"),
                ("1::int4", "bytea"),
                ("true::bool", "bytea"),
            ]);
            format!("({})::{}", a, b)
        }
        // 22P02 invalid input syntax for type boolean.
        5 => format!("({})::bool", g.rng.pick(&["'maybe'", "'2'", "''", "'yesno'"])),
        // 22007 invalid datetime input.
        6 => format!("({})::date", g.rng.pick(&["'2001-13-01'", "'not-a-date'", "'2001-02-30'"])),
        // 22001 value too long is assignment-only; here a bit-length mismatch
        // (22026 string data length mismatch) via exact bit typmod.
        _ => format!("('{}')::bit(4)", g.rng.pick(&["101", "1010101"])),
    }
}

// ---------------------------------------------------------------------------
// assign: assignment coercion via a self-contained TEMP table (INSERT/UPDATE
// of a differently-typed literal into a typed column — isExplicit=false).
// ---------------------------------------------------------------------------

fn gen_assign_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("castcoerce:assign");
    let mut out = Vec::new();
    let mut push = |s: String| out.push(StmtKind::Raw(s));
    push("DROP TABLE IF EXISTS cc_asg;".to_string());
    push(
        "CREATE TEMP TABLE cc_asg (id int4, v varchar(3), c char(3), n numeric(5,2), i int4);"
            .to_string(),
    );
    // Row 1: all assignments legal (trailing-space truncation for v/c, scale
    // rounding for n, numeric->int rounding for i).
    push(
        "INSERT INTO cc_asg VALUES (1, 'ab   ', 'x  ', 123.456, 2.7);".to_string(),
    );
    // Row 2: over-length varchar with non-space excess -> 22001 (assignment
    // truncation is an ERROR, unlike the explicit-cast silent truncation).
    // The row is rejected identically on both sides.
    push("INSERT INTO cc_asg VALUES (2, 'abcdef', 'y', 1, 1);".to_string());
    // Row 3: numeric assignment overflow -> 22003.
    push("INSERT INTO cc_asg VALUES (3, 'a', 'b', 12345.6, 1);".to_string());
    // Row 4: unknown/text literal assigned to int column (assignment cast).
    push("INSERT INTO cc_asg VALUES (4, 'c', 'd', 1, '5');".to_string());
    // Row 5: float literal assigned to numeric(5,2) column (rounds).
    push("INSERT INTO cc_asg VALUES (5, 'e', 'f', 0.005, 9);".to_string());
    // UPDATE assignment coercion: bump i by a fractional numeric (rounds).
    push("UPDATE cc_asg SET i = i + 0.6 WHERE id = 1;".to_string());
    // State probe: pk-ordered, all columns ::text — the surviving rows must
    // match byte-for-byte and the SAME rows must survive on both engines.
    push(
        "SELECT id, v::text, c::text, n::text, i::text FROM cc_asg ORDER BY id;".to_string(),
    );
    push("DROP TABLE IF EXISTS cc_asg;".to_string());
    out
}

// ---------------------------------------------------------------------------
// domain: CREATE DOMAIN + constraint recheck on cast.
// ---------------------------------------------------------------------------

fn gen_domain_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("castcoerce:domain");
    let mut out = Vec::new();
    let mut push = |s: String| out.push(StmtKind::Raw(s));
    push("DROP DOMAIN IF EXISTS cc_dom CASCADE;".to_string());
    match g.rng.below(3) {
        // int domain with CHECK + NOT NULL.
        0 => {
            push(
                "CREATE DOMAIN cc_dom AS int4 NOT NULL CHECK (VALUE > 0 AND VALUE < 100);"
                    .to_string(),
            );
            push("SELECT (5::cc_dom)::text;".to_string());
            // Constraint violation on cast -> 23514.
            push("SELECT (0::cc_dom)::text;".to_string());
            push("SELECT (200::cc_dom)::text;".to_string());
            // Via text input path (domain_check_input over io cast).
            push("SELECT (('37')::cc_dom)::text;".to_string());
            // NULL against NOT NULL domain -> 23502.
            push("SELECT (CAST(NULL AS cc_dom))::text;".to_string());
        }
        // varchar(n) domain: base typmod + CHECK.
        1 => {
            push(
                "CREATE DOMAIN cc_dom AS varchar(4) CHECK (VALUE <> '');".to_string(),
            );
            push("SELECT ('abc'::cc_dom)::text;".to_string());
            // Base-type over-length assignment via cast (explicit truncates? domain
            // applies the base typmod as assignment -> 22001 on non-space excess).
            push("SELECT ('abcdef'::cc_dom)::text;".to_string());
            push("SELECT (''::cc_dom)::text;".to_string());
        }
        // numeric domain with ALTER DOMAIN ADD CONSTRAINT (recheck path).
        _ => {
            push("CREATE DOMAIN cc_dom AS numeric(6,2);".to_string());
            push("SELECT (12.345::cc_dom)::text;".to_string());
            push(
                "ALTER DOMAIN cc_dom ADD CONSTRAINT cc_dom_pos CHECK (VALUE >= 0);"
                    .to_string(),
            );
            push("SELECT (7.5::cc_dom)::text;".to_string());
            push("SELECT ((-1)::cc_dom)::text;".to_string());
        }
    }
    push("DROP DOMAIN IF EXISTS cc_dom CASCADE;".to_string());
    out
}

// ---------------------------------------------------------------------------
// enum: CREATE TYPE AS ENUM + text<->enum casts.
// ---------------------------------------------------------------------------

fn gen_enum_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("castcoerce:enum");
    let mut out = Vec::new();
    let mut push = |s: String| out.push(StmtKind::Raw(s));
    push("DROP TYPE IF EXISTS cc_enum CASCADE;".to_string());
    push("CREATE TYPE cc_enum AS ENUM ('low', 'mid', 'high');".to_string());
    // text -> enum (io cast).
    push("SELECT ('mid'::cc_enum)::text;".to_string());
    // enum -> text.
    push("SELECT (('high')::cc_enum)::text;".to_string());
    // Out-of-set input -> 22P02 invalid input value for enum.
    push("SELECT ('none'::cc_enum)::text;".to_string());
    // Ordering (enum comparison uses declaration order, not text order).
    push("SELECT ('low'::cc_enum < 'high'::cc_enum)::text;".to_string());
    // enum_range roundtrip.
    push("SELECT (enum_range(NULL::cc_enum))::text;".to_string());
    push("DROP TYPE IF EXISTS cc_enum CASCADE;".to_string());
    out
}

// ---------------------------------------------------------------------------
// createcast: CREATE CAST then exercise it.
// ---------------------------------------------------------------------------

fn gen_createcast_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("castcoerce:createcast");
    let mut out = Vec::new();
    let mut push = |s: String| out.push(StmtKind::Raw(s));
    match g.rng.below(3) {
        // WITH FUNCTION, AS ASSIGNMENT: a fresh int4 -> bytea cast (no such
        // built-in cast exists, so CreateCast/CastCreate run clean).
        0 => {
            push("DROP CAST IF EXISTS (int4 AS bytea);".to_string());
            push("DROP FUNCTION IF EXISTS cc_i2b(int4);".to_string());
            push(
                "CREATE FUNCTION cc_i2b(int4) RETURNS bytea AS $$ SELECT int4send($1) $$ LANGUAGE sql IMMUTABLE;"
                    .to_string(),
            );
            let mode = *g.rng.pick(&["", " AS ASSIGNMENT", " AS IMPLICIT"]);
            push(format!(
                "CREATE CAST (int4 AS bytea) WITH FUNCTION cc_i2b(int4){};",
                mode
            ));
            // Exercise the new cast explicitly.
            push("SELECT (16909060::int4::bytea)::text;".to_string());
            push("DROP CAST IF EXISTS (int4 AS bytea);".to_string());
            push("DROP FUNCTION IF EXISTS cc_i2b(int4);".to_string());
        }
        // WITHOUT FUNCTION (binary coercible) via a domain over oid: int4 and
        // oid share representation, so the cast is binary-coercible.
        1 => {
            push("DROP CAST IF EXISTS (int4 AS cc_oiddom);".to_string());
            push("DROP DOMAIN IF EXISTS cc_oiddom CASCADE;".to_string());
            push("CREATE DOMAIN cc_oiddom AS oid;".to_string());
            push(
                "CREATE CAST (int4 AS cc_oiddom) WITHOUT FUNCTION AS ASSIGNMENT;".to_string(),
            );
            push("SELECT (2200::int4::cc_oiddom)::text;".to_string());
            push("DROP CAST IF EXISTS (int4 AS cc_oiddom);".to_string());
            push("DROP DOMAIN IF EXISTS cc_oiddom CASCADE;".to_string());
        }
        // WITH INOUT (io cast) between two domains over unrelated base types.
        _ => {
            push("DROP CAST IF EXISTS (cc_srcdom AS cc_dstdom);".to_string());
            push("DROP DOMAIN IF EXISTS cc_srcdom CASCADE;".to_string());
            push("DROP DOMAIN IF EXISTS cc_dstdom CASCADE;".to_string());
            push("CREATE DOMAIN cc_srcdom AS int4;".to_string());
            push("CREATE DOMAIN cc_dstdom AS text;".to_string());
            push(
                "CREATE CAST (cc_srcdom AS cc_dstdom) WITH INOUT AS IMPLICIT;".to_string(),
            );
            push("SELECT (42::cc_srcdom::cc_dstdom)::text;".to_string());
            push("DROP CAST IF EXISTS (cc_srcdom AS cc_dstdom);".to_string());
            push("DROP DOMAIN IF EXISTS cc_srcdom CASCADE;".to_string());
            push("DROP DOMAIN IF EXISTS cc_dstdom CASCADE;".to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// composite: ROW()::comptype + record text input — coerce_record_to_complex.
// ---------------------------------------------------------------------------

fn gen_composite_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("castcoerce:composite");
    let mut out = Vec::new();
    let mut push = |s: String| out.push(StmtKind::Raw(s));
    push("DROP TYPE IF EXISTS cc_comp CASCADE;".to_string());
    push("CREATE TYPE cc_comp AS (a int4, b numeric(6,2), c varchar(3));".to_string());
    // Direct field types.
    push("SELECT (ROW(1, 2.5, 'ab')::cc_comp)::text;".to_string());
    // Per-field coercion: unknown 'x' -> where legal, int->text, float round.
    push("SELECT (ROW('7', '3.005', 'yz')::cc_comp)::text;".to_string());
    // Field coercion that rounds the numeric and truncates the varchar (as
    // assignment — over-length non-space -> 22001 decision surface).
    push("SELECT (ROW(9, 1.999, 'abcd')::cc_comp)::text;".to_string());
    // record text input via the composite io cast.
    push("SELECT (('(3,4.25,zz)')::cc_comp)::text;".to_string());
    // A field value that fails its type (22P02 in a nested field io).
    push("SELECT (ROW('notint', 1, 'a')::cc_comp)::text;".to_string());
    push("DROP TYPE IF EXISTS cc_comp CASCADE;".to_string());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every castcoerce statement + the determinism
    /// law (no now()/random()/nextval), and that every production fires
    /// under defaults within a reasonable budget.
    #[test]
    fn castcoerce_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xCACE);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_castcoerce_module(&mut g);
            assert!(!stmts.is_empty(), "empty group");
            for stmt in &stmts {
                let sql = stmt.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                let low = sql.to_ascii_lowercase();
                assert!(!low.contains("now("), "nondeterministic now(): {sql}");
                assert!(!low.contains("random("), "nondeterministic random(): {sql}");
                assert!(!low.contains("nextval"), "nondeterministic nextval: {sql}");
                assert!(!low.contains("current_"), "nondeterministic current_*: {sql}");
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        for p in [
            "castcoerce:explicit", "castcoerce:typmod", "castcoerce:unify",
            "castcoerce:unknown", "castcoerce:array", "castcoerce:bincoerce",
            "castcoerce:failed", "castcoerce:assign", "castcoerce:domain",
            "castcoerce:enum", "castcoerce:createcast", "castcoerce:composite",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Every DDL group is self-contained: it leads with a DROP (idempotent
    /// cleanup), ends with a DROP (no residue), and drops at least as many
    /// objects as it creates — so groups never collide with each other or
    /// with sibling modules and leave nothing behind.
    #[test]
    fn ddl_groups_are_self_contained() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xD00D);
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_castcoerce_module(&mut g);
            let sqls: Vec<String> =
                stmts.iter().map(|s| s.to_sql().to_ascii_lowercase()).collect();
            let creates = sqls.iter().filter(|s| s.starts_with("create ")).count();
            if creates == 0 {
                continue; // scalar single-SELECT production
            }
            let drops = sqls.iter().filter(|s| s.starts_with("drop ")).count();
            let first = &sqls[0];
            let last = sqls.last().unwrap();
            assert!(
                first.starts_with("drop "),
                "DDL group must lead with DROP: {first}"
            );
            assert!(
                last.starts_with("drop "),
                "DDL group must end with DROP: {last}"
            );
            assert!(
                drops >= creates,
                "group creates {creates} objects but only drops {drops}:\n{}",
                sqls.join("\n")
            );
        }
    }

    /// Same seed -> byte-identical stream (the reproducibility law).
    #[test]
    fn castcoerce_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(0x5EED);
            let mut out = Vec::new();
            for _ in 0..800 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                for s in gen_castcoerce_module(&mut g) {
                    out.push(s.to_sql());
                }
            }
            out
        };
        assert_eq!(run(), run());
    }
}
