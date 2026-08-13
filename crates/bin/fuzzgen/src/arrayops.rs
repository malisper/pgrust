//! Array operator/function/subscripting drain module (Track-B). Targets the
//! SQL-drainable surface of arrayfuncs.c, array_userfuncs.c, arrayutils.c and
//! the expanded-array path (array_expanded): element/slice subscripting
//! (`a[i]`, `a[i:j]`, omitted bounds, out-of-range), subscript assignment in
//! UPDATE, multidimensional arrays and their introspection
//! (array_dims/ndims/lower/upper/length, cardinality), the array operators
//! (`@>`, `<@`, `&&`, `||`, `=`/`<>`/`<`/`<=`/`>`/`>=`), `ANY`/`ALL`, and the
//! array function family (append/prepend/cat/position/positions/remove/
//! replace/fill/to_string/string_to_array/array_agg/unnest [+ WITH
//! ORDINALITY]/trim_array/array_sample). `array_recv` (binary wire input) is
//! fuzzed separately; this module drives the operator/function surface.
//!
//! Comparison law: array results are cast `::text` and compared exactly (the
//! text output of arrays is deterministic across engines). Multi-row shapes
//! (unnest) compare as multisets unless they carry a total `ORDER BY`.
//! Errors are part of the surface and are emitted deliberately — subscript
//! errors, non-rectangular multidim constructors (matching-dimension errors),
//! malformed array literals, trim/sample out-of-range — matched on SQLSTATE by
//! diff::classify, never annotated here (the differential judges pgrust
//! against C). `array_sample` draws from the engine PRNG, so its output is
//! never compared directly: only deterministic invariants (result cardinality
//! and `<@` containment in the source) and the matched error arms are emitted
//! (numx:rand / xnum:rand precedent).
//!
//! Stateless: every probe is a self-contained one-statement group except the
//! subscript-assignment UPDATE family, which creates-and-drops an `fz_arr`
//! fixture in-group (earm discipline).

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

// ----------------------------------------------------------- literal pools --

/// One-dimensional int arrays spanning the empty / singleton / NULL-element /
/// negative / custom-lower-bound regimes (custom bounds exercise arrayutils.c
/// ArrayGetOffset / mda_* index math and the lower-bound-aware operators).
const A1: &[&str] = &[
    "'{}'::int[]",
    "'{1}'::int[]",
    "'{1,2,3}'::int[]",
    "'{5,4,3,2,1}'::int[]",
    "'{1,2,3,2,1}'::int[]",
    "'{1,NULL,3}'::int[]",
    "'{NULL,NULL}'::int[]",
    "'{-2,-1,0,1,2}'::int[]",
    "ARRAY[10,20,30,40]",
    "'[2:4]={7,8,9}'::int[]",
    "'[-1:1]={100,200,300}'::int[]",
    "'[0:2]={0,0,0}'::int[]",
];

/// One-dimensional text arrays (quoting, embedded blanks, NULL vs quoted
/// "NULL", empty).
const T1: &[&str] = &[
    "'{}'::text[]",
    "'{a}'::text[]",
    "'{a,b,c}'::text[]",
    "'{foo,bar,baz}'::text[]",
    "'{a,NULL,c}'::text[]",
    "'{a,\"NULL\",c}'::text[]",
    "'{\" a \",b}'::text[]",
    "ARRAY['x','y','z']",
];

/// Multidimensional int arrays (rectangular), incl. 3-D and custom bounds.
const MD: &[&str] = &[
    "'{{1,2},{3,4}}'::int[]",
    "'{{1,2,3},{4,5,6}}'::int[]",
    "'{{{1,2},{3,4}},{{5,6},{7,8}}}'::int[]",
    "ARRAY[ARRAY[1,2],ARRAY[3,4]]",
    "'[0:1][0:1]={{9,8},{7,6}}'::int[]",
    "'[1:2][1:3]={{1,2,3},{4,5,6}}'::int[]",
];

/// Scalar int operands for ANY/ALL/containment/position/remove.
const IE: &[&str] = &["1", "2", "3", "0", "-1", "5", "10", "99", "NULL"];

/// Scalar text operands.
const TE: &[&str] = &["'a'", "'b'", "'z'", "'foo'", "''", "NULL"];

/// A 1-D int array literal (never the NULL-only / empty ones when the caller
/// needs at least one element — callers add those deliberately).
fn a1(g: &mut Gen) -> &'static str {
    pick_str(g, A1)
}

const SHAPES: &[&str] = &[
    "arr:sub",
    "arr:dims",
    "arr:op",
    "arr:anyall",
    "arr:fn",
    "arr:str",
    "arr:agg",
    "arr:trim",
    "arr:ctor",
    "arr:update",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_arrayops_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arrayops");
    match g.weights.pick(g.rng, SHAPES) {
        "arr:sub" => gen_sub(g),
        "arr:dims" => gen_dims(g),
        "arr:op" => gen_op(g),
        "arr:anyall" => gen_anyall(g),
        "arr:fn" => gen_fn(g),
        "arr:str" => gen_str(g),
        "arr:agg" => gen_agg(g),
        "arr:trim" => gen_trim(g),
        "arr:ctor" => gen_ctor(g),
        _ => gen_update(g),
    }
}

// ------------------------------------------------------------- subscript ----

/// Element / slice subscripting: `a[i]`, `a[i:j]`, omitted bounds (`a[:j]`,
/// `a[i:]`, `a[:]`), out-of-range (element → NULL; slice → clamped/empty),
/// multidim `a[i][j]` and `a[i:j][k:l]`, and custom-lower-bound access.
fn gen_sub(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:sub");
    let shape = g
        .weights
        .pick(g.rng, &["arr:sub:elem", "arr:sub:slice", "arr:sub:md", "arr:sub:oob"]);
    g.fire(shape);
    let sql = match shape {
        "arr:sub:elem" => {
            let a = a1(g);
            let i = pick_str(g, &["1", "2", "3", "0", "-1", "5"]);
            format!("SELECT (({a})[{i}])::text;")
        }
        "arr:sub:slice" => {
            let a = a1(g);
            let lo = pick_str(g, &["1", "2", "", "0"]);
            let hi = pick_str(g, &["2", "3", "", "5"]);
            format!("SELECT (({a})[{lo}:{hi}])::text;")
        }
        "arr:sub:md" => {
            let a = pick_str(g, MD);
            if g.rng.chance(1, 2) {
                let i = pick_str(g, &["1", "2", "0"]);
                let j = pick_str(g, &["1", "2", "3", "0"]);
                format!("SELECT (({a})[{i}][{j}])::text;")
            } else {
                format!("SELECT (({a})[1:2][1:2])::text;")
            }
        }
        // Out-of-range element (→ NULL) and empty slices; custom-bound access.
        _ => {
            let a = pick_str(g, &[
                "'{1,2,3}'::int[]",
                "'[2:4]={7,8,9}'::int[]",
                "'[-1:1]={100,200,300}'::int[]",
            ]);
            format!(
                "SELECT (({a})[100])::text, (({a})[-5])::text, (({a})[3:2])::text;"
            )
        }
    };
    vec![raw(sql)]
}

// -------------------------------------------------------------- dims/intro --

/// Dimension introspection: array_dims / array_ndims / array_lower /
/// array_upper / array_length / cardinality across empty / 1-D / multidim /
/// custom-bound inputs (empty-array edges: length→NULL, dims→NULL,
/// cardinality→0).
fn gen_dims(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:dims");
    let a = pick_str(g, &[
        "'{}'::int[]",
        "'{1,2,3}'::int[]",
        "'{{1,2,3},{4,5,6}}'::int[]",
        "'{{{1,2},{3,4}},{{5,6},{7,8}}}'::int[]",
        "'[2:4]={7,8,9}'::int[]",
        "'[1:2][1:3]={{1,2,3},{4,5,6}}'::int[]",
    ]);
    let d = pick_str(g, &["1", "2", "3"]);
    let sql = format!(
        "SELECT array_dims({a}), array_ndims({a}), \
         array_lower({a}, {d}), array_upper({a}, {d}), \
         array_length({a}, {d}), cardinality({a});"
    );
    vec![raw(sql)]
}

// -------------------------------------------------------------- operators ---

/// Binary array operators: containment (`@>`, `<@`, `&&`) with NULL-element
/// edges (NULLs are never "contained"/"overlapping"), concatenation (`||`,
/// element-to-array both sides and array-to-array incl. multidim), and the
/// btree comparison family (`=`/`<>`/`<`/`<=`/`>`/`>=`, element-wise then
/// length).
fn gen_op(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:op");
    let shape = g
        .weights
        .pick(g.rng, &["arr:op:contain", "arr:op:concat", "arr:op:cmp"]);
    g.fire(shape);
    let sql = match shape {
        "arr:op:contain" => {
            let a = a1(g);
            let b = a1(g);
            format!(
                "SELECT ({a} @> {b}), ({a} <@ {b}), ({a} && {b});"
            )
        }
        "arr:op:concat" => {
            let a = a1(g);
            let e = pick_str(g, &["0", "42", "NULL"]);
            if g.rng.chance(1, 3) {
                // multidim concat (dimension-compatible and incompatible arms)
                let m = pick_str(g, MD);
                format!("SELECT ({m} || {m})::text, ({a} || {a})::text;")
            } else {
                format!(
                    "SELECT ({a} || {a})::text, ({e} || {a})::text, ({a} || {e})::text;"
                )
            }
        }
        _ => {
            let a = a1(g);
            let b = a1(g);
            format!(
                "SELECT ({a} = {b}), ({a} <> {b}), ({a} < {b}), ({a} <= {b}), \
                 ({a} > {b}), ({a} >= {b});"
            )
        }
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- any/all ---

/// Quantified comparison: `x = ANY(arr)`, `x <> ALL(arr)`, `x op ANY/ALL`,
/// with the NULL-element three-valued-logic edges (`x = ANY('{1,NULL}')` is
/// NULL when x∉{1}).
fn gen_anyall(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:anyall");
    let text = g.rng.chance(1, 3);
    let (x, a) = if text {
        (pick_str(g, TE).to_string(), pick_str(g, T1).to_string())
    } else {
        (pick_str(g, IE).to_string(), a1(g).to_string())
    };
    let op = pick_str(g, &["=", "<>", "<", ">", "<=", ">="]);
    let sql = format!(
        "SELECT ({x} {op} ANY({a})), ({x} {op} ALL({a})), ({x} = ANY({a})), \
         ({x} <> ALL({a}));"
    );
    vec![raw(sql)]
}

// -------------------------------------------------------------- functions ---

/// Array function family: append / prepend / cat / position / positions /
/// remove / replace / fill (incl. custom lower bounds and the matched error
/// arms: negative/oversized fill dimensions).
fn gen_fn(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:fn");
    let shape = g.weights.pick(
        g.rng,
        &["arr:fn:mutate", "arr:fn:cat", "arr:fn:find", "arr:fn:replace", "arr:fn:fill"],
    );
    g.fire(shape);
    let sql = match shape {
        "arr:fn:mutate" => {
            let a = a1(g);
            let e = pick_str(g, &["0", "9", "NULL"]);
            format!(
                "SELECT array_append({a}, {e})::text, array_prepend({e}, {a})::text;"
            )
        }
        "arr:fn:cat" => {
            let a = a1(g);
            let b = a1(g);
            format!("SELECT array_cat({a}, {b})::text;")
        }
        "arr:fn:find" => {
            let a = a1(g);
            let e = pick_str(g, IE);
            format!(
                "SELECT array_position({a}, {e}), array_positions({a}, {e})::text;"
            )
        }
        "arr:fn:replace" => {
            let a = a1(g);
            let from = pick_str(g, IE);
            let to = pick_str(g, IE);
            format!(
                "SELECT array_remove({a}, {from})::text, \
                 array_replace({a}, {from}, {to})::text;"
            )
        }
        // array_fill: dims + optional lower bounds, plus error arms.
        _ => {
            let e = pick_str(g, &["7", "0", "NULL"]);
            let sub = g.weights.pick(g.rng, &["arr:fn:fill:ok", "arr:fn:fill:err"]);
            g.fire(sub);
            if sub == "arr:fn:fill:err" {
                pick_str(g, &[
                    "SELECT array_fill(1, ARRAY[-1])::text;",
                    "SELECT array_fill(1, ARRAY[2,3], ARRAY[1])::text;",
                    "SELECT array_fill(1, ARRAY[2], ARRAY[1,1])::text;",
                ])
                .to_string()
            } else {
                let dims = pick_str(g, &["ARRAY[3]", "ARRAY[2,3]", "ARRAY[0]"]);
                if g.rng.chance(1, 2) {
                    let lb = pick_str(g, &["ARRAY[2]", "ARRAY[-1]", "ARRAY[1,1]"]);
                    format!("SELECT array_fill({e}, {dims}, {lb})::text;")
                } else {
                    format!("SELECT array_fill({e}, {dims})::text;")
                }
            }
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------- string bridge --

/// array_to_string / string_to_array with the NULL-handling matrix (NULLs
/// dropped unless a null-string is supplied; empty and NULL separators).
fn gen_str(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:str");
    let sql = if g.rng.chance(1, 2) {
        let a = pick_str(g, &[
            "'{a,b,c}'::text[]",
            "'{a,NULL,c}'::text[]",
            "'{1,2,3}'::int[]",
            "'{}'::text[]",
        ]);
        let sep = pick_str(g, &["','", "'-'", "''", "NULL"]);
        if g.rng.chance(1, 2) {
            let ns = pick_str(g, &["'*'", "''", "NULL"]);
            format!("SELECT array_to_string({a}, {sep}, {ns});")
        } else {
            format!("SELECT array_to_string({a}, {sep});")
        }
    } else {
        let s = pick_str(g, &["'a,b,c'", "'a,,c'", "'abc'", "''", "'x-y-z'", "NULL"]);
        let sep = pick_str(g, &["','", "'-'", "''", "NULL"]);
        if g.rng.chance(1, 2) {
            let ns = pick_str(g, &["'b'", "''", "NULL"]);
            format!("SELECT string_to_array({s}, {sep}, {ns})::text;")
        } else {
            format!("SELECT string_to_array({s}, {sep})::text;")
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------- agg / unnest ---

/// array_agg (deterministic via inner ORDER BY), unnest (single + parallel
/// multi-array), and unnest WITH ORDINALITY (total ORDER BY → ordered
/// compare).
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:agg");
    let shape = g
        .weights
        .pick(g.rng, &["arr:agg:agg", "arr:agg:unnest", "arr:agg:ord"]);
    g.fire(shape);
    let sql = match shape {
        "arr:agg:agg" => {
            let src = pick_str(g, &[
                "VALUES (3),(1),(2),(1),(NULL)",
                "VALUES (10),(20),(30)",
                "SELECT * FROM generate_series(1,5) g",
            ]);
            format!(
                "SELECT array_agg(x ORDER BY x)::text FROM ({src}) v(x);"
            )
        }
        "arr:agg:unnest" => {
            let a = a1(g);
            if g.rng.chance(1, 2) {
                let b = pick_str(g, &["'{10,20}'::int[]", "'{7,8,9}'::int[]", "'{}'::int[]"]);
                // parallel unnest: shorter array padded with NULLs
                format!("SELECT * FROM unnest({a}, {b}) AS u(a, b) ORDER BY a, b;")
            } else {
                format!("SELECT u::text FROM unnest({a}) AS u ORDER BY u;")
            }
        }
        _ => {
            let a = a1(g);
            format!(
                "SELECT x::text, n FROM unnest({a}) WITH ORDINALITY AS t(x, n) \
                 ORDER BY n, x;"
            )
        }
    };
    vec![raw(sql)]
}

// -------------------------------------------------------------- trim/sample -

/// trim_array (incl. out-of-range error arms) and array_sample (PG17,
/// PRNG-nondeterministic → only deterministic invariants + matched error
/// arms are emitted).
fn gen_trim(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:trim");
    let shape = g
        .weights
        .pick(g.rng, &["arr:trim:trim", "arr:trim:sample"]);
    g.fire(shape);
    let sql = match shape {
        "arr:trim:trim" => {
            let a = pick_str(g, &["ARRAY[1,2,3,4,5]", "'{a,b,c}'::text[]", "'{}'::int[]"]);
            let n = pick_str(g, &["0", "1", "2", "5", "-1", "10"]);
            format!("SELECT trim_array({a}, {n})::text;")
        }
        // array_sample: cardinality + <@ containment are deterministic
        // regardless of which elements are drawn; the error arms (n<0, n>len)
        // are deterministic SQLSTATE.
        _ => {
            let a = "ARRAY[1,2,3,4,5,6,7,8]";
            let sub = g.weights.pick(g.rng, &["arr:trim:sample:ok", "arr:trim:sample:err"]);
            g.fire(sub);
            if sub == "arr:trim:sample:err" {
                pick_str(g, &[
                    "SELECT cardinality(array_sample(ARRAY[1,2,3], -1));",
                    "SELECT cardinality(array_sample(ARRAY[1,2,3], 9));",
                ])
                .to_string()
            } else {
                let n = pick_str(g, &["0", "1", "3", "8"]);
                format!(
                    "SELECT cardinality(array_sample({a}, {n})) = {n}, \
                     (array_sample({a}, {n}) <@ {a});"
                )
            }
        }
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------- constructors ----

/// ARRAY[] constructor edges: empty typed constructor, nested constructors,
/// constructor from subquery, and the deliberate error arms (non-rectangular
/// multidimensional constructor / malformed array literal).
fn gen_ctor(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:ctor");
    let shape = g.weights.pick(g.rng, &["arr:ctor:ok", "arr:ctor:err"]);
    g.fire(shape);
    let sql = if shape == "arr:ctor:err" {
        pick_str(g, &[
            "SELECT (ARRAY[ARRAY[1,2],ARRAY[3,4,5]])::text;",
            "SELECT ('{{1,2},{3}}'::int[])::text;",
            "SELECT ('{1,2,'::int[])::text;",
            "SELECT (ARRAY[ARRAY[1],ARRAY[2,3]])::text;",
        ])
        .to_string()
    } else {
        pick_str(g, &[
            "SELECT (ARRAY[]::int[])::text;",
            "SELECT (ARRAY[1,2,3])::text, (ARRAY['a','b'])::text;",
            "SELECT (ARRAY[ARRAY[1,2],ARRAY[3,4]])::text;",
            "SELECT (ARRAY(SELECT g FROM generate_series(1,4) g))::text;",
            "SELECT (ARRAY(SELECT g FROM generate_series(1,3) g ORDER BY g DESC))::text;",
        ])
        .to_string()
    };
    vec![raw(sql)]
}

// --------------------------------------------------------- subscript UPDATE -

/// Subscript-assignment surface (array_set_element / array_set_slice via the
/// executor): element assignment, extend-beyond-bounds (grows the array,
/// NULL-filling gaps), slice assignment, and assignment into an empty array
/// (produces a custom-lower-bound array). Creates and drops an `fz_arr`
/// fixture in-group (earm discipline); the trailing ordered SELECT is the
/// compared surface.
fn gen_update(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("arr:update");
    let t = "fz_arr";
    let i = pick_str(g, &["1", "2", "3", "5", "0"]);
    let v = pick_str(g, &["99", "-7", "NULL"]);
    let lo = pick_str(g, &["1", "2"]);
    let hi = pick_str(g, &["2", "3", "4"]);
    let rhs = pick_str(g, &["'{-1,-2}'", "'{100,200,300}'", "'{}'"]);
    vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, a int[]);")),
        raw(format!(
            "INSERT INTO {t} VALUES (1,'{{1,2,3}}'), (2,'{{10,20,30,40}}'), \
             (3,'{{}}'), (4,'{{5,NULL,7}}');"
        )),
        raw(format!("UPDATE {t} SET a[{i}] = {v} WHERE id = 1;")),
        raw(format!("UPDATE {t} SET a[5] = 50 WHERE id = 2;")),
        raw(format!("UPDATE {t} SET a[{lo}:{hi}] = {rhs} WHERE id = 4;")),
        raw(format!("UPDATE {t} SET a[3] = 7 WHERE id = 3;")),
        raw(format!("SELECT id, a::text FROM {t} ORDER BY id;")),
        raw(format!("DROP TABLE {t};")),
    ]
}
