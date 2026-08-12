//! Geometric-types module (A1): the full geo operator/function surface of
//! backend/utils/adt/geo_ops.c — the #1 uncovered adt file in
//! gap-report-005 (243 gap functions). Literal pools for all seven geo
//! types (point, line, lseg, box, path open+closed, polygon, circle) with
//! structured coordinates (integers, halves, negatives, zeros, degenerate
//! and collinear shapes), a data-driven operator matrix enumerating C's
//! actual pg_operator rows for the geo type family, the user-facing
//! function set, the legal cast pairs, and a small deterministic data
//! table (idx-module pattern) so predicates, projections, KNN ordering and
//! GiST polygon/circle indexes run over rows.
//!
//! Pre-build verification (2026-08-11, scratchpad handprobe): the
//! pg_operator / pg_proc / pg_cast geo surfaces are IDENTICAL on pgrust
//! and C PostgreSQL 18 (catalog dumps diffed clean), and a 575-line probe
//! applying every operator row over three literals per type plus the
//! function/cast/table/KNN/GiST-index forms produced byte-identical output
//! on both engines — including NaN/Infinity coordinate specials — with the
//! only errors (10, identical both sides) being division by zero-coordinate
//! points, area(polygon)/center(polygon) (not user-callable names), and
//! open-path-to-polygon casts. The generator therefore: draws `/` divisor
//! coordinates from a nonzero pool, never calls area/center on polygons
//! (poly_center is the callable name), and casts only closed paths to
//! polygon.
//!
//! Float discipline: geo scalar results are float8-based and compare
//! byte-identical when pgrust ports C's arithmetic exactly — that is the
//! differential point. float8 result columns ride the differ's standard
//! ulp comparator (default tolerance 4); a stable ulp-level divergence
//! would surface as elevated ulp-tolerant matches or findings and is
//! banked as an arithmetic-parity finding, never softened here.
//!
//! Design points (mirroring crate::idx):
//!   - `GeoState` is session-persistent (swapped in and out of `Gen` like
//!     `IdxState`); tables are NOT registered into the shared catalog
//!     (geo column types are outside the generic type system); pk-ordered
//!     state probes ride on `DdlEvent`s.
//!   - Data is one INSERT..SELECT over generate_series with pure integer
//!     formulas (byte-identical both sides by construction, hand-verified).
//!   - Every row-returning query ORDER BY pk (total order, strict ordered
//!     compare); KNN orders by distance then pk so distance ties are never
//!     tie-order noise.
//!   - Scalar/cast productions need no table: the operator matrix runs
//!     over literals in single-row SELECTs, so the module contributes
//!     coverage even before its first create group.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// Live-table population cap (create groups are heavy: table + bulk load +
/// 2 GiST indexes + ANALYZE).
const MAX_LIVE_TABLES: usize = 2;

const COLS_SQL: &str = "pk int4 PRIMARY KEY, pt point, ln line, sg lseg, bx box, \
                        pa path, pgn polygon, cr circle";

/// Geo type families; indexes into the per-family column-name table.
const FAMS: &[&str] = &["point", "line", "lseg", "box", "path", "polygon", "circle"];
const COLS: &[&str] = &["pt", "ln", "sg", "bx", "pa", "pgn", "cr"];

fn col_of(fam: &str) -> &'static str {
    COLS[FAMS.iter().position(|f| *f == fam).unwrap()]
}

// -------------------------------------------------------- operator matrix ----
//
// Enumerated from pg_operator on BOTH engines (dumps diffed identical);
// each entry is (operator, left family, right family). Empty left = prefix
// operator. Only rows verified clean in the hand probe are listed.

/// Boolean-returning binary operators: predicate + scalar fuel.
const BOOL_OPS: &[(&str, &str, &str)] = &[
    ("&&", "box", "box"), ("&&", "circle", "circle"), ("&&", "polygon", "polygon"),
    ("&<", "box", "box"), ("&<", "circle", "circle"), ("&<", "polygon", "polygon"),
    ("&>", "box", "box"), ("&>", "circle", "circle"), ("&>", "polygon", "polygon"),
    ("&<|", "box", "box"), ("&<|", "circle", "circle"), ("&<|", "polygon", "polygon"),
    ("|&>", "box", "box"), ("|&>", "circle", "circle"), ("|&>", "polygon", "polygon"),
    ("<<", "box", "box"), ("<<", "circle", "circle"), ("<<", "point", "point"),
    ("<<", "polygon", "polygon"),
    (">>", "box", "box"), (">>", "circle", "circle"), (">>", "point", "point"),
    (">>", "polygon", "polygon"),
    ("<<|", "box", "box"), ("<<|", "circle", "circle"), ("<<|", "point", "point"),
    ("<<|", "polygon", "polygon"),
    ("|>>", "box", "box"), ("|>>", "circle", "circle"), ("|>>", "point", "point"),
    ("|>>", "polygon", "polygon"),
    ("<^", "box", "box"), ("<^", "point", "point"),
    (">^", "box", "box"), (">^", "point", "point"),
    ("<", "box", "box"), ("<", "circle", "circle"), ("<", "lseg", "lseg"), ("<", "path", "path"),
    ("<=", "box", "box"), ("<=", "circle", "circle"), ("<=", "lseg", "lseg"), ("<=", "path", "path"),
    (">", "box", "box"), (">", "circle", "circle"), (">", "lseg", "lseg"), (">", "path", "path"),
    (">=", "box", "box"), (">=", "circle", "circle"), (">=", "lseg", "lseg"), (">=", "path", "path"),
    ("=", "box", "box"), ("=", "circle", "circle"), ("=", "line", "line"),
    ("=", "lseg", "lseg"), ("=", "path", "path"),
    ("<>", "circle", "circle"), ("<>", "lseg", "lseg"), ("<>", "point", "point"),
    ("~=", "box", "box"), ("~=", "circle", "circle"), ("~=", "point", "point"),
    ("~=", "polygon", "polygon"),
    ("<@", "box", "box"), ("<@", "circle", "circle"), ("<@", "lseg", "box"),
    ("<@", "lseg", "line"), ("<@", "point", "box"), ("<@", "point", "circle"),
    ("<@", "point", "line"), ("<@", "point", "lseg"), ("<@", "point", "path"),
    ("<@", "point", "polygon"), ("<@", "polygon", "polygon"),
    ("@>", "box", "box"), ("@>", "box", "point"), ("@>", "circle", "circle"),
    ("@>", "circle", "point"), ("@>", "path", "point"), ("@>", "polygon", "point"),
    ("@>", "polygon", "polygon"),
    ("?#", "box", "box"), ("?#", "line", "box"), ("?#", "line", "line"),
    ("?#", "lseg", "box"), ("?#", "lseg", "line"), ("?#", "lseg", "lseg"),
    ("?#", "path", "path"),
    ("?-", "point", "point"), ("?|", "point", "point"),
    ("?-|", "line", "line"), ("?-|", "lseg", "lseg"),
    ("?||", "line", "line"), ("?||", "lseg", "lseg"),
];

/// `<->` distance pairs (all float8).
const DIST_OPS: &[(&str, &str)] = &[
    ("box", "box"), ("box", "lseg"), ("box", "point"),
    ("circle", "circle"), ("circle", "point"), ("circle", "polygon"),
    ("line", "line"), ("line", "lseg"), ("line", "point"),
    ("lseg", "box"), ("lseg", "line"), ("lseg", "lseg"), ("lseg", "point"),
    ("path", "path"), ("path", "point"),
    ("point", "box"), ("point", "circle"), ("point", "line"), ("point", "lseg"),
    ("point", "path"), ("point", "point"), ("point", "polygon"),
    ("polygon", "circle"), ("polygon", "point"), ("polygon", "polygon"),
];

/// Translate/scale arithmetic: (op, left family); right side is a point
/// (`/` divisors draw nonzero coordinates — verified error surface).
const ARITH_OPS: &[(&str, &str)] = &[
    ("+", "box"), ("+", "circle"), ("+", "path"), ("+", "point"),
    ("-", "box"), ("-", "circle"), ("-", "path"), ("-", "point"),
    ("*", "box"), ("*", "circle"), ("*", "path"), ("*", "point"),
    ("/", "box"), ("/", "circle"), ("/", "path"), ("/", "point"),
];

/// `##` closest-point pairs (result point).
const CLOSEST_OPS: &[(&str, &str)] = &[
    ("line", "lseg"), ("lseg", "box"), ("lseg", "lseg"),
    ("point", "box"), ("point", "line"), ("point", "lseg"),
];

/// `#` intersection pairs: box#box -> box, line#line -> point (NULL when
/// parallel), lseg#lseg -> point.
const INTER_OPS: &[(&str, &str)] = &[("box", "box"), ("line", "line"), ("lseg", "lseg")];

/// Prefix operators: (op, operand family). `@@` -> center point,
/// `@-@` -> length, `#` -> npoints, `?-`/`?|` -> is-horizontal/vertical.
const PREFIX_OPS: &[(&str, &str)] = &[
    ("@@", "box"), ("@@", "circle"), ("@@", "lseg"), ("@@", "polygon"),
    ("@-@", "lseg"), ("@-@", "path"),
    ("#", "path"), ("#", "polygon"),
    ("?-", "line"), ("?-", "lseg"),
    ("?|", "line"), ("?|", "lseg"),
];

/// User-facing single-argument functions (name, arg family). All verified
/// callable + byte-identical both engines.
const FUNCS1: &[(&str, &str)] = &[
    ("area", "box"), ("area", "path"), ("area", "circle"),
    ("box_center", "box"), ("center", "circle"), ("poly_center", "polygon"),
    ("diagonal", "box"), ("diameter", "circle"), ("radius", "circle"),
    ("height", "box"), ("width", "box"),
    ("isclosed", "path"), ("isopen", "path"),
    ("length", "lseg"), ("length", "path"),
    ("npoints", "path"), ("npoints", "polygon"),
    ("pclose", "path"), ("popen", "path"),
    ("lseg_center", "lseg"),
    ("point", "box"), ("point", "circle"), ("point", "polygon"), ("point", "lseg"),
    ("polygon", "box"), ("polygon", "circle"),
    ("circle", "box"), ("box", "circle"), ("box", "polygon"), ("box", "point"),
    ("lseg", "box"), ("path", "polygon"), ("circle", "polygon"),
];

/// Two-argument functions (name, fam1, fam2).
const FUNCS2: &[(&str, &str, &str)] = &[
    ("bound_box", "box", "box"),
    ("isparallel", "line", "line"), ("isparallel", "lseg", "lseg"),
    ("isperp", "line", "line"), ("isperp", "lseg", "lseg"),
    ("ishorizontal", "point", "point"), ("isvertical", "point", "point"),
    ("slope", "point", "point"),
    ("line_interpt", "line", "line"), ("lseg_interpt", "lseg", "lseg"),
    ("line", "point", "point"), ("lseg", "point", "point"),
];

/// Legal cast pairs (source family, target family) from pg_cast, both
/// engines identical. path -> polygon only for closed paths (the literal
/// generator is asked for a closed one).
const CASTS: &[(&str, &str)] = &[
    ("box", "circle"), ("box", "lseg"), ("box", "point"), ("box", "polygon"),
    ("circle", "box"), ("circle", "point"), ("circle", "polygon"),
    ("lseg", "point"), ("path", "polygon"), ("point", "box"),
    ("polygon", "box"), ("polygon", "circle"), ("polygon", "path"), ("polygon", "point"),
];

// ------------------------------------------------------------------ state ----

#[derive(Clone, Debug)]
pub struct GeoTable {
    pub name: String,
    pub live: bool,
    pub next_pk: i64,
    /// GiST polygon/circle indexes (die only with the table).
    pub indexes: Vec<String>,
}

/// Session-persistent geo table model.
#[derive(Clone, Debug, Default)]
pub struct GeoState {
    pub tables: Vec<GeoTable>,
    next_table: u32,
    next_index: u32,
    events: Vec<DdlEvent>,
}

impl GeoState {
    pub fn new() -> GeoState {
        GeoState::default()
    }

    /// Drain pending create/drop events (the session loop resolves them
    /// into probe windows, exactly like IdxState's).
    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_tables(&self) -> Vec<usize> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_geo_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("geo");
    let action = g.weights.pick(
        g.rng,
        &[
            "geo:create",
            "geo:drop",
            "geo:scalar",
            "geo:cast",
            "geo:query",
            "geo:calc",
            "geo:knn",
            "geo:join",
            "geo:churn",
        ],
    );
    match action {
        "geo:create" => gen_create(g),
        "geo:drop" => gen_drop(g),
        "geo:scalar" => gen_scalar(g),
        "geo:cast" => gen_cast(g),
        "geo:query" => gen_query(g),
        "geo:calc" => gen_calc(g),
        "geo:knn" => gen_knn(g),
        "geo:join" => gen_join(g),
        _ => gen_churn(g),
    }
}

// --------------------------------------------------------------- literals ----

/// Structured coordinate: integers, halves, negatives, zero — sized to the
/// data-table coordinate ranges (roughly -11..12) so predicates over
/// columns actually select rows.
fn coord(g: &mut Gen) -> String {
    match g.rng.below(8) {
        0 => "0".to_string(),
        1 => "1".to_string(),
        2 => "-1".to_string(),
        3 => "0.5".to_string(),
        4 => "-2.5".to_string(),
        5 => format!("{}", g.rng.range_i64(-11, 12)),
        6 => format!("{}", g.rng.range_i64(-11, 12)),
        _ => format!("{}.5", g.rng.range_i64(-10, 10)),
    }
}

/// Nonzero coordinate (for `/` divisors — zero divides error, a verified
/// both-sides-error shape not worth generating).
fn coord_nz(g: &mut Gen) -> String {
    match g.rng.below(5) {
        0 => "1".to_string(),
        1 => "-1".to_string(),
        2 => "0.5".to_string(),
        3 => "2".to_string(),
        _ => format!("{}", 1 + g.rng.below(9)),
    }
}

/// Occasionally-special coordinate: NaN/Infinity/huge (hand-verified
/// byte-identical) under the low-weight geo:lit:nan production.
fn coord_special(g: &mut Gen) -> String {
    if g.weights.pick(g.rng, &["geo:lit:nan", "geo:lit:plain"]) == "geo:lit:nan" {
        g.fire("geo:lit:nan");
        match g.rng.below(4) {
            0 => "'NaN'".to_string(),
            1 => "'Infinity'".to_string(),
            2 => "'-Infinity'".to_string(),
            _ => "1e150".to_string(),
        }
    } else {
        coord(g)
    }
}

fn point_lit(g: &mut Gen) -> String {
    if g.rng.chance(1, 4) {
        // Text-input form (point_in).
        format!("'({},{})'::point", coord(g), coord(g))
    } else {
        format!("point({}, {})", coord_special(g), coord_special(g))
    }
}

/// Point with nonzero plain coordinates (divisors).
fn point_nz_lit(g: &mut Gen) -> String {
    format!("point({}, {})", coord_nz(g), coord_nz(g))
}

fn line_lit(g: &mut Gen) -> String {
    match g.rng.below(4) {
        // {A,B,C} form; A and B never both zero.
        0 => format!("'{{1, {}, {}}}'::line", coord(g), coord(g)),
        1 => format!("'{{0, -1, {}}}'::line", coord(g)),
        // Two-point form; second point offset so the points are distinct.
        2 => {
            let (x, y) = (coord(g), coord(g));
            format!("line(point({x}, {y}), point({x} + 1, {y} + {}))", coord(g))
        }
        // Vertical / horizontal specials.
        _ => {
            let c = coord(g);
            if g.rng.chance(1, 2) {
                format!("line(point({c}, 0), point({c}, 3))")
            } else {
                format!("line(point(0, {c}), point(4, {c}))")
            }
        }
    }
}

fn lseg_lit(g: &mut Gen) -> String {
    match g.rng.below(4) {
        // Degenerate: both endpoints equal.
        0 => {
            let (x, y) = (coord(g), coord(g));
            format!("lseg(point({x}, {y}), point({x}, {y}))")
        }
        // Horizontal / vertical.
        1 => {
            let (a, b, c) = (coord(g), coord(g), coord(g));
            if g.rng.chance(1, 2) {
                format!("lseg(point({a}, {c}), point({b}, {c}))")
            } else {
                format!("lseg(point({c}, {a}), point({c}, {b}))")
            }
        }
        // Text-input form (lseg_in).
        2 => format!("'[({},{}),({},{})]'::lseg", coord(g), coord(g), coord(g), coord(g)),
        _ => format!("lseg({}, {})", point_lit(g), point_lit(g)),
    }
}

fn box_lit(g: &mut Gen) -> String {
    match g.rng.below(4) {
        // Degenerate zero-area box.
        0 => {
            let (x, y) = (coord(g), coord(g));
            format!("box(point({x}, {y}), point({x}, {y}))")
        }
        // Text-input form (box_in).
        1 => format!("'(({},{}),({},{}))'::box", coord(g), coord(g), coord(g), coord(g)),
        _ => format!("box({}, {})", point_lit(g), point_lit(g)),
    }
}

fn path_lit(g: &mut Gen) -> String {
    // Text forms with explicit point tuples: '[...]' open, '(...)' closed.
    let n = 2 + g.rng.below(3);
    let pts: Vec<String> = (0..n).map(|_| format!("({},{})", coord(g), coord(g))).collect();
    if g.rng.chance(1, 2) {
        format!("'[{}]'::path", pts.join(","))
    } else {
        format!("'({})'::path", pts.join(","))
    }
}

fn closed_path_lit(g: &mut Gen) -> String {
    let n = 2 + g.rng.below(3);
    let pts: Vec<String> = (0..n).map(|_| format!("({},{})", coord(g), coord(g))).collect();
    format!("'({})'::path", pts.join(","))
}

fn polygon_lit(g: &mut Gen) -> String {
    match g.rng.below(4) {
        // Degenerate: collinear vertices.
        0 => "'((0,0),(1,1),(2,2))'::polygon".to_string(),
        // Concave quad (vertex order makes it non-convex).
        1 => {
            let s = 1 + g.rng.below(5);
            format!("'((0,0),({s},{s}),({z},0),({s},-{s}))'::polygon", z = 2 * s)
        }
        // Circle-derived n-gon (float-heavy vertices: parity fuel).
        2 => format!(
            "polygon({}, circle(point({}, {}), {}))",
            3 + g.rng.below(6),
            coord(g),
            coord(g),
            1 + g.rng.below(5)
        ),
        // Convex: from a box.
        _ => format!("polygon({})", box_lit(g)),
    }
}

fn circle_lit(g: &mut Gen) -> String {
    let r = match g.rng.below(5) {
        0 => "0".to_string(),
        1 => "0.5".to_string(),
        2 => "2.5".to_string(),
        3 => format!("{}", g.rng.below(12)),
        _ => "10".to_string(),
    };
    if g.rng.chance(1, 4) {
        // Text-input form (circle_in).
        format!("'<({},{}),{}>'::circle", coord(g), coord(g), r)
    } else {
        format!("circle(point({}, {}), {})", coord(g), coord(g), r)
    }
}

fn lit(g: &mut Gen, fam: &str) -> String {
    match fam {
        "point" => point_lit(g),
        "line" => line_lit(g),
        "lseg" => lseg_lit(g),
        "box" => box_lit(g),
        "path" => path_lit(g),
        "polygon" => polygon_lit(g),
        _ => circle_lit(g),
    }
}

// ------------------------------------------------------------- data table ----

/// Deterministic row source for pks lo..=hi: pure integer formulas of the
/// pk (hand-verified byte-identical on both engines, including the
/// open/closed path split and the polygon/circle GiST index builds).
fn row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point((i*17) % 21 - 10, (i*31) % 23 - 11), \
         line(point((i*3) % 7, (i*5) % 9), point((i*3) % 7 + 1 + (i % 4), (i*5) % 9 + (i % 5) - 2)), \
         lseg(point((i*7) % 13 - 6, (i*11) % 17 - 8), point((i*7) % 13 - 6 + (i % 5), (i*11) % 17 - 8 + (i % 3))), \
         box(point((i*7) % 19 - 9, (i*11) % 15 - 7), point((i*7) % 19 - 9 + 1 + (i % 6), (i*11) % 15 - 7 + 1 + (i % 4))), \
         CASE WHEN i % 2 = 0 THEN path(polygon(box(point((i*5) % 11, (i*3) % 13), point((i*5) % 11 + 2, (i*3) % 13 + 3)))) \
         ELSE popen(path(polygon(box(point((i*5) % 11, (i*3) % 13), point((i*5) % 11 + 2, (i*3) % 13 + 3))))) END, \
         polygon(box(point((i*13) % 17 - 8, (i*19) % 13 - 6), point((i*13) % 17 - 8 + 1 + (i % 5), (i*19) % 13 - 6 + 1 + (i % 7)))), \
         circle(point((i*23) % 21 - 10, (i*29) % 23 - 11), (i % 6)) \
         FROM generate_series({lo}, {hi}) i"
    )
}

// ----------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.geo.live_tables().len() >= MAX_LIVE_TABLES {
        g.fire("geo:cap:tables");
        return gen_drop(g);
    }
    g.fire("geo:create");
    let name = format!("fz_geo_{}", g.geo.next_table);
    g.geo.next_table += 1;
    let rows = match g.weights.pick(g.rng, &["geo:rows:120", "geo:rows:300", "geo:rows:700"]) {
        "geo:rows:120" => 120,
        "geo:rows:300" => 300,
        _ => 700,
    };
    let mut stmts = vec![
        StmtKind::Raw(format!("CREATE TABLE {} ({});", name, COLS_SQL)),
        StmtKind::Raw(format!("INSERT INTO {} {};", name, row_source(1, rows))),
    ];
    // Core indexes on the opclasses the idx module does NOT own (it owns
    // gist point/box and spgist point): GiST polygon/circle
    // (gist_poly_consistent / gist_circle_consistent + distance) and
    // SP-GiST box/polygon (geo_spgist.c — 33 gap functions; hand-verified
    // identical plans + rowsets on both engines).
    let mut indexes = Vec::new();
    for (kind, using) in [
        ("geo:x:gistpoly", "gist (pgn)"),
        ("geo:x:gistcirc", "gist (cr)"),
        ("geo:x:spgbox", "spgist (bx)"),
        ("geo:x:spgpoly", "spgist (pgn)"),
    ] {
        g.fire(kind);
        let iname = format!("fz_geoi_{}", g.geo.next_index);
        g.geo.next_index += 1;
        stmts.push(StmtKind::Raw(format!("CREATE INDEX {} ON {} USING {};", iname, name, using)));
        indexes.push(iname);
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {};", name)));
    g.geo.tables.push(GeoTable { name: name.clone(), live: true, next_pk: rows, indexes });
    g.geo.events.push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.geo.live_tables();
    if live.is_empty() {
        // Never recurses: empty population is below the cap by definition.
        g.fire("geo:fallback:create");
        return gen_create(g);
    }
    g.fire("geo:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.geo.tables[ti].name.clone();
    g.geo.tables[ti].live = false;
    g.geo.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

// ----------------------------------------------------------------- scalar ----

/// One scalar expression over literals, from the operator/function matrix.
fn scalar_expr(g: &mut Gen) -> String {
    let class = g.weights.pick(
        g.rng,
        &["geo:sc:bool", "geo:sc:dist", "geo:sc:arith", "geo:sc:closest", "geo:sc:inter",
          "geo:sc:prefix", "geo:sc:func"],
    );
    g.fire(class);
    match class {
        "geo:sc:bool" => {
            let (op, l, r) = BOOL_OPS[g.rng.below_usize(BOOL_OPS.len())];
            format!("({}) {} ({})", lit(g, l), op, lit(g, r))
        }
        "geo:sc:dist" => {
            let (l, r) = DIST_OPS[g.rng.below_usize(DIST_OPS.len())];
            format!("({}) <-> ({})", lit(g, l), lit(g, r))
        }
        "geo:sc:arith" => {
            let (op, l) = ARITH_OPS[g.rng.below_usize(ARITH_OPS.len())];
            // path + path concatenation is the one non-point rhs (path_add).
            let rhs = if op == "+" && l == "path" && g.rng.chance(1, 2) {
                path_lit(g)
            } else if op == "/" {
                point_nz_lit(g)
            } else {
                point_lit(g)
            };
            format!("({}) {} ({})", lit(g, l), op, rhs)
        }
        "geo:sc:closest" => {
            let (l, r) = CLOSEST_OPS[g.rng.below_usize(CLOSEST_OPS.len())];
            format!("({}) ## ({})", lit(g, l), lit(g, r))
        }
        "geo:sc:inter" => {
            let (l, r) = INTER_OPS[g.rng.below_usize(INTER_OPS.len())];
            format!("({}) # ({})", lit(g, l), lit(g, r))
        }
        "geo:sc:prefix" => {
            let (op, r) = PREFIX_OPS[g.rng.below_usize(PREFIX_OPS.len())];
            format!("{} ({})", op, lit(g, r))
        }
        _ => {
            if g.rng.chance(1, 2) {
                let (f, a) = FUNCS1[g.rng.below_usize(FUNCS1.len())];
                format!("{}({})", f, lit(g, a))
            } else {
                let (f, a, b) = FUNCS2[g.rng.below_usize(FUNCS2.len())];
                format!("{}({}, {})", f, lit(g, a), lit(g, b))
            }
        }
    }
}

fn gen_scalar(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("geo:scalar");
    let n = 1 + g.rng.below(3);
    let exprs: Vec<String> = (0..n).map(|_| scalar_expr(g)).collect();
    vec![StmtKind::Raw(format!("SELECT {};", exprs.join(", ")))]
}

fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("geo:cast");
    let (s, t) = CASTS[g.rng.below_usize(CASTS.len())];
    // path -> polygon requires a closed path (open-path casts are a
    // verified both-sides-error shape).
    let src = if s == "path" && t == "polygon" { closed_path_lit(g) } else { lit(g, s) };
    vec![StmtKind::Raw(format!("SELECT (({}))::{};", src, t))]
}

// ------------------------------------------------------------ table shapes ----

fn pick_live(g: &mut Gen) -> Option<usize> {
    let live = g.geo.live_tables();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

/// Column-vs-literal boolean predicate from the matrix: pick an op whose
/// left or right family has a column, put the column on that side.
fn table_pred(g: &mut Gen) -> String {
    let fam_prod = g.weights.pick(
        g.rng,
        &["geo:fam:pt", "geo:fam:ln", "geo:fam:sg", "geo:fam:bx", "geo:fam:pa",
          "geo:fam:pgn", "geo:fam:cr"],
    );
    g.fire(fam_prod);
    let fam = FAMS[["geo:fam:pt", "geo:fam:ln", "geo:fam:sg", "geo:fam:bx", "geo:fam:pa",
                    "geo:fam:pgn", "geo:fam:cr"]
        .iter()
        .position(|p| *p == fam_prod)
        .unwrap()];
    let col = col_of(fam);
    // Candidate ops with this family on either side.
    let cands: Vec<&(&str, &str, &str)> =
        BOOL_OPS.iter().filter(|(_, l, r)| *l == fam || *r == fam).collect();
    let (op, l, r) = *cands[g.rng.below_usize(cands.len())];
    if l == fam && (r != fam || g.rng.chance(1, 2)) {
        format!("{} {} ({})", col, op, lit(g, r))
    } else {
        format!("({}) {} {}", lit(g, l), op, col)
    }
}

/// Optionally wrap `body` in the seqscan-off bracket (SET and RESET in the
/// same statement group, applied identically both sides — the idx-module
/// discipline) so the GiST/SP-GiST consistent paths actually engage.
fn maybe_bracket(g: &mut Gen, body: StmtKind) -> Vec<StmtKind> {
    if g.weights.pick(g.rng, &["geo:seqscan:off", "geo:seqscan:on"]) == "geo:seqscan:off" {
        g.fire("geo:seqscan:off");
        vec![
            StmtKind::Raw("SET enable_seqscan TO off;".to_string()),
            body,
            StmtKind::Raw("RESET enable_seqscan;".to_string()),
        ]
    } else {
        vec![body]
    }
}

fn gen_query(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("geo:fallback:create");
        return gen_create(g);
    };
    g.fire("geo:query");
    let table = g.geo.tables[ti].name.clone();
    let pred = table_pred(g);
    let shape = g.weights.pick(g.rng, &["geo:q:rows", "geo:q:count"]);
    g.fire(shape);
    let sql = match shape {
        "geo:q:rows" => format!("SELECT pk FROM {} WHERE {} ORDER BY pk;", table, pred),
        _ => format!("SELECT count(*) FROM {} WHERE {};", table, pred),
    };
    maybe_bracket(g, StmtKind::Raw(sql))
}

/// Projection expressions over columns: distances, arithmetic, functions,
/// closest points — every result deterministic per row, ORDER BY pk total.
fn calc_expr(g: &mut Gen) -> String {
    match g.rng.below(5) {
        0 => {
            // Column-vs-literal distance (column side picked from pairs).
            let (l, r) = DIST_OPS[g.rng.below_usize(DIST_OPS.len())];
            if g.rng.chance(1, 2) {
                format!("{} <-> ({})", col_of(l), lit(g, r))
            } else {
                format!("({}) <-> {}", lit(g, l), col_of(r))
            }
        }
        1 => {
            let (op, l) = ARITH_OPS[g.rng.below_usize(ARITH_OPS.len())];
            let rhs = if op == "/" { point_nz_lit(g) } else { point_lit(g) };
            format!("{} {} ({})", col_of(l), op, rhs)
        }
        2 => {
            let (f, a) = FUNCS1[g.rng.below_usize(FUNCS1.len())];
            format!("{}({})", f, col_of(a))
        }
        3 => {
            let (op, r) = PREFIX_OPS[g.rng.below_usize(PREFIX_OPS.len())];
            format!("{} {}", op, col_of(r))
        }
        _ => {
            let (l, r) = CLOSEST_OPS[g.rng.below_usize(CLOSEST_OPS.len())];
            if g.rng.chance(1, 2) {
                format!("{} ## ({})", col_of(l), lit(g, r))
            } else {
                format!("({}) ## {}", lit(g, l), col_of(r))
            }
        }
    }
}

fn gen_calc(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("geo:fallback:create");
        return gen_create(g);
    };
    g.fire("geo:calc");
    let table = g.geo.tables[ti].name.clone();
    let n = 1 + g.rng.below(3);
    let exprs: Vec<String> = (0..n).map(|_| calc_expr(g)).collect();
    vec![StmtKind::Raw(format!(
        "SELECT pk, {} FROM {} ORDER BY pk;",
        exprs.join(", "),
        table
    ))]
}

/// KNN ordering: distance then pk (distance ties on the integer grid stay
/// a total order). Column picked among the KNN-orderable families.
fn gen_knn(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("geo:fallback:create");
        return gen_create(g);
    };
    g.fire("geo:knn");
    let table = g.geo.tables[ti].name.clone();
    let col = ["pt", "bx", "pgn", "cr", "sg"][g.rng.below_usize(5)];
    let k = 5 + g.rng.below(15);
    vec![StmtKind::Raw(format!(
        "SELECT pk FROM {} ORDER BY {} <-> point({}, {}), pk LIMIT {};",
        table,
        col,
        g.rng.range_i64(-10, 11),
        g.rng.range_i64(-11, 12),
        k
    ))]
}

/// Geo-column join predicate between two live tables (or a self-join):
/// `a.<col> <op> b.<col>` join clauses drive the geo_selfuncs.c join
/// estimators (areajoinsel/contjoinsel/positionjoinsel) that per-table
/// restriction quals never reach. count(*) output: deterministic scalar.
fn gen_join(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.geo.live_tables();
    if live.is_empty() {
        g.fire("geo:fallback:create");
        return gen_create(g);
    }
    g.fire("geo:join");
    let ta = g.geo.tables[live[g.rng.below_usize(live.len())]].name.clone();
    let tb = g.geo.tables[live[g.rng.below_usize(live.len())]].name.clone();
    // Same-family column-vs-column boolean ops only.
    let cands: Vec<&(&str, &str, &str)> = BOOL_OPS.iter().filter(|(_, l, r)| l == r).collect();
    let (op, fam, _) = *cands[g.rng.below_usize(cands.len())];
    let col = col_of(fam);
    vec![StmtKind::Raw(format!(
        "SELECT count(*) FROM {} a, {} b WHERE a.{} {} b.{};",
        ta, tb, col, op, col
    ))]
}

// ------------------------------------------------------------------ churn ----

fn gen_churn(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("geo:fallback:create");
        return gen_create(g);
    };
    g.fire("geo:churn");
    let table = g.geo.tables[ti].name.clone();
    let form = g.weights.pick(
        g.rng,
        &["geo:churn:update", "geo:churn:delete", "geo:churn:insert",
          "geo:churn:reindex", "geo:churn:vacuum"],
    );
    g.fire(form);
    match form {
        "geo:churn:update" => {
            let m = 3 + g.rng.below(9);
            let r = g.rng.below(m);
            let set = match g.rng.below(3) {
                0 => "pt = point((pk*29) % 21 - 10, (pk*41) % 23 - 11), \
                      cr = circle(point((pk*7) % 21 - 10, (pk*13) % 23 - 11), (pk % 5))"
                    .to_string(),
                1 => "pgn = polygon(box(point((pk*11) % 17 - 8, (pk*17) % 13 - 6), \
                      point((pk*11) % 17 - 8 + 2, (pk*17) % 13 - 6 + 2))), \
                      bx = box(point((pk*3) % 19 - 9, (pk*5) % 15 - 7), \
                      point((pk*3) % 19 - 9 + 1 + (pk % 4), (pk*5) % 15 - 7 + 1 + (pk % 3)))"
                    .to_string(),
                _ => "sg = lseg(point((pk*13) % 13 - 6, (pk*19) % 17 - 8), \
                      point((pk*13) % 13 - 6 + 1, (pk*19) % 17 - 8 + (pk % 4)))"
                    .to_string(),
            };
            vec![StmtKind::Raw(format!("UPDATE {} SET {} WHERE pk % {} = {};", table, set, m, r))]
        }
        "geo:churn:delete" => {
            let m = 5 + g.rng.below(11);
            let r = g.rng.below(m);
            vec![StmtKind::Raw(format!("DELETE FROM {} WHERE pk % {} = {};", table, m, r))]
        }
        "geo:churn:insert" => {
            let n = 80 + g.rng.below(150) as i64;
            let lo = g.geo.tables[ti].next_pk + 1;
            let hi = g.geo.tables[ti].next_pk + n;
            g.geo.tables[ti].next_pk = hi;
            vec![StmtKind::Raw(format!("INSERT INTO {} {};", table, row_source(lo, hi)))]
        }
        "geo:churn:reindex" => {
            let xs = &g.geo.tables[ti].indexes;
            let name = xs[g.rng.below_usize(xs.len())].clone();
            vec![StmtKind::Raw(format!("REINDEX INDEX {};", name))]
        }
        _ => vec![StmtKind::Raw(format!("VACUUM {};", table))],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent GeoState across groups.
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>, GeoState) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut geo = GeoState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.geo, &mut geo);
            let kinds = gen_geo_module(&mut g);
            std::mem::swap(&mut g.geo, &mut geo);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods, geo)
    }

    fn flat(groups: &[Vec<String>]) -> Vec<String> {
        groups.iter().flatten().cloned().collect()
    }

    #[test]
    fn geo_is_deterministic() {
        let (a, _, _) = gen_many(91, 400, "");
        let (b, _, _) = gen_many(91, 400, "");
        assert_eq!(a, b);
        let (c, _, _) = gen_many(92, 400, "");
        assert_ne!(a, c);
    }

    #[test]
    fn geo_statements_are_well_formed() {
        let (groups, _, _) = gen_many(0xA1, 2000, "");
        for sql in flat(&groups) {
            assert!(!sql.contains('\n'), "multi-line: {sql}");
            assert!(sql.ends_with(';'), "unterminated: {sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "parens: {sql}");
        }
    }

    #[test]
    fn geo_variety() {
        let (groups, prods, _) = gen_many(0x6E0, 4000, "");
        let all = flat(&groups).join("\n");
        for frag in [
            "CREATE TABLE fz_geo_",
            "pt point, ln line, sg lseg, bx box",
            "USING gist (pgn)",
            "USING gist (cr)",
            "USING spgist (bx)",
            "USING spgist (pgn)",
            "ANALYZE fz_geo_",
            "DROP TABLE fz_geo_",
            " <-> ",
            " ## ",
            " ?# ",
            " ?-| ",
            " ?|| ",
            " ~= ",
            " <@ ",
            " @> ",
            "@@ (",
            "@-@ (",
            "::polygon",
            "::path",
            "::line",
            "circle(point(",
            "polygon(box(point(",
            "'NaN'",
            "area(",
            "bound_box(",
            "line_interpt(",
            "lseg_interpt(",
            "slope(",
            "pclose(",
            "popen(",
            "ORDER BY pk;",
            ", pk LIMIT ",
            "SELECT count(*) FROM fz_geo_",
            "UPDATE fz_geo_",
            "DELETE FROM fz_geo_",
            "REINDEX INDEX fz_geoi_",
            "VACUUM fz_geo_",
            "generate_series(1, ",
            "SET enable_seqscan TO off;",
            "RESET enable_seqscan;",
            "'::point",
            "'::box",
            "'::lseg",
            "'::circle",
            " a, ",
        ] {
            assert!(all.contains(frag), "geo flavor {frag:?} never generated");
        }
        for p in [
            "geo:create", "geo:drop", "geo:scalar", "geo:cast", "geo:query", "geo:calc",
            "geo:knn", "geo:join", "geo:churn", "geo:sc:bool", "geo:sc:dist", "geo:sc:arith",
            "geo:sc:closest", "geo:sc:inter", "geo:sc:prefix", "geo:sc:func",
            "geo:fam:pt", "geo:fam:ln", "geo:fam:sg", "geo:fam:bx", "geo:fam:pa",
            "geo:fam:pgn", "geo:fam:cr", "geo:q:rows", "geo:q:count", "geo:lit:nan",
            "geo:churn:update", "geo:churn:delete", "geo:churn:insert",
            "geo:churn:reindex", "geo:churn:vacuum", "geo:x:gistpoly", "geo:x:gistcirc",
            "geo:x:spgbox", "geo:x:spgpoly", "geo:seqscan:off",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Model-replay statics: no dead-relation references, pk insert batches
    /// strictly increase, REINDEX targets live indexes, `/` never divides
    /// by a zero-coordinate point, path->polygon casts are closed paths.
    #[test]
    fn model_replay_holds() {
        let (groups, _, _) = gen_many(0xF1DE, 3000, "");
        use std::collections::HashMap;
        let mut live: HashMap<String, bool> = HashMap::new();
        let mut idx_table: HashMap<String, String> = HashMap::new();
        let mut max_pk: HashMap<String, i64> = HashMap::new();
        for sql in flat(&groups) {
            if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(!live.contains_key(&name), "table name reused: {sql}");
                live.insert(name, true);
            } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                let name = rest.trim_end_matches(';').to_string();
                assert_eq!(live.get(&name), Some(&true), "drop of dead table: {sql}");
                live.insert(name, false);
            } else if let Some(rest) = sql.strip_prefix("CREATE INDEX ") {
                let iname = rest.split(' ').next().unwrap().to_string();
                let tname = sql.split(" ON ").nth(1).unwrap().split([' ', '(']).next().unwrap();
                assert_eq!(live.get(tname), Some(&true), "index on dead table: {sql}");
                idx_table.insert(iname, tname.to_string());
            } else if let Some(rest) = sql.strip_prefix("REINDEX INDEX ") {
                let iname = rest.trim_end_matches(';');
                let t = idx_table.get(iname).expect("reindex of unknown index");
                assert_eq!(live.get(t), Some(&true), "reindex on dead table: {sql}");
            } else if let Some(rest) = sql.strip_prefix("VACUUM ").or_else(|| sql.strip_prefix("ANALYZE ")) {
                let name = rest.trim_end_matches(';');
                assert_eq!(live.get(name), Some(&true), "utility on dead table: {sql}");
            } else if sql.contains(" FROM fz_geo_") || sql.starts_with("UPDATE fz_geo_") {
                let name = sql
                    .split("fz_geo_")
                    .nth(1)
                    .unwrap()
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>();
                let name = format!("fz_geo_{name}");
                assert_eq!(live.get(&name), Some(&true), "reference to dead table: {sql}");
            }
            if let Some(rest) = sql.strip_prefix("INSERT INTO ") {
                let tname = rest.split(' ').next().unwrap().to_string();
                let series = sql.split("generate_series(").nth(1).unwrap();
                let lo: i64 = series.split(',').next().unwrap().trim().parse().unwrap();
                let hi: i64 = series.split(", ").nth(1).unwrap().split(')').next().unwrap().trim().parse().unwrap();
                assert!(lo <= hi, "{sql}");
                let prev = *max_pk.get(&tname).unwrap_or(&0);
                assert!(lo > prev, "pk batch overlap (prev max {prev}): {sql}");
                max_pk.insert(tname, hi);
            }
            // No zero-coordinate divisor points anywhere.
            for after in sql.split(" / (point(").skip(1) {
                let args: String = after.chars().take_while(|c| *c != ')').collect();
                for a in args.split(',') {
                    let v: f64 = a.trim().parse().unwrap();
                    assert!(v != 0.0, "zero-coordinate divisor: {sql}");
                }
            }
            // path -> polygon casts always closed (never a '[' path form).
            if sql.contains("::path))::polygon") {
                assert!(!sql.contains("'['"), "open path cast to polygon: {sql}");
                let inner = sql.split("'").nth(1).unwrap_or("");
                assert!(inner.starts_with('('), "open path cast to polygon: {sql}");
            }
        }
    }

    #[test]
    fn population_capped_and_events_pair() {
        let (groups, _, state) = gen_many(0xCAB, 1500, "");
        assert!(state.live_tables().len() <= MAX_LIVE_TABLES);
        let mut open = 0i32;
        for sql in flat(&groups) {
            if sql.starts_with("CREATE TABLE ") {
                open += 1;
                assert!(open as usize <= MAX_LIVE_TABLES, "cap breached: {sql}");
            } else if sql.starts_with("DROP TABLE ") {
                open -= 1;
            }
        }
        // Create groups carry both GiST indexes + ANALYZE.
        for gp in &groups {
            if gp[0].starts_with("CREATE TABLE fz_geo_") {
                let joined = gp.join("\n");
                assert!(joined.contains("USING gist (pgn)"), "missing poly index");
                assert!(joined.contains("USING gist (cr)"), "missing circle index");
                assert!(joined.contains("USING spgist (bx)"), "missing spgist box index");
                assert!(joined.contains("USING spgist (pgn)"), "missing spgist poly index");
                assert!(joined.contains("ANALYZE "), "missing ANALYZE");
            }
        }
    }
}
