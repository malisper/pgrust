//! Minimal typed catalog snapshot. F0 ships the built-in fixture schema;
//! `CatalogSource` is the seam a live-server introspection loader slots
//! into at F1. F4c adds primary keys to the fixture: the DML module's
//! collision-control handle and the state probes' total-order sort key.
//! T1 adds the rich types (json/jsonb, uuid, bytea, interval, time/timetz,
//! arrays) and the fz_rich fixture table that carries them.

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum SqlType {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Bool,
    Date,
    Timestamp,
    /// Deliberately NOT in `ALL_TYPES`: json has no equality/ordering
    /// operators, so the generic machinery (comparisons, ORDER BY over
    /// output columns, GROUP BY) must never draw it. Reached only through
    /// explicit productions (json operators, DML writes to the fixture's
    /// json column).
    Json,
    Jsonb,
    Uuid,
    Bytea,
    Interval,
    Time,
    Timetz,
    /// text[]
    TextArr,
    /// int4[]
    Int4Arr,
}

pub const ALL_TYPES: &[SqlType] = &[
    SqlType::Int2,
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Float4,
    SqlType::Float8,
    SqlType::Numeric,
    SqlType::Text,
    SqlType::Varchar,
    SqlType::Bool,
    SqlType::Date,
    SqlType::Timestamp,
    // T1 rich types (all with btree ordering, so ORDER BY/DISTINCT/cmp are
    // always well-typed; Json is excluded — see the enum docs).
    SqlType::Jsonb,
    SqlType::Uuid,
    SqlType::Bytea,
    SqlType::Interval,
    SqlType::Time,
    SqlType::Timetz,
    SqlType::TextArr,
    SqlType::Int4Arr,
];

impl SqlType {
    pub fn oid(self) -> u32 {
        match self {
            SqlType::Int2 => 21,
            SqlType::Int4 => 23,
            SqlType::Int8 => 20,
            SqlType::Float4 => 700,
            SqlType::Float8 => 701,
            SqlType::Numeric => 1700,
            SqlType::Text => 25,
            SqlType::Varchar => 1043,
            SqlType::Bool => 16,
            SqlType::Date => 1082,
            SqlType::Timestamp => 1114,
            SqlType::Json => 114,
            SqlType::Jsonb => 3802,
            SqlType::Uuid => 2950,
            SqlType::Bytea => 17,
            SqlType::Interval => 1186,
            SqlType::Time => 1083,
            SqlType::Timetz => 1266,
            SqlType::TextArr => 1009,
            SqlType::Int4Arr => 1007,
        }
    }

    /// Inverse of `oid()`: None for types the generator doesn't target.
    /// (Json is resolvable here even though it is not in `ALL_TYPES` —
    /// live-catalog columns of type json must load as Json, not be
    /// silently dropped.)
    pub fn from_oid(oid: u32) -> Option<SqlType> {
        ALL_TYPES
            .iter()
            .copied()
            .chain(std::iter::once(SqlType::Json))
            .find(|t| t.oid() == oid)
    }

    /// SQL-spelling of the type name, valid in casts and DDL.
    pub fn name(self) -> &'static str {
        match self {
            SqlType::Int2 => "int2",
            SqlType::Int4 => "int4",
            SqlType::Int8 => "int8",
            SqlType::Float4 => "float4",
            SqlType::Float8 => "float8",
            SqlType::Numeric => "numeric",
            SqlType::Text => "text",
            SqlType::Varchar => "varchar",
            SqlType::Bool => "bool",
            SqlType::Date => "date",
            SqlType::Timestamp => "timestamp",
            SqlType::Json => "json",
            SqlType::Jsonb => "jsonb",
            SqlType::Uuid => "uuid",
            SqlType::Bytea => "bytea",
            SqlType::Interval => "interval",
            SqlType::Time => "time",
            SqlType::Timetz => "timetz",
            SqlType::TextArr => "text[]",
            SqlType::Int4Arr => "int4[]",
        }
    }

    pub fn is_integer(self) -> bool {
        matches!(self, SqlType::Int2 | SqlType::Int4 | SqlType::Int8)
    }

    pub fn is_float(self) -> bool {
        matches!(self, SqlType::Float4 | SqlType::Float8)
    }

    pub fn is_numeric_family(self) -> bool {
        self.is_integer() || self.is_float() || self == SqlType::Numeric
    }

    pub fn is_text_family(self) -> bool {
        matches!(self, SqlType::Text | SqlType::Varchar)
    }

    pub fn is_datetime(self) -> bool {
        matches!(self, SqlType::Date | SqlType::Timestamp)
    }

    /// Same-family types whose values compare with =, <, etc. without an
    /// explicit cast. The rich types compare same-type only (their btree
    /// operator families are per-type); json never compares (no equality
    /// operators at all).
    pub fn comparable_with(self, other: SqlType) -> bool {
        (self.is_numeric_family() && other.is_numeric_family())
            || (self.is_text_family() && other.is_text_family())
            || (self == SqlType::Bool && other == SqlType::Bool)
            || (self.is_datetime() && other.is_datetime())
            || (self == other
                && matches!(
                    self,
                    SqlType::Jsonb
                        | SqlType::Uuid
                        | SqlType::Bytea
                        | SqlType::Interval
                        | SqlType::Time
                        | SqlType::Timetz
                        | SqlType::TextArr
                        | SqlType::Int4Arr
                ))
    }

    /// Cast targets guaranteed to be accepted by the parser/analyzer
    /// (execution may still raise, e.g. out-of-range — a semantic error,
    /// which is fine).
    pub fn cast_targets(self) -> &'static [SqlType] {
        match self {
            SqlType::Int2 | SqlType::Int4 | SqlType::Int8 => &[
                SqlType::Int2,
                SqlType::Int4,
                SqlType::Int8,
                SqlType::Float4,
                SqlType::Float8,
                SqlType::Numeric,
                SqlType::Text,
            ],
            SqlType::Float4 | SqlType::Float8 => &[
                SqlType::Int2,
                SqlType::Int4,
                SqlType::Int8,
                SqlType::Float4,
                SqlType::Float8,
                SqlType::Numeric,
                SqlType::Text,
            ],
            SqlType::Numeric => &[
                SqlType::Int2,
                SqlType::Int4,
                SqlType::Int8,
                SqlType::Float4,
                SqlType::Float8,
                SqlType::Numeric,
                SqlType::Text,
            ],
            SqlType::Text | SqlType::Varchar => &[SqlType::Text, SqlType::Varchar],
            SqlType::Bool => &[SqlType::Bool, SqlType::Text, SqlType::Int4],
            SqlType::Date => &[SqlType::Date, SqlType::Timestamp, SqlType::Text],
            SqlType::Timestamp => &[
                SqlType::Date,
                SqlType::Timestamp,
                SqlType::Text,
                SqlType::Time,
            ],
            // Rich types: self, text output, and the few honest built-in
            // casts. json<->jsonb both ways; time->interval and
            // timetz->time exist; arrays render via ::text.
            SqlType::Json => &[SqlType::Json, SqlType::Jsonb, SqlType::Text],
            SqlType::Jsonb => &[SqlType::Jsonb, SqlType::Json, SqlType::Text],
            SqlType::Uuid => &[SqlType::Uuid, SqlType::Text],
            SqlType::Bytea => &[SqlType::Bytea, SqlType::Text],
            SqlType::Interval => &[SqlType::Interval, SqlType::Text, SqlType::Time],
            SqlType::Time => &[SqlType::Time, SqlType::Interval, SqlType::Text],
            SqlType::Timetz => &[SqlType::Timetz, SqlType::Time, SqlType::Text],
            SqlType::TextArr => &[SqlType::TextArr, SqlType::Text],
            SqlType::Int4Arr => &[SqlType::Int4Arr, SqlType::Text],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
    /// DDL type spelling override, e.g. `numeric(5,2)` — the typmod
    /// surface. None renders `ty.name()`. Affects DDL only: expressions
    /// still see `ty` (a typmod'd numeric is still numeric on the wire).
    pub ddl_type: Option<String>,
}

/// Primary key metadata: an int4 UNIQUE NOT NULL column. The state probes'
/// total-order sort key (`SELECT * FROM t ORDER BY <pk>` compares strictly)
/// and the DML module's collision-control handle. The fixture keeps pk
/// values dense from 1: seed rows use 1..=seeded_max, the generator
/// allocates fresh values monotonically above.
#[derive(Clone, Debug)]
pub struct Pk {
    pub column: String,
    /// Largest pk value among the seed rows (0 for empty tables): the
    /// floor for fresh-pk allocation.
    pub seeded_max: i64,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Guaranteed to hold at most one row (fixture invariant). Scalar
    /// subqueries may use such tables as a FROM source without risking a
    /// more-than-one-row error; live catalogs never set this. The DML
    /// module never writes to such tables (the invariant is load-bearing).
    pub at_most_one_row: bool,
    /// Primary key; None = not a DML target and not probeable.
    pub pk: Option<Pk>,
    /// Column whose values are unique across the table's rows (fixture
    /// invariant; live catalogs would need pg_constraint introspection and
    /// leave it unset). Order-sensitive window functions require a window
    /// ORDER BY ending in this key so the order is total per partition;
    /// fz_scalar deliberately has none, exercising the fallback path.
    /// On DML-target tables this must be the pk column — any other column
    /// is mutable (UPDATE can manufacture duplicates mid-stream, silently
    /// breaking window-order totality).
    pub unique_key: Option<String>,
    /// The pk column carries a real unique constraint (PRIMARY KEY or
    /// unique index). True for every fixture and ddl table; false for
    /// partitioned parents whose partition key is not exactly pk (a
    /// partitioned unique constraint must include every key column). When
    /// false the DML module never emits ON CONFLICT (pk) — there is no
    /// matching unique index (42P10) — and never picks colliding pk values:
    /// a collision would silently insert a duplicate row and break the
    /// state probes' pk total order.
    pub pk_unique: bool,
}

impl Table {
    pub fn columns_of_type(&self, ty: SqlType) -> Vec<&Column> {
        self.columns.iter().filter(|c| c.ty == ty).collect()
    }

    /// Is `name` the primary-key column?
    pub fn is_pk_column(&self, name: &str) -> bool {
        self.pk.as_ref().is_some_and(|pk| pk.column == name)
    }
}

#[derive(Clone, Debug)]
pub struct Catalog {
    pub tables: Vec<Table>,
}

/// Introspection seam: F0 provides the fixture; a live libpq-style loader
/// implements this at F1.
pub trait CatalogSource {
    fn load_catalog(&self) -> Result<Catalog, String>;
}

pub struct FixtureCatalog;

fn col(name: &str, ty: SqlType, nullable: bool) -> Column {
    Column { name: name.to_string(), ty, nullable, ddl_type: None }
}

/// Column with a DDL type-spelling override (the typmod surface).
fn colm(name: &str, ty: SqlType, nullable: bool, ddl: &str) -> Column {
    Column { name: name.to_string(), ty, nullable, ddl_type: Some(ddl.to_string()) }
}

fn pk(column: &str, seeded_max: i64) -> Option<Pk> {
    Some(Pk { column: column.to_string(), seeded_max })
}

impl CatalogSource for FixtureCatalog {
    fn load_catalog(&self) -> Result<Catalog, String> {
        // Five tables with realistic shape variety. Every table carries the
        // two join keys `k_int` (int4) and `k_text` (text) — both nullable
        // except fz_one.k_int, with NULLs and duplicates in the data, so
        // NULL join semantics and many-to-many matches are always in play.
        // USING/NATURAL joins ride on the shared key names. Every table
        // also carries a primary key (no defaults of any kind — DEFAULT in
        // generated INSERTs must mean NULL deterministically).
        Ok(Catalog {
            tables: vec![
                Table {
                    name: "fz_scalar".to_string(),
                    at_most_one_row: false,
                    pk: pk("pk", 8),
                    // Deliberately no unique_key hint: exercises the
                    // no-total-order window fallback path.
                    unique_key: None,
                    pk_unique: true,
                    columns: vec![
                        col("pk", SqlType::Int4, false),
                        col("k_int", SqlType::Int4, true),
                        col("k_text", SqlType::Text, true),
                        col("c_int2", SqlType::Int2, true),
                        col("c_int4", SqlType::Int4, false),
                        col("c_int8", SqlType::Int8, true),
                        col("c_float4", SqlType::Float4, true),
                        col("c_float8", SqlType::Float8, false),
                        col("c_numeric", SqlType::Numeric, true),
                        col("c_text", SqlType::Text, true),
                        col("c_varchar", SqlType::Varchar, true),
                        col("c_bool", SqlType::Bool, false),
                        col("c_date", SqlType::Date, true),
                        col("c_ts", SqlType::Timestamp, true),
                    ],
                },
                Table {
                    name: "fz_mixed".to_string(),
                    at_most_one_row: false,
                    pk: pk("id", 6),
                    unique_key: Some("id".to_string()),
                    pk_unique: true,
                    columns: vec![
                        col("id", SqlType::Int4, false),
                        col("k_int", SqlType::Int4, true),
                        col("k_text", SqlType::Text, true),
                        col("val", SqlType::Float8, true),
                        col("amount", SqlType::Numeric, true),
                        col("tag", SqlType::Text, true),
                        col("flag", SqlType::Bool, true),
                        col("created", SqlType::Date, true),
                    ],
                },
                // Wide table: many columns across the type spread.
                Table {
                    name: "fz_wide".to_string(),
                    at_most_one_row: false,
                    pk: pk("pk", 10),
                    unique_key: Some("pk".to_string()),
                    pk_unique: true,
                    columns: vec![
                        col("pk", SqlType::Int4, false),
                        col("k_int", SqlType::Int4, true),
                        col("k_text", SqlType::Text, true),
                        col("w_int2", SqlType::Int2, true),
                        col("w_int4", SqlType::Int4, false),
                        col("w_int8", SqlType::Int8, true),
                        col("w_float4", SqlType::Float4, true),
                        col("w_float8", SqlType::Float8, false),
                        col("w_numeric", SqlType::Numeric, true),
                        col("w_text", SqlType::Text, true),
                        col("w_varchar", SqlType::Varchar, true),
                        col("w_bool", SqlType::Bool, false),
                        col("w_date", SqlType::Date, true),
                        col("w_ts", SqlType::Timestamp, true),
                        col("w_int4_b", SqlType::Int4, true),
                        col("w_int8_b", SqlType::Int8, true),
                        col("w_float8_b", SqlType::Float8, true),
                        col("w_numeric_b", SqlType::Numeric, true),
                        col("w_text_b", SqlType::Text, true),
                        col("w_bool_b", SqlType::Bool, true),
                        col("w_date_b", SqlType::Date, true),
                    ],
                },
                // Empty table: outer-join NULL extension, empty-input paths.
                // DML never touches it (at_most_one_row), so it stays empty.
                Table {
                    name: "fz_empty".to_string(),
                    at_most_one_row: true,
                    pk: pk("pk", 0),
                    unique_key: Some("pk".to_string()),
                    pk_unique: true,
                    columns: vec![
                        col("pk", SqlType::Int4, false),
                        col("k_int", SqlType::Int4, true),
                        col("k_text", SqlType::Text, true),
                        col("note", SqlType::Varchar, true),
                        col("flag", SqlType::Bool, true),
                    ],
                },
                // Rich-type table (T1): json/jsonb, uuid, bytea, interval,
                // time/timetz, typmod'd numerics, arrays. DML-eligible like
                // the other multi-row tables (writes flow through the same
                // typed expression generator).
                Table {
                    name: "fz_rich".to_string(),
                    at_most_one_row: false,
                    pk: pk("pk", 8),
                    unique_key: Some("pk".to_string()),
                    pk_unique: true,
                    columns: vec![
                        col("pk", SqlType::Int4, false),
                        col("k_int", SqlType::Int4, true),
                        col("k_text", SqlType::Text, true),
                        col("r_json", SqlType::Json, true),
                        col("r_jsonb", SqlType::Jsonb, true),
                        col("r_jsonb_b", SqlType::Jsonb, true),
                        col("r_uuid", SqlType::Uuid, true),
                        col("r_bytea", SqlType::Bytea, true),
                        col("r_interval", SqlType::Interval, true),
                        col("r_time", SqlType::Time, true),
                        col("r_timetz", SqlType::Timetz, true),
                        colm("r_num52", SqlType::Numeric, true, "numeric(5,2)"),
                        colm("r_num100", SqlType::Numeric, true, "numeric(10,0)"),
                        col("r_textarr", SqlType::TextArr, true),
                        col("r_intarr", SqlType::Int4Arr, true),
                    ],
                },
                // Single-row table: deterministic scalar-subquery source.
                Table {
                    name: "fz_one".to_string(),
                    at_most_one_row: true,
                    pk: pk("k_int", 1),
                    unique_key: Some("k_int".to_string()),
                    pk_unique: true,
                    columns: vec![
                        col("k_int", SqlType::Int4, false),
                        col("k_text", SqlType::Text, true),
                        col("v_num", SqlType::Numeric, true),
                        col("seen", SqlType::Timestamp, true),
                    ],
                },
            ],
        })
    }
}

/// DDL for the fixture schema, for standing up a smoke target. Primary-key
/// columns render as PRIMARY KEY (which implies NOT NULL); no column ever
/// carries a DEFAULT (generated DEFAULTs must mean NULL deterministically).
pub fn fixture_ddl(catalog: &Catalog) -> String {
    let mut out = String::new();
    for t in &catalog.tables {
        out.push_str("DROP TABLE IF EXISTS ");
        out.push_str(&t.name);
        out.push_str(";\nCREATE TABLE ");
        out.push_str(&t.name);
        out.push_str(" (");
        for (i, c) in t.columns.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&c.name);
            out.push(' ');
            out.push_str(c.ddl_type.as_deref().unwrap_or(c.ty.name()));
            if t.is_pk_column(&c.name) {
                out.push_str(" PRIMARY KEY");
            } else if !c.nullable {
                out.push_str(" NOT NULL");
            }
        }
        out.push_str(");\n");
    }
    out
}

/// Deterministic seed rows for the fixture schema: NULLs in every nullable
/// column somewhere (join keys included — NULL join semantics), duplicate
/// join-key values on both sides (many-to-many matches, ORDER BY ties),
/// negative and fractional floats, quoting-sensitive text. Identical on
/// both servers by construction — the differential runner's baseline data.
/// fz_empty stays empty; fz_one holds exactly one row. Primary keys run
/// dense from 1 (`Pk::seeded_max` matches the largest one).
pub fn fixture_seed_sql() -> Vec<String> {
    let mut out = Vec::new();
    // Columns: pk, k_int, k_text, then the 11 scalar columns.
    let scalar_rows = [
        "(1, 1, 'a', 1, 10, 100, 1.5, 2.25, 3.14, 'alpha', 'va', true, '2020-01-01', '2020-01-01 00:00:00')",
        "(2, 2, 'b', 2, 10, -100, -1.5, -2.25, -3.14, 'beta', 'vb', false, '2021-06-15', '2021-06-15 12:34:56')",
        "(3, NULL, NULL, NULL, 20, NULL, NULL, 0.0, NULL, NULL, NULL, true, NULL, NULL)",
        "(4, 3, 'c', 4, 20, 9000000000, 0.125, 1e10, 0.00001, 'it''s', 'vd', false, '1999-12-31', '1999-12-31 23:59:59')",
        "(5, 1, 'a', 5, 30, 0, 3.5, -1e-10, 42, '', 've', true, '2020-01-01', '2020-01-01 00:00:00')",
        "(6, 2, NULL, 6, 30, 100, 1.5, 2.25, 3.14, 'alpha', 'va', false, '2024-02-29', '2024-02-29 08:00:00')",
        "(7, NULL, 'd', NULL, 40, 7, NULL, 7.75, -0.5, 'delta', NULL, true, '2022-03-03', NULL)",
        "(8, 4, 'e', 8, 40, -7, -3.5, 2.25, 3.14, 'Alpha', 'vh', false, NULL, '2023-11-11 11:11:11')",
    ];
    for r in scalar_rows {
        out.push(format!("INSERT INTO fz_scalar VALUES {r};"));
    }
    // Columns: id, k_int, k_text, val, amount, tag, flag, created.
    let mixed_rows = [
        "(1, 1, 'a', 0.5, 10.00, 'x', true, '2020-01-01')",
        "(2, 2, 'b', 0.5, 20.50, 'y', false, '2020-01-02')",
        "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "(4, 1, 'a', -0.5, 10.25, 'x', true, '2021-07-07')",
        "(5, 5, 'z', 100.25, -3.75, 'z''q', false, NULL)",
        "(6, 2, 'c', 0.5, 0.1, '', true, '2020-01-01')",
    ];
    for r in mixed_rows {
        out.push(format!("INSERT INTO fz_mixed VALUES {r};"));
    }
    // Columns: pk, k_int, k_text, w_int2, w_int4, w_int8, w_float4,
    // w_float8, w_numeric, w_text, w_varchar, w_bool, w_date, w_ts,
    // w_int4_b, w_int8_b, w_float8_b, w_numeric_b, w_text_b, w_bool_b,
    // w_date_b.
    let wide_rows = [
        "(1, 1, 'a', 1, 1, 1, 1.5, 1.25, 1.1, 'one', 'w1', true, '2020-01-01', '2020-01-01 01:00:00', 11, 101, 0.5, 9.99, 'x', false, '2020-02-01')",
        "(2, 2, 'b', 2, 2, 2, -2.5, 2.25, 2.2, 'two', 'w2', false, '2020-01-02', '2020-01-02 02:00:00', 12, 102, -0.5, 8.88, 'y', true, '2020-02-02')",
        "(3, NULL, 'a', 3, 3, NULL, NULL, 3.25, NULL, 'three', NULL, true, NULL, '2020-01-03 03:00:00', NULL, 103, 1.5, NULL, 'x', NULL, NULL)",
        "(4, 3, NULL, NULL, 4, 4, 4.5, 'NaN', 4.4, NULL, 'w4', false, '2020-01-04', NULL, 14, NULL, NULL, 7.77, NULL, false, '2020-02-04')",
        "(5, 1, 'a', 5, 5, 5, 5.5, 5.25, 5.5, 'five', 'w5', true, '2020-01-05', '2020-01-05 05:00:00', 15, 105, 2.5, 6.66, 'q''z', true, '2020-02-05')",
        "(6, 2, 'c', 6, 6, 6, 6.5, -6.25, 6.6, 'six', 'w6', false, '2020-01-06', '2020-01-06 06:00:00', 16, 106, -2.5, 5.55, '', false, '2020-02-06')",
        "(7, 5, 'e', 7, 7, 7, 7.5, 7.25, 7.7, 'seven', 'w7', true, '2020-01-07', '2020-01-07 07:00:00', NULL, 107, 3.5, 4.44, 'x', true, '2020-02-07')",
        "(8, NULL, NULL, 8, 8, 8, 8.5, 8.25, 8.8, 'eight', 'w8', false, '2020-01-08', '2020-01-08 08:00:00', 18, 108, NULL, 3.33, 'y', NULL, '2020-02-08')",
        "(9, 4, 'd', 9, 9, 9, 9.5, 9.25, 9.9, 'nine', 'w9', true, '2020-01-09', '2020-01-09 09:00:00', 19, 109, 4.5, 2.22, 'z', false, '2020-02-09')",
        "(10, 1, 'b', 10, 10, 10, 10.5, 10.25, 10.01, 'ten', 'w10', false, '2020-01-10', '2020-01-10 10:00:00', 20, 110, 5.5, 1.11, 'x', true, '2020-02-10')",
    ];
    for r in wide_rows {
        out.push(format!("INSERT INTO fz_wide VALUES {r};"));
    }
    // Columns: pk, k_int, k_text, r_json, r_jsonb, r_jsonb_b, r_uuid,
    // r_bytea, r_interval, r_time, r_timetz, r_num52, r_num100, r_textarr,
    // r_intarr. Boundary-rich: empty json objects/arrays, nested arrays,
    // duplicate json keys (json keeps both, jsonb keeps last), unicode
    // incl. combining chars, zero-length bytea, negative/oversized
    // intervals, 24:00 time, offset timetz, typmod-rounding numerics.
    // (Values stay free of ", " so the arity check's top-level comma count
    // holds.)
    let rich_rows = [
        "(1, 1, 'a', '{}', '{}', '{\"a\":1}', '00000000-0000-0000-0000-000000000000', '\\x', '0', '00:00:00', '00:00:00+00', 0.00, 0, '{}', '{}')",
        "(2, 2, 'b', '{\"a\":1,\"a\":2}', '{\"a\":1,\"a\":2}', '{\"b\":[1,2,3]}', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '\\xdeadbeef00', '-3 days', '23:59:59.999999', '23:59:59-08', 999.99, 9999999999, '{x,y,x}', '{1,2,3}')",
        "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "(4, 1, 'a', '[1,2,[3,[4]]]', '[1,2,[3,[4]]]', '{\"nested\":{\"deep\":{\"deeper\":[]}}}', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', '\\x00', '1 year 2 mons 3 days 04:05:06', '12:30:45', '12:00:00+05:45', -999.99, -9999999999, '{caf\u{e9},nai\u{308}ve}', '{-2147483648,2147483647}')",
        "(5, 3, 'c', '\"scalar\"', '\"scalar\"', 'true', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\x0001ff', '178000000 years', '24:00:00', '00:00:00.000001+14', 1.005, 0.5, '{\"\",\" \"}', '{0}')",
        "(6, 2, NULL, '   {\"ws\":true}', '{\"ws\":true}', 'null', '123e4567-e89b-12d3-a456-426614174000', '\\x27007c', '-178000000 years', '06:00:00', '06:00:00-11', -1.005, 42, '{it''s,\"q\\\"z\"}', '{7,7,7}')",
        "(7, NULL, 'd', '123', '123', '[]', NULL, '\\xff', '00:00:00.000001', '13:37:00', '13:37:00+00', 0.005, -1, NULL, '{-1}')",
        "(8, 4, 'e', '[{},[],null,0,\"\"]', '[{},[],null,0,\"\"]', '{\"k\":\"v\"}', '00010203-0405-0607-0809-0a0b0c0d0e0f', '\\x616263f09f9880', '3 mons -3 days +03:21:00.004', '01:02:03.4', '01:02:03.4+02:30', 12.34, 1234567890, '{alpha,beta}', '{100,-100}')",
    ];
    for r in rich_rows {
        out.push(format!("INSERT INTO fz_rich VALUES {r};"));
    }
    // fz_empty: intentionally no rows. fz_one: exactly one row.
    out.push(
        "INSERT INTO fz_one VALUES (1, 'a', 42.5, '2020-06-01 12:00:00');".to_string(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_oid_roundtrips() {
        for &ty in ALL_TYPES {
            assert_eq!(SqlType::from_oid(ty.oid()), Some(ty));
        }
        assert_eq!(SqlType::from_oid(600), None);
    }

    #[test]
    fn seed_rows_cover_populated_tables() {
        let sql = fixture_seed_sql();
        for t in ["fz_scalar", "fz_mixed", "fz_wide", "fz_rich", "fz_one"] {
            assert!(sql.iter().any(|s| s.contains(t)), "no seed rows for {t}");
        }
        // fz_empty stays empty; fz_one holds exactly one row.
        assert!(!sql.iter().any(|s| s.contains("fz_empty")));
        assert_eq!(sql.iter().filter(|s| s.contains("fz_one")).count(), 1);
        assert!(sql.iter().all(|s| s.starts_with("INSERT INTO ") && s.ends_with(';')));
        // Every INSERT lists exactly as many values as its table has
        // columns (top-level comma count check, no parens inside values
        // other than none — literals here are paren-free).
        let cat = FixtureCatalog.load_catalog().unwrap();
        for s in &sql {
            let table = cat
                .tables
                .iter()
                .find(|t| s.starts_with(&format!("INSERT INTO {} ", t.name)))
                .unwrap_or_else(|| panic!("unknown table in {s}"));
            let vals = s.split_once("VALUES ").unwrap().1;
            let ncommas = vals.matches(", ").count();
            assert_eq!(
                ncommas + 1,
                table.columns.len(),
                "arity mismatch for {}: {s}",
                table.name
            );
        }
    }

    #[test]
    fn seed_pks_are_dense_from_one() {
        // For each seeded table the pk column values are exactly
        // 1..=seeded_max in row order (the DmlState allocation floor).
        let cat = FixtureCatalog.load_catalog().unwrap();
        let sql = fixture_seed_sql();
        for t in &cat.tables {
            let pk = t.pk.as_ref().expect("every fixture table has a pk");
            // The pk column is the first column on every seeded table.
            assert_eq!(t.columns[0].name, pk.column, "{}: pk must lead", t.name);
            let rows: Vec<&String> = sql
                .iter()
                .filter(|s| s.starts_with(&format!("INSERT INTO {} ", t.name)))
                .collect();
            assert_eq!(rows.len() as i64, pk.seeded_max, "{}: seeded_max", t.name);
            for (i, r) in rows.iter().enumerate() {
                let first = r
                    .split_once("VALUES (")
                    .unwrap()
                    .1
                    .split(',')
                    .next()
                    .unwrap()
                    .trim();
                assert_eq!(first, (i + 1).to_string(), "{}: pk order in {r}", t.name);
            }
        }
    }

    #[test]
    fn fixture_covers_all_types_and_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        assert_eq!(cat.tables.len(), 6);
        for &ty in ALL_TYPES {
            assert!(
                cat.tables.iter().any(|t| !t.columns_of_type(ty).is_empty()),
                "no fixture column of type {:?}",
                ty
            );
        }
        // Join keys on every table; nullable somewhere (NULL join semantics).
        for t in &cat.tables {
            for key in ["k_int", "k_text"] {
                assert!(
                    t.columns.iter().any(|c| c.name == key),
                    "table {} missing join key {key}",
                    t.name
                );
            }
        }
        assert!(cat
            .tables
            .iter()
            .any(|t| t.columns.iter().any(|c| c.name == "k_int" && c.nullable)));
        // Shape spread: one wide table, single-row/empty tables flagged.
        assert!(cat.tables.iter().any(|t| t.columns.len() >= 20));
        assert_eq!(
            cat.tables.iter().filter(|t| t.at_most_one_row).count(),
            2,
            "fz_empty and fz_one carry the at-most-one-row hint"
        );
        assert!(cat.tables.iter().any(|t| t.columns.iter().any(|c| c.nullable)));
        assert!(cat.tables.iter().any(|t| t.columns.iter().any(|c| !c.nullable)));
        // Every table has an int4 NOT NULL pk column; exactly the three
        // multi-row tables are DML targets.
        for t in &cat.tables {
            let pk = t.pk.as_ref().unwrap_or_else(|| panic!("{}: no pk", t.name));
            let c = t
                .columns
                .iter()
                .find(|c| c.name == pk.column)
                .unwrap_or_else(|| panic!("{}: pk column missing", t.name));
            assert_eq!(c.ty, SqlType::Int4, "{}: pk must be int4", t.name);
            assert!(!c.nullable, "{}: pk must be NOT NULL", t.name);
        }
        assert_eq!(
            cat.tables.iter().filter(|t| t.pk.is_some() && !t.at_most_one_row).count(),
            4,
            "fz_scalar, fz_mixed, fz_wide, fz_rich are the DML targets"
        );
    }

    #[test]
    fn fixture_ddl_renders() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let ddl = fixture_ddl(&cat);
        assert!(ddl.contains("CREATE TABLE fz_scalar"));
        assert!(ddl.contains("CREATE TABLE fz_empty"));
        assert!(ddl.contains("c_int4 int4 NOT NULL"));
        assert!(ddl.contains("pk int4 PRIMARY KEY"));
        assert!(ddl.contains("id int4 PRIMARY KEY"));
        assert!(ddl.contains("k_int int4 PRIMARY KEY"));
        // Typmod overrides render in DDL; expressions still see numeric.
        assert!(ddl.contains("r_num52 numeric(5,2)"));
        assert!(ddl.contains("r_textarr text[]"));
        // No defaults anywhere: generated DEFAULT must mean NULL, and
        // nothing may diverge per-run (no serial/now()).
        assert!(!ddl.to_ascii_lowercase().contains("default"));
        assert!(!ddl.to_ascii_lowercase().contains("serial"));
    }
}
