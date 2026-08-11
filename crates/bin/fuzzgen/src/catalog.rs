//! Minimal typed catalog snapshot. F0 ships the built-in fixture schema;
//! `CatalogSource` is the seam a live-server introspection loader slots
//! into at F1.

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
        }
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
    /// explicit cast.
    pub fn comparable_with(self, other: SqlType) -> bool {
        (self.is_numeric_family() && other.is_numeric_family())
            || (self.is_text_family() && other.is_text_family())
            || (self == SqlType::Bool && other == SqlType::Bool)
            || (self.is_datetime() && other.is_datetime())
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
            SqlType::Timestamp => &[SqlType::Date, SqlType::Timestamp, SqlType::Text],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn columns_of_type(&self, ty: SqlType) -> Vec<&Column> {
        self.columns.iter().filter(|c| c.ty == ty).collect()
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
    Column { name: name.to_string(), ty, nullable }
}

impl CatalogSource for FixtureCatalog {
    fn load_catalog(&self) -> Result<Catalog, String> {
        Ok(Catalog {
            tables: vec![
                Table {
                    name: "fz_scalar".to_string(),
                    columns: vec![
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
                    columns: vec![
                        col("id", SqlType::Int4, false),
                        col("val", SqlType::Float8, true),
                        col("amount", SqlType::Numeric, true),
                        col("tag", SqlType::Text, true),
                        col("flag", SqlType::Bool, true),
                        col("created", SqlType::Date, true),
                    ],
                },
            ],
        })
    }
}

/// DDL for the fixture schema, for standing up a smoke target.
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
            out.push_str(c.ty.name());
            if !c.nullable {
                out.push_str(" NOT NULL");
            }
        }
        out.push_str(");\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_covers_all_types() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        for &ty in ALL_TYPES {
            assert!(
                cat.tables.iter().any(|t| !t.columns_of_type(ty).is_empty()),
                "no fixture column of type {:?}",
                ty
            );
        }
        assert!(cat
            .tables
            .iter()
            .any(|t| t.columns.iter().any(|c| c.nullable)));
        assert!(cat
            .tables
            .iter()
            .any(|t| t.columns.iter().any(|c| !c.nullable)));
    }

    #[test]
    fn fixture_ddl_renders() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let ddl = fixture_ddl(&cat);
        assert!(ddl.contains("CREATE TABLE fz_scalar"));
        assert!(ddl.contains("c_int4 int4 NOT NULL"));
    }
}
