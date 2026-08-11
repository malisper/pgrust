//! Scalar-expression feature module: typed random expression generation
//! over a catalog table. Every production that fires is recorded by name —
//! the generator-side self-coverage metadata the gap loop consumes.
//!
//! Typing discipline: expressions are type-correct by construction, so
//! every rendered statement must parse and analyze; only execution-time
//! (semantic) errors such as division by zero or overflow are possible.

use crate::catalog::{SqlType, Table, ALL_TYPES};
use crate::rng::Rng;

#[derive(Clone, Debug)]
pub enum Expr {
    ColRef { name: String },
    /// Pre-rendered literal text, self-delimiting (parenthesized/cast).
    Lit { sql: String },
    Null { ty: SqlType },
    Unary { op: &'static str, arg: Box<Expr> },
    Binary { op: &'static str, lhs: Box<Expr>, rhs: Box<Expr> },
    Func { name: &'static str, args: Vec<Expr> },
    Case { cond: Box<Expr>, then_e: Box<Expr>, else_e: Box<Expr> },
    Cast { arg: Box<Expr>, to: SqlType },
    IsNull { arg: Box<Expr>, negated: bool },
}

impl Expr {
    pub fn render(&self, out: &mut String) {
        match self {
            Expr::ColRef { name } => out.push_str(name),
            Expr::Lit { sql } => out.push_str(sql),
            Expr::Null { ty } => {
                out.push_str("(NULL::");
                out.push_str(ty.name());
                out.push(')');
            }
            Expr::Unary { op, arg } => {
                out.push('(');
                out.push_str(op);
                out.push(' ');
                arg.render(out);
                out.push(')');
            }
            Expr::Binary { op, lhs, rhs } => {
                out.push('(');
                lhs.render(out);
                out.push(' ');
                out.push_str(op);
                out.push(' ');
                rhs.render(out);
                out.push(')');
            }
            Expr::Func { name, args } => {
                out.push_str(name);
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.render(out);
                }
                out.push(')');
            }
            Expr::Case { cond, then_e, else_e } => {
                out.push_str("(CASE WHEN ");
                cond.render(out);
                out.push_str(" THEN ");
                then_e.render(out);
                out.push_str(" ELSE ");
                else_e.render(out);
                out.push_str(" END)");
            }
            Expr::Cast { arg, to } => {
                out.push('(');
                arg.render(out);
                out.push_str("::");
                out.push_str(to.name());
                out.push(')');
            }
            Expr::IsNull { arg, negated } => {
                out.push('(');
                arg.render(out);
                out.push_str(if *negated { " IS NOT NULL" } else { " IS NULL" });
                out.push(')');
            }
        }
    }

    pub fn to_sql(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }
}

/// Generation context for one statement: draws from the session PRNG and
/// records fired productions.
pub struct ExprGen<'a> {
    pub rng: &'a mut Rng,
    pub table: &'a Table,
    pub productions: &'a mut Vec<String>,
}

impl<'a> ExprGen<'a> {
    fn fire(&mut self, prod: &str) {
        self.productions.push(prod.to_string());
    }

    fn fire2(&mut self, prefix: &str, suffix: &str) {
        self.productions.push(format!("{}{}", prefix, suffix));
    }

    /// Random type that has at least one column in the table (all fixture
    /// types qualify for fz_scalar; guards against thinner tables).
    pub fn any_type(&mut self) -> SqlType {
        let avail: Vec<SqlType> = ALL_TYPES
            .iter()
            .copied()
            .filter(|&t| !self.table.columns_of_type(t).is_empty())
            .collect();
        if avail.is_empty() {
            *self.rng.pick(ALL_TYPES)
        } else {
            *self.rng.pick(&avail)
        }
    }

    pub fn gen_typed(&mut self, ty: SqlType, depth: u32) -> Expr {
        if depth == 0 || self.rng.chance(2, 5) {
            return self.gen_leaf(ty);
        }
        if ty == SqlType::Bool {
            return self.gen_bool(depth);
        }
        // Composite productions available for this type.
        let mut options: Vec<&'static str> = vec!["case", "cast", "coalesce", "nullif"];
        if ty.is_numeric_family() {
            options.push("arith");
            options.push("neg");
            options.push("func_num");
        }
        if ty == SqlType::Text {
            options.push("concat");
            options.push("func_text");
        }
        if ty == SqlType::Varchar {
            options.push("func_varchar");
        }
        if ty == SqlType::Int4 {
            options.push("length");
        }
        if ty == SqlType::Date {
            options.push("date_plus_int");
        }
        if ty == SqlType::Timestamp {
            options.push("date_trunc");
        }
        match *self.rng.pick(&options) {
            "case" => {
                self.fire("case");
                let cond = self.gen_bool(depth - 1);
                let then_e = self.gen_typed(ty, depth - 1);
                let else_e = self.gen_typed(ty, depth - 1);
                Expr::Case {
                    cond: Box::new(cond),
                    then_e: Box::new(then_e),
                    else_e: Box::new(else_e),
                }
            }
            "cast" => {
                let sources: Vec<SqlType> = ALL_TYPES
                    .iter()
                    .copied()
                    .filter(|s| s.cast_targets().contains(&ty))
                    .collect();
                let src = *self.rng.pick(&sources);
                self.fire2("cast:", &format!("{}->{}", src.name(), ty.name()));
                let arg = self.gen_typed(src, depth - 1);
                Expr::Cast { arg: Box::new(arg), to: ty }
            }
            "coalesce" => {
                self.fire("func:coalesce");
                let a = self.gen_typed(ty, depth - 1);
                let b = self.gen_typed(ty, depth - 1);
                Expr::Func { name: "coalesce", args: vec![a, b] }
            }
            "nullif" => {
                self.fire("func:nullif");
                let a = self.gen_typed(ty, depth - 1);
                let b = self.gen_typed(ty, depth - 1);
                Expr::Func { name: "nullif", args: vec![a, b] }
            }
            "arith" => {
                // Same-type operands keep the result type stable.
                let op = if ty.is_float() {
                    *self.rng.pick(&["+", "-", "*", "/"])
                } else {
                    *self.rng.pick(&["+", "-", "*", "/", "%"])
                };
                self.fire2("binop:", op);
                let lhs = self.gen_typed(ty, depth - 1);
                let rhs = self.gen_typed(ty, depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "neg" => {
                self.fire("unop:-");
                let arg = self.gen_typed(ty, depth - 1);
                Expr::Unary { op: "-", arg: Box::new(arg) }
            }
            "func_num" => {
                let name = match ty {
                    SqlType::Float8 => *self.rng.pick(&["abs", "ceil", "floor", "round", "sqrt"]),
                    SqlType::Numeric => *self.rng.pick(&["abs", "ceil", "floor", "round"]),
                    _ => "abs",
                };
                self.fire2("func:", name);
                let arg = self.gen_typed(ty, depth - 1);
                Expr::Func { name, args: vec![arg] }
            }
            "concat" => {
                self.fire("binop:||");
                let lt = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let rt = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let lhs = self.gen_typed(lt, depth - 1);
                let rhs = self.gen_typed(rt, depth - 1);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "func_text" => {
                let name = *self.rng.pick(&["lower", "upper", "btrim"]);
                self.fire2("func:", name);
                let arg = self.gen_typed(SqlType::Text, depth - 1);
                Expr::Func { name, args: vec![arg] }
            }
            "func_varchar" => {
                // substr(varchar, int) keeps the varchar result type.
                self.fire("func:substr");
                let arg = self.gen_typed(SqlType::Varchar, depth - 1);
                let start = self.gen_typed(SqlType::Int4, depth - 1);
                Expr::Func { name: "substr", args: vec![arg, start] }
            }
            "length" => {
                self.fire("func:length");
                let src = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let arg = self.gen_typed(src, depth - 1);
                Expr::Func { name: "length", args: vec![arg] }
            }
            "date_plus_int" => {
                self.fire("binop:date+int");
                let lhs = self.gen_typed(SqlType::Date, depth - 1);
                let rhs = self.gen_typed(SqlType::Int4, depth - 1);
                Expr::Binary { op: "+", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "date_trunc" => {
                self.fire("func:date_trunc");
                let unit = *self.rng.pick(&["day", "month", "year", "hour"]);
                let arg = self.gen_typed(SqlType::Timestamp, depth - 1);
                Expr::Func {
                    name: "date_trunc",
                    args: vec![Expr::Lit { sql: format!("'{}'", unit) }, arg],
                }
            }
            other => unreachable!("unknown production {}", other),
        }
    }

    /// Boolean expressions get their own grammar: comparisons, connectives,
    /// IS [NOT] NULL, plus the generic leaf/CASE/COALESCE machinery.
    pub fn gen_bool(&mut self, depth: u32) -> Expr {
        if depth == 0 || self.rng.chance(1, 4) {
            return self.gen_leaf(SqlType::Bool);
        }
        match *self.rng.pick(&["cmp", "cmp", "and_or", "not", "isnull", "case", "coalesce"]) {
            "cmp" => {
                let ty = self.any_type();
                let op = *self.rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
                self.fire2("cmp:", op);
                // Same type on both sides; comparable_with allows widening
                // later without changing recorded productions.
                debug_assert!(ty.comparable_with(ty));
                let lhs = self.gen_typed(ty, depth - 1);
                let rhs = self.gen_typed(ty, depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "and_or" => {
                let op = *self.rng.pick(&["AND", "OR"]);
                self.fire2("binop:", if op == "AND" { "and" } else { "or" });
                let lhs = self.gen_bool(depth - 1);
                let rhs = self.gen_bool(depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "not" => {
                self.fire("unop:not");
                let arg = self.gen_bool(depth - 1);
                Expr::Unary { op: "NOT", arg: Box::new(arg) }
            }
            "isnull" => {
                let negated = self.rng.chance(1, 2);
                self.fire(if negated { "isnotnull" } else { "isnull" });
                let ty = self.any_type();
                let arg = self.gen_typed(ty, depth - 1);
                Expr::IsNull { arg: Box::new(arg), negated }
            }
            "case" => {
                self.fire("case");
                let cond = self.gen_bool(depth - 1);
                let then_e = self.gen_bool(depth - 1);
                let else_e = self.gen_bool(depth - 1);
                Expr::Case {
                    cond: Box::new(cond),
                    then_e: Box::new(then_e),
                    else_e: Box::new(else_e),
                }
            }
            "coalesce" => {
                self.fire("func:coalesce");
                let a = self.gen_bool(depth - 1);
                let b = self.gen_bool(depth - 1);
                Expr::Func { name: "coalesce", args: vec![a, b] }
            }
            other => unreachable!("unknown production {}", other),
        }
    }

    fn gen_leaf(&mut self, ty: SqlType) -> Expr {
        let cols = self.table.columns_of_type(ty);
        // colref : literal : NULL at roughly 5 : 4 : 1.
        let roll = self.rng.below(10);
        if roll < 5 && !cols.is_empty() {
            self.fire("colref");
            let c = *self.rng.pick(&cols);
            return Expr::ColRef { name: c.name.clone() };
        }
        if roll == 9 {
            self.fire2("null:", ty.name());
            return Expr::Null { ty };
        }
        self.fire2("lit:", ty.name());
        Expr::Lit { sql: self.gen_literal(ty) }
    }

    /// Literal text, self-delimiting and pinned to `ty` (cast forms), with
    /// boundary values well represented. Random spellings are built from
    /// integer draws only so the text is platform-deterministic.
    fn gen_literal(&mut self, ty: SqlType) -> String {
        match ty {
            SqlType::Int2 => match self.rng.below(6) {
                0 => "(0)::int2".to_string(),
                1 => "(1)::int2".to_string(),
                2 => "(-1)::int2".to_string(),
                3 => "(32767)::int2".to_string(),
                4 => "(-32768)::int2".to_string(),
                _ => format!("({})::int2", self.rng.range_i64(-100, 100)),
            },
            SqlType::Int4 => match self.rng.below(6) {
                0 => "0".to_string(),
                1 => "1".to_string(),
                2 => "(-1)".to_string(),
                3 => "(2147483647)::int4".to_string(),
                4 => "(-2147483648)::int4".to_string(),
                _ => format!("{}", self.rng.range_i64(0, 1000)),
            },
            SqlType::Int8 => match self.rng.below(6) {
                0 => "(0)::int8".to_string(),
                1 => "(-1)::int8".to_string(),
                2 => "(9223372036854775807)::int8".to_string(),
                3 => "(-9223372036854775808)::int8".to_string(),
                4 => "(4294967296)::int8".to_string(),
                _ => format!("({})::int8", self.rng.range_i64(-100000, 100000)),
            },
            SqlType::Float4 => match self.rng.below(8) {
                0 => "(0)::float4".to_string(),
                1 => "('-0')::float4".to_string(),
                2 => "('NaN')::float4".to_string(),
                3 => "('Infinity')::float4".to_string(),
                4 => "('-Infinity')::float4".to_string(),
                5 => "('3.4028235e38')::float4".to_string(),
                6 => "('1.1754944e-38')::float4".to_string(),
                _ => format!(
                    "({}.{:02})::float4",
                    self.rng.range_i64(-99, 99),
                    self.rng.below(100)
                ),
            },
            SqlType::Float8 => match self.rng.below(8) {
                0 => "(0)::float8".to_string(),
                1 => "('-0')::float8".to_string(),
                2 => "('NaN')::float8".to_string(),
                3 => "('Infinity')::float8".to_string(),
                4 => "('-Infinity')::float8".to_string(),
                5 => "('1.7976931348623157e308')::float8".to_string(),
                6 => "('5e-324')::float8".to_string(),
                _ => format!(
                    "({}.{:02})::float8",
                    self.rng.range_i64(-99, 99),
                    self.rng.below(100)
                ),
            },
            SqlType::Numeric => match self.rng.below(6) {
                0 => "(0)::numeric".to_string(),
                1 => "(-1)::numeric".to_string(),
                2 => "('123456789012345678901234567890.123456789')::numeric".to_string(),
                3 => "('0.000000001')::numeric".to_string(),
                4 => "('NaN')::numeric".to_string(),
                _ => format!(
                    "({}.{})::numeric",
                    self.rng.range_i64(-9999, 9999),
                    self.rng.below(1000)
                ),
            },
            SqlType::Text | SqlType::Varchar => {
                let body = match self.rng.below(5) {
                    0 => String::new(),
                    1 => "a".to_string(),
                    2 => "foo''bar".to_string(),
                    3 => "  pad  ".to_string(),
                    _ => {
                        let len = self.rng.below(8);
                        (0..len)
                            .map(|_| (b'a' + self.rng.below(26) as u8) as char)
                            .collect()
                    }
                };
                format!("('{}')::{}", body, ty.name())
            }
            SqlType::Bool => {
                if self.rng.chance(1, 2) {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            SqlType::Date => match self.rng.below(6) {
                0 => "DATE '2000-01-01'".to_string(),
                1 => "DATE '1970-01-01'".to_string(),
                2 => "DATE '0001-01-01'".to_string(),
                3 => "DATE '9999-12-31'".to_string(),
                4 => "('infinity')::date".to_string(),
                _ => format!(
                    "DATE '{:04}-{:02}-{:02}'",
                    1900 + self.rng.below(200),
                    1 + self.rng.below(12),
                    1 + self.rng.below(28)
                ),
            },
            SqlType::Timestamp => match self.rng.below(5) {
                0 => "TIMESTAMP '2000-01-01 00:00:00'".to_string(),
                1 => "TIMESTAMP '1970-01-01 23:59:59'".to_string(),
                2 => "('infinity')::timestamp".to_string(),
                3 => "('-infinity')::timestamp".to_string(),
                _ => format!(
                    "TIMESTAMP '{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                    1900 + self.rng.below(200),
                    1 + self.rng.below(12),
                    1 + self.rng.below(28),
                    self.rng.below(24),
                    self.rng.below(60),
                    self.rng.below(60)
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    fn table() -> crate::catalog::Table {
        FixtureCatalog.load_catalog().unwrap().tables[0].clone()
    }

    #[test]
    fn generation_is_deterministic() {
        let t = table();
        let run = |seed: u64| {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = ExprGen { rng: &mut rng, table: &t, productions: &mut prods };
            let e = g.gen_typed(SqlType::Int8, 5);
            (e.to_sql(), prods)
        };
        assert_eq!(run(11), run(11));
        assert_ne!(run(11).0, run(12).0);
    }

    #[test]
    fn productions_are_recorded() {
        let t = table();
        let mut rng = Rng::new(3);
        let mut prods = Vec::new();
        let mut g = ExprGen { rng: &mut rng, table: &t, productions: &mut prods };
        let e = g.gen_typed(SqlType::Float8, 6);
        assert!(!prods.is_empty());
        assert!(!e.to_sql().is_empty());
        // Every recorded production has a known prefix.
        for p in &prods {
            let known = ["colref", "lit:", "null:", "unop:", "binop:", "cmp:", "func:", "case", "cast:", "isnull", "isnotnull"];
            assert!(known.iter().any(|k| p.starts_with(k)), "unknown production {}", p);
        }
    }

    #[test]
    fn bool_exprs_render() {
        let t = table();
        let mut rng = Rng::new(5);
        for _ in 0..50 {
            let mut prods = Vec::new();
            let mut g = ExprGen { rng: &mut rng, table: &t, productions: &mut prods };
            let e = g.gen_bool(4);
            let sql = e.to_sql();
            assert!(!sql.is_empty());
            // Balanced parens as a cheap render-sanity check.
            let opens = sql.matches('(').count();
            let closes = sql.matches(')').count();
            assert_eq!(opens, closes, "unbalanced: {}", sql);
        }
    }
}
