//! Scalar-expression feature module: typed random expression generation
//! over a lexical scope (crate::scope). Every production that fires is
//! recorded by name — the generator-side self-coverage metadata the gap
//! loop consumes. Column references draw from the in-scope aliases (never
//! the raw catalog), so expressions stay valid under joins, derived tables
//! and correlated subqueries.
//!
//! Typing discipline: expressions are type-correct by construction, so
//! every rendered statement must parse and analyze; only execution-time
//! (semantic) errors such as division by zero or overflow are possible.

use crate::catalog::{SqlType, ALL_TYPES};
use crate::render::{SelectStmt, WindowOver};
use crate::scope::Scope;
use crate::stmt::Gen;
use crate::subq::{gen_cmp_subq, gen_exists_subq, gen_in_subq, gen_scalar_subq};

#[derive(Clone, Debug)]
pub enum Expr {
    /// Alias-qualified column reference (aliases are statement-unique).
    ColRef { alias: String, name: String },
    /// Pre-rendered literal text, self-delimiting (parenthesized/cast).
    Lit { sql: String },
    Null { ty: SqlType },
    Unary { op: &'static str, arg: Box<Expr> },
    Binary { op: &'static str, lhs: Box<Expr>, rhs: Box<Expr> },
    Func { name: &'static str, args: Vec<Expr> },
    Case { cond: Box<Expr>, then_e: Box<Expr>, else_e: Box<Expr> },
    Cast { arg: Box<Expr>, to: SqlType },
    IsNull { arg: Box<Expr>, negated: bool },
    /// Scalar subquery: `(SELECT one-item ...)`, at most one row by
    /// construction (single-row/empty FROM source, or no FROM at all).
    ScalarSubq { body: Box<SelectStmt> },
    InSubq { lhs: Box<Expr>, negated: bool, body: Box<SelectStmt> },
    Exists { negated: bool, body: Box<SelectStmt> },
    /// Aggregate call (agg module): COUNT(*) via `star`; DISTINCT;
    /// ordered-input aggregates carry a mandatory inner ORDER BY — always
    /// their own first argument (crate::agg discipline) — and any
    /// aggregate may carry FILTER (WHERE ...).
    Agg {
        name: &'static str,
        star: bool,
        distinct: bool,
        args: Vec<Expr>,
        order_within: Option<(Box<Expr>, bool)>,
        filter: Option<Box<Expr>>,
    },
    /// Window function call over an inline or named window (win module).
    WindowFunc { name: &'static str, args: Vec<Expr>, over: WindowOver },
    /// Generic SQL wrapper for syntax `Func` can't spell (EXTRACT's
    /// `field FROM arg`, array subscripts/slices). `pre`/`post` carry
    /// LITERAL TEXT ONLY — never column references (the scoping checker
    /// walks `arg` but cannot see into the strings) — and must keep the
    /// combined `pre`+`post` paren-balanced.
    Wrap { pre: String, arg: Box<Expr>, post: String },
}

impl Expr {
    pub fn render(&self, out: &mut String) {
        match self {
            Expr::ColRef { alias, name } => {
                out.push_str(alias);
                out.push('.');
                out.push_str(name);
            }
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
            Expr::ScalarSubq { body } => {
                out.push('(');
                body.render(out);
                out.push(')');
            }
            Expr::InSubq { lhs, negated, body } => {
                out.push('(');
                lhs.render(out);
                out.push_str(if *negated { " NOT IN (" } else { " IN (" });
                body.render(out);
                out.push_str("))");
            }
            Expr::Exists { negated, body } => {
                out.push('(');
                out.push_str(if *negated { "NOT EXISTS (" } else { "EXISTS (" });
                body.render(out);
                out.push_str("))");
            }
            Expr::Agg { name, star, distinct, args, order_within, filter } => {
                out.push_str(name);
                out.push('(');
                if *star {
                    out.push('*');
                } else {
                    if *distinct {
                        out.push_str("DISTINCT ");
                    }
                    for (i, a) in args.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        a.render(out);
                    }
                    if let Some((oe, desc)) = order_within {
                        out.push_str(" ORDER BY ");
                        oe.render(out);
                        if *desc {
                            out.push_str(" DESC");
                        }
                    }
                }
                out.push(')');
                if let Some(f) = filter {
                    out.push_str(" FILTER (WHERE ");
                    f.render(out);
                    out.push(')');
                }
            }
            Expr::Wrap { pre, arg, post } => {
                out.push_str(pre);
                arg.render(out);
                out.push_str(post);
            }
            Expr::WindowFunc { name, args, over } => {
                out.push_str(name);
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.render(out);
                }
                out.push_str(") OVER ");
                match over {
                    WindowOver::Named(n) => out.push_str(n),
                    WindowOver::Inline(d) => {
                        out.push('(');
                        d.render(out);
                        out.push(')');
                    }
                }
            }
        }
    }

    pub fn to_sql(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }

    /// Direct expression children (subquery bodies excluded): the shared
    /// walker for AST-level invariant checks.
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::ColRef { .. }
            | Expr::Lit { .. }
            | Expr::Null { .. }
            | Expr::ScalarSubq { .. }
            | Expr::Exists { .. } => Vec::new(),
            Expr::Unary { arg, .. }
            | Expr::Cast { arg, .. }
            | Expr::IsNull { arg, .. }
            | Expr::Wrap { arg, .. } => {
                vec![arg]
            }
            Expr::Binary { lhs, rhs, .. } => vec![lhs, rhs],
            Expr::Func { args, .. } => args.iter().collect(),
            Expr::Case { cond, then_e, else_e } => vec![cond, then_e, else_e],
            Expr::InSubq { lhs, .. } => vec![lhs],
            Expr::Agg { args, order_within, filter, .. } => {
                let mut out: Vec<&Expr> = args.iter().collect();
                if let Some((oe, _)) = order_within {
                    out.push(oe);
                }
                if let Some(f) = filter {
                    out.push(f);
                }
                out
            }
            Expr::WindowFunc { args, over, .. } => {
                let mut out: Vec<&Expr> = args.iter().collect();
                if let WindowOver::Inline(d) = over {
                    out.extend(d.partition_by.iter());
                    out.extend(d.order_by.iter().map(|k| &k.expr));
                }
                out
            }
        }
    }
}

impl Gen<'_> {
    /// Random type that has at least one column in scope (guards against
    /// thinner relations; falls back to any type when the scope is bare).
    pub fn any_type(&mut self, scope: &Scope) -> SqlType {
        let avail: Vec<SqlType> =
            ALL_TYPES.iter().copied().filter(|&t| scope.has_type(t)).collect();
        if avail.is_empty() {
            *self.rng.pick(ALL_TYPES)
        } else {
            *self.rng.pick(&avail)
        }
    }

    pub fn gen_typed(&mut self, scope: &Scope, ty: SqlType, depth: u32) -> Expr {
        if depth == 0 || self.rng.chance(2, 5) {
            return self.gen_leaf(scope, ty);
        }
        if ty == SqlType::Bool {
            return self.gen_bool(scope, depth);
        }
        if ty == SqlType::Json {
            // json (not jsonb) has no equality/ordering, so it stays out of
            // the generic machinery; as an expression target (DML writes to
            // the fixture's json column) it is leaf-only.
            return self.gen_leaf(scope, ty);
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
            options.push("fmt:to_char_num");
            options.push("fmt:to_char_dt");
            options.push("jsonb:arrow_text");
            options.push("func:encode");
            options.push("ts:vector_text");
            options.push("ts:query_text");
            options.push("ts:headline");
            options.push("func:regexp_replace");
        }
        if ty == SqlType::Varchar {
            options.push("func_varchar");
        }
        if ty == SqlType::Int4 {
            options.push("length");
            options.push("arr:int");
        }
        if ty == SqlType::Date {
            options.push("date_plus_int");
            options.push("fmt:to_date_rt");
        }
        if ty == SqlType::Timestamp {
            options.push("date_trunc");
            options.push("binop:ts+interval");
            options.push("fmt:to_timestamp_rt");
        }
        if ty == SqlType::Numeric {
            options.push("fmt:extract");
            options.push("fmt:to_number_rt");
        }
        if ty == SqlType::Float8 {
            options.push("func:date_part");
        }
        if ty == SqlType::Float4 {
            options.push("ts:rank");
        }
        if ty == SqlType::Jsonb {
            options.push("jsonb:composite");
        }
        if ty == SqlType::Bytea {
            options.push("bytea:composite");
        }
        if ty == SqlType::Interval {
            options.push("interval:composite");
        }
        if ty == SqlType::Time {
            options.push("binop:time+interval");
        }
        if ty == SqlType::TextArr {
            options.push("arr:text_composite");
        }
        if ty == SqlType::Int4Arr {
            options.push("arr:int_composite");
        }
        if self.subq_depth > 0 {
            options.push("subq:scalar");
        }
        match self.weights.pick(self.rng, &options) {
            "case" => {
                self.fire("case");
                let cond = self.gen_bool(scope, depth - 1);
                let then_e = self.gen_typed(scope, ty, depth - 1);
                let else_e = self.gen_typed(scope, ty, depth - 1);
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
                let arg = self.gen_typed(scope, src, depth - 1);
                Expr::Cast { arg: Box::new(arg), to: ty }
            }
            "coalesce" => {
                self.fire("func:coalesce");
                let a = self.gen_typed(scope, ty, depth - 1);
                let b = self.gen_typed(scope, ty, depth - 1);
                Expr::Func { name: "coalesce", args: vec![a, b] }
            }
            "nullif" => {
                self.fire("func:nullif");
                let a = self.gen_typed(scope, ty, depth - 1);
                let b = self.gen_typed(scope, ty, depth - 1);
                Expr::Func { name: "nullif", args: vec![a, b] }
            }
            "arith" => {
                // Same-type operands keep the result type stable.
                let picked = if ty.is_float() {
                    self.weights.pick(self.rng, &["binop:+", "binop:-", "binop:*", "binop:/"])
                } else {
                    self.weights.pick(
                        self.rng,
                        &["binop:+", "binop:-", "binop:*", "binop:/", "binop:%"],
                    )
                };
                let op = &picked["binop:".len()..];
                self.fire2("binop:", op);
                let lhs = self.gen_typed(scope, ty, depth - 1);
                let rhs = self.gen_typed(scope, ty, depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "neg" => {
                self.fire("unop:-");
                let arg = self.gen_typed(scope, ty, depth - 1);
                Expr::Unary { op: "-", arg: Box::new(arg) }
            }
            "func_num" => {
                let picked = match ty {
                    SqlType::Float8 | SqlType::Numeric => self.weights.pick(
                        self.rng,
                        &["func:abs", "func:ceil", "func:floor", "func:round", "func:sqrt"],
                    ),
                    _ => "func:abs",
                };
                let name = &picked["func:".len()..];
                self.fire2("func:", name);
                let arg = self.gen_typed(scope, ty, depth - 1);
                Expr::Func { name, args: vec![arg] }
            }
            "concat" => {
                self.fire("binop:||");
                let lt = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let rt = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let lhs = self.gen_typed(scope, lt, depth - 1);
                let rhs = self.gen_typed(scope, rt, depth - 1);
                Expr::Binary { op: "||", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "func_text" => {
                let picked =
                    self.weights.pick(self.rng, &["func:lower", "func:upper", "func:btrim"]);
                let name = &picked["func:".len()..];
                self.fire2("func:", name);
                let arg = self.gen_typed(scope, SqlType::Text, depth - 1);
                Expr::Func { name, args: vec![arg] }
            }
            "func_varchar" => {
                // substr(varchar, int) keeps the varchar result type.
                self.fire("func:substr");
                let arg = self.gen_typed(scope, SqlType::Varchar, depth - 1);
                let start = self.gen_typed(scope, SqlType::Int4, depth - 1);
                Expr::Func { name: "substr", args: vec![arg, start] }
            }
            "length" => {
                self.fire("func:length");
                let src = *self.rng.pick(&[SqlType::Text, SqlType::Varchar]);
                let arg = self.gen_typed(scope, src, depth - 1);
                Expr::Func { name: "length", args: vec![arg] }
            }
            "date_plus_int" => {
                self.fire("binop:date+int");
                let lhs = self.gen_typed(scope, SqlType::Date, depth - 1);
                let rhs = self.gen_typed(scope, SqlType::Int4, depth - 1);
                Expr::Binary { op: "+", lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "date_trunc" => {
                self.fire("func:date_trunc");
                let unit = *self.rng.pick(&["day", "month", "year", "hour"]);
                let arg = self.gen_typed(scope, SqlType::Timestamp, depth - 1);
                Expr::Func {
                    name: "date_trunc",
                    args: vec![Expr::Lit { sql: format!("'{}'", unit) }, arg],
                }
            }
            "subq:scalar" => gen_scalar_subq(self, scope, ty),
            // T1 rich productions (crate::rich).
            "fmt:to_char_num" => self.gen_to_char_num(scope, depth),
            "fmt:to_char_dt" => self.gen_to_char_dt(scope, depth),
            "jsonb:arrow_text" => self.gen_jsonb_arrow_text(scope, depth),
            "func:encode" => self.gen_encode(scope, depth),
            "ts:vector_text" => self.gen_tsvector_text(scope, depth),
            "ts:query_text" => self.gen_tsquery_text(scope, depth),
            "ts:headline" => self.gen_ts_headline(scope, depth),
            "func:regexp_replace" => self.gen_regexp_replace(scope, depth),
            "arr:int" => self.gen_array_int(scope, depth),
            "fmt:to_date_rt" => self.gen_to_date_rt(scope, depth),
            "binop:ts+interval" => self.gen_ts_plus_interval(scope, depth),
            "fmt:to_timestamp_rt" => self.gen_to_timestamp_rt(scope, depth),
            "fmt:extract" => self.gen_extract(scope, depth),
            "fmt:to_number_rt" => self.gen_to_number_rt(scope, depth),
            "func:date_part" => self.gen_date_part(scope, depth),
            "ts:rank" => self.gen_ts_rank(scope, depth),
            "jsonb:composite" => self.gen_jsonb_composite(scope, depth),
            "bytea:composite" => self.gen_bytea_composite(scope, depth),
            "interval:composite" => self.gen_interval_composite(scope, depth),
            "binop:time+interval" => self.gen_time_plus_interval(scope, depth),
            "arr:text_composite" => self.gen_textarr_composite(scope, depth),
            "arr:int_composite" => self.gen_intarr_composite(scope, depth),
            other => unreachable!("unknown production {}", other),
        }
    }

    /// Boolean expressions get their own grammar: comparisons, connectives,
    /// IS [NOT] NULL, subquery predicates, plus the generic leaf/CASE/
    /// COALESCE machinery.
    pub fn gen_bool(&mut self, scope: &Scope, depth: u32) -> Expr {
        if depth == 0 || self.rng.chance(1, 4) {
            return self.gen_leaf(scope, SqlType::Bool);
        }
        let mut options: Vec<&'static str> = vec![
            "cmp", "and_or", "not", "isnull", "case", "coalesce", "ts:match",
            "jsonb:bool", "arr:any_all",
        ];
        if self.subq_depth > 0 {
            options.push("subq:cmp");
            options.push("subq:in");
            options.push("subq:exists");
        }
        match self.weights.pick(self.rng, &options) {
            "cmp" => {
                let ty = self.any_type(scope);
                let op = self.pick_cmp_op();
                // Same type on both sides; comparable_with allows widening
                // later without changing recorded productions.
                debug_assert!(ty.comparable_with(ty));
                let lhs = self.gen_typed(scope, ty, depth - 1);
                let rhs = self.gen_typed(scope, ty, depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "and_or" => {
                let picked = self.weights.pick(self.rng, &["binop:and", "binop:or"]);
                let op = if picked == "binop:and" { "AND" } else { "OR" };
                self.fire2("binop:", if op == "AND" { "and" } else { "or" });
                let lhs = self.gen_bool(scope, depth - 1);
                let rhs = self.gen_bool(scope, depth - 1);
                Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
            }
            "not" => {
                self.fire("unop:not");
                let arg = self.gen_bool(scope, depth - 1);
                Expr::Unary { op: "NOT", arg: Box::new(arg) }
            }
            "isnull" => {
                let negated = self.rng.chance(1, 2);
                self.fire(if negated { "isnotnull" } else { "isnull" });
                let ty = self.any_type(scope);
                let arg = self.gen_typed(scope, ty, depth - 1);
                Expr::IsNull { arg: Box::new(arg), negated }
            }
            "case" => {
                self.fire("case");
                let cond = self.gen_bool(scope, depth - 1);
                let then_e = self.gen_bool(scope, depth - 1);
                let else_e = self.gen_bool(scope, depth - 1);
                Expr::Case {
                    cond: Box::new(cond),
                    then_e: Box::new(then_e),
                    else_e: Box::new(else_e),
                }
            }
            "coalesce" => {
                self.fire("func:coalesce");
                let a = self.gen_bool(scope, depth - 1);
                let b = self.gen_bool(scope, depth - 1);
                Expr::Func { name: "coalesce", args: vec![a, b] }
            }
            "subq:cmp" => gen_cmp_subq(self, scope),
            "subq:in" => gen_in_subq(self, scope),
            "subq:exists" => gen_exists_subq(self, scope),
            "ts:match" => self.gen_ts_match(scope, depth),
            "jsonb:bool" => self.gen_jsonb_bool(scope, depth),
            "arr:any_all" => self.gen_any_all(scope, depth),
            other => unreachable!("unknown production {}", other),
        }
    }

    /// Weighted comparison-operator pick (shared with the subquery module).
    pub fn pick_cmp_op(&mut self) -> &'static str {
        let picked = self
            .weights
            .pick(self.rng, &["cmp:=", "cmp:<>", "cmp:<", "cmp:<=", "cmp:>", "cmp:>="]);
        let op = &picked["cmp:".len()..];
        self.fire2("cmp:", op);
        op
    }

    fn gen_leaf(&mut self, scope: &Scope, ty: SqlType) -> Expr {
        let cols = scope.columns_of_type(ty);
        let outer_cols = scope.outer_columns_of_type(ty);
        // Weighted leaf-kind pick (defaults keep the historical 5:4:1
        // colref:literal:NULL ratio); colref forms are only offered when a
        // column of this type is actually visible.
        let mut options: Vec<&'static str> = Vec::with_capacity(4);
        if !cols.is_empty() {
            options.push("colref");
        }
        if !outer_cols.is_empty() {
            options.push("colref:outer");
        }
        options.push("lit");
        options.push("null");
        match self.weights.pick(self.rng, &options) {
            "colref" => {
                self.fire("colref");
                let (alias, c) = *self.rng.pick(&cols);
                Expr::ColRef { alias: alias.to_string(), name: c.name.clone() }
            }
            "colref:outer" => {
                self.fire("colref:outer");
                let (alias, c) = *self.rng.pick(&outer_cols);
                Expr::ColRef { alias: alias.to_string(), name: c.name.clone() }
            }
            "null" => {
                self.fire2("null:", ty.name());
                Expr::Null { ty }
            }
            _ => {
                self.fire2("lit:", ty.name());
                Expr::Lit { sql: self.gen_literal(ty) }
            }
        }
    }

    /// Literal text, self-delimiting and pinned to `ty` (cast forms), with
    /// boundary values well represented. Random spellings are built from
    /// integer draws only so the text is platform-deterministic.
    pub fn gen_literal(&mut self, ty: SqlType) -> String {
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
            SqlType::Date => match self.rng.below(12) {
                // A2 pool: epoch edges, leap days (century rule), BC dates,
                // the 5874897 AD extreme, ±infinity.
                0 => "DATE '2000-01-01'".to_string(),
                1 => "DATE '1970-01-01'".to_string(),
                2 => "DATE '0001-01-01'".to_string(),
                3 => "DATE '9999-12-31'".to_string(),
                4 => "('infinity')::date".to_string(),
                5 => "('-infinity')::date".to_string(),
                6 => "DATE '2000-02-29'".to_string(),
                7 => "DATE '1600-02-29'".to_string(),
                8 => "DATE '0001-12-31 BC'".to_string(),
                9 => "DATE '4714-11-24 BC'".to_string(),
                10 => "DATE '5874897-12-31'".to_string(),
                _ => format!(
                    "DATE '{:04}-{:02}-{:02}'",
                    1900 + self.rng.below(200),
                    1 + self.rng.below(12),
                    1 + self.rng.below(28)
                ),
            },
            SqlType::Timestamp => match self.rng.below(10) {
                // A2 pool: DST-transition instants (meaningful once shifted
                // through named zones), microsecond edges, range extremes.
                0 => "TIMESTAMP '2000-01-01 00:00:00'".to_string(),
                1 => "TIMESTAMP '1970-01-01 23:59:59'".to_string(),
                2 => "('infinity')::timestamp".to_string(),
                3 => "('-infinity')::timestamp".to_string(),
                4 => "TIMESTAMP '1999-12-31 23:59:59.999999'".to_string(),
                5 => "TIMESTAMP '2000-01-01 00:00:00.000001'".to_string(),
                6 => "TIMESTAMP '2021-03-14 02:30:00'".to_string(),
                7 => "TIMESTAMP '294276-12-31 23:59:59.999999'".to_string(),
                8 => "TIMESTAMP '4714-11-24 00:00:00 BC'".to_string(),
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
            SqlType::Json | SqlType::Jsonb => {
                let body = match self.rng.below(8) {
                    0 => "{}".to_string(),
                    1 => "[]".to_string(),
                    2 => "null".to_string(),
                    3 => "{\"a\":1,\"b\":[true,null]}".to_string(),
                    4 => "[1,[2,[3]]]".to_string(),
                    5 => "\"txt\"".to_string(),
                    6 => format!("{}", self.rng.range_i64(-100, 100)),
                    _ => format!("{{\"k{}\":{}}}", self.rng.below(3), self.rng.below(10)),
                };
                format!("('{}')::{}", body, ty.name())
            }
            SqlType::Uuid => {
                let body = match self.rng.below(4) {
                    0 => "00000000-0000-0000-0000-000000000000".to_string(),
                    1 => "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string(),
                    2 => "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11".to_string(),
                    _ => format!(
                        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                        self.rng.below(0x1_0000_0000),
                        self.rng.below(0x1_0000),
                        self.rng.below(0x1_0000),
                        self.rng.below(0x1_0000),
                        (self.rng.below(0x1_0000_0000) << 16) | self.rng.below(0x1_0000)
                    ),
                };
                format!("('{}')::uuid", body)
            }
            SqlType::Bytea => {
                let body = match self.rng.below(5) {
                    0 => "\\x".to_string(),
                    1 => "\\x00".to_string(),
                    2 => "\\xff00".to_string(),
                    3 => "\\xdeadbeef".to_string(),
                    _ => {
                        let len = self.rng.below(6);
                        let mut s = "\\x".to_string();
                        for _ in 0..len {
                            s.push_str(&format!("{:02x}", self.rng.below(256)));
                        }
                        s
                    }
                };
                format!("('{}')::bytea", body)
            }
            SqlType::Interval => {
                // A2 pool: ISO-8601 forms, mixed-sign mon/day/us, justify
                // targets, extreme months/days/us, verbose form.
                let body = match self.rng.below(15) {
                    0 => "0".to_string(),
                    1 => "1 day".to_string(),
                    2 => "-1 mons".to_string(),
                    3 => "1 year 2 mons 3 days 04:05:06.789".to_string(),
                    4 => "178000000 years".to_string(),
                    5 => "-178000000 years".to_string(),
                    6 => "00:00:00.000001".to_string(),
                    7 => "P1Y2M3DT4H5M6S".to_string(),
                    8 => "P-1Y-2M3DT-4H".to_string(),
                    9 => "1 mon -1 day".to_string(),
                    10 => "-1 day +26:03:04.005006".to_string(),
                    11 => "2147483647 days".to_string(),
                    12 => "9223372036854.775806 seconds".to_string(),
                    13 => "@ 1 year 2 mons ago".to_string(),
                    _ => format!(
                        "{} hours {} minutes",
                        self.rng.range_i64(-48, 48),
                        self.rng.below(60)
                    ),
                };
                format!("('{}')::interval", body)
            }
            SqlType::Time => {
                let body = match self.rng.below(7) {
                    0 => "00:00:00".to_string(),
                    1 => "24:00:00".to_string(),
                    2 => "23:59:59.999999".to_string(),
                    3 => "12:00:00.5".to_string(),
                    4 => "00:00:00.000001".to_string(),
                    5 => "01:23:45.678901".to_string(),
                    _ => format!(
                        "{:02}:{:02}:{:02}",
                        self.rng.below(24),
                        self.rng.below(60),
                        self.rng.below(60)
                    ),
                };
                format!("('{}')::time", body)
            }
            SqlType::Timetz => {
                let body = match self.rng.below(7) {
                    0 => "00:00:00+00".to_string(),
                    1 => "24:00:00+00".to_string(),
                    2 => "23:59:59.999999-08".to_string(),
                    3 => "12:00:00+05:45".to_string(),
                    4 => "15:00:00+15:59:59".to_string(),
                    5 => "12:00:00-15:59:59".to_string(),
                    _ => format!(
                        "{:02}:{:02}:{:02}{}{:02}",
                        self.rng.below(24),
                        self.rng.below(60),
                        self.rng.below(60),
                        if self.rng.chance(1, 2) { "+" } else { "-" },
                        self.rng.below(13)
                    ),
                };
                format!("('{}')::timetz", body)
            }
            SqlType::TextArr => {
                let body = match self.rng.below(6) {
                    0 => "{}".to_string(),
                    1 => "{a}".to_string(),
                    2 => "{a,b,a}".to_string(),
                    3 => "{\"\",x}".to_string(),
                    4 => "{NULL,x}".to_string(),
                    _ => {
                        let n = 1 + self.rng.below(3);
                        let els: Vec<String> = (0..n)
                            .map(|_| {
                                format!("e{}", (b'a' + self.rng.below(4) as u8) as char)
                            })
                            .collect();
                        format!("{{{}}}", els.join(","))
                    }
                };
                format!("('{}')::text[]", body)
            }
            SqlType::Int4Arr => {
                let body = match self.rng.below(6) {
                    0 => "{}".to_string(),
                    1 => "{0}".to_string(),
                    2 => "{-1,2147483647}".to_string(),
                    3 => "{NULL,1}".to_string(),
                    4 => "{1,1,2}".to_string(),
                    _ => {
                        let n = 1 + self.rng.below(4);
                        let els: Vec<String> = (0..n)
                            .map(|_| format!("{}", self.rng.range_i64(-9, 9)))
                            .collect();
                        format!("{{{}}}", els.join(","))
                    }
                };
                format!("('{}')::int4[]", body)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::scope::ScopeRel;
    use crate::weights::WeightTable;

    fn catalog() -> Catalog {
        FixtureCatalog.load_catalog().unwrap()
    }

    fn rels(cat: &Catalog) -> Vec<ScopeRel> {
        vec![ScopeRel::from_table(&cat.tables[0], "t0".to_string())]
    }

    #[test]
    fn generation_is_deterministic() {
        let cat = catalog();
        let rels = rels(&cat);
        let w = WeightTable::defaults();
        let run = |seed: u64| {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 5);
            let scope = Scope { rels: &rels, outer: None };
            let e = g.gen_typed(&scope, SqlType::Int8, 5);
            (e.to_sql(), prods)
        };
        assert_eq!(run(11), run(11));
        assert_ne!(run(11).0, run(12).0);
    }

    #[test]
    fn weight_change_changes_stream_deterministically() {
        let cat = catalog();
        let rels = rels(&cat);
        let run = |seed: u64, spec: &str| {
            let w = WeightTable::parse(spec).unwrap();
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 5);
            let scope = Scope { rels: &rels, outer: None };
            let mut out = Vec::new();
            for _ in 0..20 {
                out.push(g.gen_typed(&scope, SqlType::Int8, 5).to_sql());
            }
            out
        };
        // Same seed + same weights = identical stream; a weight change is
        // a different (still deterministic) stream.
        assert_eq!(run(11, "case=9"), run(11, "case=9"));
        assert_ne!(run(11, ""), run(11, "case=9"));
    }

    #[test]
    fn zero_weights_suppress_productions() {
        let cat = catalog();
        let rels = rels(&cat);
        // Everything typed-composite except CASE off; CASE dominates.
        let w = WeightTable::parse(
            "cast=0,coalesce=0,nullif=0,arith=0,neg=0,func_num=0,concat=0,\
             func_text=0,func_varchar=0,length=0,date_plus_int=0,date_trunc=0",
        )
        .unwrap();
        let mut rng = Rng::new(17);
        let mut all = Vec::new();
        for _ in 0..100 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 5);
            let scope = Scope { rels: &rels, outer: None };
            let ty = g.any_type(&scope);
            g.gen_typed(&scope, ty, 5);
            all.extend(prods);
        }
        assert!(all.iter().all(|p| !p.starts_with("cast:")), "cast weighted 0 still fired");
        assert!(
            all.iter().all(|p| !matches!(p.as_str(), "binop:+" | "binop:-" | "binop:*")),
            "arith weighted 0 still fired"
        );
        assert!(all.iter().any(|p| p == "case"), "case never fired at dominant weight");
    }

    #[test]
    fn productions_are_recorded() {
        let cat = catalog();
        let rels = rels(&cat);
        let w = WeightTable::defaults();
        let mut rng = Rng::new(3);
        let mut prods = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 6);
        let scope = Scope { rels: &rels, outer: None };
        let e = g.gen_typed(&scope, SqlType::Float8, 6);
        let sql = e.to_sql();
        assert!(!prods.is_empty());
        assert!(!sql.is_empty());
        // Every recorded production has a known prefix.
        for p in &prods {
            let known = [
                "colref", "lit:", "null:", "unop:", "binop:", "cmp:", "func:", "case",
                "cast:", "isnull", "isnotnull", "fmt:", "jsonb:", "ts:", "arr:",
                "interval:", "bytea:",
            ];
            assert!(known.iter().any(|k| p.starts_with(k)), "unknown production {}", p);
        }
    }

    #[test]
    fn colrefs_are_alias_qualified() {
        let cat = catalog();
        let rels = rels(&cat);
        let w = WeightTable::defaults();
        let mut rng = Rng::new(8);
        for _ in 0..50 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let scope = Scope { rels: &rels, outer: None };
            let e = g.gen_bool(&scope, 4);
            let sql = e.to_sql();
            // Cheap textual invariant: every fixture column mention is
            // alias-qualified (preceded by "t0.").
            for c in ["c_int4", "k_int", "c_text"] {
                let mut start = 0;
                while let Some(pos) = sql[start..].find(c) {
                    let at = start + pos;
                    assert!(
                        at >= 3 && &sql[at - 3..at] == "t0.",
                        "unqualified column {c} in {sql}"
                    );
                    start = at + c.len();
                }
            }
            // Balanced parens as a cheap render-sanity check.
            let opens = sql.matches('(').count();
            let closes = sql.matches(')').count();
            assert_eq!(opens, closes, "unbalanced: {}", sql);
        }
    }

    #[test]
    fn outer_scope_leaves_fire_only_with_outer() {
        let cat = catalog();
        let outer_rels = rels(&cat);
        let inner_rels = vec![ScopeRel::from_table(&cat.tables[4], "t1".to_string())];
        let w = WeightTable::parse("colref:outer=100,colref=0,lit=0,null=0").unwrap();
        let mut rng = Rng::new(5);
        let mut prods = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
        let outer = Scope { rels: &outer_rels, outer: None };
        let inner = Scope { rels: &inner_rels, outer: Some(&outer) };
        let e = g.gen_typed(&inner, SqlType::Int4, 0);
        // Depth-0 leaf with colref:outer dominant: must reference t0.
        assert!(e.to_sql().starts_with("t0."), "expected outer ref, got {}", e.to_sql());
        assert!(prods.iter().any(|p| p == "colref:outer"));
        // Without an outer scope, colref:outer is never offered.
        let mut prods2 = Vec::new();
        let mut g2 = Gen::new(&mut rng, &cat, &w, &mut prods2, 3);
        for _ in 0..20 {
            g2.gen_typed(&outer, SqlType::Int4, 0);
        }
        assert!(prods2.iter().all(|p| p != "colref:outer"));
    }
}
