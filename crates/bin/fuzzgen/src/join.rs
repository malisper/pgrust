//! Joins statement module: left-deep join chains over 2-4 tables with
//! INNER/LEFT/RIGHT/FULL kinds, ON conditions built by the expr module
//! against the accumulated scope, USING/NATURAL occasionally (first join
//! only — a later USING against an ON-joined left side would reference an
//! ambiguous column), CROSS JOIN rarely. Self-joins arise naturally from
//! independent table picks (aliases are statement-unique).

use crate::catalog::SqlType;
use crate::expr::Expr;
use crate::render::{FromItem, JoinCond, JoinKind, SelectStmt};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{finish_select, Gen};

pub fn gen_join_stmt(g: &mut Gen) -> SelectStmt {
    let ntables = match g.weights.pick(g.rng, &["join:2", "join:3", "join:4"]) {
        "join:2" => 2,
        "join:3" => 3,
        _ => 4,
    };
    let mut rels: Vec<ScopeRel> = Vec::with_capacity(ntables);
    let mut from = {
        let table = g.pick_table();
        let alias = g.next_alias();
        rels.push(ScopeRel::from_table(table, alias.clone()));
        FromItem::Table { name: table.name.clone(), alias }
    };
    for step in 1..ntables {
        let table = g.pick_table();
        let alias = g.next_alias();
        let rel = ScopeRel::from_table(table, alias.clone());
        let right = FromItem::Table { name: table.name.clone(), alias };
        let (kind, cond) = gen_join_cond(g, &rels, &rel, step);
        rels.push(rel);
        from = FromItem::Join { left: Box::new(from), right: Box::new(right), kind, cond };
    }
    finish_select(g, &rels, from, Vec::new())
}

/// Join kind + condition for one step of the chain. `left` is everything
/// joined so far; `right` the newly added relation.
fn gen_join_cond(
    g: &mut Gen,
    left: &[ScopeRel],
    right: &ScopeRel,
    step: usize,
) -> (JoinKind, JoinCond) {
    let kind = match g
        .weights
        .pick(g.rng, &["join:inner", "join:left", "join:right", "join:full", "join:cross"])
    {
        "join:cross" => {
            g.fire("join:cross");
            return (JoinKind::Inner, JoinCond::Cross);
        }
        picked => {
            g.fire(picked);
            match picked {
                "join:left" => JoinKind::Left,
                "join:right" => JoinKind::Right,
                "join:full" => JoinKind::Full,
                _ => JoinKind::Inner,
            }
        }
    };
    // USING/NATURAL only on the first join: the single left relation keeps
    // the merged column names unambiguous. Both need common comparable
    // columns to exist at all.
    if step == 1 {
        let common = common_columns(&left[0], right);
        if !common.is_empty() {
            // NATURAL joins on *every* shared name, so it is only safe when
            // all of them made the comparable-and-unique cut.
            let all_shared_ok = left[0]
                .columns
                .iter()
                .filter(|ca| right.columns.iter().any(|cb| cb.name == ca.name))
                .all(|ca| common.contains(&ca.name));
            let options: &[&str] = if all_shared_ok {
                &["join:on", "join:using", "join:natural"]
            } else {
                &["join:on", "join:using"]
            };
            match g.weights.pick(g.rng, options) {
                "join:using" => {
                    g.fire("join:using");
                    // One shared column, sometimes two.
                    let ncols = if common.len() > 1 && g.rng.chance(1, 3) { 2 } else { 1 };
                    let mut cols = Vec::with_capacity(ncols);
                    let start = g.rng.below_usize(common.len());
                    for k in 0..ncols {
                        cols.push(common[(start + k) % common.len()].clone());
                    }
                    return (kind, JoinCond::Using(cols));
                }
                "join:natural" => {
                    g.fire("join:natural");
                    return (kind, JoinCond::Natural);
                }
                _ => {}
            }
        }
    }
    g.fire("join:on");
    (kind, JoinCond::On(gen_on_expr(g, left, right, kind)))
}

/// Column names shared by both relations with mutually comparable types
/// and no duplicates on either side (USING/NATURAL validity).
fn common_columns(a: &ScopeRel, b: &ScopeRel) -> Vec<String> {
    let unique = |rel: &ScopeRel, name: &str| {
        rel.columns.iter().filter(|c| c.name == name).count() == 1
    };
    a.columns
        .iter()
        .filter(|ca| {
            b.columns
                .iter()
                .any(|cb| cb.name == ca.name && ca.ty.comparable_with(cb.ty))
                && unique(a, &ca.name)
                && unique(b, &ca.name)
        })
        .map(|c| c.name.clone())
        .collect()
}

/// ON condition: a plausible key equality, a random boolean predicate over
/// the joined scope, or both ANDed. FULL joins stick to key equality — the
/// reference only supports merge/hash-joinable FULL JOIN conditions, and a
/// random cross-side predicate would degrade the statement to a matched
/// not-supported error on both sides.
fn gen_on_expr(g: &mut Gen, left: &[ScopeRel], right: &ScopeRel, kind: JoinKind) -> Expr {
    let all: Vec<ScopeRel> = left.iter().chain(std::iter::once(right)).cloned().collect();
    let key = key_equality(g, left, right);
    let picked = if kind == JoinKind::Full {
        if key.is_some() {
            "join:on_key"
        } else {
            "join:on_rand"
        }
    } else if key.is_some() {
        g.weights.pick(g.rng, &["join:on_key", "join:on_rand", "join:on_mixed"])
    } else {
        "join:on_rand"
    };
    g.fire(picked);
    let scope = Scope { rels: &all, outer: None };
    match picked {
        "join:on_key" => key.unwrap(),
        "join:on_mixed" => {
            let rand = g.gen_bool(&scope, 2);
            Expr::Binary { op: "AND", lhs: Box::new(key.unwrap()), rhs: Box::new(rand) }
        }
        _ => g.gen_bool(&scope, 2),
    }
}

/// Equality between a left-side column and a right-side column of the same
/// comparable family, preferring integer/text keys (the plausible-join-key
/// shape). None when no comparable pair exists.
fn key_equality(g: &mut Gen, left: &[ScopeRel], right: &ScopeRel) -> Option<Expr> {
    let keyish = |ty: SqlType| ty.is_integer() || ty.is_text_family();
    let mut pairs: Vec<(String, String, String, String)> = Vec::new();
    let mut key_pairs: Vec<(String, String, String, String)> = Vec::new();
    for lrel in left {
        for lc in &lrel.columns {
            for rc in &right.columns {
                if lc.ty.comparable_with(rc.ty) {
                    let p = (
                        lrel.alias.clone(),
                        lc.name.clone(),
                        right.alias.clone(),
                        rc.name.clone(),
                    );
                    if keyish(lc.ty) && keyish(rc.ty) {
                        key_pairs.push(p.clone());
                    }
                    pairs.push(p);
                }
            }
        }
    }
    let pool = if key_pairs.is_empty() { &pairs } else { &key_pairs };
    if pool.is_empty() {
        return None;
    }
    let (la, ln, ra, rn) = pool[g.rng.below_usize(pool.len())].clone();
    Some(Expr::Binary {
        op: "=",
        lhs: Box::new(Expr::ColRef { alias: la, name: ln }),
        rhs: Box::new(Expr::ColRef { alias: ra, name: rn }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::render::scope_errors;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn join_statements_are_scoped_and_varied() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(4242);
        let mut sqls = String::new();
        for i in 0..400 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_join_stmt(&mut g);
            let errs = scope_errors(&stmt, &cat);
            assert!(errs.is_empty(), "stmt {i}: {errs:?}\n{}", stmt.to_sql());
            sqls.push_str(&stmt.to_sql());
            sqls.push('\n');
        }
        // Every join flavor appears across 400 statements.
        for frag in [
            " JOIN ",
            " LEFT JOIN ",
            " RIGHT JOIN ",
            " FULL JOIN ",
            " CROSS JOIN ",
            " USING (",
            "NATURAL ",
            " ON ",
        ] {
            assert!(sqls.contains(frag), "join flavor {frag:?} never generated");
        }
    }

    #[test]
    fn common_columns_respects_types_and_duplicates() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let scalar = ScopeRel::from_table(&cat.tables[0], "t0".to_string());
        let mixed = ScopeRel::from_table(&cat.tables[1], "t1".to_string());
        let common = common_columns(&scalar, &mixed);
        assert!(common.contains(&"k_int".to_string()));
        assert!(common.contains(&"k_text".to_string()));
        assert!(!common.contains(&"id".to_string()));
        // Self-pair: every column is common with itself.
        let self_common = common_columns(&scalar, &scalar);
        assert_eq!(self_common.len(), scalar.columns.len());
    }

    #[test]
    fn using_and_natural_only_on_first_join() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Force 4-table chains and USING/NATURAL-heavy conditions.
        let w = WeightTable::parse(
            "join:2=0,join:3=0,join:4=1,join:on=0,join:using=5,join:natural=5",
        )
        .unwrap();
        let mut rng = Rng::new(11);
        for _ in 0..100 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 2);
            let stmt = gen_join_stmt(&mut g);
            assert!(scope_errors(&stmt, &cat).is_empty());
            let sql = stmt.to_sql();
            // At most one USING/NATURAL per statement (first join only).
            assert!(sql.matches(" USING (").count() <= 1, "{sql}");
            assert!(sql.matches("NATURAL ").count() <= 1, "{sql}");
        }
    }
}
