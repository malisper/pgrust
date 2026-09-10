//! Static rule-diff gate: mechanically recompute the enumeration from
//! notes/audits/unported-grammar-actions.md on every test run.
//!
//! From the automaton tables: the set of CALL-dispatch rules
//! (`DISPATCH[rule] == 0` — the only class that reaches `Parser::reduce`).
//! From actions.rs source (via `include_str!`): the set of rule numbers with
//! literal match arms (numbers, `a..=b` ranges, `|` alternations).
//!
//! The gate: every CALL rule is either handled by a literal arm or listed in
//! `KNOWN_UNPORTED` below. When a future grammar-table refresh (or an
//! actions.rs edit) introduces a CALL rule with no arm, this test fails
//! naming the rule, its production, and its gram.y line — the
//! new-enumeration event the audit's 2026-08-18 closure note anticipates.

use crate::tables::names::{YYRLINE, YYTNAME};
use crate::tables::{DISPATCH, YYNRULES, YYR1};
use std::collections::BTreeSet;

/// CALL rules with no ported action, acknowledged on purpose. After the
/// 2026-08-18 closures this list is EMPTY (`unimplemented_rule` has zero
/// known-reachable rules); a future entry must carry a comment naming the
/// production (YYTNAME/YYRLINE) and the tracking note.
const KNOWN_UNPORTED: &[usize] = &[];

/// Extract every rule number that has a literal arm in a `match rule {`
/// block anywhere in actions.rs. Nested `match rule` refinements inside an
/// arm body only ever re-match numbers the enclosing arm already matched,
/// so a flat union over all `match rule {` blocks equals the handled set.
/// Matches on other scrutinees (`match view...`, `match kind`) are never
/// scanned — only blocks introduced by the exact text `match rule {`.
fn handled_rules(src: &str) -> BTreeSet<usize> {
    let mut handled = BTreeSet::new();
    let mut search_from = 0;
    while let Some(off) = src[search_from..].find("match rule {") {
        let body_start = search_from + off + "match rule {".len();
        collect_match_arms(src, body_start, &mut handled);
        // Continue past the opener so nested `match rule {` blocks are
        // found too (their arms are subsets; the union is unchanged).
        search_from = body_start;
    }
    handled
}

/// Walk one `match` block body (starting just after its `{`), collecting
/// depth-1 arm patterns and folding their rule numbers into `out`. Skips
/// string literals, char literals, and comments; tracks brace depth for the
/// block and paren/bracket depth so commas inside calls/attributes never
/// terminate a pattern.
fn collect_match_arms(src: &str, start: usize, out: &mut BTreeSet<usize>) {
    let b = src.as_bytes();
    let mut i = start;
    let mut depth = 1usize; // inside the match block
    let mut paren = 0usize; // () and [] nesting at depth 1
    let mut buf = String::new();
    while i < b.len() && depth > 0 {
        let c = b[i];
        match c {
            b'/' if i + 1 < b.len() && b[i + 1] == b'/' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
                continue;
            }
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            b'\'' => {
                // Char literal vs lifetime: a literal is '\...' or 'X'.
                if i + 1 < b.len() && b[i + 1] == b'\\' {
                    i += 2;
                    while i < b.len() && b[i] != b'\'' {
                        i += 1;
                    }
                    i += 1;
                } else if i + 2 < b.len() && b[i + 2] == b'\'' {
                    i += 3;
                } else {
                    i += 1; // lifetime tick
                }
                continue;
            }
            b'{' => {
                depth += 1;
                i += 1;
                continue;
            }
            b'}' => {
                depth -= 1;
                if depth == 1 {
                    // An arm's block body just closed; next text is the
                    // next arm's pattern.
                    buf.clear();
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 1 {
            match c {
                b'(' | b'[' => paren += 1,
                b')' | b']' => paren = paren.saturating_sub(1),
                b',' if paren == 0 => buf.clear(),
                b'=' if paren == 0 && i + 1 < b.len() && b[i + 1] == b'>' => {
                    parse_pattern(&buf, out);
                    buf.clear();
                    i += 2;
                    continue;
                }
                _ => buf.push(c as char),
            }
        }
        i += 1;
    }
    assert_eq!(depth, 0, "unterminated match block in actions.rs");
}

/// Fold one arm pattern's rule numbers into `out`. Handles `N`, `A..=B`,
/// `X | Y | ...`, an optional trailing ` if guard` and leading attributes;
/// non-numeric alternatives (`_`, binding patterns) contribute nothing.
fn parse_pattern(pat: &str, out: &mut BTreeSet<usize>) {
    let pat = pat.trim();
    // Strip a guard: patterns here are numeric, so the first ` if ` at top
    // level starts the guard.
    let pat = pat.split(" if ").next().unwrap_or(pat);
    for alt in pat.split('|') {
        let alt = alt.trim();
        // Drop leading attribute remnants (buf clears on `]` only via
        // paren tracking, so a lone `#` can be left behind).
        let alt = alt.trim_start_matches('#').trim();
        if let Some((lo, hi)) = alt.split_once("..=") {
            if let (Ok(lo), Ok(hi)) = (lo.trim().parse::<usize>(), hi.trim().parse::<usize>()) {
                out.extend(lo..=hi);
            }
        } else if let Ok(n) = alt.parse::<usize>() {
            out.insert(n);
        }
    }
}

fn rule_desc(rule: usize) -> String {
    format!(
        "rule {} ({}, gram.y:{})",
        rule, YYTNAME[YYR1[rule] as usize], YYRLINE[rule]
    )
}

#[test]
fn every_call_rule_is_handled_or_allowlisted() {
    let handled = handled_rules(include_str!("actions.rs"));
    // Extraction sanity: the audit's method note counts 1,893 CALL rules
    // with ~1,873+ literal arms; a collapse here means the source scan
    // broke, not that the grammar shrank.
    assert!(
        handled.len() > 1500,
        "actions.rs arm extraction collapsed: only {} rule numbers found",
        handled.len()
    );
    assert!(
        handled.contains(&2),
        "rule 2 (parse_toplevel) arm not found"
    );
    assert!(
        handled.contains(&2464),
        "rule 2464 (PLpgSQL_Expr) arm not found"
    );

    let call_rules: Vec<usize> = (1..=YYNRULES).filter(|&r| DISPATCH[r] == 0).collect();
    assert!(
        call_rules.len() > 1800,
        "DISPATCH table sanity: only {} CALL rules",
        call_rules.len()
    );

    let missing: Vec<usize> = call_rules
        .iter()
        .copied()
        .filter(|r| !handled.contains(r) && !KNOWN_UNPORTED.contains(r))
        .collect();
    assert!(
        missing.is_empty(),
        "unhandled CALL-dispatch grammar rules (new-enumeration event): a rule \
         reaches Parser::reduce's `unimplemented_rule` fence with no ported arm \
         and no KNOWN_UNPORTED entry. Either port the action in actions.rs or \
         allowlist it with a comment naming the production:\n  {}",
        missing
            .iter()
            .map(|&r| rule_desc(r))
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // The allowlist must not go stale: every entry must still be a CALL
    // rule and still be unhandled.
    for &r in KNOWN_UNPORTED {
        assert!(
            r >= 1 && r <= YYNRULES && DISPATCH[r] == 0,
            "KNOWN_UNPORTED {} is not a CALL-dispatch rule; remove it",
            r
        );
        assert!(
            !handled.contains(&r),
            "KNOWN_UNPORTED {} now has a ported arm; remove it from the allowlist",
            rule_desc(r)
        );
    }
}

#[test]
fn pattern_parser_handles_all_arm_shapes() {
    let mut out = BTreeSet::new();
    parse_pattern("2", &mut out);
    parse_pattern("4..=7", &mut out);
    parse_pattern("455 | 456", &mut out);
    parse_pattern("_", &mut out);
    parse_pattern("n if n > 3", &mut out);
    parse_pattern("900 if flag", &mut out);
    assert_eq!(
        out.into_iter().collect::<Vec<_>>(),
        vec![2, 4, 5, 6, 7, 455, 456, 900]
    );
}

#[test]
fn arm_collector_ignores_nested_non_rule_matches() {
    let src = r#"
        fn f(rule: usize) {
            match rule {
                1 => {}
                10..=12 => {
                    let x = match kind { 77 => 1, _ => 0 };
                    let _ = x;
                }
                20 | 21 => self.g(view.v(1), "a, b => c"),
                _ => other(rule),
            }
        }
    "#;
    let got = handled_rules(src);
    assert_eq!(
        got.into_iter().collect::<Vec<_>>(),
        vec![1, 10, 11, 12, 20, 21]
    );
}
