//! Miri-sized exercise of the regex_dfa_kernel unsafe paths: scan loops,
//! miss/getvacant/install, REG_SMALL cache eviction, backref (cfind),
//! lookahead (lacon), and dirty thread-local scratch reuse across execs.

use mcx::MemoryContext;
use regex::RegMatch;
use regex_core::regex_compile::pg_regcomp;
use regex_core::regex_consts::{REG_ADVANCED, REG_SMALL};
use regex_core::regex_exec::pg_regexec;
use regex_core::regex_locale::pg_set_regex_collation;
use types_core::C_COLLATION_OID;

fn w(s: &str) -> Vec<u32> {
    s.chars().map(|c| c as u32).collect()
}

fn run(pat: &str, s: &str, eflags: i32, nmatch: usize) -> bool {
    let cx = MemoryContext::new("miri-compile");
    pg_set_regex_collation(cx.mcx(), C_COLLATION_OID).unwrap();
    let re = pg_regcomp(cx.mcx(), &w(pat), REG_ADVANCED, C_COLLATION_OID).unwrap();
    let guts = re.re_guts.as_ref().unwrap();
    let mut pm = vec![RegMatch::UNSET; nmatch];
    pg_regexec(guts, &w(s), 0, &mut pm, eflags).unwrap()
}

#[test]
fn kernel_paths() {
    let long = "abc".repeat(30);
    let cases: &[(&str, &str, bool)] = &[
        ("abcdefgh", "iqjrkabcdefghzzz", true),
        ("[a-z0-9_]+@[a-z0-9]+", "qwertyuiopasdfgh@abc012", true),
        ("(foo|bar|baz)[0-9]", "gggggggbar7gg", true),
        ("(a+)(b+)c\\2", "aabbcbb", true),
        ("x(?=y)", "axy", true),
        ("(x)(y)(z)", "wxyz", true),
        ("[abc]{20,}", &long, true),
        ("nope$", "nope!", false),
    ];
    for rep in 0..3 {
        for (p, s, exp) in cases {
            assert_eq!(run(p, s, 0, 4), *exp, "rep {rep} pattern {p:?}");
            assert_eq!(run(p, s, REG_SMALL, 0), *exp, "REG_SMALL rep {rep} pattern {p:?}");
        }
    }
}
