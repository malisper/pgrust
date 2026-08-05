#!/usr/bin/env python3
"""Self-tests for the SLOC denominator's test-code scope rules.

This measurement tool has now been wrong twice (filename-prefix test
detection; the brace-searching #[cfg(test)] scanner). Every defect class fixed
on 2026-07-31 has a named regression test below, plus a truth table for the
cfg predicate evaluator and a synthetic crate tree for the module graph.

Run:  python3 proofs/coverage/test_sloc_denominator.py
"""
import os
import sys
import tempfile
import textwrap
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import sloc_rules          # noqa: E402
import test_scope          # noqa: E402


def dedent(s):
    return textwrap.dedent(s).lstrip("\n")


class Blanking(unittest.TestCase):
    """Bracket counting must be lexically safe."""

    def test_braces_in_literals_are_invisible(self):
        for lit in ('"{{{"', 'r"}}"', 'r#"{ " }"#', "b\"{\"", "'{'", "'\\''",
                    "// {", "/* { */", "/* /* { */ */"):
            src = f"let x = {lit};\n"
            b = test_scope.blank_literals(src)
            self.assertEqual(len(b), len(src), lit)
            self.assertNotIn("{", b, lit)

    def test_lifetimes_are_not_char_literals(self):
        src = "fn f<'a>(x: &'a str) -> &'a str { x }\n"
        b = test_scope.blank_literals(src)
        self.assertEqual(b.count("{"), 1)
        self.assertEqual(b.count("}"), 1)

    def test_newlines_and_length_preserved(self):
        src = 'a\n/* x\ny */\nb "s\\"t"\n'
        b = test_scope.blank_literals(src)
        self.assertEqual(len(b), len(src))
        self.assertEqual(b.count("\n"), src.count("\n"))


class CfgPredicates(unittest.TestCase):
    """A cfg predicate gates test code iff it can only hold under `test`."""

    TEST_ONLY = ["test", "all(test, target_os = \"linux\")",
                 "all(test, not(loom))", "any(test)", "all(unix, test)",
                 "any(all(test, unix), all(test, windows))"]
    NOT_TEST_ONLY = ["not(test)", "all(feature = \"std\", not(test))",
                     "any(test, pgrust_sim)", "any(target_os = \"linux\", test)",
                     "any(test, feature = \"bench-internals\")",
                     "all(target_arch = \"aarch64\", any(miri, test))",
                     "unix", "feature = \"std\"", "not(any(test, sim))"]

    def test_truth_table(self):
        for p in self.TEST_ONLY:
            self.assertTrue(test_scope.cfg_implies_test(p), p)
        for p in self.NOT_TEST_ONLY:
            self.assertFalse(test_scope.cfg_implies_test(p), p)

    def test_every_non_test_only_predicate_mentioning_test_is_reported(self):
        """The ambiguity signal must fire for exactly the reportable shapes."""
        for p in self.NOT_TEST_ONLY:
            if "test" in p:
                self.assertTrue(test_scope.mentions_test(p), p)


class CfgTestSpans(unittest.TestCase):
    """Regression tests, one per defect class found on 2026-07-31."""

    def spans(self, src):
        return sloc_rules.cfg_test_spans(dedent(src), "f.rs")

    def test_D2_braceless_mod_does_not_swallow_the_next_item(self):
        """`#[cfg(test)] mod tests;` must end at its own semicolon.

        The superseded scanner searched for the next line containing `{` and
        excluded everything to its matching close — here, the whole macro."""
        src = """
            #[cfg(test)]
            mod tests;

            macro_rules! node_tags {
                ($($n:ident),+) => {
                    pub enum NodeTag { $($n),+ }
                };
            }
            node_tags!(A, B);
            """
        self.assertEqual(self.spans(src), [(1, 2)])

    def test_D2_later_spans_are_not_abandoned(self):
        """A braceless gated item must not stop the scan for later ones.

        The superseded scanner `break`ed when no later `{` existed, silently
        dropping every remaining cfg(test) span in the file."""
        src = """
            #[cfg(test)]
            mod tests;
            pub fn prod() -> i32 { 1 }
            #[cfg(test)]
            fn helper() -> i32 {
                2
            }
            #[cfg(test)]
            use std::sync::Once;
            """
        self.assertEqual(self.spans(src), [(1, 2), (4, 7), (8, 9)])

    def test_D4_all_test_predicate_is_recognised(self):
        """`#[cfg(all(test, …))]` is test-only; the old exact-string regex
        matched only the literal `#[cfg(test)]` and missed these whole."""
        src = """
            #[cfg(all(test, target_os = "linux"))]
            mod tests {
                fn a() {}
            }
            """
        self.assertEqual(self.spans(src), [(1, 4)])

    def test_cfg_test_on_a_match_arm(self):
        src = """
            fn f(k: K) -> i32 {
                match k {
                    K::A => 1,
                    #[cfg(test)]
                    K::Mock => 2,
                    _ => 3,
                }
            }
            """
        self.assertEqual(self.spans(src), [(4, 5)])

    def test_cfg_test_on_a_block_bodied_last_match_arm(self):
        src = """
            fn f(k: K) -> i32 {
                match k {
                    K::A => 1,
                    #[cfg(test)]
                    K::Mock => {
                        2
                    }
                }
            }
            """
        self.assertEqual(self.spans(src), [(4, 7)])

    def test_attribute_run_is_included_in_the_span(self):
        src = """
            #[cfg(test)]
            #[allow(dead_code)]
            fn helper() {
                let _ = 1;
            }
            """
        self.assertEqual(self.spans(src), [(1, 5)])

    def test_not_test_items_are_not_excluded(self):
        src = """
            #[cfg(not(test))]
            fn prod() {
                let _ = 1;
            }
            """
        self.assertEqual(self.spans(src), [])

    def test_brace_inside_a_string_does_not_close_the_span(self):
        src = """
            #[cfg(test)]
            mod tests {
                fn a() { let s = "}"; }
            }
            pub fn prod() -> i32 { 1 }
            """
        self.assertEqual(self.spans(src), [(1, 4)])


class Crate:
    """A synthetic crate tree on disk."""

    def __init__(self, files, manifest="[package]\nname = \"c\"\n"):
        self.dir = tempfile.mkdtemp()
        self.write("crates/c/Cargo.toml", manifest)
        for rel, body in files.items():
            self.write(os.path.join("crates/c", rel), dedent(body))
        test_scope.set_repo_root(self.dir)

    def write(self, rel, body):
        p = os.path.join(self.dir, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as fh:
            fh.write(body)

    def is_test(self, rel):
        return test_scope.is_test_file(os.path.join("crates/c", rel))

    def reason(self, rel):
        return test_scope.test_reason(os.path.join("crates/c", rel))


class ModuleGraph(unittest.TestCase):

    def tearDown(self):
        test_scope.set_repo_root(
            os.path.dirname(os.path.dirname(os.path.abspath(HERE))))

    def test_D1_D3_test_ness_is_structural_not_filename_based(self):
        """The name of the file must not matter, in either direction."""
        c = Crate({
            "src/lib.rs": """
                #[cfg(test)]
                mod ws_tests;
                #[cfg(test)]
                mod c_cases;
                mod tests_helpers;
                pub fn prod() -> i32 { tests_helpers::h() }
                """,
            "src/ws_tests.rs": "#[test]\nfn t() {}\n",
            "src/c_cases.rs": "#[test]\nfn t() {}\n",
            # production code whose NAME starts with `tests` — the prefix rule
            # dropped this whole file
            "src/tests_helpers.rs": "pub fn h() -> i32 { 1 }\n",
        })
        self.assertTrue(c.is_test("src/ws_tests.rs"))
        self.assertTrue(c.is_test("src/c_cases.rs"))
        self.assertFalse(c.is_test("src/tests_helpers.rs"))
        self.assertEqual(c.reason("src/ws_tests.rs"), "cfg-test-mod")

    def test_descendants_of_a_test_module_are_test_code(self):
        c = Crate({
            "src/lib.rs": "#[cfg(test)]\nmod tests;\n",
            "src/tests.rs": "mod fixtures;\nmod util;\n#[test]\nfn t() {}\n",
            "src/tests/fixtures.rs": "pub fn f() -> i32 { 1 }\n",
            "src/tests/util/mod.rs": "pub fn u() -> i32 { 1 }\n",
        })
        self.assertTrue(c.is_test("src/tests/fixtures.rs"))
        self.assertTrue(c.is_test("src/tests/util/mod.rs"))

    def test_inline_mod_nesting_resolves_the_child_path(self):
        c = Crate({
            "src/lib.rs": """
                pub mod stemmers {
                    pub mod english;
                }
                #[cfg(test)]
                pub mod harness {
                    mod cases;
                }
                """,
            "src/stemmers/english.rs": "pub fn s() -> i32 { 1 }\n",
            "src/harness/cases.rs": "#[test]\nfn t() {}\n",
        })
        self.assertFalse(c.is_test("src/stemmers/english.rs"))
        self.assertTrue(c.is_test("src/harness/cases.rs"))

    def test_path_attribute_override(self):
        c = Crate({
            "src/lib.rs": """
                #[cfg(test)]
                #[path = "battery/all.rs"]
                mod tests;
                pub fn prod() -> i32 { 1 }
                """,
            "src/battery/all.rs": "#[test]\nfn t() {}\n",
        })
        self.assertTrue(c.is_test("src/battery/all.rs"))

    def test_inner_cfg_test_attribute(self):
        c = Crate({
            "src/lib.rs": "mod extra;\npub fn prod() -> i32 { 1 }\n",
            "src/extra.rs": "#![cfg(test)]\n#[test]\nfn t() {}\n",
        })
        self.assertTrue(c.is_test("src/extra.rs"))

    def test_orphan_test_file_is_excluded_and_reported(self):
        """Nothing declares it => rustc never compiles it => unreachable."""
        c = Crate({
            "src/lib.rs": "pub fn prod() -> i32 { 1 }\n",
            "src/tests.rs": """
                fn helper() -> i32 { 1 }
                #[test]
                fn t() { assert_eq!(helper(), 1); }
                """,
        })
        self.assertTrue(c.is_test("src/tests.rs"))
        self.assertEqual(c.reason("src/tests.rs"), "orphan-test-file")
        self.assertTrue(any("unreachable" in d
                            for d in test_scope.diagnostics()))

    def test_unreachable_non_test_file_is_kept_and_reported(self):
        """Never silently drop production code behind a resolver miss."""
        c = Crate({
            "src/lib.rs": "pub fn prod() -> i32 { 1 }\n",
            "src/stranded.rs": "pub fn s() -> i32 { 2 }\n",
        })
        self.assertFalse(c.is_test("src/stranded.rs"))
        self.assertTrue(any("stranded.rs" in d and "KEPT in scope" in d
                            for d in test_scope.diagnostics()))

    def test_include_target_is_reachable_production_code(self):
        c = Crate({
            "src/lib.rs": "include!(\"tables.rs\");\npub fn prod() -> i32 { 1 }\n",
            "src/tables.rs": "pub const T: &[u8] = b\"x\";\n",
        })
        self.assertFalse(c.is_test("src/tables.rs"))
        self.assertFalse(any("tables.rs" in d
                             for d in test_scope.diagnostics()))

    def test_bin_entry_points_are_roots(self):
        c = Crate({
            "src/main.rs": "mod m;\nfn main() { m::go() }\n",
            "src/m.rs": "pub fn go() {}\n",
            "src/bin/tool.rs": "mod helper;\nfn main() { helper::h() }\n",
            "src/bin/tool/helper.rs": "pub fn h() {}\n",
        })
        for f in ("src/m.rs", "src/bin/tool.rs", "src/bin/tool/helper.rs"):
            self.assertFalse(c.is_test(f), f)
        self.assertEqual(test_scope.diagnostics(), [])

    def test_harness_only_file_reachable_via_a_plain_mod(self):
        c = Crate({
            "src/lib.rs": "mod checks;\npub fn prod() -> i32 { 1 }\n",
            "src/checks.rs": "#[test]\nfn t() { assert!(true); }\n",
        })
        self.assertTrue(c.is_test("src/checks.rs"))
        self.assertEqual(c.reason("src/checks.rs"), "harness-only")

    def test_test_gated_mod_with_no_file_is_reported_not_guessed(self):
        c = Crate({
            "src/lib.rs": "mod imp;\npub fn prod() -> i32 { 1 }\n",
            "src/imp.rs": "#[cfg(test)]\nmod tests;\npub fn i() -> i32 { 1 }\n",
        })
        self.assertFalse(c.is_test("src/imp.rs"))
        self.assertTrue(any("resolves to no file" in d
                            for d in test_scope.diagnostics()))

    def test_a_path_component_named_tests_is_test_code(self):
        c = Crate({
            "src/lib.rs": "pub fn prod() -> i32 { 1 }\n",
            "tests/it.rs": "#[test]\nfn t() {}\n",
            "benches/b.rs": "fn main() {}\n",
        })
        self.assertTrue(c.is_test("tests/it.rs"))
        self.assertTrue(c.is_test("benches/b.rs"))


class DenominatorIntegration(unittest.TestCase):
    """analyze_text must honour the structural verdict, and only that."""

    def tearDown(self):
        test_scope.set_repo_root(
            os.path.dirname(os.path.dirname(os.path.abspath(HERE))))

    def sloc(self, rel, crate):
        p = os.path.join(crate.dir, "crates/c", rel)
        return len(sloc_rules.sloc_lines(p))

    def test_test_file_contributes_zero_and_production_file_does_not(self):
        c = Crate({
            "src/lib.rs": """
                #[cfg(test)]
                mod ws_tests;
                mod tests_helpers;
                pub fn prod(x: i32) -> i32 {
                    let y = x + 1;
                    y * 2
                }
                """,
            "src/ws_tests.rs": """
                #[test]
                fn t() {
                    let a = 1;
                    assert_eq!(a, 1);
                }
                """,
            "src/tests_helpers.rs": """
                pub fn h(x: i32) -> i32 {
                    let y = x + 1;
                    y
                }
                """,
        })
        self.assertEqual(self.sloc("src/ws_tests.rs", c), 0)
        self.assertGreater(self.sloc("src/tests_helpers.rs", c), 0)
        # lib.rs: the fn signature + 2 body lines survive v2 (the `mod` and
        # attribute declaration lines do not) — the point is it is NOT zero
        # and the cfg(test) mod decl did not swallow `prod`.
        self.assertEqual(self.sloc("src/lib.rs", c), 3)

    def test_deterministic(self):
        c = Crate({"src/lib.rs": "pub fn p() -> i32 { 1 }\n"})
        a = self.sloc("src/lib.rs", c)
        b = self.sloc("src/lib.rs", c)
        self.assertEqual(a, b)

    def test_v2_ruling_invariants_unchanged(self):
        """The fix must not re-open the adopted ruling: asserts stay IN,
        macro template bodies stay IN, declaration lines stay OUT."""
        text = dedent("""
            use std::fmt;
            pub struct S {
                a: i32,
            }
            macro_rules! m {
                ($n:ident) => {
                    pub fn $n() -> i32 {
                        let v = 1;
                        v
                    }
                };
            }
            pub fn f(x: i32) -> i32 {
                assert!(x > 0);
                debug_assert!(x < 9);
                x
            }
            """)
        a = sloc_rules.analyze_text(text, "crates/x/src/prod.rs")
        denom, _ = sloc_rules.denominator(a, "v2", exclude_const_tables=True)
        keep = {n for n in denom}
        lines = text.splitlines()

        def line_of(frag):
            return next(i + 1 for i, l in enumerate(lines) if frag in l)

        for frag in ("assert!(x > 0)", "debug_assert!", "let v = 1;",
                     "pub fn f(x: i32)"):
            self.assertIn(line_of(frag), keep, frag)
        for frag in ("use std::fmt;", "pub struct S {", "a: i32,",
                     "macro_rules! m {"):
            self.assertNotIn(line_of(frag), keep, frag)


class StaticMutTables(unittest.TestCase):
    """Regression tests for the 2026-08-01 RE_CONST_TABLE_HEAD fix:
    `static mut NAME:` heads and `= unsafe {` initializers (the c2rust
    snowball-stemmer table shape) were not recognised as const-table heads,
    so those data tables counted IN the v2 denominator tree-wide."""

    FIXTURE = dedent("""
        static mut s_0_0: [i32; 3] = [
            1,
            2,
            3,
        ];
        static mut a_0: [Among; 2] = unsafe {
            [
                Among {
                    s: &raw const s_0_0 as *const i32,
                    result: -(1 as i32),
                },
                Among {
                    s: &raw const s_0_0 as *const i32,
                    result: -(1 as i32),
                },
            ]
        };
        pub const T: [u8; 2] = [
            1,
            2,
        ];
        pub fn stem(x: i32) -> i32 {
            let v = x + 1;
            v
        }
        """)

    def analysis(self):
        return sloc_rules.analyze_text(self.FIXTURE, "crates/x/src/prod.rs")

    def line_of(self, frag):
        return next(i + 1 for i, l in enumerate(self.FIXTURE.splitlines())
                    if frag in l)

    def test_static_mut_bracket_head_matches(self):
        self.assertIsNotNone(sloc_rules.RE_CONST_TABLE_HEAD.match(
            "static mut s_0_0: [symbol; 5] = ["))

    def test_static_mut_unsafe_brace_head_matches(self):
        self.assertIsNotNone(sloc_rules.RE_CONST_TABLE_HEAD.match(
            "static mut a_0: [among; 3] = unsafe {"))

    def test_plain_const_head_still_matches(self):
        self.assertIsNotNone(sloc_rules.RE_CONST_TABLE_HEAD.match(
            "pub const T: [u8; 2] = ["))

    def test_fn_body_is_not_a_table_head(self):
        for line in ("pub fn stem(x: i32) -> i32 {",
                     "let v = unsafe { g() };",
                     "static mut N: i32 = 0;"):
            self.assertIsNone(sloc_rules.RE_CONST_TABLE_HEAD.match(line), line)

    def test_interiors_excluded_function_body_kept(self):
        a = self.analysis()
        denom, _ = sloc_rules.denominator(a, "v2", exclude_const_tables=True)
        # table interiors (all three shapes) are OUT
        for frag in ("1,", "s: &raw const s_0_0", "result: -(1 as i32),"):
            self.assertNotIn(self.line_of(frag), denom, frag)
        # every line of the unsafe-brace table below its head is OUT
        head = self.line_of("a_0: [Among; 2] = unsafe {")
        end = self.line_of("};")
        for n in range(head + 1, end + 1):
            self.assertNotIn(n, denom, f"line {n} of a_0 span")
        # heads stay (the omission signal), real code stays
        self.assertIn(self.line_of("static mut s_0_0"), denom)
        self.assertIn(self.line_of("let v = x + 1;"), denom)
        self.assertIn(self.line_of("pub fn stem"), denom)

    def test_span_walker_terminates_on_unsafe_brace_form(self):
        stripped = list(sloc_rules.strip_line_comments(self.FIXTURE))
        spans = sloc_rules.const_table_spans(stripped)
        heads = sorted(s[0] for s in spans)
        self.assertEqual(heads, sorted([
            self.line_of("static mut s_0_0"),
            self.line_of("a_0: [Among; 2] = unsafe {"),
            self.line_of("pub const T"),
        ]))
        # the a_0 span must end at its `};`, not run away or stop short
        a0 = next(s for s in spans
                  if s[0] == self.line_of("a_0: [Among; 2] = unsafe {"))
        self.assertEqual(a0[1], self.line_of("};"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
