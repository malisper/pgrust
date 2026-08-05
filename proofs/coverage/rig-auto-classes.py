#!/usr/bin/env python3
"""rig-auto-classes.py — measured demonstration that rustc (pinned 1.96)
emits NO llvm coverage mapping (no lcov DA record) for the four shape
classes auto_exceptions.py is allowed to auto-classify, while the
surrounding executed lines DO get DA records.

This is the SLOC-RULE-V2.md verification pattern (§2/§2b), packaged as a
self-checking rig so the demonstration is re-runnable instead of a table
in a doc. The auto-classifier's honesty rule is: a class may be
mechanically classified ONLY while this rig is green for it.

Classes demonstrated:
  fmt-cont    string-literal continuation lines of a multi-line
              format!/write!-family invocation in an EXECUTED arm
  let-decl    bare `let x: T;` declarations without initializer
  macro-decl  generator-macro invocation DECLARATION lines (the fc_*!
              rows) — the mapping is displaced into the macro_rules!
              definition body (macro_attrib.py's defect class)
  table-head  multi-line const/static bracket-initializer HEAD lines

Method: write a toy crate exercising every shape, compile with
`rustc -C instrument-coverage -O`, run it, merge the profile with the
toolchain's own llvm-profdata, export lcov with its own llvm-cov, then
assert per annotated line:
  EXPECT-NODA  -> no DA record may exist for the line
  EXPECT-DA    -> a DA record with count > 0 must exist
Exit 0 with a table iff every assertion holds; exit 1 otherwise.

Usage: ./rig-auto-classes.py [--keep] [--toolchain 1.96.0]
"""
import argparse
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile

TOY = r'''
// rig toy — every substantive line carries an EXPECT marker.
use std::fmt::Write as _;

macro_rules! gen1 {                                     // EXPECT-NODA scaffold
    ($($name:ident: $mul:expr => $add:expr;)*) => {$(   // EXPECT-NODA scaffold
        pub fn $name(x: u64) -> u64 {                   // EXPECT-DA template
            x.wrapping_mul($mul).wrapping_add($add)     // EXPECT-DA template
        }
    )*};
}

gen1! {                                                 // EXPECT-NODA scaffold
    gen_a: 3 => 1;                                      // EXPECT-NODA macro-decl
    gen_b: 5 => 2;                                      // EXPECT-NODA macro-decl
    gen_c: 7 => 3;                                      // EXPECT-NODA macro-decl
}

pub static TABLE: &[u32] = &[                           // EXPECT-NODA table-head
    11, 22, 33,                                         // EXPECT-NODA table-interior
    44, 55, 66,                                         // EXPECT-NODA table-interior
];

// ---- gap-1 probe: item-declaration rows inside a macro_rules! DEFINITION
// body (Lane-F hmac shape). Template fn BODY lines stay mapped (template
// class above); the struct/const/type/impl item rows do not.
pub trait Two {
    const FACTOR: u64;
    type Wide;
    fn compute(&self) -> u64;
}
macro_rules! gen2 {                                     // EXPECT-NODA scaffold
    ($name:ident, $mul:expr) => {                       // EXPECT-NODA scaffold
        pub struct $name(u64);                          // EXPECT-NODA macro-decl-defn
        impl Two for $name {                            // EXPECT-NODA macro-decl-defn
            const FACTOR: u64 = $mul;                   // EXPECT-NODA macro-decl-defn
            type Wide = u128;                           // EXPECT-NODA macro-decl-defn
            fn compute(&self) -> u64 {                  // EXPECT-DA template
                self.0.wrapping_mul(Self::FACTOR)       // EXPECT-DA template
            }
        }
    };
}
gen2!(GenTwoA, 3);                                      // (single-line invocation; not asserted)
gen2!(GenTwoB, 5);

// ---- gap-2 probe: multi-line PAREN-form generator-macro invocation
// (head + argument continuation lines; Lane-F hmac_hash! shape).
gen2!(                                                  // EXPECT-NODA macro-inv-cont
    GenTwoC,                                            // EXPECT-NODA macro-inv-cont
    7                                                   // EXPECT-NODA macro-inv-cont
);

// ---- gap-3 probes ----
// include! row (item position, generated-module shape):
mod generated {
    include!("toy_inc.rs");                             // EXPECT-NODA include-row
}
// brace-initializer static head + field rows (fn-pointer field included —
// a plain path, NOT a closure; closures stay mapped and are excluded):
pub struct Cfg {
    factor: u64,
    bias: u64,
    hash: fn(u64) -> u64,
}
pub static CFG: Cfg = Cfg {                             // EXPECT-NODA brace-table-head
    factor: 9,                                          // EXPECT-NODA brace-table-field
    bias: 4,                                            // EXPECT-NODA brace-table-field
    hash: generated::inc_hash,                          // EXPECT-NODA brace-table-field
};
// non-bracket multi-line static: head ends `=`, continuation is a path:
pub static TABLE_REF: &[u32] =                          // EXPECT-NODA eq-cont-head
    &TABLE_INNER;                                       // EXPECT-NODA eq-cont
pub static TABLE_INNER: [u32; 3] = [9, 8, 7];           // (single-line const-static; v2-excluded)

fn fmt_shapes(v: u64, out: &mut String) {
    let s = format!(                                    // EXPECT-DA neighbor
        "prefix value={} and a literal continuation \
         line via backslash",                           // (join-glued; not asserted)
        v                                               // EXPECT-NODA fmt-cont (bare-ident arg: measured unmappable too)
    );
    out.push_str(&s);                                   // EXPECT-DA neighbor
    let t = format!(
        "multi-line invocation where line 2+ are:",     // EXPECT-NODA fmt-cont
    );
    out.push_str(&t);                                   // EXPECT-DA neighbor
    write!(                                             // EXPECT-DA neighbor
        out,
        "write with fmt string on its own line {}",     // EXPECT-NODA fmt-cont
        v                                               // (arg; measured, not asserted)
    )
    .unwrap();
    let u = format!(
        "invalid input syntax for type {}: \"{v}\"",    // EXPECT-NODA fmt-cont
        "macaddr"                                       // EXPECT-NODA fmt-cont
    );
    out.push_str(&u);                                   // EXPECT-DA neighbor
    // BOUNDARY: an argument line that CONTAINS A CALL is real code and IS
    // mapped — the auto-classifier must never classify call-bearing lines.
    let w = format!(
        "call-bearing arg boundary {}",                 // EXPECT-NODA fmt-cont
        gen_a(v)                                        // EXPECT-DA call-arg-boundary
    );
    out.push_str(&w);                                   // EXPECT-DA neighbor
    // PLAIN (non-macro) calls: a string-literal-only continuation line in an
    // ordinary call's multi-line argument list (the `.with_hint("...")` shape,
    // mac8/src/lib.rs:323). MEASURED BOUNDARY: in this toy these lines ARE
    // mapped (DA emitted) — yet the same shape reads NO-DA in real captures
    // (mac8 lib.rs:323, regress.lcov 2026-07-30) where span refinement of a
    // long inlined method chain drops them. Mapping is therefore
    // CONTEXT-DEPENDENT for this shape: auto_exceptions.py may classify it
    // (auto:call-str-cont) ONLY with per-capture line-table evidence (no DA
    // record while the enclosing context is instrumented), never shape-only.
    out.push_str(                                       // EXPECT-DA neighbor
        "string continuation inside a plain method call",   // EXPECT-DA plain-call-str-boundary
    );
    plain2(                                             // EXPECT-DA neighbor
        "plain fn call string arg on its own line",     // EXPECT-DA plain-call-str-boundary
        gen_b(v),                                       // EXPECT-DA call-arg-boundary
    );
}

#[inline(never)]
fn plain2(s: &str, x: u64) {
    if s.len() as u64 == x {                            // EXPECT-DA neighbor
        std::process::abort();
    }
}

fn letdecl_shapes(cond: bool) -> u64 {
    let a: u64;                                         // EXPECT-NODA let-decl
    let b: u64;                                         // EXPECT-NODA let-decl
    let mut c: u64;                                     // EXPECT-NODA let-decl
    let shrunk;                                         // EXPECT-NODA let-decl (gap-4: no type ascription)
    if cond {                                           // EXPECT-DA neighbor
        a = 1;                                          // EXPECT-DA neighbor
        b = 2;                                          // EXPECT-DA neighbor
        shrunk = 7u64;                                  // EXPECT-DA neighbor
    } else {
        a = 3;
        b = 4;
        shrunk = 9;
    }
    c = a.wrapping_add(b).wrapping_add(shrunk);         // EXPECT-DA neighbor
    c = c.wrapping_mul(TABLE[0] as u64);                // EXPECT-DA neighbor
    c = c.wrapping_add(GenTwoA(c).compute())            // EXPECT-DA neighbor
        .wrapping_add(GenTwoB(c).compute())
        .wrapping_add(GenTwoC(c).compute())
        .wrapping_add((CFG.hash)(CFG.factor + CFG.bias))
        .wrapping_add(TABLE_REF[2] as u64);
    c
}

fn main() {
    let n: u64 = std::env::args().count() as u64;
    let mut out = String::new();
    fmt_shapes(n, &mut out);                            // EXPECT-DA neighbor
    let x = letdecl_shapes(n == 1)                      // EXPECT-DA neighbor
        .wrapping_add(gen_a(n))                         // EXPECT-DA neighbor
        .wrapping_add(gen_b(n))
        .wrapping_add(gen_c(n));
    println!("{} {} {}", out.len(), x, TABLE.len());    // EXPECT-DA neighbor
}
'''

TOY_INC = r'''
// toy_inc.rs — body for the include! probe. Executed code here gets spans
// pointing at THIS file, never at the include! line.
pub fn inc_hash(x: u64) -> u64 {
    x.wrapping_mul(2654435761)
}
'''

MARK = re.compile(r"//\s*(EXPECT-DA|EXPECT-NODA)\s+(\S+)")


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        sys.stderr.write(f"FAIL: {' '.join(cmd)}\n{r.stdout}\n{r.stderr}\n")
        sys.exit(1)
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--toolchain", default="1.96.0")
    a = ap.parse_args()

    home = os.path.expanduser("~")
    tcs = glob.glob(os.path.join(home, ".rustup/toolchains",
                                 a.toolchain + "-*"))
    if not tcs:
        sys.stderr.write(f"no toolchain {a.toolchain} under ~/.rustup\n")
        sys.exit(1)
    tc = tcs[0]
    rustc = os.path.join(tc, "bin/rustc")
    llvmbin = glob.glob(os.path.join(tc, "lib/rustlib/*/bin"))[0]
    profdata = os.path.join(llvmbin, "llvm-profdata")
    llvmcov = os.path.join(llvmbin, "llvm-cov")

    d = tempfile.mkdtemp(prefix="rig-auto-classes.")
    try:
        src = os.path.join(d, "toy.rs")
        open(src, "w").write(TOY)
        open(os.path.join(d, "toy_inc.rs"), "w").write(TOY_INC)
        exe = os.path.join(d, "toy")
        run([rustc, "-C", "instrument-coverage", "-O", "-o", exe, src])
        env = dict(os.environ,
                   LLVM_PROFILE_FILE=os.path.join(d, "toy.profraw"))
        run([exe], env=env)
        prof = os.path.join(d, "toy.profdata")
        run([profdata, "merge", "-sparse",
             os.path.join(d, "toy.profraw"), "-o", prof])
        lcov = run([llvmcov, "export", "-format=lcov",
                    f"-instr-profile={prof}", exe]).stdout

        da = {}
        cur_is_toy = False
        for line in lcov.splitlines():
            if line.startswith("SF:"):
                # per-file DA: toy_inc.rs (the include! body) must not
                # collide with toy.rs line numbers.
                cur_is_toy = line.strip().endswith("/toy.rs")
            elif line.startswith("DA:") and cur_is_toy:
                ln, cnt = line[3:].split(",")[:2]
                da[int(ln)] = da.get(int(ln), 0) + int(cnt)

        version = run([rustc, "--version"]).stdout.strip()
        print(f"rig-auto-classes: {version}")
        print(f"{'line':>4}  {'expect':<12} {'class':<15} {'DA':<8} verdict")
        failures = 0
        per_class = {}
        for i, text in enumerate(TOY.splitlines(), 1):
            m = MARK.search(text)
            if not m:
                continue
            expect, cls = m.groups()
            got = da.get(i)
            if expect == "EXPECT-NODA":
                ok = got is None
            else:
                ok = got is not None and got > 0
            per_class.setdefault(cls, [0, 0])[0 if ok else 1] += 1
            if not ok:
                failures += 1
            print(f"{i:>4}  {expect:<12} {cls:<15} "
                  f"{'-' if got is None else got:<8} "
                  f"{'ok' if ok else 'FAIL'}  | {text.strip()[:60]}")
        print("\nper-class (ok/FAIL):")
        for cls, (ok, bad) in sorted(per_class.items()):
            print(f"  {cls:<15} {ok}/{bad}")
        if failures:
            print(f"\nRIG RED: {failures} assertion(s) failed — "
                  f"auto-classification of the failing class(es) is NOT "
                  f"licensed on this toolchain.")
            sys.exit(1)
        print("\nRIG GREEN: all probed classes demonstrated unmappable; "
              "neighbors mapped.")
    finally:
        if a.keep:
            print(f"kept: {d}")
        else:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
