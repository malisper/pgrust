# Bug report: CBMC models the C `NAN` macro as a signaling NaN (0x7ff0000000000001), violating C99 and diverging from every real compiler

For: https://github.com/diffblue/cbmc/issues

## Summary

CBMC's `<math.h>` model expands the `NAN` macro to the bit pattern
`0x7ff0000000000001` — exponent all-ones, **quiet bit (bit 51) clear**,
payload 1 — i.e. a *signaling* NaN. This is wrong on two counts:

1. **C99 §7.12p5** requires `NAN` to expand to "a constant expression of
   type float representing a **quiet** NaN."
2. Every mainstream implementation (gcc and clang, on x86-64 and aarch64,
   with glibc and Apple libm) evaluates `(double) NAN` to the canonical
   quiet NaN `0x7ff8000000000000`.

Any verification that compares NaN *bit patterns* between code using the
`NAN` macro and anything else (hardware ground truth, a Rust `f64::NAN`
under Kani, or two C functions where only one uses the macro) produces
counterexamples that do not exist on real silicon.

## Version

CBMC 6.8.0 (`cbmc --version`: `6.8.0 (cbmc-6.8.0)`), as bundled with
Kani 0.67.0. Observed on aarch64-apple-darwin; the native comparison
value is the same (`0x7ff8000000000000`) on x86-64/glibc.

## How this was found

We run machine-checked equivalence proofs between PostgreSQL's C source
and a Rust port of it: Kani 0.67.0's `-Z c-ffi` mode compiles the
verbatim C (via goto-cc) and the Rust (via MIR codegen) into one
goto-program, and a harness asserts both implementations produce
bit-identical outputs for **all** inputs up to stated bounds. Because the
theorems quantify over the full f64 domain, they exercise inputs ordinary
testing never reaches — including infinities and specific NaN payloads.

While proving PostgreSQL's `point_distance` (the `<->` operator), CBMC
produced a counterexample: with `y = -Inf` on both points, `dy` becomes
`Inf - Inf = NaN`, flows into the hypot path, and the C side's returned
NaN had different bits than Rust's `f64::NAN`. Our standing rule is that
no counterexample is accepted until it replays natively — and it did not
replay: compiling the same verbatim C with clang and running the same
inputs, **both sides return the identical canonical quiet NaN**
`0x7ff8000000000000`. The divergence exists only inside the model.

Bisecting model vs. reality: the C path reaches its NaN through
PostgreSQL's `get_float8_nan()`, which is literally
`return (float8) NAN;` — so the suspect was the constant itself. The
minimal reproducer below (no PostgreSQL, no Rust, plain `cbmc`) confirms
the `NAN` macro is the defect.

This is easy to miss in normal use: `isnan()` is true either way, so
only workloads comparing NaN **bit patterns** — e.g. differential
verification of two implementations — ever observe it. NaN *propagation*
through arithmetic (`Inf - Inf`, `NaN * x`) models correctly; only the
header constant is wrong.

## Reproducer

```c
#include <math.h>
#include <assert.h>

int main(void)
{
    union { unsigned long long u; double d; } v, w;
    v.d = (double) NAN;
    w.d = (double) NAN;

    /* 1: real compilers (clang/gcc, x86-64 and aarch64) give the
       canonical quiet NaN */
    assert(v.u == 0x7ff8000000000000ULL);

    /* 2: two evaluations of the macro are the same value */
    assert(v.u == w.u);

    /* 3: the quiet bit is set (C99 7.12p5: NAN is a QUIET NaN) */
    assert((v.u >> 51) & 1);

    return 0;
}
```

```
$ cbmc nan_repro.c
[main.assertion.1] line 12 assertion v.u == 0x7ff8000000000000ULL: FAILURE
[main.assertion.2] line 15 assertion v.u == w.u: SUCCESS
[main.assertion.3] line 18 assertion (v.u >> 51) & 1: FAILURE
VERIFICATION FAILED

$ clang -O2 nan_repro.c && ./a.out && echo ok    # all three pass natively
ok
```

`--trace` shows the modeled value explicitly:

```
v.d=+NAN (01111111 11110000 00000000 00000000 00000000 00000000 00000000 00000001)
v.u=9218868437227405313ul    // 0x7ff0000000000001 — a signaling NaN
```

## Expected behavior

`(double) NAN` models as a quiet NaN — ideally the canonical
`0x7ff8000000000000` that gcc/clang produce on x86-64 and aarch64, but at
minimum any pattern with bit 51 set (the C99 quiet-NaN requirement, which
assertion 3 checks portably).

## Actual behavior

`0x7ff0000000000001` — a signaling NaN. Assertion 3 (the standard's own
requirement) fails.

## Where it lives in the source

`src/util/ieee_float.cpp`: NaN is tracked internally as a boolean
(`make_NaN()` sets `NaN_flag=true` with `fraction=0`), and `pack()`
materializes any NaN as max exponent plus `result += 1` — the minimal
valid NaN pattern, quiet bit clear, with no comment. The lines date to
the repo's 2011 initial import (original CPROVER float model) and were
last touched by a 2012 rename. Since payload semantics are never used
internally, emitting the quiet bit in `pack()` (fraction top bit instead
of / in addition to 1) looks like a low-risk one-line fix.

## Possibly related

- The macro's expansion appears to be division-based: evaluating `NAN`
  trips the "NaN on division" property check, so standard-conforming
  code that merely uses the constant is flagged by NaN-check properties.
  That looks adjacent to #8634 ("Divide by +INFINITY raises NaN failure
  with --nan-check even when numerator is a finite number"), though that
  issue is about the check, not the constant's value.
- Through Kani's `-Z c-ffi` path (goto-linked cross-language program),
  two evaluations of `(double) NAN` in separate C functions were not
  even equal to each other (the payload half varies per call). In pure
  CBMC (this reproducer) the value is deterministic, so that part may be
  a Kani-layer interaction — happy to split it into a Kani issue if
  that's more appropriate.

## Workaround

Shim the header constant before including the code under verification
(bodies untouched):

```c
#undef NAN
static inline double canonical_nan(void)
{
    union { unsigned long long u; double d; } n;
    n.u = 0x7ff8000000000000ULL;
    return n.d;
}
#define NAN (canonical_nan())
```
