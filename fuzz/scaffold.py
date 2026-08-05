#!/usr/bin/env python3
"""scaffold.py — fuzz-target scaffold generator for the 100%-coverage campaign.

Emits the dual-exec differential fuzz target skeleton for one crate, in the
shape the done campaign crates converged on (uuid_diff / cash_diff /
strfam_diff conventions — see fuzz/fuzz_targets/ and fuzz/core/src/ in the
lane worktrees, and .claude/skills/fuzzuproof-crate/SKILL.md):

  1. fuzz/core/csrc/pg_<name>_io.c   vendored-C oracle SKELETON: provenance
                                     header + per-function TODO paste sites
                                     guarded by #error compile gates. The
                                     generator NEVER fabricates C bodies —
                                     the campaign pastes upstream C verbatim.
  2. fuzz/core/src/<name>_diff.rs    Rust driver module: selector-byte
                                     dispatch, per-function arm stubs
                                     (todo!()), the three comparison planes
                                     documented per arm (value + verdict +
                                     sqlstate), fc-wrapper-plane helpers
                                     (LocalFcinfo) that compile as-is.
  3. fuzz/fuzz_targets/<name>_diff.rs  thin libFuzzer shell.
  4. fuzz/<name>_diff.dict           dictionary stub (selector bytes + TODOs).
  5. fuzz/README-TODO-<name>_diff.md checklist to done-gate, ordered per the
                                     fuzzuproof-crate skill.
  6. Registration (idempotent): [[bin]] in fuzz/Cargo.toml, crate dep in
     fuzz/core/Cargo.toml, pub mod/pub use in fuzz/core/src/lib.rs, a
     COMMENTED-OUT .file() gate line in fuzz/core/build.rs (uncommented only
     after the verbatim C is pasted — so `cargo check` passes on the fresh
     scaffold while a half-filled shim can never silently build: the #error
     gates fire the moment the line is uncommented).

Everything generated must `cargo check` clean immediately:

  cargo check --manifest-path fuzz/Cargo.toml --bin <name>_diff

Usage (from the repo root or anywhere; paths are resolved off this file):

  fuzz/scaffold.py crates/backend/utils/adt/encode \
      --fn binary_encode:1946:encode.c \
      --fn binary_decode:1947:encode.c

  fuzz/scaffold.py crates/backend/utils/adt/encode --rows rows.tsv
      # TSV columns: function <TAB> oid <TAB> c_file   (header line optional)

stdlib-python-only. Existing files are never overwritten unless --force.
"""

import argparse
import re
import sys
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
REPO_ROOT = FUZZ_DIR.parent

ORACLE_PIN = "PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df)"
UPSTREAM_SHA = "62d6c7d3df6287f1bd83199c1a746e50d31571a0"


def die(msg: str) -> None:
    print(f"scaffold.py: error: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_rows(args) -> list[dict]:
    rows = []

    def add(fn, oid, c_file, origin):
        fn, oid, c_file = fn.strip(), oid.strip(), c_file.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", fn):
            die(f"bad function name {fn!r} in {origin}")
        if not re.fullmatch(r"\d+", oid):
            die(f"bad oid {oid!r} for {fn} in {origin} (find it in the crate's builtins.rs)")
        if not c_file:
            die(f"empty C source file for {fn} in {origin}")
        rows.append({"fn": fn, "oid": oid, "c_file": c_file})

    for spec in args.fn or []:
        parts = spec.split(":")
        if len(parts) != 3:
            die(f"--fn expects name:oid:c_file, got {spec!r}")
        add(*parts, origin=f"--fn {spec!r}")

    if args.rows:
        p = Path(args.rows)
        if not p.is_file():
            die(f"--rows file not found: {p}")
        for i, line in enumerate(p.read_text().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if i == 1 and cols[0].lower() in ("function", "fn", "name"):
                continue  # header
            if len(cols) < 3:
                die(f"{p}:{i}: expected 3 tab-separated columns (function, oid, c_file)")
            add(cols[0], cols[1], cols[2], origin=f"{p}:{i}")

    if not rows:
        die("no function rows given (use --fn name:oid:c_file and/or --rows file.tsv)")
    seen = set()
    for r in rows:
        if r["fn"] in seen:
            die(f"duplicate function row: {r['fn']}")
        seen.add(r["fn"])
    return rows


def crate_pkg_name(crate_dir: Path) -> str:
    manifest = crate_dir / "Cargo.toml"
    if not manifest.is_file():
        die(f"no Cargo.toml at {manifest}")
    in_pkg = False
    for line in manifest.read_text().splitlines():
        s = line.strip()
        if s.startswith("["):
            in_pkg = s == "[package]"
            continue
        if in_pkg:
            m = re.match(r'name\s*=\s*"([^"]+)"', s)
            if m:
                return m.group(1)
    die(f"could not find [package] name in {manifest}")


# ---------------------------------------------------------------------------
# File emitters
# ---------------------------------------------------------------------------


def gen_c_shim(target: str, base: str, crate_rel: str, rows: list[dict]) -> str:
    c_files = sorted({r["c_file"] for r in rows})
    fn_list = ", ".join(r["fn"] for r in rows)
    out = []
    out.append(f"""/*
 * pg_{base}_io.c: vendored PostgreSQL C oracle for the {target} differential
 * fuzz target (100%-coverage campaign; crate {crate_rel}).
 *
 * GENERATED SKELETON (fuzz/scaffold.py) — NOT yet a valid oracle. Every
 * TODO(scaffold) paste site below must be filled with VERBATIM upstream C,
 * and every #error compile gate removed WITH its paste, before the
 * .file("csrc/pg_{base}_io.c") line in core/build.rs is uncommented. A
 * half-filled shim can therefore never silently build or link.
 *
 * Provenance (fill in as you paste; follow csrc/pg_uuid_io.c):
 *   - Vendor sections 1..N byte-for-byte from {" / ".join(f"src/backend/utils/adt/{c}" for c in c_files)}
 *     @ postgres-src {UPSTREAM_SHA}
 *     ({ORACLE_PIN}; re-verify against the repo's vendored ground-truth
 *     checkout ../pgrust-fabled/vendor/postgres-src before pasting).
 *   - Functions to vendor: {fn_list}.
 *   - Bodies VERBATIM except documented shims; shims are PLUMBING ONLY
 *     (isxdigit/strtoul C-locale shims, ereturn -> int sentinel, fmgr
 *     PG_FUNCTION_ARGS unwrapped to plain C signatures, palloc'd results ->
 *     caller buffers, wire triples for recv/send), NEVER logic. List every
 *     shim in this header when you paste.
 *   - palloc/palloc0/repalloc/pfree -> the TLS pointer arena below (NOT
 *     bare malloc/free): models PG's memory-context reset; error paths
 *     strand allocations otherwise. Do NOT free() arena pointers by hand.
 *
 * Errcode capture follows csrc/pg_float_io.c: the shared _Thread_local
 * pg_diff_errcode (defined there) records the errcode class; map each
 * errcode this crate's C raises to a small class constant below.
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* TODO(scaffold): one class constant per distinct errcode the vendored C
 * raises, e.g.:
 *   #define PG_DIFF_ERR_INVALID_TEXT 1   (22P02)
 */

/* palloc arena shim: PostgreSQL frees these via memory-context reset; the
 * oracle mirrors that with a TLS pointer arena reset at every pg_diff_*
 * dispatcher entry, so error-path longjmp/ereturn/goto exits cannot leak.
 * (Three LSan incidents of the naive palloc->malloc mapping on 2026-07-31;
 * pattern proven on proofs/p1-lanej @ 7306d300196 — copied, not re-derived.
 * Final-exec allocations stay rooted in the arena, so LSan's exit scan is
 * quiet without any manual free().) */
#define PG_DIFF_ARENA_MAX 64
static _Thread_local void *pg_diff_arena[PG_DIFF_ARENA_MAX];
static _Thread_local int pg_diff_arena_n;

static void
pg_diff_arena_reset(void)
{{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
		free(pg_diff_arena[i]);
	pg_diff_arena_n = 0;
}}

static void *
pg_diff_palloc_impl(size_t n)
{{
	void	   *p = malloc(n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}}

static void *
pg_diff_palloc0_impl(size_t n)
{{
	void	   *p = calloc(1, n);

	assert(pg_diff_arena_n < PG_DIFF_ARENA_MAX);
	pg_diff_arena[pg_diff_arena_n++] = p;
	return p;
}}

static void *
pg_diff_repalloc_impl(void *old, size_t n)
{{
	void	   *p = realloc(old, n);
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{{
		if (pg_diff_arena[i] == old)
		{{
			pg_diff_arena[i] = p;
			return p;
		}}
	}}
	assert(!"repalloc of a pointer the arena never issued");
	return p;
}}

static void
pg_diff_pfree_impl(void *p)
{{
	int			i;

	for (i = 0; i < pg_diff_arena_n; i++)
	{{
		if (pg_diff_arena[i] == p)
		{{
			free(p);
			pg_diff_arena[i] = pg_diff_arena[--pg_diff_arena_n];
			return;
		}}
	}}
	/* abort-loud: freeing a pointer the arena never issued is a shim bug
	 * (double-free after reset, or a bare malloc that bypassed palloc). */
	assert(!"pfree of a pointer the arena never issued");
	abort();
}}

#define palloc(n) pg_diff_palloc_impl(n)
#define palloc0(n) pg_diff_palloc0_impl(n)
#define repalloc(p, n) pg_diff_repalloc_impl((p), (n))
#define pfree(p) pg_diff_pfree_impl(p)
""")
    for i, c in enumerate(c_files, 1):
        fns = [r["fn"] for r in rows if r["c_file"] == c]
        out.append(f"""
/* ==================== SECTION {i}: {c} (VERBATIM) ==================== */

/*
 * TODO(scaffold): paste here, byte-for-byte from
 * src/backend/utils/adt/{c} @ {UPSTREAM_SHA},
 * the bodies backing: {", ".join(fns)}
 * (rename with a pg_ prefix; unwrap fmgr wrappers; document every shim in
 * the file header above). Remove the #error line together with the paste.
 */
#error "SCAFFOLD-TODO({target}): verbatim C from {c} not pasted yet"
""")
    out.append(f"""
/* ========== SECTION {len(c_files) + 1}: fuzz-facing driver entries (NOT Postgres code) ===== */

/*
 * One thin pg_diff_* wrapper per fuzz arm: FIRST pg_diff_arena_reset()
 * (models PG's memory-context reset; error paths strand allocations
 * otherwise), then reset pg_diff_errcode = 0, call the vendored function,
 * return an int status (0 = ok, nonzero = error class) and write results
 * through caller-provided buffers. Shape them after csrc/pg_uuid_io.c
 * section 4, e.g.:
 *
 *   int pg_diff_uuid_in(const char *source, unsigned char *out)
 *   {{
 *       pg_uuid_t u;
 *       pg_diff_arena_reset();
 *       pg_diff_errcode = 0;
 *       if (pg_string_to_uuid(source, &u) != 0)
 *       {{
 *           pg_diff_errcode = PG_DIFF_ERR_INVALID_TEXT;
 *           return 1;
 *       }}
 *       memcpy(out, u.data, UUID_LEN);
 *       return 0;
 *   }}
 */
""")
    for r in rows:
        out.append(f"""/*
 * TODO(scaffold): int pg_diff_{r["fn"]}(...)   [oid {r["oid"]}, {r["c_file"]}]
 * (first line of the body: pg_diff_arena_reset(); — see the arena header)
 */
#error "SCAFFOLD-TODO({target}): pg_diff_{r["fn"]} driver entry not written yet"
""")
    return "".join(out)


def gen_core_module(target: str, base: str, pkg: str, crate_rel: str, rows: list[dict]) -> str:
    n = len(rows)
    sel_doc = []
    for i, r in enumerate(rows):
        sel_doc.append(
            f"//!   {i} {r['fn']}  (oid {r['oid']}, C: {r['c_file']}) — TODO(scaffold): document\n"
            f"//!     the payload this arm decodes."
        )
    sel_lines = "\n".join(sel_doc)

    arms = []
    for i, r in enumerate(rows):
        pat = f"{i} =>" if i < n - 1 else "_ =>"
        arms.append(f"        {pat} {r['fn']}_diff(payload),")
    dispatch = "\n".join(arms)

    arm_fns = []
    for r in rows:
        fn = r["fn"]
        arm_fns.append(f"""
// ---------------------------------------------------------------------------
// Arm: {fn} (oid {r["oid"]}; C source: {r["c_file"]}).
// ---------------------------------------------------------------------------

fn {fn}_diff(payload: &[u8]) {{
    let _ = payload;
    // TODO(scaffold): implement this arm ({target} conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_{base}_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe {{ pg_diff_{fn}(/* payload views + out bufs */) }};
    //        let cerr = unsafe {{ pg_diff_errcode_get() }};
    //   2. Shipped Rust core: {pkg}::{fn}(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      {pkg}::builtins::fc_{fn} via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold({target}): {fn} arm not implemented");
}}""")
    arm_bodies = "\n".join(arm_fns)

    extern_decls = "\n".join(
        f"    // TODO(scaffold): fn pg_diff_{r['fn']}(...) -> i32;   [oid {r['oid']}, {r['c_file']}]"
        for r in rows
    )

    first_fn = rows[0]["fn"]

    return f"""//! {target}: differential fuzz driver — shipped Rust `{pkg}` vs vendored
//! {ORACLE_PIN} C
//! (csrc/pg_{base}_io.c). Crate under test: {crate_rel}.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-{target}.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % {n} picks the arm:
{sel_lines}
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). C-parity keeps being carried
//! by the core comparison; the plane makes the wrapper lines execute every
//! iteration with an in-harness oracle.
//!
//! SKIPPED: TODO(scaffold) — record here every excluded row (stateful /
//! PRNG / clock / locale carve-outs) and WHY, per the fuzzuproof-crate
//! skill's exception rules.

// Scaffold state: helpers below are exercised only once the arms are
// implemented. Remove this allow together with the last todo!().
#![allow(dead_code)]

use datum::{{Datum, NullableDatum}};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_fmgr::{{LocalFcinfo, PGFunction}};

extern "C" {{
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // TODO(scaffold): declare the pg_diff_* oracle entries as you write them
    // in csrc/pg_{base}_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
{extern_decls}
}}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; verbatim from uuid_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {{
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe {{ fcinfo.set_result_mcx(m) }};
    for (i, a) in args.into_iter().enumerate() {{
        fcinfo.args[i] = NullableDatum::value(a);
    }}
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}}

/// First `n` bytes behind a by-ref result Datum. Caller contract: `d` came
/// from a wrapper that returned an `n`-byte-or-longer allocation still live
/// in the arming context (or thread-local out scratch).
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {{
    // SAFETY: caller contract above.
    unsafe {{ core::slice::from_raw_parts(d.as_usize() as *const u8, n) }}
}}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {{
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn {target}(data: &[u8]) {{
    let Some((&sel, payload)) = data.split_first() else {{
        return;
    }};
    match sel % {n} {{
{dispatch}
    }}
}}
{arm_bodies}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {{
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/{target}/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold({target}): arms not implemented yet"]
    fn seed_corpus_replays_clean() {{
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/{target}");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/{target} missing") {{
            let p = e.unwrap().path();
            if p.is_file() {{
                {target}(&std::fs::read(&p).unwrap());
                n += 1;
            }}
        }}
        assert!(n >= 30, "expected >=30 seeds, found {{n}}");
    }}

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold({target}): arms not implemented yet"]
    fn arms_smoke() {{
        // Arm 0 example: selector byte {0}, then a payload for {first_fn}.
        {target}(&[0u8]);
    }}
}}
"""


def gen_fuzz_target(target: str, pkg: str) -> str:
    return f"""#![no_main]
//! Differential: {pkg} shipped Rust vs vendored {ORACLE_PIN} C
//! — see decoder_fuzz::{target}. Scaffolded by fuzz/scaffold.py.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {{
    decoder_fuzz::{target}(data);
}});
"""


def gen_dict(target: str, rows: list[dict]) -> str:
    n = len(rows)
    sels = "\n".join(f'"\\x{i:02x}"' for i in range(n))
    return f"""# {target} dictionary (scaffolded by fuzz/scaffold.py).
# TODO(scaffold): add multi-byte tokens the parsers compare against —
# keywords, unit suffixes, type-name literals, format tokens — harvested
# mechanically from the vendored regress SQL literals (gen_seeds.sh pattern)
# plus every past divergence. CmpLog + dictionary is day-one for
# parser-shaped targets (fuzzuproof-crate skill).
# arm selectors
{sels}
"""


def gen_readme(target: str, base: str, pkg: str, crate_rel: str, rows: list[dict]) -> str:
    fn_rows = "\n".join(f"| `{r['fn']}` | {r['oid']} | `{r['c_file']}` |" for r in rows)
    return f"""# README-TODO: {target} ({crate_rel})

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: {ORACLE_PIN} — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
{fn_rows}

## 1. Vendor the C oracle (compile gate)

- [ ] Paste VERBATIM upstream C into `core/csrc/pg_{base}_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `{UPSTREAM_SHA}` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
- [ ] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
- [ ] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
- [ ] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_{base}_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/{target}.rs`)

- [ ] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [ ] fc-wrapper plane per arm via `fc_call` / `{pkg}::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
- [ ] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves).
- [ ] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin {target}` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.

## 3. Seeds, dictionary, corpus

- [ ] Extend `fuzz/{target}.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [ ] Seed `fuzz/corpus/{target}/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run {target}` — floor for any fuzz-only claim:
      >=10M execs or 24h CPU per family, all planes compared; record the
      campaign size in the ledger row.
- [ ] Ground-truth law: no divergence recorded from the vendored oracle
      alone — replay against `postgres:18.3` Docker; triage Csmith-style
      (pgrust-bug / oracle-platform-variance carve / upstream-bug).

## 5. Bookkeeping (every commit) and done-gate

- [ ] Ledger rows (`proofs/USER_FACING_FUNCTIONS.tsv`) with the standardized
      qualifier grammar; every harness in `proofs/SUITE.tsv`;
      `proofs/lint-suite-rows.py` clean.
- [ ] Flip `docs/verification/phase1-routes.tsv` statuses as functions land
      (evidence = this target's name); update the crate's claim row in
      `phase1-claims.tsv` and its `phase1-ranking.tsv` row. Pull before
      editing shared TSVs and RE-READ after any pull before writing.
- [ ] Done-gate: coverage merge 100% in-scope v2-SLOC under proof-union-fuzz
      or recorded executable exception; rendered-red-line eyeball audit;
      `cargo mutants` pilot on fuzz-only regions; replay rail in CI.
"""


# ---------------------------------------------------------------------------
# Idempotent registration edits
# ---------------------------------------------------------------------------


def register_bin(target: str, created: list, skipped: list) -> None:
    p = FUZZ_DIR / "Cargo.toml"
    text = p.read_text()
    if f'name = "{target}"' in text:
        skipped.append(f"{p} ([[bin]] {target} already registered)")
        return
    block = (
        f"\n# {target}: scaffolded by fuzz/scaffold.py — see README-TODO-{target}.md.\n"
        f"[[bin]]\n"
        f'name = "{target}"\n'
        f'path = "fuzz_targets/{target}.rs"\n'
        f"test = false\n"
        f"doc = false\n"
        f"bench = false\n"
    )
    p.write_text(text.rstrip("\n") + "\n" + block)
    created.append(f"{p} (+ [[bin]] {target})")


def register_core_dep(pkg: str, crate_rel: str, comment: str, created: list, skipped: list) -> None:
    p = FUZZ_DIR / "core" / "Cargo.toml"
    lines = p.read_text().splitlines(keepends=True)
    if any(re.match(rf"\s*{re.escape(pkg)}\s*=", ln) for ln in lines):
        skipped.append(f"{p} (dep {pkg} already present)")
        return
    dep = f'{pkg} = {{ path = "../../{crate_rel}" }}  # {comment}\n'
    # Insert at the end of the [dependencies] table (before the next [section]).
    out, in_deps, inserted = [], False, False
    for ln in lines:
        s = ln.strip()
        if s.startswith("["):
            if in_deps and not inserted:
                # Back up over blank lines so the dep sits at the table's end.
                tail = []
                while out and not out[-1].strip():
                    tail.append(out.pop())
                out.append(dep)
                out.extend(reversed(tail))
                inserted = True
            in_deps = s == "[dependencies]"
        out.append(ln)
    if in_deps and not inserted:
        out.append(dep)
        inserted = True
    if not inserted:
        die(f"could not find [dependencies] in {p}")
    p.write_text("".join(out))
    created.append(f"{p} (+ dep {pkg})")


def register_lib_mod(target: str, created: list, skipped: list) -> None:
    p = FUZZ_DIR / "core" / "src" / "lib.rs"
    text = p.read_text()
    if f"pub mod {target};" in text:
        skipped.append(f"{p} (pub mod {target} already present)")
        return
    add = (
        f"\n// {target}: scaffolded by fuzz/scaffold.py — see"
        f" ../../README-TODO-{target}.md.\n"
        f"pub mod {target};\n"
        f"pub use {target}::{target};\n"
    )
    p.write_text(text.rstrip("\n") + "\n" + add)
    created.append(f"{p} (+ pub mod {target})")


def register_build_gate(base: str, target: str, created: list, skipped: list) -> None:
    p = FUZZ_DIR / "core" / "build.rs"
    text = p.read_text()
    if f"pg_{base}_io.c" in text:
        skipped.append(f"{p} (pg_{base}_io.c gate line already present)")
        return
    # The main-oracle chain head: historically a bare `cc::Build::new()` line;
    # since the sancov refactor (PGRUST_FUZZ_CSANCOV) it is a bare `build`
    # statement line that the .file() chain hangs off. Accept either.
    m = re.search(r"^(\s*)(?:cc::Build::new\(\)|build)\n", text, re.M)
    if not m:
        die(f"could not find the main oracle cc build chain head (bare `cc::Build::new()` or `build` line) in {p}")
    indent = m.group(1) + "    "
    gate = (
        f"{indent}// COMPILE GATE ({target}, scaffold.py): uncomment ONLY after every\n"
        f"{indent}// SCAFFOLD-TODO #error paste site in csrc/pg_{base}_io.c is filled\n"
        f"{indent}// with verbatim vendored C (README-TODO-{target}.md step 1).\n"
        f"{indent}// .file(\"csrc/pg_{base}_io.c\")\n"
    )
    text = text[: m.end()] + gate + text[m.end():]
    p.write_text(text)
    created.append(f"{p} (+ commented compile-gate line)")


# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Scaffold a dual-exec differential fuzz target for one crate."
    )
    ap.add_argument("crate", help="crate path relative to the repo root, e.g. crates/backend/utils/adt/encode")
    ap.add_argument("--fn", action="append", metavar="NAME:OID:C_FILE",
                    help="one function row (repeatable), e.g. binary_encode:1946:encode.c")
    ap.add_argument("--rows", metavar="TSV",
                    help="TSV of rows: function<TAB>oid<TAB>c_file (header optional)")
    ap.add_argument("--target-name", help="override the target name (default: <cratedir>_diff)")
    ap.add_argument("--force", action="store_true",
                    help="overwrite generated files that already exist")
    args = ap.parse_args()

    crate_rel = args.crate.strip("/")
    crate_dir = (REPO_ROOT / crate_rel).resolve()
    if not crate_dir.is_dir():
        die(f"crate dir not found: {crate_dir}")
    crate_rel = crate_dir.relative_to(REPO_ROOT).as_posix()

    rows = parse_rows(args)
    pkg = crate_pkg_name(crate_dir)
    base = re.sub(r"[^A-Za-z0-9_]", "_", args.target_name or crate_dir.name)
    target = base if base.endswith("_diff") else f"{base}_diff"
    base = target[: -len("_diff")]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target):
        die(f"bad target name {target!r}")

    created, skipped = [], []

    def emit(path: Path, content: str) -> None:
        if path.exists() and not args.force:
            skipped.append(f"{path} (exists; use --force to overwrite)")
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        created.append(str(path))

    emit(FUZZ_DIR / "core" / "csrc" / f"pg_{base}_io.c",
         gen_c_shim(target, base, crate_rel, rows))
    emit(FUZZ_DIR / "core" / "src" / f"{target}.rs",
         gen_core_module(target, base, pkg, crate_rel, rows))
    emit(FUZZ_DIR / "fuzz_targets" / f"{target}.rs", gen_fuzz_target(target, pkg))
    emit(FUZZ_DIR / f"{target}.dict", gen_dict(target, rows))
    emit(FUZZ_DIR / f"README-TODO-{target}.md",
         gen_readme(target, base, pkg, crate_rel, rows))

    corpus = FUZZ_DIR / "corpus" / target
    if not corpus.is_dir():
        corpus.mkdir(parents=True)
        (corpus / ".gitkeep").write_text("")
        created.append(f"{corpus}/ (+ .gitkeep)")
    else:
        skipped.append(f"{corpus}/ (exists)")

    register_bin(target, created, skipped)
    register_core_dep(pkg, crate_rel, f"{target} (scaffold.py)", created, skipped)
    # fc-wrapper-plane plumbing deps (the proofs wrapper-level pattern run
    # natively; no-ops when already present).
    for dep_pkg, dep_rel in [
        ("datum", "crates/_support/types/datum"),
        ("types_fmgr", "crates/_support/types/fmgr"),
        ("stringinfo", "crates/_support/types/stringinfo"),
    ]:
        register_core_dep(dep_pkg, dep_rel, "fc-wrapper plane (scaffold.py)",
                          created, skipped)
    register_lib_mod(target, created, skipped)
    register_build_gate(base, target, created, skipped)

    print(f"scaffold.py: target `{target}` for crate {crate_rel} (pkg `{pkg}`, "
          f"{len(rows)} function arm(s): {', '.join(r['fn'] for r in rows)})")
    for f in created:
        print(f"  created/updated: {f}")
    for f in skipped:
        print(f"  unchanged:       {f}")
    print("next steps:")
    print(f"  1. type-check the scaffold:")
    print(f"       cargo check --manifest-path fuzz/Cargo.toml --bin {target}")
    print(f"  2. work fuzz/README-TODO-{target}.md top to bottom (verbatim C paste")
    print(f"     first; the build.rs gate line stays commented until step 1 there).")


if __name__ == "__main__":
    main()
