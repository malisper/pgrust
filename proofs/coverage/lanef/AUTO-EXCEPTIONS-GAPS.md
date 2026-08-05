# auto-exceptions classifier gaps found by p1-lanef (2026-07-30)

Ran main's `merge-coverage.py --auto-exceptions` (accelerator @ b8fdf77aac)
over both lane-F captures (crypto/hash 12M-exec + tables 3M-exec confirm
lcovs; combined scope 434 v2-SLOC, 402 fuzz-covered, 32 residual).

**Result: 4 of 32 residual lines auto-classified** (3 `auto:table-head`,
1 `auto:fmt-cont` with `--line-table-lcov` supplied — without a line table
that row lands `no_table_evidence`, per the NEEDS_TABLE_EVIDENCE rule).
Zero false positives: every auto row matches the hand-adjudicated note
already recorded in `proofs/coverage/exceptions.tsv`.

The other 28 are real shapes the current rules don't reach. All were
hand-adjudicated by this lane; recorded here so the classifier can grow
(each is a candidate for rig licensing, none is a lane blocker):

1. **`macro_rules!` DEFINITION body rows** — `crates/common/hmac/src/lib.rs:23-27`
   (`pub struct $name($ctx);`, `const BLOCK_LENGTH: usize = $block;`,
   `type Digest = [u8; $digest];`). The `auto:macro-decl` rule fires only for
   rows inside a generator-macro *invocation* block (`shapes.gen_decl_lines`);
   declaration rows inside the macro's own definition body are the same
   instrument shape (no DA for macro-generated const/type items).
2. **Multi-line macro invocation argument continuations** —
   `crates/common/hmac/src/lib.rs:41-48` (`hmac_hash!(Sha224, PgSha256Ctx,
   init_sha224, final_sha224,` / `    pg_sha2::PG_SHA224_BLOCK_LENGTH, ...);`).
   The head line carries the macro name so it is not a bare remainder, and
   `enclosing_paren` classifies the continuation as neither fmt nor call.
3. **Brace-initializer static/const heads and `include!` module rows** —
   `crates/common/keywords/src/lib.rs:6,30-35,38-41` and
   `crates/common/unicode_category/src/lib.rs:7`. `table_heads` recognizes
   multi-line **bracket** (`[`) initializers; a `pub static X: T = T {`
   brace-initializer head and its field rows, plus
   `include!(concat!(env!("OUT_DIR"), "/x.rs"));`, are the same
   compile-time-data shape.
4. **`let` declaration without type ascription** —
   `crates/common/hmac/src/lib.rs:61` (`let shrunk;`). `RE_LET_DECL` requires
   `let x: T;` (mandatory `:` type); the un-annotated form (type inferred from
   a later branch assignment, the deferred-init idiom) emits no code either.

Net for this lane: hand adjudication was still required for 28/32 lines, so
the accelerator saved roughly an eighth of the residual work here. Closing
gaps 1/3 alone would cover 22 of the 28.
