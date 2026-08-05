# proofs/scalar-datum — datum.c copy/serialize kernels (lane p1-lanep)

Kani C≡Rust dual-execution family for `adt/scalar/src/datum_ops.rs`:
`datum_get_size`, `datum_copy`, `datum_transfer`, `datum_is_equal`,
`datum_estimate_space`, `datum_serialize`, `datum_restore`.
C oracle: `c/pg_datum.c` — verbatim REL_18_STABLE `datum.c` bodies
(byte-identical to the campaign's Stamp-18.3 pin for these functions);
full shim census in that file's header.

## Ledger qualifier wording (per function)

- **datum_get_size** — `proved(by-val word full-symbolic at typlen
  {1,2,4,8}; fixed-len typlen spot cells {1,2,4,7,8,42,6666,32767};
  varlena 1B+4B full-symbolic header; external tags {1,2,3,18}; cstring
  len<=8 sym w/ pinned terminator; NULL-ptr + bad-typlen error arms
  verdict+sqlstate; modulo error-message stubs)`.
- **datum_is_equal** — `proved(by-val full u64xu64; fixed-len 4 sym images;
  varlena 1B sym sizes <=5 both paths; both verdicts cover-witnessed)`.
- **datum_estimate_space** — `proved(null/by-val/fixed-len spot cells +
  varlena sym header; expanded arm fenced)`.
- **datum_copy** — `proved(fixed-len 5 image, 56.4s DEFAULT solver; by-val
  + varlena-1B cells fleet-bound; modulo static-buffer allocator model
  both sides; expanded-object arm out of scope, trap-guarded)`.
- **datum_transfer / datum_serialize / datum_restore / roundtrip** —
  harnesses WRITTEN AND COMPILE, fleet-bound (local multi-lane load walls
  them; see SUITE.tsv notes). Do not record as local verdicts.

## Fences / documented deviations (NOT divergences)

1. **Expanded-object arms out of scope** (DatumGetEOHP / EOH_flatten_into /
   TransferExpandedObject): session machinery. Fenced by construction in
   every harness AND trap-guarded on the C side (`pg_proof_eoh_reached`
   asserted 0) — reachability insurance, not just a fence.
2. **Undefined vartag**: C `VARTAG_SIZE` yields 0 (Assert compiled out) so
   `VARSIZE_ANY` returns 2; Rust `varatt::vartag_size` panics. Fenced to
   tags {1,2,3,18}; out-of-fence difference = pgrust hardening.
3. **datum_restore corrupt/short input**: C memcpy's garbage (Assert out);
   Rust carries release asserts. Domain fenced to well-formed images;
   difference = pgrust hardening.
4. **By-val out-of-contract typlen**: Rust `debug_assert!(typLen in 1..=8)`
   is CHECKED under Kani; C's Assert compiles out. Domain = C contract
   literals. The debug_assert is a ported-in constraint (debug-assert
   masking law applies if it ever guards a release defect — here the
   by-val arm's behavior is identical either way: returns typlen).

## Measured traps re-confirmed (2026-07-31, shared laptop load)

- `assume(typlen>0)` does NOT prune dead `-1`/`-2` deref arms — symex
  expands CStr/varlena machinery on an unconstrained pointer and hangs.
  Literal case cells fixed it (1.5s). Third-plus rediscovery of the
  assume-vs-literal law.
- CBMC bounds-checks C union derefs (`varattrib_4b`) against the WHOLE
  union object: symbolic headers need >= sizeof(union) backing bytes
  (`HdrBuf`, 16B aligned(8)) or you get fabricated
  "pointer outside object bounds" on a 4-byte buffer.
- Solver inversion on many-assert harnesses: `eq_dcopy_fixedlen` kissat
  fake-walls (>120s), DEFAULT solver proves in 56.4s.
- goto-cc Unit-vs-void: `datumSerialize`'s void return rides as int.

## Run

```sh
cd proofs/scalar-datum
timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_datum.c \
    --solver kissat --harness proofs::<h> --exact
# ctx-creating harnesses (copy/serialize/restore): DEFAULT solver, 150s+,
# or the fleet kani-suite runner (preferred — see SUITE.tsv tiers).
```

Negative control: `control_dgs_fixedlen_skew_must_fail` — confirmed FAILED
on exactly `assert!(r == c)` (DEFAULT solver, 2026-07-31).

## Not covered here

- `strtoul_c` (adt/scalar lib.rs): deliberately skipped — covered
  differentially by the lane's `scalarxid_diff` fuzz target through tidin,
  and a symbolic strtoul model proof would duplicate proofs/vector-io's
  established glibc strtol10 shim work. Routes row should point at the
  fuzz target.
