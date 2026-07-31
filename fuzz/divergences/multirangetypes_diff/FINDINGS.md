# multirangetypes_diff — findings (lane p1-laneac)

Oracle: verbatim PostgreSQL 18.3 (upstream sha 62d6c7d3df) in
`fuzz/core/csrc/pg_multirangetypes_io.c` (one TU with the range oracle).

## Result: one conformance divergence (D1, ruling requested); no correctness divergence

2.5M-exec release smoke (arm64, macOS, `PGRUST_FUZZ_CSANCOV=1`, 537 committed
seeds + dictionary, 111 s): **zero crashes, zero value/verdict/sqlstate
divergences** across all 11 arms x 3 instantiations. The ≥10M-exec CI cluster
campaign is the remaining gate; nothing blocks it.

Everything below is a HARNESS defect or a documented carve, not a pgrust bug.
Decoded in that order per the FAILED-is-not-a-verdict law.

## D1 (pgrust-bug, conformance/cosmetic — RULING REQUESTED) — nummultirange canonicalization keeps a different value-equal numeric representative than C

**CI-found** (job pgrust-fuzz-campaign-1785514852-46a9-13019, sha 5217706fa2,
2,103,447 execs, cov_lines 5204). Reproducer:
`D1-multirange_in-numrange-dscale-tie.bin` (selector 0x00 = text io, type tag
2 = nummultirange).

    cd fuzz && cargo +nightly fuzz run multirangetypes_diff --release \
        ../fuzz/divergences/multirangetypes_diff/D1-*.bin   # now PASSES (fallback)

**Symptom.** `multirange_in` on
`{[20,0204),[\t0,2),[1,2),[3,42),[\t1,2),[3,42),[\t1,2),[3,42),[3,4),[3,42\n),[3,44),[\t1,2),[1,2.0000)}`
produced images differing in EXACTLY ONE BYTE at offset 25: C `0x80`, Rust
`0x82` — a numeric short header whose low nibble is the dscale: **dscale 0 in C
(`2`), dscale 4 in Rust (`2.0000`)**. Both denote the same multirange.

**Mechanism.** `multirange_canonicalize` sorts input ranges with
`range_compare`, which compares numeric bounds BY VALUE, so `2` and `2.0000`
TIE. C sorts with `qsort_arg`, which is UNSTABLE, and `range_union_internal`
returns a fixed side on a value tie; pgrust's canonicalize uses a STABLE sort
(`slice::sort_by`). When the input carries two value-equal-but-byte-different
numeric bounds, the two implementations keep different (value-equal)
representatives. The bound bytes are copied verbatim from the parsed input in
both — neither re-serializes — so the divergence is purely which representative
survives the tie.

**Ground truth (docker `postgres:18.3`, container pg183ac):**

    '{[1,2),[1,2.0000)}'::nummultirange            -> {[1,2.0000)}
    '{[1,2.0000),[1,2)}'::nummultirange            -> {[1,2)}
    '{[1,2),[1,2.0000),[1,2.00)}'::nummultirange   -> {[1,2.00)}
    '{[1,2.00),[1,2.0000),[1,2)}'::nummultirange   -> {[1,2)}
    full fuzz literal                              -> {[0,2),[3,204)}

So C's surviving representative is itself order-dependent (unstable qsort): the
"same" multiset in a different textual order yields a different printed dscale.

**Why this is a genuine divergence, not a non-surface.** `multirange_out` prints
the surviving representative, so the TEXT OUTPUT is user-visible and differs
(`{[1,2)}` vs `{[1,2.0000)}`). It is value-preserving (both are the same
multirange under every operator and under `=`), so the class is
**conformance/cosmetic**, not a correctness bug — but it is observable, so it is
not a non-surface the harness may wave away.

**My earlier carve note was the assumption that failed.** The module header
claimed "numrange bounds ... are minted from INTEGER literals only, so numerics
that compare equal are byte-identical." That holds for the arms the driver
BUILDS, but NOT for `multirange_in` / `multirange_recv`, whose bounds are user
bytes — and the driver's own text seed corpus mints `2.0000`-style literals.
Corrected in the module header.

**RULING REQUESTED (match or carve).** Whether pgrust must reproduce C's
`qsort_arg` tie-break bit-for-bit is a ruling, not the harness's call:
  * MATCH — change `multirange_canonicalize` to break value ties the way C's
    unstable qsort + fixed-side union does. This is a real behavior change to
    shipped sort semantics and I did NOT make it on my own judgement. Note C's
    own output is order-of-input-dependent, so "match C" means "match C's exact
    unstable qsort pivot sequence", which is a strong and brittle requirement.
  * CARVE — declare the surviving representative of a value tie a
    ratified non-surface (GL-PARMERGE-1 within-tie precedent), on the ground
    that it is value-preserving and only the cosmetic dscale of the printed
    representative differs. This is what the harness currently assumes.

**Harness handling until the ruling (committed).** `compare_mr_image` keeps
byte-exact comparison as the default and mandatory check. It relaxes to a
value-level comparison ONLY when: (a) the images differ in bytes, AND (b)
t == 2 (a byte difference on int4/int8 multirange has no numeric representation
to differ and is always a hard divergence), AND (c) the shipped
`multirange_cmp` certifies the two images are equal in range COUNT, per-range
FLAGS, and every bound VALUE. Any structural difference (dropped/added/reordered
range, wrong flag, wrong value) still hard-fails. Each relaxation is counted and
the tally is printed on a cadence (2.5M-exec local smoke: **238 fallbacks /
2,097,152 execs ≈ 0.011%**, never on a byval instantiation). If the ruling is
MATCH, delete the fallback and this becomes a banked bug; if CARVE, the fallback
is the ratified handling. An in-crate test (`numeric_representation_tie_D1`)
asserts the fallback path is live and never fires for byval.

## H1 (BLOCKS THE SIBLING TARGET) — `rangetypes_diff` builds malformed numrange images

**Not this target; not a pgrust bug. `rangetypes_diff` (lane p1-laneac's other
half, branch `proofs/p1-laneac`) cannot pass a CI cluster campaign until it is
fixed.** Reproducer banked here:
`H1-rangetypes_diff-numrange-image-padding-SEGV.bin` (selector 0x5d -> arm 5
`arm_ops`, type tag 2 = numrange).

    cd fuzz && cargo +nightly fuzz run rangetypes_diff --release \
        ../fuzz/divergences/multirangetypes_diff/H1-*.bin
    => AddressSanitizer: SEGV in numeric_cmp+0x1c

`fuzz/core/src/rangetypes_diff.rs::build_image` hand-serializes a range image
and, for a byref bound, pads to the element alignment before writing the value:

```rust
Bound::Num(bytes) => {
    while img.len() % 4 != 0 { img.push(0); }
    img.extend_from_slice(bytes);
}
```

PG never emits that. `range_serialize` writes bounds through
`datum_compute_size` / `datum_write`, whose FIRST arm is:

```c
if (TYPE_IS_PACKABLE(typlen, typstorage) &&
    VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
    /* convert to a short varlena header, and count NO alignment */
```

numeric is packable (typstorage 'm'), so a small numeric bound is stored with a
1-byte SHORT header and **no alignment padding at all**; even in the non-packed
arm, `att_align_datum` skips alignment when the datum is already short. The
hand-built image therefore carries 1-3 zero pad bytes where the bound should
start. `range_deserialize` reads those pad bytes as the varlena header, derives
a garbage bound pointer, and `numeric_cmp` dereferences it -> SEGV.

Why it appears only now: the P1 `range_recv` SEGV (fixed at 3c129c2bb6) used to
abort the run before the fuzzer reached the numrange image arms. With P1 fixed,
250k execs over the committed corpus find this immediately.

**Fix (what this target does):** stop hand-serializing. Build range images
through the SHIPPED constructor — `fc_range_constructor3` with a `"[)"`-style
flags text and NULL args for infinite bounds — so the bytes are exactly what
`numrange(1,2,'[)')` produces and builder/serializer skew is impossible. See
`build_range_image` in `fuzz/core/src/multirangetypes_diff.rs`. (Replicating
`datum_write`'s packing rule in the harness instead would work but re-creates
the same class of drift.)

**Scope note for the range half:** rangetypes_diff deliberately fuzzes ARBITRARY
flag bytes over hand-built images, which is a real and valuable surface
(range_deserialize's full flags lattice). Only the BYREF-bound layout is wrong.
The minimal repair is to keep the arbitrary-flags builder for byval
instantiations (int4range/int8range/daterange, where the padding rule is plain
alignment and the current builder is correct) and take numrange images from the
shipped constructor.

## H2 — three oracle defects in this target, found and fixed during smoke

1. The verbatim multirange bodies call `lookup_type_cache`, `get_type_io_data`
   and `fmgr_info_cxt` with MULTIRANGE and RANGE oids; the range oracle's mocks
   `elog` on anything but the three ranges and their scalar elements, so every
   arm returned class 99. Fixed by rename shims (`pg_mr_lookup_type_cache` etc.)
   that resolve the new oids and delegate everything else unchanged, so the
   range oracle's own bodies keep their originals.
2. `pg_diff_mr_accessors` copied its byref result through `PG_DETOAST_DATUM`,
   expanding a packed-short numeric bound that `multirange_lower/upper` (and the
   shipped code) return as-is. Now copied via `VARSIZE_ANY` exactly as returned.
3. The detoast seam was uninstalled, so `multirange_constructor2`'s array
   detoast panicked. The SHIPPED `detoast_attr` is installed (seam =
   environment, detoast logic = computation, never mocked).
4. Oracle buffers were sized 8 KiB while the text arm's output is not bounded by
   its input: one `1e16383` bound expands to ~16 KB through numeric_out. Fixed
   by reused 4 MiB thread-local scratch buffers plus a 192-byte literal cap, so
   overflow is unreachable rather than skipped (a size-conditional skip would be
   a vacuous pass).

## C1 — sqlstate carve: `multirange_constructor1`'s NULL-member guard

C uses `elog(ERROR, "multirange values cannot contain null members")` (XX000,
oracle class 99) under the comment *"This check should be guaranteed by our
signature, but let's do it just in case"*; pgrust raises 22004
(`ERRCODE_NULL_VALUE_NOT_ALLOWED`, class 13). Same defensive refusal, and
NEITHER is SQL-reachable: the builtins are registered strict, so fmgr never
delivers a NULL. The arm is still driven (the shipped line executes and is
covered) but only the error VERDICT is compared.

Class: `defensive-c-parity`. C counterpart: `multirangetypes.c`
`multirange_constructor1` / `multirange_constructor2` NULL-member guards.
Conformance nit worth a follow-up: pgrust could use `PgError::error` here to
match XX000 exactly. `adt_multirangetypes::builtins::null_member`.

## C2 — preallocation carve: `multirange_recv` wire counts > 4096

`multirange_recv` preallocates `range_count` pointers before validating the rest
of the message. C does the same (`palloc(range_count * sizeof(RangeType *))`),
so the ORDERING is C-parity. Only the allocators' reaction to an absurd size
differs: C's palloc succeeds under MaxAllocSize (the oracle's arena always
does), PgVec's fallible reserve fails and surfaces an alloc-size error. Resource
surface, not a value surface; already recorded as this lane's P2. Counts up to
4096 keep the whole wire-parsing surface under full comparison, including the
zero-length element that was P1.

## Ratified non-surface — within-tie order in canonicalization

C canonicalizes with `qsort_arg` (vendored verbatim here) and the shipped Rust
with a stable sort, so for two input ranges that compare EQUAL with DIFFERENT
bytes the surviving representative after the merge is an ordering artifact
(GL-PARMERGE-1 precedent: within-tie order is the ratified non-surface). The
driver removes the ambiguity by construction rather than asserting over it —
flags normalized through `wf_flags`, and numrange bounds entering a multirange
minted from integer literals so value-equality implies byte-equality. The
dscale-diverse numeric surface still rides the r x mr RANGE operand.
