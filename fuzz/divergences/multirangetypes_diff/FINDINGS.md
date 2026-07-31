# multirangetypes_diff — findings (lane p1-laneac)

Oracle: verbatim PostgreSQL 18.3 (upstream sha 62d6c7d3df) in
`fuzz/core/csrc/pg_multirangetypes_io.c` (one TU with the range oracle).

## Result: no pgrust divergence found

2.5M-exec release smoke (arm64, macOS, `PGRUST_FUZZ_CSANCOV=1`, 537 committed
seeds + dictionary, 111 s): **zero crashes, zero value/verdict/sqlstate
divergences** across all 11 arms x 3 instantiations. The ≥10M-exec CI cluster
campaign is the remaining gate; nothing blocks it.

Everything below is a HARNESS defect or a documented carve, not a pgrust bug.
Decoded in that order per the FAILED-is-not-a-verdict law.

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
