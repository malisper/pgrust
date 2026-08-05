# mb/conv mutants audit — p1-lanez, 2026-07-31

Fleet job: `pgrust-mutants-audit-1785512413-556c-21594` (sha 4bb37b72cfe9,
c8g.4xlarge, wall 2276s). cargo-mutants totals: **673 mutants — 328 caught
by in-crate tests, 95 unviable, 0 timeout, 250 missed-in-crate**, all 250
flagged UNSWEPT-BY-RAIL: the differential rails live in fuzz/core
(mbconv_diff), invisible to cargo-mutants' crate-local test run — the exact
lanef/crypto precedent.

## Local rail re-sweep (mutsweep.py, this dir)

Driver: apply each missed mutant's exact span replacement, run the
mbconv_diff differential test set, revert. Test-set evolution (each round's
log in this dir):

- round0 (smoke+k1k2+bad-args+quoted): 12/67 killed — 1-2-byte paths only.
- round1 (+ sampled k3/k4 strides): 4-byte window mutants still escape
  (strides miss the narrow mapped windows).
- round2 (+ **corpus_replay rail**: the committed 7395-entry coverage-guided
  corpus replayed as a plain test — now a standing CI rail in
  fuzz/core/src/mbconv_diff.rs): **58 KILLED / 33 SURVIVED of the first 91**
  before the local run was stopped per coordinator direction (CPU-intensive
  work belongs to the fleet; local budget exceeded).

## Survivor triage (the 33 round-2 survivors, by class)

1. **Disjoint-bitfield `|`→`^` (and the get_ten `-1`→`1` in a
   verifier-shadowed dead arm)** — ~24 mutants in unicode_to_utf8word /
   utf8word_to_unicode / gb_unlinear / iso8859_1 wrappers: the OR operands
   are provably bit-disjoint (`0xc0 | ((c>>6)&0x1f)` etc.), so `|` ≡ `^` —
   **equivalent mutants, ARID**. The get_ten survivor sits on
   euc2004_sjis2004.rs:112, an exception-row line exhaustively shown
   unreachable (verifier-shadowed).
2. **pg_mb_radix_conv 3/4-byte window-bound mutants (`>`→`>=`, `||`→`&&`,
   `>`→`==`)** — ~9 mutants: NOT equivalent; they are **kill-rail witness
   gaps**, not shipped-code risk — the shipped code is verified over the
   ENTIRE per-char domain by the exhaustive k1-k4 sweeps (16.3B execs, 0
   divergences), which by construction kill every non-equivalent conversion
   mutant; the committed corpus just lacks exact-boundary MAPPED characters.
   A first boundary-witness seed pack (1955 `bw*` corpus entries: first/last
   valid + off-by-one neighbors for utf8/gb18030/euc_tw-SS2/mule 4-byte)
   killed 3 of the re-tested window mutants (indices 14, 19, 20);
   the remainder need per-map mapped-boundary derivation.

## Open targeted work items (reopen-as-work-item convention)

- W1: derive exact-boundary MAPPED characters per radix map (walk each
  PgMbRadixTree's b3/b4 window bounds, emit the lowest/highest mapped char
  per window) and seed them into fuzz/corpus/mbconv_diff — closes the
  remaining window-bound survivors mechanically.
- W2: finish the rail re-sweep for the 157 not-yet-re-swept missed mutants
  (fleet-side or overnight local; driver + logs in this dir; expected split
  matches the two classes above).
