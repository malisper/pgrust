# ltree_diff injection sweep (lane p1-ltree-t74, task #74)

Gate item: plant one defect per comparison plane, confirm the plane flags it,
revert. Run AT PLANE CREATION. Honest counts, all rounds recorded — 5 of 22
planted defects were MISSED when this was run late elsewhere
(CAMPAIGN-INTELLIGENCE §C), so a plane that never failed has never been shown
to compare anything.

Instrument: `cargo test -p decoder_fuzz --test ltree_replay`, which replays
every committed `fuzz/corpus/ltree_diff` input through the driver. CAUGHT =
the rail goes red with the defect applied. Driver: scratchpad/inject2.py
(round 1: inject.py).

## Round 4 — R1 on-disk adoption controls (branch `final/ltree-cexact`, 2026-08-03)

Three plants, one per piece of the C-exact on-disk adoption, each disarmed
ALONE and re-armed. Instrument here is the dedicated witness test
`ltree_diff::tests::r1_cexact_ondisk_wrap_band_image` plus `smoke_arms` /
`fixed_defect_shapes`, all through `cargo test -p decoder_fuzz --release --lib
ltree`. 3 planted / 3 CAUGHT / 0 missed.

| plane | planted defect (disarm) | verdict |
|---|---|---|
| value/image | `LVAR_OFF_NAME` 7 → 8 (re-verified independently of round 3) | CAUGHT — `smoke_arms` + `fixed_defect_shapes` both red; C `1,0,0,97` vs port `1,0,0,0,97` on `a\|b` |
| value/image | serialize the FULL variant Vec instead of the wrapped `numvar` | CAUGHT — only by the 65,536-variant expander seed; the 4k-variant shapes pass, so this plant is what proved the numvar seed load-bearing |
| value/image | level stride `MAXALIGN(real totallen)` instead of the STORED uint16 | CAUGHT — only by the MULTI-level wrap seeds (`…\|a.b.c\|d`); single-level shapes pass, which is what forced the second seed family |

Both misses-by-construction above are the useful part of this round: the first
version of the witness test used only single-level 4k-variant shapes and BOTH
plants passed it. The seeds were widened until each plant fails, so the test now
witnesses all three pieces rather than one.

## Round 3 — final, 14 planted / 14 CAUGHT / 0 missed

| plane | planted defect | verdict |
|---|---|---|
| value/image | ltree level `len` word off by one at len 3 | CAUGHT |
| value/image | `LVAR_OFF_NAME` 7 → 8 (the real on-disk bug this target found) | CAUGHT |
| value/image | subltree emits a tidy empty ltree instead of C's pointer-walk image | CAUGHT |
| verdict | drop `\v` from the C-locale isspace set (parser accept/reject flips) | CAUGHT |
| sqlstate | ltxtquery "operand is too long" 22023 → 42601 | CAUGHT |
| soft-error | disable the hard/soft whitelist (demote elog/54001/22021 to soft) | CAUGHT |
| crc value | invert the `ctype_is_c` fold-arm selection | CAUGHT |
| crc value | one byte of the ASCII fold (`q` unfolded) | CAUGHT |
| out text | `deparse_ltree` returns empty above 900 bytes | CAUGHT |
| hash | `hash_ltree` combine `<<5 - x` → `<<5 + x` | CAUGHT |
| boolean | `inner_isparent` compares label LENGTHS instead of bytes | CAUGHT |
| int | `ltree_index` start off by one after the negative-start fold | CAUGHT |
| array | `ndim > 1` reject → `ndim > 2` (multidim arrays accepted) | CAUGHT |
| wire | send version byte 1 → 2 | CAUGHT |

## Round 2 — 11 planted / 10 CAUGHT / 1 MISSED

Same set, run before the whitespace seeds were added; 3 plants failed to
apply (stale anchors) and were rewritten for round 3.

MISS: **verdict plane, `\v` removed from the C-locale isspace set.** Decoded
as a SEED GAP, not a plane gap. `isspace` is consulted only in ltxtquery's
`WAITOPERAND` state; the corpus had `a\x0bb`, where `\v` is reached in
`INOPERAND`/`WAITOPERATOR` instead and never touches that branch. Closed by
seeding all six C-locale whitespace bytes (plus `\x1c`, `\x85` near-misses)
in every WAITOPERAND position — leading, after `&`/`|`/`!`, after `(`,
doubled, and trailing — hard and soft, 144 inputs. Re-planted in round 3:
CAUGHT.

## Round 1 — 5 planted / 2 CAUGHT / 3 MISSED

Run before the corpus existed, against the driver's unit tests only. Two of
the three misses were BAD PLANTS, not plane gaps, and are recorded as such
rather than counted against the planes:

- "sqlstate" plant changed only the error MESSAGE — message text is
  explicitly out of scope for the harness contract, so MISSED is the correct
  behavior. Re-planted as a real sqlstate change in round 3.
- "crc value" plant mapped `Z` → `z` inside the ASCII-tolower arm, which
  `to_ascii_lowercase` already does: an equivalent mutant. Re-planted as
  `q` → `Q` in round 3.
- The `\v` miss was the genuine one (see round 2).

## Standing note

The rail's power is its corpus. Any new arm or plane added to `ltree_diff`
re-runs this sweep, and a MISS is triaged to either "add a seed" or "add a
plane" before the arm counts as covered.
