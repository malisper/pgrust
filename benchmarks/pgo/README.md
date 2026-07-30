# PGO training corpus

Profile-guided optimization needs a workload to train on. This directory holds
that workload, the proof that it is **not** the workload we publish numbers
for, and the machinery that keeps the two apart.

## Why this exists

A PGO binary is shaped by whatever it was profiled running. If the profile run
executes the exact statements the published benchmark later measures, the
published number is partly a measurement of the training, not of the engine —
the compiler has been handed the answer key. That is the ordinary
train-on-test failure, and it is invisible from the outside unless the training
corpus is published alongside the binary.

So: the training corpus lives in the source tree, under version control, and a
lint refuses to build if any training statement is a published measurement
statement.

## What is here

| path | role |
|---|---|
| `corpus/analytics-hits.sql` | 48 analytical statements over the wide web-log fixture, one per line |
| `corpus/analytics-hits-engines.tsv` | per-statement execution-engine class (drives the two vectors) |
| `corpus/oltp-generic.sql` | the transactional statement-class list of record |
| `corpus/oltp-schema.sql` | fixture DDL for the transactional corpus |
| `gen-corpus.sh` | expands the corpus into the files a profile run consumes, then lints the expansion |
| `denylist/analytics-official.sql` | verbatim published analytical measurement vector — **denied** |
| `denylist/oltp-official.sql` | statement templates of the published transactional rigs — **denied** |
| `denylist/EXEMPTIONS.txt` | statements permitted despite being denied, each with a reason (currently empty) |
| `lint-training-overlap.sh` / `.awk` | the proof: training ∩ denied = ∅ |

The denylist files are inputs to the lint only. Nothing in the build reads them
as training material.

## The line this draws

**Enforced mechanically: no literal overlap.** No statement that a published
benchmark issues may appear in the training corpus, in any binding. The lint
canonicalizes both sides — string literals, numbers, `?`, `$1` and `:name` all
collapse to a single token, harness `RESET ALL;` / `SET …;` prefixes are
stripped, case is folded, punctuation is spaced and whitespace collapsed — so
what is compared is the statement's identifier-and-operator skeleton. Two
statements collide only if they read the same columns from the same tables
under the same operators. Changing a search term, a range bound, a table
suffix, or a parameter binding does not get a denied statement past the lint;
that is deliberate, because a parameterized family is one statement.

**Disclosed, not enforced: shape-class overlap.** The corpus deliberately
exercises the operator classes the product is measured on — grouped
aggregation, per-group distinct, top-N, substring and regexp predicates,
fixed-width string sorts, point lookups, indexed updates. There is no honest
way to avoid this: choosing a training workload *is* choosing which code paths
get optimized, and a corpus that exercised nothing the product is used for
would produce a worse binary and a less honest number. What we can and do
avoid is training on the literal queries. The distinction to state when
quoting a number: the binary was trained on the *kinds* of work the benchmark
does, never on the benchmark's own statements.

## Shape classes covered

Analytical corpus (`corpus/analytics-hits.sql`, ids `A1`…`A48`):

| ids | class |
|---|---|
| A1–A2 | full-relation scan with a trivial or single-predicate count |
| A3–A4, A7 | scalar aggregate batteries; min/max over a timestamp |
| A5–A6 | global duplicate elimination over a wide integer and over a string |
| A8, A13, A16 | single-key grouped count, low / medium / very high cardinality |
| A9–A12, A14 | grouped per-group distinct over integer and string keys |
| A15, A17–A19 | composite group keys, including a derived (`extract`) key and the bare-`LIMIT` no-`ORDER BY` admission path |
| A20 | selective equality probe on a wide integer |
| A21–A23 | substring and negated-substring predicates; string `MIN` under them |
| A24–A27 | top-N: full-width projection, string projection, sort on the string itself, two-key sort |
| A28–A29 | byte-length aggregates with a `HAVING` threshold; regexp-derived group key |
| A30 | wide projection fan-out of expression aggregates |
| A31–A33 | composite-key grouped aggregate batteries, filtered and unfiltered |
| A34–A36 | long-string group at high cardinality; ordinal group reference; arithmetic-expression group keys |
| A37–A38 | date-range plus boolean-flag conjunctions feeding string groups |
| A39–A43 | deep `OFFSET`, `CASE`-derived group key, `IN`-list membership, narrow-integer composite keys, `date_trunc` group key |
| A44–A48 | dilution shapes with no counterpart in any published vector: sorted distinct, multi-column filtered top-N, grouped string-length statistics, scalar subquery, nested grouped aggregate |

Transactional corpus (`corpus/oltp-generic.sql`, ids `O1`…`O20`): unique-key
lookup, bounded key-range scan, range-fed scalar aggregate, range-fed sort on a
fixed-width string, range-fed sorted distinct, secondary-index range count,
indexed-column update, non-indexed wide-column update, balance update with
in-transaction read-back, small- and very-small-relation contended updates,
append-only insert, delete/re-insert pair, whole-relation count, whole-relation
top-N, constant projection, multi-row `VALUES` insert, bulk `COPY`, and
explicit `BEGIN`/`COMMIT` blocks wrapping the above.

### Engine classes

`corpus/analytics-hits-engines.tsv` assigns each analytical statement one of
`ser` (legacy engine, no parallelism), `rt` (scan + aggregate + sort pools
armed), `rta` (aggregate pool only), `mpwpg0` (no parallel workers, product
default engine) or `default` (planner's choice). Assignment is **by shape
class** — it is not copied from any published per-query vector. The mix exists
so the profile covers the serial kernels, the morsel-runtime pools, and the
planner-default path rather than only whichever one the current planner
happens to pick.

## Running it

```sh
pgo/lint-training-overlap.sh              # lint the checked-in corpus
pgo/gen-corpus.sh /tmp/corpus             # expand, then lint the expansion
pgo/lint-training-overlap.sh <file>...    # lint arbitrary training text
```

Exit 0 = disjoint (prints a `PROOF` line naming both corpus sizes). Exit 1 =
overlap, with the offending statement, the denylist file that caught it, and
the canonical form both share. Exit 2 = the lint could not run — never treat
that as a pass.

`gen-corpus.sh` is deterministic: given the same output directory it produces
byte-identical files (fixed LCG seeds, no clock, no randomness). The one
path-dependent line is the `COPY` source, which names the emitted `copy.dat`.

### Reproducing a shipped binary

A PGO artifact is reproducible from (source sha, training corpus). Both are in
this tree, so:

1. check out the sha the binary was built at;
2. `pgo/gen-corpus.sh <dir>` — this also re-runs the lint, so the non-overlap
   proof is part of the reproduction, not a claim about it;
3. build instrumented, run the profile passes over `<dir>`, merge, rebuild with
   the merged profile.

The build recipe records the corpus hash next to the binary, so an auditor can
check that the corpus in the tree is the corpus the binary was trained on.

## Adding to the corpus

Add the statement to `corpus/`, give it an id and a one-line shape-class
comment, add its engine class if it is analytical, and run the lint. If the
lint rejects it, the statement is a published measurement statement — rewrite
it against different columns or predicates rather than adding an exemption.
Exemptions in `denylist/EXEMPTIONS.txt` are for statements that are genuinely
content-free; every one that is applied is printed, so none is silent.

Any change here changes the profile, and therefore the binary. Treat it like a
codegen change: it needs a measured before/after, not just a green lint.

## When a corpus statement crashes the engine

It happens — a corpus that is not the benchmark reaches code the benchmark
does not. `corpus/QUARANTINE.tsv` holds shape ids that are kept out of the
emitted training vectors, one per line with the reason and the defect to close.
A quarantined statement stays in `corpus/`, stays linted, and stays documented;
`gen-corpus.sh` prints a loud `QUARANTINED <id> — <reason>` line for every
exclusion, so it appears in the build log and cannot rot into silence.

Quarantine is for statements the engine cannot currently survive. It is not for
statements that are slow, awkward, or inconvenient, and a line with no defect
named is a bug in the process.
