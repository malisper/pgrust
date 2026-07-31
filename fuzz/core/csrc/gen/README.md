# csrc/gen — lane assemblers for the vendored C oracles

`pg_multirangetypes_io.c` is GENERATED, not hand-written. Its assembler lives
here so the generator is versioned with its output (the range oracle's
`assemble.py` was left in an ephemeral scratchpad, so its output could not be
reproduced from the repo — this directory fixes that for the multirange half).

    # verbatim upstream extracts, sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0
    #   multirangetypes.{c,h} rangetypes.c arrayfuncs.c arrayutils.c
    #   sort_template.h  (fetch into the same directory as assemble_mr.py)
    python3 assemble_mr.py ../pg_multirangetypes_io.c

`mr_header.h.in` (provenance + shim documentation + the `#include` of the
range oracle) and `mr_entries.c.in` (the `pg_diff_mr_*` driver entries) are the
only hand-written parts; every function body is extracted verbatim by name.

NOTE: `pg_rangetypes_io.c` carries ONE additive edit for this oracle — the
`TypeCacheEntry.rngtype` field, marked in place. Re-apply it if that file is
regenerated from its own assembler.
