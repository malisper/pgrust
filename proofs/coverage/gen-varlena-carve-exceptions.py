#!/usr/bin/env python3
"""Generate line-grain carve exception rows for adt/varlena (lane p1-lanes).

Emits phase1-exceptions.tsv rows (same 7 columns) for the crate's claimed
carve of record (docs/verification/phase1-claims.tsv row adt/varlena):

  whole-file carves (class excluded-state, citing the function-grain
  excluded() ledger rows):
    - src/string_agg.rs      agg context/state (ledger 3535/3536/3543/3544/6299/6300/6301)
    - src/split_text.rs      SRF tuplestore + array builder (376/394/6160/6161)
    - src/replace_regexp.rs  regex engine / pattern cache (regexp-crate engine dep)
    - src/concat_format.rs   variadic-any + fmgr output recursion (3058/3059/3539/3540)
    - src/abbrev.rs          sortsupport state + HyperLogLog (3255 unported sortsupport)

  region carves inside otherwise-IN files (function-name-bounded spans):
    - builtins.rs string_agg wrappers        excluded-state
    - builtins.rs pg_column_* + typlen seam  excluded-state (pre-ruled carve)
    - builtins.rs fc_icu_unicode_version     excluded-state (ICU FFI seam, 6099)
    - lib.rs varstr_cmp_locale + non-C arms  locale-carve
    - lib.rs get/set_bytea_output+init_seams excluded-state (GUC/seam plumbing)

Line universe = SLOC v2 (tables excluded), via proofs/coverage/sloc_rules.py.
Rows go to stdout; append to proofs/coverage/phase1-exceptions.tsv at gate
time (pull + re-read first).
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
from sloc_rules import sloc_lines  # noqa: E402

CRATE = "crates/backend/utils/adt/varlena"
AUTHOR = "p1-lanes"

WHOLE_FILE = [
    ("src/string_agg.rs", "excluded-state",
     "varlena.c string_agg_transfn/string_agg_finalfn/string_agg_combine/string_agg_serialize/string_agg_deserialize",
     "agg context/state carve of record: aggregate transition state + AggCheckCallContext machinery; ledger excluded(blocked: agg context/state) oids 3535/3536/3543/3544/6299/6300/6301"),
    ("src/split_text.rs", "excluded-state",
     "varlena.c text_to_array/text_to_table (split_text engine)",
     "SRF tuplestore + array-builder carve of record; ledger excluded(blocked) oids 376/394/6160/6161"),
    ("src/replace_regexp.rs", "excluded-state",
     "varlena.c replace_text_regexp",
     "regex-engine dependency (pattern cache / adt_regexp engine crate); phase-1 filter names pattern caches OUT; consumed by regexp crate entry points, none of this crate's in-scope oids reach it"),
    ("src/concat_format.rs", "excluded-state",
     "varlena.c concat_internal/text_concat/text_concat_ws/text_format",
     "variadic-any + fmgr output-function recursion carve of record; ledger excluded oids 3058/3059/3539/3540"),
    ("src/abbrev.rs", "excluded-state",
     "varlena.c varstr_abbrev_convert/varstr_abbrev_abort",
     "sortsupport abbreviation state (HyperLogLog cardinality trackers); bttextsortsupport 3255 excluded(port gap)"),
]

# (file, start_pat, end_pat, class, c_counterpart, justification)
# span = [line matching start_pat, line BEFORE the first line matching
# end_pat at or after start). Patterns anchor on fn definitions/comments.
REGIONS = [
    ("src/builtins.rs",
     r"^// string_agg_transfn / bytea_string_agg_transfn",
     r"^pub fn fc_unistr",
     "excluded-state",
     "varlena.c string_agg fc wrappers",
     "agg-state carve: fc wrappers over src/string_agg.rs; same ledger rows as the whole-file carve"),
    ("src/builtins.rs",
     r"^// C icu_unicode_version",
     r"^pub fn fc_unicode_assigned",
     "excluded-state",
     "varlena.c icu_unicode_version",
     "ICU FFI seam (ledger 6099 excluded(blocked: ICU FFI seam))"),
    ("src/builtins.rs",
     r"^// varlena\.c: pg_column_size/pg_column_compression/pg_column_toast_chunk_id",
     r"\Z",
     "excluded-state",
     "varlena.c pg_column_size/pg_column_compression/pg_column_toast_chunk_id",
     "pre-ruled ranking carve: pg_column_* system fns + cached_arg_typlen typcache seam (detoast/typlen catalog dependency); ledger candidates 1269/2121/6316 deferred to a toast-hdr proofs lane"),
    ("src/lib.rs",
     r"^fn varstr_cmp_locale",
     r"^pub fn text_cmp",
     "locale-carve",
     "varlena.c varstr_cmp non-C collation path",
     "ratified locale carve: non-C collation comparison delegates to locale provider; C-collation core covered"),
    ("src/lib.rs",
     r"^pub fn get_bytea_output",
     r"^pub fn split_identifier_string",
     "excluded-state",
     "guc.c bytea_output + seam registration",
     "GUC/seam plumbing (get/set_bytea_output, init_seams): session-state wiring, not computation; fuzz drives set_bytea_output as environment pinning only"),
]

# Extra single-function locale carves in lib.rs / builtins.rs that sit inside
# otherwise-covered code: matched per-function.
FN_CARVES = [
    ("src/lib.rs", r"^fn text_position_next_nondeterministic", "locale-carve",
     "varlena.c text_position_next_internal nondeterministic-collation arm",
     "nondeterministic-collation search arm; C/deterministic core covered"),
    ("src/lib.rs", r"^fn texteq_slow", "locale-carve",
     "varlena.c texteq non-C collation arm",
     "non-C collation equality fallback"),
    ("src/builtins.rs", r"^pub\(crate\) fn hashtext_nondeterministic", "locale-carve",
     "varlena.c hashtext nondeterministic-collation arm",
     "nondeterministic-collation hash arm"),
]


def fn_span(lines, start_idx):
    """Span of a brace-balanced item starting at start_idx (0-based)."""
    depth = 0
    seen = False
    for i in range(start_idx, len(lines)):
        depth += lines[i].count("{") - lines[i].count("}")
        if "{" in lines[i]:
            seen = True
        if seen and depth <= 0:
            return start_idx + 1, i + 1  # 1-based inclusive
    return start_idx + 1, len(lines)


def emit(path, lo, hi, cls, cpart, why, rows):
    rel = f"{CRATE}/{path}"
    v2 = sloc_lines(os.path.join(REPO, rel), rule="v2")
    for n in sorted(v2):
        if lo <= n <= hi:
            rows.append((rel, n, cls, cpart, why))


def main():
    rows = []
    for path, cls, cpart, why in WHOLE_FILE:
        emit(path, 1, 10**9, cls, cpart, why, rows)
    for path, spat, epat, cls, cpart, why in REGIONS:
        full = open(os.path.join(REPO, CRATE, path)).read()
        lines = full.splitlines()
        s = next((i for i, l in enumerate(lines) if re.match(spat, l)), None)
        assert s is not None, (path, spat)
        if epat == r"\Z":
            e = len(lines)
        else:
            e = next((i for i in range(s + 1, len(lines)) if re.match(epat, lines[i])), len(lines))
        emit(path, s + 1, e, cls, cpart, why, rows)
    for path, pat, cls, cpart, why in FN_CARVES:
        lines = open(os.path.join(REPO, CRATE, path)).read().splitlines()
        s = next((i for i, l in enumerate(lines) if re.match(pat, l)), None)
        assert s is not None, (path, pat)
        lo, hi = fn_span(lines, s)
        emit(path, lo, hi, cls, cpart, why, rows)
    seen = set()
    for rel, n, cls, cpart, why in rows:
        if (rel, n) in seen:
            continue
        seen.add((rel, n))
        print(f"{rel}\t{n}\t{cls}\t{cpart}\t{why}\t{AUTHOR}\tpending")
    print(f"# {len(seen)} carve rows generated", file=sys.stderr)


if __name__ == "__main__":
    main()
