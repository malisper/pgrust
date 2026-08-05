#!/usr/bin/env python3
"""Line-grain exception rows for adt/varlena's post-campaign residual (p1-lanes).

Complements gen-varlena-carve-exceptions.py (the claimed whole-file/region
carves). These are the lines that survive carve + `merge-coverage.py
--auto-exceptions` after the 10M-exec/target fleet differential campaigns
and the driver extensions. Every row is adjudicated individually, with the
line-table verdict (DA record present or not) recorded in the justification
where it decided the class.

Emits the standard phase1-exceptions.tsv 7 columns.
"""
import os
import sys

CRATE = "crates/backend/utils/adt/varlena/src"
AUTHOR = "p1-lanes"

# (file, [lines], class, c_counterpart, justification)
ROWS = [
    # ---- instrument-unmappable (line table: NO DA record emitted) --------
    ("builtins.rs", [183, 195],
     "instrument-unmappable",
     "varlena.c text_larger/text_smaller",
     "`} as usize,` continuation of a multi-line call-argument expression: rustc 1.96 emits NO DA record for the shape (verified against this lane's three fuzz lcovs; siblings 181/193 are the same shape). Both real branch arms (180/182, 192/194) ARE covered by seeded one-sided witness pairs"),
    ("bytea.rs", [18, 19, 20, 26, 31, 43],
     "instrument-unmappable",
     "encode.c hextbl / hexlookup static tables",
     "head/brace/binding lines of the computed const initializers below: no DA record emitted (line-table verified)"),

    # ---- const-eval-only -------------------------------------------------
    ("bytea.rs", [21, 22, 23, 24, 32, 33, 34, 35, 36, 37, 38, 39, 41],
     "const-eval-only",
     "encode.c hextbl[] / hexlookup[] (C writes the tables literally)",
     "HEXTBL2 / HEXLOOKUP are `static ... = { while-loop }` const initializers evaluated at COMPILE time into data; no runtime instrument can observe them. Their VALUES are differentially witnessed on every hex encode/decode exec (vlbytea_diff arms 1/2/36 vs vendored encode.c)"),

    # ---- locale-carve (ratified: non-C/nondeterministic collation) -------
    ("builtins.rs", [64, 78],
     "locale-carve",
     "hashfunc.c hashtext/hashtextextended pg_strnxfrm sort-key arm",
     "the `Some(h)` return of hashtext_nondeterministic: taken only for NONDETERMINISTIC collations, which are the crate's ratified locale carve. The deterministic/C-collation arm is fuzz-covered; collid=0 error arm is proved (ledger 400/448)"),
    ("lib.rs", [182],
     "locale-carve",
     "hashfunc.c hashtext nondeterministic arm (fcinfo-free kernel)",
     "same nondeterministic-collation arm as builtins.rs:64, reached through the executor's fcinfo-free text probe kernel"),
    ("lib.rs", [115, 155, 192, 193, 194, 196, 204, 637],
     "locale-carve",
     "varlena.c varstr_cmp / texteq / text_position_next_internal non-C-collation arms",
     "collation-provider delegation lines (collation_is_deterministic seam call, varstr_cmp_locale dispatch, texteq_slow dispatch, nondeterministic text_position dispatch, text_collation_is_raw_bytes invalid/non-C arms). C-collation core fully fuzz-covered; locale paths are the crate's carve of record (name-crate precedent)"),
    ("lib.rs", [253, 254, 255, 257],
     "locale-carve",
     "varlena.c text_position_setup nondeterministic-collation ereport",
     "the 0A000 'nondeterministic collations are not supported for substring searches' arm: reachable only with a nondeterministic collation (carved). C raises the identical error"),

    # ---- encoding-carve: the ratified UTF8 database-encoding pin ---------
    ("lib.rs", [291],
     "proof-covered-unmeasured",
     "varlena.c text_length pg_database_encoding_max_length()==1 arm",
     "single-byte-encoding fast path: NOT merely carved — PROVED by ledger oids 1257/1317/1369/1381 textlen `proved(len<=8 per-encoding: LATIN1 + UTF8)` (proofs/text-slice), and the pre-existing Kani capture in proofs/coverage/files/ marks this exact line covered. The differential pins UTF8 so fuzz cannot reach it; the tree-wide Kani join converts this row to measured credit"),
    ("lib.rs", [714, 721, 726, 727, 728, 729, 730, 731, 732],
     "encoding-carve",
     "varlena.c text_position_next_internal is_multibyte_char_in_char refpoint walk (varlena.c:1300-1304)",
     "UNREACHABLE BY CONSTRUCTION under the UTF8 pin: text_position_setup sets is_multibyte_char_in_char only when encoding_max_length != 1 AND encoding != PG_UTF8 (C sets the same flag false for UTF8), so the refpoint-retry block is dead for every in-scope entry point. lib.rs:714 is C's identical `if (needle_len <= 0) return false;` defensive guard, which every in-scope caller pre-guards. Re-opens if a non-UTF8 multibyte arm is added"),
    ("unicode.rs", [24, 25, 27, 28],
     "encoding-carve",
     "varlena.c unicode_normalize_func non-UTF8 ereport",
     "'Unicode normalization can only be performed if server encoding is UTF8' (42601): unreachable under the UTF8 pin. C raises the identical error"),
    ("unicode.rs", [50, 51, 53],
     "encoding-carve",
     "varlena.c unicode_assigned non-UTF8 ereport",
     "'Unicode categorization can only be performed if server encoding is UTF8': unreachable under the UTF8 pin. C raises the identical error"),
    ("builtins.rs", [682],
     "encoding-carve",
     "varlena.c unicode_assigned error propagation",
     "the `?` unwind edge of fc_unicode_assigned's call: unicode_assigned errors ONLY on non-UTF8 encoding (unicode.rs:50-53, carved above), so the propagation edge is dead under the pin"),
    ("lib.rs", [305],
     "encoding-carve",
     "varatt.h VARATT_IS_SHORT arm of VARDATA_ANY",
     "inline_payload's SHORT (1-byte header) arm: the differential's C oracle images are plain-4B by construction (documented shim fence in fuzz/core/csrc/pg_vltext_io.c — every C entry takes (ptr,len) and builds its own 4B image), so short-header images are not fed to either side. The 4B arm is fuzz-covered. Re-opens with a short-image-passing oracle entry point"),

    # ---- defensive-c-parity ---------------------------------------------
    ("levenshtein.rs", [90],
     "defensive-c-parity",
     "levenshtein.c:158-160 (identical sub_c clamp + identical stop_column guard)",
     "PROVABLY DEAD: sub_c is clamped to ins_c+del_c immediately above, so with D=ins_c+del_c and S=sub_c<=D, max_d <= min_theo_d + S*min(m,n) - 1 forces stop_column = best_column + slack_d/D + 1 <= best_column + min(m,n) = m0 in both the n>=m (best_column 0) and m>n (best_column m-n) cases — never > m0. Verified by exhaustive sweep: m0,n0 in [1,40) x all 8^3 driver cost triples x max_d in [0,600) => ZERO reaching inputs. C carries the identical guard, so the arm is ported-in C parity, NOT dead code to delete. Good Kani infeasibility-proof candidate"),
    ("bytea.rs", [244, 245, 246, 247],
     "defensive-c-parity",
     "varlena.c byteaout escape-path `if (len > MaxAllocSize) ereport(54000)`",
     "capacity-class arm: needs an escape-mode bytea whose expansion exceeds MaxAllocSize (~1GB, i.e. >256MB of non-printable input). Not reachable at fuzz scale (per-exec inputs are capped at 2KiB so multi-MB allocations do not tank fleet throughput). C raises the identical 54000"),
    ("bytea.rs", [264],
     "defensive-c-parity",
     "varlena.c byteaout `default: elog(ERROR, \"unrecognized bytea_output setting\")`",
     "unreachable-by-GUC-validation arm: bytea_output is an enum GUC, so only the two valid values reach here (the differential drives BOTH). C's counterpart is an elog, i.e. also a can't-happen assertion"),
    ("builtins.rs", [18, 19],
     "unreachable-arm",
     "fmgr.h FunctionCallInfo->flinfo (never NULL for a catalog call)",
     "no_flinfo's panic: the cstring-returning wrappers need the FmgrInfo scratch buffer, and every catalog call site provides a resolved FmgrInfo. Direct (non-fmgr) callers use the value core instead, so the None arm cannot fire in a served query"),
    ("builtins.rs", [319],
     "unreachable-arm",
     "varlena.c byteain soft-error contract (ereturn requires an escontext)",
     "byteain returning None WITHOUT an escontext is a contract violation of the soft-error protocol (None means 'soft error already saved into the escontext'); the panic makes the impossible side loud. Both real arms (hard error, soft error with escontext) are fuzz-covered by vlbytea_diff arm 1"),
    ("builtins.rs", [425],
     "unreachable-arm",
     "varlena.c bytea_substr_no_len (calls bytea_substring with length_not_specified=true)",
     "the `?` unwind edge of fc_bytea_substr_no_len's call: bytea_substring's only error is the 22011 negative-length arm, which length_not_specified=true bypasses, so this propagation edge is dead. The 2-arg form (fc_bytea_substr) DOES take it and is fuzz-covered"),
    ("builtins.rs", [277],
     "defensive-c-parity",
     "varlena.c byteacat -> bytea_catenate palloc failure",
     "the `?` unwind edge of fc_byteacat's call: bytea_catenate errors only on an allocation/size failure (capacity class, >1GB result), not reachable at the 2KiB per-arg fuzz cap"),

    # ---- cross-crate-entry (NEW class, flagged for adjudication) --------
    ("lib.rs", [144, 145, 146, 147],
     "cross-crate-entry",
     "varlena.c bpcharfastcmp_c",
     "bpchar comparator kernel: its SQL entry points (bpchareq/bpcharcmp/... ) live in crates/backend/utils/adt/varchar, CLAIMED BY LANE p1-lanem. No adt/varlena entry point reaches it. Covered as part of that crate's family, not double-counted here"),
    ("lib.rs", [746, 747],
     "cross-crate-entry",
     "varlena.c text_position_get_match_len",
     "TextPositionState accessor consumed by the regexp/replace engines (carved out of this crate's phase-1 scope); no in-scope varlena entry point calls it — split_part/textpos use get_match_off/get_match_pos, both covered"),
    ("lib.rs", [64, 65, 66, 67, 72, 73, 74, 75, 76, 77, 79, 80, 81, 83, 84, 85],
     "cross-crate-entry",
     "postgres.h pg_detoast_datum_packed + VARDATA_ANY",
     "VarPayload::as_bytes + open_image are pub API with NO caller inside adt/varlena (verified by call-graph read): they are consumed by catalog/reloptions readers outside this crate. The toast-header arms additionally sit behind the detoast carve (same class as the pre-ruled pg_column_* carve)"),

    # ---- excluded-state: toast/detoast slice arms ------------------------
    ("lib.rs", [332, 333, 334, 335, 337, 338, 340, 341, 343, 347, 348, 354, 377, 378, 379, 380, 381, 382],
     "excluded-state",
     "varlena.c text_substring DatumGetTextPSlice / toast slice path",
     "the TOASTED-input slice arms of text_substring (detoast_attr_slice seam): reaching them needs an on-disk toast pointer, i.e. relation/toast-table state, which phase 1 excludes (the pre-ruled detoast/pg_column_* carve class). The inline (non-toasted) path — including the full clamp arithmetic and the 22011 error plane — is fuzz-covered, and the clamp arithmetic is additionally witnessed by the proved text-slice cover harnesses (ledger 877/883/936/937 notes)"),
]


def main():
    rows = []
    for path, lines, cls, cpart, why in ROWS:
        for ln in lines:
            rows.append((f"{CRATE}/{path}", ln, cls, cpart, why))
    seen = set()
    for rel, ln, cls, cpart, why in sorted(rows, key=lambda r: (r[0], r[1])):
        if (rel, ln) in seen:
            print(f"DUPLICATE ROW {rel}:{ln}", file=sys.stderr)
            continue
        seen.add((rel, ln))
        print(f"{rel}\t{ln}\t{cls}\t{cpart}\t{why}\t{AUTHOR}\tpending")
    print(f"# {len(seen)} residual exception rows", file=sys.stderr)


if __name__ == "__main__":
    main()
