# Vendored-C provenance audit vs REL_18_STABLE — 2026-07-28

Full fetch-and-diff of every vendored C function under proofs/ against
REL_18_STABLE (policy baseline). Verdict: NO proof verdicts invalidated.
Two value-equivalent master-drift characterizations + header nits.

## Real drift (both value-equivalent, proofs stand)

1. **bitutils** (`c_bitutils.c`): vendored pg_popcount32/64 are master's
   branchless bithack; REL_18's fallback is __builtin_popcount / byte-walk.
   Same function computed — the *proof target* (Rust intrinsics ≡ popcount)
   is unaffected; the vendored text is master-only. Re-vendor REL_18 _slow
   bodies at next touch, or keep with this note. The uncompiled
   `pg_bitutils_upstream.{c,h}` reference copies are master files with
   substantial non-vendored drift (SVE added etc.).
   [APPLIED 2026-07-28: characterization note added to the c_bitutils.c
   header (kept, per the "keep with this note" option); re-vendor of the
   REL_18 _slow bodies remains deferred to next code touch.]
2. **datetime-cmp** (`c/pg_datetime_cmp.c`): 4 date.c functions
   (date2timestamp[tz] + cross-type cmp internals) carry master's
   escontext/ereturn error-channel shape; REL_18 uses `int *overflow`.
   Comparator RESULTS proven value-equivalent (overflow=+1 ⇔ error+NOEND
   etc.); the error-transport protocol differs. Characterized here, same
   pattern as pg_lsn's drift witness.
   [APPLIED 2026-07-28: characterization note added to the
   pg_datetime_cmp.c header.]

## Header corrections owed (text only, next touch)

All corrections in this section were APPLIED 2026-07-28 (comment/doc-only
sweep; no executable logic touched):

- **ascii-case**: header says "removed from master; REL_17 source of
  record" — but pg_ascii_toupper/tolower exist BYTE-IDENTICAL in REL_18;
  crate is REL_18-conformant, no REL_17 dependence.
  [APPLIED 2026-07-28: csrc/case_shim.c header now cites REL_18_STABLE and
  retracts the "removed from master" claim; re-verified against
  REL_18_STABLE pgstrcasecmp.c (functions present, byte-identical).]
- **bytea-cmp**: REL_18 keeps these functions in varlena.c (bytea.c is a
  master-era split); bodies byte-identical — cite varlena.c for REL_18.
  [APPLIED 2026-07-28: c/pg_bytea_cmp.c header now cites REL_18_STABLE
  varlena.c (~3918-4062) and corrects the "on master (PG 18+)" wording —
  the bytea.c split is post-18 master-only.]
- Minor inert nits: net_ops.c undocumented Assert drop; json-escape loop
  index int→size_t shim undocumented; proofs/char/src/lib.rs lines 3 vs 58
  contradictory provenance comments.
  [APPLIED 2026-07-28: net_ops.c header documents the compiled-out
  Assert(bits <= ip_maxbits(dst)) in cidr_set_masklen_internal (the only
  Assert in the vendored set); json-escape pg_escape.c documents the
  size_t-vs-int inner loop index (header shim list + inline comment at the
  loop); char provenance harmonized to REL_18_STABLE in both src/lib.rs
  and csrc/char_shim.c (charin/charout verified byte-identical to
  REL_18_STABLE char.c).]

## Everything else

All other prioritized crates: IDENTICAL to REL_18 modulo documented shims
(utf8's kernels REL_18-derived despite "master" wording — PG19 removed
MULE; hash/strtoint/json-escape/int-cmp/mac/mac8/uuid/name-ascii/intout/
scalar-misc/bytea-cmp/network/bool/cash/float-cmp: zero code drift).
REL_18-claiming crates spot-verified. hex + bytea-varbit deliberately
REL_15 (documented). pg_lsn drift previously characterized.

[APPLIED 2026-07-28: the "master"-claiming vendored headers cleared above
now carry an explicit REL_18_STABLE-conformance line citing this audit:
bool, bool-parse, cash, char, float-cmp, hash, int-cmp, intout, json-escape,
mac, mac8, name-ascii, network (net_shim.c), scalar-misc, strtoint, uuid,
utf8 (pg_wchar.c; pg_wchar_kernels.c already documented its REL_18_3
cross-check). pg_lsn, hex, bytea-varbit headers were already accurate and
unchanged. Live-lane crates (geo-cmp, typcache-inst, mbconv, pseudotypes,
int-arith, jsonb-probe) were left untouched; their headers already cite
REL_18_STABLE, no corrections owed there. Uncompiled raw reference
snapshots (char/csrc-shared/*, strtoint/numutils_master.c,
utf8/wchar_full.c, bitutils/pg_bitutils_upstream.{c,h}) deliberately not
edited — they are verbatim upstream copies, not provenance-bearing
headers.]
