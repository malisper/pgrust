# Soft-error (escontext) plane — wave 2 queue (proofs/softerror-plane-2)

Independent re-enumeration 2026-07-31 (grep errsave|ereturn|escontext|
ErrorSaveNode over shipped src/), cross-referenced against the wave-1 survey
(proofs/softerror-plane WAVE2-CHARTER.md) and against which fuzz targets
actually COMPARE a soft plane at origin/main 0b8be188d4.

Already covered (do not re-charter): rangetypes, multirangetypes (laneac);
jsonb/json/jsonpath (proofs/jsonb-soft-plane, fleet-confirmed); formatting +
network (sibling proofs/softerror-plane, HANDS OFF); arrayfuncs, arrayutils,
rowtypes, uuid, pg_lsn, numutils, adt_timestamp/adt_date (closeout diff),
bool core-grain (fc shape was hard-only — armed by THIS lane).

## THIS lane's work queue (target exists, plane missing) — hit counts = soft
## arms in shipped src

1. adt/geo — 74 hits, 7 SQL input fns (point/box/path/polygon/line/lseg/
   circle), geo_io_diff passed None at 7 call sites. Branch proofs/sp2-geo.
2. adt/numeric — 21 hits; numeric_in + apply_typmod(_special); harness
   numericfam.rs:574 was `.expect("no escontext")` (soft path FORBIDDEN by
   the harness until now). Blast radius maximal (numeric everywhere).
   Branch proofs/sp2-num.
3. Bulk one-arm batch — cash (8), mac (5), mac8 (4), float (24 incl. fmgr-
   frame excluded rows), bool fc-shape (4). Targets cash_diff, mac_diff,
   float_in_diff, bool_diff. Branch proofs/sp2-bulk.

## Backlog: escontext arms with NO diff target at all (needs target first;
## counts from this re-enumeration)

- adt/regproc — 28 (regprocin family; SQL-reachable via casts)
- adt/acl — 30 (aclitemin)
- contrib/isn — 28 (isn family in fns)
- adt/varchar — 18 / adt/varlena — 11 / adt/varbit — 8
- adt/int — 14 + adt/int8 — 3 (int2in/int4in/int8in soft path unexercised
  ANYWHERE directly; only via array/range element input)
- adt/xml — 11 (xml_in; libxml seam)
- adt/tsvector_core — 8 / adt/tsquery_core — 8 (NOTE: a tsquery instance
  once killed the whole server on deep nesting — when a target is built,
  the recursion guard on the soft path MUST be verified release-effective)
- adt/scalar — 7 (oidin family), adt/adt_enum — 7 (enum_in needs catalog),
  adt/domains — 7 (needs typcache/session), adt/xid8funcs — 8,
  adt/expandedrecord — 2
- contrib: cube 6, intarray 4, hstore 2, ltree 2, seg 2
- non-adt consumers (session-grain, out of phase-1 pure-fn scope but the
  plane's ultimate consumers): executor/execexpr 69 (domain checks),
  commands/copy 15 (ON_ERROR ignore), parser/parse_utilcmd 11,
  statistics/stats_import 2

Ranking rationale: (a) recorded/excepted soft lines convertible to measured,
(b) SQL blast radius of a soften-vs-throw divergence (pg_input_is_valid /
COPY ON_ERROR ignore are user-facing), (c) cheapness (target exists >>
target must be built). int/int8 + regproc + varchar/varbit are the highest-
value backlog items for a wave 3 because their input fns are ubiquitous.
