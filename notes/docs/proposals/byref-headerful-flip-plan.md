# By-ref Datum → header-ful-everywhere flip — execution plan

**Goal:** eliminate the invented header-LESS varlena convention so there is ONE representation (header-ful, self-describing) exactly like PostgreSQL. This deletes the whole class of header-ful/header-less disambiguation bugs (text-INSERT, EXTRACT, aclitem, numeric/array/jsonb facets) by construction.

**Design (confirmed):** `RefPayload::Varlena` (crates/types-fmgr/src/boundary.rs:42-46) carries the HEADER-FUL image. fmgr-core carries it verbatim (delete all strip/restamp). Each adt core's `arg_*` helper reads via `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` (type-known, deterministic — faithful to PG); `ret_*` prepends a 4-byte header (`set_varsize_4b`). Structured types already header-ful (numeric/jsonb/range/multirange) drop to verbatim. Canonical heap `Datum::ByRef` is already header-ful — unchanged.

**Reference template:** `crates/backend-utils-adt-formatting/src/fmgr_boundary.rs` is ALREADY half-flipped (arg reads `&image[VARHDRSZ..]`, write restamps) — it is the proven pattern every other crate's helper should match.

**RECOMMENDATION:** keep cores emitting 4-byte (not short 1-byte) headers so the wire/send strips that use fixed `VARHDRSZ` stay valid.

**Scope:** 228 `RefPayload::Varlena(` WRITE sites + 81 `as_varlena()` READ sites tree-wide. ~110 uniform per-crate helper edits + ~12 fmgr-core boundary deletions + 3 heuristic deletions + 2 special cases (inet, name).

**ATOMIC:** a half-flip (header-ful producer + still-header-less consumer) breaks runtime. Work on a feature branch; build must stay COMPILING at every commit; merge to main ONLY when the full smoke matrix is green.

## A. crates/types-fmgr/src/boundary.rs
Redocument `RefPayload::Varlena` invariant to "header-ful image". No struct change.

## B. crates/backend-utils-fmgr-core/src/lib.rs — DELETE heuristics + carry verbatim
- `byref_to_headerless_payload` (def ~2299) — DELETE; callers (~2331,2437,2492,2871) carry verbatim.
- `byref_payload_for_typlen` (~2487) + `proc_arg_typlens` (~2539) + `datum_to_ref_arg_typed`/`owned_typed` (~2503,2518) typlen lane — DELETE (verbatim removes the need).
- `byref_element_ondisk_image` (~2934) restamp — Varlena arm carry verbatim (cores now RETURN header-ful); keep Cstring NUL-append + Composite passthrough. Callers ~2602 (ref_out_to_datum), ~2969 (array element word).
- `tuple_value_to_arg` ~2323, `datum_to_ref_arg` ~2423: ByRef arm → `RefPayload::Varlena(b.to_vec())` verbatim.
- `oid_output_function_call_datum_seam` ~2852, `oid_output_function_call_array` ~3931 (uses `varhdrsz_of` ~3939): carry verbatim — DELETE varhdrsz_of strip.
- `elem_to_arg` ~3808, `array_send_function_call_seam` ~3987, anyarray wrap ~2262: ALREADY verbatim — become correct once cores read header-ful; leave verbatim.
- WIRE/SEND result strips that hardcode `image.get(VARHDRSZ..)`: ~2376 (oid_send), ~3767 (record_column_send), ~4010 (array_send) — strip the send-fn RESULT (a bytea image) to wire payload — protocol-correct, STAY. Keep cores emitting 4B headers so fixed `VARHDRSZ` stays valid.
- `input_function_call_for_heap_form_seam` ~2808 / `fmgr_out_element_word` ~2965: already verbatim — no change.

## C. crates/types-datum/src/varlena.rs
DELETE `varsize_4b_of` (~48), `varsize_1b_of` (~69), `varhdrsz_of` (~85) once their B consumers are gone. KEEP `set_varsize_4b`, the Varlena codec.

## D. PER-CRATE adt helper flips (the bulk; ~1 read + ~1 write helper each)
Text-family (header-LESS today → strip on read, prepend header on write): `arg_*` reads `VARDATA_ANY` off the header-ful image (with 4B-only producers, skip `[4..]`); `ret_*` builds `set_varsize_4b(4+payload.len()) ++ payload`. Crates (file = src/fmgr_builtins.rs unless noted):
int, float, varchar, char, oid, datetime, name (typlen>0 fixed, NameData 64B — must carry a varlena-framed image; verify round-trip), enum, like, uuid, cash, varbit (+ varbit-header framing), varlena (+ string_agg arg_text/ret_text), ascii, quote, mac, mac8, oracle-compat, mb-mbutils, misc, misc2, cryptohashfuncs, pseudotypes, regexp, xml, dbsize, acl, tsginidx, xid8funcs, geo-ops, amutils, format-type, version, ruleutils, arrayfuncs (+ element image). Cross-tree consumers: backend-libpq-be-fsstubs, backend-executor-execSRF/pg_input_error_info.rs, probe-adt-scalar-bool, backend-catalog-catalog, backend-access-hashfunc, backend-access-brin-minmax-multi, backend-replication-slotfuncs, backend-replication-logical-origin, backend-utils-time-snapmgr, backend-utils-misc-guc-funcs, backend-utils-misc-more, backend-catalog-objectaddress, backend-commands-dbcommands, backend-commands-collationcmds, backend-access-transam-xlogfuncs, types-nodes (1 read), backend-utils-init-miscinit, backend-utils-adt-version.

## E. ALREADY HEADER-FUL (verbatim) — STOP STRIPPING ONLY
numeric (arg_numeric/ret_numeric verbatim — but numeric arg_varlena/ret_varlena ARE text-family → flip per D; agg_fmgr.rs too), jsonb (arg_jsonb_image/ret_jsonb verbatim; arg_text_payload → D), jsonfuncs (jsonb reader verbatim / json reader → D), rangetypes (verbatim), multirangetypes (verbatim), json (arg_text_payload/ret → D; `varlena_full_image` re-attach heuristic — DELETE).

## F. SPECIAL CASE — network/inet (the soundness hole)
crates/backend-utils-adt-network/src/fmgr_builtins.rs `arg_inet`/`ret_inet`: inet/cidr is typlen==-1 but today carries an 18-byte HEADER-LESS canonical image. Under header-ful: `ret_inet` writes `set_varsize_4b(4+18) ++ to_datum_bytes()`; `arg_inet` reads `from_datum_bytes(&image[4..])`. This is the ONLY typlen==-1 ambiguity (header-ful numeric vs header-less inet) — header-ful-everywhere RESOLVES it. `ret_text` → D.

## G. crates/backend-utils-adt-formatting/src/fmgr_boundary.rs
ALREADY half-flipped (the reference pattern). Re-verify only; no change.

## H. Array build/container
crates/backend-utils-adt-arrayfuncs construct.rs (text element), foundation.rs (set_varsize): already emit 4B header-ful element images — consistent once cores read header-ful. Keep 4B-only (no short packing at the boundary) so wire strips stay fixed-VARHDRSZ-valid.

## GATE (before any merge to main)
`cargo build --bin postgres` + `seams-init` + `no-todo-guard` green, then the full smoke matrix live (`-c io_method=sync`): SELECT 1; count(*) pg_class=415; `'bool'::text||'X'`=boolX; length('hello')=5; substring('hello' from 2 for 3)=ell; ARRAY int/text/nested; jsonb `[]`/`{}`/`[1, 2]`/`{"a": 1}`; numeric 1.5 & 0.5+0.25=0.75; CREATE/INSERT/SELECT text table; `'ab'::bpchar < 'ac'::bpchar`=t; EXTRACT YEAR=2001 / SECOND=25.575401. Merge to main ONLY when ALL green.
