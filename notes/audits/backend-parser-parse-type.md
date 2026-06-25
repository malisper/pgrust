# Audit: backend-parser-parse-type (`src/backend/parser/parse_type.c`)

Audited commit: `95c41aadc` (branch `port/backend-parser-parse-type`).
Source: PostgreSQL 18.3 `src/backend/parser/parse_type.c` (821 lines).
Cross-checked against `../pgrust/c2rust-runs/backend-parser-medium1/`.

Verdict: **PASS**. Every C function MATCHes or is faithfully SEAMED per the seam
rules; zero seam findings; all gates green.

## Function inventory

| # | C fn (line) | Port (lib.rs) | Verdict | Notes |
|---|-------------|---------------|---------|-------|
| 1 | `LookupTypeName` (37) | `LookupTypeName` (121) | MATCH | thin wrapper, `temp_ok=true`. |
| 2 | `LookupTypeNameExtended` (72) | `LookupTypeNameExtended` (132) + `finish_lookup` (273) | MATCH | All 4 branches (names==NIL / pct_type / normal / tail). `%TYPE` switch arms 1-4 + default match C exactly; arrayBounds Assert->`debug_assert!`; NOTICE not errposition'd. Array-type branch applies `get_array_type` then OID-valid check via shared tail. |
| 3 | `LookupTypeNameOid` (232) | `LookupTypeNameOid` (298) | MATCH | missing_ok->Err(UNDEFINED_OBJECT) vs InvalidOid; reads `->oid`. |
| 4 | `typenameType` (264) | `typenameType` (327) | MATCH | not-found + shell errors, both UNDEFINED_OBJECT. |
| 5 | `typenameTypeId` (291) | `typenameTypeId` (359) | MATCH | |
| 6 | `typenameTypeIdAndMod` (310) | `typenameTypeIdAndMod` (371) | MATCH | |
| 7 | `typenameTypeMod` (332, static) | `typenameTypeMod` (382) | MATCH | no-typmods early return; shell + InvalidOid typmodin SYNTAX_ERRORs; value-node decode (Integer `%ld`/Float string/String string; ColumnRef-identifier folds into String per trimmed node model); `!cstr` SYNTAX_ERROR; construct_array_builtin+OidFunctionCall1 via `fmgr::typmodin` seam (carries the errposition callback). |
| 8 | `appendTypeNameToBuffer` (439, static) | `appendTypeNameToBuffer` (463) | MATCH | dotted names / internal `format_type_be` / `%TYPE` / `[]`. |
| 9 | `TypeNameToString` (478) | `TypeNameToString` (495) | MATCH | |
| 10 | `TypeNameListToString` (492) | `TypeNameListToString` (503) | MATCH | comma-joined. |
| 11 | `LookupCollation` (515) | `LookupCollation` (516) | MATCH | `get_collation_oid(..., false)`; errposition callback is the established no-live-push idiom (location-tagging only). |
| 12 | `GetColumnDefCollation` (540) | `GetColumnDefCollation` (534) | MATCH | collClause / precooked collOid / type default; uncollatable DATATYPE_MISMATCH. ColumnDef projected to `ColumnDefInput`. |
| 13 | `typeidType` (578) | `typeidType` (573) | MATCH | cache-miss elog. |
| 14 | `typeTypeId` (590) | `typeTypeId` (581) | MATCH | NULL->internal elog. |
| 15 | `typeLen` (599) | `typeLen` (594) | MATCH | |
| 16 | `typeByVal` (609) | `typeByVal` (599) | MATCH | |
| 17 | `typeTypeName` (619) | `typeTypeName` (604) | MATCH | NameStr trim-at-NUL (pstrdup->owned String). |
| 18 | `typeTypeRelid` (630) | `typeTypeRelid` (610) | MATCH | |
| 19 | `typeTypeCollation` (640) | `typeTypeCollation` (615) | MATCH | |
| 20 | `stringTypeDatum` (654) | `stringTypeDatum` (622) | MATCH | typinput + getTypeIOParam + `OidInputFunctionCall` via `fmgr::input_function_call` (OID-resolving variant == OidInputFunctionCall; None cstring = C NULL). |
| 21 | `typeidTypeRelid` (668) | `typeidTypeRelid` (650) | MATCH | |
| 22 | `typeOrDomainTypeRelid` (689) | `typeOrDomainTypeRelid` (661) | MATCH | domain-chase loop, TYPTYPE_DOMAIN=`'d'`. |
| 23 | `pts_error_callback` (718, static) | (folded into driver seam) | SEAMED | the `errcontext("invalid type name")` is carried by `raw_parse_type_name`'s grammar drive. |
| 24 | `typeStringToTypeName` (738) | `typeStringToTypeName` (684) + `fail_type_string` (717) | MATCH | empty/whitespace `strspn`==`strlen` -> fail; SETOF reject -> fail; both `ereturn(SYNTAX_ERROR "invalid type name")`. raw_parse via driver seam. |
| 25 | `parseTypeString` (785) | `parseTypeString` (728) | MATCH | soft = escontext present (always ErrorSaveContext); not-found + shell ereturns. |
| (h)| `getTypeIOParam` (lsyscache.c:2443) | `getTypeIOParam` (641) | MATCH | local copy of the 4-line helper (lsyscache's is private); body identical (typelem else oid). Benign duplication, not a divergence. |

## Seam audit

Owned inward seam crate: `backend-parser-parse-type-seams` -- 8 declarations
(`parse_type_string`, `name_list_to_string`, `typename_type_id{,_node}`,
`typename_to_string{,_node}`, `lookup_type_name_oid`, `type_name_list_to_string`).
All 8 installed by `init_seams()` (set-only); `init_seams()` wired into
`seams-init::init_all()`. Verified by `seams-init` recurrence guards (green).

Outward seams -- each justified by a real dep cycle, thin marshal+delegate:
- `syscache` (`pg_type_form`, `get_type_oid`) -- syscache is a downstream owner.
- `fmgr` (`input_function_call`; NEW `typmodin` = construct_array_builtin(CSTRINGOID)
  + OidFunctionCall1) -- fmgr owner; the cstring-array build + call live behind the
  seam (no branching on the parse_type side beyond the value-node decode, which
  is parse_type's own logic). fmgr-fmgr-seams is guard-exempt.
- `small1` (`parser_errposition`) -- parse_node.c owner.
- `format-type-seams` (`format_type_be`, `format_type_be_owned`).
- NEW `backend-parser-driver-seams::raw_parse_type_name` -- grammar (gram.y) not
  ported; mirror-PG-and-panic. Listed CONTRACT_RECONCILE_PENDING +
  DESIGN_DEBT TD-PARSETYPE-RAWGRAMMAR; recurrence guard exempts
  `(backend_parser_driver, raw_parse_type_name)` until gram.y lands. Correct.

No outward seam contains branching/node-construction/computation beyond
marshalling. No body replaced by a "somewhere else" seam.

## Design conformance

- `Type` = value-copied `FormData_pg_type` via `pg_type_form` (ReleaseSysCache
  implicit) -- mirrors lsyscache's accessor; no invented opacity.
- Allocating paths take `Mcx` + return `PgResult`; errors mirror C SQLSTATE +
  severity. No shared statics, no ambient globals, no registry side tables, no
  locks across `?`.
- errposition-callback no-live-push (LookupCollation / schema-lookup branch) is
  the repo's established idiom; behavior-preserving (location tagging only).

## Gates (isolated target `/tmp/parsetype-merge-target`, CARGO_INCREMENTAL=0)

- `cargo check -p backend-parser-parse-type`: OK.
- `cargo check --workspace`: OK (warnings only).
- `cargo test -p no-todo-guard`: ok (1 passed).
- `cargo test -p seams-init`: ok (2 passed -- both recurrence guards).
