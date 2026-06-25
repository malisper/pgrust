# Audit: backend-commands-explain-format

Unit: `backend-commands-explain-format` (`src/backend/commands/explain_format.c`,
714 lines, PostgreSQL 18.3).
Crates audited: `crates/backend-commands-explain-format`, `crates/types-explain`,
`crates/backend-utils-adt-json-seams`, `crates/backend-utils-adt-xml-seams`.
Cross-checked against
`../pgrust/c2rust-runs/backend-commands-explain-format/src/explain_format.rs`.
Auditor: independent re-derivation from the C source and `commands/explain_state.h`,
`utils/json.h`, `utils/xml.h`, `lib/stringinfo.h`.

## Function inventory (every definition in explain_format.c)

| # | C function (explain_format.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExplainPropertyList` (:38) | `lib.rs::ExplainPropertyList` | MATCH | Four-arm switch on `es->format`. TEXT: IndentText, `"%s: "`, comma-joined items, `'\n'`. XML: XMLTag(OPENING), per item `indent*2+2` spaces + `<Item>` + escape_xml + `</Item>\n`, XMLTag(CLOSING). JSON: JSONLineEnding, `indent*2` spaces, escape_json(qlabel), `": ["`, comma-joined escape_json items, `']'`. YAML: YAMLLineStarting, `"%s: "`, per item `'\n'` + `indent*2+2` spaces + `"- "` + escape_yaml. `first` flag semantics identical (TEXT/JSON comma between items; XML/YAML emit unconditionally). |
| 2 | `ExplainPropertyListNested` (:108) | `lib.rs::ExplainPropertyListNested` | MATCH | TEXT/XML delegate to ExplainPropertyList (early return). JSON: line-ending, `indent*2` spaces, `'['`, comma-joined escape_json, `']'` (no label). YAML: YAMLLineStarting, `"- ["`, comma-joined escape_yaml, `']'`. |
| 3 | `ExplainProperty` (:161, static) | `lib.rs::ExplainProperty` (private) | MATCH | `unit` is `Option<&str>` (C `const char *` nullable). TEXT: IndentText then `"%s: %s %s\n"` (with unit) / `"%s: %s\n"` (without) — decomposed into the exact same byte sequence. XML: `indent*2` spaces, XMLTag(OPENING\|NOWHITESPACE), escape_xml(value), XMLTag(CLOSING\|NOWHITESPACE), `'\n'`. JSON: line-ending, `indent*2` spaces, escape_json(qlabel), `": "`, then `numeric ? raw value : escape_json(value)`. YAML: YAMLLineStarting, `"%s: "`, then `numeric ? raw : escape_yaml`. |
| 4 | `ExplainPropertyText` (:214) | `lib.rs::ExplainPropertyText` | MATCH | `ExplainProperty(qlabel, NULL, value, false, es)`. |
| 5 | `ExplainPropertyInteger` (:223) | `lib.rs::ExplainPropertyInteger` | MATCH | `char buf[32]; snprintf(INT64_FORMAT)` → fixed `[u8;32]` scratch (alloc-safety `char buf[N]` exemption), decimal `i64` Display, `ExplainProperty(..., numeric=true)`. |
| 6 | `ExplainPropertyUInteger` (:236) | `lib.rs::ExplainPropertyUInteger` | MATCH | `UINT64_FORMAT` → decimal `u64` Display in `[u8;32]`, numeric=true. |
| 7 | `ExplainPropertyFloat` (:250) | `lib.rs::ExplainPropertyFloat` | MATCH | `psprintf("%.*f", ndigits, value)` → `write!("{:.*}", prec, value)` with `prec = max(ndigits,0)` into a fixed scratch buffer, numeric=true. Negative `ndigits` clamps to 0 (C `%.*f` with negative precision treats it as omitted; PG callers never pass negative). |
| 8 | `ExplainPropertyBool` (:264) | `lib.rs::ExplainPropertyBool` | MATCH | `ExplainProperty(qlabel, NULL, value?"true":"false", true, es)` — numeric=true (unquoted in JSON), exact. |
| 9 | `ExplainOpenGroup` (:279) | `lib.rs::ExplainOpenGroup` | MATCH | TEXT no-op. XML: XMLTag(OPENING), `indent++`. JSON: line-ending, `2*indent` spaces, optional escape_json(labelname)+`": "`, `labeled?'{':'['`, `lcons_int(0)`, `indent++`. YAML: YAMLLineStarting, label→`"%s: "`+`lcons_int(1)` / no label→`"- "`+`lcons_int(0)`, `indent++`. |
| 10 | `ExplainCloseGroup` (:342) | `lib.rs::ExplainCloseGroup` | MATCH | TEXT no-op. XML: `indent--`, XMLTag(CLOSING). JSON: `indent--`, `'\n'`, `2*indent` spaces, `labeled?'}':']'`, `list_delete_first`. YAML: `indent--`, `list_delete_first`. `labelname` unused in C body (only objtype/labeled) → `_labelname`. |
| 11 | `ExplainOpenSetAsideGroup` (:389) | `lib.rs::ExplainOpenSetAsideGroup` | MATCH | Emits nothing. TEXT no-op. XML: `indent += depth`. JSON: `lcons_int(0)`, `indent += depth`. YAML: `labelname ? lcons_int(1) : lcons_int(0)`, `indent += depth`. `objtype`/`labeled` unused in C body → `_objtype`/`_labeled`. |
| 12 | `ExplainSaveGroup` (:428) | `lib.rs::ExplainSaveGroup` | MATCH | C writes `*state_save`; port returns the i32 (idiomatic out-param). TEXT no-op→0. XML: `indent -= depth`→0. JSON/YAML: `indent -= depth`, `state_save = linitial_int`, `list_delete_first`. For TEXT/XML the C leaves `*state_save` untouched; callers pair Save/Restore per format, and Restore ignores the value for TEXT/XML, so returning 0 is observably identical. |
| 13 | `ExplainRestoreGroup` (:458) | `lib.rs::ExplainRestoreGroup` | MATCH | TEXT no-op. XML: `indent += depth`. JSON/YAML: `lcons_int(state_save)`, `indent += depth`. |
| 14 | `ExplainDummyGroup` (:489) | `lib.rs::ExplainDummyGroup` | MATCH | TEXT no-op. XML: XMLTag(CLOSE_IMMEDIATE). JSON: line-ending, `2*indent` spaces, optional escape_json(labelname)+`": "`, escape_json(objtype). YAML: YAMLLineStarting, label→escape_yaml(labelname)+`": "` / no label→`"- "`, escape_yaml(objtype). |
| 15 | `ExplainBeginOutput` (:535) | `lib.rs::ExplainBeginOutput` | MATCH | TEXT no-op. XML: `<explain xmlns="http://www.postgresql.org/2009/explain">\n` (namespace string byte-verified vs C :545 and c2rust :1303), `indent++`. JSON: `'['`, `lcons_int(0)`, `indent++`. YAML: `lcons_int(0)`. |
| 16 | `ExplainEndOutput` (:566) | `lib.rs::ExplainEndOutput` | MATCH | TEXT no-op. XML: `indent--`, `</explain>`. JSON: `indent--`, `"\n]"`, `list_delete_first`. YAML: `list_delete_first`. |
| 17 | `ExplainSeparatePlans` (:595) | `lib.rs::ExplainSeparatePlans` | MATCH | TEXT: `'\n'`. XML/JSON/YAML: no-op. |
| 18 | `ExplainXMLTag` (:624, static) | `lib.rs::ExplainXMLTag` (private) | MATCH | `valid` set byte-verified vs C :627 / c2rust :1364. NOWHITESPACE→skip `2*indent` spaces. `'<'`; CLOSING→`'/'`; per tagname byte: `strchr(valid, b)` → pass else `'-'` (port: `VALID.contains(&b)`; loop never reaches NUL so the `strchr`-matches-NUL edge cannot occur). CLOSE_IMMEDIATE→`" /"`; `'>'`; NOWHITESPACE→skip trailing `'\n'`. |
| 19 | `ExplainIndentText` (:651) | `lib.rs::ExplainIndentText` | MATCH | `Assert(format==TEXT)`→`debug_assert_eq`. `len==0 || data[len-1]=='\n'` → `indent*2` spaces. Port: `is_empty() || as_bytes().last()==Some(b'\n')`. |
| 20 | `ExplainJSONLineEnding` (:666, static) | `lib.rs::ExplainJSONLineEnding` (private) | MATCH | `Assert(format==JSON)`. `linitial_int != 0` → `','` else set head to `1` (C lvalue `linitial_int(...) = 1` → `grouping_stack[0] = 1`); then `'\n'`. |
| 21 | `ExplainYAMLLineStarting` (:686, static) | `lib.rs::ExplainYAMLLineStarting` (private) | MATCH | `Assert(format==YAML)`. head==0 → set head to 1 (no output); else `'\n'` + `indent*2` spaces. |
| 22 | `escape_yaml` (:711, static) | `lib.rs::escape_yaml` (private) | MATCH | C body is literally `escape_json(buf, str)`; port delegates to the `escape_json` seam — no separate slot, as designed. |

All 22 definitions present. No `MISSING`/`PARTIAL`/`DIVERGES`.

## Helper / vocabulary parity

- `es->str` (C `StringInfo`) → `PgString<'mcx>` in `types-explain`; `appendStringInfoString`
  → `try_push_str`, `appendStringInfoChar`/`appendStringInfoCharMacro` → `try_push`,
  `appendStringInfoSpaces(n)` → n× `try_push(' ')`. All fallible (mcx OOM) → every public
  fn returns `PgResult<()>`, matching the C palloc failure surface (`enlargeStringInfo`
  ereport). `appendStringInfo("%s: ", ...)` format strings decomposed into the exact byte
  sequence.
- `es->grouping_stack` (C integer `List`) → `PgVec<'mcx, i32>`: `lcons_int(v)` = front-insert
  (fallible try_reserve + insert(0)), `list_delete_first` = remove(0) (guarded on empty, an
  Assert-level case in C), `linitial_int` = index 0. MATCH.
- `INT64_FORMAT`/`UINT64_FORMAT`/`%.*f` rendered into fixed `[u8;N]` scratch — the documented
  `char buf[32]` non-allocating exemption; values are non-data-derived. `from_utf8(...).unwrap_or("")`
  is infallible here (only ASCII digits/`.`/`-` written) and is a non-panicking fallback, not
  an error-path stand-in.

## Design conformance (merge-blocking checks)

- **Neighbor types**: `ExplainState`/`ExplainFormat`/`ExplainSerializeOption` defined as real
  trimmed types in the new `types-explain` crate (owner `explain_state.c`), values verified vs
  `explain_state.h`; node-tree fields (pstmt/rtable/deparse_cxt/printed_subplans/workers_state/
  extension_state) intentionally omitted (extend-not-restructure; their owners add them). No
  Oid/usize/&[u8] stand-ins. types.md rules 6-7 satisfied.
- **Allocating seams take Mcx + PgResult**: `escape_xml` (C `palloc`s its result) →
  `escape_xml(mcx, str) -> PgResult<PgString<'mcx>>`. `escape_json` (C appends into the caller's
  StringInfo) → `escape_json(&mut PgString, str) -> PgResult<()>` — mirrors the C signature exactly
  (append-in-place), no intermediate owned String. Both owners (`utils/adt/json.c`,
  `utils/adt/xml.c`) are unported → calls panic loudly until they land (mirror-PG-and-panic).
- **No shared statics**: none (no globals in this unit).
- **No locks/registries**: none.
- **No unledgered divergence markers**: grep clean (`for now`/`simplified`/`hack`/`TODO`/`FIXME`).
- **Panics in owned logic**: none. `debug_assert_eq` mirror the C `Assert`s (format-tag invariants),
  not error paths.

## Seam audit

Owned seam crates by C-source coverage: `explain_format.c` has **no** consumer that calls it
across a dependency cycle yet (its callers — `explain.c` and the extension explainers — are
unported and will depend on it directly), so there is no `backend-commands-explain-format-seams`
crate and `init_seams()` is correctly empty (no owned seam decls outstanding → not a FAIL).

Outward seam calls (both into unported owners, both genuine — a direct dep on json/xml would
require those crates to exist):

| Seam | Owner crate | Shape | Verdict |
|---|---|---|---|
| `escape_json::call(buf, str)` | `backend-utils-adt-json-seams` | append-into-buf, PgResult | thin delegate (used by JSON paths + escape_yaml) |
| `escape_xml::call(mcx, str)` | `backend-utils-adt-xml-seams` | Mcx in, PgString out | thin delegate (XML paths) |

No branching/computation in any seam path (the formatter does all control flow; the seam is one
call + push the result). `init_seams()` is wired into `seams-init::init_all()` (one line) and the
crate is in `seams-init`'s deps.

Tests: 33 in-crate tests (one per format × emitter family, grouping_stack semantics, XML
tag-sanitizing, JSON comma bookkeeping, YAML line-starting, Save/Restore round-trip, escaping
routed through test-installed seams) — all pass.

## Verdict: PASS

Every function MATCH; zero seam findings; design-conformance clean. Mergeable.
