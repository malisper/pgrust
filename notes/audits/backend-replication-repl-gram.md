# Audit: backend-replication-repl-gram

Unit: `backend-replication-repl-gram`
C source: `src/backend/replication/repl_gram.y` (Bison) → generated
`build-clean/src/backend/replication/repl_gram.c`
c2rust: `../pgrust/c2rust-runs/backend-replication-repl-gram/src/repl_gram.rs`
Port: `crates/backend-replication-repl-gram/src/lib.rs` (+ `tests.rs`)
Branch: `port/backend-replication-repl-gram`
Verdict: **PASS**

## Method note

`repl_gram.y` is a Bison LALR(1) grammar. The c2rust artifact is the generated
state machine (`replication_yyparse` + the `yytranslate`/`yyr1`/`yyr2`/`yydefact`
tables + the inline reduce-action `switch`), not hand-written C. The audit
therefore compares the port against the **grammar productions and their semantic
actions** (the human-readable source of truth), cross-checking each action
against the generated reduce-case in the c2rust switch. The port is a
recursive-descent recognizer that transcribes every production 1:1; this is the
standard faithful representation of a Bison grammar (no Bison runtime exists in
the repo) and is acceptable provided the accepted language and the constructed
nodes are provably identical. They are — verified below per production.

## Generated-C function inventory

| C function (repl_gram.c) | Role | Port location | Verdict |
|---|---|---|---|
| `newNode` (static) | c2rust rendering of `makeNode` palloc0+tag | node-construction helpers `make_*_node` / enum ctors | MATCH (owned-value equivalent) |
| `yydestruct` (static) | Bison error-recovery value cleanup (palloc, no observable effect) | n/a — owned values drop automatically | MATCH (no behavior) |
| `replication_yyparse` | LALR driver + reduce-action switch (all 21 productions' semantic actions) | `Parser::parse_*` methods + `parse_first_cmd` | MATCH (see production table) |
| `replication_yyerror` (extern, repl_scanner.l) | `ereport(ERROR, ERRCODE_SYNTAX_ERROR, errmsg_internal("%s",msg))` | `replication_yyerror()` → recoverable `PgError` | MATCH |
| `replication_yylex` (extern, scanner unit) | token source | `replication_lex_all` seam (scanner unit) | SEAMED (cross-unit, real dep) |

## Production-by-production (grammar source vs port)

| Production (`repl_gram.y`) | Reduce-case # | Port method | Verdict |
|---|---|---|---|
| `firstcmd: command opt_semicolon` + `$end` | 2 | `parse_first_cmd` | MATCH (writes result; rejects trailing tokens) |
| `opt_semicolon: ';' \| ε` | 3,4 | `parse_opt_semicolon` | MATCH |
| `command:` (11 alts) | dispatch | `parse_command` | MATCH (dispatch on first keyword) |
| `identify_system: K_IDENTIFY_SYSTEM` → `IdentifySystemCmd` | 16 | `parse_identify_system` | MATCH |
| `read_replication_slot: K_READ_REPLICATION_SLOT var_name` | 17 | `parse_read_replication_slot` | MATCH |
| `show: K_SHOW var_name` → `VariableShowStmt` | 18 | `parse_show` | MATCH |
| `var_name: IDENT \| var_name '.' IDENT {psprintf("%s.%s")}` | 19,20 | `parse_var_name` | MATCH (left-recursion → fold loop; `format!` == psprintf) |
| `base_backup: K_BASE_BACKUP '(' generic_option_list ')' \| K_BASE_BACKUP` | 21,22 | `parse_base_backup` | MATCH |
| `create_replication_slot:` PHYSICAL / LOGICAL forms | 23,24 | `parse_create_replication_slot` | MATCH (kind, slotname, temporary, plugin, options) |
| `create_slot_options: '(' generic_option_list ')' \| create_slot_legacy_opt_list` | 25,26 | `parse_create_slot_options` | MATCH |
| `create_slot_legacy_opt_list:` list \| ε | 27,28 | `parse_create_slot_legacy_opt_list` | MATCH (NIL on empty) |
| `create_slot_legacy_opt:` 5 keyword→DefElem alts | 29–33 | inline in legacy_opt_list loop | MATCH (snapshot=export/nothing/use, reserve_wal=true, two_phase=true) |
| `drop_replication_slot: ... \| ... K_WAIT` | 34,35 | `parse_drop_replication_slot` | MATCH (wait=false/true) |
| `alter_replication_slot: K_ALTER... IDENT '(' generic_option_list ')'` | 36 | `parse_alter_replication_slot` | MATCH |
| `start_replication: K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline` | 37 | `parse_start_replication` (physical branch) | MATCH |
| `start_logical_replication: K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options` | 38 | `parse_start_replication` (logical branch) | MATCH (requires SLOT clause; bare LOGICAL = syntax error) |
| `timeline_history: K_TIMELINE_HISTORY UCONST` + `$2<=0` guard | 39 | `parse_timeline_history` | MATCH (`==0` guard; "invalid timeline %u") |
| `upload_manifest: K_UPLOAD_MANIFEST` | 40 | `parse_upload_manifest` | MATCH |
| `opt_physical: K_PHYSICAL \| ε` | 41,42 | inline in `parse_start_replication` | MATCH |
| `opt_temporary: K_TEMPORARY {true} \| ε {false}` | 43,44 | `parse_opt_temporary` | MATCH |
| `opt_slot: K_SLOT IDENT \| ε {NULL}` | 45,46 | inline in `parse_start_replication` | MATCH |
| `opt_timeline: K_TIMELINE UCONST` + `$2<=0` guard `\| ε {0}` | 47,48 | `parse_opt_timeline` | MATCH (`==0` guard; "invalid timeline %u"; ε→0) |
| `plugin_options: '(' plugin_opt_list ')' \| ε {NIL}` | 49,50 | `parse_plugin_options` | MATCH |
| `plugin_opt_list:` first \| list ',' elem | 51,52 | `parse_plugin_opt_list` | MATCH |
| `plugin_opt_elem: IDENT plugin_opt_arg` | 53 | `parse_plugin_opt_elem` | MATCH |
| `plugin_opt_arg: SCONST {makeString} \| ε {NULL}` | 54,55 | `parse_plugin_opt_arg` | MATCH |
| `generic_option_list:` list ',' opt \| opt | 56,57 | `parse_generic_option_list` | MATCH |
| `generic_option:` 4 alts (bare / IDENT / SCONST / UCONST) | 58–61 | `parse_generic_option` | MATCH (UCONST→`makeInteger($2)`, `$2` uint32→int narrowing reproduced as `u as i32`) |
| `ident_or_keyword:` IDENT \| 20 keyword→lowercase-spelling alts | 62–82 | `parse_ident_or_keyword` | MATCH (all 20 keyword spellings verified char-for-char vs grammar) |

### Spot-checked in detail (re-derived)

- **`var_name` dotted fold**: C reduce-case 20 = `psprintf("%s.%s", $1, $3)` over
  left-recursion. Port folds each `.IDENT` suffix with `format!("{name}.{next}")`.
  Identical output for `a.b.c` (`((a).b).c` == `a.b.c`). Verified by test
  `show_dotted_var_name`.
- **timeline guard**: C `if ($2 <= 0)` with `$2` of type `uint32` — unsigned, so
  the only satisfying value is `0`. Port uses `== 0` in both `timeline_history`
  and `opt_timeline`. SQLSTATE `ERRCODE_SYNTAX_ERROR`, message `"invalid timeline
  %u"` reproduced as `format!("invalid timeline {val}")`. Verified by
  `timeline_history_zero_is_error`.
- **`makeInteger($2)` from UCONST**: C `$2` is `uint32` stored into `Integer.ival`
  (a C `int`) — bit-preserving narrowing. Port: `u as i32`. MATCH. Verified by
  `generic_option_keyword_name_and_integer_value`.
- **`makeDefElem(name, arg, -1)`**: matches `makefuncs.c` `makeDefElem` exactly:
  `defnamespace=NULL`, `defname=name`, `arg`, `defaction=DEFELEM_UNSPEC`,
  `location=-1`.
- **start_replication ambiguity**: physical (`opt_slot opt_physical RECPTR
  opt_timeline`) vs logical (mandatory `K_SLOT IDENT K_LOGICAL RECPTR
  plugin_options`). Port reads `opt_slot`, branches on `K_LOGICAL`; a bare
  `START_REPLICATION LOGICAL` (no SLOT) matches no production → syntax error,
  exactly as the LALR table (logical requires the SLOT clause). Verified by
  `start_logical_replication` and `start_logical_without_slot_is_error`.
- **punctuation tokens**: scanner catch-all `.` rule returns `yytext[0]` (raw
  char); grammar references `'('`,`')'`,`','`,`';'`,`'.'`. Port models all five
  via `Token::Char(u8)`. MATCH.

## Node-struct conformance (`nodes/replnodes.h` → `types-replication::replnodes`)

All eight command structs + `ReplicationKind` verified field-for-field against
`replnodes.h`:
- `ReplicationKind { PHYSICAL=0, LOGICAL=1 }` — MATCH.
- `BaseBackupCmd{options}`, `CreateReplicationSlotCmd{slotname,kind,plugin,
  temporary,options}`, `DropReplicationSlotCmd{slotname,wait}`,
  `AlterReplicationSlotCmd{slotname,options}`, `StartReplicationCmd{kind,slotname,
  timeline,startpoint,options}`, `ReadReplicationSlotCmd{slotname}`,
  `TimeLineHistoryCmd{timeline}`, `IdentifySystemCmd`/`UploadManifestCmd` (empty)
  — all MATCH (`char*`→`Option<String>`, `List*`→`Vec<DefElem>`, `NodeTag` tag →
  enum discriminant via `ReplCommand`).
- `VariableShowStmt{name}` (from `parsenodes.h`, built by `show`) — MATCH.

## Seam / wiring audit

- **Owned inward seams: NONE.** No `crates/backend-replication-repl-gram-seams`
  exists. By the C-source-coverage ownership rule, repl_gram.c maps to no seam
  crate; this unit declares/owns no inward seam. Consequently it has no
  `init_seams()` and correctly is not wired into `seams-init::init_all()` — the
  same shape as dest/functioncmds (consumer-only crate). The `recurrence_guard`
  tests confirm this (both pass).
- **Outward seams consumed**: `replication_lex_all` and
  `replication_scanner_is_replication_command` from
  `backend-replication-repl-scanner-seams`. Owner = the **separate** unported
  `backend-replication-repl-scanner` unit (`repl_scanner.l`). Both calls are thin
  marshal-and-delegate (one `::call`, propagate `PgResult`). The grammar genuinely
  cannot lex without the scanner, and bundling the full token stream is
  behavior-preserving (LALR(1) consumes left-to-right, one-token lookahead). The
  seams panic until the scanner lands — correct mirror-and-panic for an unported
  callee. These are scanner-owned, not this unit's wiring responsibility.

## No-stub / no-deferral check

- No `todo!()`/`unimplemented!()`. No own-logic replaced by a "somewhere else"
  seam. The only seam calls are the genuine cross-unit scanner dependency. Every
  grammar production and semantic action is implemented in-crate. `unreachable!()`
  appears only in match arms already proven exhaustive by a preceding `peek()`
  guard (e.g. `Token::Sconst(_)` peeked then `bump`ed) — not a stub.

## Gates

- `cargo test -p backend-replication-repl-gram`: 18 passed.
- `cargo check --workspace`: clean (only pre-existing warnings in unrelated
  crates).
- `cargo test -p seams-init`: 2 passed (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`).

## Verdict

**PASS.** Every generated-C function and every grammar production has a faithful
counterpart; all node tags, constants, error SQLSTATEs/messages, and integer
narrowings match; no own-logic stubs or deferrals; the only seams are the real
cross-unit scanner dependency (owned + installed by the scanner unit). CATALOG
row set to `audited`.
