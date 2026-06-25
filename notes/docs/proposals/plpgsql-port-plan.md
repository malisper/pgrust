# PL/pgSQL Port Plan

Port plan for PostgreSQL 18.3's `src/pl/plpgsql/` subsystem into this repo,
following the crate-correspondence discipline: every owner crate maps 1:1 to a
real c2rust translation unit (here, one `.c` per crate); `types-*` and `*-seams`
crates are the shared substrate and are exempt from the 1:1-to-a-C-file rule.

This is a **large multi-crate campaign**, not a single lane. Each owner crate is
ported to 100% of its C unit (every function, every branch) per
`port-full-functions-no-bounded-partial`. The plan below sequences the crates
dependency-correctly and calls out the keystones that gate each phase.

---

## 1. Crate layout

### Substrate (types + seams)

| Crate | Kind | Contents |
|---|---|---|
| `types-plpgsql` | types | Every `PLpgSQL_*` struct/enum from `plpgsql.h`: the datum hierarchy (`PLpgSQL_datum`/`_variable`/`_var`/`_row`/`_rec`/`_recfield`), the 27-node statement parse-tree hierarchy (`PLpgSQL_stmt` + all `_stmt_*` subtypes), `PLpgSQL_expr` (with the runtime simple-expr fast-path cache fields), `PLpgSQL_type`, `PLpgSQL_function` (embedding `CachedFunction cfunc`), `PLpgSQL_execstate`, `PLpgSQL_nsitem`, `PLpgSQL_condition`/`_exception`/`_exception_block`, `_case_when`/`_if_elsif`/`_raise_option`/`_diag_item`, `PLpgSQL_plugin` vtable, and the `DTYPE_*`/`PLPGSQL_STMT_*`/`GETDIAG_*`/`RESOLVE_*`/`RAISEOPTION_*`/`PLPGSQL_LABEL_*`/`IdentifierLookup` enums. |
| `backend-pl-plpgsql-gram-seams` | seams | Inward seam for `plpgsql_yyparse` if a cycle forces it (gram → scanner → comp form a tight cluster; see §2). Likely only needed for `comp → gram` (compiler invokes the parser). |
| `backend-pl-plpgsql-scanner-seams` | seams | Inward seams for scanner entry points consumed across a cycle by gram/comp (`plpgsql_yylex`, `plpgsql_push_back_token`, `plpgsql_peek`/`_peek2`, `plpgsql_token_length`, `plpgsql_append_source_text`, `plpgsql_location_to_lineno`, `plpgsql_scanner_errposition`). |
| `backend-pl-plpgsql-comp-seams` | seams | Inward seams for the compiler callbacks the scanner/grammar fire (`plpgsql_parse_word`/`_dblword`/`_tripword`, `plpgsql_parse_wordtype`/`_cwordtype`/`_wordrowtype`/`_cwordrowtype`, `plpgsql_build_variable`/`_record`/`_recfield`/`_datatype`, `plpgsql_adddatum`, `plpgsql_add_initdatums`, `plpgsql_parse_err_condition`, `plpgsql_getdiag_kindname`) plus the global compile state accessors (`plpgsql_curr_compile`, `plpgsql_Datums`/`nDatums`) modeled as a thread-local `CompileState`. |
| `backend-pl-plpgsql-funcs-seams` | seams | Inward seams for the namespace stack + walker + teardown that gram/comp/exec/handler all call (`plpgsql_ns_*`, `plpgsql_stmt_typename`, `plpgsql_free_function_memory`, `plpgsql_delete_callback`, `plpgsql_mark_local_assignment_targets`). |
| `backend-pl-plpgsql-exec-seams` | seams | Inward seam for `plpgsql_exec_get_datum_type_info` (called back from comp), plus the three exec entry points if the handler↔exec edge cycles. |

### Owner crates (1:1 with a C unit)

| Crate | C unit | Notes |
|---|---|---|
| `backend-pl-plpgsql-scanner` | `pl_scanner.c` | Two-level lexer wrapping the core SQL lexer; `IdentifierLookup` global → thread-local. |
| `backend-pl-plpgsql-gram` | `pl_gram.y` (→ `pl_gram.c`/`.h`) | The bison grammar. **Use the repo's `fgram` approach** (see §4). |
| `backend-pl-plpgsql-comp` | `pl_comp.c` | Compiler / front-end; builds `PLpgSQL_function`. |
| `backend-pl-plpgsql-exec` | `pl_exec.c` | 9108-LOC tree-walking interpreter. Largest unit; ports as one campaign over multiple lanes but the deliverable is the whole unit. |
| `backend-pl-plpgsql-funcs` | `pl_funcs.c` | Namespace, tree-walker, teardown, dumptree. Low complexity; port early. |
| `backend-pl-plpgsql-handler` | `pl_handler.c` | fmgr-facing call/inline/validator handlers + `_PG_init`. |

### Required-but-prerequisite owner crates (not part of plpgsql, must land first)

| Crate | C unit | Why blocking |
|---|---|---|
| `backend-utils-cache-funccache` | `funccache.c` | `CATALOG.tsv` status **todo**. `PLpgSQL_function` embeds `CachedFunction cfunc`; `plpgsql_compile` enters via `cached_function_compile`; `plpgsql_delete_callback` is the funccache deletion callback. **Hard prerequisite — port this first.** |
| (existing) `backend-executor-spi` | `spi.c` | Already present (`backend-executor-spi` + `-seams`). `pl_exec.c` makes ~132 SPI calls; verify the SPI surface is installed, not just declared. |
| (existing) `backend-parser-gram-core` / `-scan-fgram` | `gram.y`/`scan.l` | The plpgsql scanner wraps the **core** SQL lexer (`core_yylex`, `scanner_init/finish`, `core_yy_extra_type`) and the grammar `check_sql_expr` calls `raw_parser`. Both already exist via the fgram stack. |

---

## 2. Port order (dependency-correct)

The plpgsql internal dependency cluster is tight and partly cyclic: the grammar
calls the scanner and the compiler; the scanner calls the compiler's
`plpgsql_parse_word*` resolvers; the compiler runs the grammar; `pl_funcs.c` is
called by all three; `pl_exec.c` is called back by the compiler
(`plpgsql_exec_get_datum_type_info`); the handler drives compile + exec. Seam
crates break the cycles. Order:

0. **`types-plpgsql`** — foundation. Pure declarations; get the
   struct-inheritance-by-field-prefix idiom right (enum + tag, preserving the
   `cmd_type`/`dtype` discriminator). No logic. Everything else depends on it.

0b. **`backend-utils-cache-funccache`** — prerequisite. Port the whole
   `funccache.c` unit (cache hash, `cached_function_compile`, polymorphic
   argtype resolution, deletion-callback registration). Verify SPI is installed.

1. **`backend-pl-plpgsql-funcs`** (`pl_funcs.c`) — lowest plpgsql owner.
   Low complexity (pure tree-walking / switch dispatch / linked-list + bitmapset);
   no fmgr/executor reentry, one SPI call (`SPI_freeplan`), one
   `MemoryContextDelete`. Provides `plpgsql_ns_*`, the statement walker,
   `plpgsql_free_function_memory`, `plpgsql_stmt_typename`. Declare its inward
   seams (`backend-pl-plpgsql-funcs-seams`) and install at port time.

2. **`backend-pl-plpgsql-scanner`** (`pl_scanner.c`) — depends on funcs (none
   directly) and on the **core** SQL lexer (already present). Its calls into the
   compiler's `plpgsql_parse_word*` go through `backend-pl-plpgsql-comp-seams`
   (panic until comp lands). Declare `backend-pl-plpgsql-scanner-seams`.

3. **`backend-pl-plpgsql-gram`** (`pl_gram.y`) — the grammar. Calls scanner
   (direct dep on scanner crate) and compiler builders (via comp-seams).
   Use the **fgram** approach (§4). Declare `backend-pl-plpgsql-gram-seams`
   (the single `plpgsql_yyparse` entry comp calls).

4. **`backend-pl-plpgsql-comp`** (`pl_comp.c`) — the compiler. Runs the grammar
   (direct dep on gram, or via gram-seams), reads catalogs/syscache, builds
   `PLpgSQL_function`. **Installs** the comp-seams the scanner/grammar call.
   Calls back into exec's `plpgsql_exec_get_datum_type_info` via exec-seams
   (panics until exec lands — acceptable). Depends on funccache.

5. **`backend-pl-plpgsql-exec`** (`pl_exec.c`) — the interpreter. The largest and
   hardest unit; depends on comp (compiles the tree it walks), SPI, the executor
   expr-eval internals (`ExprEvalStep` param callbacks), plancache refcounting,
   expanded-record datums. Installs `plpgsql_exec_get_datum_type_info`. Multi-lane
   campaign; deliverable is the whole unit.

6. **`backend-pl-plpgsql-handler`** (`pl_handler.c`) — top of the stack. Depends
   on comp (`plpgsql_compile`/`_compile_inline`), exec (the three entry points),
   funcs (`plpgsql_xact_cb`/`_subxact_cb`/`plpgsql_free_function_memory`), GUC,
   SPI, resowner, fmgr V1 ABI. **Installs** the three call-handler builtins.

Seam-crate decls are created when first needed (step where the *consumer* lands)
and **installed in the same change as the owner** (`init_seams()` wired into
`seams-init::init_all()` per repo rule).

---

## 3. Integration points (fmgr / functioncmds / language registration)

PL/pgSQL is an fmgr-dispatched extension: its three handlers are
`PG_FUNCTION_INFO_V1` builtins (`plpgsql_call_handler`, `plpgsql_inline_handler`,
`plpgsql_validator`) reached via the `pg_language` catalog row
(`lanplcallfoid`/`laninline`/`lanvalidator`) and `pg_proc` `prolang`.

**Inline (`DO`) path — already seamed.** `backend-commands-functioncmds`'s
`ExecuteDoStmt` (`cast_transform_do.rs`) already reads `language_struct.laninline`
and calls `seam::execute_inline_handler::call(laninline, codeblock)`. The seam
`execute_inline_handler(laninline: Oid, codeblock: InlineCodeBlock)` is declared
in `backend-commands-functioncmds-seams` (lib.rs:405) and is currently
**uninstalled**. `backend-pl-plpgsql-handler::init_seams()` installs it to a
thin adapter that calls `plpgsql_inline_handler` (or directly the inline body).
This is the cleanest first integration point and a good smoke target.

**Call path (functions/triggers).** fmgr dispatch on `prolang` → the language's
`lanplcallfoid` → `plpgsql_call_handler`. This requires the fmgr language-handler
dispatch surface: when fmgr looks up a function whose `prolang` is a PL, it must
resolve `lanplcallfoid` from `pg_language` and call that handler with the original
`FunctionCallInfo`. Wire the handler builtin into the builtin-function/`fmgrtab`
registry (the same mechanism that registers other C-language builtins) so that
the OID stored in the `pg_language` row resolves to the Rust `plpgsql_call_handler`.

**Validator path.** `CREATE FUNCTION ... LANGUAGE plpgsql` triggers fmgr dispatch
on `lanvalidator` → `plpgsql_validator` (test-compile via `plpgsql_compile`).
`backend-commands-functioncmds::CreateFunction` already performs the validator
call after creating the `pg_proc` row; the validator OID resolves through the same
builtin registry.

**Language registration.** `plpgsql` is an `initdb`-created bootstrap-ish language:
the `pg_language` row (`lanname=plpgsql`, `lanpltrusted=t`, handler/inline/validator
OIDs pointing at the three builtins) is normally created by the
`CREATE LANGUAGE plpgsql` / extension SQL. For this repo's single-backend boot,
the row + the three builtin OIDs must exist in the catalog (genbki/initdb data or
a `CREATE EXTENSION plpgsql`-equivalent). Record the three handler OIDs and ensure
they appear in the builtin fmgr table so fmgr can dispatch to the Rust functions.

**`_PG_init`.** Registered once at module load: defines the custom GUCs
(`plpgsql.variable_conflict`, `print_strict_params`, `check_asserts`,
`extra_warnings`/`extra_errors`), registers the persistent xact/subxact callbacks
(`plpgsql_xact_cb`/`_subxact_cb` from funcs), and sets the plugin rendezvous
pointer. In-repo this fires from the handler crate's init path (gated by a
run-once guard, mirroring the C static).

---

## 4. The hard parts

### 4a. ereport/longjmp → panic / `PgError`

Per AGENTS.md and `types-error`, the C `ereport(ERROR)`/`elog`/`longjmp` escape is
modeled two ways depending on layer:

- **In the fgram grammar/scanner mechanism** (copied c2rust frames): the repo's
  existing fgram stack already maps `ereport(ERROR)` to a Rust **panic caught by
  `catch_unwind`** at the `base_yyparse` boundary (the copied parser frames are
  plain Rust, so the unwind is sound — see `backend-parser-gram-core`'s
  `support.rs`). The plpgsql grammar reuses that exact escape.
- **In hand-ported owner logic** (comp/exec/funcs/handler): FATAL/ERROR sites
  become `Err(PgError)` returned up via `PgResult`, not panics, per
  `port-full-functions-no-bounded-partial` ("FATAL/PANIC C sites are Err(PgError)").
- **The exec exception handler is the crux.** `exec_stmt_block`'s
  `PG_TRY`/`PG_CATCH` (pl_exec.c ~1793/1841) is *the* catchable error channel for
  `EXCEPTION WHEN ... THEN`: it wraps the body in an internal subtransaction
  (`BeginInternalSubTransaction`), traps via `CopyErrorData`/`FlushErrorState`,
  and matches `WHEN` conditions. This must be modeled as a **catchable error
  channel** (`catch_unwind` around the subxact body, reconstructing `ErrorData`
  from the caught `PgError`), **not** as ordinary Rust panics that escape — the
  error must be inspectable (SQLSTATE matching) and the subxact rolled back. This
  is the single most delicate control-flow port in the subsystem.

### 4b. SPI dependency

`pl_exec.c` makes ~132 SPI calls; `pl_handler.c` brackets every handler with
`SPI_connect_ext`/`SPI_finish` (nonatomic flag from `CallContext->atomic`).
`backend-executor-spi` exists in the tree — but per repo discipline, **verify the
SPI surface is installed, not merely declared**: `SPI_connect_ext`, `SPI_finish`,
`SPI_execute*`, `SPI_prepare*`, `SPI_cursor_*`, `SPI_freeplan`, `SPI_processed`/
`SPI_tuptable` accessors. Any uninstalled SPI seam is a blocker for exec. The
simple-expr fast path (`exec_eval_simple_expr`) deliberately **bypasses SPI** and
evaluates inline through the executor's `ExprState`/`ExprEvalStep` — that path
depends on the executor expr-eval internals and the `plpgsql_param_eval_*`
callbacks (the deepest executor coupling), which may be keystone-blocked
independently of SPI.

### 4c. Memory-context lifetimes

Three+ contexts juggled explicitly, correctness-load-bearing:

- **Compiler:** a per-function `AllocSetContext` (`func_cxt`) becomes the
  function's lifetime arena, reparented onto `CacheMemoryContext` *on success*
  (reparent-on-error-leak pattern); a sibling `plpgsql_compile_tmp_cxt` for
  scratch. Model with the repo's `Mcx`/owned-context substrate; the
  reparent-on-success is the subtle bit (on error the arena is dropped with the
  caller context).
- **Executor:** SPI Proc context (call-lifespan), an on-demand stmt mcontext
  stack (`get/push/pop_stmt_mcontext`), and the eval per-tuple `eval_econtext`
  reset by `exec_eval_cleanup`. Session-wide cast caches + a shared simple-eval
  `EState`/`ResourceOwner` are reset by `plpgsql_xact_cb`/`subxact_cb` at
  (sub)transaction boundaries — correctness depends on those resets firing.
- **Handler:** `procedure_resowner`/`simple_eval_resowner` are deliberately
  **parentless** (survive COMMIT/ROLLBACK inside `DO`/`CALL`); drop order in the
  inline handler's catch path must be replicated as RAII/guard ordering or
  process-lifespan resources leak.
- **Global compile state:** `plpgsql_Datums`/`nDatums`/`plpgsql_curr_compile`/
  `plpgsql_compile_tmp_cxt`/`IdentifierLookup` are per-backend mutable globals
  (the compiler is explicitly non-reentrant). Model as a **thread-local
  `CompileState`** (AGENTS.md global-state rule), not ambient statics scattered
  across crates — own it in the comp/scanner-seams substrate.

### 4d. The bison grammar (fgram approach)

`pl_gram.c`/`.h` are **bison-generated** — do not hand-port the LALR tables. The
repo's established pattern (see `backend-parser-gram-core` and the `repl_gram`
recursive-descent precedent) is the **`fgram`** approach:

- For the core SQL grammar, the repo wraps the **c2rust translation of the
  generated `gram.c`** (the LALR tables + rule actions) inside a contained,
  audited-unsafe crate (`pgrust-gram-c2rust-fgram`) and exposes a **safe owned
  boundary** crate (`-gram-core`) that converts the raw `*mut Node` graph into
  the repo's owned `types_nodes` tree at the parser return.
- For plpgsql, apply the same: a c2rust-of-`pl_gram.c` mechanism crate
  (`pgrust-pl-gram-c2rust-fgram` or similar) holding the generated tables + the
  hand-rolled lookahead actions (`read_sql_construct` family,
  `make_execsql_stmt`, `read_into_target`, `read_raise_options`, …), with
  `backend-pl-plpgsql-gram` as the safe boundary that hands out owned
  `types-plpgsql` AST. The `ereport`→panic→`catch_unwind` escape (§4a) is reused.
- **Alternatively**, because `pl_gram.y` is far smaller and simpler than the core
  SQL grammar (it RAW-parses for syntax only — no analyzer, no SPI in the grammar
  itself), the `repl_gram` precedent of a **hand-written recursive-descent over
  the scanner token stream** (every production transcribed 1:1) is viable and
  avoids carrying a second c2rust LALR table. The recursive-descent route is
  recommended if the production count is tractable; fall back to fgram if not.
- The hardest sub-part either way is the **two-level lexing coroutine**: grammar
  actions call `yylex`/`plpgsql_push_back_token`/`plpgsql_peek` directly and flip
  `plpgsql_IdentifierLookup` (NORMAL/DECLARE/EXPR) to steer how the scanner
  resolves identifiers as datums. This scanner↔grammar dance, plus the
  source-text slicing (`plpgsql_append_source_text` reconstructs SQL/expr strings
  by byte offset instead of building Node trees; PERFORM is faked by overwriting
  "PERFORM" with "SELECT "), must be ported faithfully.

---

## 5. Summary

**Crates (in port order):** `types-plpgsql` → `backend-utils-cache-funccache`
(prerequisite) → `backend-pl-plpgsql-funcs` → `backend-pl-plpgsql-scanner` →
`backend-pl-plpgsql-gram` → `backend-pl-plpgsql-comp` → `backend-pl-plpgsql-exec`
→ `backend-pl-plpgsql-handler`, plus the seam crates
`backend-pl-plpgsql-{gram,scanner,comp,funcs,exec}-seams`.

**Top 3 hard parts:** (1) the `exec_stmt_block` `PG_TRY`/`PG_CATCH` exception
channel — a catchable, SQLSTATE-inspectable error path over an internal
subtransaction, modeled with `catch_unwind` reconstructing `ErrorData`, not bare
panics; (2) the deep executor coupling — SPI surface installation plus the
simple-expr `plpgsql_param_eval_*` callbacks plugged into the SQL executor's
`ExprEvalStep` dispatch (possibly keystone-blocked); (3) the bison grammar via the
fgram approach (or `repl_gram`-style recursive-descent) with its two-level
scanner↔grammar lexing coroutine and `IdentifierLookup` mode flipping.

Each owner crate is ported to 100% of its C unit and audited (`/audit-crate`, a
hard merge blocker) before merge.
