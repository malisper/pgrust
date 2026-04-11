---
name: pgrust-add-type
description: Add a new SQL type to pgrust. Use when the user asks how to introduce a new type, add a builtin type like bytea or name, wire a new type through parser/catalog/executor/protocol layers, or asks which files must change for a new type.
metadata:
  short-description: Add a new type to pgrust
---

# pgrust Add Type

Use this skill when introducing a new SQL type or auditing which places must be touched for an existing type implementation.

## First Decision

Decide which kind of type you are adding before editing code:

- Full first-class SQL type
  Examples: `bytea`, `name`, `lseg`
  Requires parser, logical typing, storage/runtime value support, casts/input, protocol output, tests.

- Narrow input-helper-only type
  Examples: `int2vector`, `oidvector` in `pg_input_*`
  Do not add a full `SqlTypeKind` unless the regression actually needs table columns, casts, or wire output.

- Special internal type
  Example: quoted `"char"`
  Usually needs a distinct `SqlTypeKind` and `Value` variant, not an alias to an existing SQL surface type.

Do not widen scope accidentally. If the regression only needs `pg_input_is_valid(..., 'oidvector')`, keep it input-helper-only.

## Default Workflow

1. Find the closest existing type and copy its shape.
2. Decide whether the type needs:
   - `SqlTypeKind`
   - `Value` variant
   - `ScalarType`
   - builtin functions/operators
   - storage support
   - protocol output
3. Implement parser/type recognition first.
4. Implement runtime value/storage plumbing next.
5. Add cast/input/output semantics after the type can flow through execution.
6. Add builtins/operators that the target regression actually uses.
7. Add focused unit tests before running the regression file.

## Common Touch Points

### 1. Shared type identity

Usually required for a first-class type:

- [src/include/nodes/parsenodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/parsenodes.rs)
  Add `SqlTypeKind`.

- [src/include/nodes/datum.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/datum.rs)
  Add `Value` variant and update `to_owned_value`, equality, hashing, and other exhaustive matches.

- [src/include/nodes/plannodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/plannodes.rs)
  Add `ScalarType` if the type is physically stored.
  Add builtin ids if the new type needs dedicated functions.

### 2. Parser and type names

- [src/backend/parser/gram.pest](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/gram.pest)
  Add keywords or exact type-name syntax.

- [src/backend/parser/gram.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/gram.rs)
  Map grammar to `SqlType`.
  Update type-name rendering helpers.

- [src/backend/parser/analyze/functions.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/functions.rs)
  If function-style casts or type-name resolution need to accept the new type.

- [src/backend/parser/mod.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/mod.rs)
  If standalone `parse_type_name(...)` support matters for `pg_input_*`.

### 3. Catalog and storage metadata

- [src/backend/catalog/catalog.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/catalog/catalog.rs)
  Add `SqlTypeKind -> ScalarType` mapping and storage layout metadata.

Questions:
- fixed-length or varlena?
- alignment?
- array OID / wire type OID needed?

### 4. Type inference and binder support

- [src/backend/parser/analyze/infer.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/infer.rs)
  Infer the type for new literals or builtin results.

- [src/backend/parser/analyze/coerce.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/coerce.rs)
  Add SQL-visible type names and any binder-time special-cast lowering.

- [src/backend/parser/analyze/expr.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/expr.rs)
  Bind new builtins or special syntax if the type needs them.

Important:
- If a cast needs source-type-aware semantics, do not rely on a target-only runtime cast.
- Lower it at bind time to a dedicated builtin cast helper instead.
  Example shape: `bpchar -> text`.

### 5. Runtime casts, input, and soft input helpers

- [src/backend/executor/expr_casts.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_casts.rs)
  This is the main place for:
  - text input parser
  - `cast_text_value`
  - `cast_value`
  - `pg_input_is_valid`
  - `pg_input_error_info`

If the type has nontrivial semantics, prefer a focused sibling helper module instead of bloating `expr_casts.rs`.
Examples:
- `expr_bool.rs`
- `expr_string.rs`

For input semantics, decide explicitly:
- does parsing operate on already-decoded SQL string contents?
- does whitespace matter?
- is output hex/escape/textual?
- what SQLSTATE and message shape should invalid input use?

### 6. Tuple encoding and storage round-trip

- [src/backend/executor/value_io.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/value_io.rs)
  Add tuple encode/decode, assignment coercion, array element encode/decode, and text formatting helpers.

- [src/backend/executor/exec_tuples.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/exec_tuples.rs)
  Add compiled tuple decoding branches.

- [src/pgrust/session.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/pgrust/session.rs)
  COPY text-path parsing often needs a new branch for stored scalar types.

- [src/backend/commands/copyfrom.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/commands/copyfrom.rs)
  Array text parsing may need the new element type.

### 7. Operators, ordering, equality

- [src/backend/executor/expr_ops.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/expr_ops.rs)
  Add equality, distinctness, comparison, and ordering semantics when the type is comparable.

If the type has special ordering semantics, implement them here rather than faking output.

### 8. Builtin functions on the type

If the target regression uses builtins:

- [src/include/nodes/plannodes.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/include/nodes/plannodes.rs)
  Add `BuiltinScalarFunction` or aggregate ids.

- [src/backend/parser/analyze/functions.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/functions.rs)
  Resolve names and validate arity.

- [src/backend/parser/analyze/infer.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/parser/analyze/infer.rs)
  Infer return type.

- [src/backend/executor/exec_expr.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/executor/exec_expr.rs)
  Add dispatch.

- A focused executor helper module
  Examples: `expr_string.rs`, `expr_math.rs`, `expr_bool.rs`

### 9. Protocol output and error shaping

- [src/backend/libpq/pqformat.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/libpq/pqformat.rs)
  Add row rendering, wire type OIDs, output formatting, and error text where needed.

- [src/backend/tcop/postgres.rs](/Users/malisper/workspace/work/postgres-rewrite/pgrust/src/backend/tcop/postgres.rs)
  Add SQLSTATE mapping and caret-position extraction for new input errors.

If output has a session-controlled format, thread it through `Session` and `FloatFormatOptions`-style render options instead of hardcoding it in one call site.

## Common Failure Pattern

If a regression still says `ERROR: typename` after parser work, the usual missing layers are:

- function/type-name lookup in `analyze/functions.rs`
- type inference in `infer.rs`
- runtime cast/input handling in `expr_casts.rs`

If it parses and binds but fails later with mismatched rows:

- storage round-trip is incomplete
- output formatting is wrong
- comparison/order semantics are wrong

If `cargo check` fails after adding a `Value` variant, search exhaustively for:

```bash
rg "Value::InternalChar|Value::Jsonb|Value::Array|match value|match key|match ty.kind" src
```

and patch all exhaustive matches, including demo binaries under `src/bin/`.

## Validation Loop

Always do these in order:

1. `cargo check`
2. targeted parser tests
3. targeted executor tests
4. targeted regression file

For regressions, use:

```bash
bash .codex/skills/pgrust-regression/scripts/run_regression.sh --pgrust-setup --test <name>
```

## Good Patterns

- Add a dedicated helper module when the type has substantial semantics.
  Example: boolean logic in `expr_bool.rs`.

- Reuse the real input parser for `pg_input_is_valid` and `pg_input_error_info`.

- Lower special casts at bind time when runtime cannot infer the source type.

- Keep scope aligned to the regression file.
  Do not add arrays, user-defined type frameworks, or generic cast catalogs unless the file actually forces it.

## Examples

- `bytea`
  Full first-class type with storage, protocol output, `pg_input_*`, and builtin `md5`.

- quoted `"char"`
  Distinct internal type, not an alias to SQL `char(n)`.

- `oidvector`
  Input-helper-only support for `pg_input_*`; no full SQL type required.
