# jsonbio_diff — DONE (p1-lanev, adt/jsonb io+cast family)

Scaffold checklist worked top to bottom; state as of the lane's first
commit:

1. **Verbatim C pasted** — `csrc/jsonbfam/` holds whole-file vendored TUs
   (`jsonapi.c`, `wchar.c`, `stringinfo.c`, `jsonb_util.c`, `qsort_arg.c`)
   plus function-boundary-exact extracted segments (`jsonb_c.inc`,
   `jsonfuncs_c.inc`, `numeric_c.inc`, `json_escape_c.inc`,
   `pqformat_c.inc`, `mbutils_c.inc`), each carrying an upstream line-range
   provenance header. Real upstream headers under `csrc/jsonbfam/include/`;
   plumbing-only shims under `csrc/jsonbfam/shim/` (every shim file states
   what it stubs and why). Full shim list in the `csrc/pg_jsonbio_io.c`
   header.
2. **build.rs gate opened** — the family builds as its OWN `cc::Build`
   (`pg_difffuzz_jsonbfam`), because its shim `postgres.h` would collide
   with the main oracle lib's shims. Sancov rides `PGRUST_FUZZ_CSANCOV=1`
   exactly as the main lib.
3. **Symbol isolation** — every external symbol the family defines is
   `#define`d to a `jbfam_` prefix in the shim `postgres.h`. Required, not
   cosmetic: without it `psprintf`/`hash_any`/`GetDatabaseEncoding` bound
   against sibling lanes' vendored copies (mac/geo/strfam), mixing
   allocators and aborting in the arena `pfree` guard within seconds of
   fuzzing.
4. **Driver arms** — 5 selector arms (in_full / recv / op1 / cast /
   build_noargs) covering 17 ledger oids; Rust side runs the SHIPPED
   `fc_*` wrappers on a native `LocalFcinfo`. Planes: value bytes +
   verdict + errcode class. Buffer protocol: C returns `-2` with the needed
   length and the driver retries with grown buffers (a 2KB input can render
   ~10KB+; `1e100000` found this immediately).
5. **Dictionary + seeds** — `jsonbio_diff.dict` (JSON tokens, escapes,
   exponent/overflow edges); seeds harvested mechanically from
   `src/test/regress/sql/jsonb.sql` literals, one per arm, plus every
   crash artifact banked as a permanent regression seed.
6. **Smoke** — 392,565 execs / 121s local, 0 divergences, after the two
   harness defects above were fixed. Fleet campaign (>=10M execs) is the
   lane's next step.

## Open follow-ups (not this target)
- `jsonbops_diff`: two-doc ops + mutate family (cmp/contains/exists/
  getfield/concat/set/delete/insert, hash, jsonb_object, extract_path).
- Deep-nesting (54001) parity is an explicit input carve — see the module
  header's INPUT CARVES; would need stack-depth accounting in the oracle.
