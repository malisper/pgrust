# jsonbops_diff — BUILT + SMOKED (p1-lanev, adt/jsonb two-doc ops family)

Sibling of jsonbio_diff; shares the csrc/jsonbfam oracle family, shim
environment, jbfam_ symbol isolation, and errcode-class contract (extended:
10 = 2202E array-subscript, 11 = 22004 null-value-not-allowed).

State at first commit:
1. **Verbatim C vendored** — new family TUs `jsonbfam/jsonb_op.c` (whole
   file) + `jsonbfam/hashfn.c` (whole file, with the REAL common/hashfn.h
   replacing the io lane's abort-stub decls); driver TU `pg_jsonbops.c`
   pulling `arrayfuncs_c.inc` (deconstruct_array(+builtin),
   array_contains_nulls, ArrayGetNItems[Safe]), `string_c.inc` (strtoint),
   `jsonb_object_c.inc`, `jsonfuncs_ops_c.inc` (getfield/extract_path/
   concat/delete*/set/insert/IteratorConcat/setPath*); io TU gained
   `numeric_cmp_c.inc` (numeric_cmp/eq + cmp_* statics +
   hash_numeric[_extended]) and `varlena_cmp_c.inc` (varstr_cmp +
   check_collation_set) replacing its abort stubs, plus verbatim
   hashchar/hashcharextended (hashfunc.c) for the jbvBool hash arm.
2. **Environment pins** — database collation C: shim/utils/pg_locale.h
   (collate_is_c=true; pg_strncoll abort-loud) mirrored on the Rust side by
   pg_locale_seams::varstr_cmp_locale := varlena::varstrfastcmp_c; real
   detoast installed for text[] args. text[] arguments are ONE flat
   ArrayType image built by the Rust driver, handed to both sides
   (construction = environment; each side runs its own deconstruction).
3. **Arms** — 11 selector arms / 30 catalog oids' worth of surface:
   cmp(+all 6 bool wrappers), contains/contained, exists, exists_any/all,
   hash/hash_extended, object_field(_text), array_element(_text),
   extract_path(_text), set/insert/delete_path, delete key/idx/text[],
   concat, jsonb_object(text[]) + two-arg. -1 = SQL NULL plane; -2 grow
   retry protocol as jsonbio_diff.
4. **Seeding obligation met** — 143 hand seeds incl. 28 single-field-
   difference witness PAIRS (both orders: one key, one value, one element,
   one nesting level apart), VT-whitespace (0x0b) subscripts, negative and
   INT32_MIN indices, 12-digit subscripts, -0 vs 0, empty-array quirks,
   null-element arrays, 2-D jsonb_object shapes; plus the fuzz-grown corpus
   committed.
5. **Smoke** — 2,403,255 execs / 121 s local (ASan + C sancov), 0
   divergences; in-crate tests green (arms_smoke + seed replay).

Open follow-ups (not this target): jsonb_set_lax (non-strict fmgr NULL
handling + null_value_treatment text arg — needs a NULL-capable fc plane);
deep-nesting 54001 parity (jsonbio_diff carve, same here).
