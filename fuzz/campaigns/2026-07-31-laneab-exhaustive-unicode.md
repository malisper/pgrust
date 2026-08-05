# a0 EXHAUSTIVE-DIFF record — adt/json \uXXXX escape domain (p1-laneab)

- driver: fuzz/core/src/json_diff.rs tests::exhaustive_unicode_escape_domain
- domain: ALL 2^16 single \uXXXX escapes through json_in (validation lane)
  AND ->> de-escape (json_object_field_text), plus ALL high-surrogate x
  2^16 second-escape pairs (0xD800..=0xDBFF x 0x0000..=0xFFFF) through the
  de-escape lane = 67,174,400 cases. TOTAL over the 1- and 2-escape domain
  (non-high first escapes carry no state; low-first fails before the second
  escape is read — argument in the test doc comment).
- planes: value + verdict + sqlstate + SQL-NULL + fc-wrapper + soft-error.
- oracle: verbatim PG 18.3 jsonapi.c/json.c/jsonfuncs.c (csrc/jsonfam,
  csrc/pg_json_io.c), UTF8 pin.
- result: PASS, 0 divergences.
- host: Apple M4 Pro (laptop, release build), wall 83.07s, 2026-07-31.
- repo sha at run: ff70d8f172edeac4d5eb3e1712aead77e349504e
