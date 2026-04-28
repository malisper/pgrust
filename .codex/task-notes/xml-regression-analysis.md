Goal:
Analyze xml regression diff from .context/attachments/pasted_text_2026-04-27_11-18-40.txt, count failure reasons, then implement the XMLTABLE bucket.

Key decisions:
Grouped failures by root cause rather than exact output line. XMLTABLE/XPath/xmltext are unsupported feature buckets; XMLELEMENT/XMLROOT/XMLSERIALIZE/XML validation are partial implementation buckets.
Implemented XMLTABLE through parser, raw AST, binder, set-returning plan node, executor, view deparse, EXPLAIN, dependency/reference walkers, PL/pgSQL normalization, and PG_NOTIFY checks. Kept XPath support internal to XMLTABLE and regression-scoped.

Files touched:
XMLTABLE implementation touches grammar/parser, parsenodes/primnodes, analyzer scope/query visitors, executor XML/SRF, EXPLAIN/view deparse, optimizer/rewrite expression walkers, catalog dependency/reference walkers, PL/pgSQL normalization, and focused executor/parser tests.

Tests run:
- `scripts/cargo_isolated.sh check`
- `CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet xml_table`
- `CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-xmltable-target scripts/run_regression.sh --test xml --jobs 1 --timeout 240 --port 55472 --results-dir /tmp/pgrust-xmltable-krakow3`

Remaining:
The XMLTABLE parser/binder/runtime bucket is no longer an unsupported/misparsed failure, and `xmltableview1/2` now exist and query. Remaining xml regression diffs are mostly unrelated XML function parity plus XMLTABLE formatting/explain details and a few scoped XPath gaps such as CDATA relative path without a leading slash and `*` row-path behavior.
