Goal:
Diagnose and reduce current PostgreSQL `xml` regression failures in pgrust.

Key decisions:
Ran `scripts/run_regression.sh --test xml --results-dir /tmp/pgrust_xml_regress --timeout 120 --port 61000 --jobs 1` because default ports 5433 and 55433 were occupied.
Initial current result differed from the prompt: 159 mismatched queries, 122/281 matched, 1529 diff lines.
Fixed parser XML element argument unwrapping so `xmlelement(...)` preserves XMLATTRIBUTES and content.
Added PostgreSQL-style default output names for raw XML expressions (`xmlelement`, `xmlparse`, `xmlserialize`, etc.) instead of `?column?`.
Tightened XML validation for malformed declarations, undefined entity references, and DOCTYPE-containing content/document shape. This makes several semantic failures match PostgreSQL even where error cursor/detail formatting still differs.
Current result after fixes: 103 mismatched queries, 178/281 matched, 1051 diff lines.
Follow-up XPath fix added `xpath`, `xpath_exists`, and standard `xmlexists(... PASSING ...)` support over the existing XML parser. It covers text/attribute/node results, basic predicates, counts, booleans, namespace aliases, namespace validation details, relative namespace warnings, and PostgreSQL-style XML array output.
Current result after XPath fixes: 68 mismatched queries, 213/281 matched, 790 diff lines.

Files touched:
.codex/task-notes/xml-regression.md
crates/pgrust_sql_grammar/src/gram.pest
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/functions.rs
src/backend/executor/expr_xml.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs

Tests run:
`scripts/run_regression.sh --test xml --results-dir /tmp/pgrust_xml_regress --timeout 120 --port 61000 --jobs 1`
`scripts/cargo_isolated.sh test --lib --quiet xml`
`scripts/cargo_isolated.sh test --lib --quiet xpath`
`scripts/cargo_isolated.sh test --lib --quiet parse_xml_exists_expr`
`scripts/cargo_isolated.sh check`

Remaining:
Remaining top clusters: XMLTABLE output/view/EXPLAIN formatting and a CDATA row-path gap, XML validation detail/cursor formatting, XMLPI compatibility, XMLELEMENT/XMLFOREST compatibility, XMLROOT declaration handling, XMLSERIALIZE comparison/type behavior, XMLAGG builtin OID/runtime, prepared EXECUTE support, `xmltext`, XMLCONCAT type checking. XPath residuals are now limited to missing PostgreSQL `CONTEXT: SQL function "xpath" statement 1` lines on two errors plus the unrelated PL/pgSQL `RAISE LOG` parse failure in the non-ASCII XPath DO block.
