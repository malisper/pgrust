Goal:
Diagnose and reduce current PostgreSQL `xml` regression failures in pgrust.

Key decisions:
Ran `scripts/run_regression.sh --test xml --results-dir /tmp/pgrust_xml_regress --timeout 120 --port 61000 --jobs 1` because default ports 5433 and 55433 were occupied.
Initial current result differed from the prompt: 159 mismatched queries, 122/281 matched, 1529 diff lines.
Fixed parser XML element argument unwrapping so `xmlelement(...)` preserves XMLATTRIBUTES and content.
Added PostgreSQL-style default output names for raw XML expressions (`xmlelement`, `xmlparse`, `xmlserialize`, etc.) instead of `?column?`.
Tightened XML validation for malformed declarations, undefined entity references, and DOCTYPE-containing content/document shape. This makes several semantic failures match PostgreSQL even where error cursor/detail formatting still differs.
Current result after fixes: 103 mismatched queries, 178/281 matched, 1051 diff lines.

Files touched:
.codex/task-notes/xml-regression.md
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/executor/expr_xml.rs
src/backend/executor/expr_casts.rs

Tests run:
`scripts/run_regression.sh --test xml --results-dir /tmp/pgrust_xml_regress --timeout 120 --port 61000 --jobs 1`
`scripts/cargo_isolated.sh test --lib --quiet xml`
`scripts/cargo_isolated.sh check`

Remaining:
Remaining top clusters: XPath behavior/formatting, XML validation detail/cursor formatting, XMLEXISTS unsupported, XMLPI compatibility, XMLTABLE output/EXPLAIN formatting, `xmltext`, prepared EXECUTE, XMLROOT declaration handling, XMLAGG builtin OID/runtime, XMLCONCAT type checking.
