# General FROM-list support for SRFs and cross joins

## Summary

The parser and planner only support a narrow subset of `FROM` joins:

- `table`
- `table JOIN table ON ...`
- `table, table`
- a single set-returning function such as `generate_series(1, 10)`

They do not support a general `FROM` list of arbitrary from-items. Queries like
`select * from generate_series(1, 10), generate_series(5, 20)` therefore fail
even though both inputs are individually supported.

Today this is limited in three places:

- `SelectStatement` stores only one `from: Option<FromItem>`
- the grammar only accepts `identifier , identifier` for comma joins
- the analyzer resolves comma joins as two catalog tables, not arbitrary plans

## Why deferred

Fixing this cleanly wants a broader representation for `FROM`, not another
special case for `generate_series`.

A proper implementation should:

- represent `FROM` as a list or recursive join tree of arbitrary from-items
- parse comma joins over any `from_item`, including SRFs
- plan cross joins where either side is a table, SRF, or future subquery
- preserve sensible column naming and scope resolution for mixed sources

Until that exists, support for `generate_series` in `FROM` remains limited to a
single function call, and `FROM a, b` only works for plain table names.
