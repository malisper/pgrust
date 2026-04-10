# Feature gaps for shipments query

Target query:

```sql
SELECT
    om_shipments.company_id,
    COUNT(DISTINCT om_shipments.id) AS shipments_filtered,
    SUM(
        (SELECT COUNT(*)
         FROM unnest(
             om_shipments.container_numbers,
             om_shipments.container_types_categories,
             om_shipments.container_size_categories
         ) AS c(num, type_cat, size_cat)
         WHERE (c.size_cat)::text = ANY(ARRAY['40_high_cube']::varchar[])
        )
    ) AS containers_filtered
FROM om_shipments om_shipments, _timeout
WHERE om_shipments.year = '2024'
    AND om_shipments.container_size_categories && ARRAY['40_high_cube']::varchar[]
GROUP BY om_shipments.company_id
```

## Parser / Grammar gaps

1. **Table aliases** — `FROM om_shipments om_shipments` — `table_from_item` is just `identifier`, no optional alias
2. ~~**Column aliases (AS in SELECT list)**~~ — DONE
3. ~~**COUNT(DISTINCT expr)**~~ — DONE
4. **Scalar subqueries** — `SUM((SELECT ...))` — no subquery expression support anywhere
5. **Type cast operator `::`** — `(c.size_cat)::text` — not in grammar
6. **`= ANY(...)` operator** — `ANY`/`SOME` array comparison not supported
7. **`ARRAY[...]` constructor** — array literal syntax not in grammar
8. **Array type syntax** (`varchar[]`) — no array type notation
9. **`&&` (array overlap) operator** — not in `comp_op` (only `=`, `<`, `>`, `~`)
10. **`VARCHAR` type** — `type_name` only has int4/int/integer/text/bool/boolean/timestamp/char

## Type system gaps

11. **Array types** — no concept of array columns, array storage, or array operations in the type system (`SqlType` has no array variant)
12. **`VARCHAR(n)` type** — no variable-length character type with length constraint

## Function / Expression gaps

13. **`UNNEST()` function** — set-returning function that expands arrays into rows
14. **Column alias list for SRFs** — `AS c(num, type_cat, size_cat)` — naming columns returned from a function
15. **Correlated subqueries** — the inner `SELECT` references `om_shipments.container_*` from the outer query
16. **Parenthesized expressions in FROM** — `(SELECT ... FROM unnest(...))` used as a from-source

## Executor / Planner gaps

17. **SubPlan execution** — executing a subquery per-row as a scalar expression
18. **Array overlap (`&&`) operator implementation** — runtime semantics for array containment checks
19. **`= ANY(array)` evaluation** — comparing a scalar against each element of an array
20. ~~**SRF (set-returning function) in FROM**~~ — DONE for basic single-function cases such as `generate_series(...)`; general `FROM`-list support with SRF cross joins and `UNNEST` is still missing
21. **Multi-argument UNNEST** — PostgreSQL's `unnest(a, b, c)` zips multiple arrays into parallel columns
