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

1. ~~**Table aliases**~~ — DONE
2. ~~**Column aliases (AS in SELECT list)**~~ — DONE
3. ~~**COUNT(DISTINCT expr)**~~ — DONE
4. ~~**Scalar subqueries**~~ — DONE
5. ~~**Type cast operator `::`**~~ — DONE
6. ~~**`= ANY(...)` operator**~~ — DONE
7. ~~**`ARRAY[...]` constructor**~~ — DONE
8. ~~**Array type syntax** (`varchar[]`)~~ — DONE
9. ~~**`&&` (array overlap) operator**~~ — DONE
10. ~~**`VARCHAR` type**~~ — DONE

## Type system gaps

11. ~~**Array types**~~ — DONE
12. ~~**`VARCHAR(n)` type**~~ — DONE

## Function / Expression gaps

13. ~~**`UNNEST()` function**~~ — DONE
14. ~~**Column alias list for SRFs**~~ — DONE
15. ~~**Correlated subqueries**~~ — DONE
16. ~~**Derived tables / subqueries in FROM**~~ — DONE for non-`LATERAL` cases, including parenthesized join grouping and derived tables such as `FROM (SELECT ...) alias`

## Executor / Planner gaps

17. ~~**SubPlan execution**~~ — DONE
18. ~~**Array overlap (`&&`) operator implementation**~~ — DONE
19. ~~**`= ANY(array)` evaluation**~~ — DONE
20. ~~**SRF (set-returning function) in FROM**~~ — DONE, including general `FROM`-list support and SRF cross joins such as `generate_series(...), generate_series(...)`; `UNNEST` itself is still missing
21. ~~**Multi-argument UNNEST**~~ — DONE
