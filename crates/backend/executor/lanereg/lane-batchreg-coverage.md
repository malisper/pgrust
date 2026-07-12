# Lane batch-function registry — coverage-drift report

Generated from `lanereg::ENTRIES` (`lanereg::coverage_report`).
`IN` = in-tree, `..` = pending on a side branch, `RF` = documented refusal
(see the refusals section), `-` = not covered.

| OID | name | aot-cmp | jit-arith | fold | fold-affine | stitch-cmp | stitch-arith | drift |
|----:|------|:-------:|:---------:|:----:|:-----------:|:----------:|:------------:|-------|
| 65 | int4eq | IN | - | - | - | .. | - |  |
| 144 | int4ne | IN | - | - | - | .. | - |  |
| 66 | int4lt | IN | - | - | - | .. | - |  |
| 149 | int4le | IN | - | - | - | .. | - |  |
| 147 | int4gt | IN | - | - | - | .. | - |  |
| 150 | int4ge | IN | - | - | - | .. | - |  |
| 467 | int8eq | IN | - | - | - | .. | - |  |
| 468 | int8ne | IN | - | - | - | .. | - |  |
| 469 | int8lt | IN | - | - | - | .. | - |  |
| 471 | int8le | IN | - | - | - | .. | - |  |
| 470 | int8gt | IN | - | - | - | .. | - |  |
| 472 | int8ge | IN | - | - | - | .. | - |  |
| 63 | int2eq | IN | - | - | - | .. | - |  |
| 145 | int2ne | IN | - | - | - | .. | - |  |
| 64 | int2lt | IN | - | - | - | .. | - |  |
| 148 | int2le | IN | - | - | - | .. | - |  |
| 146 | int2gt | IN | - | - | - | .. | - |  |
| 151 | int2ge | IN | - | - | - | .. | - |  |
| 474 | int84eq | IN | - | - | - | .. | - |  |
| 475 | int84ne | IN | - | - | - | .. | - |  |
| 476 | int84lt | IN | - | - | - | .. | - |  |
| 478 | int84le | IN | - | - | - | .. | - |  |
| 477 | int84gt | IN | - | - | - | .. | - |  |
| 479 | int84ge | IN | - | - | - | .. | - |  |
| 852 | int48eq | IN | - | - | - | .. | - |  |
| 853 | int48ne | IN | - | - | - | .. | - |  |
| 854 | int48lt | IN | - | - | - | .. | - |  |
| 856 | int48le | IN | - | - | - | .. | - |  |
| 855 | int48gt | IN | - | - | - | .. | - |  |
| 857 | int48ge | IN | - | - | - | .. | - |  |
| 158 | int24eq | IN | - | - | - | .. | - |  |
| 164 | int24ne | IN | - | - | - | .. | - |  |
| 160 | int24lt | IN | - | - | - | .. | - |  |
| 166 | int24le | IN | - | - | - | .. | - |  |
| 162 | int24gt | IN | - | - | - | .. | - |  |
| 168 | int24ge | IN | - | - | - | .. | - |  |
| 159 | int42eq | IN | - | - | - | .. | - |  |
| 165 | int42ne | IN | - | - | - | .. | - |  |
| 161 | int42lt | IN | - | - | - | .. | - |  |
| 167 | int42le | IN | - | - | - | .. | - |  |
| 163 | int42gt | IN | - | - | - | .. | - |  |
| 169 | int42ge | IN | - | - | - | .. | - |  |
| 184 | oideq | IN | - | - | - | .. | - |  |
| 185 | oidne | IN | - | - | - | .. | - |  |
| 716 | oidlt | IN | - | - | - | .. | - |  |
| 717 | oidle | IN | - | - | - | .. | - |  |
| 1638 | oidgt | IN | - | - | - | .. | - |  |
| 1639 | oidge | IN | - | - | - | .. | - |  |
| 287 | float4eq | IN | - | - | - | .. | - |  |
| 288 | float4ne | IN | - | - | - | .. | - |  |
| 289 | float4lt | IN | - | - | - | .. | - |  |
| 290 | float4le | IN | - | - | - | .. | - |  |
| 291 | float4gt | IN | - | - | - | .. | - |  |
| 292 | float4ge | IN | - | - | - | .. | - |  |
| 293 | float8eq | IN | - | - | - | .. | - |  |
| 294 | float8ne | IN | - | - | - | .. | - |  |
| 295 | float8lt | IN | - | - | - | .. | - |  |
| 296 | float8le | IN | - | - | - | .. | - |  |
| 297 | float8gt | IN | - | - | - | .. | - |  |
| 298 | float8ge | IN | - | - | - | .. | - |  |
| 299 | float48eq | IN | - | - | - | .. | - |  |
| 300 | float48ne | IN | - | - | - | .. | - |  |
| 301 | float48lt | IN | - | - | - | .. | - |  |
| 302 | float48le | IN | - | - | - | .. | - |  |
| 303 | float48gt | IN | - | - | - | .. | - |  |
| 304 | float48ge | IN | - | - | - | .. | - |  |
| 305 | float84eq | IN | - | - | - | .. | - |  |
| 306 | float84ne | IN | - | - | - | .. | - |  |
| 307 | float84lt | IN | - | - | - | .. | - |  |
| 308 | float84le | IN | - | - | - | .. | - |  |
| 309 | float84gt | IN | - | - | - | .. | - |  |
| 310 | float84ge | IN | - | - | - | .. | - |  |
| 177 | int4pl | - | IN | - | IN | - | .. |  |
| 181 | int4mi | - | IN | - | IN | - | .. |  |
| 141 | int4mul | - | IN | - | IN | - | .. |  |
| 463 | int8pl | - | IN | - | RF | - | - |  |
| 464 | int8mi | - | IN | - | RF | - | - |  |
| 465 | int8mul | - | IN | - | RF | - | - |  |
| 178 | int24pl | - | IN | - | IN | - | - |  |
| 179 | int42pl | - | IN | - | IN | - | - |  |
| 182 | int24mi | - | IN | - | IN | - | - |  |
| 183 | int42mi | - | IN | - | IN | - | - |  |
| 170 | int24mul | - | IN | - | IN | - | - |  |
| 171 | int42mul | - | IN | - | IN | - | - |  |
| 172 | int24div | - | IN | - | IN | - | - |  |
| 1219 | int8inc | - | - | IN | - | - | - |  |
| 2804 | int8inc_any | - | - | IN | - | - | - |  |
| 1840 | int2_sum | - | - | IN | - | - | - |  |
| 1841 | int4_sum | - | - | IN | - | - | - |  |
| 1962 | int2_avg_accum | - | - | IN | - | - | - |  |
| 1963 | int4_avg_accum | - | - | IN | - | - | - |  |
| 768 | int4larger | - | - | IN | - | - | - |  |
| 769 | int4smaller | - | - | IN | - | - | - |  |
| 770 | int2larger | - | - | IN | - | - | - |  |
| 771 | int2smaller | - | - | IN | - | - | - |  |
| 1236 | int8larger | - | - | IN | - | - | - |  |
| 1237 | int8smaller | - | - | IN | - | - | - |  |
| 1138 | date_larger | - | - | IN | - | - | - |  |
| 1139 | date_smaller | - | - | IN | - | - | - |  |
| 2036 | timestamp_larger | - | - | IN | - | - | - |  |
| 2035 | timestamp_smaller | - | - | IN | - | - | - |  |
| 1196 | timestamptz_larger | - | - | IN | - | - | - |  |
| 1195 | timestamptz_smaller | - | - | IN | - | - | - |  |
| 209 | float4larger | - | - | .. | - | - | - | pending-only |
| 211 | float4smaller | - | - | .. | - | - | - | pending-only |
| 223 | float8larger | - | - | .. | - | - | - | pending-only |
| 224 | float8smaller | - | - | .. | - | - | - | pending-only |
| 2515 | booland_statefunc | - | - | .. | - | - | - | pending-only |
| 2516 | boolor_statefunc | - | - | .. | - | - | - | pending-only |
| 1892 | int2and | - | - | .. | - | - | - | pending-only |
| 1893 | int2or | - | - | .. | - | - | - | pending-only |
| 1898 | int4and | - | - | .. | - | - | - | pending-only |
| 1899 | int4or | - | - | .. | - | - | - | pending-only |
| 1904 | int8and | - | - | .. | - | - | - | pending-only |
| 1905 | int8or | - | - | .. | - | - | - | pending-only |
| 458 | text_larger | - | - | .. | - | - | - | pending-only |
| 459 | text_smaller | - | - | .. | - | - | - | pending-only |
| 1063 | bpchar_larger | - | - | .. | - | - | - | pending-only |
| 1064 | bpchar_smaller | - | - | .. | - | - | - | pending-only |

## Summary

- registered OIDs: 119
- stencil-but-no-census: 0 (stitch comparator stencils with no AOT qual census)
- fold-affine-but-no-jit: 0 (fold affine ops the JIT does not inline)
- jit-but-no-fold-affine: 0 (JIT-inlined arith unknown to the fold affine admission)
- pending-only: 16 (coverage only on a side branch)

## Documented refusals

Tier admissions evaluated and deliberately refused — the tier cannot
reproduce byte-identical C semantics under its current framework.

- 463 `int8pl` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
- 464 `int8mi` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
- 465 `int8mul` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
