# Lane batch-function registry — coverage-drift report

Generated from `lanereg::ENTRIES` (`lanereg::coverage_report`).
`IN` = in-tree, `..` = pending on a side branch, `RF` = documented refusal
(see the refusals section), `-` = not covered.

| OID | name | aot-cmp | jit-arith | fold | fold-affine | stitch-cmp | stitch-arith | stitch-saop | drift |
|----:|------|:-------:|:---------:|:----:|:-----------:|:----------:|:------------:|:-----------:|-------|
| 65 | int4eq | IN | - | - | - | IN | - | IN |  |
| 144 | int4ne | IN | - | - | - | IN | - | IN |  |
| 66 | int4lt | IN | - | - | - | IN | - | IN |  |
| 149 | int4le | IN | - | - | - | IN | - | IN |  |
| 147 | int4gt | IN | - | - | - | IN | - | IN |  |
| 150 | int4ge | IN | - | - | - | IN | - | IN |  |
| 467 | int8eq | IN | - | - | - | IN | - | IN |  |
| 468 | int8ne | IN | - | - | - | IN | - | IN |  |
| 469 | int8lt | IN | - | - | - | IN | - | IN |  |
| 471 | int8le | IN | - | - | - | IN | - | IN |  |
| 470 | int8gt | IN | - | - | - | IN | - | IN |  |
| 472 | int8ge | IN | - | - | - | IN | - | IN |  |
| 63 | int2eq | IN | - | - | - | IN | - | IN |  |
| 145 | int2ne | IN | - | - | - | IN | - | IN |  |
| 64 | int2lt | IN | - | - | - | IN | - | IN |  |
| 148 | int2le | IN | - | - | - | IN | - | IN |  |
| 146 | int2gt | IN | - | - | - | IN | - | IN |  |
| 151 | int2ge | IN | - | - | - | IN | - | IN |  |
| 474 | int84eq | IN | - | - | - | IN | - | IN |  |
| 475 | int84ne | IN | - | - | - | IN | - | IN |  |
| 476 | int84lt | IN | - | - | - | IN | - | IN |  |
| 478 | int84le | IN | - | - | - | IN | - | IN |  |
| 477 | int84gt | IN | - | - | - | IN | - | IN |  |
| 479 | int84ge | IN | - | - | - | IN | - | IN |  |
| 852 | int48eq | IN | - | - | - | IN | - | IN |  |
| 853 | int48ne | IN | - | - | - | IN | - | IN |  |
| 854 | int48lt | IN | - | - | - | IN | - | IN |  |
| 856 | int48le | IN | - | - | - | IN | - | IN |  |
| 855 | int48gt | IN | - | - | - | IN | - | IN |  |
| 857 | int48ge | IN | - | - | - | IN | - | IN |  |
| 158 | int24eq | IN | - | - | - | IN | - | IN |  |
| 164 | int24ne | IN | - | - | - | IN | - | IN |  |
| 160 | int24lt | IN | - | - | - | IN | - | IN |  |
| 166 | int24le | IN | - | - | - | IN | - | IN |  |
| 162 | int24gt | IN | - | - | - | IN | - | IN |  |
| 168 | int24ge | IN | - | - | - | IN | - | IN |  |
| 159 | int42eq | IN | - | - | - | IN | - | IN |  |
| 165 | int42ne | IN | - | - | - | IN | - | IN |  |
| 161 | int42lt | IN | - | - | - | IN | - | IN |  |
| 167 | int42le | IN | - | - | - | IN | - | IN |  |
| 163 | int42gt | IN | - | - | - | IN | - | IN |  |
| 169 | int42ge | IN | - | - | - | IN | - | IN |  |
| 184 | oideq | IN | - | - | - | IN | - | IN |  |
| 185 | oidne | IN | - | - | - | IN | - | IN |  |
| 716 | oidlt | IN | - | - | - | IN | - | IN |  |
| 717 | oidle | IN | - | - | - | IN | - | IN |  |
| 1638 | oidgt | IN | - | - | - | IN | - | IN |  |
| 1639 | oidge | IN | - | - | - | IN | - | IN |  |
| 287 | float4eq | IN | - | - | - | IN | - | - |  |
| 288 | float4ne | IN | - | - | - | IN | - | - |  |
| 289 | float4lt | IN | - | - | - | IN | - | - |  |
| 290 | float4le | IN | - | - | - | IN | - | - |  |
| 291 | float4gt | IN | - | - | - | IN | - | - |  |
| 292 | float4ge | IN | - | - | - | IN | - | - |  |
| 293 | float8eq | IN | - | - | - | IN | - | - |  |
| 294 | float8ne | IN | - | - | - | IN | - | - |  |
| 295 | float8lt | IN | - | - | - | IN | - | - |  |
| 296 | float8le | IN | - | - | - | IN | - | - |  |
| 297 | float8gt | IN | - | - | - | IN | - | - |  |
| 298 | float8ge | IN | - | - | - | IN | - | - |  |
| 299 | float48eq | IN | - | - | - | IN | - | - |  |
| 300 | float48ne | IN | - | - | - | IN | - | - |  |
| 301 | float48lt | IN | - | - | - | IN | - | - |  |
| 302 | float48le | IN | - | - | - | IN | - | - |  |
| 303 | float48gt | IN | - | - | - | IN | - | - |  |
| 304 | float48ge | IN | - | - | - | IN | - | - |  |
| 305 | float84eq | IN | - | - | - | IN | - | - |  |
| 306 | float84ne | IN | - | - | - | IN | - | - |  |
| 307 | float84lt | IN | - | - | - | IN | - | - |  |
| 308 | float84le | IN | - | - | - | IN | - | - |  |
| 309 | float84gt | IN | - | - | - | IN | - | - |  |
| 310 | float84ge | IN | - | - | - | IN | - | - |  |
| 1086 | date_eq | IN | - | - | - | IN | - | IN |  |
| 1091 | date_ne | IN | - | - | - | IN | - | IN |  |
| 1087 | date_lt | IN | - | - | - | IN | - | IN |  |
| 1088 | date_le | IN | - | - | - | IN | - | IN |  |
| 1089 | date_gt | IN | - | - | - | IN | - | IN |  |
| 1090 | date_ge | IN | - | - | - | IN | - | IN |  |
| 2052 | timestamp_eq | IN | - | - | - | IN | - | IN |  |
| 2053 | timestamp_ne | IN | - | - | - | IN | - | IN |  |
| 2054 | timestamp_lt | IN | - | - | - | IN | - | IN |  |
| 2055 | timestamp_le | IN | - | - | - | IN | - | IN |  |
| 2057 | timestamp_gt | IN | - | - | - | IN | - | IN |  |
| 2056 | timestamp_ge | IN | - | - | - | IN | - | IN |  |
| 1152 | timestamptz_eq | IN | - | - | - | IN | - | IN |  |
| 1153 | timestamptz_ne | IN | - | - | - | IN | - | IN |  |
| 1154 | timestamptz_lt | IN | - | - | - | IN | - | IN |  |
| 1155 | timestamptz_le | IN | - | - | - | IN | - | IN |  |
| 1157 | timestamptz_gt | IN | - | - | - | IN | - | IN |  |
| 1156 | timestamptz_ge | IN | - | - | - | IN | - | IN |  |
| 177 | int4pl | - | IN | - | IN | - | IN | - |  |
| 181 | int4mi | - | IN | - | IN | - | IN | - |  |
| 141 | int4mul | - | IN | - | IN | - | IN | - |  |
| 463 | int8pl | - | IN | - | RF | - | - | - |  |
| 464 | int8mi | - | IN | - | RF | - | - | - |  |
| 465 | int8mul | - | IN | - | RF | - | - | - |  |
| 178 | int24pl | - | IN | - | IN | - | - | - |  |
| 179 | int42pl | - | IN | - | IN | - | - | - |  |
| 182 | int24mi | - | IN | - | IN | - | - | - |  |
| 183 | int42mi | - | IN | - | IN | - | - | - |  |
| 170 | int24mul | - | IN | - | IN | - | - | - |  |
| 171 | int42mul | - | IN | - | IN | - | - | - |  |
| 172 | int24div | - | IN | - | IN | - | - | - |  |
| 1219 | int8inc | - | - | IN | - | - | - | - |  |
| 2804 | int8inc_any | - | - | IN | - | - | - | - |  |
| 1840 | int2_sum | - | - | IN | - | - | - | - |  |
| 1841 | int4_sum | - | - | IN | - | - | - | - |  |
| 1962 | int2_avg_accum | - | - | IN | - | - | - | - |  |
| 1963 | int4_avg_accum | - | - | IN | - | - | - | - |  |
| 2746 | int8_avg_accum | - | - | IN | - | - | - | - |  |
| 768 | int4larger | - | - | IN | - | - | - | - |  |
| 769 | int4smaller | - | - | IN | - | - | - | - |  |
| 770 | int2larger | - | - | IN | - | - | - | - |  |
| 771 | int2smaller | - | - | IN | - | - | - | - |  |
| 1236 | int8larger | - | - | IN | - | - | - | - |  |
| 1237 | int8smaller | - | - | IN | - | - | - | - |  |
| 1138 | date_larger | - | - | IN | - | - | - | - |  |
| 1139 | date_smaller | - | - | IN | - | - | - | - |  |
| 2036 | timestamp_larger | - | - | IN | - | - | - | - |  |
| 2035 | timestamp_smaller | - | - | IN | - | - | - | - |  |
| 1196 | timestamptz_larger | - | - | IN | - | - | - | - |  |
| 1195 | timestamptz_smaller | - | - | IN | - | - | - | - |  |
| 209 | float4larger | - | - | IN | - | - | - | - |  |
| 211 | float4smaller | - | - | IN | - | - | - | - |  |
| 223 | float8larger | - | - | IN | - | - | - | - |  |
| 224 | float8smaller | - | - | IN | - | - | - | - |  |
| 2515 | booland_statefunc | - | - | IN | - | - | - | - |  |
| 2516 | boolor_statefunc | - | - | IN | - | - | - | - |  |
| 1892 | int2and | - | - | IN | - | - | - | - |  |
| 1893 | int2or | - | - | IN | - | - | - | - |  |
| 1898 | int4and | - | - | IN | - | - | - | - |  |
| 1899 | int4or | - | - | IN | - | - | - | - |  |
| 1904 | int8and | - | - | IN | - | - | - | - |  |
| 1905 | int8or | - | - | IN | - | - | - | - |  |
| 458 | text_larger | - | - | IN | - | - | - | - |  |
| 459 | text_smaller | - | - | IN | - | - | - | - |  |
| 1063 | bpchar_larger | - | - | IN | - | - | - | - |  |
| 1064 | bpchar_smaller | - | - | IN | - | - | - | - |  |
| 204 | float4pl | - | - | IN | - | - | - | - |  |
| 218 | float8pl | - | - | IN | - | - | - | - |  |
| 208 | float4_accum | - | - | IN | - | - | - | - |  |
| 222 | float8_accum | - | - | IN | - | - | - | - |  |
| 2858 | numeric_avg_accum | - | - | IN | - | - | - | - |  |
| 2806 | float8_regr_accum | - | - | IN | - | - | - | - |  |
| 2805 | int8inc_float8_float8 | - | - | IN | - | - | - | - |  |

## Summary

- registered OIDs: 145
- stencil-but-no-census: 0 (stitch comparator stencils with no AOT qual census)
- fold-affine-but-no-jit: 0 (fold affine ops the JIT does not inline)
- jit-but-no-fold-affine: 0 (JIT-inlined arith unknown to the fold affine admission)
- pending-only: 0 (coverage only on a side branch)

## Documented refusals

Tier admissions evaluated and deliberately refused — the tier cannot
reproduce byte-identical C semantics under its current framework.

- 463 `int8pl` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
- 464 `int8mi` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
- 465 `int8mul` × fold-affine: int8 affine needs i128 interval proofs (safe_interval/guards are i64, coefficients i32); without an exact interval the fold cannot reproduce C's int8 overflow ereport
