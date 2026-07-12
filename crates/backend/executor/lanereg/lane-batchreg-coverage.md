# Lane batch-function registry — coverage-drift report

Generated from `lanereg::ENTRIES` (`lanereg::coverage_report`).
`IN` = in-tree, `..` = pending on a side branch, `-` = not covered.

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
| 158 | int24eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 164 | int24ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 160 | int24lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 166 | int24le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 162 | int24gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 168 | int24ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 159 | int42eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 165 | int42ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 161 | int42lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 167 | int42le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 163 | int42gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 169 | int42ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 184 | oideq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 185 | oidne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 716 | oidlt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 717 | oidle | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 1638 | oidgt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 1639 | oidge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 287 | float4eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 288 | float4ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 289 | float4lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 290 | float4le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 291 | float4gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 292 | float4ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 293 | float8eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 294 | float8ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 295 | float8lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 296 | float8le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 297 | float8gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 298 | float8ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 299 | float48eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 300 | float48ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 301 | float48lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 302 | float48le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 303 | float48gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 304 | float48ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 305 | float84eq | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 306 | float84ne | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 307 | float84lt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 308 | float84le | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 309 | float84gt | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 310 | float84ge | - | - | - | - | .. | - | stencil-but-no-census, pending-only |
| 177 | int4pl | - | IN | - | IN | - | .. |  |
| 181 | int4mi | - | IN | - | IN | - | .. |  |
| 141 | int4mul | - | IN | - | IN | - | .. |  |
| 463 | int8pl | - | IN | - | - | - | - | jit-but-no-fold-affine |
| 464 | int8mi | - | IN | - | - | - | - | jit-but-no-fold-affine |
| 465 | int8mul | - | IN | - | - | - | - | jit-but-no-fold-affine |
| 178 | int24pl | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 179 | int42pl | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 182 | int24mi | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 183 | int42mi | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 170 | int24mul | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 171 | int42mul | - | - | - | IN | - | - | fold-affine-but-no-jit |
| 172 | int24div | - | - | - | IN | - | - | fold-affine-but-no-jit |
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
- stencil-but-no-census: 42 (stitch comparator stencils with no AOT qual census)
- fold-affine-but-no-jit: 7 (fold affine ops the JIT does not inline)
- jit-but-no-fold-affine: 3 (JIT-inlined arith unknown to the fold affine admission)
- pending-only: 58 (coverage only on a side branch)
