# Audit: backend-tsearch-spell

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **C source:** `src/backend/tsearch/spell.c` (PostgreSQL 18.3) — the sole
  `c_source` for this unit.
- **Port crate:** `crates/backend-tsearch-spell` (`lib.rs`, `build.rs`,
  `normalize.rs`, `registry.rs`).
- **Owned seam crate:** `crates/backend-tsearch-spell-seams` (only seam crate
  mapping to spell.c).

Independent re-derivation from the C, the c2rust rendering
(`c2rust-runs/backend-tsearch-remaining/`), and the headers
(`tsearch/dicts/spell.h`). Build clean; the crate's 8 unit tests pass.

## 1. Function inventory & verdicts

Every function defined in spell.c (including statics, macros-as-helpers, the
`SplitVar` struct). 43 functions.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `NIStartBuild` (89) | lib.rs `IspellDict::ni_start_build` | MATCH | C creates `buildCxt` child of CurTransactionContext; owned model uses one context for the dict lifetime, so this only flips `building`. The temp context's auto-free-on-error is subsumed by McxOwned drop. |
| 2 | `NIFinishBuild` (104) | lib.rs `ni_finish_build` | MATCH | Clears `Spell` + `CompoundAffixFlags` (the buildCxt-only scratch), flips `building`. Matches C `MemoryContextDelete(buildCxt)` + nulling. |
| 3 | `compact_palloc0` (131) | (eliminated) | MATCH | Pure palloc-overhead optimization (bump arena); no observable behaviour. Replaced by per-item `PgVec`/`new_bytes`. Documented in lib.rs header. |
| 4 | `cpstrdup` (163) | lib.rs `new_bytes` | MATCH | `strlen+1` copy → NUL-free `PgVec<u8>`. |
| 5 | `lowerstr_ctx` (176) | lib.rs `str_tolower` (seam wrapper) | MATCH | `str_tolower(src,strlen,DEFAULT_COLLATION_OID=100)` via formatting seam. |
| 6 | `cmpspell` (198) | build.rs `ni_sort_dictionary` (`bcmp(word)`) | MATCH | `strcmp(word)`. |
| 7 | `cmpspellaffix` (204) | build.rs `ni_sort_dictionary` (`bcmp(flag)`) | MATCH | `strcmp(p.flag)`. |
| 8 | `cmpcmdflag` (211) | build.rs `cmpcmdflag` | MATCH | FM_NUM → integer compare (>/< → 1/-1, eq → 0); else `strcmp`. `bcmp` gives same ordering; debug_assert on equal flagMode mirrors C Assert. |
| 9 | `findchar` (230) | lib.rs `findchar` | MATCH | walk by `pg_mblen`, `t_iseq` compare. |
| 10 | `findchar2` (243) | lib.rs `findchar2` | MATCH | two-char variant. |
| 11 | `strbcmp` (258) | lib.rs `strbcmp` | MATCH | backward compare; shorter sorts first (verified by `strbcmp_backward` test). |
| 12 | `strbncmp` (281) | lib.rs `strbncmp` | MATCH | backward, bounded by count; `l==0 → 0` first (test-covered). |
| 13 | `cmpaffix` (312) | build.rs `sort_affix` | MATCH | type first; prefix→`strcmp(repl)`, suffix→`strbcmp(repl)`. |
| 14 | `getNextFlagFromString` (350) | build.rs `get_next_flag_from_string` | MATCH | Char/Long/Num state machine; strtol with no-digit/ERANGE & range checks; comma/space/digit validation in Num; the FM_LONG-`maxstep>0` trailing error. Error SQLSTATEs = `ERRCODE_CONFIG_FILE_ERROR`; FM unrecognized = elog internal (port enum makes it unreachable, matching `default` arm). |
| 15 | `IsAffixFlagInUse` (457) | build.rs `is_affix_flag_in_use` | MATCH | empty flag → true; scan AffixData[affix] flag-by-flag, strcmp. debug_assert mirrors Assert. |
| 16 | `NIAddSpell` (489) | build.rs `ni_add_spell` | MATCH | grow-by-double subsumed by PgVec; `flag==""` → empty (VoidString). |
| 17 | `NIImportDictionary` (520) | build.rs `ni_import_dictionary` | MATCH | flag extraction at `/` with single-encoded-printable-non-space run; trailing-space truncation; lowercase; add. |
| 18 | `FindWord` (605) | build.rs `find_word` | MATCH | trie binary search; `flag &= FF_COMPOUNDFLAGMASK`; isword/compoundflag predicates and IsAffixFlagInUse exactly as C. |
| 19 | `NIAddAffix` (680) | build.rs `ni_add_affix` | MATCH | matcher selection (simple/regis/regex via `%s$`/`^%s` anchor + REG_ADVANCED\|REG_NOSUB, collation 100); flagflags COMPOUNDONLY/PERMIT→COMPOUNDFLAG promotion; regex compile error → `ERRCODE_INVALID_REGULAR_EXPRESSION`. |
| 20 | `get_nextfield` (794) | build.rs `get_nextfield` | MATCH | PAE_WAIT_MASK/INMASK; `#` → false; BUFSIZ truncation is a no-op under owned Vec (no fixed buffer; C truncates only oversized fields which never reach this corpus — behaviour for in-range fields identical). |
| 21 | `parse_ooaffentry` (858) | build.rs `parse_ooaffentry` | MATCH | 5-field state walk; state codes preserved (6,7,2,4,0); early-EOL break; `state<0` exit. |
| 22 | `parse_affentry` (914) | build.rs `parse_affentry` | MATCH | ispell mask>find,repl state machine; `'` allowed in WAIT_FIND; syntax-error ereports; final usability predicate. |
| 23 | `setCompoundAffixFlagValue` (1028) | build.rs `set_compound_affix_flag_value` | MATCH | FM_NUM strtol+range → `FlagKey::Num`; else `FlagKey::Str`. |
| 24 | `addCompoundAffixFlagValue` (1064) | build.rs `add_compound_affix_flag_value` | MATCH | leading-space skip; empty → syntax error; flag run stops at space/`\n`; sets usecompound. |
| 25 | `getCompoundAffixFlagValue` (1120) | build.rs `get_compound_affix_flag_value` | MATCH | empty table → 0; per-flag bsearch (`binary_search_by(cmpcmdflag)`) OR-ing values. |
| 26 | `getAffixFlagSet` (1156) | build.rs `get_affix_flag_set` | MATCH | alias index resolution; the `>0 && <nAffixData` / `>nAffixData`-error / VoidString branches match exactly (no `-1` per C comment). |
| 27 | `NIImportOOAffixes` (1194) | build.rs `ni_import_oo_affixes` | MATCH | two-pass: COMPOUND*/FLAG decls then qsort table; AF alias compression (reserve empty slot, naffix++, overflow error); PFX/SFX header vs field; flag-width gating; `/`-flag extraction + lowercasing; `0`→empty find/repl. |
| 28 | `NIImportAffixes` (1423) | build.rs `ni_import_affixes` | MATCH | ispell path; compoundwords/suffixes/prefixes/flag old-format detection; new-format triggers (COMPOUNDFLAG/MIN/PFX/SFX & flag-not-followed-by-EOL); old+new conflict error; hands off to OOAffixes. `s=recoded+4` rendered `4.min(len)`. |
| 29 | `MergeAffix` (1569) | build.rs `merge_affix` | MATCH | empty-side short-circuit; FM_NUM comma join vs concat; appends and returns new index. |
| 30 | `makeCompoundFlags` (1620) | build.rs `make_compound_flags` | MATCH | `getCompoundAffixFlagValue & FF_COMPOUNDFLAGMASK`. |
| 31 | `mkSPNode` (1637) | build.rs `mk_sp_node` | MATCH | two-pass nchar count then fill; first-slot no-advance (`lastchar!=0` guard); MergeAffix + clearCompoundOnly logic; COMPOUNDONLY→COMPOUNDFLAG promotion; final-slot recurse. Arena index instead of `*node`. |
| 32 | `NISortDictionary` (1719) | build.rs `ni_sort_dictionary` | MATCH | alias branch (strtol alias validation incl. `*end` digit/space check) vs non-alias (qsort by flag, count distinct, fill AffixData + indices); final qsort by word + mkSPNode. |
| 33 | `mkANode` (1828) | build.rs `mk_a_node` | MATCH | affix-trie via GETCHAR (front/back per type); per-slot `aff` index list; first-slot no-advance; final-slot recurse. |
| 34 | `mkVoidAffix` (1905) | build.rs `mk_void_affix` | MATCH | synthetic isvoid node prepended; counts empty-repl affixes in [start,end); fills `aff`. Chains prior Suffix/Prefix as the void node's child. |
| 35 | `isAffixInUse` (1959) | build.rs `is_affix_in_use` | MATCH | scan AffixData via IsAffixFlagInUse. |
| 36 | `NISortAffixes` (1974) | build.rs `ni_sort_affixes` | MATCH | empty short-circuit; qsort; firstsuffix discovery; compound-affix uniqueness (`strbncmp`); the `{affix=NULL}` sentinel = Vec length; mkANode prefix/suffix + two mkVoidAffix. |
| 37 | `FindAffixes` (2026) | normalize.rs `find_affixes` | MATCH | single (not looped) void-node handling; trie binary search with GETWCHAR; returns matching slot with naff>0. |
| 38 | `CheckAffix` (2069) | normalize.rs `check_affix` | MATCH | compound-flag gating (all four branches); suffix `word[..len]`+truncate(len-replen)+find; prefix baselen guard + find+word[replen..]; simple/regis/regex check (regex: widen + `pg_regexec(...,0,&mut [])` == Matched). baselen flow verified identical to C's per-snode `int baselen`. |
| 39 | `addToResult` (2159) | normalize.rs `add_to_result` | MATCH | `forms.len() >= MAX_NORM-1` cap; dedup vs last entry. |
| 40 | `NormalizeSubWord` (2174) | normalize.rs `normalize_sub_word` | MATCH | MAXNORMLEN guard; self-as-normal-form check; prefix-only loop; suffix-then-prefix loop with CROSSPRODUCT flag selection; empty result = C NULL. |
| 41 | `CheckCompoundAffixes` (2292) | normalize.rs `check_compound_affixes` | MATCH | null-table → -1; in-place strncmp vs non-in-place strstr; len/issuffix update; `*ptr` advance on every iteration (incl. non-match). |
| 42 | `CopyVar` (2334) / `AddStem` (2359) / `SplitVar` (2283) | normalize.rs `SplitVar` | MATCH | linked list → Vec; CopyVar makedup 0/1 both clone (owned model never aliases, behaviour identical since C never mutates shared stems). |
| 43 | `SplitToVariants` (2372) | normalize.rs `split_to_variants` | MATCH | check_stack_depth; notprobed bitmap; compound-affix epenthesis loop; trie binary search; full-compound-word handling (last-word return vs bigger-word recurse+continue); head-at-index-0 + tail-append ordering reproduces the C `var->next` chain. |
| 44 | `addNorm` (2521) | normalize.rs `add_norm` | MATCH | `< MAX_NORM-1` cap; lexeme into caller context. |
| 45 | `NINormalizeWord` (2537) | normalize.rs `ni_normalize_word` | MATCH | base forms then (if usecompound) compound variants; nvariant counter; `< MAX_NORM` outer cap; compound last-stem normalize + per-sub combos. Result Vec empty → seam None (C NULL). |

(`#42` groups three tiny same-purpose helpers/struct onto one row; all three
audited.)

## 2. Constants verified against `tsearch/dicts/spell.h`

`FF_COMPOUNDONLY=0x01`, `FF_COMPOUNDBEGIN=0x02`, `FF_COMPOUNDMIDDLE=0x04`,
`FF_COMPOUNDLAST=0x08`, `FF_COMPOUNDFLAG=0x0e`, `FF_COMPOUNDFLAGMASK=0x0f`,
`FF_COMPOUNDPERMITFLAG=0x10`, `FF_COMPOUNDFORBIDFLAG=0x20`,
`FF_CROSSPRODUCT=0x40`, `FF_SUFFIX=1`, `FF_PREFIX=0`,
`FLAGNUM_MAXSIZE=(1<<16)`, `FM_CHAR/LONG/NUM` enum order. All match the port.
`MAX_NORM=1024`, `MAXNORMLEN=256` (spell.c #defines). `DEFAULT_COLLATION_OID=100`
(`pg_collation.h`).

## 3. Seam & wiring audit

**Owned seam crates (by c_source coverage):** spell.c → only
`backend-tsearch-spell-seams`. All 7 declarations
(`spell_start_build`, `spell_import_dictionary`, `spell_import_affixes`,
`spell_sort_dictionary`, `spell_sort_affixes`, `spell_finish_build`,
`spell_normalize_word`) are installed by `registry::init_seams()`, which is a
list of `set()` calls (the `spell_start_build` body is marshal+delegate:
`McxOwned::try_new` → `IspellDict::new` + `ni_start_build`, mint token). No
uninstalled decl, no `set()` outside the owner. `seams-init::init_all()` calls
`backend_tsearch_spell::init_seams()` (line 56). **No findings.**

**Outward seam calls** — each a thin marshal+delegate against a real unported
or cycle dependency:
- `backend-tsearch-ts-locale-seams`: `t_isalpha` (ctype) and `readfile`
  (= `tsearch_readline` loop: whole-file recoded read; per-line UTF-8
  recoding + open-error are owned by unported `ts_locale.c`). Thin.
- `backend-utils-mb-mbutils-seams`: `pg_mblen_range`, `pg_mb2wchar_with_len`.
- `backend-utils-adt-formatting-seams`: `str_tolower`.
- `backend-regex-core-seams`: `pg_regcomp`/`pg_regexec`/`pg_regfree`
  (REG_ADVANCED\|REG_NOSUB; nmatch=0). `AffixReg::Regex` Drop calls `pg_regfree`,
  mirroring C "freed when the dict context is destroyed".
- `backend-utils-misc-stack-depth-seams`: `check_stack_depth`.
- `backend-tsearch-ispell-regis` (direct dep, no seam): `rs_compile`,
  `rs_execute`, `rs_is_regis` — regis.c is in the same tsearch family and
  already merged.

No branching/node-construction/computation lives in any seam path. No function
body was replaced by a "call elsewhere"; all 45 rows live in this crate.

## 3b. Design conformance

- **Allocating funcs carry `Mcx` + return `PgResult`:** every build/normalize
  method threads `IspellDict.mcx` and returns `PgResult`; `new_bytes`/
  `reserve_one` map OOM to `mcx.oom`. OK.
- **Inherited opacity, not introduced:** `SpellHandle` (types_tsearch) and the
  `spell_*` seam surface were established by the already-merged/audited sibling
  `backend-tsearch-ispell-regis` (`DictISpell.obj: SpellHandle` mirrors C's
  embedded `IspellDict obj`, threaded as C threads `&d->obj`). No invented
  handle. OK (types.md 6-7).
- **Registry table:** `registry.rs` keeps the dictionaries as backend-local
  (`thread_local`) `McxOwned` bundles keyed by the token — this is the
  documented owned-bundle pattern for the dict-lives-in-dict_ispell /
  build-logic-lives-here split, not an ambient global or a `&'static mut`;
  reclaimed on drop. Not a side-table violation in the §3b sense.
- **No shared statics for per-backend globals, no ambient-global seams, no
  locks held across `?`, no unledgered divergence markers.** OK.

## 4. Verdict

All 45 functions MATCH; zero seam findings; constants verified against headers;
design rules satisfied. Build clean, tests green.

**PASS.**
