# Audit: backend-libpq-hba

Independent function-by-function audit of `crates/backend-libpq-hba` against
`src/backend/libpq/hba.c` (3126 LOC) and `src/backend/utils/adt/hbafuncs.c`
(610 LOC), re-derived from the PostgreSQL 18.3 C sources and the c2rust
renderings (`c2rust-runs/backend-libpq-hba/src/hba.rs`,
`c2rust-runs/backend-utils-adt-misc2/src/hbafuncs.rs`).

Verdict: **PASS**. Every function MATCH or correctly SEAMED. Zero seam findings,
zero design-conformance findings. No fixes were required.

## Model reconciliation (verified correct)

* **SockAddr**: real `[u8; 128]` `sockaddr_storage` buffer + `salen`. `ss_family`
  reads the first field of the buffer (AF_UNSPEC when `salen == 0`). The
  `addr`/`mask` IP comparisons convert sockaddr bytes ↔ `IpAddr` via
  `sockaddr_to_ipaddr` (reads `sin_addr`/`sin6_addr`, big-endian-correct via
  `u32::from_be` / `s6_addr` octets) and `ipaddr_to_sockaddr` (writes family +
  `salen = sizeof(sockaddr_in[6])`). The C `ipv4eq`/`ipv6eq` family+byte compares
  reduce to `IpAddr == IpAddr` (which already discriminates V4/V6 → implicit
  family match). `check_ip` enforces `ss_family(raddr) == ss_family(addr)` before
  delegating to `pg_range_sockaddr(IpAddr,IpAddr,IpAddr)`. `memcpy(&addr, ai_addr,
  ai_addrlen)` ↔ `addr = gai[0].addr.addr; addrlen = gai[0].addr.salen`
  (`copy_addrinfo` sets `salen = ai_addrlen` + memcpy of the bytes — verified).
  `pg_sockaddr_cidr_mask` result is stored back via `ipaddr_to_sockaddr` →
  `parsedline.mask`. All byte/family conversions are correct.
* **AuthToken**: `types_net::AuthToken { string: Option<String>, quoted, regex:
  Option<RegexCompiled> }`. Hand-impl `PartialEq`/`Eq` compares only
  `string`/`quoted` — sound because `copy_auth_token` drops the regex in C and
  the regex `Rc` is not comparable; equality is never load-bearing on the regex.
* **File model**: `AllocateFile` + `pg_get_line_append` loop → whole-file read
  into `Vec<u8>` (`open_auth_file` via `fd::allocate_file_read`, `Ok(None)` ==
  ENOENT), lines iterated by `split_lines` + `strip_crlf`. Behavior-equivalent.
* **Per-backend globals**: `parsed_hba_lines`/`parsed_ident_lines` as
  `thread_local! RefCell<Vec<…>>` (sanctioned per-backend-global pattern).
  `check_hba`/`check_usermap` snapshot-clone out of the RefCell before the matcher
  calls (which re-enter other state) — no borrow held across `?`.
* **#ifdef arms**: USE_SSL/ENABLE_GSS/ENABLE_SSPI/USE_PAM/USE_BSD_AUTH/USE_LDAP/
  HAVE_LDAP_INITIALIZE/LDAP_API_FEATURE_X_OPENLDAP are `false` `const fn`s →
  the dead OpenLDAP `ldapurl` parse collapses to the not-supported branch,
  faithful to a no-optional-features build. `ldapscope = LDAP_SCOPE_SUBTREE` is
  under `#ifdef USE_LDAP` in C, so (correctly) not set in the port.

## hba.c per-function table

| C fn (location) | Port (file) | Verdict | Notes |
|---|---|---|---|
| `pg_isblank` (145) | token.rs:39 | MATCH | `' ' \| '\t' \| '\r'`. |
| `next_token` (186) | token.rs:50 | MATCH | whitespace/comma skip loop, comment-to-EOL, terminating-comma, dequote `""`→`"`, `initial_quote` on empty-buf quote, `saw_quote`/`in_quote`/`was_quote` state, un-eat trailing char (`*pos -= 1`), return `saw_quote \|\| !buf.empty()`. `getc!` models `*(*lineptr)++` with NUL past end. |
| `make_auth_token` (258) | token.rs:137 | MATCH | string copy + quoted; `regex = None`. |
| `free_auth_token` (279) | token.rs:147 | MATCH | `pg_regfree` via regex seam when present. |
| `copy_auth_token` (289) | token.rs:158 | MATCH | `make_auth_token(string, quoted)`; drops regex (as C). |
| `regcomp_auth_token` (302) | token.rs:168 | MATCH | `string[0] != '/'` → return 0; `pg_mb2wchar_with_len(string+1)` + `pg_regcomp(REG_ADVANCED, C_COLLATION_OID=950)`; on fail → ereport INVALID_REGULAR_EXPRESSION + errcontext + set err_msg + return nonzero. Assert(regex==NULL) as debug_assert. |
| `regexec_auth_token` (347) | token.rs:221 | MATCH | Assert(string[0]=='/' && regex); `pg_mb2wchar_with_len(match)` + `pg_regexec(nmatch,pmatch)`; returns (REG_OKAY/REG_NOMATCH/1, matches, errstr). |
| `next_field_expand` (380) | tokenize.rs:41 | MATCH | do/while(trailing_comma && err_msg==NULL); `@`-file expand on `!initial_quote && len>1 && data[0]=='@'`; else append token. |
| `tokenize_include_file` (439) | tokenize.rs:86 | MATCH | AbsoluteConfigLocation → open; ENOENT+missing_ok → "skipping missing" log + err_msg=NULL + return; else tokenize+free. `last_errno` seam used for the errno==ENOENT test. |
| `tokenize_expand_file` (494) | tokenize.rs:134 | MATCH | local `inc_lines`, propagate first line err_msg (break), flatten all fields' tokens into `tokens`. `_tok_lines` unused (faithful — C does not append to outer list here). |
| `free_auth_file` (571) | token.rs:249 | MATCH | FreeFile == drop the owned buffer; tokenize_context drop is N/A (owned Vecs). |
| `open_auth_file` (596) | token.rs:258 | MATCH | depth>CONF_FILE_MAX_DEPTH(=10) → file-access ereport + err_msg + None; AllocateFile fail (ENOENT) → file-access ereport(`%m`) + err_msg + None; tokenize_context create is N/A. |
| `tokenize_error_callback` (661) | (folded) | MATCH | errcontext attached directly to each built error via `line_context`; no error_context_stack. |
| `tokenize_auth_file` (690) | tokenize.rs:185 | MATCH | per-line backslash-continuation (`buf.len > last_backslash_buflen && last=='\\'`), continuations count, `line_number += continuations + 1`; field loop `while *lineptr && err_msg==NULL`; include-directive detection on `list_length==2` reading `[0][0]`/`[1][0]`; include / include_dir (err_buf accumulate + `\n` join) / include_if_exists; goto next_line/process_line via `goto_next_line` flag; raw_line = de-continuated buf. I/O-error path absent (whole file pre-read) — faithful to the read model. |
| `is_member` (924) | matchers.rs:106 | MATCH | `!OidIsValid(userid)`→false; `get_role_oid(role,true)`; `!OidIsValid(roleid)`→false; `is_member_of_role_nosuper`. |
| `check_role` (953) | matchers.rs:127 | MATCH | member-check `+`/keyword `all`/regexp(REG_OKAY)/case-insensitive/exact, in order. |
| `check_db` (992) | matchers.rs:158 | MATCH | walsender&&!db_walsender→only `replication` matches (else no-op); `all`/`sameuser`(dbname==role)/`samegroup`\|`samerole`(is_member)/`replication`(continue)/regexp/exact. Globals read once before loop (invariant in loop). |
| `ipv4eq` (1036) | (folded into IpAddr eq) | MATCH | |
| `ipv6eq` (1042) | (folded into IpAddr eq) | MATCH | |
| `hostname_match` (1057) | matchers.rs:200 | MATCH | `.`-prefix suffix match (hlen<plen→false; compare tail), else full; `pg_strcasecmp`. |
| `check_hostname` (1077) | matchers.rs:217 | MATCH | resolv<0 quick-out; reverse lookup (NI_NAMEREQD)→cache or -2/errcode; `hostname_match`; resolv==+1 short-circuit; forward `getaddrinfo`→compare each `IpAddr` to client; DEBUG2 reject log; resolv = +1/-1. |
| `check_ip` (1168) | matchers.rs:303 | MATCH | family equality then `pg_range_sockaddr`. |
| `check_network_callback` (1182) | matchers.rs:321 | MATCH | already-found short-circuit; ipCmpSameHost → all-ones cidr mask (NULL bits); else interface netmask; cidr-mask failure leaves result unchanged (C ignores the return). |
| `check_same_host_or_net` (1209) | matchers.rs:355 | MATCH | `pg_foreach_ifaddr(callback)`; on <0 → LOG "error enumerating network interfaces" + false. |
| `INVALID_AUTH_OPTION` (1244) | parse_hba.rs:905 | MATCH | macro: ereport CONFIG_FILE_ERROR + errcontext + err_msg + return false. |
| `REQUIRE_AUTH_OPTION` (1258) | parse_hba.rs:927 | MATCH | guards method then INVALID_AUTH_OPTION. |
| `MANDATORY_AUTH_ARG` (1264) | parse_hba.rs (inlined) | MATCH | inlined per call site (ldapserver/radiusservers/radiussecrets/scope/issuer) with exact message + return NULL. |
| `IDENT_FIELD_ABSENT` (1288) | parse_ident.rs:244 | MATCH | "missing entry at end of line". |
| `IDENT_MULTI_VALUE` (1301) | parse_ident.rs:263 | MATCH | "multiple values in ident field". |
| `parse_hba_line` (1327) | parse_hba.rs:48 | MATCH | conntype dispatch (`string[4]` host-variant `s/g/n+s/n+g`/else), SSL/GSS not-supported reports; db/role field copy+regcomp; IP field (`all`/`samehost`/`samenet`/IP+netmask), CIDR-slash isolation, `pg_getaddrinfo_all(AI_NUMERICHOST,AF_UNSPEC)`, EAI_NONAME→hostname, error report; cidr→`pg_sockaddr_cidr_mask` + masklen=addrlen + both-hostname-and-CIDR error; else separate mask field + family-match check; auth method dispatch + unsupauth(build-off) reports; ident→peer on local; gss-on-local / peer-not-local / cert-not-hostssl rejects; GSS/SSPI include_realm default; SSPI compat_realm/upn_username defaults; remaining name=value args; LDAP/RADIUS/CERT/OAUTH mandatory-arg + count checks; OAuth `check_oauth_validator` seam + map/delegate conflict. All `field` advancement via index, EOL checks `field >= len`. |
| `parse_hba_auth_opt` (2086) | parse_hba.rs:894 | MATCH | all ~35 option arms: map (method set), clientcert (hostssl-only, verify-full/verify-ca/cert-restriction), clientname (CN/DN), pamservice, pam_use_hostname, ldapurl (not-supported feature error), ldaptls, ldapscheme (validate), ldapserver/port(atoi,==0 error)/binddn/bindpasswd/searchattribute/searchfilter/basedn/prefix/suffix, krb_realm, include_realm, compat_realm, upn_username, radiusservers (SplitGUCList + per-entry getaddrinfo SOCK_DGRAM), radiusports (atoi==0), radiussecrets, radiusidentifiers, issuer, scope, validator, delegate_ident_mapping, unrecognized. `atoi` C-leading-int semantics; `split_guc_list` via varlena seam (None == C false). LDAP_SCOPE_SUBTREE default omitted (USE_LDAP off). |
| `check_hba` (2530) | loaders.rs:40 | MATCH | get_role_oid; per-line conntype/AF_UNIX gating, SSL state (hostnossl/hostssl skip), GSS state (ENABLE_GSS off → ctHostGSS always skipped), IP switch (ipCmpMask hostname/ip, ipCmpAll, samehost/samenet, default continue), check_db, check_role(false); on miss → implicit-reject HbaLine (uaImplicitReject). |
| `load_hba` (2644) | loaders.rs:133 | MATCH | open(LOG)→false; tokenize; parse each (skip err lines, set ok=false on error, keep going); empty→"contains no entries" LOG + ok=false; free; replace parsed_hba_lines only on ok. |
| `parse_ident_line` (2750) | parse_ident.rs:22 | MATCH | map token (multi-value), system_user (absent/multi), pg_user (absent/multi), copy tokens, regcomp both. |
| `check_ident_usermap` (2818) | parse_ident.rs:114 | MATCH | usermap strcmp gate; get_role_oid; regexp path: regexec(nmatch=2), REG_NOMATCH no-error vs error LOG, `\1` substitution (only if pg_user not member-check/regexp and `strstr("\\1")`), `matches[1].rm_so<0` "no subexpressions" error, expand `pg[..ofs]+sys[so..eo]+pg[ofs+2..]` as quoted token, check_role + free temp token; non-regex path: case-insensitive/exact system_user match then check_role. |
| `check_usermap` (2965) | loaders.rs:239 | MATCH | NULL/empty map → pg==system (ci or exact) → STATUS_OK, else "do not match" LOG + STATUS_ERROR; else iterate ident lines, break on found/error; no-match LOG; return OK/ERROR. |
| `load_ident` (3020) | loaders.rs:187 | MATCH | mirror of load_hba (no empty-file check, per C). |
| `hba_getauthmethod` (3109) | loaders.rs:228 | MATCH | `check_hba(port)`. |
| `hba_authname` (3122) | loaders.rs:233 | MATCH | `USER_AUTH_NAME[method]`. |

## hbafuncs.c per-function table

| C fn (location) | Port | Verdict | Notes |
|---|---|---|---|
| `get_hba_options` (51) | views.rs:55 | MATCH | exact option order + strings: GSS/SSPI(include_realm=true, krb_realm), map, clientcert(verify-ca/verify-full when != clientCertOff), pamservice, LDAP block (server/port!=0/scheme/tls/prefix/suffix/basedn/binddn/bindpasswd/searchattribute/searchfilter/scope!=0), RADIUS(*_s), OAUTH(issuer/scope/validator/delegate_ident_mapping=true). MAX_HBA_OPTIONS=15 (Vec-bounded; assert is N/A). Empty → no array. |
| `fill_hba_line` (201) | views.rs:203 | MATCH | NUM_PG_HBA_FILE_RULES_ATTS=11; rule_number(null on err)/file_name/line_number; type switch (6 conntypes, no default → null); database/user `strlist_to_textarray` of token strings (null if empty); address+netmask switch (ipCmpMask hostname/numeric_host(NI_NUMERICHOST + clean_ipv6_addr) guarded by addrlen/masklen>0; all/samehost/samenet); auth_method; options array (null if empty); no-parse branch `memset(&nulls[3], true, ATTS-4)` == `nulls[3..ATTS-1]=true`; err column last (or null). |
| `fill_hba_view` (392) | views.rs:339 | MATCH | open(ERROR)+tokenize(DEBUG3); per line parse(DEBUG3) only if no err; rule_number++ only if no err; fill_hba_line. mcx from rsinfo->setDesc allocator. |
| `pg_hba_file_rules` (448) | misc2 admin.rs:483 (SEAMED) | MATCH | misc2-owned SRF wrapper (InitMaterializedSRF + `fill_hba_view` seam). Correct ownership split. |
| `fill_ident_line` (486) | views.rs:384 | MATCH | NUM_PG_IDENT_FILE_MAPPINGS_ATTS=7; map_number/file_name/line_number; usermap/system_user.string/pg_user.string; no-parse `nulls[3..ATTS-1]=true`; err last. |
| `fill_ident_view` (539) | views.rs:443 | MATCH | mirror of fill_hba_view. |
| `pg_ident_file_mappings` (592) | misc2 admin.rs:497 (SEAMED) | MATCH | misc2-owned SRF wrapper. |

## Constants (verified against headers)

C_COLLATION_OID=950 ✓, REG_ADVANCED=0o3 ✓, REG_OKAY=0 ✓, REG_NOMATCH=1 ✓,
CONF_FILE_START_DEPTH=0 ✓, CONF_FILE_MAX_DEPTH=10 ✓, STATUS_OK=0 ✓,
STATUS_ERROR=-1 ✓, ENOENT=2 ✓, EAI_NONAME=`libc::EAI_NONAME` ✓,
NUM_PG_HBA_FILE_RULES_ATTS=11 ✓, NUM_PG_IDENT_FILE_MAPPINGS_ATTS=7 ✓,
MAX_HBA_OPTIONS=15 ✓ (Vec-bounded), `USER_AUTH_NAME[16]` order/values match
`UserAuthName[]` exactly (reject, implicit reject, trust, ident, password, md5,
scram-sha-256, gss, sspi, pam, bsd, ldap, cert, radius, peer, oauth) ✓,
LDAP_SCOPE_SUBTREE arm correctly omitted (USE_LDAP off).

## Seam audit

Owned seam crates (by C-source coverage: `libpq/hba.c`):
* `backend-libpq-auth-seams` — this crate owns and installs exactly the 3
  hba.c-sourced entries: `hba_getauthmethod`, `check_usermap`, `hba_authname_of`
  (all `set()` in `init_seams`). The remaining auth-seams entries are auth.c/
  crypt.c/SASL/external-lib owned and are NOT touched here — correct.
* `backend-libpq-hba-seams` — `fill_hba_view`, `fill_ident_view`,
  `hba_authname`: all 3 installed in `init_seams`.

`seams-init::init_all()` calls `backend_libpq_hba::init_seams()` (line 194) ✓.
All 6 owned seams installed; nothing uninstalled, no `set()` outside the owner.

Outward seams (each a thin marshal+delegate to a genuine unported owner, no
logic in the seam path):
* `backend-libpq-oauth-seams::check_oauth_validator` — declared by this port,
  owned by unported `auth-oauth.c`, consumed in parse_hba.rs; correctly NOT
  installed here (panics until the OAuth owner lands; only reachable for a
  configured `oauth` line). ✓
* acl `get_role_oid`/`is_member_of_role_nosuper`, walsender `am_*`, guc
  `live::get_string`, regex `pg_regcomp`/`pg_regexec`/`pg_regfree`, mbutils
  `pg_mb2wchar_with_len`, common_ip getaddr/getnameinfo, ifaddr
  `pg_range_sockaddr`/`pg_sockaddr_cidr_mask`/`pg_foreach_ifaddr`, varlena
  `split_guc_list`, arrayfuncs `construct_text_array`, network `clean_ipv6_addr`,
  funcapi `materialized_srf_putvalues`, miscinit `client_connection_info`,
  init-small `with_my_proc_port`, conffiles + fd file primitives — all thin
  marshal+delegate, real dependency edges.

## Design conformance

* No invented opacity — all carriers are real `types_net`/owned types; AuthToken
  regex is a real `RegexCompiled`, not a token/handle.
* Allocating/fallible functions take `Mcx` (view fills, `hba_authname_of`,
  `regcomp`/`regexec`) and return `PgResult`; ereports route through the real
  builder + `.finish()` (ERROR+ → Err, below → log-and-continue) — matches C
  `ereport` level semantics.
* Per-backend globals use the sanctioned `thread_local! RefCell` pattern; borrows
  are snapshot-cloned before matcher calls, never held across `?`.
* No ambient-global seams, no registry side-tables, no unledgered divergence
  markers. `debug_assert!`/`expect`/`unreachable!` mirror C `Assert`/dead #ifdef
  arms (acceptable).

## Verdict: PASS

All 41 hba.c functions + 7 hbafuncs.c functions verified MATCH (or correctly
SEAMED for the 2 misc2-owned SRF wrappers). Build green
(`cargo check -p backend-libpq-hba`). No fixes required. CATALOG row set to
`audited`.
