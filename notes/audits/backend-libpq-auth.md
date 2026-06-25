# Audit: backend-libpq-auth

- **Unit:** `backend-libpq-auth` (C: `src/backend/libpq/auth.c` + the
  `CheckSASLAuth` driver of `src/backend/libpq/auth-sasl.c`, PostgreSQL 18.3)
- **Branch:** `port/backend-libpq-auth`
- **Date:** 2026-06-15
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent function-by-function audit re-derived from the C source and headers
(`libpq/protocol.h`, `libpq/auth.h`, `libpq/sasl.h`, `libpq/hba.h`, RFC 2865).

## 1. Function inventory

`auth.c` defines 26 function bodies; `auth-sasl.c` contributes `CheckSASLAuth`.
The build config here has GSS (`ENABLE_GSS`), SSPI (`ENABLE_SSPI`), PAM
(`USE_PAM`), BSD (`USE_BSD_AUTH`), LDAP (`USE_LDAP`) all **off**, so those
handlers are `Assert(false)` arms in the dispatch — their bodies reach external
libraries and are correctly seamed (they are not part of this build's logic).

| # | C function (loc) | Rust port | Verdict |
|---|---|---|---|
| 1 | `auth_failed` (auth.c:239) | `lib::auth_failed` | MATCH |
| 2 | `set_authn_id` (auth.c:341) | `lib::set_authn_id` | MATCH |
| 3 | `ClientAuthentication` (auth.c:379) | `lib::ClientAuthentication` + `reject_arm` + `hostname_lookup_detail` | MATCH |
| 4 | `sendAuthRequest` (auth.c:677) | `lib::sendAuthRequest` | MATCH |
| 5 | `recv_password_packet` (auth.c:707) | `lib::recv_password_packet` | MATCH |
| 6 | `CheckPasswordAuth` (auth.c:788) | `lib::CheckPasswordAuth` | MATCH (1 SEAMED callee: plain_crypt_verify) |
| 7 | `CheckPWChallengeAuth` (auth.c:823) | `lib::CheckPWChallengeAuth` | MATCH (SEAMED: get_role_password/get_password_type/SCRAM) |
| 8 | `CheckMD5Auth` (auth.c:883) | `lib::CheckMD5Auth` | MATCH (SEAMED: md5_crypt_verify) |
| 9 | `interpret_ident_response` (auth.c:1590) | `ident::interpret_ident_response` | MATCH |
| 10 | `ident_inet` (auth.c:1671) | `ident::ident_inet` | MATCH |
| 11 | `auth_peer` (auth.c:1856) | `peer::auth_peer` | MATCH (SEAMED: check_usermap) |
| 12 | `CheckCertAuth` (auth.c:2687) | `lib::CheckCertAuth` | MATCH (SEAMED: check_usermap) |
| 13 | `radius_add_attribute` (auth.c:2819) | `radius::radius_add_attribute` | MATCH |
| 14 | `CheckRADIUSAuth` (auth.c:2845) | `radius::CheckRADIUSAuth` | MATCH |
| 15 | `PerformRadiusTransaction` (auth.c:2940) | `radius::PerformRadiusTransaction` | MATCH (SEAMED: pg_md5_binary) |
| 16 | `CheckSASLAuth` (auth-sasl.c:44) | seamed per-mech (check_scram_sasl_auth / check_oauth_sasl_auth) | SEAMED |
| — | `pg_GSS_recvauth`/`pg_GSS_checkauth` (ENABLE_GSS) | `seams::check_gss_auth` | SEAMED (external lib, off in build) |
| — | `pg_SSPI_recvauth`/`pg_SSPI_make_upn` (ENABLE_SSPI) | `seams::check_sspi_auth` | SEAMED (external lib) |
| — | `CheckPAMAuth`/`pam_passwd_conv_proc` (USE_PAM) | `seams::check_pam_auth` | SEAMED (external lib) |
| — | `CheckBSDAuth` (USE_BSD_AUTH) | `seams::check_bsd_auth` | SEAMED (external lib) |
| — | `InitializeLDAPConnection`/`dummy_ldap_password_mutator`/`FormatSearchFilter`/`CheckLDAPAuth` (USE_LDAP) | `seams::check_ldap_auth` | SEAMED (external lib) |

### `CheckSASLAuth` note

`CheckSASLAuth(mech, port, shadow_pass, logdetail)` is a loop driven entirely by
the mechanism vtable (`get_mechanisms`/`init`/`exchange`). The only two
mechanisms — SCRAM (`auth-scram.c`) and OAuth (`auth-oauth.c`) — are unported,
and the loop is inseparable from per-mechanism opaque state. Faithful porting
of the loop must land with the mechanism owner; here each mechanism's whole
`CheckSASLAuth(&mech, …)` crosses one seam, which is the correct boundary. This
is the only function whose body does not live in-crate, and it is justified: its
body *is* the mechanism dispatch over unported owners, not absent logic.

## 2. Key correctness spot-checks (re-derived)

- **Protocol constants** (verified against `libpq/protocol.h`):
  `AUTH_REQ_OK=0, _PASSWORD=3, _MD5=5, _GSS=7, _SSPI=9, _SASL=10, _SASL_CONT=11,
  _SASL_FIN=12`; `PqMsg_AuthenticationRequest='R'`, `PqMsg_PasswordMessage='p'`.
  `PG_MAX_AUTH_TOKEN_LENGTH=65535` (`libpq/auth.h`). All MATCH.
- **`auth_failed`**: EOF short-circuit `proc_exit(0)` fires before any hba read;
  the message/SQLSTATE table (pass/md5/scram → `28P01`, else `28000`) and the
  `cdetail` composition match line-for-line, including the `default` arm.
- **`recv_password_packet`**: `'p'` type check (EOF silent, other type →
  `08P01`), `strlen+1 != len` → `08P01`, `len==1` → empty-password `28P01`,
  `DEBUG5` log. MATCH.
- **`sendAuthRequest`**: two `CHECK_FOR_INTERRUPTS`, the `extralen>0` guard, and
  the flush-except-`OK`/`SASL_FIN` rule. MATCH.
- **`CheckPWChallengeAuth`**: md5-vs-scram selection
  (`auth_method==uaMD5 && pwtype==PASSWORD_TYPE_MD5` → MD5, else SCRAM);
  `pwtype` from stored secret or `Password_encryption`. MATCH.
- **RADIUS password encryption** (RFC 2865 §5.2): `e[0]=p[0] XOR
  MD5(secret+RequestAuth)`, `e[i]=p[i] XOR MD5(secret+e[i-1])`. The C sets
  `md5trailer=encryptedpassword+i` *before* the XOR so the next iteration reads
  the XORed ciphertext `e[i-1]`; the port assigns `md5trailer = ciphertext
  block` after the XOR — equivalent. `service = pg_hton32(8)` =
  `to_be_bytes()`. MATCH.
- **RADIUS response MAC**: `MD5(code+id+length + RequestAuth + attrs + secret)`
  compared against `receivepacket->vector`; the response-port filter, length
  consistency, id match, and `ACCEPT/REJECT`/other code handling all MATCH.
  Absolute `gettimeofday`-based deadline with `select` and `EINTR` retry. MATCH.
- **`ident_inet`**: getaddrinfo(remote,113)/getaddrinfo(local) →
  socket/bind/connect/send/recv with `EINTR` retry; `interpret_ident_response`
  state machine (port skip, `USERID` check, OS-field skip, username up to
  `\r`/512). Socket closed via RAII guard (the C `ident_inet_done` label).
  MATCH.
- **`auth_peer`**: `getpeereid` (`ENOSYS` → feature-not-supported `0A000`),
  `getpwuid_r`, `set_authn_id(pw_name)`, `check_usermap`. MATCH.
- **`CheckCertAuth`**: DN/CN selection, empty → error, `uaCert` requires
  `peer_dn` and calls `set_authn_id`, then `check_usermap` with the
  verify-full DN/CN mismatch log. MATCH.

## 3. Seam audit

Owned inward seam crate: `backend-libpq-auth-seams`. The 4 seams `auth.c` owns
and `postinit` consumes — `client_authentication`, `authentication_timeout`,
`log_connection_authorization`, `client_authn_id` — are all installed by
`backend_libpq_auth::init_seams()` (4 `set()` calls, nothing else), and
`init_seams()` is wired into `seams-init::init_all()`. The four signatures are
unchanged from the pre-existing declarations that `postinit` (merged) already
consumes — no contract divergence.

The remaining 17 declarations in `backend-libpq-auth-seams` are dependencies
`auth.c` *consumes* whose owners are unported (`hba.c`, `crypt.c`,
`auth-scram.c`/`auth-oauth.c`, `common/md5.c`, `be-secure.c`, GUC, the five
external-lib methods). They are placed in this consumer's seam crate (the
nodeLockRows convention for cross-unit consumed seams) and left unset — a call
panics loudly until the owner lands, which is correct and matches the repo's
established pattern. Each is a thin marshal+delegate at the call site; no logic
hides behind a seam. `plain_crypt_verify`/`get_password_type` reuse the existing
`backend-commands-user-seams` declarations rather than redeclaring.

Direct (non-seam) deps used because they do not cycle: `backend-libpq-pqcomm`
(`pq_startmsgread`/`pq_getbyte`/`pq_getmessage`/`pq_flush`),
`backend-libpq-pqformat` (message building), `common-ip`
(`pg_getaddrinfo_all`/`pg_getnameinfo_all`), `backend-utils-init-miscinit`
(`MyClientConnectionInfo`), `libc` (sockets).

## 4. Design conformance

- No invented opacity: works on the real `types_net::Port`; no handle/token
  stand-ins introduced (the sole `i32` status is the C `STATUS_*` contract).
- Allocating helpers (`sendAuthRequest`, `recv_password_packet`) build their
  `StringInfo` in a short-lived `MemoryContext` (`MemCtx`) and return
  `PgResult` — the idiomatic stand-in for C's implicit `CurrentMemoryContext`.
- No shared statics; `MyClientConnectionInfo` is read/written through the
  miscinit thread-local accessor it already owns.
- `expect()`/`panic` sites are all `port->hba`/`MyProcPort` non-NULL invariants
  the C also assumes (the postmaster guarantees them before authentication),
  not error paths — error paths return `Err(PgError)`/`Ok(STATUS_*)`.
- Sockets are closed via `Drop` guards (the C `goto …_done` cleanup), satisfying
  the held-resource rule.
- No `todo!`/`unimplemented!`/own-logic stub.

## Verdict

Every in-build function is **MATCH**; `CheckSASLAuth` is **SEAMED** per the
step-3 mechanism-dispatch rule; the `#ifdef`-off external-lib methods are
**SEAMED**. Zero seam findings, zero design-conformance findings.

**PASS.**
