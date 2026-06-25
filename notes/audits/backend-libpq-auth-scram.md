# Audit: backend-libpq-auth-scram

C source: `src/backend/libpq/auth-scram.c` (1505 LOC) + the SCRAM arm of
`src/backend/libpq/auth-sasl.c:CheckSASLAuth`.
Port: `crates/backend-libpq-auth-scram` (lib.rs).
Seam crate: `crates/backend-libpq-auth-scram-seams`.

Audit method: re-derived from the C and the c2rust rendering
(`c2rust-runs/backend-libpq-auth-scram/src/auth_scram.rs`), independent of the
port's comments.

## Function inventory

| C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `scram_get_mechanisms` (206) | `scram_get_mechanisms` | MATCH | `!USE_SSL`: PLUS arm omitted; appends `SCRAM-SHA-256` + `\0`. The driver appends the trailing list-terminator `\0` (as C `CheckSASLAuth` does), so the helper emits exactly the one name+NUL. |
| `scram_init` (240) | `scram_init` | MATCH | mechanism parse (PLUS arm `!USE_SSL`-omitted → invalid-mech error preserved); shadow_pass present → get_password_type → SCRAM arm parse_scram_secret (LOG on parse fail) / non-SCRAM arm sets logdetail; absent → dummy. `!got_secret` → mock_scram_secret + doomed. errcodes/severity match. |
| `scram_exchange` (352) | `scram_exchange` + driver tail | MATCH | NULL input → empty challenge CONTINUE; empty/length-mismatch → PROTOCOL_VIOLATION; INIT→read-first/build-first/CONTINUE; SALT_SENT→read-final/verify_final_nonce (else error)/verify_client_proof||doomed→FAILURE else build-final/SUCCESS/FINISHED; default→`elog(ERROR,"invalid SCRAM exchange state")`. Key copy into MyProcPort + `*logdetail` relocated to the driver with the identical `SUCCESS && FINISHED` / `FAILURE && logdetail` guards (behavior-identical; only reachable once). |
| `pg_be_scram_build_secret` (483) | `pg_be_scram_build_secret` | MATCH | saslprep (fallback to raw), pg_strong_random salt (error on fail), scram_build_secret seam, elog on errstr. |
| `scram_verify_plain_password` (523) | `scram_verify_plain_password` | MATCH | parse (LOG+false on fail), b64-decode salt (`<0` → LOG+false), saslprep, SaltedPassword→ServerKey (elog on `<0`), `memcmp(...,key_length)==0`. |
| `parse_scram_secret` (600) | `parse_scram_secret` | MATCH | strsep on `$`,`:`,`$`,`:`; scheme check; strtol full-consume+overflow; salt b64-validate (`<0`→invalid) + pstrdup encoded; stored/server decode with `!= key_length`→invalid (negative-return also `!=`); invalid_secret clears `*salt`. |
| `mock_scram_secret` (697) | `mock_scram_secret` | MATCH | SHA256/key_len; scram_mock_salt (NULL→elog "could not encode salt"); b64 encode 16 bytes (`<0`→elog); iterations=default; stored/server zeroed. |
| `read_attr_value` (743) | `read_attr_value` | MATCH | attr/`=` checks with sanitize_char detail; scan to `,`/NUL; NUL-terminate + advance. Cursor-into-buffer model equivalent to C pointer mutation. |
| `is_scram_printable` (779) | `is_scram_printable` | MATCH | `<0x21 || >0x7E || ==0x2C` → false; stops at NUL. |
| `sanitize_char` (807) | `sanitize_char` | MATCH | `'c'` for 0x21..=0x7E else `0xHH`. |
| `sanitize_str` (827) | `sanitize_str` | MATCH | 30-char cap, `?` for non-printable, stop at NUL. |
| `read_any_attr` (855) | `read_any_attr` | MATCH | end-of-string / non-ALPHA errors; `=` check; scan to `,`/NUL; NUL-terminate + advance; writes `*attr_p`. |
| `read_client_first_message` (913) | `read_client_first_message` | MATCH | cbind_flag n/y/p/default arms (PLUS/SSL arms `!USE_SSL`-correct); authzid `a` forbidden; `,`; bare = pstrdup(p); `m` extension forbidden; username read (ignored, kept); nonce read + printable check; trailing read_any_attr loop. errcodes (PROTOCOL_VIOLATION / FEATURE_NOT_SUPPORTED) match. |
| `verify_final_nonce` (1127) | `verify_final_nonce` | MATCH | len(final)==len(client)+len(server); prefix==client; suffix==server. |
| `verify_client_proof` (1149) | `verify_client_proof` | MATCH | HMAC(StoredKey, bare","sfirst","cfinal-wo-proof) folded to one pg_hmac_sha256 (concat == multi-update); ClientKey = Proof XOR Sig; scram_H(ClientKey) compared to StoredKey. elog on crypto fail. |
| `build_server_first_message` (1202) | `build_server_first_message` | MATCH | strong-random 18-byte nonce (error on fail), b64 encode (`<0` error), `r=%s%s,s=%s,i=%d`, returns pstrdup. |
| `read_client_final_message` (1266) | `read_client_final_message` | MATCH | channel-binding read; `!USE_SSL` else-branch (biws/eSws vs cbind_flag) preserved, SSL arm is the C `#else elog`; nonce read; do/while extension loop tracking `proof = p-1`; b64 proof decode `!= key_length` → error; trailing-garbage check; without-proof = `input[..proof_offset]`. Offset arithmetic re-derived: `cur-1` == C `proof - begin`. |
| `build_server_final_message` (1412) | `build_server_final_message` | MATCH | HMAC(ServerKey, same AuthMessage); b64 encode (`<0`→elog); `v=%s`. |
| `scram_mock_salt` (1471) | `scram_mock_salt` | MATCH | GetMockAuthenticationNonce (seam); StaticAssert(DIGEST>=SALT) as const-assert; Assert(SHA256) as debug_assert; cryptohash create/init/update(user)/update(nonce,MOCK_AUTH_NONCE_LEN)/final → NULL on any `<0`; returns digest. |

19/19 functions present, all MATCH. No MISSING / PARTIAL / DIVERGES.

## Seam audit

Owned seam crate `backend-libpq-auth-scram-seams` declares the genuinely-absent
crypto/normalization/nonce leaves, all OUTWARD to unported owners:
- `scram_h`, `scram_salted_password`, `scram_server_key`, `scram_build_secret`
  → `common/scram-common.c` (unported)
- `pg_hmac_sha256` → `common/hmac.c` (unported)
- `pg_saslprep` → `common/saslprep.c` (unported)
- `get_mock_authentication_nonce` → `access/xlog.c` (unported)

These are legitimately uninstalled (mirror-pg-and-panic): their real owners are
unported, and the owner crate only `::call`s them, never `::set`s them. The
seams-init recurrence guard `every_declared_seam_is_installed_by_its_owner`
passes (owner status is `ported`, not yet `merged/audited`; once audited the
owner-calls discriminator exempts these outward seams).

The inward seam this unit OWNS is `check_scram_sasl_auth` (declared in
`backend-libpq-auth-seams`, the C `CheckSASLAuth(&pg_be_scram_mech, ...)` arm).
It is installed by this crate's `init_seams()`
(`backend_libpq_auth_seams::check_scram_sasl_auth::set(check_scram_sasl_auth_entry)`),
and `seams-init::init_all()` calls `backend_libpq_auth_scram::init_seams()`.
Guard `every_seam_installing_crate_is_wired_into_init_all` passes.

Reused (not re-declared): `pg_cryptohash_*` (common-cryptohash-seams),
`pg_strong_random` (port-pg-strong-random-seams), `get_password_type`
(backend-commands-user-seams). All have real owners or are pre-existing seams.

Seam-path bodies: the `check_scram_sasl_auth_entry` is thin (with_my_proc_port
+ delegate). The driver loop `check_scram_sasl_auth_impl` contains real logic
but it is the faithful port of `CheckSASLAuth` (auth-sasl.c), legitimately
in-crate (it lands with the mechanism owner per the auth.c catalog note), not a
seam path. No node construction / branching hidden inside a seam declaration.

## Design conformance

- No invented opacity: the opaque `pg_cryptohash_ctx` crosses as the raw
  pointer C holds (consumers never deref), via the pre-existing cryptohash seam.
- `hash_type` carried as the real `pg_cryptohash_type` enum, not a raw int.
- No allocating seam without PgResult where the C can `ereport` (crypto seams
  return `Result<_, String>` mirroring the C `int<0`+errstr convention, raised
  by the caller via elog).
- No shared statics for per-backend state: the GUC backing
  `SCRAM_SHA_256_ITERATIONS` mirrors the C file-scope `int` (a real GUC global,
  not per-backend connection state).
- No locks, no registry side-tables, no unledgered divergence markers.
- `!USE_SSL` build configuration is explicit and matches the C preprocessor
  `#else` paths exactly (PLUS never advertised; p/c arms error).

## Verdict: PASS

All 19 functions MATCH; the CheckSASLAuth SCRAM driver is a faithful in-crate
port; zero seam findings; design rules satisfied. cargo check --workspace,
no-todo-guard, and both seams-init recurrence guards are green; 17 unit tests
pass.
