#!/usr/bin/env python3
"""p1-pgcryptofam residual-line classifier (lane p1-pgcryptofam-resid).

Closes the coverage equation for crates/contrib/pgcrypto at
f8231ca7535106f5e15d1e439f8b1c433d6b64fc against the lcov of record
(fleet floor pgrust-fuzz-campaign-1785664060-1cd9-61609, 10,209,267 execs).

Recomputes the residual set (v2 SLOC, exclude_const_tables=True, test-scope
filtered) from the lcov, asserts it matches the number of record
(IN-SCOPE 1401 = 1153 measured + 248 residual; CARVED 1635; total 3036),
classifies every residual line, and emits:
  residual-classification.tsv   per-line class map (evidence)
  ledger-rows.tsv               rows to append to phase1-exceptions.tsv
The classification map is hand-authored from reading every line (see
rendered-red.txt); this script guarantees the arithmetic can't drift.
"""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "proofs", "coverage"))
import sloc_rules, test_scope  # noqa: E402

LCOV = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "pgcf.lcov")
CRATE = "crates/contrib/pgcrypto"
AUTHOR = "p1-pgcryptofam-resid"

IN_SCOPE = ["src/crypt.rs", "src/crypt/bcrypt.rs", "src/crypt/cryptdes.rs",
            "src/crypt/desc.rs", "src/crypt/shacrypt.rs", "src/hashing.rs",
            "src/lib.rs", "src/pgp/armor.rs"]
CARVED = {
    "src/cipher.rs": ("contrib/pgcrypto/openssl.c (px cipher providers)",
                      "raw px block-cipher registry"),
    "src/pgp/cfb.rs": ("contrib/pgcrypto/pgp-cfb.c", "PGP CFB mode"),
    "src/pgp/compress.rs": ("contrib/pgcrypto/pgp-compress.c", "PGP compression"),
    "src/pgp/consts.rs": ("contrib/pgcrypto/pgp.c + pgp.h (algo tables)", "PGP algo tables"),
    "src/pgp/context.rs": ("contrib/pgcrypto/pgp.c (pgp_init/ctx setters)", "PGP context"),
    "src/pgp/decrypt.rs": ("contrib/pgcrypto/pgp-decrypt.c", "PGP decrypt"),
    "src/pgp/encrypt.rs": ("contrib/pgcrypto/pgp-encrypt.c", "PGP encrypt"),
    "src/pgp/keyid.rs": ("contrib/pgcrypto/pgp-info.c", "PGP key-id"),
    "src/pgp/mod.rs": ("contrib/pgcrypto/pgp.c", "PGP family root"),
    "src/pgp/mpi.rs": ("contrib/pgcrypto/pgp-mpi.c + pgp-mpi-internal.c", "PGP MPI"),
    "src/pgp/packet.rs": ("contrib/pgcrypto/pgp-decrypt.c (packet stream)", "PGP packet stream"),
    "src/pgp/pubdec.rs": ("contrib/pgcrypto/pgp-pubdec.c", "PGP public-key decrypt"),
    "src/pgp/pubenc.rs": ("contrib/pgcrypto/pgp-pubenc.c", "PGP public-key encrypt"),
    "src/pgp/pubkey.rs": ("contrib/pgcrypto/pgp-pubkey.c", "PGP pubkey parse"),
    "src/pgp/s2k.rs": ("contrib/pgcrypto/pgp-s2k.c", "PGP string-to-key"),
}
CARVE_WHY = ("census-OUT carve (carved family): {what} — needs an "
             "OpenSSL-provider-dependent oracle (upstream builds pgcrypto "
             "against OpenSSL EVP; ed74dda8eb0 dropped the bf/cast5/des e2e "
             "suites because PGDG's OpenSSL-3 legacy provider is off); whole "
             "family out of lane p1-pgcryptofam's scope per its claim")

V = "fuzz/core/csrc/pgcryptofam/vendor"  # vendored C tree of record

# (file, lines, class, c_counterpart, justification) — lines: int | (lo,hi) | list
C = []


def row(f, lines, cls, cc, just):
    C.append((f, lines, cls, cc, just))


# ---------------------------------------------------------------- crypt.rs
row("src/crypt.rs", 76, "instrument-unmappable",
    f"{V}/crypt-gensalt.c:25-40 _crypt_gensalt_traditional_rn",
    "`Some(` head line of the traditional-salt success expression: rustc 1.96 "
    "emits NO DA record for it (noDA in the lcov of record) while the "
    "expression body lines 77-81 carry DA=9 — measured false-red; the "
    "success path IS fuzz-executed")
row("src/crypt.rs", 92, "defensive-c-parity", f"{V}/crypt-gensalt.c:58-59",
    "xdes count==0 default (725): dead through px_gen_salt — the gen_list "
    "xdes row has def_rounds=PX_XDES_ROUNDS=725, so rounds==0 is substituted "
    "before the generator runs (crypt.rs:197-199 = px-crypt.c:163-171); C's "
    "identical `if (!count) count = 725` is equally unreachable via px_gen_salt")
row("src/crypt.rs", 120, "defensive-c-parity", f"{V}/crypt-gensalt.c:164-170",
    "bf generator entry guard: input is always the 16-byte rbuf px_gen_salt "
    "sizes from gen_list input_len, and rounds are range-checked into [4,31] "
    "(def 6 on 0) before the call (crypt.rs:197-203) — C's identical guard "
    "in _crypt_gensalt_blowfish_rn is equally dead via px_gen_salt")
row("src/crypt.rs", 123, "defensive-c-parity", f"{V}/crypt-gensalt.c:172-173",
    "bf count==0 default (5): px_gen_salt substitutes def_rounds=6 for "
    "rounds==0 before calling, so count is in [4,31] here — C identical")
row("src/crypt.rs", 138, "defensive-c-parity", f"{V}/crypt-gensalt.c:210",
    "sha salt-size guard: input is always the PX_SHACRYPT_SALT_MAX_LEN(16)-"
    "byte rbuf sized from gen_list input_len — C's `PX_SHACRYPT_SALT_MAX_LEN "
    "!= size` in _crypt_gensalt_sha is equally dead via px_gen_salt")
row("src/crypt.rs", 167, "const-eval-only", f"{V}/px-crypt.c:137-153 gen_list",
    "multi-line `static GEN_LIST` head (interior rows are the excluded const "
    "table): static initializer is const-evaluated; no DA record exists")
row("src/crypt.rs", 208, "defensive-c-parity", f"{V}/px-crypt.c:180-181",
    "PXE_NO_RANDOM arm: pg_strong_random fails only on OS entropy "
    "exhaustion, which pgrust's OS entropy never takes (driver banner, "
    "ENTROPY CARVE — the C side is padded to >=32 bytes so ITS arm can "
    "never fire one-sided either); same dead arm in C px_gen_salt")
row("src/crypt.rs", 242, "const-eval-only", f"{V}/px-crypt.c:88-99 px_crypt_list",
    "multi-line `static PX_CRYPT_LIST` head — same class as the GEN_LIST head")

# ------------------------------------------------------------- bcrypt.rs
row("src/crypt/bcrypt.rs", 40, "defensive-c-parity", f"{V}/crypt-blowfish.c:393-394",
    "BF_decode post-2nd-byte `out.len() >= count` break: bf_decode's only "
    "caller is crypt_bf's salt decode with count=16 (bcrypt.rs:160), and "
    "16 % 3 == 1 means the loop always exits at the post-1st-byte break "
    "(line 33-34, DA=1584); C's BF_decode is likewise only called with 16 "
    "and its twin `if (dptr >= end) break` is equally dead")

# ----------------------------------------------------------- cryptdes.rs
row("src/crypt/cryptdes.rs", [22, 23, 25, 31, 39, 44, 87, 92], "const-eval-only",
    f"{V}/crypt-des.c:41-180 (IP/key_perm/comp_perm/sbox/pbox/bits32 tables)",
    "const-table heads + the CRYPT_A64 string continuation (interiors are "
    "the excluded const tables): const-evaluated, no DA records; the "
    "consuming des_init/table code is fuzz-measured (DA up to 35840)")
row("src/crypt/cryptdes.rs", 206, "defensive-c-parity", f"{V}/crypt-des.c:325",
    "des_init comp-perm probe `continue` on inv_key_perm sentinel 255: "
    "probed inbits are 8k+j with j in 0..=6 (bit 7 of each byte never "
    "probed), while the 255 entries are exactly the PC-1 parity positions "
    "7,15..63 absent from KEY_PERM — C's identical continue is dead too")
row("src/crypt/cryptdes.rs", 388, "defensive-c-parity", f"{V}/crypt-des.c:510-517",
    "do_des decrypt-direction key selection (count<0): the crate's only "
    "do_des/des_cipher callers pass count=1 (xdes key fold, "
    "cryptdes.rs:566), 25 (traditional) or a 24-bit xdes count (>=0, ==0 "
    "errors at :383-384 first) — C's `Decrypting` arm is equally dead "
    "through px_crypt_des")
row("src/crypt/cryptdes.rs", [547, 548], "instrument-unmappable",
    f"{V}/crypt-des.c:640-647 (count/salt locals)",
    "deferred-init `let count: i32; let salt: i64;` declarations: rustc "
    "1.96 emits no DA record (noDA in the lcov of record); both assigning "
    "branches and every consumer line are DA-measured (154/873)")
row("src/crypt/cryptdes.rs", 579, "defensive-c-parity", f"{V}/crypt-des.c:723",
    "NUL-stop in the strlcpy(output, setting, 10) port: xdes settings reach "
    "here with len>=9 enforced by crypt_xdes (desc.rs:37-40 = C's 9-char "
    "requirement) and SQL text / the driver's NUL-sanitized domain cannot "
    "carry 0x00, so none of the first 9 bytes is NUL; C's strlcpy NUL-stop "
    "cannot fire either when strlen(setting)>=9")
row("src/crypt/cryptdes.rs", 593, "defensive-c-parity", f"{V}/crypt-des.c:757",
    "traditional 1-char-salt repair (`setting[1] != 0` else-arm): crypt_des "
    "enforces len>=2 (desc.rs:15-17) mirroring C's strlen(setting)<2 "
    "ereport (crypt-des.c:741-744), and text cannot carry NUL, so "
    "setting[1] != 0 always — C's `setting[1] ? : output[0]` repair is "
    "equally dead after its own length check")

# ----------------------------------------------------------- shacrypt.rs
row("src/crypt/shacrypt.rs", 116, "defensive-c-parity",
    f"{V}/crypt-sha.c:316-321 (%.*s + pg_mblen 1-byte default)",
    "mb_char continuation-byte fallback (`_ => 1`, first byte 0x80-0xBF): "
    "the salt scan advances only over ASCII itoa64/'$' bytes, so the error "
    "position is always a char boundary of the &str (valid UTF-8) setting — "
    "the first rejected byte is ASCII (:112 arm) or a lead byte (:113-115), "
    "never a continuation byte; mirrors pg_mblen's 1-byte default row")
row("src/crypt/shacrypt.rs", [138, 139, 140, 142], "census-OUT-carve",
    f"{V}/crypt-sha.c:211-216",
    "COST-BOUND carve (stated in the target banner): srounds > ROUNDS_MAX "
    "needs a parsed rounds in (999999999, 2^31), and C then RUNS the "
    "clamped 999,999,999 rounds — the driver's probe refuses every "
    "rounds>1000 setting as a counted cost skip, symmetrically, before "
    "either side executes; executable witness: "
    "shacrypt::tests::clamp_notice_prints_the_truncated_signed_value "
    "drives clamp_rounds(1_000_000_000) through these lines")
row("src/crypt/shacrypt.rs", 183, "unreachable-arm",
    f"{V}/crypt-sha.c:526-541 (per-case b64 tail; no generic encoder in C)",
    "hash64_encode len%3==0 arm (`_ => ()`): both call sites pass the "
    "transposed digest, 32 (sha256) or 64 (sha512) bytes; 32%3==2 and "
    "64%3==1, so the 0-mod-3 arm is dead given the only two DSIZEs")
row("src/crypt/shacrypt.rs", (224, 229), "const-eval-only",
    f"{V}/crypt-sha.c:526-541 (b64_from_24bit emission order)",
    "SHA256_TRANSPOSE/SHA512_TRANSPOSE byte-string consts (heads + "
    "continuation lines): const-evaluated, no DA records; the consuming "
    "transpose loop (:343-346) is fuzz-measured")
row("src/crypt/shacrypt.rs", 361, "defensive-c-parity", f"{V}/crypt-sha.c:137-140",
    "strlen<3 entry check: the only route in is px_crypt's PX_CRYPT_LIST "
    "3-byte '$5$'/'$6$' prefix dispatch (crypt.rs:242-250), so s.len()>=3 "
    "always; C's identical check is equally dead via px_crypt (ported for "
    "direct callers, shacrypt.rs:350-352)")
row("src/crypt/shacrypt.rs", (365, 369), "defensive-c-parity",
    f"{V}/crypt-sha.c:146-150",
    "magic-byte enclosure check: the prefix dispatch guarantees "
    "s[0]=='$' && s[2]=='$' — C's arm is equally dead via px_crypt")
row("src/crypt/shacrypt.rs", [379, 380, 382], "defensive-c-parity",
    f"{V}/crypt-sha.c:273-274",
    "unknown-crypt-identifier elog: the prefix dispatch guarantees s[1] in "
    "{5,6} — C's elog is equally dead via px_crypt")
row("src/crypt/shacrypt.rs", 462, "census-OUT-carve",
    f"{V}/crypt-sha.c:236-266 (non-custom-rounds result assembly)",
    "COST-BOUND carve (stated in the target banner): !rounds_custom implies "
    "the ROUNDS_DEFAULT=5000 run, which the probe refuses (sha pinned to "
    "1000) — so every executed shacrypt has rounds= in its output; "
    "executable witness: divergence_witness::div_d13_shacrypt_empty_salt_"
    "accepted (crypt('foox','$5$') == 18.3-captured '$5$$…')")

# ---------------------------------------------------------------- lib.rs
row("src/lib.rs", (28, 30), "unreachable-arm",
    "none (Rust-only enum variant; crypt.rs:23)",
    "crypt_err's CryptError::Unsupported arm: the variant is declared but "
    "constructed NOWHERE in the crate (only pattern-matched in test code) — "
    "provably dead given the callers")
row("src/lib.rs", 70, "excluded-state",
    "contrib/pgcrypto/pgcrypto.c CheckBuiltinCryptoMode (builtin_crypto_enabled)",
    "GUC-state arm: fires only with pgcrypto.builtin_crypto_enabled='off'; "
    "the harness GUC store holds boot defaults and no arm sets GUCs — GUC "
    "state is excluded-state per campaign convention; the guard call itself "
    "is fuzz-measured (DA=1235)")
row("src/lib.rs", [104, 105, 106, 107, 108, 110], "census-OUT-carve",
    "contrib/pgcrypto/pgcrypto.c pg_encrypt/pg_decrypt error translation",
    "carved-family wrapper: cipher_err translates cipher.rs errors and "
    "cipher.rs (raw px block ciphers) is the OpenSSL-provider carved family "
    "(see the carved-file rows)")
row("src/lib.rs", [115, 117, 118, 120, 121, 122, 123, 126, 127, 129, 131,
                   132, 133, 136, 137, 139, 140], "census-OUT-carve",
    "contrib/pgcrypto/pgcrypto.c pg_encrypt/pg_decrypt/pg_encrypt_iv/pg_decrypt_iv",
    "carved-family wrapper: fc_cipher! macro body dispatching into "
    "cipher.rs — OpenSSL-provider carve; note fc*!-macro lines are also the "
    "known false-UNCOVERED instrument class tree-wide")
row("src/lib.rs", (145, 148), "census-OUT-carve",
    "contrib/pgcrypto/pgcrypto.c (the four cipher wrappers)",
    "fc_cipher! invocation lines (no DA records — macro-invocation "
    "instrument class) expanding to the carved cipher wrappers above")
row("src/lib.rs", [150, 151, 152, 153, 154, 155, 157, 158, 159, 161],
    "excluded-state", "contrib/pgcrypto/pgcrypto.c pg_random_bytes",
    "PRNG surface: output is entropy, no differential oracle exists "
    "(excluded-state PRNG per campaign convention); the length-check error "
    "arm additionally needs OS entropy exhaustion for :158-159")
row("src/lib.rs", [164, 165, 166], "excluded-state",
    "contrib/pgcrypto/pgcrypto.c pg_random_uuid -> gen_random_uuid",
    "PRNG surface: gen_random_uuid entropy output — excluded-state")
row("src/lib.rs", [169, 170], "census-OUT-carve",
    "contrib/pgcrypto/openssl.c CheckFIPSMode / pg_check_fipsmode",
    "OpenSSL-provider carve: FIPS-mode probe is the OpenSSL EVP surface "
    "upstream compiles against; pgrust ships constant false")
row("src/lib.rs", [175, 177, 178, 181, 182], "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c (add_notice/ereport NOTICE path)",
    "carved-family wrapper machinery: here()/pgp_notice serve only the pgp "
    "sym/pub decrypt NOTICE path — PGP session family carve")
row("src/lib.rs", 195, "defensive-c-parity", f"{V}/px.c:96-101",
    "px_msg's PXE_NO_RANDOM->XX000 arm: the message arises only from "
    "entropy failure (never fires — driver ENTROPY CARVE) or the carved "
    "pgp paths; C px_THROW_ERROR's special case is the counterpart; the "
    "39000 arm is fuzz-measured (DA=222)")
row("src/lib.rs", [206, 207, 208, 211, 212], "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c PG_NARGS/PG_GETARG optional-arg pattern",
    "carved-family wrapper helper: opt_arg_bytes' only callers are the pgp "
    "sym/pub wrappers below")
row("src/lib.rs", [215, 217, 218, 219, 220, 221, 224, 226, 227, 228, 229,
                   231, 232, 234, 235, 237, 240, 241, 243, 248, 250, 251,
                   252, 253, 254, 257, 259, 260, 261, 262, 263, 265, 266,
                   268, 269, 271, 274, 275, 277, 282, 283, 284, 286, 287,
                   289, 290, 292, 293, 295, 296, 298, 299, 301, 302, 304,
                   305, 308, 310, 311, 312], "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c pgp_sym_*/pgp_pub_*/pgp_key_id_w",
    "carved-family wrappers: thin fc shells dispatching into src/pgp/* "
    "session machinery (OpenSSL-provider-dependent oracle; family carved — "
    "see the carved-file rows)")
row("src/lib.rs", 325, "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c pg_armor PG_NARGS()==3 else-arm",
    "driver call-shape: the fuzz driver always invokes pg_armor with 3 args "
    "(empty text[]s for the 0-header case), so the 1-arg SQL signature's "
    "else-arm never runs in-harness; the no-headers armor VALUE plane "
    "itself IS fuzz-covered through the empty-array path (lib.rs:415-416, "
    "DA-measured) — the two shapes produce byte-identical armor")
row("src/lib.rs", [338, 340, 341, 342, 343, 345, 346, 347, 348, 349, 350, 352],
    "census-OUT-carve", "contrib/pgcrypto/pgp-pgsql.c pgp_armor_headers",
    "SRF surface (stated in the target banner): fc_pgp_armor_headers is a "
    "MATERIALIZE-SRF wrapper — InitMaterializedSRF(..,0) resolves its "
    "tupdesc via get_call_result_type -> pg_proc, needing executor "
    "syscache/tuplestore fixtures; the SRF surface every sibling lane carves")
row("src/lib.rs", (364, 369), "census-OUT-carve",
    "src/backend/utils/fmgr/fmgr.c pg_detoast_datum (short-varlena expansion)",
    "driver arg-delivery shape: the driver constructs 4B-header array "
    "images (construct_md_array/construct_empty_array), so array_image's "
    "short-varlena re-expansion arm never runs in-harness; production "
    "executors can deliver 1B-header images — NO in-tree witness, flagged "
    "in the lane report")
row("src/lib.rs", [410, 411, 412, 425, 426, 427], "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c:772-788 parse_key_value_arrays dims/count checks",
    "driver array-construction domain: the driver builds keys/values with "
    "equal counts and matching ndims by construction, so the subscript/"
    "count-mismatch rejections cannot fire in-harness; executable witnesses "
    "(18.3-captured messages+SQLSTATEs): lib.rs armor_header_tests::"
    "{multidim_rejected, empty_array_against_nonempty_is_a_subscript_error, "
    "count_mismatch_rejected}")
row("src/lib.rs", [437, 438, 439, 465, 466, 467], "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c:790-834 NULL-element checks",
    "driver domain, stated in the target's c_model_validate banner ('the "
    "driver never builds a text[] with NULLs'); executable witnesses: "
    "armor_header_tests::{null_key_rejected, null_value_rejected, "
    "pairs_are_validated_interleaved_not_keys_first}")
row("src/lib.rs", 504, "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c pgp_armor_headers",
    "SRF surface dispatch row (see the fc_pgp_armor_headers rows)")
row("src/lib.rs", (505, 513), "census-OUT-carve",
    "contrib/pgcrypto/pgp-pgsql.c (pgp_key_id_w + sym/pub wrappers)",
    "carved-family dispatch rows: name->fn arms for the PGP session family")
row("src/lib.rs", 514, "census-OUT-carve",
    "src/backend/utils/fmgr/dfmgr.c lookup_external_function miss path",
    "dfmgr symbol-miss arm: the driver resolves only the crate's registered "
    "names; a miss requires a catalog entry naming a symbol pgcrypto does "
    "not export — dfmgr-surface behavior, owned by the dfmgr lane")

# -------------------------------------------------------------- armor.rs
row("src/pgp/armor.rs", [2, 3], "const-eval-only",
    f"{V}/pgp-armor.c:47-56 (_base64 table)",
    "BASE64 const head + string continuation: const-evaluated, no DA "
    "records; every consumer (encode/decode) is fuzz-measured")
row("src/pgp/armor.rs", 159, "defensive-c-parity",
    f"{V}/pgp-armor.c:262 (find_str trailing return NULL)",
    "find_str loop-exit None: every iteration either returns (needle[0] "
    "miss :146, short tail :150, match :153) or re-enters with "
    "p <= len-needle.len()+1 < len (needles are 8/10 bytes), so the while "
    "condition never goes false; C's trailing `return NULL` after its "
    "identical loop is dead by the same argument")
row("src/pgp/armor.rs", 165, "instrument-unmappable",
    f"{V}/pgp-armor.c:266-273 find_header",
    "deferred-init `let start;` declaration: rustc 1.96 emits no DA record "
    "(noDA in the lcov of record); the assigning loop and every consumer "
    "are DA-measured (692/732)")
row("src/pgp/armor.rs", [209, 215], "defensive-c-parity",
    f"{V}/pgp-armor.c:329-330 and :335-336 (`if (hlen <= 0)`)",
    "hlen==0 guards: find_header's Ok value is p-start >= 13 (8/10-byte "
    "sep + 5 dashes), so the ==0 half of C's `<= 0` check is dead — the "
    "negative/Err half is the fuzz-measured `?` on :207/:213")
row("src/pgp/armor.rs", 221, "defensive-c-parity", f"{V}/pgp-armor.c:341-346",
    "no-newline-before-armor_end Err: find_header admits a header only at "
    "a line start, so src[armor_end-1]=='\\n' whenever the header-skip "
    "loop is entered (armor_end>p) and position() always finds it; C's "
    "memchr-NULL goto is equally dead")
row("src/pgp/armor.rs", 243, "unreachable-arm",
    f"{V}/pgp-armor.c:361 (C reads p+1..p+5 with no bounds check)",
    "Rust-only crc slice-bounds guard: crc_eq < armor_end (src[armor_end] "
    "is '-' so the backward scan stops strictly before it) and the END "
    "header occupies >=13 bytes from armor_end, so crc_eq+5 <= src.len()-9 "
    "always; the guard exists to keep the slice panic-free where C reads "
    "unchecked under the same invariant")
row("src/pgp/armor.rs", [259, 260, 261, 262, 264, 266, 267, 268, 270, 272,
                         273, 274, 275, 276, 279, 281, 283, 284, 286, 287,
                         288, 290, 291, 292, 293, 295, 296, 297, 298, 299,
                         300, 302, 305, 306, 307, 309], "census-OUT-carve",
    f"{V}/pgp-armor.c:389-465 pgp_extract_armor_headers",
    "SRF surface (stated in the target banner): extract_armor_headers/"
    "find_subslice's only shipped entry point is fc_pgp_armor_headers, the "
    "MATERIALIZE-SRF wrapper carved with the SRF-engine surface; the C "
    "oracle entry pg_diff_pgcryptofam_armor_headers exists and is "
    "smoke-anchored; routing a pgrust side to it needs a pinned pg_proc "
    "fixture or a proof, owed elsewhere")


def expand(lines):
    if isinstance(lines, int):
        return [lines]
    if isinstance(lines, tuple):
        return list(range(lines[0], lines[1] + 1))
    return list(lines)


def compress(ls):
    runs = []
    for ln in sorted(ls):
        if runs and ln == runs[-1][1] + 1:
            runs[-1][1] = ln
        else:
            runs.append([ln, ln])
    return ";".join(f"{a}-{b}" if a != b else f"{a}" for a, b in runs)


def main():
    # ---- recompute the residual sets from the lcov of record ----
    da, cur = {}, None
    for raw in open(LCOV, encoding="utf-8", errors="replace"):
        raw = raw.strip()
        if raw.startswith("SF:"):
            p = raw[3:]
            cur = p if p.startswith(CRATE) else None
            if cur is not None:
                da.setdefault(cur, {})
        elif cur and raw.startswith("DA:"):
            ln, cnt = raw[3:].split(",")[:2]
            d = da[cur]
            d[int(ln)] = max(d.get(int(ln), 0), int(cnt))
        elif raw == "end_of_record":
            cur = None

    test_scope.set_repo_root(REPO)
    ts = test_scope.scope_for_crate(CRATE)

    resid, measured, in_sloc = {}, 0, 0
    for f in IN_SCOPE:
        rel = f"{CRATE}/{f}"
        assert not ts.is_test_file(rel), rel
        lines = sloc_rules.sloc_lines(os.path.join(REPO, rel))
        d = da.get(rel, {})
        r = sorted(ln for ln in lines if d.get(ln, 0) == 0)
        resid[f] = set(r)
        in_sloc += len(lines)
        measured += len(lines) - len(r)
    carved_sloc = {}
    for f in CARVED:
        rel = f"{CRATE}/{f}"
        assert not ts.is_test_file(rel), rel
        carved_sloc[f] = sloc_rules.sloc_lines(os.path.join(REPO, rel))
    carved_total = sum(len(v) for v in carved_sloc.values())
    total = in_sloc + carved_total

    # numbers of record
    assert in_sloc == 1401, in_sloc
    assert measured == 1153, measured
    assert sum(len(v) for v in resid.values()) == 248
    assert carved_total == 1635, carved_total
    assert total == 3036, total

    # ---- validate the classification covers the residual exactly ----
    claimed = {f: set() for f in IN_SCOPE}
    for f, lines, cls, cc, just in C:
        ls = expand(lines)
        dup = claimed[f] & set(ls)
        assert not dup, (f, sorted(dup))
        claimed[f].update(ls)
    for f in IN_SCOPE:
        missing = resid[f] - claimed[f]
        extra = claimed[f] - resid[f]
        assert not missing, (f, "UNCLASSIFIED", sorted(missing))
        assert not extra, (f, "NOT-RESIDUAL", sorted(extra))

    # ---- per-class counts ----
    counts = {}
    for f, lines, cls, cc, just in C:
        counts[cls] = counts.get(cls, 0) + len(expand(lines))
    print("per-class residual line counts (in-scope):")
    for cls in sorted(counts, key=counts.get, reverse=True):
        print(f"  {counts[cls]:4d}  {cls}")
    assert sum(counts.values()) == 248

    # ---- emit evidence table ----
    with open(os.path.join(HERE, "residual-classification.tsv"), "w") as out:
        out.write("# per-line classification of the 248 in-scope residual "
                  "lines (lcov of record: fleet floor "
                  "pgrust-fuzz-campaign-1785664060-1cd9-61609 @ f8231ca7535)\n")
        out.write("file\tline\tclass\tc_counterpart\tjustification\n")
        for f, lines, cls, cc, just in C:
            for ln in expand(lines):
                out.write(f"{CRATE}/{f}\t{ln}\t{cls}\t{cc}\t{just}\n")

    # ---- emit ledger rows (contiguous runs per classification entry) ----
    with open(os.path.join(HERE, "ledger-rows.tsv"), "w") as out:
        for f, lines, cls, cc, just in C:
            out.write(f"{CRATE}/{f}\t{compress(expand(lines))}\t{cls}\t{cc}"
                      f"\t{just}\t{AUTHOR}\tpending\n")
        for f, (cc, what) in CARVED.items():
            ls = carved_sloc[f]
            out.write(f"{CRATE}/{f}\t{compress(ls)}\tcensus-OUT-carve\t{cc}\t"
                      f"{CARVE_WHY.format(what=what)} [{len(ls)} v2-SLOC lines,"
                      f" whole file]\t{AUTHOR}\tpending\n")

    print(f"\nEQUATION: {total} == {measured} (measured) + "
          f"{sum(counts.values())} (exception lines) + {carved_total} "
          f"(carved lines)   unaccounted = "
          f"{total - measured - sum(counts.values()) - carved_total}")


if __name__ == "__main__":
    main()
