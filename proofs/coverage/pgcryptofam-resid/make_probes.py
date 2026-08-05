#!/usr/bin/env python3
"""Adversarial probe seeds for the SEED-GAP audit (lane p1-pgcryptofam-resid).

Every residual line flagged as a possible seed gap gets the strongest input
the driver encoding can express toward it (fuzz/core/src/pgcryptofam_diff.rs:
data[0]=arm, data[1]=mode, data[2..]=payload). The classification says each
target line is dead-in-harness (defensive / dispatch-guaranteed / cost- or
domain-carved); these probes are the executable cross-check: replayed under
coverage, every probe must leave its target line at DA:0 while executing the
guard/neighbor lines around it. A probe that DID light its line up would
refute the classification (and become a real corpus seed).
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "probes")
os.makedirs(OUT, exist_ok=True)

P = {}

# --- crypt (arm 0) -------------------------------------------------------
# cryptdes.rs:593 — needs setting[1]==0; best attempt: 1-char salt.
P["crypt-trad-1char-salt"] = bytes([0, 0, 2]) + b"pw" + b"a"
# control: 2-char salt success (covers the 588-594 neighborhood)
P["crypt-trad-2char-salt"] = bytes([0, 0, 2]) + b"pw" + b"rl"
# cryptdes.rs:579 — needs a NUL inside the first 9 xdes setting bytes; the
# domain carve rewrites 0x00 -> 0x01 (text_field). '_' prefix = mode 18.
P["crypt-xdes-embedded-nul"] = bytes([0, 18, 4]) + b"pass" + b"z1.." + b"\x00bcd"
# control: xdes count 255 success
P["crypt-xdes-count255"] = bytes([0, 18, 8]) + b"password" + b"z1..abcd"
# shacrypt.rs:462 — needs !rounds_custom => default 5000 rounds => cost skip.
P["crypt-sha-no-rounds"] = bytes([0, 4, 2]) + b"pw" + b"abc"
# shacrypt.rs:138-142 — needs parsed rounds in (1e9, 2^31) => cost skip.
P["crypt-sha-rounds-1e9"] = bytes([0, 20, 2]) + b"pw" + b"1000000000$ab"
# control: rounds=1000 runs (the cost pin's accept band)
P["crypt-sha-rounds-1000"] = bytes([0, 20, 2]) + b"pw" + b"1000$abcdefgh"
# shacrypt.rs:361/365-369/379-382 — dispatch-guaranteed prefixes; "$7$" and
# a bare-magic-looking setting route to DES catch-all, never shacrypt.
P["crypt-dollar7-catchall"] = bytes([0, 22, 2]) + b"pw" + b"x"
# shacrypt.rs:116 — multibyte salt char: lead byte is rejected first.
P["crypt-sha-multibyte-salt"] = bytes([0, 20, 2]) + b"pw" + "1000$aéb".encode()
# bcrypt.rs:40 — bf_decode only ever runs with count=16.
P["crypt-bf-cost04"] = bytes([0, 8, 2]) + b"pw" + b"04$......................"

# --- gen_salt (arm 1) ----------------------------------------------------
# crypt.rs:92 — gen_salt('xdes', 0): def_rounds=725 substituted before gen.
# mode 38: algo table idx 2 (xdes), raw-i32 rounds, two-arg wrapper.
P["gensalt-xdes-rounds0"] = bytes([1, 38]) + (0).to_bytes(4, "little")
# crypt.rs:120/123 — gen_salt('bf', 0): def 6; range-check [4,31] first.
P["gensalt-bf-rounds0"] = bytes([1, 54]) + (0).to_bytes(4, "little")
P["gensalt-bf-rounds-neg"] = bytes([1, 54]) + (-1).to_bytes(4, "little", signed=True)
# crypt.rs:138 — sha generator always gets the 16-byte rbuf.
P["gensalt-sha256-rounds0"] = bytes([1, (4 << 4) | 2 | 4]) + (0).to_bytes(4, "little")
# crypt.rs:76 — false-red cross-check: traditional-des salt success path
# (body lines 77-81 must light up; the `Some(` head has no DA record).
P["gensalt-des-rounds25"] = bytes([1, (0 << 4) | 2 | 4]) + (25).to_bytes(4, "little")
P["gensalt-md5-rounds0"] = bytes([1, (1 << 4) | 2 | 4]) + (0).to_bytes(4, "little")

# --- armor (arm 2) -------------------------------------------------------
# lib.rs:410-413/425-427/437-439/465-467 — driver builds equal-count 1-D,
# never-NULL arrays; 0 headers exercises the ndim==0 accept path instead.
P["armor-no-headers"] = bytes([2, 0]) + b"hello pgcrypto"
P["armor-two-headers"] = bytes([2, 1, 2, 3, 4]) + b"Key" + b"valu" + bytes([2, 3]) + b"Ab" + b"xyz" + b"data"

# --- dearmor (arm 3) -----------------------------------------------------
# armor.rs:159 — no BEGIN marker at all: find_str returns via :146.
P["dearmor-no-marker"] = bytes([3, 0]) + b"junk with no marker at all"
# armor.rs:159 — trailing '-' tail: find_str returns via :150.
P["dearmor-dash-tail"] = bytes([3, 0]) + b"x\n-"
P["dearmor-dashes-tail"] = bytes([3, 0]) + b"x\n-----BEGI"
# armor.rs:209/215 — hlen==0 needs find_header to return Ok(0): impossible
# (Ok is always >= 13). Best attempt: minimal BEGIN header, no tail.
P["dearmor-begin-only"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----"
P["dearmor-begin-nl"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----\n"
# armor.rs:221 — needs no '\n' before armor_end; but END must sit at a line
# start, so src[armor_end-1]=='\n'. Best attempt: END right after BEGIN.
P["dearmor-end-immediately"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----\n-----END PGP MESSAGE-----\n"
# armor.rs:243 — needs crc_eq+5 > len; END header (>=13B) past crc_eq makes
# that impossible. Best attempt: '=' as late as an envelope allows.
P["dearmor-late-crc"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=Tf-----END PGP MESSAGE-----\n"
P["dearmor-crc-truncated"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=Tf"
# mutation kinds 2/4 (truncate / shorten CRC) over a real envelope
P["dearmor-mut-truncate"] = bytes([3, 1, 1, 3, 4]) + b"Keyval" + bytes([6]) + b"abcdef" + bytes([2, 40, 0])
P["dearmor-mut-shortcrc"] = bytes([3, 1, 0]) + bytes([6]) + b"abcdef" + bytes([4])
# valid envelope control
P["dearmor-valid"] = bytes([3, 0]) + b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=TfTH\n-----END PGP MESSAGE-----\n"

for name, data in P.items():
    with open(os.path.join(OUT, name), "wb") as f:
        f.write(data)
print(f"wrote {len(P)} probes to {OUT}")
