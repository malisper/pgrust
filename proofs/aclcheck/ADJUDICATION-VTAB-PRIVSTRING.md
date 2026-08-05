# Adjudication package: \v (0x0B) trim in privilege strings (aclcheck lane)

Proof lane aclcheck, 2026-07-30. Divergence candidate #A2 (pre-screened in
the WAVE 9 runqueue header). Evidence only — options below, NO ruling.

## Mechanism

C aclchk.c `convert_any_priv_string` splits the privilege text on ',' and
trims each chunk with C-locale `isspace()`, which accepts
`' ' \t \n \v \f \r`. Rust `adt_acl::convert_any_priv_string`
(crates/backend/utils/adt/acl/src/ops.rs:374) trims with
`char::is_ascii_whitespace`, which accepts `' ' \t \n \f \r` — **no \v**.
A privilege chunk with a leading/trailing vertical tab is accepted by C and
rejected by pgrust.

## Ground truth (real binaries, identical SQL, 2026-07-30)

`select has_table_privilege('tt_owner','pg_class', <priv>)`:

| priv text            | C PG 18.4 (docker postgres:18) | pgrust v0.2 |
|----------------------|--------------------------------|-------------|
| E'SELECT\x0B'        | t                              | ERROR: unrecognized privilege type: "SELECT" (the \v is inside the quoted name, invisible) |
| E'\x0BSELECT'        | t                              | same ERROR  |
| E'SELECT\x0B, INSERT'| t                              | same ERROR  |

Direction: pgrust REJECTS input C accepts (fail-closed; no privilege
over-grant). Also note pgrust's error MESSAGE embeds the untrimmed chunk, so
the quoted name looks identical to the valid one — cosmetic confusion.

## Formal witness

The family's symbolic eq_* harnesses (priv text len<=8, bytes 1..=127)
cover byte 0x0B by construction and will surface this divergence as a
counterexample whenever they complete within budget; at the 450s lane cap
the fc-level harnesses wall (see results-2026-07-30.tsv), so the docker
pair above is the evidence of record. A concrete-spot harness is cheap to
add post-ruling as the regression gate (literal "SELECT\x0B").

## User-visible surface

Marginal: privilege strings containing vertical tabs. SQL-reachable but
essentially only via deliberately crafted input. No security exposure in
the accept direction (pgrust is stricter).

## Options (no ruling)

(a) C-parity: trim with an explicit matcher including \v
    (`c == ' ' || ('\t'..='\r').contains(&c)`-style, i.e. C isspace set) —
    one-line change at ops.rs:374; add the literal spot harness.
(b) Keep stricter Rust behavior; document as ratified non-surface
    (divergence(ratified) rows for the priv-string parse plane).

Candidate default is (a) (C-exactness doctrine; also what the family's
symbolic harnesses assume when they run at full budget).
