# Audit: backend-utils-adt-char (`*/char.c`)

C source: `postgres-18.3/src/backend/utils/adt/char.c` (255 lines, 14 fns, no
statics; only the ISOCTAL/TOOCTAL/FROMOCTAL macros). Port:
`crates/backend-utils-adt-char/src/lib.rs`.

## Function inventory

| C fn (line) | Port | Verdict | Notes |
|---|---|---|---|
| `charin` (41) | `charin` | MATCH | `len==4 && '\\' && ISOCTAL×3` → `decode_octal`; else first byte / NUL. |
| `charout` (64) | `charout` | MATCH | high-bit → `\ooo`; nonzero → byte; 0x00 → empty. Mcx + PgResult for the C `palloc(5)`. |
| `charrecv` (94) | `charrecv` | MATCH | `pq_getmsgbyte(buf) as i8` (C truncates int → char). |
| `charsend` (105) | `charsend` | MATCH | `pq_begintypsend`/`pq_sendbyte`/`pq_endtypsend`; Mcx + PgResult (bytea). |
| `chareq` (127) | `chareq` | MATCH | `==`. |
| `charne` (136) | `charne` | MATCH | `!=`. |
| `charlt` (145) | `charlt` | MATCH | compared `as u8` (C `(uint8)`). |
| `charle` (154) | `charle` | MATCH | compared `as u8`. |
| `chargt` (163) | `chargt` | MATCH | compared `as u8`. |
| `charge` (172) | `charge` | MATCH | compared `as u8`. |
| `chartoi4` (182) | `chartoi4` | MATCH | `arg1 as i32` ((int32)(int8)). |
| `i4tochar` (190) | `i4tochar` | MATCH | `< i8::MIN || > i8::MAX` (= SCHAR_MIN/MAX -128/127) → Err(22003) "\"char\" out of range"; else `as i8`. |
| `text_char` (204) | `text_char` | MATCH | detoasted text payload `&[u8]` (= VARDATA_ANY / VARSIZE_ANY_EXHDR); same 3 branches as charin + honest empty. |
| `char_text` (228) | `char_text` | MATCH / SEAMED | char→string via in-crate `charout`; text varlena built via `cstring_to_text` seam (varlena.c owner, real cycle). Byte-identical payload+VARSIZE. |

## Findings (fixed during audit)

1. **Octal-escape overflow (charin + text_char), FIXED.** C computes
   `(FROMOCTAL(o1) << 6) + (FROMOCTAL(o2) << 3) + FROMOCTAL(o3)` in promoted
   `int` (sum up to 511 for `\777`) then truncates to `char` on the
   `PG_RETURN_CHAR` assignment. The first cut did the arithmetic in `u8`, which
   panics in debug (and wraps in release) for a leading octal digit > 3 (e.g.
   `\700`). Re-homed the math into a `from_octal -> u32` + `decode_octal` helper
   that truncates with `as u8`, matching C's wrap. Regression test
   `charin_octal_high_leading_digit_wraps` (`\700`→192, `\777`→255).

## Seams & wiring

- Owns no `-seams` crate (no inbound cyclic callers; none exists in tree).
- Outward seam `cstring_to_text` (backend-utils-adt-varlena-seams): justified
  cycle, thin marshal+delegate.
- `init_seams()` empty (no owned seams), wired into `seams-init::init_all()`;
  both recurrence_guard tests pass.

## Design conformance

Allocating fns carry `Mcx` + return `PgResult`; `i8` is the real C `char`,
`&[u8]` the real detoasted text payload (no invented opacity); no statics,
locks, registries, todo!/unimplemented!.

## Verdict: PASS

`cargo check -p backend-utils-adt-char -p seams-init` clean; 11 crate tests +
2 guard tests green.
