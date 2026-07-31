#!/usr/bin/env python3
"""Targeted residual seeds for jsonpath_diff (p1-laneaa, coverage close-out).

The first local coverage merge (fuzz/coverage/jsonpath_diff.lcov over the
committed corpus) left ~287 uncovered v2-SLOC lines concentrated in:
  - mutability.rs DtStatus lattice branches (datetime-method comparisons,
    Any bounds, lax/strict AnyArray resets, var-type table draws),
  - path.rs reader/printer arms for rare item shapes,
  - scan.rs / gram.rs cold error arms.
These are grammar shapes the regress corpus never combines; seed them
deliberately. Every text is emitted across arms 0 (hard+soft), 1 and 2, and
arm-2 texts fan out over the full varsel byte range so every VAR_TABLE draw
(count x start offset) is witnessed.

Usage: python3 fuzz/gen_seeds_jsonpath_residual.py
"""
import hashlib
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "fuzz", "corpus", "jsonpath_diff")

DT = ['datetime()', 'datetime("HH24:MI")', 'datetime("HH24:MI TZH")',
      'date()', 'time()', 'time_tz()', 'time(2)', 'time_tz(2)',
      'timestamp()', 'timestamp_tz()', 'timestamp(2)', 'timestamp_tz(2)',
      'string()', 'type()']

TEXTS = []
# Every datetime-status pair under every comparison op (mutability lattice).
OPS = ['==', '!=', '<', '<=', '>', '>=']
for i, a in enumerate(DT):
    for j, b in enumerate(DT):
        TEXTS.append(f'$.a.{a} {OPS[(i + j) % len(OPS)]} $.b.{b}')
# Var-driven datetime status + var draws.
for a in DT:
    TEXTS.append(f'$x.{a} == $y.datetime()')
    TEXTS.append(f'$x == $y.{a}')
# Any / AnyArray / AnyKey bounds resets, lax vs strict.
for pre in ('', 'strict ', 'lax '):
    TEXTS += [
        f'{pre}$.**.datetime() == $.a.date()',
        f'{pre}$.**{{2}}.datetime() == $.a.date()',
        f'{pre}$.**{{0 to last}}.datetime() == $.b.time()',
        f'{pre}$[*].datetime() == $.a.time_tz()',
        f'{pre}$.*.datetime() == $.a.timestamp()',
        f'{pre}$[0 to last].datetime() == $.a.timestamp_tz()',
        f'{pre}$[last].datetime() == $.a.datetime()',
    ]
# Walker arms: not/exists/is unknown/unary/binary/starts with/like_regex.
TEXTS += [
    '!($.a.datetime() == $.b.date())',
    '($.a.datetime() == $.b.date()) is unknown',
    'exists($.a.datetime()) && $.b.date() == $.c.date()',
    '(+$.a.x == -$.b.y) || $.c.datetime() == $.d.datetime()',
    '$.a + $.b == $.c.datetime()',
    '$.a - $.b.floor() == $.c.date()',
    '$.a * 2 == 4 && $.b.datetime() != $.c.time()',
    '$.a / 2 == 1 || $.b % 3 == 0',
    '$.a starts with "x" && $.b.datetime() == $.c.datetime()',
    '$.a like_regex "^x" && $.b.datetime() == $.c.datetime()',
    '$.a like_regex "^x" flag "iq"',
    '$.a ? (@.datetime() == $x.datetime()) . b',
    '$.a ? (exists(@.b)) . c.datetime() == $.d.date()',
    '$.a.keyvalue().datetime() == $.b.date()',
    '$.a.size() == 1 && $.b.datetime() == $.c.date()',
    '$.a.abs().ceiling().floor() == $.b.double()',
    '$.a.bigint() == $.b.integer() && $.c.number() == $.d.decimal()',
    '$.a.decimal(10,2) == $.b.decimal(5)',
    '$.a.boolean() == true && $.b.string() == "x"',
    'last > 0',
    '$[$.a.datetime()]',
    '$[0, 1 to 2, $x]',
    '$.a[0].datetime() == $.b[last].date()',
    '"str" == $.a.datetime()',
    'null != $.a.date()',
    '1.5 == $.a.time()',
    'true == ($.a.datetime() == $.b.date())',
]
# Cold printer/reader shapes.
TEXTS += [
    '$ ? (@ == $" quoted var name ")',
    '$."k e y"."\\u0041\\u{1F600}"',
    '$.a.type().size().double()',
    'strict $.a ? ((@ == 1) is unknown)',
    '-(3 + 4 * $.a)', '+$.x', '-$.x', '$ + $ - $ * $ / $ % $',
    '0x0', '0o17', '0b101', '1_000_000.000_1', '1e1_0',
    '.1', '1.', '1e', '00', '1..2', '$.',  # scanner error arms
    '$ ?? 1', '$ ? (1 ==== 2)', 'strict', 'lax', '@', '@.a',
]
# Round 4 (coverage close-out, final REVIEW drive):
#  - \u{XXXXXX} escapes above U+10FFFF: drives add_unicode_char's
#    pg_unicode_to_server_noerror None branch (scan.rs ~493-501) in soft
#    mode and the hard-error twin; brace escapes admit up to 6 hex digits
#    so 0x110000..0xFFFFFF are lexable but unassignable codepoints.
#  - `to`-subscript expressions whose flatten soft-fails ("@ is not allowed
#    in root expressions"): drives the IndexArray to-branch None propagation
#    (path.rs:377).
TEXTS += [
    '$."\\u{110000}"',          # invalid codepoint in quoted string
    '$."a\\u{FFFFFF}b"',        # max 6-hex-digit brace escape, invalid
    '$"v\\u{110000}"',          # same, variable-quoted (xvq)
    '$.\\u{110000}',            # same, unquoted identifier escape (xnq)
    '$."\\u0041\\u{110000}"',   # valid escape then invalid in one run
    '$[0 to @]',                # flatten soft error inside `to` subscript
    '$[0 to @ + 1]',
    '$[last to @]',
]

def emit(seed: bytes):
    h = hashlib.sha1(seed).hexdigest()[:16]
    p = os.path.join(OUT, f"residual-{h}")
    with open(p, "wb") as f:
        f.write(seed)

def main():
    n = 0
    for i, t in enumerate(TEXTS):
        b = t.encode()
        emit(bytes([0x00, 0x00]) + b)
        emit(bytes([0x00, 0x01]) + b)
        emit(bytes([0x01, 0x01]) + b)
        # arm 2: sweep varsel so every (count,start) draw appears somewhere.
        for vs in range(0, 256, 13):
            if (i + vs) % 5 == 0 or vs in (0, 13):
                emit(bytes([0x02, vs]) + b)
        n += 1
    print(f"emitted seeds for {n} texts into {OUT}")

if __name__ == "__main__":
    main()
