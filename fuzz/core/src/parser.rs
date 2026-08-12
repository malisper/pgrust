//! parser — adversarial TEXT-INPUT bank + campaign for the HAND-ROLLED
//! `*_in` type input parsers (lane PARSER).
//!
//! ## What this targets, and why it is a distinct surface
//!
//! PostgreSQL's type input functions split into two very different
//! populations:
//!
//!   * The **numeric / fixed-shape** parsers (`int4in`, `float8in`,
//!     `numeric_in`) — narrow, heavily audited, backed by strtol/strtod-shaped
//!     cores. `edge.rs` already sprays that surface with INT_MIN/MAX and
//!     overflow-string forms.
//!   * The **bespoke, hand-rolled TEXT parsers** — functions that manually
//!     walk a cstring with a cursor, matching delimiters by hand, counting
//!     fields, and doing coordinate/number arithmetic as they go. `tidin`
//!     parsing `"(block,offset)"` is the archetype: it hand-scans for `(`,
//!     `,`, `)`, converts each field, and range-checks block against uint32
//!     and offset against uint16.
//!
//! The phase-1 C bugs of this campaign lived almost entirely in the SECOND
//! population — ltree's parser integer overflow (bug-103a),
//! `DeescapeQuotedString` (bug-104), the spell.c parsers (bug-80/81/83). The
//! recurring shapes are:
//!
//!   1. **empty input** — `""`, `"()"`, `"{}"`, `"[]"`: a cursor loop that
//!      assumes at least one field, or a `ptr[-1]` / `len - 1` after a
//!      zero-length scan.
//!   2. **unbalanced / missing delimiters** — `"[("`, `"(1,2"`, `"{1,2"`: a
//!      scan-to-closing-delimiter that runs off the end of the buffer.
//!   3. **missing or extra fields** — `"( , )"`, `"(1,2,3)"`, `"(1)"`: a
//!      fixed-arity parser that indexes a field it never filled.
//!   4. **coordinate / number OVERFLOW** — the TYPE-SPECIFIC boundary, not the
//!      generic one: `tid` block is uint32 and offset is uint16, so
//!      `"(4294967296,0)"` and `"(0,65536)"` are the interesting values, and
//!      neither appears in a generic int bank. Same for inet masks (`/33`,
//!      `/129`), array `[lb:ub]` decorations, and geometric point counts.
//!   5. **embedded NUL** — a cstring cannot carry one, so every side must
//!      truncate identically; a Rust `&[u8]` parser that does NOT truncate
//!      sees a longer string than C did.
//!   6. **huge / deeply nested** — recursion depth in `array_in` / `record_in`
//!      / `range_in` / `json_in`, and length-extreme coordinate lists.
//!
//! ## The bar
//!
//! pgrust must ACCEPT-or-REJECT each string IDENTICALLY to verbatim REL_18_3
//! C, with the same error verdict and SQLSTATE class. The `*_diff` drivers
//! this module sprays already assert exactly that and PANIC on mismatch, so:
//!
//!   * a captured panic whose message is a comparator mismatch = an
//!     **accept/reject or value divergence** (MED unless it is a pgrust-side
//!     crash);
//!   * a captured panic that is a pgrust index-out-of-bounds / slice-range /
//!     unwrap / arithmetic-overflow panic, on an input C cleanly rejects, is
//!     the **HIGH** finding — the exact tid/geo/ltree phase-1 class.
//!
//! ## Relationship to `edge.rs`
//!
//! `edge.rs` owns the generic numeric/length boundary bank and is REUSED
//! wholesale here ([`parser_payloads`] folds [`crate::edge::all_edge_texts`]
//! in). This module adds what a generic bank structurally cannot contain: the
//! per-type *grammar* mutations — a `tid` with its parens stripped, a polygon
//! with one point, an inet with a `/-1` mask, an array with `[5:1]` reversed
//! bounds. Those only exist relative to a specific parser's syntax.
//!
//! ## Seed shaping
//!
//! The drivers do NOT share one input layout. Three families exist:
//!   * `[sel][text]`            — scalarxid (tidin = sel 0), network, mac,
//!                                uuid, vlbytea, ltree, hstore, contriba,
//!                                contribb, json, jsonbio, timestamp,
//!                                datetime_io, interval_engine.
//!   * `[sel][mode][text]`      — geo_io (mode bit0 = 0 selects text-in),
//!                                rangetypes (`[sel][typ]`), multirange,
//!                                arrayfuncs (`[sel][esel]`), rowtypes
//!                                (`[sel][flags]`).
//! [`ParserDriver::seeds`] emits both a 1-byte and a 2-byte header for every
//! driver whose family is ambiguous, so the text always lands at the offset
//! the parser reads from — a mis-shaped seed is a VACUOUS case, and a bank
//! that only produces vacuous cases is the gate-blindness failure mode this
//! campaign exists to avoid.

#![allow(clippy::type_complexity)]

// ===========================================================================
// 1. THE BANK — per-parser grammar mutations.
// ===========================================================================

/// `tidin` — `"(blocknumber, offsetnumber)"`. block is `BlockNumber` (uint32),
/// offset is `OffsetNumber` (uint16). The TYPE-SPECIFIC overflow points are
/// 2^32 and 2^16, neither of which a generic int bank carries in this shape.
pub const TID_TEXT: &[&[u8]] = &[
    // well-formed anchors
    b"(0,0)",
    b"(1,1)",
    b"(12,34)",
    b"(4294967295,65535)", // block MAX, offset MAX — both accepted
    // TYPE-SPECIFIC OVERFLOW: MAX+1 on each field independently
    b"(4294967296,0)",     // block MAX+1  -> reject
    b"(0,65536)",          // offset MAX+1 -> reject
    b"(4294967296,65536)", // both over
    b"(-1,0)",             // negative block
    b"(0,-1)",             // negative offset
    b"(-1,-1)",
    b"(18446744073709551616,0)", // beyond u64
    b"(99999999999999999999999999,0)",
    // EMPTY / MISSING FIELDS
    b"",
    b"()",
    b"( )",
    b"(,)",
    b"( , )",
    b"(,0)",
    b"(0,)",
    b"(  ,  )",
    // MISSING / UNBALANCED DELIMITERS
    b"(0,0",
    b"0,0)",
    b"0,0",
    b"(0 0)",
    b"((0,0))",
    b"(0,0))",
    b"((0,0)",
    b")0,0(",
    b"[0,0]",
    b"{0,0}",
    // EXTRA FIELDS
    b"(0,0,0)",
    b"(1,2,3,4)",
    b"(0,0,)",
    // WHITESPACE / SIGN / RADIX forms
    b" (0,0) ",
    b"\t(0,0)\n",
    b"(+0,+0)",
    b"( 0 , 0 )",
    b"(0x10,0x10)",
    b"(010,010)",
    b"(0b1,0b1)",
    b"( 1e3 , 1e3 )",
    b"(1.5,2.5)",
    b"(1 ,2)",
    // TRAILING GARBAGE
    b"(0,0)x",
    b"(0,0) trailing",
    b"(0,0);",
    b"x(0,0)",
];

/// Geometric text inputs — `point`/`lseg`/`box`/`path`/`polygon`/`circle`/
/// `line`. The bespoke `pair_decode`/`path_decode` cursor walkers in geo_ops.c
/// (the family that already produced bug-114) count coordinates by hand and
/// match nested `(`/`[`/`<` groupings.
pub const GEO_TEXT: &[&[u8]] = &[
    // well-formed anchors, one per shape
    b"(1,2)",
    b"(1,2),(3,4)",
    b"[(1,2),(3,4)]",
    b"((1,2),(3,4),(5,6))",
    b"<(1,2),3>",
    b"{1,2,3}",
    // EMPTY / DEGENERATE COUNTS
    b"",
    b"()",
    b"[]",
    b"<>",
    b"{}",
    b"[()]",
    b"(())",
    b"((1,2))", // 1-point polygon
    b"[(1,2)]", // 1-point path
    b"[]",      // 0-point path
    b"[,]",
    b"<(),0>",   // circle with empty centre
    b"<(1,2),>", // circle with missing radius
    b"<,3>",
    b"{}",        // line with no coefficients
    b"{1,2}",     // line missing C
    b"{1,2,3,4}", // line with an extra coefficient
    b"{0,0,0}",   // degenerate line A=B=0 -> 22P02 in C
    // UNBALANCED BRACKETS — the classic
    b"[(",
    b"[(1,2)",
    b"((1,2)",
    b"(1,2",
    b"<(1,2),3",
    b"{1,2,3",
    b"[(1,2),(3,4)",
    b"((1,2),(3,4)",
    b"]",
    b")",
    b">",
    b"}",
    b"[)",
    b"(]",
    b"<}",
    b"[(1,2)>",
    // MISSING COMMAS / EXTRA COMMAS
    b"(1 2)",
    b"[(1,2)(3,4)]",
    b"((1,2)(3,4))",
    b"(1,,2)",
    b"(,1,2)",
    b"(1,2,)",
    b"[(1,2),,(3,4)]",
    // HUGE / Inf / NaN COORDS
    b"(Infinity,Infinity)",
    b"(-Infinity,0)",
    b"(NaN,NaN)",
    b"(inf,-inf)",
    b"(1e400,1e400)",
    b"(-1e400,1e-400)",
    b"(1e308,1e308)",
    b"(1.7976931348623157e308,1.7976931348623157e308)",
    b"<(0,0),Infinity>",
    b"<(0,0),NaN>",
    b"<(0,0),-1>", // negative radius
    b"{Infinity,NaN,0}",
    // TRAILING GARBAGE / LEADING JUNK
    b"(1,2)junk",
    b"junk(1,2)",
    b"(1,2) (3,4)",
    b"[(1,2),(3,4)] x",
    b" ((1,2),(3,4)) ",
];

/// Network text inputs — `inet`/`cidr`/`macaddr`/`macaddr8`. The hand-rolled
/// part is the `/masklen` suffix scan and the per-octet accumulate loop.
pub const NET_TEXT: &[&[u8]] = &[
    // anchors
    b"1.2.3.4",
    b"1.2.3.4/24",
    b"::1",
    b"::1/128",
    b"08:00:2b:01:02:03",
    b"08:00:2b:01:02:03:04:05",
    // MASK OUT OF RANGE — the type-specific boundary
    b"1.2.3.4/-1",
    b"1.2.3.4/32",
    b"1.2.3.4/33",
    b"1.2.3.4/255",
    b"1.2.3.4/256",
    b"1.2.3.4/4294967296",
    b"1.2.3.4/99999999999999999999",
    b"::1/-1",
    b"::1/128",
    b"::1/129",
    b"::1/1000",
    // MASK MISSING / EMPTY
    b"1.2.3.4/",
    b"1.2.3.4/ ",
    b"/24",
    b"/",
    b"::1/",
    b"1.2.3.4//24",
    b"1.2.3.4/24/24",
    b"1.2.3.4/+24",
    b"1.2.3.4/024",
    b"1.2.3.4/0x18",
    // MALFORMED OCTETS
    b"256.1.1.1",
    b"1.256.1.1",
    b"1.1.1.256",
    b"999.999.999.999",
    b"1.1.1.1.1",
    b"1.1.1",
    b"1.1",
    b"1",
    b"1...1",
    b"....",
    b".1.1.1",
    b"1.1.1.",
    b"-1.1.1.1",
    b"1.-1.1.1",
    b"01.02.03.04",
    b"0x1.0x2.0x3.0x4",
    // EMPTY / WHITESPACE / GARBAGE
    b"",
    b" ",
    b"  1.2.3.4  ",
    b"\t1.2.3.4",
    b"1.2.3.4 ",
    b"1.2.3.4x",
    b"x1.2.3.4",
    b"not-an-address",
    // v6 SHAPES
    b":::",
    b"::::",
    b"1::2::3",
    b"::ffff:1.2.3.4",
    b"::ffff:256.1.1.1",
    b"1:2:3:4:5:6:7:8:9",
    b"1:2:3:4:5:6:7",
    b"gggg::1",
    b"12345::1",
    b"[::1]",
    b"[::1]/64",
    // MAC SHAPES
    b"08:00:2b:01:02",
    b"08:00:2b:01:02:03:04",
    b"08:00:2b:01:02:03:04:05:06",
    b"08-00-2b-01-02-03",
    b"08002b010203",
    b"08002b:010203",
    b"0800.2b01.0203",
    b"zz:00:2b:01:02:03",
    b"08:00:2b:01:02:0g",
    b"08::2b:01:02:03",
    b":00:2b:01:02:03",
    b"08:00:2b:01:02:03:",
];

/// Range / multirange text — `range_in` and the recursive `multirange_in`.
/// Bound sub-literals are themselves parsed by a nested input function, and
/// the quote/escape state machine is hand-rolled.
pub const RANGE_TEXT: &[&[u8]] = &[
    // anchors
    b"[1,2]",
    b"(1,2)",
    b"[1,2)",
    b"(1,2]",
    b"empty",
    b"{[1,2]}",
    b"{}",
    b"{[1,2],[3,4]}",
    // EMPTY vs the `empty` KEYWORD
    b"",
    b" ",
    b"EMPTY",
    b"Empty",
    b"empty ",
    b" empty",
    b"emptyx",
    b"xempty",
    b"[empty,empty]",
    b"[,]",
    b"[]",
    b"()",
    b"(,)",
    b"[,)",
    b"(,]",
    // INFINITE / MISSING BOUNDS
    b"[,2]",
    b"[1,]",
    b"(,2)",
    b"(1,)",
    b"[-infinity,infinity]",
    b"[infinity,-infinity]",
    // BACKWARDS BOUNDS (must be a clean 22000 range error, never a crash)
    b"[2,1]",
    b"(2,1)",
    b"[10,-10]",
    b"[9223372036854775807,-9223372036854775808]",
    // UNBALANCED DELIMITERS
    b"[1,2",
    b"1,2]",
    b"[1,2>",
    b"<1,2]",
    b"[[1,2]]",
    b"[1,2]]",
    b"[[1,2]",
    b"{[1,2]",
    b"[1,2]}",
    b"{{[1,2]}}",
    b"{[1,2],}",
    b"{,[1,2]}",
    b"{[1,2][3,4]}",
    // MISSING COMMA
    b"[1 2]",
    b"[12]",
    b"[1;2]",
    b"[1,,2]",
    b"[1,2,3]",
    // NESTED QUOTE / ESCAPE handling
    br#"["1","2"]"#,
    br#"["1,2","3"]"#,
    br#"["1"#,
    br#"["1\"2",3]"#,
    br#"["","" ]"#,
    br#"[",",","]"#,
    br#"["\\",2]"#,
    br#"[\,,2]"#,
    br#"["1"x,2]"#,
    // OVERFLOWING BOUNDS (nested element parser)
    b"[2147483648,0]",
    b"[0,2147483648]",
    b"[-2147483649,0]",
    b"[9223372036854775808,0]",
    // TRAILING GARBAGE
    b"[1,2]x",
    b"[1,2] ",
    b" [1,2]",
    b"[1,2];",
];

/// Array / record text — the two recursive descent parsers with dimension
/// decorations, quote/escape state, and per-element recursion.
pub const ARRAY_TEXT: &[&[u8]] = &[
    // anchors
    b"{1,2,3}",
    b"{{1,2},{3,4}}",
    b"{}",
    b"[1:3]={1,2,3}",
    // EMPTY / DEGENERATE
    b"",
    b" ",
    b"{ }",
    b"{,}",
    b"{,,,}",
    b"{{}}",
    b"{{},{}}",
    b"{{{}}}",
    b"{\"\"}",
    b"{NULL}",
    b"{null}",
    b"{NULL,NULL}",
    // JAGGED DIMENSIONS
    b"{{1,2},{3}}",
    b"{{1},{2,3}}",
    b"{1,{2}}",
    b"{{1},2}",
    b"{{{1}},{2}}",
    // BAD [lb:ub] DECORATIONS — the type-specific overflow class
    b"[1:0]={}",
    b"[5:1]={1}", // ub < lb
    b"[0:-1]={}",
    b"[1:2147483647]={1}", // huge extent
    b"[-2147483648:2147483647]={1}",
    b"[2147483647:2147483647]={1}",
    b"[-2147483648:-2147483648]={1}",
    b"[2147483648:1]={1}", // lb overflow
    b"[1:2147483648]={1}", // ub overflow
    b"[99999999999999999999:1]={1}",
    b"[1:3]={1,2}", // decoration/content mismatch
    b"[1:2]={1,2,3}",
    b"[1:1][1:1]={{1}}",
    b"[1:1]{1}", // missing '='
    b"[1:1]=",
    b"[1:1]",
    b"[:]={1}",
    b"[1:]={1}",
    b"[:1]={1}",
    b"[1]={1}",
    b"[a:b]={1}",
    b"=[1:1]{1}",
    // UNBALANCED BRACES / QUOTES
    b"{1,2",
    b"1,2}",
    b"{{1,2}",
    b"{1,2}}",
    b"{{1,2},{3,4}",
    b"{",
    b"}",
    b"{{",
    b"}}",
    br#"{"1}"#,
    br#"{"1","2}"#,
    br#"{"}"#,
    br#"{"\"}"#,
    br#"{1,"}"#,
    // ESCAPE handling
    br#"{"a,b",c}"#,
    br#"{"a\"b"}"#,
    br#"{a\,b}"#,
    br#"{"\\"}"#,
    br#"{\}"#,
    br#"{"}"}"#,
    br#"{" "}"#,
    // TRAILING GARBAGE / WHITESPACE
    b"{1,2,3}x",
    b"x{1,2,3}",
    b"  {1,2,3}  ",
    b"{1,2,3} {4}",
    b"{1 2 3}",
];

/// `record_in` — `"(f1,f2,...)"` with its own quote/escape walker and a
/// column-count check against the tuple descriptor.
pub const RECORD_TEXT: &[&[u8]] = &[
    b"(1,2)",
    b"(a,b)",
    b"()",
    b"",
    b"( )",
    b"(,)",
    b"(,,)",
    b"(1)",     // too few columns
    b"(1,2,3)", // too many columns
    b"(1,2,)",
    b"(,1,2)",
    b"(1,2", // unbalanced
    b"1,2)",
    b"((1,2))",
    b"(1,2))",
    b"((1,2)",
    b"(1,2) ",
    b" (1,2)",
    b"(1,2)x",
    br#"("a","b")"#,
    br#"("a,b",c)"#,
    br#"("a\"b",c)"#,
    br#"("a"#,
    br#"("",""#,
    br#"(""x,b)"#,
    br#"(\,,b)"#,
    b"(NULL,NULL)",
    b"(null,)",
];

/// `bytea_in` (hex + escape), `bit`/`varbit`, and `uuid_in` malformed shapes.
pub const BINTEXT_TEXT: &[&[u8]] = &[
    // bytea hex
    b"\\x",
    b"\\x0", // ODD hex length -> 22P03/22P02 in C, never a panic
    b"\\x00",
    b"\\x000",
    b"\\xgg",
    b"\\x0g",
    b"\\x 00",
    b"\\x00 ",
    b"\\X00",
    b"\\",
    b"\\\\",
    b"\\\\x00",
    // bytea escape
    b"\\000",
    b"\\00", // short octal
    b"\\0",
    b"\\400", // octal > 255
    b"\\777",
    b"\\888",
    b"\\n",
    b"abc\\", // trailing lone backslash
    b"abc\\0",
    // bit / varbit
    b"B101",
    b"X0f",
    b"101",
    b"",
    b"2",
    b"B",
    b"X",
    b"B2",
    b"Xg",
    b"B 1",
    b"0101010101010101010101010101010101",
    // uuid malformed
    b"a0eebc999c0b4ef8bb6d6bb9bd380a11",
    b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    b"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
    b"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
    b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",   // one short
    b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111", // one long
    b"a0eebc99+9c0b+4ef8+bb6d+6bb9bd380a11",
    b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aZZ",
    b"-a0eebc999c0b4ef8bb6d6bb9bd380a11",
    b"a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11",
    b"                                    ",
];

/// `interval_in` unit words + field overflow, and the time/timestamp field
/// walkers (`ParseDateTime` splits fields by hand, then `DecodeInterval`
/// accumulates each with its own arithmetic).
pub const TEMPORAL_TEXT: &[&[u8]] = &[
    // interval anchors + unit parsing
    b"1 day",
    b"1 days 2 hours",
    b"P1Y2M3D",
    b"PT1H",
    b"P",
    b"PT",
    b"P1",
    b"PT1",
    b"1",
    b"",
    b" ",
    b"day",
    b"1 dayss",
    b"1 xyzzy",
    b"1 d a y",
    b"1day",
    b"-1 day",
    b"- 1 day",
    b"+-1 day",
    b"1 day ago",
    b"@ 1 day",
    b"@",
    b"ago",
    // interval FIELD OVERFLOW
    b"9223372036854775807 days",
    b"9223372036854775808 days",
    b"-9223372036854775808 days",
    b"2147483648 months",
    b"-2147483649 months",
    b"178000000 years",
    b"100000000000 hours",
    b"99999999999999999999 seconds",
    b"1e400 seconds",
    b"P99999999999999999999D",
    b"PT99999999999999999999S",
    // time / timestamp field walkers
    b"00:00:00",
    b"24:00:00",
    b"24:00:01",
    b"25:00:00",
    b"00:60:00",
    b"00:00:60",
    b"00:00:61",
    b"::",
    b":",
    b"1:",
    b":1",
    b"1:2:3:4",
    b"0:0:0.9999999999",
    b"12:34:56.789012345678901234567890",
    b"2000-01-01",
    b"2000-13-01",
    b"2000-01-32",
    b"2000-00-00",
    b"0000-00-00",
    b"294277-01-01",
    b"-4714-11-23 BC",
    b"2000-01-01 00:00:00+99",
    b"2000-01-01 00:00:00+99:99",
    b"2000-01-01T00:00:00",
    b"infinity",
    b"-infinity",
    b"epoch",
    b"now",
    b"2000-01-01 00:00:00 XYZZY",
];

/// Contrib bespoke parsers already implicated in phase-1: `ltree`/`lquery`,
/// `hstore`, `cube`, `seg`, `isn`.
pub const CONTRIB_TEXT: &[&[u8]] = &[
    // ltree / lquery
    b"a.b.c",
    b"",
    b".",
    b"..",
    b"a..b",
    b".a",
    b"a.",
    b"a.b.",
    b"*.a",
    b"*{1,2}",
    b"*{2,1}", // backwards repeat bounds
    b"*{-1}",
    b"*{4294967296}",
    b"*{99999999999999999999}",
    b"*{,}",
    b"*{}",
    b"a@",
    b"a%",
    b"!a",
    b"!",
    b"a|",
    b"|a",
    b"a|b|",
    b"a&b",
    b"(a)",
    b"a{",
    // hstore
    b"a=>b",
    b"a=>",
    b"=>b",
    b"=>",
    b"a",
    b"a=>b,",
    b",a=>b",
    b"a=>b,,c=>d",
    br#""a"=>"b""#,
    br#""a=>b"#,
    br#""a"=>"#,
    br#""a"=>"b"x"#,
    b"a=>b=>c",
    // cube / seg
    b"(1,2),(3,4)",
    b"(1,2,3)",
    b"()",
    b"(",
    b")",
    b"(,)",
    b"(1,2),(3)", // dimension mismatch
    b"1..2",
    b"1 .. 2",
    b"..2",
    b"1..",
    b"..",
    b"1..2..3",
    b">1",
    b"<1",
    b"~1",
    b"1e400..1e400",
    // isn / ean13
    b"978-0-393-04002-9",
    b"9780393040029",
    b"978-0-393-04002-X",
    b"978",
    b"",
    b"----------",
    b"9780393040029999999999",
];

// ===========================================================================
// 2. RUNTIME BANKS — embedded NUL, huge, deep nesting.
// ===========================================================================

/// EMBEDDED-NUL forms. A cstring cannot carry an interior NUL, so BOTH sides
/// must truncate at the first NUL and see the same string. A Rust parser
/// working over `&[u8]` that forgets to truncate reads PAST what C read — the
/// divergence these seeds detect. Every family gets a NUL planted at a
/// grammar-significant position (mid-field, on a delimiter, after the closer).
pub fn embedded_nul_texts() -> Vec<Vec<u8>> {
    let bases: &[&[u8]] = &[
        b"(0,0)",
        b"(1,2)",
        b"[(1,2),(3,4)]",
        b"1.2.3.4/24",
        b"[1,2]",
        b"{1,2,3}",
        b"\\x00",
        b"a.b.c",
        b"a=>b",
        b"1 day",
        b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    ];
    let mut out = Vec::new();
    for b in bases {
        // NUL at the very front — everything after is invisible to C.
        let mut v = vec![0u8];
        v.extend_from_slice(b);
        out.push(v);
        // NUL after the first byte, in the middle, and just before the end.
        for cut in [1usize, b.len() / 2, b.len().saturating_sub(1)] {
            if cut == 0 || cut >= b.len() {
                continue;
            }
            let mut v = b[..cut].to_vec();
            v.push(0);
            v.extend_from_slice(&b[cut..]);
            out.push(v);
        }
        // NUL appended (the benign case — both sides see the full literal).
        let mut v = b.to_vec();
        v.push(0);
        out.push(v);
        // NUL then trailing garbage after a COMPLETE literal: C sees a valid
        // parse, a non-truncating reader sees trailing junk and rejects. This
        // is the inverse (pgrust-rejects-where-C-accepts, MED) detector.
        let mut v = b.to_vec();
        v.extend_from_slice(b"\0garbage");
        out.push(v);
    }
    out
}

/// HUGE / DEEPLY NESTED parser inputs — recursion depth for the recursive
/// descent parsers (`array_in`, `record_in`, `range_in`, `json_in`) and
/// length-extreme coordinate lists for the geometric walkers.
///
/// Sizes are chosen to be adversarial but to keep the sweep affordable: a
/// depth of 4096 comfortably exceeds every hand-written depth guard while
/// staying far below the point where the C oracle's own stack-depth check
/// would dominate the runtime.
pub fn huge_parser_texts() -> Vec<Vec<u8>> {
    let mut out = Vec::new();

    // Deep array nesting: {{{{...1...}}}}
    for depth in [8usize, 64, 1024, 4096] {
        let mut v = vec![b'{'; depth];
        v.push(b'1');
        v.extend(std::iter::repeat(b'}').take(depth));
        out.push(v);
        // UNBALANCED at depth — closers one short.
        let mut v = vec![b'{'; depth];
        v.push(b'1');
        v.extend(std::iter::repeat(b'}').take(depth - 1));
        out.push(v);
        // Openers only — the run-off-the-end shape.
        out.push(vec![b'{'; depth]);
        // Closers only.
        out.push(vec![b'}'; depth]);
    }

    // Deep record/geo paren nesting.
    for depth in [64usize, 4096] {
        let mut v = vec![b'('; depth];
        v.extend(std::iter::repeat(b')').take(depth));
        out.push(v);
        out.push(vec![b'('; depth]);
        out.push(vec![b')'; depth]);
    }

    // Deep range nesting (the recursive bound parser).
    for depth in [64usize, 1024] {
        let mut v = vec![b'['; depth];
        v.extend_from_slice(b"1,2");
        v.extend(std::iter::repeat(b']').take(depth));
        out.push(v);
    }

    // Long polygon: a 4096-point coordinate list, and the same list with the
    // closing bracket removed (unterminated long scan).
    {
        let mut v = Vec::with_capacity(4096 * 8);
        v.push(b'(');
        for i in 0..4096 {
            if i > 0 {
                v.push(b',');
            }
            v.extend_from_slice(b"(1,2)");
        }
        v.push(b')');
        out.push(v.clone());
        v.pop();
        out.push(v);
    }

    // Huge single coordinate / field: a 65536-digit number inside a tid and a
    // point — the number-accumulate loop's overflow path over a long run.
    {
        let digits = vec![b'9'; 65_536];
        let mut v = Vec::with_capacity(digits.len() * 2 + 4);
        v.push(b'(');
        v.extend_from_slice(&digits);
        v.push(b',');
        v.extend_from_slice(&digits);
        v.push(b')');
        out.push(v);
    }

    // Huge inet masklen digit-run, and a huge unquoted array element.
    {
        let mut v = b"1.2.3.4/".to_vec();
        v.extend(std::iter::repeat(b'9').take(65_536));
        out.push(v);
    }
    {
        let mut v = vec![b'{'];
        v.extend(std::iter::repeat(b'a').take(65_536));
        v.push(b'}');
        out.push(v);
        // Many empty elements — the zero-length-element loop.
        let mut v = vec![b'{'];
        v.extend(std::iter::repeat(b',').take(16_384));
        v.push(b'}');
        out.push(v);
    }

    // Unterminated quote spanning a long buffer (the quote state machine).
    {
        let mut v = br#"{""#.to_vec();
        v.extend(std::iter::repeat(b'a').take(65_536));
        out.push(v);
        // Trailing lone backslash after a long run — escape reads one past.
        let mut v = vec![b'{'];
        v.extend(std::iter::repeat(b'a').take(65_536));
        v.push(b'\\');
        out.push(v);
    }

    out
}

/// The full parser corpus: every per-type grammar bank, the generic edge bank
/// from [`crate::edge`] (reused wholesale — the numeric boundary forms are
/// still meaningful inside a coordinate slot), the embedded-NUL forms, and the
/// huge/deep bank.
pub fn parser_payloads() -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for bank in [
        TID_TEXT,
        GEO_TEXT,
        NET_TEXT,
        RANGE_TEXT,
        ARRAY_TEXT,
        RECORD_TEXT,
        BINTEXT_TEXT,
        TEMPORAL_TEXT,
        CONTRIB_TEXT,
    ] {
        out.extend(bank.iter().map(|s| s.to_vec()));
    }
    // REUSE the EDGE bank: generic INT_MIN/MAX, overflow-string and empty
    // forms land in every coordinate/field slot too.
    out.extend(
        crate::edge::all_edge_texts()
            .into_iter()
            .map(|s| s.to_vec()),
    );
    out.extend(embedded_nul_texts());
    out.sort();
    out.dedup();
    out
}

/// [`parser_payloads`] plus the allocating huge/deep bank. Kept separate so a
/// cheap smoke sweep can skip the length-extreme cases.
pub fn parser_payloads_with_huge() -> Vec<Vec<u8>> {
    let mut out = parser_payloads();
    out.extend(huge_parser_texts());
    out
}

// ===========================================================================
// 3. DRIVER TABLE + SEED SHAPING.
// ===========================================================================

/// One target parser driver: its crate-root entry point, the selectors whose
/// arms are TEXT-input parses, and the HEADER BYTES those arms consume before
/// they reach the literal.
///
/// Getting the header exactly right is the difference between a real case and
/// a vacuous one. The drivers do not share a layout: `hstorefam_diff`'s
/// in/out arm reads a flag byte AND a u64 seed before the text, `geo_io_diff`
/// reads a mode byte, `contriba_diff`'s isn arm reads a type byte, a flag byte
/// and then a LENGTH byte that bounds the literal. A header that is one byte
/// short feeds the parser a literal starting at the wrong offset — the case
/// still "runs", still reports no finding, and tests nothing.
pub struct ParserDriver {
    /// Label used in findings and the witness table.
    pub name: &'static str,
    /// The driver entry point.
    pub drive: fn(&[u8]),
    /// Selector bytes that select a hand-rolled TEXT `*_in` arm.
    pub text_selectors: &'static [u8],
    /// Header byte sequences consumed between the selector and the literal.
    /// One seed is emitted per prefix, so a driver can sweep its flag/mode
    /// combinations. `NOPRE` means the literal follows the selector directly.
    pub prefixes: &'static [&'static [u8]],
    /// When `Some(cap)`, the arm reads a LENGTH BYTE before the literal and
    /// takes that many bytes: emit `[len]` then the literal, truncated to
    /// `cap`. (`contriba_diff`'s isn arm: `n = r.u8() % 40`.)
    pub len_prefixed: Option<usize>,
    /// Human note: which hand-rolled `*_in` functions this arm set reaches.
    pub covers: &'static str,
    /// Optional ENVIRONMENT PROBE — see [`typcache_env_free`].
    pub env_probe: Option<fn() -> bool>,
}

impl ParserDriver {
    /// Shape one seed: `[sel] ++ prefix ++ [len?] ++ text`.
    pub fn seed(&self, sel: u8, prefix: &[u8], text: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(2 + prefix.len() + text.len());
        v.push(sel);
        v.extend_from_slice(prefix);
        match self.len_prefixed {
            Some(cap) => {
                let n = text.len().min(cap);
                v.push(n as u8);
                v.extend_from_slice(&text[..n]);
            }
            None => v.extend_from_slice(text),
        }
        v
    }

    /// All seeds for this driver over `payloads`.
    pub fn seeds(&self, payloads: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for &sel in self.text_selectors {
            for pre in self.prefixes {
                for p in payloads {
                    out.push(self.seed(sel, pre, p));
                }
            }
        }
        out
    }
}

/// THE TYPCACHE-OWNER PROBE.
///
/// `rowtypes_diff`, `rangetypes_diff` and `multirangetypes_diff` each install
/// their own pinned typcache/fmgr fixture into PROCESS-GLOBAL seams, and each
/// one's entry guard reads "if someone already installed, return" — a
/// pre-existing, documented harness convention ("whichever module owns the env
/// wins; fuzz binaries are one-target-per-process and unaffected", see
/// `rangetypes_diff.rs` and `rowtypes_diff::install`).
///
/// Consequence for THIS campaign: at most ONE of the three can ever run in a
/// given process. That is not something the bank can fix; what it must not do
/// is let the two losers report a silent green. So the probe is evaluated
/// IMMEDIATELY BEFORE each driver runs (not up-front — driving the winner
/// flips the answer for the rest), and the losers are reported as CARVED.
/// [`parser_drivers_ordered`] lets a filtered, single-owner run put any chosen
/// owner first, so all three are covered across three invocations.
pub fn typcache_env_free() -> bool {
    !(syscache_seams::pg_type_io_shape::is_installed()
        || syscache_seams::lookup_pg_type_typcache_shape::is_installed()
        || fmgr_seams::fmgr_info::is_installed())
}

/// The three drivers that contend for the typcache/fmgr seams.
pub const TYPCACHE_OWNERS: &[&str] = &["rowtypes_diff", "rangetypes_diff", "multirangetypes_diff"];

/// "No header bytes": the literal follows the selector directly.
const NOPRE: &[&[u8]] = &[&[]];

/// THE TARGET TABLE — every vendored hand-rolled text `*_in` parser reachable
/// from an existing `*_diff` driver.
///
/// NOT VENDORED (no C oracle in `csrc/`, so no differential is possible here —
/// flagged for a vendoring lane, see `findings-parser.md`):
///   * `bit_in` / `varbit_in` (utils/adt/varbit.c). The shipped Rust crate
///     `crates/backend/utils/adt/varbit` EXISTS, but `csrc/` carries no
///     `pg_varbit_io.c`, so its hand-rolled `B`/`X` prefix scan and
///     per-character bit-accumulate loop have NO differential oracle. The
///     `BINTEXT_TEXT` bank already carries the literals; only the oracle is
///     missing.
pub fn parser_drivers() -> Vec<ParserDriver> {
    vec![
        // rowtypes FIRST: record_in's driver installs process-global syscache
        // and fmgr seams and refuses every seed if a sibling module got there
        // first, so it must have the first claim on the environment.
        ParserDriver {
            name: "rowtypes_diff",
            drive: crate::rowtypes_diff,
            // [sel][flags][text]; flags bits 0-2 = descriptor, bit 3 = soft.
            text_selectors: &[0],
            prefixes: &[&[0], &[1], &[2], &[4], &[5], &[8]],
            len_prefixed: None,
            covers: "record_in (field walker, column-count check, quote/escape)",
            env_probe: Some(typcache_env_free),
        },
        ParserDriver {
            name: "scalarxid_diff",
            drive: crate::scalarxid_diff,
            // [sel][text]. 0 tidin, 31 oidin, 33 oidvectorin, 35 xidin, 38 xid8in.
            text_selectors: &[0, 31, 33, 35, 38],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "tidin (block u32 / offset u16), oidin, oidvectorin, xidin, xid8in",
            env_probe: None,
        },
        ParserDriver {
            name: "geo_io_diff",
            drive: crate::geo_io_diff,
            // [sel][mode][text]; sel % 7 = type, mode bit0 = 0 -> text-in.
            text_selectors: &[0, 1, 2, 3, 4, 5, 6],
            prefixes: &[&[0], &[2]],
            len_prefixed: None,
            covers: "point/box/lseg/line/path/poly/circle _in (pair_decode/path_decode)",
            env_probe: None,
        },
        ParserDriver {
            name: "network_diff",
            drive: crate::network_diff,
            text_selectors: &[0, 1],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "inet_in / cidr_in (octet accumulate + /masklen scan)",
            env_probe: None,
        },
        ParserDriver {
            name: "mac_diff",
            drive: crate::mac_diff,
            text_selectors: &[0, 1],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "macaddr_in / macaddr8_in",
            env_probe: None,
        },
        ParserDriver {
            name: "rangetypes_diff",
            drive: crate::rangetypes_diff,
            // [sel][typ][text]; sel 0 = text io, typ %3 = int4/int8/numrange.
            text_selectors: &[0],
            prefixes: &[&[0], &[1], &[2]],
            len_prefixed: None,
            covers: "range_in (bound quote/escape walker, nested element parse)",
            env_probe: Some(typcache_env_free),
        },
        ParserDriver {
            name: "multirangetypes_diff",
            drive: crate::multirangetypes_diff,
            text_selectors: &[0],
            prefixes: &[&[0], &[1], &[2]],
            len_prefixed: None,
            covers: "multirange_in (recursive range_in over a brace list)",
            env_probe: Some(typcache_env_free),
        },
        ParserDriver {
            name: "arrayfuncs_diff",
            drive: crate::arrayfuncs_diff,
            // [sel][esel][text]; sel 0 = array_in, esel 0 int4 / 1 text.
            text_selectors: &[0],
            prefixes: &[&[0], &[1]],
            len_prefixed: None,
            covers: "array_in ([lb:ub] decorations, braces, quote/escape, dims)",
            env_probe: None,
        },
        ParserDriver {
            name: "vlbytea_diff",
            drive: crate::vlbytea_diff,
            text_selectors: &[0],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "byteain (hex odd-length + escape octal)",
            env_probe: None,
        },
        ParserDriver {
            name: "uuid_diff",
            drive: crate::uuid_diff,
            text_selectors: &[0],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "uuid_in (hyphen-position state machine)",
            env_probe: None,
        },
        ParserDriver {
            // [sel=0][istyle][range][text] — DecodeInterval takes TWO header
            // bytes, unlike its ISO-8601 sibling below.
            name: "interval_engine_diff/decode",
            drive: crate::interval_engine_diff::interval_engine_diff,
            text_selectors: &[0],
            prefixes: &[&[0, 0], &[1, 0], &[2, 0], &[3, 0]],
            len_prefixed: None,
            covers: "DecodeInterval (unit words, field overflow, all 4 IntervalStyles)",
            env_probe: None,
        },
        ParserDriver {
            // [sel=1][text] — no header byte.
            name: "interval_engine_diff/iso8601",
            drive: crate::interval_engine_diff::interval_engine_diff,
            text_selectors: &[1],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "DecodeISO8601Interval (P/T designators, field overflow)",
            env_probe: None,
        },
        ParserDriver {
            name: "datetime_io_diff",
            drive: crate::datetime_io_diff,
            // [sel][text]; 0 date_in, 2 time_in, 4 timetz_in (odd sels are _out).
            text_selectors: &[0, 2, 4],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "date_in / time_in / timetz_in (ParseDateTime field walkers)",
            env_probe: None,
        },
        ParserDriver {
            name: "timestamp_diff",
            drive: crate::timestamp_diff,
            // [sel][text]; 0 timestamp_in, 2 interval_in (the SQL entry point).
            text_selectors: &[0, 2],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "timestamp_in / interval_in (SQL entry points)",
            env_probe: None,
        },
        ParserDriver {
            name: "ltree_diff",
            drive: crate::ltree_diff,
            // [sel][flags][text]; flags bit0 = soft-error mode.
            text_selectors: &[0, 1, 2],
            prefixes: &[&[0], &[1]],
            len_prefixed: None,
            covers: "ltree_in / lquery_in / ltxtquery_in (bug-103a's parser family)",
            env_probe: None,
        },
        ParserDriver {
            name: "hstorefam_diff",
            drive: crate::hstorefam_diff,
            // [sel][flags][u64 seed][text] — NINE header bytes before the text.
            text_selectors: &[0],
            prefixes: &[&[0, 0, 0, 0, 0, 0, 0, 0, 0], &[1, 0, 0, 0, 0, 0, 0, 0, 0]],
            len_prefixed: None,
            covers: "hstore_in (=> scan, quote/escape)",
            env_probe: None,
        },
        ParserDriver {
            name: "contribb_diff",
            drive: crate::contribb_diff,
            // [sel][text]; 0 seg_in, 4 cube_in.
            text_selectors: &[0, 4],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "seg_in / cube_in (flex/bison text parse)",
            env_probe: None,
        },
        ParserDriver {
            name: "contriba_diff",
            drive: crate::contriba_diff,
            // [sel=5][isn type][flags][len][text]; the arm reads n = u8 % 40.
            text_selectors: &[5],
            prefixes: &[&[0, 0], &[1, 0], &[2, 0], &[3, 1]],
            len_prefixed: Some(39),
            covers: "isn / ean13_in (hyphen-position + check-digit walker)",
            env_probe: None,
        },
        ParserDriver {
            name: "json_diff",
            drive: crate::json_diff,
            text_selectors: &[0, 4],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "json_in / json_validate (lexer + parser depth)",
            env_probe: None,
        },
        ParserDriver {
            name: "jsonbio_diff",
            drive: crate::jsonbio_diff,
            text_selectors: &[0],
            prefixes: NOPRE,
            len_prefixed: None,
            covers: "jsonb_in (lexer + parser depth)",
            env_probe: None,
        },
    ]
}

/// [`parser_drivers`] with `first` moved to the front. The typcache owners
/// (see [`TYPCACHE_OWNERS`]) can only be covered one per process, so a full
/// campaign runs this once per owner in three separate `cargo test`
/// invocations and unions the results.
pub fn parser_drivers_ordered(first: &str) -> Vec<ParserDriver> {
    let mut ds = parser_drivers();
    if let Some(i) = ds.iter().position(|d| d.name == first) {
        let d = ds.remove(i);
        ds.insert(0, d);
    }
    // DROP the rival typcache owners. They could only ever be carved, and
    // leaving them in is actively harmful: with rangetypes first, rowtypes
    // still passes its own install() guard (it checks a different seam set)
    // and then PANICS with "seam installed twice: detoast_seams::detoast_attr"
    // — a rig artifact that would otherwise land in the ledger every run.
    if TYPCACHE_OWNERS.contains(&first) {
        ds.retain(|d| d.name == first || !TYPCACHE_OWNERS.contains(&d.name));
    }
    ds
}

// ===========================================================================
// 4. THE CAMPAIGN.
// ===========================================================================

/// Run `f` on a thread with a BACKEND-SIZED stack.
///
/// THE TRAP THIS EXISTS FOR (found by this lane, see `findings-parser.md`):
/// the deep-nesting bank hard-ABORTED the process with "has overflowed its
/// stack" on `ltxtquery_in` over a 4096-deep paren nest. That is NOT a product
/// bug and NOT catchable by `catch_unwind`. Both sides guard recursion with
/// `check_stack_depth()` at max_stack_depth = 2048 kB — but libtest runs tests
/// on a spawned thread whose stack is ALSO ~2 MiB, so the hardware stack ends
/// at exactly the depth the guard is waiting for and the process dies before
/// the clean 54001 can be raised. A real backend has an 8 MB stack behind the
/// same 2048 kB guard, so the guard fires with room to spare.
///
/// Giving the sweep a 16 MiB stack reproduces the backend's headroom, and the
/// entire deep bank then runs to completion with zero divergences. Baked in
/// here rather than left to `RUST_MIN_STACK` so the gate cannot go flaky
/// depending on how it was invoked.
pub fn with_backend_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(f)
        .expect("spawn parser-campaign thread")
        .join()
        .expect("parser-campaign thread panicked")
}

/// A captured divergence out of the parser sweep.
#[derive(Debug, Clone)]
pub struct ParserFinding {
    pub driver: &'static str,
    pub seed: Vec<u8>,
    pub message: String,
    /// True when the panic message looks like a RUST-SIDE CRASH (index out of
    /// bounds / slice range / unwrap / arithmetic overflow) rather than a
    /// comparator's own mismatch assertion. This is the HIGH-severity class:
    /// pgrust crashing where C returns a clean parse error.
    pub rust_crash: bool,
    /// True when this is the RIG's own limit rather than a product finding —
    /// see [`is_harness_artifact`].
    pub harness_artifact: bool,
}

impl ParserFinding {
    /// Ledger severity: HIGH = pgrust crash, MED = product accept/reject or
    /// value divergence, INFO = harness artifact (not a product finding).
    pub fn severity(&self) -> &'static str {
        if self.harness_artifact {
            "INFO"
        } else if self.rust_crash {
            "HIGH"
        } else {
            "MED"
        }
    }
}

/// HARNESS ARTIFACTS: panics that are neither a pgrust crash nor a real
/// accept/reject divergence, but the differential RIG hitting its own limit.
/// Both observed in this lane's full sweep:
///   * "C harness internal failure" — the C oracle's staging buffer refusing a
///     100k-digit literal. The oracle declined to answer; there is no verdict
///     to compare, so this is a CAPACITY CARVE, not a divergence.
///   * "seam installed twice" — two `*_diff` modules contending for the same
///     process-global seam (see `typcache_env_free`).
/// Reporting these as product findings would poison the ledger, and silently
/// dropping them would hide real rig breakage, so they get their own class.
pub fn is_harness_artifact(msg: &str) -> bool {
    const ARTIFACT_MARKERS: &[&str] = &[
        "C harness internal failure",
        "seam installed twice",
        "seams owned by a sibling",
        "driver refusal",
    ];
    ARTIFACT_MARKERS.iter().any(|m| msg.contains(m))
}

/// Classify a panic message: is this a raw Rust crash (HIGH) or the
/// comparator reporting a value/verdict mismatch (MED)?
pub fn is_rust_crash(msg: &str) -> bool {
    const CRASH_MARKERS: &[&str] = &[
        "index out of bounds",
        "slice index starts at",
        "range end index",
        "range start index",
        "byte index",
        "attempt to add with overflow",
        "attempt to subtract with overflow",
        "attempt to multiply with overflow",
        "attempt to negate with overflow",
        "attempt to divide by zero",
        "called `Option::unwrap()` on a `None` value",
        "called `Result::unwrap()` on an `Err` value",
        "capacity overflow",
        "memory allocation of",
        "unreachable",
        "not yet implemented",
        "internal error: entered unreachable code",
    ];
    CRASH_MARKERS.iter().any(|m| msg.contains(m))
}

/// Run the parser campaign over the given payload corpus.
///
/// Returns `(cases_run, findings)`. Panics are captured rather than
/// propagated, but — exactly as in [`crate::edge::run_campaign`] — a driver's
/// sweep STOPS at its first finding: the drivers serialize through a
/// process-global C-oracle mutex, and continuing past a genuine panic can
/// poison it and fabricate a cascade of phantom findings. An honest ledger of
/// one real finding per driver beats a long list of artifacts.
pub fn run_parser_campaign_with(payloads: &[Vec<u8>]) -> (usize, Vec<ParserFinding>) {
    let (cases, findings, _) = run_parser_campaign_timed(payloads);
    (cases, findings)
}

/// Per-driver execution witness: `(driver, cases, elapsed)`. A driver that
/// burns ~no time over thousands of cases is REFUSING them at its entry guard
/// (wrong selector, payload rejected by its `text_payload` gate, seed shaped
/// for the wrong layout) — i.e. the sweep is VACUOUS for that driver and its
/// green means nothing. [`assert_not_vacuous`] turns that into a gate.
#[derive(Debug, Clone)]
pub struct DriverWitness {
    pub driver: &'static str,
    pub cases: usize,
    pub elapsed: std::time::Duration,
    /// False when the driver's [`ParserDriver::env_probe`] reported its
    /// process-global environment was already owned by a sibling module, so
    /// the driver refuses every seed BY DESIGN. Such a driver's "no findings"
    /// is a carve, never a pass — see [`carved_drivers`].
    pub env_available: bool,
}

impl DriverWitness {
    /// Nanoseconds of real work per case.
    pub fn ns_per_case(&self) -> u128 {
        if self.cases == 0 {
            0
        } else {
            self.elapsed.as_nanos() / self.cases as u128
        }
    }
}

/// As [`run_parser_campaign_with`], additionally returning the per-driver
/// execution witness.
pub fn run_parser_campaign_timed(
    payloads: &[Vec<u8>],
) -> (usize, Vec<ParserFinding>, Vec<DriverWitness>) {
    run_parser_campaign_drivers(payloads, parser_drivers())
}

/// As [`run_parser_campaign_timed`] over an explicit driver list — used with
/// [`parser_drivers_ordered`] to give a chosen typcache owner the first claim.
pub fn run_parser_campaign_drivers(
    payloads: &[Vec<u8>],
    drivers: Vec<ParserDriver>,
) -> (usize, Vec<ParserFinding>, Vec<DriverWitness>) {
    let mut cases = 0usize;
    let mut findings = Vec::new();
    let mut witness = Vec::new();

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    for d in drivers {
        // Probe JUST BEFORE driving: whichever typcache owner runs first wins
        // the seams, and driving it flips the answer for its rivals. Probing
        // up-front would report all three as available and hide the carve.
        let env_available = d.env_probe.map(|p| p()).unwrap_or(true);
        let seeds = d.seeds(payloads);
        let n = seeds.len();
        let t0 = std::time::Instant::now();
        for seed in seeds {
            cases += 1;
            let s = seed.clone();
            let drive = d.drive;
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                drive(&s);
            }));
            if let Err(payload) = res {
                let message = panic_message(&payload);
                let artifact = is_harness_artifact(&message);
                // Record each distinct artifact ONCE per driver and keep
                // sweeping: a rig limit (an oversized literal the C oracle
                // declines) says nothing about the parser and must not cost
                // the driver its remaining thousands of real cases. A genuine
                // finding still stops the driver — the C oracle mutex may be
                // poisoned, and a cascade of phantom findings is worse than
                // one honest one.
                let dup = artifact
                    && findings
                        .iter()
                        .any(|f: &ParserFinding| f.driver == d.name && f.message == message);
                if !dup {
                    findings.push(ParserFinding {
                        driver: d.name,
                        seed,
                        rust_crash: is_rust_crash(&message),
                        harness_artifact: artifact,
                        message,
                    });
                }
                if !artifact {
                    break;
                }
            }
        }
        witness.push(DriverWitness {
            driver: d.name,
            cases: n,
            elapsed: t0.elapsed(),
            env_available,
        });
    }

    std::panic::set_hook(prev);
    (cases, findings, witness)
}

/// The ANTI-VACUITY GATE. Every driver in the table must spend real time per
/// case; a driver under `floor_ns` is refusing its seeds at the entry guard,
/// which makes its "no findings" result meaningless. Returns the offending
/// driver names.
///
/// The floor is deliberately low (a debug-build FFI round trip plus a
/// MemoryContext is microseconds; an early `return` is nanoseconds), so this
/// separates "ran" from "refused" without being a performance assertion.
pub fn vacuous_drivers(witness: &[DriverWitness], floor_ns: u128) -> Vec<&'static str> {
    witness
        .iter()
        .filter(|w| w.env_available && w.cases > 0 && w.ns_per_case() < floor_ns)
        .map(|w| w.driver)
        .collect()
}

/// Drivers that did not run because a sibling `*_diff` module owned their
/// process-global environment. Reported separately from [`vacuous_drivers`]:
/// this is a KNOWN, EXPLAINED refusal, not an undetected hole — but it is
/// still an absence of coverage and must never read as a clean green.
pub fn carved_drivers(witness: &[DriverWitness]) -> Vec<&'static str> {
    witness
        .iter()
        .filter(|w| !w.env_available)
        .map(|w| w.driver)
        .collect()
}

/// The full campaign: every driver x the full corpus INCLUDING the huge/deep
/// bank.
pub fn run_parser_campaign() -> (usize, Vec<ParserFinding>, Vec<DriverWitness>) {
    run_parser_campaign_timed(&parser_payloads_with_huge())
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Render a findings ledger (the `findings-parser.md` body) from a sweep.
pub fn format_findings(cases: usize, findings: &[ParserFinding]) -> String {
    let mut s = String::new();
    let (high, med, info) = (
        findings.iter().filter(|f| f.severity() == "HIGH").count(),
        findings.iter().filter(|f| f.severity() == "MED").count(),
        findings.iter().filter(|f| f.severity() == "INFO").count(),
    );
    s.push_str(&format!(
        "parser campaign: {cases} cases, {} finding(s) — {high} HIGH, {med} MED, \
         {info} INFO (harness artifacts, not product findings)\n",
        findings.len()
    ));
    for f in findings {
        // Seeds can be 100kB; the head is what identifies the case.
        let head = &f.seed[..f.seed.len().min(96)];
        s.push_str(&format!(
            "[{}] driver={} seedlen={} seedhead={:?} :: {}\n",
            f.severity(),
            f.driver,
            f.seed.len(),
            String::from_utf8_lossy(head),
            f.message
        ));
    }
    s
}

// ===========================================================================
// 5. TESTS.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Bank sanity — always green. This is the CI gate: it asserts the bank
    /// actually carries the type-specific boundary forms a generic integer
    /// bank cannot express, so the corpus can never silently degrade into
    /// "generic ints sprayed at a parser".
    #[test]
    fn parser_bank_sanity() {
        // tid: the uint32 block / uint16 offset MAX+1 points, in tid SYNTAX.
        assert!(TID_TEXT.contains(&b"(4294967296,0)".as_slice()));
        assert!(TID_TEXT.contains(&b"(0,65536)".as_slice()));
        assert!(TID_TEXT.contains(&b"(4294967295,65535)".as_slice()));
        // tid: empty / missing-field / unbalanced-paren shapes.
        assert!(TID_TEXT.contains(&b"()".as_slice()));
        assert!(TID_TEXT.contains(&b"( , )".as_slice()));
        assert!(TID_TEXT.contains(&b"(0,0".as_slice()));

        // geo: the unbalanced-bracket and degenerate-count shapes.
        assert!(GEO_TEXT.contains(&b"[(".as_slice()));
        assert!(GEO_TEXT.contains(&b"((1,2))".as_slice())); // 1-point polygon
        assert!(GEO_TEXT.contains(&b"(NaN,NaN)".as_slice()));
        assert!(GEO_TEXT.contains(&b"(1e400,1e400)".as_slice()));

        // net: the out-of-range masks on both address families.
        assert!(NET_TEXT.contains(&b"1.2.3.4/-1".as_slice()));
        assert!(NET_TEXT.contains(&b"1.2.3.4/33".as_slice()));
        assert!(NET_TEXT.contains(&b"::1/129".as_slice()));
        assert!(NET_TEXT.contains(&b"256.1.1.1".as_slice()));

        // range: empty-vs-`empty`, backwards bounds, unbalanced.
        assert!(RANGE_TEXT.contains(&b"empty".as_slice()));
        assert!(RANGE_TEXT.contains(&b"[2,1]".as_slice()));
        assert!(RANGE_TEXT.contains(&b"[1,2".as_slice()));

        // array: the [lb:ub] overflow decorations.
        assert!(ARRAY_TEXT.contains(&b"[5:1]={1}".as_slice()));
        assert!(ARRAY_TEXT.contains(&b"[1:2147483648]={1}".as_slice()));
        assert!(ARRAY_TEXT.contains(&b"{{1,2},{3}}".as_slice())); // jagged

        // bytea: the odd-length hex case.
        assert!(BINTEXT_TEXT.contains(&b"\\x0".as_slice()));

        // Embedded NUL bank plants NULs at grammar positions and always
        // includes the "valid literal, NUL, then garbage" inverse detector.
        let nuls = embedded_nul_texts();
        assert!(nuls.iter().any(|v| v.contains(&0)));
        assert!(nuls
            .iter()
            .any(|v| v.starts_with(b"(0,0)") && v.ends_with(b"garbage")));
        assert!(nuls.iter().any(|v| v[0] == 0));

        // Huge bank reaches real depth and real length.
        let huge = huge_parser_texts();
        assert!(huge.iter().any(|v| v.len() >= 65_536));
        assert!(huge
            .iter()
            .any(|v| v.iter().filter(|&&c| c == b'{').count() >= 4096));

        // The corpus is substantial and deduped.
        let p = parser_payloads();
        assert!(p.len() > 400, "parser corpus too small: {}", p.len());
        let mut sorted = p.clone();
        sorted.dedup();
        assert_eq!(sorted.len(), p.len(), "corpus not deduped");
    }

    /// Seed shaping — the anti-vacuity gate. A seed whose header is the wrong
    /// length puts the literal at an offset the parser never reads, and the
    /// case tests nothing. Assert the shapes explicitly.
    #[test]
    fn parser_seed_shaping() {
        let ds = parser_drivers();

        // tidin is selector 0 of scalarxid with NO header byte: the literal
        // must start at index 1.
        let sx = ds.iter().find(|d| d.name == "scalarxid_diff").unwrap();
        let s = sx.seed(0, &[], b"(4294967296,0)");
        assert_eq!(s[0], 0);
        assert_eq!(&s[1..], b"(4294967296,0)");

        // geo carries a mode byte: literal starts at index 2, and mode bit0
        // must be 0 (text-in) for every configured prefix.
        let geo = ds.iter().find(|d| d.name == "geo_io_diff").unwrap();
        for pre in geo.prefixes {
            assert_eq!(pre.len(), 1, "geo prefix must be exactly the mode byte");
            assert_eq!(pre[0] & 1, 0, "geo prefix {pre:?} is not a text-in mode");
        }
        let s = geo.seed(4, &[0], b"[(");
        assert_eq!(&s[..2], &[4, 0]);
        assert_eq!(&s[2..], b"[(");

        // hstore's in/out arm reads a flag byte AND a u64 seed: NINE header
        // bytes. An 8-byte header would silently feed the parser a literal
        // starting one byte late — the vacuity trap this test exists to pin.
        let hs = ds.iter().find(|d| d.name == "hstorefam_diff").unwrap();
        for pre in hs.prefixes {
            assert_eq!(pre.len(), 9, "hstore header must be flags + u64 seed");
        }
        let s = hs.seed(0, hs.prefixes[0], b"a=>b");
        assert_eq!(&s[10..], b"a=>b");

        // contriba's isn arm is LENGTH-PREFIXED and caps the literal at 39.
        let ca = ds.iter().find(|d| d.name == "contriba_diff").unwrap();
        assert_eq!(ca.len_prefixed, Some(39));
        let s = ca.seed(5, &[0, 0], b"978-0-393-04002-9");
        assert_eq!(s[0], 5);
        assert_eq!(s[3], 17, "length byte must equal the literal length");
        assert_eq!(&s[4..], b"978-0-393-04002-9");
        let long = vec![b'9'; 100];
        let s = ca.seed(5, &[0, 0], &long);
        assert_eq!(s[3], 39, "literal must be truncated to the arm's cap");
        assert_eq!(s.len(), 4 + 39);

        // Every driver produces selectors x prefixes x payloads seeds.
        let payloads = vec![b"a".to_vec(), b"bb".to_vec()];
        for d in &ds {
            assert!(!d.prefixes.is_empty(), "{} has no prefixes", d.name);
            assert_eq!(
                d.seeds(&payloads).len(),
                d.text_selectors.len() * d.prefixes.len() * payloads.len(),
                "{} seed count",
                d.name
            );
        }
    }

    /// The crash classifier must separate a pgrust panic (HIGH) from a
    /// comparator mismatch (MED) — the ledger's severity depends on it.
    #[test]
    fn crash_classifier() {
        assert!(is_rust_crash(
            "index out of bounds: the len is 0 but the index is 0"
        ));
        assert!(is_rust_crash(
            "range end index 5 out of range for slice of length 2"
        ));
        assert!(is_rust_crash("attempt to subtract with overflow"));
        assert!(is_rust_crash("called `Option::unwrap()` on a `None` value"));
        assert!(!is_rust_crash(
            "tidin verdict mismatch: rust=Ok c=Err input=\"(0,65536)\""
        ));
        assert!(!is_rust_crash("errcode class mismatch: rust=1 c=2"));

        // Harness artifacts are their own class: neither HIGH nor a product
        // finding. Both strings are verbatim from this lane's full sweep.
        assert!(is_harness_artifact(
            "C harness internal failure -2 (record_in)"
        ));
        assert!(is_harness_artifact(
            "seam installed twice: detoast_seams::detoast_attr"
        ));
        assert!(!is_harness_artifact("tidin DIVERGENCE input=\"(0,65536)\""));

        let mk = |m: &str| ParserFinding {
            driver: "d",
            seed: vec![],
            rust_crash: is_rust_crash(m),
            harness_artifact: is_harness_artifact(m),
            message: m.to_string(),
        };
        assert_eq!(mk("index out of bounds: len is 0").severity(), "HIGH");
        assert_eq!(mk("tidin DIVERGENCE").severity(), "MED");
        assert_eq!(mk("C harness internal failure -2").severity(), "INFO");
    }

    /// ACCEPT/REJECT BALANCE — the second anti-vacuity gate.
    ///
    /// A corpus that every parser rejects at the first character exercises
    /// only the error path, and a driver comparing "both rejected" is a very
    /// weak oracle. The bank must therefore contain well-formed anchors that
    /// the parser ACCEPTS, so the value-comparison plane runs too. Checked
    /// directly against the shipped `tidin`, the lane's archetype.
    #[test]
    fn bank_exercises_both_verdicts() {
        let accepted = TID_TEXT
            .iter()
            .filter(|t| adt_scalar::tidin(t).is_some())
            .count();
        let rejected = TID_TEXT.len() - accepted;
        assert!(
            accepted >= 4,
            "tid bank accepts only {accepted} literals — the value-comparison \
             plane is barely exercised"
        );
        assert!(rejected >= 20, "tid bank rejects only {rejected} literals");

        // The type-specific overflow points must land on the REJECT side, and
        // the MAX values on the ACCEPT side — if tidin ever accepted
        // (0,65536) this assert is the tripwire.
        assert!(adt_scalar::tidin(b"(4294967295,65535)").is_some());
        assert!(adt_scalar::tidin(b"(4294967296,0)").is_none());
        assert!(adt_scalar::tidin(b"(0,65536)").is_none());
    }

    /// CHEAP SMOKE SWEEP — runs in CI. Uses the non-allocating corpus over a
    /// short driver prefix so the gate is fast, but is a REAL differential
    /// sweep (thousands of cases through the vendored C oracle), not a
    /// structural no-op. Any finding fails the gate loudly.
    #[test]
    fn parser_campaign_smoke() {
        // A representative slice of the corpus: every bank's first entries
        // plus the whole tid bank (the archetype).
        let mut payloads: Vec<Vec<u8>> = TID_TEXT.iter().map(|s| s.to_vec()).collect();
        for bank in [GEO_TEXT, NET_TEXT, RANGE_TEXT, ARRAY_TEXT, RECORD_TEXT] {
            payloads.extend(bank.iter().take(24).map(|s| s.to_vec()));
        }
        payloads.extend(embedded_nul_texts().into_iter().take(16));

        let (cases, findings, witness) =
            with_backend_stack(move || run_parser_campaign_timed(&payloads));
        eprintln!("{}", format_findings(cases, &findings));
        for w in &witness {
            eprintln!(
                "  witness {:<24} cases={:<6} {:>8}ns/case",
                w.driver,
                w.cases,
                w.ns_per_case()
            );
        }
        assert!(cases > 3_000, "smoke sweep ran too few cases: {cases}");

        // ANTI-VACUITY: a fast green is only meaningful if every driver
        // actually executed its parser. See vacuous_drivers().
        let vac = vacuous_drivers(&witness, 300);
        let carved = carved_drivers(&witness);
        eprintln!("  CARVED (environment owned by a sibling module): {carved:?}");
        assert!(
            vac.is_empty(),
            "VACUOUS drivers (seeds refused at the entry guard, so their green \
             is meaningless): {vac:?}"
        );

        assert!(
            findings.is_empty(),
            "parser smoke sweep found divergences:\n{}",
            format_findings(cases, &findings)
        );
    }

    /// TRIAGE HARNESS for a hard abort (stack overflow / SIGSEGV), which
    /// `catch_unwind` cannot capture: drive the deep/huge bank ONE case at a
    /// time, flushing an identifying line BEFORE each, so the last line in the
    /// log names the culprit. Run:
    ///   PARSER_TRIAGE_DRIVER=geo_io_diff cargo test -p decoder_fuzz --lib -- \
    ///     parser::tests::parser_deep_triage --ignored --nocapture
    #[test]
    #[ignore = "triage tool for hard aborts; run explicitly with PARSER_TRIAGE_DRIVER"]
    fn parser_deep_triage() {
        use std::io::Write;
        let only = std::env::var("PARSER_TRIAGE_DRIVER").ok();
        let payloads = huge_parser_texts();
        for d in parser_drivers() {
            if let Some(o) = &only {
                if d.name != o.as_str() {
                    continue;
                }
            }
            for (pi, p) in payloads.iter().enumerate() {
                for &sel in d.text_selectors {
                    for pre in d.prefixes {
                        eprintln!(
                            "TRIAGE driver={} sel={} prefix={:?} payload#{} len={} head={:?}",
                            d.name,
                            sel,
                            pre,
                            pi,
                            p.len(),
                            String::from_utf8_lossy(&p[..p.len().min(24)])
                        );
                        let _ = std::io::stderr().flush();
                        let seed = d.seed(sel, pre, p);
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            (d.drive)(&seed);
                        }));
                    }
                }
            }
        }
        eprintln!("TRIAGE COMPLETE");
    }

    /// THE FULL CAMPAIGN — the heavy sweep (tens of thousands of cases,
    /// including the length-extreme and deep-nesting bank). `#[ignore]`d for
    /// the same reason as `edge_campaign_full`: it is expensive, and on a
    /// genuine divergence it is EXPECTED to surface findings rather than stay
    /// silent, so it is a hunting tool, not a land gate. Run explicitly:
    ///   cargo test -p decoder_fuzz parser_campaign_full -- --ignored --nocapture
    #[test]
    #[ignore = "heavy differential sweep; run explicitly to hunt parser divergences"]
    fn parser_campaign_full() {
        // PARSER_FIRST picks which typcache owner gets the seams this
        // process (see typcache_env_free). A complete campaign runs this test
        // once per name in TYPCACHE_OWNERS and unions the three reports.
        let first = std::env::var("PARSER_FIRST").unwrap_or_else(|_| "rowtypes_diff".into());
        eprintln!("parser campaign: typcache owner = {first}");
        let (cases, findings, witness) = with_backend_stack(move || {
            run_parser_campaign_drivers(
                &parser_payloads_with_huge(),
                parser_drivers_ordered(&first),
            )
        });
        eprintln!("{}", format_findings(cases, &findings));
        for w in &witness {
            eprintln!(
                "  witness {:<24} cases={:<6} {:>8}ns/case  total={:?}",
                w.driver,
                w.cases,
                w.ns_per_case(),
                w.elapsed
            );
        }
        eprintln!("VACUOUS: {:?}", vacuous_drivers(&witness, 300));
        eprintln!("CARVED:  {:?}", carved_drivers(&witness));
        assert!(cases > 20_000, "campaign ran too few cases: {cases}");
    }
}
