

pub const REG_OKAY: i32 = 0; /* no errors detected */
pub const REG_NOMATCH: i32 = 1; /* failed to match */
pub const REG_BADPAT: i32 = 2; /* invalid regexp */
pub const REG_ECOLLATE: i32 = 3; /* invalid collating element */
pub const REG_ECTYPE: i32 = 4; /* invalid character class */
pub const REG_EESCAPE: i32 = 5; /* invalid escape \ sequence */
pub const REG_ESUBREG: i32 = 6; /* invalid backreference number */
pub const REG_EBRACK: i32 = 7; /* brackets [] not balanced */
pub const REG_EPAREN: i32 = 8; /* parentheses () not balanced */
pub const REG_EBRACE: i32 = 9; /* braces {} not balanced */
pub const REG_BADBR: i32 = 10; /* invalid repetition count(s) */
pub const REG_ERANGE: i32 = 11; /* invalid character range */
pub const REG_ESPACE: i32 = 12; /* out of memory */
pub const REG_BADRPT: i32 = 13; /* quantifier operand invalid */
pub const REG_ASSERT: i32 = 15; /* "can't happen" -- you found a bug */
pub const REG_INVARG: i32 = 16; /* invalid argument to regex function */
pub const REG_MIXED: i32 = 17; /* character widths of regex and string differ */
pub const REG_BADOPT: i32 = 18; /* invalid embedded option */
pub const REG_ETOOBIG: i32 = 19; /* regular expression is too complex */
pub const REG_ECOLORS: i32 = 20; /* too many colors */

pub const REG_ATOI: i32 = 101; /* convert error-code name to number */
pub const REG_ITOA: i32 = 102; /* convert error-code number to name */

pub const REG_PREFIX: i32 = -1; /* identified a common prefix */
pub const REG_EXACT: i32 = -2; /* identified an exact match */


pub const REG_UBACKREF: i32 = 0o000001; /* has back-reference (\n) */
pub const REG_ULOOKAROUND: i32 = 0o000002; /* has lookahead/lookbehind constraint */
pub const REG_UBOUNDS: i32 = 0o000004; /* has bounded quantifier ({m,n}) */
pub const REG_UBRACES: i32 = 0o000010; /* has { that doesn't begin a quantifier */
pub const REG_UBSALNUM: i32 = 0o000020; /* has backslash-alphanumeric in non-ARE */
pub const REG_UPBOTCH: i32 = 0o000040; /* has unmatched right paren in ERE */
pub const REG_UBBS: i32 = 0o000100; /* has backslash within bracket expr */
pub const REG_UNONPOSIX: i32 = 0o000200; /* has any construct that extends POSIX */
pub const REG_UUNSPEC: i32 = 0o000400; /* has any case disallowed by POSIX */
pub const REG_UUNPORT: i32 = 0o001000; /* has numeric character code dependency */
pub const REG_ULOCALE: i32 = 0o002000; /* has locale dependency */
pub const REG_UEMPTYMATCH: i32 = 0o004000; /* can match a zero-length string */
pub const REG_UIMPOSSIBLE: i32 = 0o010000; /* provably cannot match anything */
pub const REG_USHORTEST: i32 = 0o020000; /* has non-greedy quantifier */


pub const REG_BASIC: i32 = 0o000000; /* BREs (convenience) */
pub const REG_EXTENDED: i32 = 0o000001; /* EREs */
pub const REG_ADVF: i32 = 0o000002; /* advanced features in EREs */
pub const REG_ADVANCED: i32 = 0o000003; /* AREs (which are also EREs) */
pub const REG_QUOTE: i32 = 0o000004; /* no special characters, none */
pub const REG_NOSPEC: i32 = REG_QUOTE; /* historical synonym */
pub const REG_ICASE: i32 = 0o000010; /* ignore case */
pub const REG_NOSUB: i32 = 0o000020; /* caller doesn't need subexpr match data */
pub const REG_EXPANDED: i32 = 0o000040; /* expanded format, white space & comments */
pub const REG_NLSTOP: i32 = 0o000100; /* \n doesn't match . or [^ ] */
pub const REG_NLANCH: i32 = 0o000200; /* ^ matches after \n, $ before */
pub const REG_NEWLINE: i32 = 0o000300; /* newlines are line terminators */
pub const REG_PEND: i32 = 0o000400; /* ugh -- backward-compatibility hack */
pub const REG_EXPECT: i32 = 0o001000; /* report details on partial/limited matches */
pub const REG_BOSONLY: i32 = 0o002000; /* temporary kludge for BOS-only matches */
pub const REG_DUMP: i32 = 0o004000; /* none of your business :-) */
pub const REG_FAKE: i32 = 0o010000; /* none of your business :-) */
pub const REG_PROGRESS: i32 = 0o020000; /* none of your business :-) */


pub const REG_NOTBOL: i32 = 0o0001; /* BOS is not BOL */
pub const REG_NOTEOL: i32 = 0o0002; /* EOS is not EOL */
pub const REG_STARTEND: i32 = 0o0004; /* backward compatibility kludge */
pub const REG_FTRACE: i32 = 0o0010; /* none of your business */
pub const REG_MTRACE: i32 = 0o0020; /* none of your business */
pub const REG_SMALL: i32 = 0o0040; /* none of your business */


pub const REMAGIC: i32 = 0xfed7;
pub const GUTSMAGIC: i32 = 0xfed9;
pub const CMMAGIC: i32 = 0x876;


pub const DUPMAX: i32 = 255;
pub const DUPINF: i32 = DUPMAX + 1;


pub const NUM_CCLASSES: i32 = 14;


pub const LATYPE_AHEAD_POS: i32 = 0o3; /* positive lookahead */
pub const LATYPE_AHEAD_NEG: i32 = 0o2; /* negative lookahead */
pub const LATYPE_BEHIND_POS: i32 = 0o1; /* positive lookbehind */
pub const LATYPE_BEHIND_NEG: i32 = 0o0; /* negative lookbehind */

#[inline]
pub const fn latype_is_pos(la: i32) -> bool {
    (la & 0o1) != 0
}

#[inline]
pub const fn latype_is_ahead(la: i32) -> bool {
    (la & 0o2) != 0
}


pub const COLOR_WHITE: i32 = 0;
pub const COLOR_RAINBOW: i32 = -2;
