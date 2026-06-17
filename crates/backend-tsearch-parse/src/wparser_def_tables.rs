// State/action tables for the default word parser, transcribed 1:1 from the
// `actionTPS_*` arrays in `src/backend/tsearch/wparser_def.c`.  Each row keeps
// the original (isclass, c, flags, tostate, type, special) tuple and the same
// ordering, which the matcher walks top-to-bottom exactly like the C loop.
//
// This file is `include!`d into `wparser_def.rs`, so it shares the `act!`
// macro, the `CharTest`/`Special`/`TParserState` types, and the flag consts.

static ACTION_TPS_BASE: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'<', A_PUSH, InTagFirst, 0, None_),
    act!(IsIgnore, 0, A_NEXT, InSpace, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InAsciiWord, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InUnsignedInt, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InSignedIntFirst, 0, None_),
    act!(IsEqC, b'+', A_PUSH, InSignedIntFirst, 0, None_),
    act!(IsEqC, b'&', A_PUSH, InXMLEntityFirst, 0, None_),
    act!(IsEqC, b'~', A_PUSH, InFileTwiddle, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InFileFirst, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InPathFirstFirst, 0, None_),
    act!(None_, 0, A_NEXT, InSpace, 0, None_),
];

static ACTION_TPS_IN_NUM_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, NUMWORD, None_),
    act!(IsAlnum, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InFileFirst, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InFileNext, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenNumWordFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, NUMWORD, None_),
];

static ACTION_TPS_IN_ASCII_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, ASCIIWORD, None_),
    act!(IsAscLet, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InHostFirstDomain, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InFileNext, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenAsciiWordFirst, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(IsEqC, b':', A_PUSH, InProtocolFirst, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InFileFirst, 0, None_),
    act!(IsDigit, 0, A_PUSH, InHost, 0, None_),
    act!(IsDigit, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InWord, 0, None_),
    act!(None_, 0, A_BINGO, Base, ASCIIWORD, None_),
];

static ACTION_TPS_IN_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, WORD_T, None_),
    act!(IsAlpha, 0, A_NEXT, Null, 0, None_),
    act!(IsSpecial, 0, A_NEXT, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenWordFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, WORD_T, None_),
];

static ACTION_TPS_IN_UNSIGNED_INT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, UNSIGNEDINT, None_),
    act!(IsDigit, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InHostFirstDomain, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InUDecimalFirst, 0, None_),
    act!(IsEqC, b'e', A_PUSH, InMantissaFirst, 0, None_),
    act!(IsEqC, b'E', A_PUSH, InMantissaFirst, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(IsAscLet, 0, A_PUSH, InHost, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InNumWord, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InFileFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, UNSIGNEDINT, None_),
];

static ACTION_TPS_IN_SIGNED_INT_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT | A_CLEAR, InSignedInt, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_SIGNED_INT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, SIGNEDINT, None_),
    act!(IsDigit, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InDecimalFirst, 0, None_),
    act!(IsEqC, b'e', A_PUSH, InMantissaFirst, 0, None_),
    act!(IsEqC, b'E', A_PUSH, InMantissaFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, SIGNEDINT, None_),
];

static ACTION_TPS_IN_SPACE: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, SPACE, None_),
    act!(IsEqC, b'<', A_BINGO, Base, SPACE, None_),
    act!(IsIgnore, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'-', A_BINGO, Base, SPACE, None_),
    act!(IsEqC, b'+', A_BINGO, Base, SPACE, None_),
    act!(IsEqC, b'&', A_BINGO, Base, SPACE, None_),
    act!(IsEqC, b'/', A_BINGO, Base, SPACE, None_),
    act!(IsNotAlnum, 0, A_NEXT, InSpace, 0, None_),
    act!(None_, 0, A_BINGO, Base, SPACE, None_),
];

static ACTION_TPS_IN_UDECIMAL_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InUDecimal, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_UDECIMAL: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, DECIMAL_T, None_),
    act!(IsDigit, 0, A_NEXT, InUDecimal, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InVersionFirst, 0, None_),
    act!(IsEqC, b'e', A_PUSH, InMantissaFirst, 0, None_),
    act!(IsEqC, b'E', A_PUSH, InMantissaFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, DECIMAL_T, None_),
];

static ACTION_TPS_IN_DECIMAL_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InDecimal, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_DECIMAL: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, DECIMAL_T, None_),
    act!(IsDigit, 0, A_NEXT, InDecimal, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InVerVersion, 0, None_),
    act!(IsEqC, b'e', A_PUSH, InMantissaFirst, 0, None_),
    act!(IsEqC, b'E', A_PUSH, InMantissaFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, DECIMAL_T, None_),
];

static ACTION_TPS_IN_VER_VERSION: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_RERUN, InSVerVersion, 0, VerVersion),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_SVER_VERSION: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_BINGO | A_CLRALL, InUnsignedInt, SPACE, None_),
    act!(None_, 0, A_NEXT, Null, 0, None_),
];

static ACTION_TPS_IN_VERSION_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InVersion, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_VERSION: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, VERSIONNUMBER, None_),
    act!(IsDigit, 0, A_NEXT, InVersion, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InVersionFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, VERSIONNUMBER, None_),
];

static ACTION_TPS_IN_MANTISSA_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InMantissa, 0, None_),
    act!(IsEqC, b'+', A_NEXT, InMantissaSign, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InMantissaSign, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_MANTISSA_SIGN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InMantissa, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_MANTISSA: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, SCIENTIFIC, None_),
    act!(IsDigit, 0, A_NEXT, InMantissa, 0, None_),
    act!(None_, 0, A_BINGO, Base, SCIENTIFIC, None_),
];

static ACTION_TPS_IN_XML_ENTITY_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'#', A_NEXT, InXMLEntityNumFirst, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b':', A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InXMLEntity, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAlnum, 0, A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b':', A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b'.', A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InXMLEntity, 0, None_),
    act!(IsEqC, b';', A_NEXT, InXMLEntityEnd, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY_NUM_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'x', A_NEXT, InXMLEntityHexNumFirst, 0, None_),
    act!(IsEqC, b'X', A_NEXT, InXMLEntityHexNumFirst, 0, None_),
    act!(IsDigit, 0, A_NEXT, InXMLEntityNum, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY_HEX_NUM_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsXdigit, 0, A_NEXT, InXMLEntityHexNum, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY_NUM: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InXMLEntityNum, 0, None_),
    act!(IsEqC, b';', A_NEXT, InXMLEntityEnd, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY_HEX_NUM: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsXdigit, 0, A_NEXT, InXMLEntityHexNum, 0, None_),
    act!(IsEqC, b';', A_NEXT, InXMLEntityEnd, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_ENTITY_END: &[TParserStateActionItem] =
    &[act!(None_, 0, A_BINGO | A_CLEAR, Base, XMLENTITY, None_)];

static ACTION_TPS_IN_TAG_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InTagCloseFirst, 0, None_),
    act!(IsEqC, b'!', A_PUSH, InCommentFirst, 0, None_),
    act!(IsEqC, b'?', A_PUSH, InXMLBegin, 0, None_),
    act!(IsAscLet, 0, A_PUSH, InTagName, 0, None_),
    act!(IsEqC, b':', A_PUSH, InTagName, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InTagName, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_XML_BEGIN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'x', A_NEXT, InTag, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_TAG_CLOSE_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InTagName, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_TAG_NAME: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InTagBeginEnd, 0, None_),
    act!(IsEqC, b'>', A_NEXT, InTagEnd, 0, Tags),
    act!(IsSpace, 0, A_NEXT, InTag, 0, Tags),
    act!(IsAlnum, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b':', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'_', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'.', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, Null, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_TAG_BEGIN_END: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'>', A_NEXT, InTagEnd, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_TAG: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'>', A_NEXT, InTagEnd, 0, Tags),
    act!(IsEqC, b'\'', A_NEXT, InTagEscapeK, 0, None_),
    act!(IsEqC, b'"', A_NEXT, InTagEscapeKK, 0, None_),
    act!(IsAscLet, 0, A_NEXT, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, Null, 0, None_),
    act!(IsEqC, b'=', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'_', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'#', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'/', A_NEXT, Null, 0, None_),
    act!(IsEqC, b':', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'.', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'&', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'?', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'%', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'~', A_NEXT, Null, 0, None_),
    act!(IsSpace, 0, A_NEXT, Null, 0, Tags),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_TAG_ESCAPE_K: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'\\', A_PUSH, InTagBackSleshed, 0, None_),
    act!(IsEqC, b'\'', A_NEXT, InTag, 0, None_),
    act!(None_, 0, A_NEXT, InTagEscapeK, 0, None_),
];

static ACTION_TPS_IN_TAG_ESCAPE_KK: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'\\', A_PUSH, InTagBackSleshed, 0, None_),
    act!(IsEqC, b'"', A_NEXT, InTag, 0, None_),
    act!(None_, 0, A_NEXT, InTagEscapeKK, 0, None_),
];

static ACTION_TPS_IN_TAG_BACK_SLESHED: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(None_, 0, A_MERGE, Null, 0, None_),
];

static ACTION_TPS_IN_TAG_END: &[TParserStateActionItem] =
    &[act!(None_, 0, A_BINGO | A_CLRALL, Base, TAG_T, None_)];

static ACTION_TPS_IN_COMMENT_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InCommentLast, 0, None_),
    act!(IsEqC, b'D', A_NEXT, InTag, 0, None_),
    act!(IsEqC, b'd', A_NEXT, InTag, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_COMMENT_LAST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InComment, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_COMMENT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InCloseCommentFirst, 0, None_),
    act!(None_, 0, A_NEXT, Null, 0, None_),
];

static ACTION_TPS_IN_CLOSE_COMMENT_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InCloseCommentLast, 0, None_),
    act!(None_, 0, A_NEXT, InComment, 0, None_),
];

static ACTION_TPS_IN_CLOSE_COMMENT_LAST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'-', A_NEXT, Null, 0, None_),
    act!(IsEqC, b'>', A_NEXT, InCommentEnd, 0, None_),
    act!(None_, 0, A_NEXT, InComment, 0, None_),
];

static ACTION_TPS_IN_COMMENT_END: &[TParserStateActionItem] =
    &[act!(None_, 0, A_BINGO | A_CLRALL, Base, TAG_T, None_)];

static ACTION_TPS_IN_HOST_FIRST_DOMAIN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHostDomainSecond, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHost, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HOST_DOMAIN_SECOND: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHostDomain, 0, None_),
    act!(IsDigit, 0, A_PUSH, InHost, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InHostFirstDomain, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HOST_DOMAIN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLRALL, Base, HOST, None_),
    act!(IsAscLet, 0, A_NEXT, InHostDomain, 0, None_),
    act!(IsDigit, 0, A_PUSH, InHost, 0, None_),
    act!(IsEqC, b':', A_PUSH, InPortFirst, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InHostFirstDomain, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(IsDigit, 0, A_POP, Null, 0, None_),
    act!(IsStopHost, 0, A_BINGO | A_CLRALL, InURLPathStart, HOST, None_),
    act!(IsEqC, b'/', A_PUSH, InFURL, 0, None_),
    act!(None_, 0, A_BINGO | A_CLRALL, Base, HOST, None_),
];

static ACTION_TPS_IN_PORT_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InPort, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PORT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLRALL, Base, HOST, None_),
    act!(IsDigit, 0, A_NEXT, InPort, 0, None_),
    act!(IsStopHost, 0, A_BINGO | A_CLRALL, InURLPathStart, HOST, None_),
    act!(IsEqC, b'/', A_PUSH, InFURL, 0, None_),
    act!(None_, 0, A_BINGO | A_CLRALL, Base, HOST, None_),
];

static ACTION_TPS_IN_HOST_FIRST_AN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHost, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHost, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HOST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHost, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHost, 0, None_),
    act!(IsEqC, b'@', A_PUSH, InEmail, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InHostFirstDomain, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHostFirstAN, 0, None_),
    act!(IsEqC, b'_', A_PUSH, InHostFirstAN, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_EMAIL: &[TParserStateActionItem] = &[
    act!(IsStopHost, 0, A_POP, Null, 0, None_),
    act!(IsHost, 0, A_BINGO | A_CLRALL, Base, EMAIL, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_FILE_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InFile, 0, None_),
    act!(IsDigit, 0, A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'.', A_NEXT, InPathFirst, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'~', A_PUSH, InFileTwiddle, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_FILE_TWIDDLE: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InFile, 0, None_),
    act!(IsDigit, 0, A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InFileFirst, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PATH_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InFile, 0, None_),
    act!(IsDigit, 0, A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'.', A_NEXT, InPathSecond, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InFileFirst, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PATH_FIRST_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'.', A_NEXT, InPathSecond, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InFileFirst, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PATH_SECOND: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLEAR, Base, FILEPATH, None_),
    act!(IsEqC, b'/', A_NEXT | A_PUSH, InFileFirst, 0, None_),
    act!(IsEqC, b'/', A_BINGO | A_CLEAR, Base, FILEPATH, None_),
    act!(IsSpace, 0, A_BINGO | A_CLEAR, Base, FILEPATH, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_FILE: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, FILEPATH, None_),
    act!(IsAscLet, 0, A_NEXT, InFile, 0, None_),
    act!(IsDigit, 0, A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'.', A_PUSH, InFileNext, 0, None_),
    act!(IsEqC, b'_', A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'-', A_NEXT, InFile, 0, None_),
    act!(IsEqC, b'/', A_PUSH, InFileFirst, 0, None_),
    act!(None_, 0, A_BINGO, Base, FILEPATH, None_),
];

static ACTION_TPS_IN_FILE_NEXT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_CLEAR, InFile, 0, None_),
    act!(IsDigit, 0, A_CLEAR, InFile, 0, None_),
    act!(IsEqC, b'_', A_CLEAR, InFile, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_URL_PATH_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsUrlChar, 0, A_NEXT, InURLPath, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_URL_PATH_START: &[TParserStateActionItem] =
    &[act!(None_, 0, A_NEXT, InURLPath, 0, None_)];

static ACTION_TPS_IN_URL_PATH: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, URLPATH, None_),
    act!(IsUrlChar, 0, A_NEXT, InURLPath, 0, None_),
    act!(None_, 0, A_BINGO, Base, URLPATH, None_),
];

static ACTION_TPS_IN_FURL: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsURLPath, 0, A_BINGO | A_CLRALL, Base, URL_T, FURL),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PROTOCOL_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InProtocolSecond, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PROTOCOL_SECOND: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsEqC, b'/', A_NEXT, InProtocolEnd, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PROTOCOL_END: &[TParserStateActionItem] =
    &[act!(None_, 0, A_BINGO | A_CLRALL, Base, PROTOCOL, None_)];

static ACTION_TPS_IN_HYPHEN_ASCII_WORD_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHyphenAsciiWord, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenDigitLookahead, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HYPHEN_ASCII_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLRALL, InParseHyphen, ASCIIHWORD, Hyphen),
    act!(IsAscLet, 0, A_NEXT, InHyphenAsciiWord, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenAsciiWordFirst, 0, None_),
    act!(None_, 0, A_BINGO | A_CLRALL, InParseHyphen, ASCIIHWORD, Hyphen),
];

static ACTION_TPS_IN_HYPHEN_WORD_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenDigitLookahead, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HYPHEN_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLRALL, InParseHyphen, HWORD, Hyphen),
    act!(IsAlpha, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenWordFirst, 0, None_),
    act!(None_, 0, A_BINGO | A_CLRALL, InParseHyphen, HWORD, Hyphen),
];

static ACTION_TPS_IN_HYPHEN_NUM_WORD_FIRST: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenDigitLookahead, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HYPHEN_NUM_WORD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO | A_CLRALL, InParseHyphen, NUMHWORD, Hyphen),
    act!(IsAlnum, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InHyphenNumWordFirst, 0, None_),
    act!(None_, 0, A_BINGO | A_CLRALL, InParseHyphen, NUMHWORD, Hyphen),
];

static ACTION_TPS_IN_HYPHEN_DIGIT_LOOKAHEAD: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenDigitLookahead, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenNumWord, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_PARSE_HYPHEN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_RERUN, Base, 0, None_),
    act!(IsAscLet, 0, A_NEXT, InHyphenAsciiWordPart, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWordPart, 0, None_),
    act!(IsDigit, 0, A_PUSH, InHyphenUnsignedInt, 0, None_),
    act!(IsEqC, b'-', A_PUSH, InParseHyphenHyphen, 0, None_),
    act!(None_, 0, A_RERUN, Base, 0, None_),
];

static ACTION_TPS_IN_PARSE_HYPHEN_HYPHEN: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsAlnum, 0, A_BINGO | A_CLEAR, InParseHyphen, SPACE, None_),
    act!(IsSpecial, 0, A_BINGO | A_CLEAR, InParseHyphen, SPACE, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

static ACTION_TPS_IN_HYPHEN_WORD_PART: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, PARTHWORD, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWordPart, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenWordPart, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenNumWordPart, 0, None_),
    act!(None_, 0, A_BINGO, InParseHyphen, PARTHWORD, None_),
];

static ACTION_TPS_IN_HYPHEN_ASCII_WORD_PART: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, ASCIIPARTHWORD, None_),
    act!(IsAscLet, 0, A_NEXT, InHyphenAsciiWordPart, 0, None_),
    act!(IsAlpha, 0, A_NEXT, InHyphenWordPart, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenWordPart, 0, None_),
    act!(IsDigit, 0, A_NEXT, InHyphenNumWordPart, 0, None_),
    act!(None_, 0, A_BINGO, InParseHyphen, ASCIIPARTHWORD, None_),
];

static ACTION_TPS_IN_HYPHEN_NUM_WORD_PART: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_BINGO, Base, NUMPARTHWORD, None_),
    act!(IsAlnum, 0, A_NEXT, InHyphenNumWordPart, 0, None_),
    act!(IsSpecial, 0, A_NEXT, InHyphenNumWordPart, 0, None_),
    act!(None_, 0, A_BINGO, InParseHyphen, NUMPARTHWORD, None_),
];

static ACTION_TPS_IN_HYPHEN_UNSIGNED_INT: &[TParserStateActionItem] = &[
    act!(IsEOF, 0, A_POP, Null, 0, None_),
    act!(IsDigit, 0, A_NEXT, Null, 0, None_),
    act!(IsAlpha, 0, A_CLEAR, InHyphenNumWordPart, 0, None_),
    act!(IsSpecial, 0, A_CLEAR, InHyphenNumWordPart, 0, None_),
    act!(None_, 0, A_POP, Null, 0, None_),
];

/// Equivalent of the C `Actions[]` table: map a state to its action array.
fn actions_for(state: TParserState) -> &'static [TParserStateActionItem] {
    use TParserState::*;
    match state {
        Base => ACTION_TPS_BASE,
        InNumWord => ACTION_TPS_IN_NUM_WORD,
        InAsciiWord => ACTION_TPS_IN_ASCII_WORD,
        InWord => ACTION_TPS_IN_WORD,
        InUnsignedInt => ACTION_TPS_IN_UNSIGNED_INT,
        InSignedIntFirst => ACTION_TPS_IN_SIGNED_INT_FIRST,
        InSignedInt => ACTION_TPS_IN_SIGNED_INT,
        InSpace => ACTION_TPS_IN_SPACE,
        InUDecimalFirst => ACTION_TPS_IN_UDECIMAL_FIRST,
        InUDecimal => ACTION_TPS_IN_UDECIMAL,
        InDecimalFirst => ACTION_TPS_IN_DECIMAL_FIRST,
        InDecimal => ACTION_TPS_IN_DECIMAL,
        InVerVersion => ACTION_TPS_IN_VER_VERSION,
        InSVerVersion => ACTION_TPS_IN_SVER_VERSION,
        InVersionFirst => ACTION_TPS_IN_VERSION_FIRST,
        InVersion => ACTION_TPS_IN_VERSION,
        InMantissaFirst => ACTION_TPS_IN_MANTISSA_FIRST,
        InMantissaSign => ACTION_TPS_IN_MANTISSA_SIGN,
        InMantissa => ACTION_TPS_IN_MANTISSA,
        InXMLEntityFirst => ACTION_TPS_IN_XML_ENTITY_FIRST,
        InXMLEntity => ACTION_TPS_IN_XML_ENTITY,
        InXMLEntityNumFirst => ACTION_TPS_IN_XML_ENTITY_NUM_FIRST,
        InXMLEntityNum => ACTION_TPS_IN_XML_ENTITY_NUM,
        InXMLEntityHexNumFirst => ACTION_TPS_IN_XML_ENTITY_HEX_NUM_FIRST,
        InXMLEntityHexNum => ACTION_TPS_IN_XML_ENTITY_HEX_NUM,
        InXMLEntityEnd => ACTION_TPS_IN_XML_ENTITY_END,
        InTagFirst => ACTION_TPS_IN_TAG_FIRST,
        InXMLBegin => ACTION_TPS_IN_XML_BEGIN,
        InTagCloseFirst => ACTION_TPS_IN_TAG_CLOSE_FIRST,
        InTagName => ACTION_TPS_IN_TAG_NAME,
        InTagBeginEnd => ACTION_TPS_IN_TAG_BEGIN_END,
        InTag => ACTION_TPS_IN_TAG,
        InTagEscapeK => ACTION_TPS_IN_TAG_ESCAPE_K,
        InTagEscapeKK => ACTION_TPS_IN_TAG_ESCAPE_KK,
        InTagBackSleshed => ACTION_TPS_IN_TAG_BACK_SLESHED,
        InTagEnd => ACTION_TPS_IN_TAG_END,
        InCommentFirst => ACTION_TPS_IN_COMMENT_FIRST,
        InCommentLast => ACTION_TPS_IN_COMMENT_LAST,
        InComment => ACTION_TPS_IN_COMMENT,
        InCloseCommentFirst => ACTION_TPS_IN_CLOSE_COMMENT_FIRST,
        InCloseCommentLast => ACTION_TPS_IN_CLOSE_COMMENT_LAST,
        InCommentEnd => ACTION_TPS_IN_COMMENT_END,
        InHostFirstDomain => ACTION_TPS_IN_HOST_FIRST_DOMAIN,
        InHostDomainSecond => ACTION_TPS_IN_HOST_DOMAIN_SECOND,
        InHostDomain => ACTION_TPS_IN_HOST_DOMAIN,
        InPortFirst => ACTION_TPS_IN_PORT_FIRST,
        InPort => ACTION_TPS_IN_PORT,
        InHostFirstAN => ACTION_TPS_IN_HOST_FIRST_AN,
        InHost => ACTION_TPS_IN_HOST,
        InEmail => ACTION_TPS_IN_EMAIL,
        InFileFirst => ACTION_TPS_IN_FILE_FIRST,
        InFileTwiddle => ACTION_TPS_IN_FILE_TWIDDLE,
        InPathFirst => ACTION_TPS_IN_PATH_FIRST,
        InPathFirstFirst => ACTION_TPS_IN_PATH_FIRST_FIRST,
        InPathSecond => ACTION_TPS_IN_PATH_SECOND,
        InFile => ACTION_TPS_IN_FILE,
        InFileNext => ACTION_TPS_IN_FILE_NEXT,
        InURLPathFirst => ACTION_TPS_IN_URL_PATH_FIRST,
        InURLPathStart => ACTION_TPS_IN_URL_PATH_START,
        InURLPath => ACTION_TPS_IN_URL_PATH,
        InFURL => ACTION_TPS_IN_FURL,
        InProtocolFirst => ACTION_TPS_IN_PROTOCOL_FIRST,
        InProtocolSecond => ACTION_TPS_IN_PROTOCOL_SECOND,
        InProtocolEnd => ACTION_TPS_IN_PROTOCOL_END,
        InHyphenAsciiWordFirst => ACTION_TPS_IN_HYPHEN_ASCII_WORD_FIRST,
        InHyphenAsciiWord => ACTION_TPS_IN_HYPHEN_ASCII_WORD,
        InHyphenWordFirst => ACTION_TPS_IN_HYPHEN_WORD_FIRST,
        InHyphenWord => ACTION_TPS_IN_HYPHEN_WORD,
        InHyphenNumWordFirst => ACTION_TPS_IN_HYPHEN_NUM_WORD_FIRST,
        InHyphenNumWord => ACTION_TPS_IN_HYPHEN_NUM_WORD,
        InHyphenDigitLookahead => ACTION_TPS_IN_HYPHEN_DIGIT_LOOKAHEAD,
        InParseHyphen => ACTION_TPS_IN_PARSE_HYPHEN,
        InParseHyphenHyphen => ACTION_TPS_IN_PARSE_HYPHEN_HYPHEN,
        InHyphenWordPart => ACTION_TPS_IN_HYPHEN_WORD_PART,
        InHyphenAsciiWordPart => ACTION_TPS_IN_HYPHEN_ASCII_WORD_PART,
        InHyphenNumWordPart => ACTION_TPS_IN_HYPHEN_NUM_WORD_PART,
        InHyphenUnsignedInt => ACTION_TPS_IN_HYPHEN_UNSIGNED_INT,
        Null => unreachable!("TPS_Null has no action table"),
    }
}
