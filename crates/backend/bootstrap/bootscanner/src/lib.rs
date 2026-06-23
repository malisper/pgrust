#![allow(non_snake_case)]

//! Port of `src/backend/bootstrap/bootscanner.l` — the `.bki` bootstrap-mode
//! lexer.
//!
//! Safe owned-value rewrite of the flex scanner used by `BootstrapModeMain` to
//! tokenize the bootstrap data file (`postgres.bki`). The scanner keeps its
//! state in an owned [`BootScanner`] value (input buffer, byte position, line
//! counter, beginning-of-line flag, and the most recent `yytext`); there are no
//! `*mut`/`*const` pointers, no `extern "C"`, and no `c_void`. Errors are
//! returned as owned [`PgError`] values through [`PgResult`] instead of
//! `longjmp` (the C `elog(ERROR, ...)` / `ereport(ERROR, ...)` sites). The
//! `{sid}` quoted-string token de-escapes via the ported `DeescapeQuotedString`
//! from `backend-utils-misc-guc-file`, exactly as the C scanner shares that
//! routine. The integer token codes match the Bison-generated `bootparse.h`
//! token numbers and keep the C `int` type via [`core::ffi::c_int`].

use utils_error::{PgError, PgResult};
use ::guc_file::DeescapeQuotedString;
use core::ffi::c_int;
use ::types_error::ERROR;

pub const ID: c_int = 258;
pub const COMMA: c_int = 259;
pub const EQUALS: c_int = 260;
pub const LPAREN: c_int = 261;
pub const RPAREN: c_int = 262;
pub const NULLVAL: c_int = 263;
pub const OPEN: c_int = 264;
pub const XCLOSE: c_int = 265;
pub const XCREATE: c_int = 266;
pub const INSERT_TUPLE: c_int = 267;
pub const XDECLARE: c_int = 268;
pub const INDEX: c_int = 269;
pub const ON: c_int = 270;
pub const USING: c_int = 271;
pub const XBUILD: c_int = 272;
pub const INDICES: c_int = 273;
pub const UNIQUE: c_int = 274;
pub const XTOAST: c_int = 275;
pub const OBJ_ID: c_int = 276;
pub const XBOOTSTRAP: c_int = 277;
pub const XSHARED_RELATION: c_int = 278;
pub const XROWTYPE_OID: c_int = 279;
pub const XFORCE: c_int = 280;
pub const XNOT: c_int = 281;
pub const XNULL: c_int = 282;

/// The semantic value a token carries (the C `YYSTYPE` union: `kw` is a
/// constant keyword string, `str` is a palloc'd identifier/quoted string).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootTokenValue {
    None,
    String(String),
    Keyword(&'static str),
}

impl BootTokenValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            Self::Keyword(value) => Some(value),
            Self::None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootTokenKind {
    Id,
    Comma,
    Equals,
    LeftParen,
    RightParen,
    NullVal,
    Open,
    Close,
    Create,
    InsertTuple,
    Declare,
    Index,
    On,
    Using,
    Build,
    Indices,
    Unique,
    Toast,
    ObjId,
    Bootstrap,
    SharedRelation,
    RowtypeOid,
    Force,
    Not,
    Null,
    Eof,
}

impl BootTokenKind {
    pub fn token_code(self) -> c_int {
        match self {
            Self::Id => ID,
            Self::Comma => COMMA,
            Self::Equals => EQUALS,
            Self::LeftParen => LPAREN,
            Self::RightParen => RPAREN,
            Self::NullVal => NULLVAL,
            Self::Open => OPEN,
            Self::Close => XCLOSE,
            Self::Create => XCREATE,
            Self::InsertTuple => INSERT_TUPLE,
            Self::Declare => XDECLARE,
            Self::Index => INDEX,
            Self::On => ON,
            Self::Using => USING,
            Self::Build => XBUILD,
            Self::Indices => INDICES,
            Self::Unique => UNIQUE,
            Self::Toast => XTOAST,
            Self::ObjId => OBJ_ID,
            Self::Bootstrap => XBOOTSTRAP,
            Self::SharedRelation => XSHARED_RELATION,
            Self::RowtypeOid => XROWTYPE_OID,
            Self::Force => XFORCE,
            Self::Not => XNOT,
            Self::Null => XNULL,
            Self::Eof => 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootToken {
    pub kind: BootTokenKind,
    pub value: BootTokenValue,
    pub yytext: String,
    pub line: i32,
}

impl BootToken {
    pub fn token_code(&self) -> c_int {
        self.kind.token_code()
    }

    pub fn semantic_text(&self) -> Option<&str> {
        self.value.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootScanner {
    input: String,
    position: usize,
    line: i32,
    at_bol: bool,
    yytext: String,
}

pub fn boot_scanner_init(input: impl Into<String>) -> BootScanner {
    BootScanner {
        input: input.into(),
        position: 0,
        line: 1,
        at_bol: true,
        yytext: String::new(),
    }
}

pub fn boot_scanner_finish(_scanner: BootScanner) {}

pub fn boot_yylex(scanner: &mut BootScanner) -> PgResult<BootToken> {
    scanner.next_token()
}

/// `boot_yyerror` (bootscanner.l): `elog(ERROR, "%s at line %d", message,
/// yylineno)`.
pub fn boot_yyerror(scanner: &BootScanner, message: &str) -> PgResult<()> {
    Err(PgError::new(
        ERROR,
        format!("{} at line {}", message, scanner.line()),
    ))
}

pub fn scan_tokens(input: &str) -> PgResult<Vec<BootToken>> {
    let mut scanner = boot_scanner_init(input);
    let mut tokens = Vec::new();
    loop {
        let token = boot_yylex(&mut scanner)?;
        if token.kind == BootTokenKind::Eof {
            break;
        }
        tokens.push(token);
    }
    Ok(tokens)
}

impl BootScanner {
    pub fn line(&self) -> i32 {
        self.line
    }

    pub fn yytext(&self) -> &str {
        &self.yytext
    }

    pub fn next_token(&mut self) -> PgResult<BootToken> {
        loop {
            if self.position >= self.input.len() {
                self.yytext.clear();
                return Ok(BootToken {
                    kind: BootTokenKind::Eof,
                    value: BootTokenValue::None,
                    yytext: String::new(),
                    line: self.line,
                });
            }

            let line = self.line;
            let start = self.position;
            let byte = self.input.as_bytes()[self.position];

            match byte {
                // [\n] { yylineno++; }
                b'\n' => {
                    self.position += 1;
                    self.line += 1;
                    self.at_bol = true;
                    continue;
                }
                // [\r\t ] ;
                b'\r' | b'\t' | b' ' => {
                    self.position += 1;
                    self.at_bol = false;
                    continue;
                }
                // ^\#[^\n]* ; (comment to end of line, only at start of line)
                b'#' if self.at_bol => {
                    self.skip_comment();
                    continue;
                }
                b',' => return Ok(self.single_char_token(start, line, BootTokenKind::Comma)),
                b'=' => return Ok(self.single_char_token(start, line, BootTokenKind::Equals)),
                b'(' => return Ok(self.single_char_token(start, line, BootTokenKind::LeftParen)),
                b')' => return Ok(self.single_char_token(start, line, BootTokenKind::RightParen)),
                // {sid} \'([^']|\'\')*\' { yylval->str = DeescapeQuotedString(yytext); }
                b'\'' => {
                    if let Some(end) = self.quoted_end(start) {
                        self.position = end;
                        self.at_bol = false;
                        self.yytext = self.input[start..end].to_owned();
                        return Ok(BootToken {
                            kind: BootTokenKind::Id,
                            value: BootTokenValue::String(DeescapeQuotedString(&self.yytext)),
                            yytext: self.yytext.clone(),
                            line,
                        });
                    }
                    return Err(self.unexpected_character("'"));
                }
                // {id} [-A-Za-z0-9_]+ — but flex matches the longest rule, so a
                // keyword (a fixed string) wins over {id} only when the whole
                // token equals the keyword; longest-match then keyword lookup
                // reproduces that.
                byte if is_id_byte(byte) => return Ok(self.identifier_token(start, line)),
                // . { elog(ERROR, "syntax error at line %d: unexpected character \"%s\"", ...) }
                _ => {
                    let ch = self.next_char().unwrap_or('\0').to_string();
                    return Err(self.unexpected_character(&ch));
                }
            }
        }
    }

    fn single_char_token(&mut self, start: usize, line: i32, kind: BootTokenKind) -> BootToken {
        self.position += 1;
        self.at_bol = false;
        self.yytext = self.input[start..self.position].to_owned();
        BootToken {
            kind,
            value: BootTokenValue::None,
            yytext: self.yytext.clone(),
            line,
        }
    }

    fn identifier_token(&mut self, start: usize, line: i32) -> BootToken {
        while self.position < self.input.len() && is_id_byte(self.input.as_bytes()[self.position]) {
            self.position += 1;
        }
        self.at_bol = false;
        self.yytext = self.input[start..self.position].to_owned();

        let (kind, value) = match keyword_token(&self.yytext) {
            Some((kind, keyword)) => (kind, BootTokenValue::Keyword(keyword)),
            None if self.yytext == "_null_" => (BootTokenKind::NullVal, BootTokenValue::None),
            None => (
                BootTokenKind::Id,
                BootTokenValue::String(self.yytext.clone()),
            ),
        };

        BootToken {
            kind,
            value,
            yytext: self.yytext.clone(),
            line,
        }
    }

    fn quoted_end(&self, start: usize) -> Option<usize> {
        let bytes = self.input.as_bytes();
        let mut position = start + 1;
        while position < bytes.len() {
            if bytes[position] == b'\'' {
                if position + 1 < bytes.len() && bytes[position + 1] == b'\'' {
                    position += 2;
                } else {
                    return Some(position + 1);
                }
            } else {
                position += 1;
            }
        }
        None
    }

    fn skip_comment(&mut self) {
        while self.position < self.input.len() && self.input.as_bytes()[self.position] != b'\n' {
            self.position += 1;
        }
    }

    fn next_char(&self) -> Option<char> {
        self.input[self.position..].chars().next()
    }

    fn unexpected_character(&self, character: &str) -> PgError {
        PgError::new(
            ERROR,
            format!(
                "syntax error at line {}: unexpected character \"{}\"",
                self.line, character
            ),
        )
    }
}

fn keyword_token(text: &str) -> Option<(BootTokenKind, &'static str)> {
    Some(match text {
        "open" => (BootTokenKind::Open, "open"),
        "close" => (BootTokenKind::Close, "close"),
        "create" => (BootTokenKind::Create, "create"),
        "OID" => (BootTokenKind::ObjId, "OID"),
        "bootstrap" => (BootTokenKind::Bootstrap, "bootstrap"),
        "shared_relation" => (BootTokenKind::SharedRelation, "shared_relation"),
        "rowtype_oid" => (BootTokenKind::RowtypeOid, "rowtype_oid"),
        "insert" => (BootTokenKind::InsertTuple, "insert"),
        "declare" => (BootTokenKind::Declare, "declare"),
        "build" => (BootTokenKind::Build, "build"),
        "indices" => (BootTokenKind::Indices, "indices"),
        "unique" => (BootTokenKind::Unique, "unique"),
        "index" => (BootTokenKind::Index, "index"),
        "on" => (BootTokenKind::On, "on"),
        "using" => (BootTokenKind::Using, "using"),
        "toast" => (BootTokenKind::Toast, "toast"),
        "FORCE" => (BootTokenKind::Force, "FORCE"),
        "NOT" => (BootTokenKind::Not, "NOT"),
        "NULL" => (BootTokenKind::Null, "NULL"),
        _ => return None,
    })
}

fn is_id_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<BootTokenKind> {
        scan_tokens(input)
            .unwrap()
            .into_iter()
            .map(|token| token.kind)
            .collect()
    }

    #[test]
    fn scans_bootstrap_keywords_and_punctuation() {
        assert_eq!(
            kinds("open close create insert declare index on using build indices unique toast OID bootstrap shared_relation rowtype_oid FORCE NOT NULL _null_ , = ( )"),
            vec![
                BootTokenKind::Open,
                BootTokenKind::Close,
                BootTokenKind::Create,
                BootTokenKind::InsertTuple,
                BootTokenKind::Declare,
                BootTokenKind::Index,
                BootTokenKind::On,
                BootTokenKind::Using,
                BootTokenKind::Build,
                BootTokenKind::Indices,
                BootTokenKind::Unique,
                BootTokenKind::Toast,
                BootTokenKind::ObjId,
                BootTokenKind::Bootstrap,
                BootTokenKind::SharedRelation,
                BootTokenKind::RowtypeOid,
                BootTokenKind::Force,
                BootTokenKind::Not,
                BootTokenKind::Null,
                BootTokenKind::NullVal,
                BootTokenKind::Comma,
                BootTokenKind::Equals,
                BootTokenKind::LeftParen,
                BootTokenKind::RightParen,
            ]
        );
    }

    #[test]
    fn keyword_matching_is_case_sensitive_and_whole_token_only() {
        let tokens = scan_tokens("OPEN opened open").unwrap();
        assert_eq!(tokens[0].kind, BootTokenKind::Id);
        assert_eq!(tokens[0].semantic_text(), Some("OPEN"));
        assert_eq!(tokens[1].kind, BootTokenKind::Id);
        assert_eq!(tokens[1].semantic_text(), Some("opened"));
        assert_eq!(tokens[2].kind, BootTokenKind::Open);
        assert_eq!(tokens[2].semantic_text(), Some("open"));
    }

    #[test]
    fn id_accepts_historical_bootstrap_characters() {
        let tokens = scan_tokens("-abc 123 some_name a-b").unwrap();
        assert_eq!(
            tokens
                .iter()
                .map(BootToken::semantic_text)
                .collect::<Vec<_>>(),
            vec![Some("-abc"), Some("123"), Some("some_name"), Some("a-b")]
        );
    }

    #[test]
    fn quoted_strings_are_deescaped_like_postgres_guc_strings() {
        let tokens = scan_tokens("'it''s' '\\n\\141'").unwrap();
        assert_eq!(tokens[0].kind, BootTokenKind::Id);
        assert_eq!(tokens[0].semantic_text(), Some("it's"));
        assert_eq!(tokens[1].semantic_text(), Some("\na"));
    }

    #[test]
    fn comments_only_start_at_beginning_of_line() {
        let tokens = scan_tokens("# skip\nopen").unwrap();
        assert_eq!(tokens[0].kind, BootTokenKind::Open);
        assert_eq!(tokens[0].line, 2);

        let err = scan_tokens(" # not a comment").unwrap_err();
        assert_eq!(
            err.message(),
            "syntax error at line 1: unexpected character \"#\""
        );
    }

    #[test]
    fn reports_unexpected_character_at_current_line() {
        let err = scan_tokens("open\n@").unwrap_err();
        assert_eq!(
            err.message(),
            "syntax error at line 2: unexpected character \"@\""
        );
    }

    #[test]
    fn boot_yyerror_matches_postgres_message_shape() {
        let mut scanner = boot_scanner_init("open\nclose");
        assert_eq!(boot_yylex(&mut scanner).unwrap().kind, BootTokenKind::Open);
        assert_eq!(boot_yylex(&mut scanner).unwrap().kind, BootTokenKind::Close);

        let err = boot_yyerror(&scanner, "syntax error").unwrap_err();
        assert_eq!(err.message(), "syntax error at line 2");
    }
}
