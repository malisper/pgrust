// synchronous_standby_names parser: syncrep_scanner.l + syncrep_gram.y,
// hand-rolled with C-identical token rules and error messages.

/// SyncRepConfigData (syncrep.h), flat member_names unpacked to a Vec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncRepConfigData {
    pub num_sync: i32,
    pub syncrep_method: u8,
    pub members: Vec<String>,
}

pub const SYNC_REP_PRIORITY: u8 = 0;
pub const SYNC_REP_QUORUM: u8 = 1;

#[derive(Debug, PartialEq)]
enum Token {
    Any,
    First,
    Name(String),
    Num(String),
    Comma,
    LParen,
    RParen,
    Junk,
}

/// One scanned token with its yytext (syncrep_scanner.l): the raw text the
/// flex rule matched, which syncrep_yyerror quotes verbatim. A quoted name's
/// token is the closing dquote (<xd>{xdstop}), so its yytext is `"`.
#[derive(Debug, PartialEq)]
struct Tok {
    kind: Token,
    yytext: String,
}

// syncrep_scanner.l: whitespace-skipping tokenizer. Identifiers are
// ident_start [A-Za-z\200-\377_] then ident_cont [A-Za-z\200-\377_0-9$];
// double-quoted names use "" as an escaped quote; "*" is a NAME.
fn scan(input: &str) -> Result<Vec<Tok>, String> {
    let b = input.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' | b'\x0c' | b'\x0b' => i += 1,
            b'"' => {
                // <xd> exclusive state: gather until an unescaped quote.
                let mut name = String::new();
                let mut j = i + 1;
                loop {
                    if j >= b.len() {
                        return Err("unterminated quoted identifier at end of input".into());
                    }
                    if b[j] == b'"' {
                        if j + 1 < b.len() && b[j + 1] == b'"' {
                            name.push('"');
                            j += 2;
                        } else {
                            j += 1;
                            break;
                        }
                    } else {
                        let start = j;
                        while j < b.len() && b[j] != b'"' {
                            j += 1;
                        }
                        name.push_str(&input[start..j]);
                    }
                }
                toks.push(Tok { kind: Token::Name(name), yytext: "\"".into() });
                i = j;
            }
            b'0'..=b'9' => {
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                let text = input[start..i].to_string();
                toks.push(Tok { kind: Token::Num(text.clone()), yytext: text });
            }
            b'*' => {
                toks.push(Tok { kind: Token::Name("*".into()), yytext: "*".into() });
                i += 1;
            }
            b',' => {
                toks.push(Tok { kind: Token::Comma, yytext: ",".into() });
                i += 1;
            }
            b'(' => {
                toks.push(Tok { kind: Token::LParen, yytext: "(".into() });
                i += 1;
            }
            b')' => {
                toks.push(Tok { kind: Token::RParen, yytext: ")".into() });
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                let start = i;
                i += 1;
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric()
                        || b[i] == b'_'
                        || b[i] == b'$'
                        || b[i] >= 0x80)
                {
                    i += 1;
                }
                let word = &input[start..i];
                let kind = if word.eq_ignore_ascii_case("any") {
                    Token::Any
                } else if word.eq_ignore_ascii_case("first") {
                    Token::First
                } else {
                    Token::Name(word.to_string())
                };
                toks.push(Tok { kind, yytext: word.to_string() });
            }
            _ => {
                // Bytes >= 0x80 are ident_start, so junk is always one ASCII byte.
                let text = input[i..].chars().next().unwrap().to_string();
                i += text.len();
                toks.push(Tok { kind: Token::Junk, yytext: text });
            }
        }
    }
    Ok(toks)
}

fn syntax_error(toks: &[Tok], pos: usize) -> String {
    // syncrep_yyerror (syncrep_scanner.l:165): "%s at or near \"%s\"" with
    // yytext of the offending lookahead token, or "%s at end of input".
    match toks.get(pos) {
        Some(t) => format!("syntax error at or near \"{}\"", t.yytext),
        None => "syntax error at end of input".into(),
    }
}

/// syncrep_gram.y:106 `config->num_sync = atoi(num_sync)`: (int) strtol() of
/// a digit string — strtol saturates at LONG_MAX (64-bit) on overflow and the
/// narrowing cast keeps the low 32 bits.
fn c_atoi(digits: &str) -> i32 {
    digits.parse::<i64>().unwrap_or(i64::MAX) as i32
}

/// syncrep_gram.y:
///   standby_config: standby_list
///                 | NUM '(' standby_list ')'
///                 | ANY NUM '(' standby_list ')'
///                 | FIRST NUM '(' standby_list ')'
///   standby_list: standby_name (',' standby_name)*
///   standby_name: NAME | NUM
pub fn parse_synchronous_standby_names(input: &str) -> Result<SyncRepConfigData, String> {
    let toks = scan(input)?;
    let mut pos = 0;

    let kind = |pos: usize| toks.get(pos).map(|t| &t.kind);

    let (num_sync_str, method, parenthesized) = match kind(0) {
        Some(Token::Any) | Some(Token::First) => {
            let method = if kind(0) == Some(&Token::Any) { SYNC_REP_QUORUM } else { SYNC_REP_PRIORITY };
            pos = 1;
            let Some(Token::Num(n)) = kind(pos) else {
                return Err(syntax_error(&toks, pos));
            };
            let n = n.clone();
            pos += 1;
            if kind(pos) != Some(&Token::LParen) {
                return Err(syntax_error(&toks, pos));
            }
            pos += 1;
            (n, method, true)
        }
        Some(Token::Num(n)) if kind(1) == Some(&Token::LParen) => {
            let n = n.clone();
            pos = 2;
            (n, SYNC_REP_PRIORITY, true)
        }
        _ => ("1".to_string(), SYNC_REP_PRIORITY, false),
    };

    // standby_list
    let mut members = Vec::new();
    loop {
        match kind(pos) {
            Some(Token::Name(s)) | Some(Token::Num(s)) => {
                members.push(s.clone());
                pos += 1;
            }
            _ => return Err(syntax_error(&toks, pos)),
        }
        if kind(pos) == Some(&Token::Comma) {
            pos += 1;
            continue;
        }
        break;
    }

    if parenthesized {
        if kind(pos) != Some(&Token::RParen) {
            return Err(syntax_error(&toks, pos));
        }
        pos += 1;
    }
    if pos != toks.len() {
        return Err(syntax_error(&toks, pos));
    }

    let num_sync = c_atoi(&num_sync_str);

    Ok(SyncRepConfigData { num_sync, syncrep_method: method, members })
}
