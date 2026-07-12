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
    Junk(String),
}

// syncrep_scanner.l: whitespace-skipping tokenizer. Identifiers are
// ident_start [A-Za-z\200-\377_] then ident_cont [A-Za-z\200-\377_0-9$];
// double-quoted names use "" as an escaped quote; "*" is a NAME.
fn scan(input: &str) -> Result<Vec<Token>, String> {
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
                toks.push(Token::Name(name));
                i = j;
            }
            b'0'..=b'9' => {
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                toks.push(Token::Num(input[start..i].to_string()));
            }
            b'*' => {
                toks.push(Token::Name("*".into()));
                i += 1;
            }
            b',' => {
                toks.push(Token::Comma);
                i += 1;
            }
            b'(' => {
                toks.push(Token::LParen);
                i += 1;
            }
            b')' => {
                toks.push(Token::RParen);
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
                if word.eq_ignore_ascii_case("any") {
                    toks.push(Token::Any);
                } else if word.eq_ignore_ascii_case("first") {
                    toks.push(Token::First);
                } else {
                    toks.push(Token::Name(word.to_string()));
                }
            }
            _ => {
                toks.push(Token::Junk(input[i..].chars().next().unwrap().to_string()));
                i += input[i..].chars().next().unwrap().len_utf8();
            }
        }
    }
    Ok(toks)
}

fn syntax_error(toks: &[Token], pos: usize) -> String {
    // syncrep_yyerror: "syntax error at or near \"%s\"" / "at end of input".
    match toks.get(pos) {
        Some(Token::Any) => "syntax error at or near \"ANY\"".into(),
        Some(Token::First) => "syntax error at or near \"FIRST\"".into(),
        Some(Token::Name(s)) | Some(Token::Num(s)) | Some(Token::Junk(s)) => {
            format!("syntax error at or near \"{s}\"")
        }
        Some(Token::Comma) => "syntax error at or near \",\"".into(),
        Some(Token::LParen) => "syntax error at or near \"(\"".into(),
        Some(Token::RParen) => "syntax error at or near \")\"".into(),
        None => "syntax error at end of input".into(),
    }
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

    let (num_sync_str, method, parenthesized) = match toks.first() {
        Some(Token::Any) | Some(Token::First) => {
            let method = if toks[0] == Token::Any { SYNC_REP_QUORUM } else { SYNC_REP_PRIORITY };
            pos = 1;
            let Some(Token::Num(n)) = toks.get(pos) else {
                return Err(syntax_error(&toks, pos));
            };
            let n = n.clone();
            pos += 1;
            if toks.get(pos) != Some(&Token::LParen) {
                return Err(syntax_error(&toks, pos));
            }
            pos += 1;
            (n, method, true)
        }
        Some(Token::Num(n)) if toks.get(1) == Some(&Token::LParen) => {
            let n = n.clone();
            pos = 2;
            (n, SYNC_REP_PRIORITY, true)
        }
        _ => ("1".to_string(), SYNC_REP_PRIORITY, false),
    };

    // standby_list
    let mut members = Vec::new();
    loop {
        match toks.get(pos) {
            Some(Token::Name(s)) | Some(Token::Num(s)) => {
                members.push(s.clone());
                pos += 1;
            }
            _ => return Err(syntax_error(&toks, pos)),
        }
        if toks.get(pos) == Some(&Token::Comma) {
            pos += 1;
            continue;
        }
        break;
    }

    if parenthesized {
        if toks.get(pos) != Some(&Token::RParen) {
            return Err(syntax_error(&toks, pos));
        }
        pos += 1;
    }
    if pos != toks.len() {
        return Err(syntax_error(&toks, pos));
    }

    // C: atoi() of the NUM token.
    let num_sync = num_sync_str.parse::<i32>().unwrap_or(i32::MAX);

    Ok(SyncRepConfigData { num_sync, syncrep_method: method, members })
}
