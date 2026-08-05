//! Error/notice message building: a faithful port of libpq's
//! pqBuildErrorMessage3 (PQERRORS_DEFAULT verbosity) + reportErrorPosition
//! (fe-protocol3.c), which is what psql prints for ErrorResponse /
//! NoticeResponse. UTF-8 display widths via the ported wchar crate.

use crate::print::dsplen;
use crate::proto::ErrorFields;

const DISPLAY_SIZE: i32 = 60; // screen width limit, in screen cols
const MIN_RIGHT_CUT: i32 = 10; // try to keep this far away from EOL

/// Build the full message text, e.g.
/// "ERROR:  syntax error at or near \"x\"\nLINE 1: ...\n        ^\n".
/// `query` = the query the error is about (for LINE/caret display).
/// `is_error` selects PQSHOW_CONTEXT_ERRORS behavior (context shown for
/// errors, suppressed for notices at default verbosity).
pub fn build_message(f: &ErrorFields, query: Option<&str>, is_error: bool) -> String {
    let mut msg = String::new();
    if !f.severity.is_empty() {
        msg.push_str(&format!("{}:  ", f.severity));
    }
    msg.push_str(&f.primary);

    let mut querytext: Option<&str> = None;
    let mut querypos: i32 = 0;
    if !f.position.is_empty() {
        if let Some(q) = query {
            querytext = Some(q);
            querypos = f.position.parse().unwrap_or(0);
        } else {
            msg.push_str(&format!(" at character {}", f.position));
        }
    } else if !f.internal_position.is_empty() {
        if !f.internal_query.is_empty() {
            querytext = Some(&f.internal_query);
            querypos = f.internal_position.parse().unwrap_or(0);
        } else {
            msg.push_str(&format!(" at character {}", f.internal_position));
        }
    }
    msg.push('\n');

    if let (Some(q), true) = (querytext, querypos > 0) {
        report_error_position(&mut msg, q, querypos);
    }
    if !f.detail.is_empty() {
        msg.push_str(&format!("DETAIL:  {}\n", f.detail));
    }
    if !f.hint.is_empty() {
        msg.push_str(&format!("HINT:  {}\n", f.hint));
    }
    if !f.internal_query.is_empty() {
        msg.push_str(&format!("QUERY:  {}\n", f.internal_query));
    }
    if is_error && !f.context.is_empty() {
        msg.push_str(&format!("CONTEXT:  {}\n", f.context));
    }
    msg
}

/// Port of reportErrorPosition: emit "LINE %d: <excerpt>\n<spaces>^\n".
/// loc is the 1-based CHARACTER index into query.
fn report_error_position(msg: &mut String, query: &str, loc: i32) {
    let mut loc = loc - 1; // 0-based
    if loc < 0 {
        return;
    }
    // Tabs are displayed as spaces (C mutates its working copy).
    let wquery: String = query.replace('\t', " ");
    let chars: Vec<char> = wquery.chars().collect();

    // scridx[cno] = starting screen column of logical char cno.
    let mut scridx: Vec<i32> = Vec::with_capacity(chars.len() + 1);
    let mut scroffset: i32 = 0;
    let mut loc_line: i32 = 1;
    let mut ibeg: usize = 0;
    let mut iend: i32 = -1;
    let mut cno: usize = 0;
    while cno < chars.len() {
        let ch = chars[cno];
        scridx.push(scroffset);
        if ch == '\r' || ch == '\n' {
            if (cno as i32) < loc {
                if ch == '\r' || cno == 0 || chars[cno - 1] != '\r' {
                    loc_line += 1;
                }
                ibeg = cno + 1;
            } else {
                iend = cno as i32;
                break;
            }
        }
        let mut buf = [0u8; 4];
        let s = ch.encode_utf8(&mut buf);
        let mut w = char_dsplen(s);
        if w <= 0 {
            w = 1;
        }
        scroffset += w;
        cno += 1;
    }
    if iend < 0 {
        iend = cno as i32;
        scridx.push(scroffset);
    }
    let iend = iend as usize;

    // Print only if loc is within the computed query length.
    if loc as usize > cno {
        return;
    }
    if loc as usize >= scridx.len() {
        loc = (scridx.len() - 1) as i32;
    }
    let locu = loc as usize;

    let mut beg_trunc = false;
    let mut end_trunc = false;
    let mut ibeg = ibeg;
    let mut iend = iend;
    if scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
        if scridx[ibeg] + DISPLAY_SIZE >= scridx[locu] + MIN_RIGHT_CUT {
            while scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
                iend -= 1;
            }
            end_trunc = true;
        } else {
            while scridx[locu] + MIN_RIGHT_CUT < scridx[iend] {
                iend -= 1;
                end_trunc = true;
            }
            while scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
                ibeg += 1;
                beg_trunc = true;
            }
        }
    }

    let mut prefix = format!("LINE {loc_line}: ");
    if beg_trunc {
        prefix.push_str("...");
    }
    let prefix_width = dsplen(&prefix) as i32;
    msg.push_str(&prefix);
    let excerpt: String = chars[ibeg..iend].iter().collect();
    msg.push_str(&excerpt);
    if end_trunc {
        msg.push_str("...");
    }
    msg.push('\n');
    let caret_col = prefix_width + (scridx[locu] - scridx[ibeg]);
    for _ in 0..caret_col {
        msg.push(' ');
    }
    msg.push('^');
    msg.push('\n');
}

fn char_dsplen(s: &str) -> i32 {
    wchar::pg_encoding_dsplen(wchar::PG_UTF8, s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caret_basic() {
        let f = ErrorFields {
            severity: "ERROR".into(),
            primary: "relation \"nosuch\" does not exist".into(),
            position: "15".into(),
            ..Default::default()
        };
        let m = build_message(&f, Some("select * from nosuch;"), true);
        assert_eq!(
            m,
            "ERROR:  relation \"nosuch\" does not exist\nLINE 1: select * from nosuch;\n                      ^\n"
        );
    }

    #[test]
    fn caret_second_line() {
        let f = ErrorFields {
            severity: "ERROR".into(),
            primary: "x".into(),
            position: "10".into(),
            ..Default::default()
        };
        // "select\n1 bogus;" -> position 10 = 'b' (1-based chars incl \n)
        let m = build_message(&f, Some("select\n1 bogus;"), true);
        assert!(m.contains("LINE 2: 1 bogus;"), "{m}");
        assert!(m.ends_with("LINE 2: 1 bogus;\n          ^\n"), "{m}");
    }
}
