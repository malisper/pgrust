//! Format-picture parser (`parse_format`) and number-description preparation
//! (`NUMDesc_prepare`).
//!
//! Faithful port of formatting.c:1202-1529 (PG 18.3).
//!
//! The only genuine external is `pg_mblen` (mbutils.c / wchar.c), routed through
//! the `backend_utils_mb_mbutils_seams::pg_mblen_range` slot.

use types_error::{PgError, PgResult};
use types_error::{ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_SYNTAX_ERROR};

use crate::case::{index_seq_search, is_separator_char, suff_search};
use crate::tables::*;

fn syntax_error(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into()).with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

/// C: `pg_mblen` of the lead byte of `s`, via the mbutils seam (infallible).
fn pg_mblen_cstr(s: &[u8]) -> i32 {
    backend_utils_mb_mbutils_seams::pg_mblen_range::call(s)
}

/// C: `NUMDesc_prepare` (formatting.c:1202).
pub fn numdesc_prepare(num: &mut NUMDesc, key_id: i32, is_action: bool) -> PgResult<()> {
    // C: if (n->type != NODE_TYPE_ACTION) return;
    if !is_action {
        return Ok(());
    }

    if num.is_eeee() && key_id != NUM_E {
        return Err(syntax_error("\"EEEE\" must be the last pattern used"));
    }

    match key_id {
        NUM_9 => {
            if num.is_bracket() {
                return Err(syntax_error("\"9\" must be ahead of \"PR\""));
            }
            if num.is_multi() {
                num.multi += 1;
            } else if num.is_decimal() {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        NUM_0 => {
            if num.is_bracket() {
                return Err(syntax_error("\"0\" must be ahead of \"PR\""));
            }
            if !num.is_zero() && !num.is_decimal() {
                num.flag |= NUM_F_ZERO;
                num.zero_start = num.pre + 1;
            }
            if !num.is_decimal() {
                num.pre += 1;
            } else {
                num.post += 1;
            }
            num.zero_end = num.pre + num.post;
        }
        NUM_B => {
            if num.pre == 0 && num.post == 0 && !num.is_zero() {
                num.flag |= NUM_F_BLANK;
            }
        }
        NUM_D | NUM_DEC => {
            if key_id == NUM_D {
                num.flag |= NUM_F_LDECIMAL;
                num.need_locale = 1;
            }
            // FALLTHROUGH from NUM_D to NUM_DEC.
            if num.is_decimal() {
                return Err(syntax_error("multiple decimal points"));
            }
            if num.is_multi() {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= NUM_F_DECIMAL;
        }
        NUM_FM => {
            num.flag |= NUM_F_FILLMODE;
        }
        NUM_S => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" twice"));
            }
            if num.is_plus() || num.is_minus() || num.is_bracket() {
                return Err(syntax_error(
                    "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together",
                ));
            }
            if !num.is_decimal() {
                num.lsign = NUM_LSIGN_PRE;
                num.pre_lsign_num = num.pre;
                num.need_locale = 1;
                num.flag |= NUM_F_LSIGN;
            } else if num.lsign == NUM_LSIGN_NONE {
                num.lsign = NUM_LSIGN_POST;
                num.need_locale = 1;
                num.flag |= NUM_F_LSIGN;
            }
        }
        NUM_MI => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" and \"MI\" together"));
            }
            num.flag |= NUM_F_MINUS;
            if num.is_decimal() {
                num.flag |= NUM_F_MINUS_POST;
            }
        }
        NUM_PL => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" and \"PL\" together"));
            }
            num.flag |= NUM_F_PLUS;
            if num.is_decimal() {
                num.flag |= NUM_F_PLUS_POST;
            }
        }
        NUM_SG => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" and \"SG\" together"));
            }
            num.flag |= NUM_F_MINUS;
            num.flag |= NUM_F_PLUS;
        }
        NUM_PR => {
            if num.is_lsign() || num.is_plus() || num.is_minus() {
                return Err(syntax_error(
                    "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                ));
            }
            num.flag |= NUM_F_BRACKET;
        }
        NUM_RN_LOWER | NUM_RN => {
            if num.is_roman() {
                return Err(syntax_error("cannot use \"RN\" twice"));
            }
            num.flag |= NUM_F_ROMAN;
        }
        NUM_L | NUM_G => {
            num.need_locale = 1;
        }
        NUM_V => {
            if num.is_decimal() {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= NUM_F_MULTI;
        }
        NUM_E => {
            if num.is_eeee() {
                return Err(syntax_error("cannot use \"EEEE\" twice"));
            }
            if num.is_blank()
                || num.is_fillmode()
                || num.is_lsign()
                || num.is_bracket()
                || num.is_minus()
                || num.is_plus()
                || num.is_roman()
                || num.is_multi()
            {
                return Err(syntax_error("\"EEEE\" is incompatible with other formats")
                    .with_detail(
                        "\"EEEE\" may only be used together with digit and decimal point patterns.",
                    ));
            }
            num.flag |= NUM_F_EEEE;
        }
        _ => {}
    }

    if num.is_roman() && (num.flag & !(NUM_F_ROMAN | NUM_F_FILLMODE)) != 0 {
        return Err(syntax_error("\"RN\" is incompatible with other formats")
            .with_detail("\"RN\" may only be used together with \"FM\"."));
    }

    Ok(())
}

/// C: `parse_format` (formatting.c:1388).
///
/// Parses `str` (a NUL-free byte slice) into a vector of `FormatNode`s
/// terminated by a `NODE_TYPE_END` node, mirroring the C output array which the
/// callers walk until they hit `NODE_TYPE_END`.
pub fn parse_format(
    str: &[u8],
    kw: &[KeyWord],
    suf: &[KeySuffix],
    index: &[i32],
    flags: u32,
    num: Option<&mut NUMDesc>,
) -> PgResult<Vec<FormatNode>> {
    let mut nodes: Vec<FormatNode> = Vec::new();
    let mut num = num;
    let mut pos = 0usize; // position in str (C's moving `str` pointer)

    while pos < str.len() && str[pos] != 0 {
        let mut suffix: u8 = 0;

        // Prefix
        if (flags & DCH_FLAG) != 0 {
            if let Some(si) = suff_search(&str[pos..], suf, SUFFTYPE_PREFIX) {
                suffix |= suf[si].id;
                if suf[si].len != 0 {
                    pos += suf[si].len;
                }
            }
        }

        // Keyword
        if pos < str.len() && str[pos] != 0 && index_seq_search(&str[pos..], kw, index).is_some() {
            let ki = index_seq_search(&str[pos..], kw, index).unwrap();
            let mut node = FormatNode {
                typ: NODE_TYPE_ACTION,
                suffix,
                key: ki as i32,
                ..Default::default()
            };
            if kw[ki].len != 0 {
                pos += kw[ki].len;
            }

            // NUM version: prepare global NUMDesc struct
            if (flags & NUM_FLAG) != 0 {
                if let Some(n) = num.as_deref_mut() {
                    numdesc_prepare(n, kw[ki].id, true)?;
                }
            }

            // Postfix
            if (flags & DCH_FLAG) != 0 && pos < str.len() && str[pos] != 0 {
                if let Some(si) = suff_search(&str[pos..], suf, SUFFTYPE_POSTFIX) {
                    node.suffix |= suf[si].id;
                    if suf[si].len != 0 {
                        pos += suf[si].len;
                    }
                }
            }

            nodes.push(node);
        } else if pos < str.len() && str[pos] != 0 {
            if (flags & STD_FLAG) != 0 && str[pos] != b'"' {
                // Standard mode separators: "-./,':; "
                if !b"-./,':; ".contains(&str[pos]) {
                    let chlen = pg_mblen_cstr(&str[pos..]) as usize;
                    let bad = String::from_utf8_lossy(&str[pos..pos + chlen]).into_owned();
                    return Err(PgError::error(format!(
                        "invalid datetime format separator: \"{bad}\""
                    ))
                    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT));
                }

                let mut character = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
                character[0] = str[pos];
                let node = FormatNode {
                    typ: if str[pos] == b' ' {
                        NODE_TYPE_SPACE
                    } else {
                        NODE_TYPE_SEPARATOR
                    },
                    character,
                    key: -1,
                    suffix: 0,
                };
                nodes.push(node);
                pos += 1;
            } else if str[pos] == b'"' {
                // Process double-quoted literal string.
                pos += 1;
                while pos < str.len() && str[pos] != 0 {
                    if str[pos] == b'"' {
                        pos += 1;
                        break;
                    }
                    // backslash quotes the next character, if any
                    if str[pos] == b'\\' && pos + 1 < str.len() && str[pos + 1] != 0 {
                        pos += 1;
                    }
                    let chlen = pg_mblen_cstr(&str[pos..]) as usize;
                    let mut node = FormatNode {
                        typ: NODE_TYPE_CHAR,
                        ..Default::default()
                    };
                    node.character[..chlen].copy_from_slice(&str[pos..pos + chlen]);
                    node.character[chlen] = 0;
                    node.key = -1;
                    node.suffix = 0;
                    nodes.push(node);
                    pos += chlen;
                }
            } else {
                // Outside double-quoted strings, backslash is only special if it
                // immediately precedes a double quote.
                if str[pos] == b'\\' && pos + 1 < str.len() && str[pos + 1] == b'"' {
                    pos += 1;
                }
                let chlen = pg_mblen_cstr(&str[pos..]) as usize;

                let mut character = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
                character[..chlen].copy_from_slice(&str[pos..pos + chlen]);
                let node = FormatNode {
                    typ: if (flags & DCH_FLAG) != 0 && is_separator_char(str[pos]) {
                        NODE_TYPE_SEPARATOR
                    } else if is_c_space(str[pos]) {
                        NODE_TYPE_SPACE
                    } else {
                        NODE_TYPE_CHAR
                    },
                    character,
                    key: -1,
                    suffix: 0,
                };
                nodes.push(node);
                pos += chlen;
            }
        }
    }

    // Terminator.
    nodes.push(FormatNode {
        typ: NODE_TYPE_END,
        suffix: 0,
        ..Default::default()
    });

    Ok(nodes)
}

/// C `isspace` for the "C" locale on a `char` (unsigned-promoted).
#[inline]
pub fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numdesc_counts_digits_no_mb() {
        // `999.99` contains no multibyte chars and no literal text, so it never
        // touches the pg_mblen seam — exercises NUMDesc_prepare end to end.
        let mut num = NUMDesc::default();
        let nodes = parse_format(
            b"999.99",
            NUM_KEYWORDS,
            &[],
            &NUM_INDEX,
            NUM_FLAG,
            Some(&mut num),
        )
        .unwrap();
        assert_eq!(num.pre, 3);
        assert_eq!(num.post, 2);
        assert!(num.is_decimal());
        assert_eq!(nodes.last().unwrap().typ, NODE_TYPE_END);
    }

    #[test]
    fn eeee_twice_errors() {
        let mut num = NUMDesc::default();
        let err = parse_format(
            b"EEEEEEEE",
            NUM_KEYWORDS,
            &[],
            &NUM_INDEX,
            NUM_FLAG,
            Some(&mut num),
        )
        .unwrap_err();
        assert_eq!(err.message(), "cannot use \"EEEE\" twice");
    }

    #[test]
    fn numdesc_prepare_direct() {
        let mut num = NUMDesc::default();
        numdesc_prepare(&mut num, NUM_9, true).unwrap();
        numdesc_prepare(&mut num, NUM_DEC, true).unwrap();
        numdesc_prepare(&mut num, NUM_9, true).unwrap();
        assert_eq!(num.pre, 1);
        assert_eq!(num.post, 1);
        assert!(num.is_decimal());
    }
}
