use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_SYNTAX_ERROR,
};

use crate::case::{index_seq_search, is_separator_char, suff_search};
use crate::tables::*;

fn syntax_error(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into()).with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

// pg_mblen does not validate; the range-clamped path only Errs on overrun,
// where the clamped length is the slice length (dead path falls to s.len()).
fn pg_mblen_cstr(s: &[u8]) -> i32 {
    mbutils::pg_mblen_range(s).unwrap_or(s.len() as i32)
}

pub fn numdesc_prepare(num: &mut NUMDesc, key_id: i32, is_action: bool) -> PgResult<()> {
    if !is_action {
        return Ok(());
    }

    if num.is_eeee() && key_id != NUM_E {
        return Err(syntax_error("\"EEEE\" must be the last pattern used").into());
    }

    match key_id {
        NUM_9 => {
            if num.is_bracket() {
                return Err(syntax_error("\"9\" must be ahead of \"PR\"").into());
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
                return Err(syntax_error("\"0\" must be ahead of \"PR\"").into());
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
            if num.is_decimal() {
                return Err(syntax_error("multiple decimal points").into());
            }
            if num.is_multi() {
                return Err(syntax_error("cannot use \"V\" and decimal point together").into());
            }
            num.flag |= NUM_F_DECIMAL;
        }
        NUM_FM => {
            num.flag |= NUM_F_FILLMODE;
        }
        NUM_S => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" twice").into());
            }
            if num.is_plus() || num.is_minus() || num.is_bracket() {
                return Err(syntax_error(
                    "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together",
                )
                .into());
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
                return Err(syntax_error("cannot use \"S\" and \"MI\" together").into());
            }
            num.flag |= NUM_F_MINUS;
            if num.is_decimal() {
                num.flag |= NUM_F_MINUS_POST;
            }
        }
        NUM_PL => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" and \"PL\" together").into());
            }
            num.flag |= NUM_F_PLUS;
            if num.is_decimal() {
                num.flag |= NUM_F_PLUS_POST;
            }
        }
        NUM_SG => {
            if num.is_lsign() {
                return Err(syntax_error("cannot use \"S\" and \"SG\" together").into());
            }
            num.flag |= NUM_F_MINUS;
            num.flag |= NUM_F_PLUS;
        }
        NUM_PR => {
            if num.is_lsign() || num.is_plus() || num.is_minus() {
                return Err(syntax_error(
                    "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                )
                .into());
            }
            num.flag |= NUM_F_BRACKET;
        }
        NUM_RN_LOWER | NUM_RN => {
            if num.is_roman() {
                return Err(syntax_error("cannot use \"RN\" twice").into());
            }
            num.flag |= NUM_F_ROMAN;
        }
        NUM_L | NUM_G => {
            num.need_locale = 1;
        }
        NUM_V => {
            if num.is_decimal() {
                return Err(syntax_error("cannot use \"V\" and decimal point together").into());
            }
            num.flag |= NUM_F_MULTI;
        }
        NUM_E => {
            if num.is_eeee() {
                return Err(syntax_error("cannot use \"EEEE\" twice").into());
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
                    )
                    .into());
            }
            num.flag |= NUM_F_EEEE;
        }
        _ => {}
    }

    if num.is_roman() && (num.flag & !(NUM_F_ROMAN | NUM_F_FILLMODE)) != 0 {
        return Err(syntax_error("\"RN\" is incompatible with other formats")
            .with_detail("\"RN\" may only be used together with \"FM\".")
            .into());
    }

    Ok(())
}

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
    let mut pos = 0usize;

    while pos < str.len() && str[pos] != 0 {
        let mut suffix: u8 = 0;

        if (flags & DCH_FLAG) != 0 {
            if let Some(si) = suff_search(&str[pos..], suf, SUFFTYPE_PREFIX) {
                suffix |= suf[si].id;
                if suf[si].len != 0 {
                    pos += suf[si].len;
                }
            }
        }

        if pos < str.len() && str[pos] != 0 {
            if let Some(ki) = index_seq_search(&str[pos..], kw, index) {
                let mut node = FormatNode {
                    typ: NODE_TYPE_ACTION,
                    suffix,
                    key: ki as i32,
                    ..Default::default()
                };
                if kw[ki].len != 0 {
                    pos += kw[ki].len;
                }

                if (flags & NUM_FLAG) != 0 {
                    if let Some(n) = num.as_deref_mut() {
                        numdesc_prepare(n, kw[ki].id, true)?;
                    }
                }

                if (flags & DCH_FLAG) != 0 && pos < str.len() && str[pos] != 0 {
                    if let Some(si) = suff_search(&str[pos..], suf, SUFFTYPE_POSTFIX) {
                        node.suffix |= suf[si].id;
                        if suf[si].len != 0 {
                            pos += suf[si].len;
                        }
                    }
                }

                nodes.push(node);
                continue;
            }
        }

        if pos < str.len() && str[pos] != 0 {
            if (flags & STD_FLAG) != 0 && str[pos] != b'"' {
                if !b"-./,':; ".contains(&str[pos]) {
                    let chlen = pg_mblen_cstr(&str[pos..]) as usize;
                    let bad = String::from_utf8_lossy(&str[pos..pos + chlen]).into_owned();
                    return Err(PgError::error(format!(
                        "invalid datetime format separator: \"{bad}\""
                    ))
                    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
                    .into());
                }

                let mut character = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
                character[0] = str[pos];
                nodes.push(FormatNode {
                    typ: if str[pos] == b' ' {
                        NODE_TYPE_SPACE
                    } else {
                        NODE_TYPE_SEPARATOR
                    },
                    character,
                    key: -1,
                    suffix: 0,
                });
                pos += 1;
            } else if str[pos] == b'"' {
                pos += 1;
                while pos < str.len() && str[pos] != 0 {
                    if str[pos] == b'"' {
                        pos += 1;
                        break;
                    }
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
                    nodes.push(node);
                    pos += chlen;
                }
            } else {
                if str[pos] == b'\\' && pos + 1 < str.len() && str[pos + 1] == b'"' {
                    pos += 1;
                }
                let chlen = pg_mblen_cstr(&str[pos..]) as usize;
                let mut character = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
                character[..chlen].copy_from_slice(&str[pos..pos + chlen]);
                nodes.push(FormatNode {
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
                });
                pos += chlen;
            }
        }
    }

    nodes.push(FormatNode {
        typ: NODE_TYPE_END,
        suffix: 0,
        ..Default::default()
    });

    Ok(nodes)
}

#[inline]
pub fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}
