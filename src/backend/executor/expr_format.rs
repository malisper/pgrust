use super::ExecError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignKind {
    S,
    Mi,
    Pl,
    Sg,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Digit9,
    Digit0,
    Decimal,
    Group,
    Literal(String),
    Sign(SignKind),
}

pub(crate) fn to_char_int(value: i128, format: &str) -> Result<String, ExecError> {
    let mut parser = FormatParser::new(format);
    let spec = parser.parse()?;
    if spec.roman {
        return Ok(format_roman(value, spec.fill_mode));
    }
    if spec.scientific {
        return Ok(format_scientific(value, &spec));
    }
    Ok(format_standard(value, &spec))
}

#[derive(Debug)]
struct FormatSpec {
    fill_mode: bool,
    ordinal_lower: bool,
    ordinal: bool,
    angle_pr: bool,
    scientific: bool,
    roman: bool,
    scale_digits: usize,
    tokens: Vec<Token>,
}

struct FormatParser<'a> {
    chars: Vec<char>,
    idx: usize,
    raw: &'a str,
}

impl<'a> FormatParser<'a> {
    fn new(raw: &'a str) -> Self {
        Self {
            chars: raw.chars().collect(),
            idx: 0,
            raw,
        }
    }

    fn parse(&mut self) -> Result<FormatSpec, ExecError> {
        let mut fill_mode = false;
        if self.peek_ci("FM") {
            self.idx += 2;
            fill_mode = true;
        }

        let mut tokens = Vec::new();
        while self.idx < self.chars.len() {
            if self.peek_ci("PR") || self.peek_ci("TH") || self.peek_ci("EEEE") {
                break;
            }
            if self.peek_ci("MI") {
                self.idx += 2;
                tokens.push(Token::Sign(SignKind::Mi));
                continue;
            }
            if self.peek_ci("PL") {
                self.idx += 2;
                tokens.push(Token::Sign(SignKind::Pl));
                continue;
            }
            if self.peek_ci("SG") {
                self.idx += 2;
                tokens.push(Token::Sign(SignKind::Sg));
                continue;
            }

            let ch = self.chars[self.idx];
            self.idx += 1;
            match ch {
                '9' => tokens.push(Token::Digit9),
                '0' => tokens.push(Token::Digit0),
                'S' | 's' => tokens.push(Token::Sign(SignKind::S)),
                'D' | 'd' | '.' => tokens.push(Token::Decimal),
                'G' | 'g' | ',' => tokens.push(Token::Group),
                'L' | 'l' => tokens.push(Token::Literal(" ".into())),
                'V' | 'v' => {
                    let scale_digits = self
                        .chars
                        .iter()
                        .skip(self.idx)
                        .filter(|&&c| c == '9' || c == '0')
                        .count();
                    let rest = self.parse()?;
                    let mut spec = FormatSpec {
                        fill_mode,
                        ordinal_lower: rest.ordinal_lower,
                        ordinal: rest.ordinal,
                        angle_pr: rest.angle_pr,
                        scientific: rest.scientific,
                        roman: rest.roman,
                        scale_digits,
                        tokens,
                    };
                    spec.tokens.extend(rest.tokens);
                    return Ok(spec);
                }
                '"' => tokens.push(Token::Literal(self.parse_quoted_literal())),
                '\\' => {
                    if self.idx < self.chars.len() {
                        let lit = self.chars[self.idx];
                        self.idx += 1;
                        tokens.push(Token::Literal(lit.to_string()));
                    }
                }
                other => tokens.push(Token::Literal(other.to_string())),
            }
        }

        let mut ordinal = false;
        let mut ordinal_lower = false;
        let mut scientific = false;
        let mut angle_pr = false;
        while self.idx < self.chars.len() {
            if self.peek_ci("PR") {
                self.idx += 2;
                angle_pr = true;
            } else if self.peek_ci("TH") {
                ordinal = true;
                ordinal_lower = self.peek_exact("th");
                self.idx += 2;
            } else if self.peek_ci("EEEE") {
                self.idx += 4;
                scientific = true;
            } else {
                break;
            }
        }

        let roman = self.idx == self.chars.len() && self.raw[self.raw.len().saturating_sub(2)..].eq_ignore_ascii_case("RN");
        if roman {
            tokens.clear();
        }

        Ok(FormatSpec {
            fill_mode,
            ordinal_lower,
            ordinal,
            angle_pr,
            scientific,
            roman,
            scale_digits: 0,
            tokens,
        })
    }

    fn parse_quoted_literal(&mut self) -> String {
        let mut out = String::new();
        while self.idx < self.chars.len() {
            let ch = self.chars[self.idx];
            self.idx += 1;
            if ch == '"' {
                break;
            }
            if ch == '\\' && self.idx < self.chars.len() {
                out.push(self.chars[self.idx]);
                self.idx += 1;
            } else {
                out.push(ch);
            }
        }
        out
    }

    fn peek_ci(&self, needle: &str) -> bool {
        let end = self.idx + needle.len();
        end <= self.chars.len()
            && self.chars[self.idx..end]
                .iter()
                .copied()
                .collect::<String>()
                .eq_ignore_ascii_case(needle)
    }

    fn peek_exact(&self, needle: &str) -> bool {
        let end = self.idx + needle.len();
        end <= self.chars.len()
            && self.chars[self.idx..end].iter().copied().collect::<String>() == needle
    }
}

fn format_standard(value: i128, spec: &FormatSpec) -> String {
    let negative = value < 0;
    let abs_value = value.unsigned_abs();
    let scaled = abs_value.saturating_mul(10_u128.saturating_pow(spec.scale_digits as u32));
    let int_digits = scaled.to_string();

    let decimal_idx = spec
        .tokens
        .iter()
        .position(|token| matches!(token, Token::Decimal));
    let int_end = decimal_idx.unwrap_or(spec.tokens.len());

    let mut rendered = spec
        .tokens
        .iter()
        .map(|token| match token {
            Token::Digit9 | Token::Digit0 => String::new(),
            Token::Decimal => ".".into(),
            Token::Group => ",".into(),
            Token::Literal(text) => text.clone(),
            Token::Sign(_) => String::new(),
        })
        .collect::<Vec<_>>();

    let mut digit_chars = int_digits.chars().rev();
    for idx in (0..int_end).rev() {
        match spec.tokens[idx] {
            Token::Digit9 => {
                rendered[idx] = digit_chars.next().map(|ch| ch.to_string()).unwrap_or_else(|| " ".into());
            }
            Token::Digit0 => {
                rendered[idx] = digit_chars.next().map(|ch| ch.to_string()).unwrap_or_else(|| "0".into());
            }
            _ => {}
        }
    }

    let mut seen_digit = false;
    for idx in 0..int_end {
        match spec.tokens[idx] {
            Token::Digit9 | Token::Digit0 => {
                if rendered[idx].trim() != "" {
                    seen_digit = true;
                }
            }
            Token::Group if !seen_digit => rendered[idx] = " ".into(),
            _ => {}
        }
    }

    if let Some(dot) = decimal_idx {
        for idx in dot + 1..spec.tokens.len() {
            match spec.tokens[idx] {
                Token::Digit9 => {
                    rendered[idx] = if spec.fill_mode { " ".into() } else { "0".into() }
                }
                Token::Digit0 => rendered[idx] = "0".into(),
                _ => {}
            }
        }
    }

    let sign_text = |kind: SignKind, negative: bool| match kind {
        SignKind::S | SignKind::Sg => {
            if negative {
                "-"
            } else {
                "+"
            }
        }
        SignKind::Mi => {
            if negative {
                "-"
            } else {
                " "
            }
        }
        SignKind::Pl => {
            if negative {
                "-"
            } else {
                "+"
            }
        }
    };

    for (idx, token) in spec.tokens.iter().enumerate() {
        if let Token::Sign(kind) = token {
            rendered[idx] = match kind {
                SignKind::S if idx == 0 => " ".into(),
                SignKind::Pl if negative => " ".into(),
                _ => sign_text(*kind, negative).into(),
            };
        }
    }

    if spec.tokens.iter().take(int_end).any(|token| matches!(token, Token::Digit0)) {
        for idx in 0..int_end {
            if matches!(spec.tokens[idx], Token::Digit9) && rendered[idx] == " " {
                rendered[idx] = "0".into();
            }
        }
    }

    for (idx, token) in spec.tokens.iter().enumerate() {
        if idx == 0 && matches!(token, Token::Sign(SignKind::S)) {
            if let Some(target_idx) = rendered
                .iter()
                .enumerate()
                .find(|(_, cell)| cell.chars().any(|ch| ch.is_ascii_digit()))
                .map(|(digit_idx, _)| digit_idx.saturating_sub(1))
            {
                rendered[target_idx] = sign_text(SignKind::S, negative).into();
            }
        }
    }

    let mut out = rendered.concat();
    if !spec.tokens.iter().any(|token| matches!(token, Token::Sign(_))) && !spec.angle_pr {
        out = if negative
            && matches!(spec.tokens.first(), Some(Token::Literal(space)) if space == " ")
        {
            if let Some(digit_idx) = out.find(|ch: char| ch.is_ascii_digit()) {
                let sign_idx = digit_idx.saturating_sub(1);
                let mut chars = out.chars().collect::<Vec<_>>();
                chars[sign_idx] = '-';
                chars.into_iter().collect()
            } else {
                format!("-{out}")
            }
        } else if negative {
            format!("-{out}")
        } else {
            format!(" {out}")
        };
    }

    if negative && spec.tokens.iter().any(|token| matches!(token, Token::Sign(SignKind::Pl))) {
        out = format!("-{}", out.trim_start());
    }

    if spec.ordinal && !negative {
        let suffix = if spec.ordinal_lower {
            ordinal_suffix(abs_value as i128).to_ascii_lowercase()
        } else {
            ordinal_suffix(abs_value as i128).to_string()
        };
        out.push_str(&suffix);
    }

    if spec.angle_pr {
        if negative {
            out = format!("<{}>", out.trim().trim_start_matches('-').trim());
        } else {
            out = format!(" {out} ");
        }
    }

    if spec.fill_mode {
        out = out.trim().to_string();
    }
    out
}

fn format_scientific(value: i128, spec: &FormatSpec) -> String {
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let digits = abs.to_string();
    let frac_digits = spec
        .tokens
        .iter()
        .skip_while(|token| !matches!(token, Token::Decimal))
        .skip(1)
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let exponent = digits.len().saturating_sub(1) as i32;
    let mut mantissa = digits.chars().next().unwrap_or('0').to_string();
    if frac_digits > 0 {
        mantissa.push('.');
        let tail = digits.chars().skip(1).collect::<String>();
        let mut frac = tail.chars().take(frac_digits).collect::<String>();
        while frac.len() < frac_digits {
            frac.push('0');
        }
        mantissa.push_str(&frac);
    }
    let sign = if negative { "-" } else { " " };
    format!("{sign}{mantissa}e{exponent:+03}")
}

fn format_roman(value: i128, fill_mode: bool) -> String {
    if value <= 0 || value > 3999 {
        return "#".repeat(15);
    }
    let mut n = value as usize;
    let map = [
        (1000, "M"),
        (900, "CM"),
        (500, "D"),
        (400, "CD"),
        (100, "C"),
        (90, "XC"),
        (50, "L"),
        (40, "XL"),
        (10, "X"),
        (9, "IX"),
        (5, "V"),
        (4, "IV"),
        (1, "I"),
    ];
    let mut out = String::new();
    for (unit, numeral) in map {
        while n >= unit {
            out.push_str(numeral);
            n -= unit;
        }
    }
    if fill_mode {
        out
    } else {
        format!("{out:>15}")
    }
}

fn ordinal_suffix(value: i128) -> &'static str {
    let rem100 = value % 100;
    if (11..=13).contains(&rem100) {
        "TH"
    } else {
        match value % 10 {
            1 => "ST",
            2 => "ND",
            3 => "RD",
            _ => "TH",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::to_char_int;

    #[test]
    fn formats_grouped_integers() {
        assert_eq!(
            to_char_int(4567890123456789, "9,999,999,999,999,999").unwrap(),
            " 4,567,890,123,456,789"
        );
    }

    #[test]
    fn formats_roman_numerals() {
        assert_eq!(to_char_int(456, "FMRN").unwrap(), "CDLVI");
        assert_eq!(to_char_int(4567890123456789, "FMRN").unwrap(), "###############");
    }

    #[test]
    fn formats_scientific_notation() {
        assert_eq!(to_char_int(1234, "9.99EEEE").unwrap(), " 1.23e+03");
        assert_eq!(to_char_int(-1234, "9.99eeee").unwrap(), "-1.23e+03");
    }
}
