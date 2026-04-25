use super::ExecError;
use super::expr_ops::parse_numeric_text;
use crate::include::nodes::datum::NumericValue;
use num_traits::Signed;

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
        return Ok(format_roman(value, spec.fill_mode, spec.roman_lower));
    }
    if spec.scientific {
        return Ok(format_scientific(value, &spec));
    }
    Ok(format_standard(value, &spec))
}

pub(crate) fn to_char_numeric(value: &NumericValue, format: &str) -> Result<String, ExecError> {
    let value = value.normalize_display_scale();
    let mut parser = FormatParser::new(format);
    let spec = parser.parse()?;
    if spec.roman {
        return Ok(format_roman_numeric(
            &value,
            spec.fill_mode,
            spec.roman_lower,
        ));
    }
    if spec.scientific {
        return format_scientific_numeric(&value, &spec);
    }
    Ok(format_standard_numeric(&value, &spec))
}

pub(crate) fn to_char_float(value: f64, format: &str) -> Result<String, ExecError> {
    let mut parser = FormatParser::new(format);
    let spec = parser.parse()?;
    if spec.roman {
        let rounded = value.round();
        let intvalue =
            if !rounded.is_nan() && rounded >= i32::MIN as f64 && rounded <= i32::MAX as f64 {
                rounded as i32
            } else {
                i32::MAX
            };
        return Ok(format_roman(
            intvalue as i128,
            spec.fill_mode,
            spec.roman_lower,
        ));
    }
    if spec.scientific {
        let numeric = parse_numeric_text(&value.to_string())
            .ok_or_else(|| ExecError::InvalidNumericInput(value.to_string()))?;
        return format_scientific_numeric(&numeric, &spec);
    }

    let adjusted_value = if spec.scale_digits == 0 {
        value
    } else {
        value * 10f64.powi(spec.scale_digits as i32)
    };
    let decimal_idx = spec
        .tokens
        .iter()
        .position(|token| matches!(token, Token::Decimal));
    let int_end = decimal_idx.unwrap_or(spec.tokens.len());
    let int_slots = spec
        .tokens
        .iter()
        .take(int_end)
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let post_slots = spec
        .tokens
        .iter()
        .skip(int_end + usize::from(decimal_idx.is_some()))
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();

    let abs_text = format!("{:.0}", adjusted_value.abs());
    let pre_len = abs_text.len();
    let max_digits = f64::DIGITS as usize;
    let effective_post = if pre_len >= max_digits {
        0
    } else {
        post_slots.min(max_digits.saturating_sub(pre_len))
    };

    let mut adjusted_spec = truncate_fractional_digit_tokens(&spec, effective_post);
    adjusted_spec.scale_digits = 0;

    if pre_len > int_slots {
        return Ok(overflow_pattern(
            &adjusted_spec,
            adjusted_value.is_sign_negative(),
        ));
    }

    let rounded_text = format!("{adjusted_value:.effective_post$}");
    let numeric = parse_numeric_text(&rounded_text)
        .ok_or_else(|| ExecError::InvalidNumericInput(rounded_text.clone()))?;
    Ok(format_standard_numeric(&numeric, &adjusted_spec))
}

pub(crate) fn to_number_numeric(input: &str, format: &str) -> Result<NumericValue, ExecError> {
    let mut parser = FormatParser::new(format);
    let spec = parser.parse()?;
    if spec.roman {
        return parse_roman_to_number(input, format);
    }

    let mut negative = false;
    let mut text = input.trim().to_string();
    if spec.angle_pr && text.starts_with('<') && text.ends_with('>') && text.len() >= 2 {
        negative = true;
        text = text[1..text.len() - 1].to_string();
    } else if let Some(idx) = text.find('-') {
        negative = true;
        text.remove(idx);
    } else if let Some(idx) = text.find('+') {
        text.remove(idx);
    }
    text = text.replace('$', "");

    let mut chars = text.chars().peekable();
    let mut digits_before = String::new();
    let mut digits_after = String::new();
    let mut in_fraction = false;

    for token in &spec.tokens {
        match token {
            Token::Digit9 | Token::Digit0 => {
                while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
                    chars.next();
                }
                match chars.peek().copied() {
                    Some(ch) if ch.is_ascii_digit() => {
                        if in_fraction {
                            digits_after.push(ch);
                        } else {
                            digits_before.push(ch);
                        }
                        chars.next();
                    }
                    _ => {}
                }
            }
            Token::Decimal => {
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' {
                        chars.next();
                        break;
                    }
                    if ch.is_whitespace() {
                        chars.next();
                        continue;
                    }
                    break;
                }
                in_fraction = true;
            }
            Token::Group => {
                while let Some(ch) = chars.peek().copied() {
                    if ch == ',' || ch.is_whitespace() {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            Token::Literal(lit) => {
                if lit == " " {
                    while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
                        chars.next();
                    }
                } else {
                    for expected in lit.chars() {
                        if chars.peek().copied() == Some(expected) {
                            chars.next();
                        }
                    }
                }
            }
            Token::Sign(_) => {}
        }
    }

    if spec.ordinal {
        while matches!(chars.peek(), Some(ch) if ch.is_ascii_alphabetic()) {
            chars.next();
        }
    }

    if digits_before.is_empty() {
        digits_before.push('0');
    }
    if spec.scale_digits > 0 {
        let mut all_digits = digits_before;
        all_digits.push_str(&digits_after);
        if all_digits.is_empty() {
            all_digits.push('0');
        }
        let mut scaled =
            parse_numeric_text(&format!("{}{all_digits}", if negative { "-" } else { "" }))
                .ok_or_else(|| ExecError::InvalidNumericInput(input.to_string()))?;
        let divisor_text = format!("1{}", "0".repeat(spec.scale_digits));
        let divisor = parse_numeric_text(&divisor_text)
            .expect("power-of-ten divisor for to_number V format should parse");
        scaled = scaled
            .div(&divisor, 18)
            .ok_or_else(|| ExecError::InvalidNumericInput(input.to_string()))?;
        return Ok(scaled);
    }

    let rendered = if digits_after.is_empty() {
        format!("{}{digits_before}", if negative { "-" } else { "" })
    } else {
        format!(
            "{}{}.{}",
            if negative { "-" } else { "" },
            digits_before,
            digits_after
        )
    };

    parse_numeric_text(&rendered).ok_or_else(|| ExecError::InvalidNumericInput(input.to_string()))
}

#[derive(Clone, Debug)]
struct FormatSpec {
    fill_mode: bool,
    ordinal_lower: bool,
    ordinal: bool,
    roman_lower: bool,
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
                        roman_lower: rest.roman_lower,
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
                '\\' if self.idx < self.chars.len() && self.chars[self.idx] == '"' => {
                    self.idx += 1;
                    tokens.push(Token::Literal("\"".into()));
                }
                '\\' => tokens.push(Token::Literal("\\".into())),
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

        let roman = self.idx == self.chars.len()
            && self.raw[self.raw.len().saturating_sub(2)..].eq_ignore_ascii_case("RN");
        let roman_lower = roman && self.raw[self.raw.len().saturating_sub(2)..] == *"rn";
        if roman {
            tokens.clear();
        }

        Ok(FormatSpec {
            fill_mode,
            ordinal_lower,
            ordinal,
            roman_lower,
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
            && self.chars[self.idx..end]
                .iter()
                .copied()
                .collect::<String>()
                == needle
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

    let suppress_zero_integer = decimal_idx.is_some()
        && abs_value == 0
        && !spec.fill_mode
        && !spec
            .tokens
            .iter()
            .take(int_end)
            .any(|token| matches!(token, Token::Digit0));
    let int_digits = if suppress_zero_integer {
        String::new()
    } else {
        int_digits
    };
    let mut digit_chars = int_digits.chars().rev();
    for idx in (0..int_end).rev() {
        match spec.tokens[idx] {
            Token::Digit9 => {
                rendered[idx] = digit_chars
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_else(|| " ".into());
            }
            Token::Digit0 => {
                rendered[idx] = digit_chars
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_else(|| "0".into());
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
                    rendered[idx] = if spec.fill_mode {
                        " ".into()
                    } else {
                        "0".into()
                    }
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
                SignKind::S if idx == 0 => {
                    if spec.fill_mode {
                        sign_text(*kind, negative).into()
                    } else {
                        " ".into()
                    }
                }
                SignKind::Pl if negative => " ".into(),
                _ => sign_text(*kind, negative).into(),
            };
        }
    }

    if spec
        .tokens
        .iter()
        .take(int_end)
        .any(|token| matches!(token, Token::Digit0))
    {
        for idx in 0..int_end {
            if matches!(spec.tokens[idx], Token::Digit9) && rendered[idx] == " " {
                rendered[idx] = "0".into();
            }
        }
    }

    if !(spec.fill_mode && matches!(spec.tokens.first(), Some(Token::Sign(SignKind::S)))) {
        move_s_sign_to_number(spec, &mut rendered, negative);
    }
    apply_implicit_sign(spec, &mut rendered, negative);

    let mut out = rendered.concat();

    if negative
        && spec
            .tokens
            .iter()
            .any(|token| matches!(token, Token::Sign(SignKind::Pl)))
    {
        out = format!("-{}", out.trim_start());
    }

    if spec.ordinal && !negative && decimal_idx.is_none() {
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

fn round_decimal_parts(
    negative: bool,
    int_part: &str,
    frac_part: &str,
    frac_slots: usize,
) -> (bool, String, String) {
    let mut digits = String::new();
    digits.push_str(int_part);
    digits.push_str(frac_part);
    if digits.is_empty() {
        digits.push('0');
    }
    let current_frac = frac_part.len();
    if current_frac <= frac_slots {
        let mut padded_frac = frac_part.to_string();
        while padded_frac.len() < frac_slots {
            padded_frac.push('0');
        }
        return (negative, int_part.to_string(), padded_frac);
    }
    let trim = current_frac - frac_slots;
    let split = digits.len() - trim;
    let head = &digits[..split];
    let tail = &digits[split..];
    let mut carry = tail.chars().next().map(|ch| ch >= '5').unwrap_or(false);
    let mut rounded: Vec<u8> = head.bytes().collect();
    if carry {
        for digit in rounded.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            rounded.insert(0, b'1');
        }
    }
    let rounded = String::from_utf8(rounded).unwrap_or_else(|_| "0".to_string());
    let split = rounded.len().saturating_sub(frac_slots);
    let int_part = if frac_slots == 0 {
        rounded.clone()
    } else if split == 0 {
        "0".to_string()
    } else {
        rounded[..split].to_string()
    };
    let frac_part = if frac_slots == 0 {
        String::new()
    } else {
        let mut frac = if split == 0 {
            rounded
        } else {
            rounded[split..].to_string()
        };
        while frac.len() < frac_slots {
            frac.insert(0, '0');
        }
        frac
    };
    (negative, int_part, frac_part)
}

fn shift_value_text_for_v(rendered: &str, scale_digits: usize) -> String {
    if scale_digits == 0 {
        return rendered.to_string();
    }
    let negative = rendered.starts_with('-');
    let unsigned = rendered.trim_start_matches('-');
    let mut parts = unsigned.split('.');
    let int_part = parts.next().unwrap_or("0");
    let frac_part = parts.next().unwrap_or("");
    let mut digits = String::new();
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let frac_len = frac_part.len();
    let new_scale = frac_len.saturating_sub(scale_digits);
    if new_scale == 0 {
        let mut out = if digits.is_empty() {
            "0".to_string()
        } else {
            digits
        };
        if negative && out != "0" {
            out.insert(0, '-');
        }
        return out;
    }
    let split = digits.len().saturating_sub(new_scale);
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if split == 0 {
        out.push('0');
        out.push('.');
        for _ in 0..(new_scale - digits.len()) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

fn split_rendered_decimal(rendered: &str) -> (bool, String, String) {
    let negative = rendered.starts_with('-');
    let unsigned = rendered.trim_start_matches('-');
    let mut parts = unsigned.split('.');
    let int_part = parts.next().unwrap_or("0").to_string();
    let frac_part = parts.next().unwrap_or("").to_string();
    (negative, int_part, frac_part)
}

fn scientific_parts(rendered: &str) -> (bool, String, i32) {
    let (negative, int_part, frac_part) = split_rendered_decimal(rendered);
    let int_trimmed = int_part.trim_start_matches('0');
    if !int_trimmed.is_empty() {
        let exponent = (int_trimmed.len() as i32).saturating_sub(1);
        let mut digits = String::with_capacity(int_trimmed.len() + frac_part.len());
        digits.push_str(int_trimmed);
        digits.push_str(&frac_part);
        return (negative, digits, exponent);
    }
    let leading_zeros = frac_part.chars().take_while(|ch| *ch == '0').count();
    let sig = frac_part[leading_zeros..].to_string();
    if sig.is_empty() {
        return (negative, "0".into(), 0);
    }
    (negative, sig, -((leading_zeros as i32) + 1))
}

fn round_scientific_digits(mut digits: String, precision: usize) -> (String, bool) {
    let needed = precision.saturating_add(1);
    while digits.len() <= needed {
        digits.push('0');
    }
    let carry_digit = digits.as_bytes().get(precision).copied().unwrap_or(b'0');
    let mut rounded = digits[..precision].bytes().collect::<Vec<_>>();
    if carry_digit >= b'5' {
        let mut carry = true;
        for digit in rounded.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            rounded.insert(0, b'1');
        }
    }
    let carried = rounded.len() > precision;
    if carried {
        rounded.truncate(precision);
    }
    while rounded.len() < precision {
        rounded.push(b'0');
    }
    (
        String::from_utf8(rounded).unwrap_or_else(|_| "0".into()),
        carried,
    )
}

fn overflow_pattern(spec: &FormatSpec, negative: bool) -> String {
    let mut rendered = spec
        .tokens
        .iter()
        .map(|token| match token {
            Token::Digit9 | Token::Digit0 => "#".to_string(),
            Token::Decimal => ".".to_string(),
            Token::Group => ",".to_string(),
            Token::Literal(text) => text.clone(),
            Token::Sign(_) => String::new(),
        })
        .collect::<Vec<_>>();

    for (idx, token) in spec.tokens.iter().enumerate() {
        if let Token::Sign(kind) = token {
            rendered[idx] = match kind {
                SignKind::S if idx == 0 && spec.fill_mode => " ".into(),
                SignKind::S | SignKind::Sg => {
                    if negative {
                        "-".into()
                    } else {
                        "+".into()
                    }
                }
                SignKind::Mi => {
                    if negative {
                        "-".into()
                    } else {
                        " ".into()
                    }
                }
                SignKind::Pl => {
                    if negative {
                        " ".into()
                    } else {
                        "+".into()
                    }
                }
            };
        }
    }

    if !(spec.fill_mode && matches!(spec.tokens.first(), Some(Token::Sign(SignKind::S)))) {
        move_s_sign_to_number(spec, &mut rendered, negative);
    }
    apply_implicit_sign(spec, &mut rendered, negative);

    let mut out = if spec.fill_mode {
        trim_fill_mode_edges(spec, &mut rendered);
        rendered.concat()
    } else {
        rendered.concat()
    };
    if spec.angle_pr {
        if negative {
            out = format!("<{}>", out.trim().trim_start_matches('-').trim());
        } else {
            out = format!(" {out} ");
        }
    }
    out
}

fn truncate_fractional_digit_tokens(spec: &FormatSpec, keep: usize) -> FormatSpec {
    let decimal_idx = spec
        .tokens
        .iter()
        .position(|token| matches!(token, Token::Decimal));
    let mut clone = spec.clone();
    let Some(dot_idx) = decimal_idx else {
        return clone;
    };
    let mut kept = 0usize;
    for idx in dot_idx + 1..clone.tokens.len() {
        if matches!(clone.tokens[idx], Token::Digit9 | Token::Digit0) {
            kept += 1;
            if kept > keep {
                clone.tokens[idx] = Token::Literal(String::new());
            }
        }
    }
    clone
}

fn special_value_fixed_text(value: &NumericValue) -> Option<(bool, &'static str)> {
    match value {
        NumericValue::PosInf => Some((false, "Infinity")),
        NumericValue::NegInf => Some((true, "Infinity")),
        NumericValue::NaN => Some((false, "NaN")),
        NumericValue::Finite { .. } => None,
    }
}

fn token_has_visible_number_text(token: &Token, cell: &str) -> bool {
    match token {
        Token::Digit9 | Token::Digit0 => !cell.is_empty() && cell != " ",
        Token::Decimal => cell == ".",
        Token::Group | Token::Literal(_) | Token::Sign(_) => false,
    }
}

fn find_number_anchor(spec: &FormatSpec, rendered: &[String]) -> Option<usize> {
    spec.tokens
        .iter()
        .zip(rendered.iter())
        .position(|(token, cell)| token_has_visible_number_text(token, cell))
}

fn move_s_sign_to_number(spec: &FormatSpec, rendered: &mut [String], negative: bool) {
    if !matches!(spec.tokens.first(), Some(Token::Sign(SignKind::S))) {
        return;
    }
    let Some(anchor_idx) = find_number_anchor(spec, rendered) else {
        return;
    };
    let target_idx = (0..anchor_idx)
        .rev()
        .find(|idx| rendered[*idx] == " ")
        .unwrap_or(anchor_idx);
    rendered[target_idx] = if negative { "-" } else { "+" }.into();
}

fn apply_implicit_sign(spec: &FormatSpec, rendered: &mut [String], negative: bool) {
    if spec
        .tokens
        .iter()
        .any(|token| matches!(token, Token::Sign(_)))
        || spec.angle_pr
    {
        return;
    }
    if spec.fill_mode && !negative {
        return;
    }
    let Some(anchor_idx) = find_number_anchor(spec, rendered) else {
        return;
    };
    let sign = if negative { "-" } else { " " };
    rendered[anchor_idx] = format!("{sign}{}", rendered[anchor_idx]);
}

fn trim_fill_mode_fraction(spec: &FormatSpec, rendered: &mut [String], decimal_idx: usize) {
    let mut trimming = false;
    for idx in (decimal_idx + 1..spec.tokens.len()).rev() {
        match &spec.tokens[idx] {
            Token::Digit9 => {
                if rendered[idx].is_empty() || rendered[idx] == "0" {
                    rendered[idx].clear();
                    trimming = true;
                } else {
                    break;
                }
            }
            Token::Digit0 => break,
            Token::Literal(_) => {}
            Token::Group => {
                if trimming {
                    rendered[idx].clear();
                }
            }
            Token::Decimal | Token::Sign(_) => break,
        }
    }
}

fn trim_fill_mode_integer(spec: &FormatSpec, rendered: &mut [String], int_end: usize) {
    let Some(anchor_idx) =
        (0..int_end).find(|idx| token_has_visible_number_text(&spec.tokens[*idx], &rendered[*idx]))
    else {
        return;
    };
    for idx in 0..anchor_idx {
        match &spec.tokens[idx] {
            Token::Digit9 | Token::Group => rendered[idx].clear(),
            Token::Literal(_) => {}
            Token::Digit0 | Token::Decimal | Token::Sign(_) => {}
        }
    }
}

fn trim_fill_mode_edges(spec: &FormatSpec, rendered: &mut [String]) {
    let mut start = 0usize;
    while start < rendered.len() {
        match &spec.tokens[start] {
            Token::Literal(_) => break,
            _ if rendered[start].is_empty() || rendered[start] == " " => {
                rendered[start].clear();
                start += 1;
            }
            _ => break,
        }
    }

    let mut end = rendered.len();
    while end > start {
        let idx = end - 1;
        match &spec.tokens[idx] {
            Token::Literal(_) => break,
            _ if rendered[idx].is_empty() || rendered[idx] == " " => {
                rendered[idx].clear();
                end -= 1;
            }
            _ => break,
        }
    }
}

fn format_standard_numeric(value: &NumericValue, spec: &FormatSpec) -> String {
    if let Some((negative, text)) = special_value_fixed_text(value) {
        let decimal_idx = spec
            .tokens
            .iter()
            .position(|token| matches!(token, Token::Decimal));
        let int_end = decimal_idx.unwrap_or(spec.tokens.len());
        let pre_digit_positions = spec
            .tokens
            .iter()
            .take(int_end)
            .enumerate()
            .filter_map(|(idx, token)| {
                matches!(token, Token::Digit9 | Token::Digit0).then_some(idx)
            })
            .collect::<Vec<_>>();
        let digit_slots = pre_digit_positions.len();
        if digit_slots < text.len() {
            return overflow_pattern(spec, negative);
        }
        let mut out = spec
            .tokens
            .iter()
            .map(|token| match token {
                Token::Digit9 | Token::Digit0 => " ".to_string(),
                Token::Decimal | Token::Group => " ".to_string(),
                Token::Literal(text) => text.clone(),
                Token::Sign(kind) => match kind {
                    SignKind::Mi | SignKind::S | SignKind::Sg | SignKind::Pl if negative => {
                        "-".to_string()
                    }
                    _ => " ".to_string(),
                },
            })
            .collect::<Vec<_>>();
        let start = digit_slots.saturating_sub(text.chars().count());
        for (ch, idx) in text
            .chars()
            .zip(pre_digit_positions.iter().copied().skip(start))
        {
            out[idx] = ch.to_string();
        }
        move_s_sign_to_number(spec, &mut out, negative);
        apply_implicit_sign(spec, &mut out, negative);
        if spec.fill_mode {
            trim_fill_mode_edges(spec, &mut out);
        }
        return out.concat();
    }
    let shifted = shift_value_text_for_v(&value.render(), spec.scale_digits);
    let (negative, int_part, frac_part) = split_rendered_decimal(&shifted);
    let decimal_idx = spec
        .tokens
        .iter()
        .position(|token| matches!(token, Token::Decimal));
    let int_end = decimal_idx.unwrap_or(spec.tokens.len());
    let int_slots = spec
        .tokens
        .iter()
        .take(int_end)
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let has_integer_zero_placeholder = spec
        .tokens
        .iter()
        .take(int_end)
        .any(|token| matches!(token, Token::Digit0));
    let leftmost_integer_zero_idx = spec
        .tokens
        .iter()
        .take(int_end)
        .position(|token| matches!(token, Token::Digit0));
    let frac_slots = spec
        .tokens
        .iter()
        .skip(int_end + usize::from(decimal_idx.is_some()))
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let (negative, int_part, frac_part) =
        round_decimal_parts(negative, &int_part, &frac_part, frac_slots);
    if int_part.chars().filter(|ch| ch.is_ascii_digit()).count() > int_slots {
        return overflow_pattern(spec, negative);
    }
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

    let suppress_zero_integer = decimal_idx.is_some()
        && int_part.chars().all(|ch| ch == '0')
        && !has_integer_zero_placeholder;
    let int_render = if suppress_zero_integer {
        String::new()
    } else {
        int_part.clone()
    };
    let mut digit_chars = int_render.chars().rev();
    for idx in (0..int_end).rev() {
        match spec.tokens[idx] {
            Token::Digit9 => {
                rendered[idx] = digit_chars
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_else(|| {
                        if spec.fill_mode {
                            if leftmost_integer_zero_idx.is_some_and(|zero_idx| idx >= zero_idx) {
                                "0".into()
                            } else {
                                String::new()
                            }
                        } else {
                            " ".into()
                        }
                    });
            }
            Token::Digit0 => {
                rendered[idx] = digit_chars
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_else(|| "0".into());
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
            Token::Group if !seen_digit => {
                rendered[idx] = if spec.fill_mode {
                    String::new()
                } else {
                    " ".into()
                }
            }
            _ => {}
        }
    }

    if spec
        .tokens
        .iter()
        .take(int_end)
        .any(|token| matches!(token, Token::Digit0))
    {
        for idx in 0..int_end {
            if matches!(spec.tokens[idx], Token::Digit9) && rendered[idx] == " " {
                rendered[idx] = "0".into();
            }
        }
    }

    if let Some(dot) = decimal_idx {
        let mut frac_iter = frac_part.chars();
        for idx in dot + 1..spec.tokens.len() {
            match spec.tokens[idx] {
                Token::Digit9 => {
                    rendered[idx] =
                        frac_iter
                            .next()
                            .map(|ch| ch.to_string())
                            .unwrap_or_else(|| {
                                if spec.fill_mode {
                                    String::new()
                                } else {
                                    "0".into()
                                }
                            });
                }
                Token::Digit0 => {
                    rendered[idx] = frac_iter
                        .next()
                        .map(|ch| ch.to_string())
                        .unwrap_or_else(|| "0".into());
                }
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

    if spec.fill_mode {
        if let Some(dot_idx) = decimal_idx {
            trim_fill_mode_fraction(spec, &mut rendered, dot_idx);
            let has_visible_integer = rendered
                .iter()
                .enumerate()
                .take(int_end)
                .any(|(idx, cell)| token_has_visible_number_text(&spec.tokens[idx], cell));
            if !has_visible_integer
                && matches!(spec.tokens.first(), Some(Token::Sign(_)))
                && let Some(zero_idx) = (0..int_end)
                    .rev()
                    .find(|idx| matches!(spec.tokens[*idx], Token::Digit9 | Token::Digit0))
            {
                rendered[zero_idx] = "0".into();
            }
            let has_fraction_tokens = spec
                .tokens
                .iter()
                .skip(dot_idx + 1)
                .any(|token| matches!(token, Token::Digit9 | Token::Digit0));
            let has_visible_fraction = rendered
                .iter()
                .enumerate()
                .skip(dot_idx + 1)
                .any(|(idx, cell)| token_has_visible_number_text(&spec.tokens[idx], cell));
            if !has_fraction_tokens && !has_visible_fraction {
                rendered[dot_idx].clear();
            }
        }
        trim_fill_mode_integer(spec, &mut rendered, int_end);
        trim_fill_mode_edges(spec, &mut rendered);
    }

    for (idx, token) in spec.tokens.iter().enumerate() {
        if let Token::Sign(kind) = token {
            rendered[idx] = match kind {
                SignKind::S if idx == 0 => " ".into(),
                SignKind::Mi if !negative && spec.fill_mode => String::new(),
                SignKind::Pl if negative && spec.fill_mode => String::new(),
                SignKind::Pl if negative => " ".into(),
                _ => sign_text(*kind, negative).into(),
            };
        }
    }

    move_s_sign_to_number(spec, &mut rendered, negative);
    apply_implicit_sign(spec, &mut rendered, negative);
    let mut out = rendered.concat();
    if negative
        && spec
            .tokens
            .iter()
            .any(|token| matches!(token, Token::Sign(SignKind::Pl)))
    {
        out = format!("-{}", out.trim_start());
    }
    if spec.fill_mode && (out.starts_with('.') || out.starts_with("-.") || out.starts_with("+.")) {
        let dot_idx = out.find('.').unwrap_or(0);
        if dot_idx + 1 == out.len() {
            out.insert(dot_idx, '0');
        }
    }
    if spec.ordinal && !negative && frac_part.chars().all(|ch| ch == '0') && decimal_idx.is_none() {
        let ordinal_value = int_part.parse::<i128>().unwrap_or(0);
        let suffix = if spec.ordinal_lower {
            ordinal_suffix(ordinal_value).to_ascii_lowercase()
        } else {
            ordinal_suffix(ordinal_value).to_string()
        };
        out.push_str(&suffix);
    }
    if spec.angle_pr {
        if negative {
            out = format!("<{}>", out.trim().trim_start_matches('-').trim());
        } else if spec.fill_mode {
            out = out.trim().to_string();
        } else {
            out = format!(" {out} ");
        }
    }
    out
}

fn format_scientific_numeric(value: &NumericValue, spec: &FormatSpec) -> Result<String, ExecError> {
    match value {
        NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN => {
            let mut rendered = spec
                .tokens
                .iter()
                .map(|token| match token {
                    Token::Digit9 | Token::Digit0 => "#".to_string(),
                    Token::Decimal => ".".to_string(),
                    _ => String::new(),
                })
                .collect::<Vec<_>>();
            if spec.fill_mode {
                trim_fill_mode_edges(spec, &mut rendered);
            }
            let mut out = rendered.concat();
            out.push_str("####");
            out.insert(0, ' ');
            return Ok(out);
        }
        NumericValue::Finite { .. } => {}
    }
    let frac_digits = spec
        .tokens
        .iter()
        .skip_while(|token| !matches!(token, Token::Decimal))
        .skip(1)
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let precision = frac_digits.saturating_add(1);
    let rendered = value.render();
    let (negative, digits, mut exponent) = scientific_parts(&rendered);
    let (rounded, carried) = round_scientific_digits(digits, precision);
    if carried {
        exponent = exponent.saturating_add(1);
    }
    let mut mantissa = rounded.chars();
    let head = mantissa.next().unwrap_or('0');
    let tail = mantissa.collect::<String>();
    let exp_sign = if exponent >= 0 { '+' } else { '-' };
    let exp_text = exponent.abs().to_string();
    let exp_width = exp_text.len().max(2);
    let mut text = if frac_digits == 0 {
        format!("{head}e{exp_sign}{:0>width$}", exp_text, width = exp_width)
    } else {
        format!(
            "{head}.{tail}e{exp_sign}{:0>width$}",
            exp_text,
            width = exp_width
        )
    };
    text.insert(0, if negative { '-' } else { ' ' });
    Ok(text)
}

fn format_roman_numeric(value: &NumericValue, fill_mode: bool, lower: bool) -> String {
    match value {
        NumericValue::Finite { coeff, scale, .. } => {
            if coeff.is_negative() {
                return "#".repeat(15);
            }
            let text = coeff.abs().to_string();
            let split = text.len().saturating_sub(*scale as usize);
            let integer_text = if *scale == 0 {
                text
            } else if split == 0 {
                "0".into()
            } else {
                text[..split].to_string()
            };
            integer_text
                .parse::<i128>()
                .ok()
                .map(|integer| format_roman(integer, fill_mode, lower))
                .unwrap_or_else(|| "#".repeat(15))
        }
        _ => "#".repeat(15),
    }
}

fn parse_roman_to_number(input: &str, format: &str) -> Result<NumericValue, ExecError> {
    let trimmed = input.trim_matches(' ');
    if input.is_empty() {
        return Err(ExecError::DetailedError {
            message: "invalid input syntax for type numeric: \" \"".to_string(),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        });
    }
    let normalized_format = format.trim_matches(' ');
    let stripped_format =
        if normalized_format.len() >= 2 && normalized_format[..2].eq_ignore_ascii_case("FM") {
            &normalized_format[2..]
        } else {
            normalized_format
        };
    if stripped_format.eq_ignore_ascii_case("rn") {
        let non_space = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphabetic())
            .collect::<String>();
        if non_space.is_empty() {
            if input.is_empty() {
                return Err(ExecError::InvalidNumericInput(" ".to_string()));
            }
            return Err(ExecError::InvalidStorageValue {
                column: String::new(),
                details: "invalid Roman numeral".to_string(),
            });
        }
        let upper = non_space.to_ascii_uppercase();
        let value = parse_valid_roman(&upper).ok_or_else(|| ExecError::InvalidStorageValue {
            column: String::new(),
            details: "invalid Roman numeral".to_string(),
        })?;
        return Ok(NumericValue::from_i64(value.into()));
    }
    if format.to_ascii_uppercase().contains("RN") {
        if format.to_ascii_uppercase().matches("RN").count() > 1 {
            return Err(ExecError::InvalidStorageValue {
                column: String::new(),
                details: "cannot use \"RN\" twice".to_string(),
            });
        }
        return Err(ExecError::DetailedError {
            message: "\"RN\" is incompatible with other formats".to_string(),
            detail: Some("\"RN\" may only be used together with \"FM\".".to_string()),
            hint: None,
            sqlstate: "22023",
        });
    }
    Err(ExecError::InvalidStorageValue {
        column: String::new(),
        details: "invalid Roman numeral".to_string(),
    })
}

fn parse_valid_roman(input: &str) -> Option<i32> {
    if input.is_empty() {
        return None;
    }
    let bytes = input.as_bytes();
    fn roman_value(byte: u8) -> Option<i32> {
        match byte {
            b'I' => Some(1),
            b'V' => Some(5),
            b'X' => Some(10),
            b'L' => Some(50),
            b'C' => Some(100),
            b'D' => Some(500),
            b'M' => Some(1000),
            _ => None,
        }
    }
    let mut total = 0;
    let mut i = 0;
    while i < bytes.len() {
        let cur = roman_value(bytes[i])?;
        if i + 1 < bytes.len() {
            let next = roman_value(bytes[i + 1])?;
            if cur < next {
                let valid_pair = matches!(
                    (bytes[i], bytes[i + 1]),
                    (b'I', b'V' | b'X') | (b'X', b'L' | b'C') | (b'C', b'D' | b'M')
                );
                if !valid_pair {
                    return None;
                }
                total += next - cur;
                i += 2;
                continue;
            }
        }
        total += cur;
        i += 1;
    }
    if !(1..=3999).contains(&total) {
        return None;
    }
    if format_roman(total.into(), true, false) != input {
        return None;
    }
    Some(total)
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

fn format_roman(value: i128, fill_mode: bool, lower: bool) -> String {
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
    if lower {
        out = out.to_ascii_lowercase();
    }
    if fill_mode { out } else { format!("{out:>15}") }
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
    use super::{to_char_float, to_char_int, to_char_numeric, to_number_numeric};
    use crate::backend::executor::ExecError;
    use crate::include::nodes::datum::NumericValue;

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
        assert_eq!(
            to_char_int(4567890123456789, "FMRN").unwrap(),
            "###############"
        );
        assert_eq!(to_char_int(456, "rn").unwrap(), "          cdlvi");
    }

    #[test]
    fn formats_scientific_notation() {
        assert_eq!(to_char_int(1234, "9.99EEEE").unwrap(), " 1.23e+03");
        assert_eq!(to_char_int(-1234, "9.99eeee").unwrap(), "-1.23e+03");
    }

    #[test]
    fn formats_numeric_fixed_and_overflow_cases() {
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("-34338492.215397047"),
                "FM9999999999999999.999999999999999"
            )
            .unwrap(),
            "-34338492.215397047"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::PosInf, "MI99.99").unwrap(),
            " ##.##"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::NegInf, "MI99.99").unwrap(),
            "-##.##"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("4200000000"), "MI99.99").unwrap(),
            " ##.##"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("4200000000"), "MI9999999999.99")
                .unwrap()
                .trim(),
            "4200000000.00"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::PosInf, "MI9999999999.99")
                .unwrap()
                .trim(),
            "Infinity"
        );
    }

    #[test]
    fn formats_numeric_roman_and_scientific_cases() {
        assert_eq!(
            to_char_numeric(&NumericValue::from("1234"), "rn").unwrap(),
            "       mccxxxiv"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("1234.56"), "99999V99")
                .unwrap()
                .trim(),
            "123456"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("1234.56"), "99999V99").unwrap(),
            "  123456"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::PosInf, "9.999EEEE").unwrap(),
            " #.#######"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("4.31"), "FMRN").unwrap(),
            "IV"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("1.2345e2345"), "9.999EEEE").unwrap(),
            " 1.235e+2345"
        );
    }

    #[test]
    fn formats_float_overflow_masks_like_postgres() {
        assert_eq!(
            to_char_float(12345678901.0, "FM9999999999D9999900000000000000000").unwrap(),
            "##########.####"
        );
        assert_eq!(to_char_float(1236.0, "rn").unwrap(), "       mccxxxvi");
    }

    #[test]
    fn formats_numeric_fill_mode_and_literal_cases() {
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "FM9999999999999999.999999999999999"
            )
            .unwrap(),
            "0."
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "FM999.").unwrap(),
            "100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "foo999").unwrap(),
            "foo 100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "f\\oo999").unwrap(),
            "f\\oo 100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "f\"ool\"999").unwrap(),
            "fool 100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "f\\\"oo999").unwrap(),
            "f\"oo 100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "f\\\\\"oo999").unwrap(),
            "f\\\"oo 100"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("100"), "f\"ool\\\"999").unwrap(),
            "fool\"999"
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "FM0999999999999999.999999999999999"
            )
            .unwrap(),
            "0000000000000000."
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "FM9999999999990999.990999999999999"
            )
            .unwrap(),
            "0000.000"
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "FM9999999999999999.099999999999999"
            )
            .unwrap(),
            ".0"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("0"), "FMS 9 9 9 . 9 9 9").unwrap(),
            "   +0 .   "
        );
        assert_eq!(
            to_char_numeric(&NumericValue::from("4.31"), "FMS 9 9 9 . 9 9 9").unwrap(),
            "   +4 . 3 1 "
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("-83028485"),
                "FMS 9 9 9 9 9 9 9 9 . 9 9"
            )
            .unwrap(),
            " -8 3 0 2 8 4 8 5 .  "
        );
        let wide_fms = to_char_numeric(
            &NumericValue::from("0"),
            "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9",
        )
        .unwrap();
        assert_eq!(wide_fms.len(), 37);
        assert_eq!(wide_fms, "                +0 .                 ");
    }

    #[test]
    fn formats_numeric_sign_and_decimal_masks_like_postgres() {
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("-34338492.215397047"),
                "9G999G999G999G999G999D999G999G999G999G999"
            )
            .unwrap()
            .trim_start(),
            "-34,338,492.215,397,047,000,000"
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "9999999999999999.999999999999999PR"
            )
            .unwrap()
            .trim(),
            ".000000000000000"
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("74881"),
                "FM9999999999999999.999999999999999THPR"
            )
            .unwrap(),
            "74881."
        );
        assert_eq!(
            to_char_numeric(
                &NumericValue::from("0"),
                "SG9999999999999999.999999999999999th"
            )
            .unwrap()
            .trim_start(),
            "+                .000000000000000"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::NegInf, "MI9999999999.99")
                .unwrap()
                .trim_end(),
            "-  Infinity"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::PosInf, "MI99.99").unwrap(),
            " ##.##"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::NegInf, "MI99.99").unwrap(),
            "-##.##"
        );
        assert_eq!(
            to_char_numeric(&NumericValue::NaN, "MI99.99").unwrap(),
            " ##.##"
        );
    }

    #[test]
    fn parses_to_number_decimal_formats() {
        assert_eq!(
            to_number_numeric("-34,338,492", "99G999G999")
                .unwrap()
                .render(),
            "-34338492"
        );
        assert_eq!(
            to_number_numeric("<564646.654564>", "999999.999999PR")
                .unwrap()
                .render(),
            "-564646.654564"
        );
        assert_eq!(
            to_number_numeric("123456", "99999V99").unwrap().render(),
            "1234.560000000000000000"
        );
        assert_eq!(to_number_numeric("42nd", "99th").unwrap().render(), "42");
    }

    #[test]
    fn parses_to_number_roman_formats() {
        assert_eq!(to_number_numeric("CvIiI", "rn").unwrap().render(), "108");
        assert_eq!(to_number_numeric("  XIV", "  RN").unwrap().render(), "14");
        assert_eq!(to_number_numeric("CM", "FMRN").unwrap().render(), "900");
        assert_eq!(to_number_numeric("M CC", "RN").unwrap().render(), "1000");
        assert!(matches!(
            to_number_numeric("viv", "RN"),
            Err(ExecError::InvalidStorageValue { .. })
        ));
        for invalid in [
            "DCCCD", "XIXL", "MCCM", "MMMM", "VV", "IL", "VIX", "LXC", "DCM", "MMMDCM", "CLXC",
            "qiv", " ",
        ] {
            assert!(matches!(
                to_number_numeric(invalid, "RN"),
                Err(ExecError::InvalidStorageValue { .. })
            ));
        }
        assert!(matches!(
            to_number_numeric("CM", "MIRN"),
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate: "22023",
                ..
            }) if message == "\"RN\" is incompatible with other formats"
                && detail == "\"RN\" may only be used together with \"FM\"."
        ));
        assert!(matches!(
            to_number_numeric("", "RN"),
            Err(ExecError::DetailedError {
                message,
                detail: None,
                sqlstate: "22P02",
                ..
            }) if message == "invalid input syntax for type numeric: \" \""
        ));
    }
}
