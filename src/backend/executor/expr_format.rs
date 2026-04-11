use super::ExecError;
use crate::include::nodes::datum::NumericValue;

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
    let mut parser = FormatParser::new(format);
    let spec = parser.parse()?;
    if spec.roman {
        return Ok(format_roman_numeric(value, spec.fill_mode, spec.roman_lower));
    }
    if spec.scientific {
        return format_scientific_numeric(value, &spec);
    }
    Ok(format_standard_numeric(value, &spec))
}

#[derive(Debug)]
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

        let roman =
            self.idx == self.chars.len() && self.raw[self.raw.len().saturating_sub(2)..].eq_ignore_ascii_case("RN");
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
    let mut carry = tail
        .chars()
        .next()
        .map(|ch| ch >= '5')
        .unwrap_or(false);
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
        let mut out = if digits.is_empty() { "0".to_string() } else { digits };
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

fn overflow_pattern(spec: &FormatSpec, negative: bool) -> String {
    let mut rendered = spec
        .tokens
        .iter()
        .map(|token| match token {
            Token::Digit9 | Token::Digit0 => "#".to_string(),
            Token::Decimal => ".".to_string(),
            Token::Group => ",".to_string(),
            Token::Literal(text) => text.clone(),
            Token::Sign(kind) => match kind {
                SignKind::Mi | SignKind::S | SignKind::Sg | SignKind::Pl if negative => "-".to_string(),
                _ => " ".to_string(),
            },
        })
        .collect::<Vec<_>>()
        .concat();
    if !spec.tokens.iter().any(|token| matches!(token, Token::Sign(_))) && negative && !spec.angle_pr {
        rendered = format!("-{rendered}");
    }
    if spec.fill_mode {
        rendered = rendered.trim().to_string();
    }
    rendered
}

fn special_value_fixed_text(value: &NumericValue) -> Option<(bool, &'static str)> {
    match value {
        NumericValue::PosInf => Some((false, "Infinity")),
        NumericValue::NegInf => Some((true, "Infinity")),
        NumericValue::NaN => Some((false, "NaN")),
        NumericValue::Finite { .. } => None,
    }
}

fn format_standard_numeric(value: &NumericValue, spec: &FormatSpec) -> String {
    if let Some((negative, text)) = special_value_fixed_text(value) {
        let digit_slots = spec
            .tokens
            .iter()
            .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
            .count();
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
                    SignKind::Mi | SignKind::S | SignKind::Sg | SignKind::Pl if negative => "-".to_string(),
                    _ => " ".to_string(),
                },
            })
            .collect::<Vec<_>>();
        let char_positions = out
            .iter()
            .enumerate()
            .filter(|(_, cell)| *cell == " ")
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        if !char_positions.is_empty() {
            for (ch, idx) in text.chars().zip(char_positions.iter().copied()) {
                out[idx] = ch.to_string();
            }
            if text.chars().count() > char_positions.len() {
                return overflow_pattern(spec, negative);
            }
        } else {
            return overflow_pattern(spec, negative);
        }
        let mut out = out.concat();
        if !spec.tokens.iter().any(|token| matches!(token, Token::Sign(_))) && negative && !spec.angle_pr {
            out = format!("-{out}");
        }
        if spec.fill_mode {
            out = out.trim().to_string();
        }
        return out;
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

    let mut digit_chars = int_part.chars().rev();
    for idx in (0..int_end).rev() {
        match spec.tokens[idx] {
            Token::Digit9 => {
                rendered[idx] = digit_chars
                    .next()
                    .map(|ch| ch.to_string())
                    .unwrap_or_else(|| {
                        if spec.fill_mode {
                            String::new()
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
                rendered[idx] = if spec.fill_mode { String::new() } else { " ".into() }
            }
            _ => {}
        }
    }

    if spec.tokens.iter().take(int_end).any(|token| matches!(token, Token::Digit0)) {
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
                    rendered[idx] = frac_iter
                        .next()
                        .map(|ch| ch.to_string())
                        .unwrap_or_else(|| if spec.fill_mode { String::new() } else { "0".into() });
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

    for (idx, token) in spec.tokens.iter().enumerate() {
        if let Token::Sign(kind) = token {
            rendered[idx] = match kind {
                SignKind::S if idx == 0 => " ".into(),
                SignKind::Pl if negative => " ".into(),
                _ => sign_text(*kind, negative).into(),
            };
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
        } else {
            out = format!(" {out} ");
        }
    }
    if spec.fill_mode {
        out = out.trim().to_string();
        if let Some(dot_idx) = out.find('.') {
            let frac_pattern = spec.tokens.iter().skip(int_end + 1).collect::<Vec<_>>();
            let mut frac_chars = out[dot_idx + 1..].chars().collect::<Vec<_>>();
            while let Some(last) = frac_chars.last() {
                let idx = frac_chars.len() - 1;
                if matches!(frac_pattern.get(idx), Some(Token::Digit9)) && *last == '0' {
                    frac_chars.pop();
                } else {
                    break;
                }
            }
            let mut rebuilt = out[..dot_idx + 1].to_string();
            rebuilt.extend(frac_chars);
            out = rebuilt;
        }
    }
    out
}

fn format_scientific_numeric(value: &NumericValue, spec: &FormatSpec) -> Result<String, ExecError> {
    match value {
        NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN => {
            let hashes = spec
                .tokens
                .iter()
                .map(|token| match token {
                    Token::Digit9 | Token::Digit0 => "#",
                    Token::Decimal => ".",
                    _ => "",
                })
                .collect::<String>();
            let mut out = hashes;
            out.push_str("####");
            if matches!(value, NumericValue::NegInf) {
                out.insert(0, '-');
            } else {
                out.insert(0, ' ');
            }
            return Ok(out);
        }
        NumericValue::Finite { .. } => {}
    }
    let rendered = value.render();
    let as_f64: f64 = rendered.parse().map_err(|_| ExecError::TypeMismatch {
        op: "to_char",
        left: crate::include::nodes::datum::Value::Numeric(value.clone()),
        right: super::node_types::Value::Text("".into()),
    })?;
    let frac_digits = spec
        .tokens
        .iter()
        .skip_while(|token| !matches!(token, Token::Decimal))
        .skip(1)
        .filter(|token| matches!(token, Token::Digit9 | Token::Digit0))
        .count();
    let mut text = format!("{as_f64:.frac_digits$e}");
    if let Some(idx) = text.rfind('e') {
        let (mantissa, exponent) = text.split_at(idx);
        let exponent_value: i32 = exponent[1..].parse().unwrap_or(0);
        let sign = if exponent_value >= 0 { '+' } else { '-' };
        text = format!("{mantissa}e{sign}{:02}", exponent_value.abs());
    }
    if as_f64 >= 0.0 {
        text.insert(0, ' ');
    }
    Ok(text)
}

fn format_roman_numeric(value: &NumericValue, fill_mode: bool, lower: bool) -> String {
    match value {
        NumericValue::Finite { coeff, scale } if *scale == 0 => {
            if let Some(integer) = coeff.to_string().parse::<i128>().ok() {
                format_roman(integer, fill_mode, lower)
            } else {
                "#".repeat(15)
            }
        }
        _ => "#".repeat(15),
    }
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
    use super::{to_char_int, to_char_numeric};
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
        assert_eq!(to_char_int(4567890123456789, "FMRN").unwrap(), "###############");
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
            to_char_numeric(&NumericValue::from("-34338492.215397047"), "FM9999999999999999.999999999999999").unwrap(),
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
    }

    #[test]
    fn formats_numeric_roman_and_scientific_cases() {
        assert_eq!(to_char_numeric(&NumericValue::from("1234"), "rn").unwrap(), "       mccxxxiv");
        assert_eq!(to_char_numeric(&NumericValue::from("1234.56"), "99999V99").unwrap().trim(), "123456");
        assert_eq!(to_char_numeric(&NumericValue::PosInf, "9.999EEEE").unwrap(), " #.#######");
    }
}
