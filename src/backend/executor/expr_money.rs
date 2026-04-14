use super::ExecError;
use crate::include::nodes::datum::Value;

const MONEY_SCALE: i64 = 100;

fn money_range_error(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("value \"{input}\" is out of range for type money"),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

fn money_syntax_error(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type money: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

fn money_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "money out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

pub(crate) fn money_parse_text(input: &str) -> Result<i64, ExecError> {
    let mut s = input.trim();
    if s.is_empty() {
        return Err(money_syntax_error(input));
    }

    let mut negative = false;
    if let Some(rest) = s.strip_prefix('(') {
        negative = true;
        s = rest.trim_start();
    }
    if let Some(rest) = s.strip_prefix('-') {
        negative = true;
        s = rest.trim_start();
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest.trim_start();
    }
    if let Some(rest) = s.strip_prefix('$') {
        s = rest.trim_start();
    }

    let mut digits = String::with_capacity(s.len());
    let mut seen_dot = false;
    let mut frac_digits = 0usize;
    let mut round_up = false;
    let mut saw_digit = false;
    let mut rest = "";
    for (idx, ch) in s.char_indices() {
        match ch {
            '0'..='9' => {
                saw_digit = true;
                if !seen_dot {
                    digits.push(ch);
                } else if frac_digits < 2 {
                    digits.push(ch);
                    frac_digits += 1;
                } else {
                    if ch >= '5' {
                        round_up = true;
                    }
                    rest = &s[idx + ch.len_utf8()..];
                    break;
                }
            }
            ',' if !seen_dot => {}
            '.' if !seen_dot => seen_dot = true,
            _ => {
                rest = &s[idx..];
                break;
            }
        }
    }
    if rest.is_empty() {
        rest = "";
    }
    if !saw_digit {
        return Err(money_syntax_error(input));
    }
    while frac_digits < 2 {
        digits.push('0');
        frac_digits += 1;
    }

    let mut tail = rest.trim();
    if let Some(next) = tail.strip_prefix(')') {
        negative = true;
        tail = next.trim_start();
    }
    if let Some(next) = tail.strip_prefix('-') {
        negative = true;
        tail = next.trim_start();
    } else if let Some(next) = tail.strip_prefix('+') {
        tail = next.trim_start();
    }
    if let Some(next) = tail.strip_prefix('$') {
        tail = next.trim_start();
    }
    if !tail.is_empty() {
        return Err(money_syntax_error(input));
    }

    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| money_range_error(input))?;
    let rounded = magnitude + i128::from(round_up);
    let signed = if negative { -rounded } else { rounded };
    i64::try_from(signed).map_err(|_| money_range_error(input))
}

pub fn money_format_text(value: i64) -> String {
    let negative = value < 0;
    let cents = value.unsigned_abs();
    let frac = cents % MONEY_SCALE as u64;
    let mut whole = (cents / MONEY_SCALE as u64).to_string();
    let mut grouped = String::with_capacity(whole.len() + whole.len() / 3);
    while whole.len() > 3 {
        let split = whole.len() - 3;
        let chunk = whole.split_off(split);
        if grouped.is_empty() {
            grouped = chunk;
        } else {
            grouped = format!("{chunk},{grouped}");
        }
    }
    if grouped.is_empty() {
        grouped = whole;
    } else {
        grouped = format!("{whole},{grouped}");
    }
    let prefix = if negative { "-$" } else { "$" };
    format!("{prefix}{grouped}.{frac:02}")
}

pub(crate) fn money_numeric_text(value: i64) -> String {
    let negative = value < 0;
    let cents = value.unsigned_abs();
    let whole = cents / MONEY_SCALE as u64;
    let frac = cents % MONEY_SCALE as u64;
    if negative {
        format!("-{whole}.{frac:02}")
    } else {
        format!("{whole}.{frac:02}")
    }
}

pub(crate) fn money_add(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_add(right).ok_or_else(money_out_of_range)
}

pub(crate) fn money_sub(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_sub(right).ok_or_else(money_out_of_range)
}

pub(crate) fn money_mul_int(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_mul(right).ok_or_else(money_out_of_range)
}

pub(crate) fn money_div_int(left: i64, right: i64) -> Result<i64, ExecError> {
    if right == 0 {
        return Err(ExecError::DivisionByZero("/"));
    }
    Ok(left / right)
}

pub(crate) fn money_mul_float(left: i64, right: f64) -> Result<i64, ExecError> {
    let result = (left as f64 * right).round();
    if !result.is_finite() || result < i64::MIN as f64 || result > i64::MAX as f64 {
        return Err(money_out_of_range());
    }
    Ok(result as i64)
}

pub(crate) fn money_div_float(left: i64, right: f64) -> Result<i64, ExecError> {
    if right == 0.0 {
        return Err(ExecError::DivisionByZero("/"));
    }
    let result = (left as f64 / right).round();
    if !result.is_finite() || result < i64::MIN as f64 || result > i64::MAX as f64 {
        return Err(money_out_of_range());
    }
    Ok(result as i64)
}

pub(crate) fn money_from_float(value: f64) -> Result<i64, ExecError> {
    let result = (value * MONEY_SCALE as f64).round();
    if !result.is_finite() || result < i64::MIN as f64 || result > i64::MAX as f64 {
        return Err(money_out_of_range());
    }
    Ok(result as i64)
}

pub(crate) fn money_cash_div(left: i64, right: i64) -> Result<f64, ExecError> {
    if right == 0 {
        return Err(ExecError::DivisionByZero("/"));
    }
    Ok(left as f64 / right as f64)
}

pub(crate) fn money_cmp(left: i64, right: i64) -> std::cmp::Ordering {
    left.cmp(&right)
}

pub(crate) fn money_larger(left: i64, right: i64) -> i64 {
    left.max(right)
}

pub(crate) fn money_smaller(left: i64, right: i64) -> i64 {
    left.min(right)
}

fn append_num_word(out: &mut String, value: u64) {
    const SMALL: [&str; 28] = [
        "zero",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen",
        "fifteen",
        "sixteen",
        "seventeen",
        "eighteen",
        "nineteen",
        "twenty",
        "thirty",
        "forty",
        "fifty",
        "sixty",
        "seventy",
        "eighty",
        "ninety",
    ];
    let tens = &SMALL[18..];
    let tu = value % 100;
    if value <= 20 {
        out.push_str(SMALL[value as usize]);
        return;
    }
    if tu == 0 {
        out.push_str(SMALL[(value / 100) as usize]);
        out.push_str(" hundred");
        return;
    }
    if value > 99 {
        out.push_str(SMALL[(value / 100) as usize]);
        if value % 10 == 0 && tu > 10 {
            out.push_str(" hundred ");
            out.push_str(tens[(tu / 10) as usize]);
        } else if tu < 20 {
            out.push_str(" hundred and ");
            out.push_str(SMALL[tu as usize]);
        } else {
            out.push_str(" hundred ");
            out.push_str(tens[(tu / 10) as usize]);
            out.push(' ');
            out.push_str(SMALL[(tu % 10) as usize]);
        }
        return;
    }
    if value % 10 == 0 && tu > 10 {
        out.push_str(tens[(tu / 10) as usize]);
    } else if tu < 20 {
        out.push_str(SMALL[tu as usize]);
    } else {
        out.push_str(tens[(tu / 10) as usize]);
        out.push(' ');
        out.push_str(SMALL[(tu % 10) as usize]);
    }
}

pub(crate) fn cash_words_text(value: i64) -> String {
    let mut out = String::new();
    let mut value = value;
    if value < 0 {
        value = -value;
        out.push_str("minus ");
    }
    let val = value as u64;
    let dollars = val / 100;
    let cents = val % 100;
    let groups = [
        (100_000_000_000_000_000u64, "quadrillion"),
        (100_000_000_000_000u64, "trillion"),
        (100_000_000_000u64, "billion"),
        (100_000_000u64, "million"),
        (100_000u64, "thousand"),
    ];
    let rem = dollars;
    for (div, label) in groups {
        let chunk = (rem / div) % 1000;
        if chunk > 0 {
            append_num_word(&mut out, chunk);
            out.push(' ');
            out.push_str(label);
            out.push(' ');
        }
    }
    let final_chunk = rem % 1000;
    if final_chunk > 0 {
        append_num_word(&mut out, final_chunk);
    }
    if dollars == 0 {
        out.push_str("zero");
    }
    if dollars == 1 {
        out.push_str(" dollar and ");
    } else {
        out.push_str(" dollars and ");
    }
    append_num_word(&mut out, cents);
    if cents == 1 {
        out.push_str(" cent");
    } else {
        out.push_str(" cents");
    }
    let mut chars = out.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => out,
    }
}

pub(crate) fn money_value(value: &Value) -> Option<i64> {
    match value {
        Value::Money(v) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_money() {
        assert_eq!(money_parse_text("$123.45").unwrap(), 12345);
        assert_eq!(money_parse_text("($123,456.78)").unwrap(), -12345678);
        assert_eq!(money_format_text(-12345678), "-$123,456.78");
    }

    #[test]
    fn rounds_money_input() {
        assert_eq!(money_parse_text("$123.451").unwrap(), 12345);
        assert_eq!(money_parse_text("$123.455").unwrap(), 12346);
    }

    #[test]
    fn renders_cash_words() {
        assert_eq!(
            cash_words_text(12423),
            "One hundred twenty four dollars and twenty three cents"
        );
    }
}
