use super::ExecError;
use crate::compat::backend::access::hash::hash_bytes_extended;
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use std::cmp::Ordering;

pub fn parse_macaddr_text(text: &str) -> Result<[u8; 6], ExecError> {
    parse_macaddr_text_inner(text).map_err(|_| mac_input_error(text, "macaddr"))
}

pub fn parse_macaddr8_text(text: &str) -> Result<[u8; 8], ExecError> {
    parse_macaddr8_text_inner(text).map_err(|_| mac_input_error(text, "macaddr8"))
}

pub fn parse_macaddr_bytes(bytes: &[u8]) -> Result<[u8; 6], ExecError> {
    bytes
        .try_into()
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "macaddr".into(),
            details: "invalid macaddr storage payload".into(),
        })
}

pub fn parse_macaddr8_bytes(bytes: &[u8]) -> Result<[u8; 8], ExecError> {
    bytes
        .try_into()
        .map_err(|_| ExecError::InvalidStorageValue {
            column: "macaddr8".into(),
            details: "invalid macaddr8 storage payload".into(),
        })
}

pub fn render_macaddr_text(value: &[u8; 6]) -> String {
    render_colon_hex(value)
}

pub fn render_macaddr8_text(value: &[u8; 8]) -> String {
    render_colon_hex(value)
}

pub fn macaddr_to_macaddr8(value: [u8; 6]) -> [u8; 8] {
    [
        value[0], value[1], value[2], 0xff, 0xfe, value[3], value[4], value[5],
    ]
}

pub fn macaddr8_to_macaddr(value: [u8; 8]) -> Result<[u8; 6], ExecError> {
    if value[3] == 0xff && value[4] == 0xfe {
        Ok([value[0], value[1], value[2], value[5], value[6], value[7]])
    } else {
        Err(ExecError::DetailedError {
            message: "macaddr8 data out of range to convert to macaddr".into(),
            detail: None,
            hint: Some(
                "Only addresses that have FF and FE as values in the 4th and 5th bytes from the left, for example xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted from macaddr8 to macaddr."
                    .into(),
            ),
            sqlstate: "22003",
        })
    }
}

pub fn eval_macaddr_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    use BuiltinScalarFunction::*;

    let result = match func {
        MacAddrEq => compare_macaddr(values, "macaddr_eq", |ord| ord == Ordering::Equal),
        MacAddrNe => compare_macaddr(values, "macaddr_ne", |ord| ord != Ordering::Equal),
        MacAddrLt => compare_macaddr(values, "macaddr_lt", |ord| ord == Ordering::Less),
        MacAddrLe => compare_macaddr(values, "macaddr_le", |ord| ord != Ordering::Greater),
        MacAddrGt => compare_macaddr(values, "macaddr_gt", |ord| ord == Ordering::Greater),
        MacAddrGe => compare_macaddr(values, "macaddr_ge", |ord| ord != Ordering::Less),
        MacAddrCmp => cmp_macaddr(values, "macaddr_cmp"),
        MacAddrNot => unary_macaddr(values, "macaddr_not", |value| value.map(|byte| !byte)),
        MacAddrAnd => binary_macaddr(values, "macaddr_and", |left, right| {
            std::array::from_fn(|index| left[index] & right[index])
        }),
        MacAddrOr => binary_macaddr(values, "macaddr_or", |left, right| {
            std::array::from_fn(|index| left[index] | right[index])
        }),
        MacAddrTrunc => unary_macaddr(values, "trunc", |mut value| {
            value[3..].fill(0);
            value
        }),
        MacAddrToMacAddr8 => match values {
            [Value::MacAddr(value)] => Ok(Value::MacAddr8(macaddr_to_macaddr8(*value))),
            [Value::Null] => Ok(Value::Null),
            _ => mac_func_type_error("macaddr8", values),
        },
        MacAddr8Eq => compare_macaddr8(values, "macaddr8_eq", |ord| ord == Ordering::Equal),
        MacAddr8Ne => compare_macaddr8(values, "macaddr8_ne", |ord| ord != Ordering::Equal),
        MacAddr8Lt => compare_macaddr8(values, "macaddr8_lt", |ord| ord == Ordering::Less),
        MacAddr8Le => compare_macaddr8(values, "macaddr8_le", |ord| ord != Ordering::Greater),
        MacAddr8Gt => compare_macaddr8(values, "macaddr8_gt", |ord| ord == Ordering::Greater),
        MacAddr8Ge => compare_macaddr8(values, "macaddr8_ge", |ord| ord != Ordering::Less),
        MacAddr8Cmp => cmp_macaddr8(values, "macaddr8_cmp"),
        MacAddr8Not => unary_macaddr8(values, "macaddr8_not", |value| value.map(|byte| !byte)),
        MacAddr8And => binary_macaddr8(values, "macaddr8_and", |left, right| {
            std::array::from_fn(|index| left[index] & right[index])
        }),
        MacAddr8Or => binary_macaddr8(values, "macaddr8_or", |left, right| {
            std::array::from_fn(|index| left[index] | right[index])
        }),
        MacAddr8Trunc => unary_macaddr8(values, "trunc", |mut value| {
            value[3..].fill(0);
            value
        }),
        MacAddr8ToMacAddr => match values {
            [Value::MacAddr8(value)] => macaddr8_to_macaddr(*value).map(Value::MacAddr),
            [Value::Null] => Ok(Value::Null),
            _ => mac_func_type_error("macaddr", values),
        },
        MacAddr8Set7Bit => unary_macaddr8(values, "macaddr8_set7bit", |mut value| {
            value[0] |= 0x02;
            value
        }),
        HashMacAddr => hash_macaddr(values),
        HashMacAddrExtended => hash_macaddr_extended(values),
        HashMacAddr8 => hash_macaddr8(values),
        HashMacAddr8Extended => hash_macaddr8_extended(values),
        _ => return None,
    };

    Some(result)
}

fn parse_macaddr_text_inner(text: &str) -> Result<[u8; 6], ()> {
    let input = text.trim();
    if !input.contains(':') && !input.contains('-') && !input.contains('.') {
        return parse_hex_bytes::<6>(input);
    }
    if let Some(bytes) = parse_macaddr_six_groups(input, ':')? {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_macaddr_six_groups(input, '-')? {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_fixed_hex_groups::<6>(input, ':', &[6, 6])? {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_fixed_hex_groups::<6>(input, '-', &[6, 6])? {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_fixed_hex_groups::<6>(input, '.', &[4, 4, 4])? {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_fixed_hex_groups::<6>(input, '-', &[4, 4, 4])? {
        return Ok(bytes);
    }
    Err(())
}

fn parse_macaddr8_text_inner(text: &str) -> Result<[u8; 8], ()> {
    let input = text.trim();
    let bytes = parse_pair_stream(input)?;
    match bytes.as_slice() {
        [a, b, c, d, e, f] => Ok([*a, *b, *c, 0xff, 0xfe, *d, *e, *f]),
        [a, b, c, d, e, f, g, h] => Ok([*a, *b, *c, *d, *e, *f, *g, *h]),
        _ => Err(()),
    }
}

fn parse_macaddr_six_groups(input: &str, sep: char) -> Result<Option<[u8; 6]>, ()> {
    if !uses_only_separator(input, sep) {
        return Ok(None);
    }
    let parts: Vec<&str> = input.split(sep).collect();
    if parts.len() != 6 {
        return Ok(None);
    }
    let mut bytes = [0_u8; 6];
    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() || !part.as_bytes().iter().all(u8::is_ascii_hexdigit) {
            return Err(());
        }
        let value = u16::from_str_radix(part, 16).map_err(|_| ())?;
        if value > u8::MAX as u16 {
            return Err(());
        }
        bytes[idx] = value as u8;
    }
    Ok(Some(bytes))
}

fn parse_fixed_hex_groups<const N: usize>(
    input: &str,
    sep: char,
    group_lens: &[usize],
) -> Result<Option<[u8; N]>, ()> {
    if !uses_only_separator(input, sep) {
        return Ok(None);
    }
    let parts: Vec<&str> = input.split(sep).collect();
    if parts.len() != group_lens.len()
        || parts
            .iter()
            .zip(group_lens.iter())
            .any(|(part, len)| part.len() != *len)
    {
        return Ok(None);
    }
    let mut hex = String::new();
    for part in parts {
        hex.push_str(part);
    }
    parse_hex_bytes::<N>(&hex).map(Some)
}

fn parse_pair_stream(input: &str) -> Result<Vec<u8>, ()> {
    let bytes = input.as_bytes();
    let mut index = 0;
    let mut spacer = None;
    let mut out = Vec::with_capacity(8);

    while index < bytes.len() {
        if index + 1 >= bytes.len() {
            return Err(());
        }
        let byte = parse_hex_pair(bytes[index], bytes[index + 1]).ok_or(())?;
        out.push(byte);
        index += 2;
        if index == bytes.len() {
            break;
        }
        let next = bytes[index] as char;
        if matches!(next, ':' | '-' | '.') {
            if let Some(existing) = spacer {
                if existing != next {
                    return Err(());
                }
            } else {
                spacer = Some(next);
            }
            index += 1;
        } else if next.is_ascii_whitespace() {
            if out.len() != 6 && out.len() != 8 {
                return Err(());
            }
            if input[index..].trim().is_empty() {
                break;
            }
            return Err(());
        }
        if out.len() > 8 {
            return Err(());
        }
    }

    Ok(out)
}

fn uses_only_separator(input: &str, sep: char) -> bool {
    input.contains(sep)
        && !input
            .chars()
            .any(|ch| matches!(ch, ':' | '-' | '.') && ch != sep)
}

fn parse_hex_bytes<const N: usize>(text: &str) -> Result<[u8; N], ()> {
    if text.len() != N * 2 || !text.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(());
    }
    let mut bytes = [0_u8; N];
    for index in 0..N {
        bytes[index] =
            parse_hex_pair(text.as_bytes()[index * 2], text.as_bytes()[index * 2 + 1]).ok_or(())?;
    }
    Ok(bytes)
}

fn parse_hex_pair(high: u8, low: u8) -> Option<u8> {
    Some((hex_value(high)? << 4) | hex_value(low)?)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn render_colon_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3 - 1);
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            out.push(':');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn mac_input_error(text: &str, type_name: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type {type_name}: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

fn compare_macaddr(
    values: &[Value],
    name: &'static str,
    pred: impl FnOnce(Ordering) -> bool,
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(left), Value::MacAddr(right)] => Ok(Value::Bool(pred(left.cmp(right)))),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn compare_macaddr8(
    values: &[Value],
    name: &'static str,
    pred: impl FnOnce(Ordering) -> bool,
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(left), Value::MacAddr8(right)] => Ok(Value::Bool(pred(left.cmp(right)))),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn cmp_macaddr(values: &[Value], name: &'static str) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(left), Value::MacAddr(right)] => Ok(Value::Int32(cmp_i32(left.cmp(right)))),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn cmp_macaddr8(values: &[Value], name: &'static str) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(left), Value::MacAddr8(right)] => {
            Ok(Value::Int32(cmp_i32(left.cmp(right))))
        }
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn unary_macaddr(
    values: &[Value],
    name: &'static str,
    op: impl FnOnce([u8; 6]) -> [u8; 6],
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(value)] => Ok(Value::MacAddr(op(*value))),
        [Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn unary_macaddr8(
    values: &[Value],
    name: &'static str,
    op: impl FnOnce([u8; 8]) -> [u8; 8],
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(value)] => Ok(Value::MacAddr8(op(*value))),
        [Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn binary_macaddr(
    values: &[Value],
    name: &'static str,
    op: impl FnOnce([u8; 6], [u8; 6]) -> [u8; 6],
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(left), Value::MacAddr(right)] => Ok(Value::MacAddr(op(*left, *right))),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn binary_macaddr8(
    values: &[Value],
    name: &'static str,
    op: impl FnOnce([u8; 8], [u8; 8]) -> [u8; 8],
) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(left), Value::MacAddr8(right)] => Ok(Value::MacAddr8(op(*left, *right))),
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error(name, values),
    }
}

fn hash_macaddr(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(value)] => Ok(Value::Int32(hash32(value))),
        [Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error("hashmacaddr", values),
    }
}

fn hash_macaddr_extended(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr(value), Value::Int64(seed)] => {
            Ok(Value::Int64(hash_bytes_extended(value, *seed as u64) as i64))
        }
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error("hashmacaddrextended", values),
    }
}

fn hash_macaddr8(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(value)] => Ok(Value::Int32(hash32(value))),
        [Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error("hashmacaddr8", values),
    }
}

fn hash_macaddr8_extended(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::MacAddr8(value), Value::Int64(seed)] => {
            Ok(Value::Int64(hash_bytes_extended(value, *seed as u64) as i64))
        }
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        _ => mac_func_type_error("hashmacaddr8extended", values),
    }
}

fn hash32(bytes: &[u8]) -> i32 {
    let hash = hash_bytes_extended(bytes, 0);
    let low = hash as u32;
    let high = (hash >> 32) as u32;
    (low ^ high) as i32
}

fn cmp_i32(ordering: Ordering) -> i32 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn mac_func_type_error(name: &'static str, values: &[Value]) -> Result<Value, ExecError> {
    Err(ExecError::DetailedError {
        message: format!("invalid arguments for {name}: {values:?}"),
        detail: None,
        hint: None,
        sqlstate: "42883",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macaddr_accepts_postgres_text_forms_and_renders_canonical() {
        for input in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b-010203",
            "0800.2b01.0203",
            "0800-2b01-0203",
            "08002b010203",
            "  08:00:2B:01:02:03  ",
        ] {
            let value = parse_macaddr_text(input).unwrap();
            assert_eq!(render_macaddr_text(&value), "08:00:2b:01:02:03");
        }
    }

    #[test]
    fn macaddr_rejects_invalid_text_forms() {
        for input in [
            "",
            "08:00:2b:01:02",
            "08:00:2b:01:02:0304",
            "08:00:2b:01:02:gg",
            "0800:2b01:0203",
            "08:00-2b:01:02:03",
        ] {
            assert!(parse_macaddr_text(input).is_err(), "{input}");
        }
    }

    #[test]
    fn macaddr8_accepts_pair_forms_and_expands_six_byte_input() {
        for input in [
            "08:00:2b:01:02:03:04:05",
            "08-00-2b-01-02-03-04-05",
            "08.00.2b.01.02.03.04.05",
            "08002b0102030405",
            "  08:00:2B:01:02:03:04:05  ",
        ] {
            let value = parse_macaddr8_text(input).unwrap();
            assert_eq!(render_macaddr8_text(&value), "08:00:2b:01:02:03:04:05");
        }

        let expanded = parse_macaddr8_text("08:00:2b:01:02:03").unwrap();
        assert_eq!(render_macaddr8_text(&expanded), "08:00:2b:ff:fe:01:02:03");
    }

    #[test]
    fn macaddr8_rejects_invalid_text_forms() {
        for input in [
            "",
            "08:00:2b:01:02:03:04",
            "08:00:2b:01:02:03:04:0506",
            "08:00:2b:01:02:03:04:gg",
            "08:00-2b:01:02:03:04:05",
        ] {
            assert!(parse_macaddr8_text(input).is_err(), "{input}");
        }
    }

    #[test]
    fn macaddr8_conversion_requires_embedded_fffe() {
        let mac = [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03];
        let mac8 = macaddr_to_macaddr8(mac);
        assert_eq!(mac8, [0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]);
        assert_eq!(macaddr8_to_macaddr(mac8).unwrap(), mac);

        match macaddr8_to_macaddr([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]).unwrap_err() {
            ExecError::DetailedError {
                sqlstate,
                hint: Some(hint),
                ..
            } => {
                assert_eq!(sqlstate, "22003");
                assert!(hint.contains("FF and FE"));
            }
            other => panic!("expected conversion range error, got {other:?}"),
        }
    }

    #[test]
    fn macaddr_functions_cover_compare_bitwise_trunc_and_hash_helpers() {
        let left = Value::MacAddr([0xff, 0x00, 0x2b, 0x01, 0x02, 0x03]);
        let right = Value::MacAddr([0x0f, 0x0f, 0x0f, 0x0f, 0x0f, 0x0f]);

        assert_eq!(
            eval_macaddr_function(BuiltinScalarFunction::MacAddrAnd, &[left.clone(), right])
                .unwrap()
                .unwrap(),
            Value::MacAddr([0x0f, 0x00, 0x0b, 0x01, 0x02, 0x03])
        );
        assert_eq!(
            eval_macaddr_function(
                BuiltinScalarFunction::MacAddrNot,
                std::slice::from_ref(&left)
            )
            .unwrap()
            .unwrap(),
            Value::MacAddr([0x00, 0xff, 0xd4, 0xfe, 0xfd, 0xfc])
        );
        assert_eq!(
            eval_macaddr_function(
                BuiltinScalarFunction::MacAddrTrunc,
                &[Value::MacAddr([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03])]
            )
            .unwrap()
            .unwrap(),
            Value::MacAddr([0x08, 0x00, 0x2b, 0x00, 0x00, 0x00])
        );
        assert_eq!(
            eval_macaddr_function(
                BuiltinScalarFunction::MacAddrCmp,
                &[
                    Value::MacAddr([0x08, 0, 0, 0, 0, 0]),
                    Value::MacAddr([0x09, 0, 0, 0, 0, 0])
                ]
            )
            .unwrap()
            .unwrap(),
            Value::Int32(-1)
        );
        assert!(matches!(
            eval_macaddr_function(
                BuiltinScalarFunction::HashMacAddr,
                &[Value::MacAddr([0x08, 0, 0, 0, 0, 0])]
            )
            .unwrap()
            .unwrap(),
            Value::Int32(_)
        ));
        assert!(matches!(
            eval_macaddr_function(
                BuiltinScalarFunction::HashMacAddrExtended,
                &[Value::MacAddr([0x08, 0, 0, 0, 0, 0]), Value::Int64(42)]
            )
            .unwrap()
            .unwrap(),
            Value::Int64(_)
        ));
    }
}
