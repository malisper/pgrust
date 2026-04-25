use super::ExecError;
use crate::include::nodes::datum::{InetValue, Value};
use crate::include::nodes::primnodes::BuiltinScalarFunction;
use crate::pgrust::compact_string::CompactString;
use std::cmp::Ordering;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub(crate) fn parse_inet_text(text: &str) -> Result<InetValue, ExecError> {
    parse_network_text(text, false)
}

pub(crate) fn parse_cidr_text(text: &str) -> Result<InetValue, ExecError> {
    parse_network_text(text, true)
}

pub(crate) fn parse_inet_bytes(bytes: &[u8]) -> Result<InetValue, ExecError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ExecError::InvalidStorageValue {
        column: "inet".into(),
        details: "invalid inet storage payload".into(),
    })?;
    parse_inet_text(text)
}

pub(crate) fn parse_cidr_bytes(bytes: &[u8]) -> Result<InetValue, ExecError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ExecError::InvalidStorageValue {
        column: "cidr".into(),
        details: "invalid cidr storage payload".into(),
    })?;
    parse_cidr_text(text)
}

pub(crate) fn render_network_text(value: &Value) -> Option<String> {
    match value {
        Value::Inet(value) => Some(value.render_inet()),
        Value::Cidr(value) => Some(value.render_cidr()),
        _ => None,
    }
}

pub(crate) fn network_bitwise_not(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Inet(value) | Value::Cidr(value) => Ok(Value::Inet(InetValue {
            addr: invert_addr(value.addr),
            bits: value.bits,
        })),
        other => Err(network_type_mismatch("~", other, Value::Null)),
    }
}

pub(crate) fn network_bitwise_binary(
    op: &'static str,
    left: Value,
    right: Value,
) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Inet(left) | Value::Cidr(left), Value::Inet(right) | Value::Cidr(right)) => {
            if max_bits(left.addr) != max_bits(right.addr) {
                return Err(network_type_mismatch(
                    op,
                    Value::Inet(left),
                    Value::Inet(right),
                ));
            }
            Ok(Value::Inet(InetValue {
                addr: match op {
                    "&" => binary_addr(left.addr, right.addr, |l, r| l & r),
                    "|" => binary_addr(left.addr, right.addr, |l, r| l | r),
                    _ => return Err(network_arity_error(op)),
                },
                bits: left.bits,
            }))
        }
        (left, right) => Err(network_type_mismatch(op, left, right)),
    }
}

pub(crate) fn network_add(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Inet(value) | Value::Cidr(value), offset) => {
            let offset = int_value_to_i64(&offset).ok_or_else(|| {
                network_type_mismatch("+", Value::Inet(value.clone()), offset.clone())
            })?;
            Ok(Value::Inet(offset_network_addr(
                &value,
                i128::from(offset),
            )?))
        }
        (offset, Value::Inet(value) | Value::Cidr(value)) => {
            let offset = int_value_to_i64(&offset).ok_or_else(|| {
                network_type_mismatch("+", offset.clone(), Value::Inet(value.clone()))
            })?;
            Ok(Value::Inet(offset_network_addr(
                &value,
                i128::from(offset),
            )?))
        }
        (left, right) => Err(network_type_mismatch("+", left, right)),
    }
}

pub(crate) fn network_sub(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Inet(left) | Value::Cidr(left), Value::Inet(right) | Value::Cidr(right)) => {
            if max_bits(left.addr) != max_bits(right.addr) {
                return Err(result_out_of_range());
            }
            let left = addr_u128(left.addr);
            let right = addr_u128(right.addr);
            let diff = if left >= right {
                i64::try_from(left - right).map_err(|_| result_out_of_range())?
            } else {
                let magnitude = i64::try_from(right - left).map_err(|_| result_out_of_range())?;
                magnitude.checked_neg().ok_or_else(result_out_of_range)?
            };
            Ok(Value::Int64(diff))
        }
        (Value::Inet(value) | Value::Cidr(value), offset) => {
            let offset = int_value_to_i64(&offset).ok_or_else(|| {
                network_type_mismatch("-", Value::Inet(value.clone()), offset.clone())
            })?;
            Ok(Value::Inet(offset_network_addr(
                &value,
                -i128::from(offset),
            )?))
        }
        (left, right) => Err(network_type_mismatch("-", left, right)),
    }
}

pub(crate) fn compare_network_values(left: &InetValue, right: &InetValue) -> Ordering {
    let left_family = max_bits(left.addr);
    let right_family = max_bits(right.addr);
    if left_family != right_family {
        return left_family.cmp(&right_family);
    }

    let order = compare_network_prefix(left.addr, right.addr, left.bits.min(right.bits));
    if order != Ordering::Equal {
        return order;
    }
    let order = left.bits.cmp(&right.bits);
    if order != Ordering::Equal {
        return order;
    }
    compare_network_prefix(left.addr, right.addr, left_family)
}

fn int_value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int16(value) => Some(i64::from(*value)),
        Value::Int32(value) => Some(i64::from(*value)),
        Value::Int64(value) => Some(*value),
        _ => None,
    }
}

fn offset_network_addr(value: &InetValue, offset: i128) -> Result<InetValue, ExecError> {
    let max_value = match value.addr {
        IpAddr::V4(_) => u128::from(u32::MAX),
        IpAddr::V6(_) => u128::MAX,
    };
    let raw = addr_u128(value.addr);
    let shifted = if offset >= 0 {
        raw.checked_add(offset as u128)
    } else {
        raw.checked_sub(offset.unsigned_abs())
    }
    .filter(|shifted| *shifted <= max_value)
    .ok_or_else(result_out_of_range)?;
    Ok(InetValue {
        addr: addr_from_u128(value.addr, shifted),
        bits: value.bits,
    })
}

fn binary_addr(left: IpAddr, right: IpAddr, f: impl FnOnce(u128, u128) -> u128) -> IpAddr {
    addr_from_u128(left, f(addr_u128(left), addr_u128(right)))
}

fn addr_u128(addr: IpAddr) -> u128 {
    match addr {
        IpAddr::V4(addr) => u128::from(u32::from(addr)),
        IpAddr::V6(addr) => u128::from(addr),
    }
}

fn addr_from_u128(family: IpAddr, raw: u128) -> IpAddr {
    match family {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::from(raw as u32)),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::from(raw)),
    }
}

fn result_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "result is out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

pub(crate) fn encode_network_bytes(value: &InetValue, cidr: bool) -> Vec<u8> {
    if cidr {
        value.render_cidr().into_bytes()
    } else {
        value.render_inet().into_bytes()
    }
}

fn parse_network_text(text: &str, cidr: bool) -> Result<InetValue, ExecError> {
    let trimmed = text.trim();
    let (addr_text, bits_text) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    let (addr, inferred_bits) =
        parse_network_addr(addr_text, cidr).ok_or_else(|| network_input_error(text, cidr))?;
    let max_bits = max_bits(addr);
    let bits = if bits_text.is_empty() {
        inferred_bits.unwrap_or(max_bits)
    } else {
        let parsed = bits_text
            .parse::<u8>()
            .map_err(|_| network_input_error(text, cidr))?;
        if parsed > max_bits {
            return Err(network_input_error(text, cidr));
        }
        parsed
    };
    let value = InetValue { addr, bits };
    if cidr && !host_bits_are_zero(&value) {
        return Err(ExecError::DetailedError {
            message: format!("invalid cidr value: \"{text}\""),
            detail: Some("Value has bits set to right of mask.".into()),
            hint: None,
            sqlstate: "22P02",
        });
    }
    Ok(value)
}

fn parse_network_addr(text: &str, cidr: bool) -> Option<(IpAddr, Option<u8>)> {
    if let Some(addr) = parse_ipv4_decimal(text) {
        return Some((addr, None));
    }
    if let Ok(addr) = text.parse::<IpAddr>() {
        return Some((addr, None));
    }
    parse_ipv4_shorthand(text, cidr)
}

fn parse_ipv4_decimal(text: &str) -> Option<IpAddr> {
    let parts = text.split('.').collect::<Vec<_>>();
    if parts.len() != 4 {
        return None;
    }
    let mut octets = [0u8; 4];
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() || !part.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        octets[index] = part.parse::<u8>().ok()?;
    }
    Some(IpAddr::V4(Ipv4Addr::from(octets)))
}

fn parse_ipv4_shorthand(text: &str, cidr: bool) -> Option<(IpAddr, Option<u8>)> {
    let parts = text.split('.').collect::<Vec<_>>();
    if parts.len() >= 4 || parts.is_empty() {
        return None;
    }
    let mut octets = [0u8; 4];
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() || !part.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        octets[index] = part.parse::<u8>().ok()?;
    }
    let bits = cidr.then_some((parts.len() as u8) * 8);
    Some((IpAddr::V4(Ipv4Addr::from(octets)), bits))
}

fn network_input_error(text: &str, cidr: bool) -> ExecError {
    ExecError::InvalidStorageValue {
        column: if cidr { "cidr" } else { "inet" }.into(),
        details: format!(
            "invalid input syntax for type {}: \"{text}\"",
            if cidr { "cidr" } else { "inet" }
        ),
    }
}

fn max_bits(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

fn host_bits_are_zero(value: &InetValue) -> bool {
    match value.addr {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = if value.bits == 0 {
                0
            } else {
                u32::MAX << (32 - value.bits)
            };
            raw & !mask == 0
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = if value.bits == 0 {
                0
            } else {
                u128::MAX << (128 - value.bits)
            };
            raw & !mask == 0
        }
    }
}

fn compare_network_prefix(left: IpAddr, right: IpAddr, bits: u8) -> Ordering {
    match (left, right) {
        (IpAddr::V4(left), IpAddr::V4(right)) => {
            let mask = prefix_mask_u32(bits);
            (u32::from(left) & mask).cmp(&(u32::from(right) & mask))
        }
        (IpAddr::V6(left), IpAddr::V6(right)) => {
            let mask = prefix_mask_u128(bits);
            (u128::from(left) & mask).cmp(&(u128::from(right) & mask))
        }
        (left, right) => max_bits(left).cmp(&max_bits(right)),
    }
}

pub(crate) fn eval_network_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    Some(match func {
        BuiltinScalarFunction::NetworkHost => eval_unary_network(values, |value, _| {
            Value::Text(CompactString::from_owned(render_host(value)))
        }),
        BuiltinScalarFunction::NetworkAbbrev => eval_unary_network(values, |value, is_cidr| {
            Value::Text(CompactString::from_owned(if is_cidr {
                render_cidr_abbrev(value)
            } else {
                value.render_inet()
            }))
        }),
        BuiltinScalarFunction::NetworkBroadcast => {
            eval_unary_network(values, |value, _| Value::Inet(network_broadcast(value)))
        }
        BuiltinScalarFunction::NetworkNetwork => {
            eval_unary_network(values, |value, _| Value::Cidr(network_prefix(value)))
        }
        BuiltinScalarFunction::NetworkMasklen => {
            eval_unary_network(values, |value, _| Value::Int32(i32::from(value.bits)))
        }
        BuiltinScalarFunction::NetworkFamily => eval_unary_network(values, |value, _| {
            Value::Int32(match value.addr {
                IpAddr::V4(_) => 4,
                IpAddr::V6(_) => 6,
            })
        }),
        BuiltinScalarFunction::NetworkNetmask => {
            eval_unary_network(values, |value, _| Value::Inet(network_netmask(value)))
        }
        BuiltinScalarFunction::NetworkHostmask => {
            eval_unary_network(values, |value, _| Value::Inet(network_hostmask(value)))
        }
        BuiltinScalarFunction::NetworkSetMasklen => eval_set_masklen(values),
        BuiltinScalarFunction::NetworkSameFamily => eval_binary_network(values, |left, right| {
            Value::Bool(max_bits(left.addr) == max_bits(right.addr))
        }),
        BuiltinScalarFunction::NetworkMerge => eval_network_merge(values),
        BuiltinScalarFunction::NetworkSubnet => eval_binary_network(values, |left, right| {
            Value::Bool(network_contains(right, left, true))
        }),
        BuiltinScalarFunction::NetworkSubnetEq => eval_binary_network(values, |left, right| {
            Value::Bool(network_contains(right, left, false))
        }),
        BuiltinScalarFunction::NetworkSupernet => eval_binary_network(values, |left, right| {
            Value::Bool(network_contains(left, right, true))
        }),
        BuiltinScalarFunction::NetworkSupernetEq => eval_binary_network(values, |left, right| {
            Value::Bool(network_contains(left, right, false))
        }),
        BuiltinScalarFunction::NetworkOverlap => eval_binary_network(values, |left, right| {
            Value::Bool(
                network_contains(left, right, false) || network_contains(right, left, false),
            )
        }),
        _ => return None,
    })
}

fn eval_unary_network(
    values: &[Value],
    f: impl FnOnce(&InetValue, bool) -> Value,
) -> Result<Value, ExecError> {
    match values {
        [Value::Inet(value)] => Ok(f(value, false)),
        [Value::Cidr(value)] => Ok(f(value, true)),
        [other] => Err(network_type_mismatch(
            "network function",
            other.clone(),
            Value::Null,
        )),
        _ => Err(network_arity_error("network function")),
    }
}

fn eval_binary_network(
    values: &[Value],
    f: impl FnOnce(&InetValue, &InetValue) -> Value,
) -> Result<Value, ExecError> {
    match values {
        [
            Value::Inet(left) | Value::Cidr(left),
            Value::Inet(right) | Value::Cidr(right),
        ] => Ok(f(left, right)),
        [left, right] => Err(network_type_mismatch(
            "network function",
            left.clone(),
            right.clone(),
        )),
        _ => Err(network_arity_error("network function")),
    }
}

fn eval_set_masklen(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Inet(value), Value::Int32(bits)] => {
            Ok(Value::Inet(set_masklen(value, *bits, false)?))
        }
        [Value::Cidr(value), Value::Int32(bits)] => {
            Ok(Value::Cidr(set_masklen(value, *bits, true)?))
        }
        [left, right] => Err(network_type_mismatch(
            "set_masklen",
            left.clone(),
            right.clone(),
        )),
        _ => Err(network_arity_error("set_masklen")),
    }
}

fn eval_network_merge(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [
            Value::Inet(left) | Value::Cidr(left),
            Value::Inet(right) | Value::Cidr(right),
        ] => {
            if max_bits(left.addr) != max_bits(right.addr) {
                return Err(ExecError::DetailedError {
                    message: "cannot merge addresses from different families".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
            Ok(Value::Cidr(network_merge(left, right)))
        }
        [left, right] => Err(network_type_mismatch(
            "inet_merge",
            left.clone(),
            right.clone(),
        )),
        _ => Err(network_arity_error("inet_merge")),
    }
}

fn set_masklen(value: &InetValue, bits: i32, cidr: bool) -> Result<InetValue, ExecError> {
    let max_bits = i32::from(value.max_bits());
    if bits < -1 || bits > max_bits {
        return Err(ExecError::DetailedError {
            message: format!("invalid mask length: {bits}"),
            detail: None,
            hint: None,
            sqlstate: "22003",
        });
    }
    let bits = if bits == -1 {
        max_bits as u8
    } else {
        bits as u8
    };
    let mut out = InetValue {
        addr: value.addr,
        bits,
    };
    if cidr {
        out = network_prefix(&out);
    }
    Ok(out)
}

fn render_host(value: &InetValue) -> String {
    match value.addr {
        IpAddr::V4(addr) => addr.to_string(),
        IpAddr::V6(addr) => InetValue {
            addr: IpAddr::V6(addr),
            bits: value.max_bits(),
        }
        .render_inet(),
    }
}

fn render_cidr_abbrev(value: &InetValue) -> String {
    match value.addr {
        IpAddr::V4(addr) if value.bits % 8 == 0 && value.bits < 32 => {
            let octets = addr.octets();
            let keep = usize::from(value.bits / 8).max(1);
            if octets[keep..].iter().all(|octet| *octet == 0) {
                let prefix = octets[..keep]
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(".");
                return format!("{prefix}/{}", value.bits);
            }
            value.render_cidr()
        }
        _ => value.render_cidr(),
    }
}

fn network_prefix(value: &InetValue) -> InetValue {
    InetValue {
        addr: mask_addr(value.addr, value.bits, false),
        bits: value.bits,
    }
}

fn network_broadcast(value: &InetValue) -> InetValue {
    InetValue {
        addr: mask_addr(value.addr, value.bits, true),
        bits: value.bits,
    }
}

fn network_netmask(value: &InetValue) -> InetValue {
    InetValue {
        addr: mask_addr(all_ones_addr(value.addr), value.bits, false),
        bits: value.max_bits(),
    }
}

fn network_hostmask(value: &InetValue) -> InetValue {
    InetValue {
        addr: invert_addr(network_netmask(value).addr),
        bits: value.max_bits(),
    }
}

pub(crate) fn network_merge(left: &InetValue, right: &InetValue) -> InetValue {
    let max = left.max_bits();
    let common = common_prefix_bits(left.addr, right.addr, max);
    let bits = left.bits.min(right.bits).min(common);
    network_prefix(&InetValue {
        addr: left.addr,
        bits,
    })
}

fn common_prefix_bits(left: IpAddr, right: IpAddr, max: u8) -> u8 {
    match (left, right) {
        (IpAddr::V4(left), IpAddr::V4(right)) => {
            let xor = u32::from(left) ^ u32::from(right);
            if xor == 0 {
                max
            } else {
                xor.leading_zeros() as u8
            }
        }
        (IpAddr::V6(left), IpAddr::V6(right)) => {
            let xor = u128::from(left) ^ u128::from(right);
            if xor == 0 {
                max
            } else {
                xor.leading_zeros() as u8
            }
        }
        _ => 0,
    }
}

pub(crate) fn network_contains(container: &InetValue, value: &InetValue, strict: bool) -> bool {
    if max_bits(container.addr) != max_bits(value.addr) {
        return false;
    }
    if strict && container.bits >= value.bits {
        return false;
    }
    if !strict && container.bits > value.bits {
        return false;
    }
    network_prefix(&InetValue {
        addr: value.addr,
        bits: container.bits,
    })
    .addr
        == network_prefix(container).addr
}

fn mask_addr(addr: IpAddr, bits: u8, fill_host: bool) -> IpAddr {
    match addr {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = prefix_mask_u32(bits);
            let raw = if fill_host { raw | !mask } else { raw & mask };
            IpAddr::V4(Ipv4Addr::from(raw))
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = prefix_mask_u128(bits);
            let raw = if fill_host { raw | !mask } else { raw & mask };
            IpAddr::V6(Ipv6Addr::from(raw))
        }
    }
}

fn all_ones_addr(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::from(u32::MAX)),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::from(u128::MAX)),
    }
}

fn invert_addr(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V4(addr) => IpAddr::V4(Ipv4Addr::from(!u32::from(addr))),
        IpAddr::V6(addr) => IpAddr::V6(Ipv6Addr::from(!u128::from(addr))),
    }
}

fn prefix_mask_u32(bits: u8) -> u32 {
    if bits == 0 {
        0
    } else {
        u32::MAX << (32 - bits)
    }
}

fn prefix_mask_u128(bits: u8) -> u128 {
    if bits == 0 {
        0
    } else {
        u128::MAX << (128 - bits)
    }
}

fn network_type_mismatch(op: &'static str, left: Value, right: Value) -> ExecError {
    ExecError::TypeMismatch { op, left, right }
}

fn network_arity_error(op: &'static str) -> ExecError {
    ExecError::TypeMismatch {
        op,
        left: Value::Null,
        right: Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        network_broadcast, network_hostmask, network_netmask, network_prefix, parse_cidr_text,
        parse_inet_text, render_cidr_abbrev,
    };

    #[test]
    fn inet_default_host_prefix_is_not_rendered() {
        let value = parse_inet_text("192.168.1.10").unwrap();

        assert_eq!(value.bits, 32);
        assert_eq!(value.render_inet(), "192.168.1.10");
    }

    #[test]
    fn inet_preserves_non_host_prefix() {
        let value = parse_inet_text("192.168.1.10/24").unwrap();

        assert_eq!(value.bits, 24);
        assert_eq!(value.render_inet(), "192.168.1.10/24");
    }

    #[test]
    fn cidr_requires_host_bits_to_be_clear() {
        let err = parse_cidr_text("192.168.1.10/24").unwrap_err();

        match err {
            crate::backend::executor::ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "invalid cidr value: \"192.168.1.10/24\"");
                assert_eq!(
                    detail.as_deref(),
                    Some("Value has bits set to right of mask.")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn cidr_renders_with_prefix() {
        let value = parse_cidr_text("192.168.1.0/24").unwrap();

        assert_eq!(value.bits, 24);
        assert_eq!(value.render_cidr(), "192.168.1.0/24");
    }

    #[test]
    fn cidr_accepts_postgres_ipv4_shorthand() {
        assert_eq!(
            parse_cidr_text("192.168.1").unwrap().render_cidr(),
            "192.168.1.0/24"
        );
        assert_eq!(parse_cidr_text("10").unwrap().render_cidr(), "10.0.0.0/8");
        assert_eq!(
            parse_cidr_text("10.1").unwrap().render_cidr(),
            "10.1.0.0/16"
        );
        assert_eq!(
            parse_cidr_text("10.1.2").unwrap().render_cidr(),
            "10.1.2.0/24"
        );
        assert_eq!(
            parse_cidr_text("10.0.0.0").unwrap().render_cidr(),
            "10.0.0.0/32"
        );
    }

    #[test]
    fn inet_accepts_decimal_ipv4_with_leading_zeroes() {
        assert_eq!(
            parse_inet_text("255.255.000.000/0").unwrap().render_inet(),
            "255.255.0.0/0"
        );
    }

    #[test]
    fn network_rendering_preserves_dotted_ipv6_tail() {
        assert_eq!(
            parse_cidr_text("::ffff:1.2.3.4").unwrap().render_cidr(),
            "::ffff:1.2.3.4/128"
        );
        assert_eq!(
            parse_inet_text("::4.3.2.1/24").unwrap().render_inet(),
            "::4.3.2.1/24"
        );
    }

    #[test]
    fn network_helpers_match_postgres_shapes() {
        let value = parse_inet_text("192.168.1.226/24").unwrap();
        assert_eq!(network_prefix(&value).render_cidr(), "192.168.1.0/24");
        assert_eq!(network_broadcast(&value).render_inet(), "192.168.1.255/24");
        assert_eq!(network_netmask(&value).render_inet(), "255.255.255.0");
        assert_eq!(network_hostmask(&value).render_inet(), "0.0.0.255");
        assert_eq!(
            render_cidr_abbrev(&parse_cidr_text("10.0.0.0/8").unwrap()),
            "10/8"
        );
    }

    #[test]
    fn ipv6_inet_supports_prefix() {
        let value = parse_inet_text("2001:db8::1/64").unwrap();

        assert_eq!(value.bits, 64);
        assert_eq!(value.render_inet(), "2001:db8::1/64");
    }
}
