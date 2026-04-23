use super::ExecError;
use crate::include::nodes::datum::{InetValue, Value};
use std::cmp::Ordering;
use std::net::IpAddr;

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

pub(crate) fn compare_network_values(left: &InetValue, right: &InetValue) -> Ordering {
    network_sort_key(left).cmp(&network_sort_key(right))
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
    let addr: IpAddr = addr_text
        .parse()
        .map_err(|_| network_input_error(text, cidr))?;
    let max_bits = max_bits(addr);
    let bits = if bits_text.is_empty() {
        max_bits
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
        return Err(ExecError::InvalidStorageValue {
            column: "cidr".into(),
            details: format!("invalid cidr value: \"{text}\" has bits set to right of mask"),
        });
    }
    Ok(value)
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

fn network_sort_key(value: &InetValue) -> (u8, [u8; 16], u8) {
    match value.addr {
        IpAddr::V4(addr) => {
            let mut bytes = [0; 16];
            bytes[..4].copy_from_slice(&addr.octets());
            (4, bytes, value.bits)
        }
        IpAddr::V6(addr) => (6, addr.octets(), value.bits),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_cidr_text, parse_inet_text};

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
            crate::backend::executor::ExecError::InvalidStorageValue { details, .. } => {
                assert!(details.contains("bits set to right of mask"));
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
    fn ipv6_inet_supports_prefix() {
        let value = parse_inet_text("2001:db8::1/64").unwrap();

        assert_eq!(value.bits, 64);
        assert_eq!(value.render_inet(), "2001:db8::1/64");
    }
}
