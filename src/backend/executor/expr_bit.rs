use super::ExecError;
use crate::include::nodes::datum::BitString;
use crate::backend::parser::{SqlType, SqlTypeKind};

pub(crate) fn parse_bit_text(text: &str) -> Result<BitString, ExecError> {
    let trimmed = text.trim_matches(|ch: char| ch.is_ascii_whitespace());
    let (radix, digits) = match trimmed.as_bytes() {
        [prefix @ (b'b' | b'B' | b'x' | b'X'), rest @ ..] => (*prefix, rest),
        _ => (b'b', trimmed.as_bytes()),
    };

    match radix {
        b'b' | b'B' => parse_binary_bits(digits),
        b'x' | b'X' => parse_hex_bits(digits),
        _ => unreachable!(),
    }
}

pub(crate) fn render_bit_text(bits: &BitString) -> String {
    bits.render()
}

pub(crate) fn coerce_bit_string(
    bits: BitString,
    ty: SqlType,
    explicit: bool,
) -> Result<BitString, ExecError> {
    match ty.kind {
        SqlTypeKind::Bit => {
            let target_len = ty.bit_len().unwrap_or(1);
            if bits.bit_len == target_len {
                Ok(bits)
            } else if explicit {
                Ok(resize_bit_string(bits, target_len))
            } else {
                Err(ExecError::BitStringLengthMismatch {
                    actual: bits.bit_len,
                    expected: target_len,
                })
            }
        }
        SqlTypeKind::VarBit => match ty.bit_len() {
            None => Ok(bits),
            Some(target_len) if bits.bit_len <= target_len => Ok(bits),
            Some(target_len) if explicit => Ok(resize_bit_string(bits, target_len)),
            Some(target_len) => Err(ExecError::BitStringTooLong {
                actual: bits.bit_len,
                limit: target_len,
            }),
        },
        _ => Ok(bits),
    }
}

pub(crate) fn resize_bit_string(bits: BitString, target_len: i32) -> BitString {
    if target_len <= 0 {
        return BitString::new(0, Vec::new());
    }
    let mut out = vec![0u8; BitString::byte_len(target_len)];
    let copy_bits = bits.bit_len.min(target_len).max(0) as usize;
    for bit_idx in 0..copy_bits {
        let src_byte = bits.bytes[bit_idx / 8];
        let src_shift = 7 - (bit_idx % 8);
        if ((src_byte >> src_shift) & 1) != 0 {
            out[bit_idx / 8] |= 1 << (7 - (bit_idx % 8));
        }
    }
    BitString::new(target_len, out)
}

fn parse_binary_bits(digits: &[u8]) -> Result<BitString, ExecError> {
    let mut bytes = vec![0u8; BitString::byte_len(digits.len() as i32)];
    for (idx, ch) in digits.iter().enumerate() {
        match ch {
            b'0' => {}
            b'1' => {
                bytes[idx / 8] |= 1 << (7 - (idx % 8));
            }
            other => {
                return Err(ExecError::InvalidBitInput {
                    digit: char::from(*other),
                    is_hex: false,
                });
            }
        }
    }
    Ok(BitString::new(digits.len() as i32, bytes))
}

fn parse_hex_bits(digits: &[u8]) -> Result<BitString, ExecError> {
    let mut bytes = Vec::with_capacity(digits.len().div_ceil(2));
    let mut pending = None::<u8>;
    for ch in digits {
        let nibble = match ch {
            b'0'..=b'9' => *ch - b'0',
            b'a'..=b'f' => *ch - b'a' + 10,
            b'A'..=b'F' => *ch - b'A' + 10,
            other => {
                return Err(ExecError::InvalidBitInput {
                    digit: char::from(*other),
                    is_hex: true,
                });
            }
        };
        if let Some(high) = pending.take() {
            bytes.push((high << 4) | nibble);
        } else {
            pending = Some(nibble);
        }
    }
    if let Some(high) = pending {
        bytes.push(high << 4);
    }
    Ok(BitString::new((digits.len() * 4) as i32, bytes))
}
