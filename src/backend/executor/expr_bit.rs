use super::ExecError;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::BitString;
use std::cmp::Ordering;

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

pub(crate) fn compare_bit_strings(left: &BitString, right: &BitString) -> Ordering {
    let min_len = left.bytes.len().min(right.bytes.len());
    match left.bytes[..min_len].cmp(&right.bytes[..min_len]) {
        Ordering::Equal => left.bit_len.cmp(&right.bit_len),
        other => other,
    }
}

pub(crate) fn concat_bit_strings(left: &BitString, right: &BitString) -> BitString {
    let mut out = vec![0u8; BitString::byte_len(left.bit_len + right.bit_len)];
    for bit_idx in 0..left.bit_len.max(0) as usize {
        if bit_is_set(left, bit_idx as i32) {
            set_raw_bit(&mut out, bit_idx, true);
        }
    }
    let left_len = left.bit_len.max(0) as usize;
    for bit_idx in 0..right.bit_len.max(0) as usize {
        if bit_is_set(right, bit_idx as i32) {
            set_raw_bit(&mut out, left_len + bit_idx, true);
        }
    }
    BitString::new(left.bit_len + right.bit_len, out)
}

pub(crate) fn bitwise_not(bits: &BitString) -> BitString {
    let mut bytes = bits.bytes.clone();
    for byte in &mut bytes {
        *byte = !*byte;
    }
    BitString::new(bits.bit_len, bytes)
}

pub(crate) fn bitwise_binary(
    op: &'static str,
    left: &BitString,
    right: &BitString,
) -> Result<BitString, ExecError> {
    if left.bit_len != right.bit_len {
        return Err(ExecError::BitStringSizeMismatch { op });
    }
    let bytes = left
        .bytes
        .iter()
        .zip(right.bytes.iter())
        .map(|(l, r)| match op {
            "&" => *l & *r,
            "|" => *l | *r,
            "#" => *l ^ *r,
            _ => unreachable!(),
        })
        .collect();
    Ok(BitString::new(left.bit_len, bytes))
}

pub(crate) fn shift_left(bits: &BitString, count: i32) -> BitString {
    shift(bits, count, true)
}

pub(crate) fn shift_right(bits: &BitString, count: i32) -> BitString {
    shift(bits, count, false)
}

pub(crate) fn bit_length(bits: &BitString) -> i32 {
    bits.bit_len
}

pub(crate) fn substring(
    bits: &BitString,
    start: i32,
    len: Option<i32>,
) -> Result<BitString, ExecError> {
    let total = bits.bit_len.max(0);
    let start = start as i64;
    let total_i64 = total as i64;
    let effective_start = start.max(1);
    let start_index = (effective_start - 1).min(total_i64).max(0) as i32;
    let available = total.saturating_sub(start_index);
    let desired = match len {
        Some(len) if len < 0 => return Err(ExecError::NegativeSubstringLength),
        Some(len) => {
            let adjusted = (len as i64).saturating_sub((1 - start).max(0));
            adjusted.max(0).min(i32::MAX as i64) as i32
        }
        None => available,
    };
    let take = desired.min(available);
    Ok(slice_bits(bits, start_index, take))
}

pub(crate) fn overlay(
    bits: &BitString,
    placing: &BitString,
    start: i32,
    len: Option<i32>,
) -> Result<BitString, ExecError> {
    let replace_len = len.unwrap_or(placing.bit_len);
    if replace_len < 0 {
        return Err(ExecError::NegativeSubstringLength);
    }
    let prefix_len = (start - 1).max(0).min(bits.bit_len);
    let suffix_start = (start - 1)
        .saturating_add(replace_len)
        .max(0)
        .min(bits.bit_len);
    let prefix = slice_bits(bits, 0, prefix_len);
    let suffix = slice_bits(
        bits,
        suffix_start,
        bits.bit_len.saturating_sub(suffix_start),
    );
    Ok(concat_bit_strings(
        &concat_bit_strings(&prefix, placing),
        &suffix,
    ))
}

pub(crate) fn position(needle: &BitString, haystack: &BitString) -> i32 {
    if needle.bit_len == 0 {
        return if haystack.bit_len == 0 { 0 } else { 1 };
    }
    if needle.bit_len > haystack.bit_len {
        return 0;
    }
    for start in 0..=(haystack.bit_len - needle.bit_len) {
        if slice_bits(haystack, start, needle.bit_len) == *needle {
            return start + 1;
        }
    }
    0
}

pub(crate) fn get_bit(bits: &BitString, index: i32) -> Result<i32, ExecError> {
    validate_bit_index(bits, index)?;
    Ok(if bit_is_set(bits, index) { 1 } else { 0 })
}

pub(crate) fn set_bit(
    bits: &BitString,
    index: i32,
    new_value: i32,
) -> Result<BitString, ExecError> {
    validate_bit_index(bits, index)?;
    let mut out = bits.bytes.clone();
    set_raw_bit(&mut out, index as usize, new_value != 0);
    Ok(BitString::new(bits.bit_len, out))
}

pub(crate) fn bit_count(bits: &BitString) -> i64 {
    bits.bytes.iter().map(|b| b.count_ones() as i64).sum()
}

fn shift(bits: &BitString, count: i32, left: bool) -> BitString {
    let count = count.max(0) as usize;
    let len = bits.bit_len.max(0) as usize;
    let mut out = vec![0u8; BitString::byte_len(bits.bit_len)];
    if count >= len {
        return BitString::new(bits.bit_len, out);
    }
    for bit_idx in 0..len {
        let src_idx = if left {
            bit_idx + count
        } else if bit_idx >= count {
            bit_idx - count
        } else {
            continue;
        };
        if src_idx < len && bit_is_set(bits, src_idx as i32) {
            set_raw_bit(&mut out, bit_idx, true);
        }
    }
    BitString::new(bits.bit_len, out)
}

fn slice_bits(bits: &BitString, start: i32, len: i32) -> BitString {
    if len <= 0 {
        return BitString::new(0, Vec::new());
    }
    let mut out = vec![0u8; BitString::byte_len(len)];
    for offset in 0..len as usize {
        if bit_is_set(bits, start + offset as i32) {
            set_raw_bit(&mut out, offset, true);
        }
    }
    BitString::new(len, out)
}

fn validate_bit_index(bits: &BitString, index: i32) -> Result<(), ExecError> {
    if index < 0 || index >= bits.bit_len {
        return Err(ExecError::BitIndexOutOfRange {
            index,
            max_index: bits.bit_len.saturating_sub(1),
        });
    }
    Ok(())
}

fn bit_is_set(bits: &BitString, index: i32) -> bool {
    if index < 0 || index >= bits.bit_len {
        return false;
    }
    let idx = index as usize;
    ((bits.bytes[idx / 8] >> (7 - (idx % 8))) & 1) != 0
}

fn set_raw_bit(bytes: &mut [u8], index: usize, value: bool) {
    let mask = 1 << (7 - (index % 8));
    if value {
        bytes[index / 8] |= mask;
    } else {
        bytes[index / 8] &= !mask;
    }
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
