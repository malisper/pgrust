use super::super::ExecError;
use pgrust_core::CompactString;
use pgrust_nodes::tsearch::{TsLexeme, TsPosition, TsVector, TsWeight};

pub fn parse_tsvector_text(text: &str) -> Result<TsVector, ExecError> {
    TsVector::parse(text).map_err(|message| tsvector_input_parse_error(text, message))
}

pub fn render_tsvector_text(vector: &TsVector) -> String {
    vector.render()
}

pub fn encode_tsvector_bytes(vector: &TsVector) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(vector.lexemes.len() as u32).to_le_bytes());
    for lexeme in &vector.lexemes {
        let text = lexeme.text.as_bytes();
        out.extend_from_slice(&(text.len() as u32).to_le_bytes());
        out.extend_from_slice(text);
        out.extend_from_slice(&(lexeme.positions.len() as u32).to_le_bytes());
        for position in &lexeme.positions {
            out.extend_from_slice(&position.position.to_le_bytes());
            out.push(weight_code(position.weight));
        }
    }
    out
}

pub fn decode_tsvector_bytes(bytes: &[u8]) -> Result<TsVector, ExecError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.read_u32()? as usize;
    let mut lexemes = Vec::with_capacity(count);
    for _ in 0..count {
        let text_len = cursor.read_u32()? as usize;
        let text = cursor.read_str(text_len)?;
        let positions_len = cursor.read_u32()? as usize;
        let mut positions = Vec::with_capacity(positions_len);
        for _ in 0..positions_len {
            let position = cursor.read_u16()?;
            let weight = decode_weight(cursor.read_u8()?)?;
            positions.push(TsPosition { position, weight });
        }
        lexemes.push(TsLexeme {
            text: CompactString::from_owned(text),
            positions,
        });
    }
    cursor.ensure_finished()?;
    Ok(TsVector::new(lexemes))
}

pub fn tsvector_input_error(message: String) -> ExecError {
    ExecError::InvalidStorageValue {
        column: "<tsvector>".into(),
        details: message,
    }
}

fn tsvector_input_parse_error(text: &str, _message: String) -> ExecError {
    ExecError::DetailedError {
        message: format!("syntax error in tsvector: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

pub fn weight_code(weight: Option<TsWeight>) -> u8 {
    match weight {
        None => 0,
        Some(TsWeight::D) => 1,
        Some(TsWeight::C) => 2,
        Some(TsWeight::B) => 3,
        Some(TsWeight::A) => 4,
    }
}

pub fn decode_weight(code: u8) -> Result<Option<TsWeight>, ExecError> {
    match code {
        0 => Ok(None),
        1 => Ok(Some(TsWeight::D)),
        2 => Ok(Some(TsWeight::C)),
        3 => Ok(Some(TsWeight::B)),
        4 => Ok(Some(TsWeight::A)),
        other => Err(ExecError::InvalidStorageValue {
            column: "<tsvector>".into(),
            details: format!("invalid tsvector weight code {other}"),
        }),
    }
}

pub struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub fn read_u8(&mut self) -> Result<u8, ExecError> {
        if self.offset >= self.bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<tsvector>".into(),
                details: "unexpected end of tsvector payload".into(),
            });
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    pub fn read_u16(&mut self) -> Result<u16, ExecError> {
        if self.offset + 2 > self.bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<tsvector>".into(),
                details: "truncated 2-byte field in tsvector payload".into(),
            });
        }
        let value =
            u16::from_le_bytes(self.bytes[self.offset..self.offset + 2].try_into().unwrap());
        self.offset += 2;
        Ok(value)
    }

    pub fn read_u32(&mut self) -> Result<u32, ExecError> {
        if self.offset + 4 > self.bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<tsvector>".into(),
                details: "truncated 4-byte field in tsvector payload".into(),
            });
        }
        let value =
            u32::from_le_bytes(self.bytes[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(value)
    }

    pub fn read_str(&mut self, len: usize) -> Result<String, ExecError> {
        if self.offset + len > self.bytes.len() {
            return Err(ExecError::InvalidStorageValue {
                column: "<tsvector>".into(),
                details: "truncated string field in tsvector payload".into(),
            });
        }
        let text =
            std::str::from_utf8(&self.bytes[self.offset..self.offset + len]).map_err(|_| {
                ExecError::InvalidStorageValue {
                    column: "<tsvector>".into(),
                    details: "invalid UTF-8 in tsvector payload".into(),
                }
            })?;
        self.offset += len;
        Ok(text.to_string())
    }

    pub fn ensure_finished(&self) -> Result<(), ExecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ExecError::InvalidStorageValue {
                column: "<tsvector>".into(),
                details: "trailing bytes in tsvector payload".into(),
            })
        }
    }
}
