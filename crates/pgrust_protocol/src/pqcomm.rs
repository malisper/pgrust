use std::collections::HashMap;
use std::io::{self, Read, Write};

pub const SSL_REQUEST_CODE: i32 = 80877103;
pub const PROTOCOL_VERSION_3_0: i32 = 196608;

pub fn read_byte(r: &mut impl Read) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub fn read_i16_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i16> {
    let end = *offset + 2;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i16 field"))?;
    *offset = end;
    Ok(i16::from_be_bytes(slice.try_into().unwrap()))
}

pub fn read_i32_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i32> {
    let end = *offset + 4;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i32 field"))?;
    *offset = end;
    Ok(i32::from_be_bytes(slice.try_into().unwrap()))
}

pub fn read_cstr(bytes: &[u8], offset: &mut usize) -> io::Result<String> {
    let start = *offset;
    let rel_end = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unterminated cstring"))?;
    let end = start + rel_end;
    *offset = end + 1;
    Ok(String::from_utf8_lossy(&bytes[start..end]).into_owned())
}

pub fn cstr_from_bytes(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

pub fn parse_startup_parameters(payload: &[u8]) -> io::Result<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut offset = 0usize;
    while offset < payload.len() {
        let key = read_cstr(payload, &mut offset)?;
        if key.is_empty() {
            break;
        }
        let value = read_cstr(payload, &mut offset)?;
        params.insert(key, value);
    }
    Ok(params)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    SslRequest,
    StartupParameters(HashMap<String, String>),
    UnsupportedProtocol(i32),
}

pub fn parse_startup_packet(payload: &[u8]) -> io::Result<StartupPacket> {
    if payload.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "startup packet missing protocol code",
        ));
    }
    let code = i32::from_be_bytes(payload[0..4].try_into().unwrap());
    match code {
        SSL_REQUEST_CODE => Ok(StartupPacket::SslRequest),
        PROTOCOL_VERSION_3_0 => {
            parse_startup_parameters(&payload[4..]).map(StartupPacket::StartupParameters)
        }
        other => Ok(StartupPacket::UnsupportedProtocol(other)),
    }
}

pub fn read_startup_parameters(
    reader: &mut impl Read,
    writer: &mut impl Write,
) -> io::Result<Result<HashMap<String, String>, i32>> {
    loop {
        let len = read_i32(reader)? as usize;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "startup packet too short",
            ));
        }
        let mut payload = vec![0u8; len - 4];
        reader.read_exact(&mut payload)?;

        match parse_startup_packet(&payload)? {
            StartupPacket::SslRequest => {
                writer.write_all(b"N")?;
                writer.flush()?;
            }
            StartupPacket::StartupParameters(params) => return Ok(Ok(params)),
            StartupPacket::UnsupportedProtocol(code) => return Ok(Err(code)),
        }
    }
}
