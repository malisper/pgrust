use std::io::{self, Read};

pub(crate) fn read_byte(r: &mut impl Read) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub(crate) fn read_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub(crate) fn read_i16_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i16> {
    let end = *offset + 2;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i16 field"))?;
    *offset = end;
    Ok(i16::from_be_bytes(slice.try_into().unwrap()))
}

pub(crate) fn read_i32_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i32> {
    let end = *offset + 4;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i32 field"))?;
    *offset = end;
    Ok(i32::from_be_bytes(slice.try_into().unwrap()))
}

pub(crate) fn read_cstr(bytes: &[u8], offset: &mut usize) -> io::Result<String> {
    let start = *offset;
    let rel_end = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unterminated cstring"))?;
    let end = start + rel_end;
    *offset = end + 1;
    Ok(String::from_utf8_lossy(&bytes[start..end]).into_owned())
}

pub(crate) fn cstr_from_bytes(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}
