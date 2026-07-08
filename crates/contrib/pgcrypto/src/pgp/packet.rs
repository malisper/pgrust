//! New-format packet length encoding/decoding (RFC 4880 §4.2) shared by the
//! encrypt and decrypt pipelines.

/// `render_newlen` — append the new-format body-length octets.
pub fn render_newlen(dst: &mut Vec<u8>, len: usize) {
    if len <= 191 {
        dst.push(len as u8);
    } else if len <= 8383 {
        dst.push((((len - 192) >> 8) + 192) as u8);
        dst.push(((len - 192) & 255) as u8);
    } else {
        dst.push(255);
        dst.push((len >> 24) as u8);
        dst.push((len >> 16) as u8);
        dst.push((len >> 8) as u8);
        dst.push(len as u8);
    }
}

/// Write a new-format definite-length packet (`0xC0|tag` + newlen + body).
pub fn write_packet(dst: &mut Vec<u8>, tag: i32, body: &[u8]) {
    dst.push(0xC0 | (tag as u8));
    render_newlen(dst, body.len());
    dst.extend_from_slice(body);
}

/// A cursor reading new-format/old-format packets from a byte buffer.
pub struct PktReader<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

/// A parsed packet header.
pub struct PktHdr {
    pub tag: i32,
    /// Body length. `None` = old-format indeterminate length (rest of input).
    pub len: Option<usize>,
    /// `true` if the length was a new-format partial (streamed) length.
    pub partial: bool,
}

impl<'a> PktReader<'a> {
    pub fn new(data: &'a [u8]) -> PktReader<'a> {
        PktReader { data, pos: 0 }
    }

    pub fn eof(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn byte(&mut self) -> Result<u8, ()> {
        if self.pos >= self.data.len() {
            return Err(());
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read a packet header. Returns `Ok(None)` at clean EOF.
    pub fn read_hdr(&mut self) -> Result<Option<PktHdr>, ()> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let p = self.byte()?;
        if p & 0x80 == 0 {
            return Err(());
        }
        if p & 0x40 != 0 {
            // new format
            let tag = (p & 0x3f) as i32;
            let (len, partial) = self.parse_new_len()?;
            Ok(Some(PktHdr { tag, len, partial }))
        } else {
            // old format
            let lentype = p & 3;
            let tag = ((p >> 2) & 0x0f) as i32;
            if lentype == 3 {
                Ok(Some(PktHdr {
                    tag,
                    len: None,
                    partial: false,
                }))
            } else {
                let len = self.parse_old_len(lentype)?;
                Ok(Some(PktHdr {
                    tag,
                    len: Some(len),
                    partial: false,
                }))
            }
        }
    }

    fn parse_new_len(&mut self) -> Result<(Option<usize>, bool), ()> {
        let b = self.byte()?;
        if b <= 191 {
            Ok((Some(b as usize), false))
        } else if b <= 223 {
            let b2 = self.byte()?;
            Ok((Some((((b as usize) - 192) << 8) + 192 + b2 as usize), false))
        } else if b == 255 {
            let mut len = 0usize;
            for _ in 0..4 {
                len = (len << 8) | self.byte()? as usize;
            }
            Ok((Some(len), false))
        } else {
            // partial body length
            Ok((Some(1usize << (b & 0x1f)), true))
        }
    }

    fn parse_old_len(&mut self, lentype: u8) -> Result<usize, ()> {
        let extra = match lentype {
            0 => 0,
            1 => 1,
            2 => 3,
            _ => return Err(()),
        };
        let mut len = self.byte()? as usize;
        for _ in 0..extra {
            len = (len << 8) | self.byte()? as usize;
        }
        Ok(len)
    }

    /// Read the full body of a packet whose header was just read, collecting
    /// partial-length continuations. `hdr` came from `read_hdr`.
    pub fn read_body(&mut self, hdr: &PktHdr) -> Result<Vec<u8>, ()> {
        match hdr.len {
            None => {
                // indeterminate: rest of input
                let body = self.data[self.pos..].to_vec();
                self.pos = self.data.len();
                Ok(body)
            }
            Some(first_len) => {
                let mut body = Vec::new();
                let mut len = first_len;
                let mut partial = hdr.partial;
                loop {
                    if self.pos + len > self.data.len() {
                        return Err(());
                    }
                    body.extend_from_slice(&self.data[self.pos..self.pos + len]);
                    self.pos += len;
                    if !partial {
                        break;
                    }
                    let (nlen, npartial) = self.parse_new_len()?;
                    len = nlen.ok_or(())?;
                    partial = npartial;
                }
                Ok(body)
            }
        }
    }
}
