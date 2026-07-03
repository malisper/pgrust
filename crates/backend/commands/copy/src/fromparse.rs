// copyfromparse.c, text + CSV formats: raw_buf -> input_buf -> line_buf ->
// attribute_buf pipeline, file or frontend source. Binary and the callback
// source are loud before this module is reached.

use core::ffi::CStr;

use datum::Datum;
use elog::ereport;
use mcx::{vec_append_bytes, Mcx};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_QUERY_CANCELED, ERROR,
};
use types_fmgr::{input_function_call_safe, FmgrInfo};

use crate::from::{CopyFromState, CopySrc};

pub const RAW_BUF_SIZE: usize = 65536;
pub const INPUT_BUF_SIZE: usize = 65536;
const MAX_CONVERSION_INPUT_LENGTH: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EolType {
    Unknown,
    Nl,
    Cr,
    Crnl,
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("copyfromparse.c", 0, funcname)
}

#[cold]
#[inline(never)]
fn bad_copy_format(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT))
}

const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fffffff - 1;

#[cold]
#[inline(never)]
fn unexpected_default_marker(attname: &str) -> Box<PgError> {
    Box::new(
        PgError::error("unexpected default marker in COPY data")
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_detail(format!("Column \"{attname}\" has no default value.")),
    )
}

#[cold]
#[inline(never)]
fn unexpected_eof() -> Box<PgError> {
    Box::new(
        PgError::error("unexpected EOF on client connection with an open transaction")
            .with_sqlstate(ERRCODE_CONNECTION_FAILURE),
    )
}

impl<'mcx, 's> CopyFromState<'mcx, 's> {
    // CopyGetData: >= minread and <= maxread bytes, less than minread only at
    // source EOF.
    fn copy_get_data(&mut self, at: usize, minread: usize, maxread: usize) -> PgResult<usize> {
        match &mut self.src {
            CopySrc::File { fd, .. } => {
                let fd = *fd;
                let dst = &mut self.raw_buf[at..at + maxread];
                let read = fd::with_allocated_stdio(fd, |f| {
                    use std::io::Read;
                    f.read(dst)
                });
                let bytesread = match read {
                    Some(Ok(n)) => n,
                    Some(Err(e)) => {
                        ereport(ERROR)
                            .with_saved_errno(e.raw_os_error().unwrap_or(0))
                            .errcode_for_file_access()
                            .errmsg("could not read from COPY file: %m")
                            .finish(loc("CopyGetData"))?;
                        unreachable!()
                    }
                    None => panic!("COPY FROM: AllocateFile index {fd} vanished"),
                };
                if bytesread == 0 {
                    self.raw_reached_eof = true;
                }
                Ok(bytesread)
            }
            CopySrc::Frontend { .. } => self.copy_get_data_frontend(at, minread, maxread),
        }
    }

    // CopyGetData, COPY_FRONTEND arm: drain CopyData messages, terminate on
    // CopyDone/CopyFail, ignore Flush/Sync per protocol.
    fn copy_get_data_frontend(
        &mut self,
        at: usize,
        minread: usize,
        mut maxread: usize,
    ) -> PgResult<usize> {
        let mut dst = at;
        let mut bytesread = 0usize;
        while maxread > 0 && bytesread < minread && !self.raw_reached_eof {
            loop {
                let msgbuf = match &self.src {
                    CopySrc::Frontend { msgbuf } => msgbuf,
                    CopySrc::File { .. } => unreachable!(),
                };
                if msgbuf.cursor < msgbuf.len() {
                    break;
                }
                struct CancelHoldoff;
                impl Drop for CancelHoldoff {
                    fn drop(&mut self) {
                        init_small::globals::ResumeCancelInterrupts();
                    }
                }
                init_small::globals::HoldCancelInterrupts();
                let _holdoff = CancelHoldoff;

                pqcomm::pq_startmsgread()?;
                let mtype = pqcomm::pq_getbyte()?;
                if mtype == pqcomm::EOF {
                    return Err(unexpected_eof());
                }
                let maxmsglen = match mtype as u8 {
                    b'd' => PQ_LARGE_MESSAGE_LIMIT,
                    b'c' | b'f' | b'H' | b'S' => PQ_SMALL_MESSAGE_LIMIT,
                    other => {
                        return Err(Box::new(
                            PgError::error(format!(
                                "unexpected message type 0x{other:02X} during COPY from stdin"
                            ))
                            .with_sqlstate(ERRCODE_PROTOCOL_VIOLATION),
                        ))
                    }
                };
                let msgbuf = match &mut self.src {
                    CopySrc::Frontend { msgbuf } => msgbuf,
                    CopySrc::File { .. } => unreachable!(),
                };
                if pqcomm::pq_getmessage(msgbuf, maxmsglen)? != 0 {
                    return Err(unexpected_eof());
                }
                match mtype as u8 {
                    b'd' => break,
                    b'c' => {
                        self.raw_reached_eof = true;
                        return Ok(bytesread);
                    }
                    b'f' => {
                        let body = msgbuf.as_bytes();
                        let nul = body.iter().position(|&b| b == 0).unwrap_or(body.len());
                        let msg = String::from_utf8_lossy(&body[..nul]);
                        return Err(Box::new(
                            PgError::error(format!("COPY from stdin failed: {msg}"))
                                .with_sqlstate(ERRCODE_QUERY_CANCELED),
                        ));
                    }
                    _ => continue, /* Flush/Sync: ignore */
                }
            }
            let msgbuf = match &mut self.src {
                CopySrc::Frontend { msgbuf } => msgbuf,
                CopySrc::File { .. } => unreachable!(),
            };
            let avail = (msgbuf.len() - msgbuf.cursor).min(maxread);
            let (from, cursor) = (msgbuf.as_bytes().as_ptr(), msgbuf.cursor);
            // SAFETY: msgbuf and raw_buf are disjoint allocations; avail is
            // within both slices' bounds.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    from.add(cursor),
                    self.raw_buf.as_mut_ptr().add(dst),
                    avail,
                );
            }
            msgbuf.cursor += avail;
            dst += avail;
            maxread -= avail;
            bytesread += avail;
        }
        Ok(bytesread)
    }

    // CopyLoadRawBuf.
    fn copy_load_raw_buf(&mut self) -> PgResult<()> {
        if !self.need_transcoding {
            debug_assert!(self.raw_buf_index == self.input_buf_index);
            debug_assert!(self.input_buf_len <= self.raw_buf_len);
        }
        let mut nbytes = self.raw_buf_len - self.raw_buf_index;
        if nbytes > 0 && self.raw_buf_index > 0 {
            self.raw_buf
                .copy_within(self.raw_buf_index..self.raw_buf_len, 0);
        }
        self.raw_buf_len -= self.raw_buf_index;
        self.raw_buf_index = 0;
        if !self.need_transcoding {
            self.input_buf_len -= self.input_buf_index;
            self.input_buf_index = 0;
        }

        let inbytes = self.copy_get_data(self.raw_buf_len, 1, RAW_BUF_SIZE - self.raw_buf_len)?;
        nbytes += inbytes;
        self.raw_buf[nbytes] = 0;
        self.raw_buf_len = nbytes;
        self.bytes_processed += inbytes as u64;
        Ok(())
    }

    // CopyConvertBuf.
    fn copy_convert_buf(&mut self) -> PgResult<()> {
        if !self.need_transcoding {
            let preverified = self.input_buf_len;
            let unverified = self.raw_buf_len - self.input_buf_len;
            if unverified == 0 {
                if self.raw_reached_eof {
                    self.input_reached_eof = true;
                }
                return Ok(());
            }
            let nverified = wchar::pg_encoding_verifymbstr(
                self.file_encoding,
                &self.raw_buf[preverified..self.raw_buf_len],
            );
            if nverified == 0 {
                if self.raw_reached_eof
                    || unverified >= wchar::pg_encoding_max_length(self.file_encoding) as usize
                {
                    self.input_reached_error = true;
                }
                return Ok(());
            }
            self.input_buf_len += nverified as usize;
            return Ok(());
        }

        if self.raw_buf_len - self.raw_buf_index == 0 {
            if self.raw_reached_eof {
                self.input_reached_eof = true;
            }
            return Ok(());
        }
        let input_buf = self.input_buf.as_mut().expect("transcoding input_buf");
        let nbytes = self.input_buf_len - self.input_buf_index;
        if nbytes > 0 && self.input_buf_index > 0 {
            input_buf.copy_within(self.input_buf_index..self.input_buf_len, 0);
        }
        self.input_buf_index = 0;
        self.input_buf_len = nbytes;
        input_buf[nbytes] = 0;

        let src = &self.raw_buf[self.raw_buf_index..self.raw_buf_len];
        let srclen = src.len();
        let dstlen = (INPUT_BUF_SIZE - self.input_buf_len + 1) as i32;
        self.convertcx.reset();
        let (consumed, out) = mbutils::pg_do_encoding_conversion_buf(
            self.convertcx.mcx(),
            self.conversion_proc,
            self.file_encoding,
            mbutils::GetDatabaseEncoding(),
            src,
            dstlen,
            true,
        )?;
        if consumed == 0 {
            if self.raw_reached_eof || srclen >= MAX_CONVERSION_INPUT_LENGTH {
                self.input_reached_error = true;
            }
            return Ok(());
        }
        let input_buf = self.input_buf.as_mut().expect("transcoding input_buf");
        input_buf[self.input_buf_len..self.input_buf_len + out.len()].copy_from_slice(&out);
        self.raw_buf_index += consumed as usize;
        self.input_buf_len += out.len();
        input_buf[self.input_buf_len] = 0;
        Ok(())
    }

    // CopyConversionError.
    #[cold]
    fn copy_conversion_error(&mut self) -> PgResult<()> {
        debug_assert!(self.input_reached_error);
        if !self.need_transcoding {
            mbutils::pg_verify_mbstr(
                self.file_encoding,
                &self.raw_buf[self.input_buf_len..self.raw_buf_len],
                false,
            )?;
            panic!("encoding verification failed without error");
        }
        let src = &self.raw_buf[self.raw_buf_index..self.raw_buf_len];
        self.convertcx.reset();
        mbutils::pg_do_encoding_conversion_buf(
            self.convertcx.mcx(),
            self.conversion_proc,
            self.file_encoding,
            mbutils::GetDatabaseEncoding(),
            src,
            (INPUT_BUF_SIZE + 1) as i32,
            false,
        )?;
        panic!("encoding conversion failed without error");
    }

    // CopyLoadInputBuf.
    fn copy_load_input_buf(&mut self) -> PgResult<()> {
        let nbytes = self.input_buf_len - self.input_buf_index;
        if !self.need_transcoding {
            debug_assert!(self.input_buf_index >= self.raw_buf_index);
            self.raw_buf_index = self.input_buf_index;
        }
        loop {
            self.copy_convert_buf()?;
            if self.input_buf_len - self.input_buf_index > nbytes {
                return Ok(());
            }
            if self.input_reached_error {
                self.copy_conversion_error()?;
            }
            if self.input_reached_eof {
                return Ok(());
            }
            debug_assert!(!self.raw_reached_eof);
            self.copy_load_raw_buf()?;
        }
    }

    #[inline]
    fn input_byte(&self, i: usize) -> u8 {
        match &self.input_buf {
            Some(b) => b[i],
            None => self.raw_buf[i],
        }
    }

    // REFILL_LINEBUF.
    fn refill_linebuf(&mut self, upto: usize) -> PgResult<()> {
        if upto > self.input_buf_index {
            let range = self.input_buf_index..upto;
            match &self.input_buf {
                Some(b) => vec_append_bytes(&mut self.line_buf, &b[range])?,
                None => vec_append_bytes(&mut self.line_buf, &self.raw_buf[range])?,
            }
            self.input_buf_index = upto;
        }
        Ok(())
    }

    /// `CopyReadLine`. Returns true on EOF.
    pub(crate) fn copy_read_line(&mut self, is_csv: bool) -> PgResult<bool> {
        self.line_buf.clear();
        self.line_buf_valid = false;
        let result = if is_csv {
            self.copy_read_line_text::<true>()?
        } else {
            self.copy_read_line_text::<false>()?
        };
        if result {
            // After \., protocol 3 ignores anything up to the end of the
            // CopyData stream.
            if matches!(self.src, CopySrc::Frontend { .. }) {
                while self.copy_get_data(0, 1, RAW_BUF_SIZE)? > 0 {}
                self.input_buf_index = 0;
                self.input_buf_len = 0;
                self.raw_buf_index = 0;
                self.raw_buf_len = 0;
            }
        } else {
            let strip = match self.eol_type {
                EolType::Nl | EolType::Cr => 1,
                EolType::Crnl => 2,
                EolType::Unknown => unreachable!("EOL found with unknown type"),
            };
            let newlen = self.line_buf.len() - strip;
            self.line_buf.truncate(newlen);
        }
        self.line_buf_valid = true;
        Ok(result)
    }

    // CopyReadLineText.
    fn copy_read_line_text<const IS_CSV: bool>(&mut self) -> PgResult<bool> {
        let mut input_buf_ptr = self.input_buf_index;
        let mut copy_buf_len = self.input_buf_len;
        let mut need_data = false;
        let mut hit_eof = false;
        let mut result = false;

        let mut in_quote = false;
        let mut last_was_esc = false;
        let quotec = self.opts.quote;
        // Escape==quote means no special escape processing (the common case).
        let escapec = if IS_CSV && self.opts.escape != quotec {
            self.opts.escape
        } else {
            0
        };

        macro_rules! need_refill_and_not_eof_continue {
            ($ptr:expr, $prev:expr) => {
                if $ptr >= copy_buf_len && !hit_eof {
                    input_buf_ptr = $prev;
                    need_data = true;
                    continue;
                }
            };
        }

        loop {
            if input_buf_ptr >= copy_buf_len || need_data {
                self.refill_linebuf(input_buf_ptr)?;
                self.copy_load_input_buf()?;
                hit_eof = self.input_reached_eof;
                input_buf_ptr = self.input_buf_index;
                copy_buf_len = self.input_buf_len;
                if self.input_buf_len - self.input_buf_index == 0 {
                    result = true;
                    break;
                }
                need_data = false;
            }

            let prev_raw_ptr = input_buf_ptr;
            // At EOF the guaranteed NUL pad makes lookahead reads yield 0.
            let c = self.input_byte(input_buf_ptr);
            input_buf_ptr += 1;

            if IS_CSV {
                // Lookahead below may refill; do it before the quote-state
                // update in case '\r' is also the quote or escape character.
                if c == b'\r' {
                    need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                }
                if in_quote && c == escapec {
                    last_was_esc = !last_was_esc;
                }
                if c == quotec && !last_was_esc {
                    in_quote = !in_quote;
                }
                if c != escapec {
                    last_was_esc = false;
                }
                if in_quote
                    && c == if self.eol_type == EolType::Nl { b'\n' } else { b'\r' }
                {
                    self.cur_lineno += 1;
                }
            }

            if c == b'\r' && (!IS_CSV || !in_quote) {
                if self.eol_type == EolType::Unknown || self.eol_type == EolType::Crnl {
                    need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                    let c2 = self.input_byte(input_buf_ptr);
                    if c2 == b'\n' {
                        input_buf_ptr += 1;
                        self.eol_type = EolType::Crnl;
                    } else {
                        if self.eol_type == EolType::Crnl {
                            return Err(literal_cr(IS_CSV));
                        }
                        self.eol_type = EolType::Cr;
                    }
                } else if self.eol_type == EolType::Nl {
                    return Err(literal_cr(IS_CSV));
                }
                break;
            }

            if c == b'\n' && (!IS_CSV || !in_quote) {
                if self.eol_type == EolType::Cr || self.eol_type == EolType::Crnl {
                    return Err(literal_nl(IS_CSV));
                }
                self.eol_type = EolType::Nl;
                break;
            }

            if c == b'\\' && !IS_CSV {
                need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                if input_buf_ptr >= copy_buf_len && hit_eof {
                    result = true;
                    break;
                }
                let c2 = self.input_byte(input_buf_ptr);
                if c2 == b'.' {
                    input_buf_ptr += 1;
                    if self.eol_type == EolType::Crnl {
                        need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                        let c2 = self.input_byte(input_buf_ptr);
                        input_buf_ptr += 1;
                        if c2 == b'\n' {
                            return Err(bad_copy_format(
                                "end-of-copy marker does not match previous newline style",
                            ));
                        } else if c2 != b'\r' {
                            return Err(marker_not_alone());
                        }
                    }
                    need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                    let c2 = self.input_byte(input_buf_ptr);
                    input_buf_ptr += 1;
                    if c2 != b'\r' && c2 != b'\n' {
                        return Err(marker_not_alone());
                    }
                    if (self.eol_type == EolType::Nl && c2 != b'\n')
                        || (self.eol_type == EolType::Crnl && c2 != b'\n')
                        || (self.eol_type == EolType::Cr && c2 != b'\r')
                    {
                        return Err(bad_copy_format(
                            "end-of-copy marker does not match previous newline style",
                        ));
                    }
                    if !self.line_buf.is_empty() || prev_raw_ptr > self.input_buf_index {
                        return Err(marker_not_alone());
                    }
                    self.input_buf_index = input_buf_ptr;
                    result = true;
                    break;
                } else {
                    // Non-CSV: the char after a backslash is data; skip it so
                    // \\. is not taken for an end-of-copy marker.
                    input_buf_ptr += 1;
                }
            }
        }

        self.refill_linebuf(input_buf_ptr)?;
        Ok(result)
    }

    /// `CopyReadAttributesText`: split line_buf into de-escaped fields in
    /// attribute_buf; raw_fields holds byte offsets, -1 for NULL.
    pub(crate) fn copy_read_attributes_text(&mut self) -> PgResult<usize> {
        let delimc = self.opts.delim;
        if self.max_fields == 0 {
            if !self.line_buf.is_empty() {
                return Err(extra_data());
            }
            return Ok(0);
        }

        let line: &[u8] = &self.line_buf;
        let null_print = self.opts.null_print.as_bytes();
        let default_print = self.opts.default_print.map(str::as_bytes);
        let out = &mut self.attribute_buf;
        out.clear();
        self.raw_fields.clear();
        // De-escaping shrinks and each field adds one NUL for its delimiter
        // (plus one for the last), so line len + 1 bounds the output.
        let cap = line.len() + 1;
        out.try_reserve(cap)
            .map_err(|_| PgError::error("out of memory"))?;
        let dst = out.as_mut_ptr();

        // SAFETY: raw cursors mirror C's pointer loop (post-increment
        // addressing; index forms cost 2 insns/byte on V2). `cur` advances
        // only behind `cur < line_end` guards; `op` writes ≤ cap bytes
        // (de-escaping shrinks, one NUL per field).
        unsafe {
            let line_end = line.as_ptr().add(line.len());
            let mut cur = line.as_ptr();
            let mut op = dst;
            let mut fieldno = 0usize;
            loop {
                let mut found_delim = false;
                let mut saw_non_ascii = false;
                let start_ptr = cur;
                let field_start = op;
                // end_ptr is set per exit (not carried per byte: the carried
                // copy cost 2 movs/byte).
                let end_ptr;
                loop {
                    if cur >= line_end {
                        end_ptr = cur;
                        break;
                    }
                    let mut c = *cur;
                    cur = cur.add(1);
                    if c == delimc {
                        end_ptr = cur.sub(1);
                        found_delim = true;
                        break;
                    }
                    if c == b'\\' {
                        if cur >= line_end {
                            end_ptr = cur.sub(1);
                            break;
                        }
                        c = *cur;
                        cur = cur.add(1);
                        match c {
                            b'0'..=b'7' => {
                                let mut val = (c - b'0') as u32;
                                for _ in 0..2 {
                                    if cur < line_end && (b'0'..=b'7').contains(&*cur) {
                                        val = (val << 3) + (*cur - b'0') as u32;
                                        cur = cur.add(1);
                                    } else {
                                        break;
                                    }
                                }
                                c = (val & 0o377) as u8;
                                if c == 0 || c >= 0x80 {
                                    saw_non_ascii = true;
                                }
                            }
                            b'x' => {
                                if cur < line_end && (*cur).is_ascii_hexdigit() {
                                    let mut val = hex_val(*cur);
                                    cur = cur.add(1);
                                    if cur < line_end && (*cur).is_ascii_hexdigit() {
                                        val = (val << 4) + hex_val(*cur);
                                        cur = cur.add(1);
                                    }
                                    c = (val & 0xff) as u8;
                                    if c == 0 || c >= 0x80 {
                                        saw_non_ascii = true;
                                    }
                                }
                            }
                            b'b' => c = 0x08,
                            b'f' => c = 0x0c,
                            b'n' => c = b'\n',
                            b'r' => c = b'\r',
                            b't' => c = b'\t',
                            b'v' => c = 0x0b,
                            _ => {}
                        }
                    }
                    op.write(c);
                    op = op.add(1);
                }

                let raw_field =
                    core::slice::from_raw_parts(start_ptr, end_ptr.offset_from(start_ptr) as usize);
                if raw_field == null_print {
                    op = field_start;
                    self.raw_fields.push(-1);
                } else if fieldno < self.attnumlist.len()
                    && default_print.is_some_and(|d| d == raw_field)
                {
                    let m = self.attnumlist[fieldno] as usize - 1;
                    if self.defexprs[m].is_none() {
                        return Err(unexpected_default_marker(&self.attname(m)));
                    }
                    self.defaults[m] = true;
                    self.raw_fields.push(field_start.offset_from(dst) as i32);
                    op.write(0);
                    op = op.add(1);
                } else {
                    if saw_non_ascii {
                        let fld = core::slice::from_raw_parts(
                            field_start,
                            op.offset_from(field_start) as usize,
                        );
                        mbutils::pg_verify_mbstr(mbutils::GetDatabaseEncoding(), fld, false)?;
                    }
                    self.raw_fields.push(field_start.offset_from(dst) as i32);
                    op.write(0);
                    op = op.add(1);
                }

                fieldno += 1;
                if !found_delim {
                    break;
                }
            }
            out.set_len(op.offset_from(dst) as usize);
            Ok(fieldno)
        }
    }

    /// `CopyReadAttributesCSV`: split line_buf into de-quoted fields in
    /// attribute_buf; raw_fields holds byte offsets, -1 for NULL.
    pub(crate) fn copy_read_attributes_csv(&mut self) -> PgResult<usize> {
        let delimc = self.opts.delim;
        let quotec = self.opts.quote;
        let escapec = self.opts.escape;
        if self.max_fields == 0 {
            if !self.line_buf.is_empty() {
                return Err(extra_data());
            }
            return Ok(0);
        }

        let line: &[u8] = &self.line_buf;
        let null_print = self.opts.null_print.as_bytes();
        let default_print = self.opts.default_print.map(str::as_bytes);
        let out = &mut self.attribute_buf;
        out.clear();
        self.raw_fields.clear();
        // De-quoting shrinks; one NUL per field bounds the output as in the
        // text arm.
        let cap = line.len() + 1;
        out.try_reserve(cap)
            .map_err(|_| PgError::error("out of memory"))?;

        let mut cur = 0usize;
        let mut fieldno = 0usize;
        loop {
            let mut found_delim = false;
            let mut saw_quote = false;
            let start = cur;
            let field_start = out.len();
            let end;
            'field: loop {
                loop {
                    if cur >= line.len() {
                        end = cur;
                        break 'field;
                    }
                    let c = line[cur];
                    cur += 1;
                    if c == delimc {
                        end = cur - 1;
                        found_delim = true;
                        break 'field;
                    }
                    if c == quotec {
                        saw_quote = true;
                        break;
                    }
                    out.push(c);
                }
                loop {
                    if cur >= line.len() {
                        return Err(bad_copy_format("unterminated CSV quoted field"));
                    }
                    let c = line[cur];
                    cur += 1;
                    if c == escapec
                        && cur < line.len()
                        && (line[cur] == escapec || line[cur] == quotec)
                    {
                        out.push(line[cur]);
                        cur += 1;
                        continue;
                    }
                    if c == quotec {
                        break;
                    }
                    out.push(c);
                }
            }

            if !saw_quote && &line[start..end] == null_print {
                out.truncate(field_start);
                self.raw_fields.push(-1);
            } else {
                if fieldno < self.attnumlist.len()
                    && default_print.is_some_and(|d| d == &line[start..end])
                {
                    let m = self.attnumlist[fieldno] as usize - 1;
                    if self.defexprs[m].is_none() {
                        return Err(unexpected_default_marker(&self.attname(m)));
                    }
                    self.defaults[m] = true;
                }
                self.raw_fields.push(field_start as i32);
                out.push(0);
            }

            fieldno += 1;
            if !found_delim {
                break;
            }
        }
        Ok(fieldno)
    }

    /// `NextCopyFromRawFields` + `NextCopyFrom`, text/CSV arms: fill
    /// values/nulls (arrays over all physical attrs). Returns false at EOF.
    pub(crate) fn next_copy_from(
        &mut self,
        row_mcx: Mcx<'mcx>,
        values: &mut [Datum],
        nulls: &mut [bool],
    ) -> PgResult<bool> {
        let is_csv = self.opts.csv_mode;
        for v in values.iter_mut() {
            *v = Datum::null();
        }
        for n in nulls.iter_mut() {
            *n = true;
        }
        for d in self.defaults.iter_mut() {
            *d = false;
        }

        // C's COPY_HEADER_TRUE arm: consume and discard the header line.
        if self.cur_lineno == 0 && self.opts.header_line {
            self.cur_lineno += 1;
            if self.copy_read_line(is_csv)? {
                return Ok(false);
            }
        }

        self.cur_lineno += 1;
        let done = self.copy_read_line(is_csv)?;
        if done && self.line_buf.is_empty() {
            return Ok(false);
        }
        let fldct = if is_csv {
            self.copy_read_attributes_csv()?
        } else {
            self.copy_read_attributes_text()?
        };

        let attr_count = self.attnumlist.len();
        if attr_count > 0 && fldct > attr_count {
            return Err(extra_data());
        }

        for i in 0..attr_count {
            let attnum = self.attnumlist[i];
            let m = attnum as usize - 1;
            if i >= fldct {
                return Err(missing_data(&self.attname(m)));
            }
            let mut off = self.raw_fields[i];
            if is_csv {
                if off < 0 && self.force_notnull_flags[m] {
                    // FORCE_NOT_NULL: convert NULL back to the null string.
                    off = self.null_print_field();
                } else if off >= 0
                    && self.force_null_flags[m]
                    && self.field_is_null_print(off)
                {
                    off = -1;
                }
            }
            self.cur_attidx = Some(m);
            self.cur_attval_off = (off >= 0).then_some(off);
            let cstr: Option<&CStr> = if off < 0 {
                None
            } else {
                let bytes = &self.attribute_buf[off as usize..];
                // SAFETY: the read_attributes arms NUL-terminate each field.
                Some(unsafe { CStr::from_ptr(bytes.as_ptr() as *const core::ffi::c_char) })
            };
            if cstr.is_some() {
                nulls[m] = false;
            } else {
                nulls[m] = true;
            }
            if self.defaults[m] {
                let state = self.defexprs[m].as_mut().expect("DEFAULT marker sans defexpr");
                let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
                let r = execexpr::exec_eval_expr(state, &mut slots)?;
                values[m] = r.value;
                nulls[m] = r.isnull;
            } else {
                let in_fn: &mut FmgrInfo = &mut self.in_functions[i];
                let ok = input_function_call_safe(
                    in_fn,
                    cstr,
                    self.typioparams[i],
                    self.atttypmods[i],
                    row_mcx,
                    None,
                    &mut values[m],
                )?;
                debug_assert!(ok);
            }
            self.cur_attidx = None;
            self.cur_attval_off = None;
        }
        for k in 0..self.defmap.len() {
            let m = self.defmap[k];
            let state = self.defexprs[m].as_mut().expect("defmap entry sans defexpr");
            let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(state, &mut slots)?;
            values[m] = r.value;
            nulls[m] = r.isnull;
        }
        Ok(true)
    }

    // FORCE_NOT_NULL rewrite target: null_print appended (NUL-terminated) at
    // the end of attribute_buf, once per row at most.
    fn null_print_field(&mut self) -> i32 {
        let off = self.attribute_buf.len() as i32;
        let null_print = self.opts.null_print.as_bytes();
        vec_append_bytes(&mut self.attribute_buf, null_print).expect("attribute_buf grow");
        self.attribute_buf.push(0);
        off
    }

    fn field_is_null_print(&self, off: i32) -> bool {
        let bytes = &self.attribute_buf[off as usize..];
        let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        &bytes[..nul] == self.opts.null_print.as_bytes()
    }
}

#[inline]
fn hex_val(c: u8) -> u32 {
    match c {
        b'0'..=b'9' => (c - b'0') as u32,
        b'a'..=b'f' => (c - b'a' + 10) as u32,
        _ => (c.to_ascii_lowercase() - b'a' + 10) as u32,
    }
}

#[cold]
#[inline(never)]
fn literal_cr(is_csv: bool) -> Box<PgError> {
    let (msg, hint) = if is_csv {
        (
            "unquoted carriage return found in data",
            "Use quoted CSV field to represent carriage return.",
        )
    } else {
        (
            "literal carriage return found in data",
            "Use \"\\r\" to represent carriage return.",
        )
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_hint(hint),
    )
}

#[cold]
#[inline(never)]
fn literal_nl(is_csv: bool) -> Box<PgError> {
    let (msg, hint) = if is_csv {
        (
            "unquoted newline found in data",
            "Use quoted CSV field to represent newline.",
        )
    } else {
        ("literal newline found in data", "Use \"\\n\" to represent newline.")
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_hint(hint),
    )
}

#[cold]
#[inline(never)]
fn marker_not_alone() -> Box<PgError> {
    bad_copy_format("end-of-copy marker is not alone on its line")
}

#[cold]
#[inline(never)]
fn extra_data() -> Box<PgError> {
    bad_copy_format("extra data after last expected column")
}

#[cold]
#[inline(never)]
fn missing_data(attname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("missing data for column \"{attname}\""))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
    )
}

#[doc(hidden)]
pub mod bench_internals {
    use mcx::{vec_append_bytes, Mcx, MemoryContext, PgVec};

    use crate::from::{CopyFromState, CopySrc};
    use crate::CopyFormatOptions;

    use super::EolType;

    pub fn readattrs_state<'mcx>(
        mcx: Mcx<'mcx>,
        delim: u8,
        null_print: &'static str,
        max_fields: usize,
    ) -> CopyFromState<'mcx, 'static> {
        CopyFromState {
            opts: CopyFormatOptions {
                file_encoding: -1,
                binary: false,
                csv_mode: false,
                freeze: false,
                delim,
                quote: b'"',
                escape: b'"',
                null_print,
                default_print: None,
                header_line: false,
                force_quote: None,
                force_quote_all: false,
                force_notnull: None,
                force_notnull_all: false,
                force_null: None,
                force_null_all: false,
                on_error: crate::CopyOnErrorChoice::Stop,
                log_verbosity: crate::CopyLogVerbosityChoice::Default,
                reject_limit: 0,
            },
            src: CopySrc::File { fd: -1, filename: "" },
            raw_buf: PgVec::new_in(mcx),
            raw_buf_index: 0,
            raw_buf_len: 0,
            raw_reached_eof: false,
            input_reached_eof: false,
            input_reached_error: false,
            input_buf: None,
            input_buf_index: 0,
            input_buf_len: 0,
            line_buf: PgVec::new_in(mcx),
            line_buf_valid: false,
            attribute_buf: PgVec::new_in(mcx),
            raw_fields: PgVec::new_in(mcx),
            max_fields,
            eol_type: EolType::Unknown,
            cur_lineno: 0,
            cur_attidx: None,
            cur_attval_off: None,
            file_encoding: 0,
            need_transcoding: false,
            conversion_proc: 0,
            convertcx: MemoryContext::new("copy bench"),
            attnumlist: PgVec::new_in(mcx),
            in_functions: PgVec::new_in(mcx),
            typioparams: PgVec::new_in(mcx),
            atttypmods: PgVec::new_in(mcx),
            attnames: PgVec::new_in(mcx),
            force_notnull_flags: PgVec::new_in(mcx),
            force_null_flags: PgVec::new_in(mcx),
            defexprs: PgVec::new_in(mcx),
            defmap: PgVec::new_in(mcx),
            defaults: mcx::vec_from_elem_in(mcx, false, max_fields),
            bytes_processed: 0,
        }
    }

    pub fn read_attributes_text(st: &mut CopyFromState<'_, '_>, line: &[u8]) -> usize {
        st.line_buf.clear();
        vec_append_bytes(&mut st.line_buf, line).unwrap();
        st.copy_read_attributes_text().unwrap()
    }

    pub fn raw_fields<'a>(st: &'a CopyFromState<'_, '_>) -> (&'a [i32], &'a [u8]) {
        (&st.raw_fields, &st.attribute_buf)
    }
}
