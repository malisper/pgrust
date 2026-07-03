// copyfromparse.c, text format: raw_buf -> input_buf -> line_buf ->
// attribute_buf pipeline. CSV/binary and the frontend/callback sources are
// loud before this module is reached.

use core::ffi::CStr;

use datum::Datum;
use elog::ereport;
use mcx::{vec_append_bytes, Mcx};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT, ERROR};
use types_fmgr::{input_function_call_safe, FmgrInfo};

use crate::from::CopyFromState;

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

impl<'mcx, 's> CopyFromState<'mcx, 's> {
    // CopyGetData, COPY_FILE arm.
    fn copy_get_data(&mut self, at: usize, maxread: usize) -> PgResult<usize> {
        let dst = &mut self.raw_buf[at..at + maxread];
        let read = fd::with_allocated_stdio(self.copy_file, |f| {
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
            None => panic!("COPY FROM: AllocateFile index {} vanished", self.copy_file),
        };
        if bytesread == 0 {
            self.raw_reached_eof = true;
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

        let inbytes = self.copy_get_data(self.raw_buf_len, RAW_BUF_SIZE - self.raw_buf_len)?;
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

    /// `CopyReadLine` (text arm). Returns true on EOF.
    pub(crate) fn copy_read_line(&mut self) -> PgResult<bool> {
        self.line_buf.clear();
        let result = self.copy_read_line_text()?;
        if !result {
            let strip = match self.eol_type {
                EolType::Nl | EolType::Cr => 1,
                EolType::Crnl => 2,
                EolType::Unknown => unreachable!("EOL found with unknown type"),
            };
            let newlen = self.line_buf.len() - strip;
            self.line_buf.truncate(newlen);
        }
        Ok(result)
    }

    // CopyReadLineText, text (non-CSV) arm.
    fn copy_read_line_text(&mut self) -> PgResult<bool> {
        let mut input_buf_ptr = self.input_buf_index;
        let mut copy_buf_len = self.input_buf_len;
        let mut need_data = false;
        let mut hit_eof = false;
        let mut result = false;

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

            if c == b'\r' {
                if self.eol_type == EolType::Unknown || self.eol_type == EolType::Crnl {
                    need_refill_and_not_eof_continue!(input_buf_ptr, prev_raw_ptr);
                    let c2 = self.input_byte(input_buf_ptr);
                    if c2 == b'\n' {
                        input_buf_ptr += 1;
                        self.eol_type = EolType::Crnl;
                    } else {
                        if self.eol_type == EolType::Crnl {
                            return Err(literal_cr());
                        }
                        self.eol_type = EolType::Cr;
                    }
                } else if self.eol_type == EolType::Nl {
                    return Err(literal_cr());
                }
                break;
            }

            if c == b'\n' {
                if self.eol_type == EolType::Cr || self.eol_type == EolType::Crnl {
                    return Err(literal_nl());
                }
                self.eol_type = EolType::Nl;
                break;
            }

            if c == b'\\' {
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
        let out = &mut self.attribute_buf;
        out.clear();
        self.raw_fields.clear();
        // De-escaping shrinks and each field adds one NUL for its delimiter
        // (plus one for the last), so line len + 1 bounds the output.
        let cap = line.len() + 1;
        out.try_reserve(cap)
            .map_err(|_| PgError::error("out of memory"))?;
        let dst = out.as_mut_ptr();
        let mut outlen = 0usize;

        let mut cur = 0usize;
        let line_end = line.len();
        let mut fieldno = 0usize;
        loop {
            let mut found_delim = false;
            let mut saw_non_ascii = false;
            let start_ptr = cur;
            let field_start_out = outlen;
            let mut end_ptr;
            loop {
                end_ptr = cur;
                if cur >= line_end {
                    break;
                }
                let mut c = line[cur];
                cur += 1;
                if c == delimc {
                    found_delim = true;
                    break;
                }
                if c == b'\\' {
                    if cur >= line_end {
                        break;
                    }
                    c = line[cur];
                    cur += 1;
                    match c {
                        b'0'..=b'7' => {
                            let mut val = (c - b'0') as u32;
                            for _ in 0..2 {
                                if cur < line_end && (b'0'..=b'7').contains(&line[cur]) {
                                    val = (val << 3) + (line[cur] - b'0') as u32;
                                    cur += 1;
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
                            if cur < line_end && line[cur].is_ascii_hexdigit() {
                                let mut val = hex_val(line[cur]);
                                cur += 1;
                                if cur < line_end && line[cur].is_ascii_hexdigit() {
                                    val = (val << 4) + hex_val(line[cur]);
                                    cur += 1;
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
                // SAFETY: outlen < cap (bounded above); dst has cap reserved.
                unsafe { *dst.add(outlen) = c };
                outlen += 1;
            }

            let raw_field = &line[start_ptr..end_ptr];
            if raw_field == null_print {
                outlen = field_start_out;
                self.raw_fields.push(-1);
            } else {
                if saw_non_ascii {
                    // SAFETY: bytes written above; field_start_out..outlen live.
                    let fld = unsafe {
                        core::slice::from_raw_parts(dst.add(field_start_out), outlen - field_start_out)
                    };
                    mbutils::pg_verify_mbstr(mbutils::GetDatabaseEncoding(), fld, false)?;
                }
                self.raw_fields.push(field_start_out as i32);
                // SAFETY: as above; one NUL per field fits the cap bound.
                unsafe { *dst.add(outlen) = 0 };
                outlen += 1;
            }

            fieldno += 1;
            if !found_delim {
                break;
            }
        }
        // SAFETY: outlen <= cap bytes initialized above.
        unsafe { out.set_len(outlen) };
        Ok(fieldno)
    }

    /// `NextCopyFromRawFields` + `NextCopyFrom` text arm: fill values/nulls
    /// (arrays over all physical attrs). Returns false at EOF.
    pub(crate) fn next_copy_from(
        &mut self,
        row_mcx: Mcx<'mcx>,
        values: &mut [Datum],
        nulls: &mut [bool],
    ) -> PgResult<bool> {
        for v in values.iter_mut() {
            *v = Datum::null();
        }
        for n in nulls.iter_mut() {
            *n = true;
        }

        // C's COPY_HEADER_TRUE arm: consume and discard the header line.
        if self.cur_lineno == 0 && self.opts.header_line {
            self.cur_lineno += 1;
            if self.copy_read_line()? {
                return Ok(false);
            }
        }

        self.cur_lineno += 1;
        let done = self.copy_read_line()?;
        if done && self.line_buf.is_empty() {
            return Ok(false);
        }
        let fldct = self.copy_read_attributes_text()?;

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
            let off = self.raw_fields[i];
            let cstr: Option<&CStr> = if off < 0 {
                None
            } else {
                let bytes = &self.attribute_buf[off as usize..];
                // SAFETY: copy_read_attributes_text NUL-terminates each field.
                Some(unsafe { CStr::from_ptr(bytes.as_ptr() as *const core::ffi::c_char) })
            };
            if cstr.is_some() {
                nulls[m] = false;
            }
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
        Ok(true)
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
fn literal_cr() -> Box<PgError> {
    Box::new(
        PgError::error("literal carriage return found in data")
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_hint("Use \"\\r\" to represent carriage return."),
    )
}

#[cold]
#[inline(never)]
fn literal_nl() -> Box<PgError> {
    Box::new(
        PgError::error("literal newline found in data")
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_hint("Use \"\\n\" to represent newline."),
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

    use crate::from::CopyFromState;
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
                delim,
                null_print,
                header_line: false,
            },
            copy_file: -1,
            filename: "",
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
            attribute_buf: PgVec::new_in(mcx),
            raw_fields: PgVec::new_in(mcx),
            max_fields,
            eol_type: EolType::Unknown,
            cur_lineno: 0,
            file_encoding: 0,
            need_transcoding: false,
            conversion_proc: 0,
            convertcx: MemoryContext::new("copy bench"),
            attnumlist: PgVec::new_in(mcx),
            in_functions: PgVec::new_in(mcx),
            typioparams: PgVec::new_in(mcx),
            atttypmods: PgVec::new_in(mcx),
            attnames: PgVec::new_in(mcx),
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
