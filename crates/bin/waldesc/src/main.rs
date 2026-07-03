//! Minimal pg_waldump: renders every record in a pg_wal directory through the
//! landed xlogreader + the rmgr table's rm_identify/rm_desc, one line per
//! record in pg_waldump's exact format. Differential oracle harness:
//! scripts/waldesc-diff.sh diffs this against real pg_waldump 18.3.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::io::Write;

use stringinfo::StringInfo;
use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use xlogreader::{XLogReaderRoutine, XLogReaderState, XLogSegmentRoutine};
use xlogreader_seams::{XLogReaderState as ReaderView, XLOG_BLCKSZ};

struct WalDirSource {
    dir: String,
    segsize: u64,
    tli: TimeLineID,
    file: i32,
    open_segno: XLogSegNo,
}

impl WalDirSource {
    fn seg_file_name(&self, segno: XLogSegNo) -> String {
        let per_id = 0x1_0000_0000u64 / self.segsize;
        format!("{:08X}{:08X}{:08X}", self.tli, segno / per_id, segno % per_id)
    }

    fn close_file(&mut self) {
        if self.file >= 0 {
            // SAFETY: fd owned by this source.
            unsafe { libc::close(self.file) };
            self.file = -1;
        }
    }
}

impl XLogSegmentRoutine for WalDirSource {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _next_seg_no: XLogSegNo,
        _tli: &mut TimeLineID,
    ) -> PgResult<()> {
        unreachable!("waldesc opens files in page_read");
    }
    fn segment_close(&mut self, v: &mut ReaderView) {
        self.close_file();
        v.seg.ws_file = -1;
    }
}

impl XLogReaderRoutine for WalDirSource {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        _req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let segno = target_page_ptr / self.segsize;
        if self.file >= 0 && segno != self.open_segno {
            self.close_file();
        }
        if self.file < 0 {
            let path = format!("{}/{}", self.dir, self.seg_file_name(segno));
            let cpath = std::ffi::CString::new(path).unwrap();
            // SAFETY: NUL-terminated path, O_RDONLY.
            let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
            if fd < 0 {
                return Ok(xlogreader::XLREAD_FAIL);
            }
            self.file = fd;
            self.open_segno = segno;
        }
        let off = (target_page_ptr % self.segsize) as libc::off_t;
        // SAFETY: cur_page is the reader's XLOG_BLCKSZ buffer.
        let r = unsafe {
            libc::pread(self.file, cur_page.as_mut_ptr() as *mut libc::c_void, XLOG_BLCKSZ, off)
        };
        if r != XLOG_BLCKSZ as isize {
            return Ok(xlogreader::XLREAD_FAIL);
        }
        v.seg.ws_tli = self.tli;
        Ok(XLOG_BLCKSZ as i32)
    }
}

// pg_waldump/compat.c timestamptz_to_str: localtime + strftime, frontend
// flavor (the backend flavor prints ISO +TZ offsets instead).
fn frontend_timestamptz_to_str(t: i64, out: &mut [u8; rmgrdesc::MAXDATELEN + 1]) -> usize {
    const USECS_PER_SEC: i64 = 1_000_000;
    const PG_EPOCH_OFFSET: i64 = 946_684_800; // (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY
    let secs = t.div_euclid(USECS_PER_SEC) + PG_EPOCH_OFFSET;
    let time = secs as libc::time_t;
    // SAFETY: localtime_r with valid out-param; strftime with NUL-terminated fmt.
    unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&time, &mut tm).is_null() {
            return 0;
        }
        let mut ts = [0u8; 64];
        let mut zone = [0u8; 64];
        let n = libc::strftime(
            ts.as_mut_ptr() as *mut libc::c_char,
            ts.len(),
            c"%Y-%m-%d %H:%M:%S".as_ptr(),
            &tm,
        );
        let zn = libc::strftime(
            zone.as_mut_ptr() as *mut libc::c_char,
            zone.len(),
            c"%Z".as_ptr(),
            &tm,
        );
        // C: snprintf("%s.%06d %s", ts, (int)(t % USECS_PER_SEC), zone)
        let s = format!(
            "{}.{:06} {}",
            std::str::from_utf8(&ts[..n]).unwrap_or(""),
            t % USECS_PER_SEC,
            std::str::from_utf8(&zone[..zn]).unwrap_or("")
        );
        let b = s.as_bytes();
        let n = b.len().min(rmgrdesc::MAXDATELEN);
        out[..n].copy_from_slice(&b[..n]);
        n
    }
}

// XLogRecGetLen (xlogstats.c).
fn rec_lens(reader: &XLogReaderState<'_>) -> (u32, u32) {
    let mut fpi_len = 0u32;
    for block_id in 0..=reader.XLogRecMaxBlockId().max(-1) {
        if reader.XLogRecHasBlockImage(block_id as u8) {
            fpi_len += reader.v.block(block_id as u8).bimg_len as u32;
        }
    }
    (reader.XLogRecGetTotalLen() - fpi_len, fpi_len)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| {
        eprintln!("usage: waldesc <pg_wal_dir> [first_segment_file]");
        std::process::exit(2);
    });

    let first_seg = match args.next() {
        Some(s) => s,
        None => {
            let mut segs: Vec<String> = std::fs::read_dir(&dir)
                .expect("read pg_wal dir")
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.len() == 24 && n.chars().all(|c| c.is_ascii_hexdigit()))
                .collect();
            segs.sort();
            segs.into_iter().next().unwrap_or_else(|| {
                eprintln!("waldesc: no WAL segment files in {dir}");
                std::process::exit(2);
            })
        }
    };

    let segsize = std::fs::metadata(format!("{dir}/{first_seg}"))
        .expect("stat first segment")
        .len();
    let tli = u32::from_str_radix(&first_seg[..8], 16).expect("segment name tli");
    let log = u64::from_str_radix(&first_seg[8..16], 16).expect("segment name log");
    let seg = u64::from_str_radix(&first_seg[16..24], 16).expect("segment name seg");
    let per_id = 0x1_0000_0000u64 / segsize;
    let start_segno = log * per_id + seg;

    relpath::init_seams();
    rmgrdesc::install_timestamptz_to_str(frontend_timestamptz_to_str);

    let ctx = Box::leak(Box::new(mcx::MemoryContext::new("waldesc")));
    let mcx = ctx.mcx();

    let mut src = WalDirSource { dir: format!("{dir}"), segsize, tli, file: -1, open_segno: 0 };
    let mut reader = XLogReaderState::allocate(mcx, segsize as i32).expect("allocate reader");

    let start = reader
        .XLogFindNextRecord(&mut src, start_segno * segsize)
        .expect("find first record");
    reader.XLogBeginRead(start);

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    let mut buf = StringInfo::new_in(mcx).expect("stringinfo");

    loop {
        match reader.XLogReadRecord(&mut src) {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Some(msg) = reader.errormsg() {
                    let lsn = reader.v.ReadRecPtr;
                    eprintln!(
                        "waldesc: error in WAL record at {:X}/{:X}: {msg}",
                        (lsn >> 32) as u32,
                        lsn as u32
                    );
                }
                break;
            }
            Err(e) => {
                eprintln!("waldesc: read error: {e:?}");
                break;
            }
        }

        let rmid = reader.XLogRecGetRmid();
        let desc = rmgr::GetRmgr(rmid).expect("builtin rmgr");
        let (rec_len, _fpi) = rec_lens(&reader);
        let info = reader.XLogRecGetInfo();
        let lsn = reader.v.ReadRecPtr;
        let prev = reader.XLogRecGetPrev();

        let _ = write!(
            out,
            "rmgr: {:<11} len (rec/tot): {:>6}/{:>6}, tx: {:>10}, lsn: {:X}/{:08X}, prev {:X}/{:08X}, ",
            desc.rm_name,
            rec_len,
            reader.XLogRecGetTotalLen(),
            reader.XLogRecGetXid(),
            (lsn >> 32) as u32,
            lsn as u32,
            (prev >> 32) as u32,
            prev as u32,
        );

        match (desc.rm_identify)(info) {
            Some(id) => {
                let _ = write!(out, "desc: {id} ");
            }
            None => {
                let _ = write!(out, "desc: UNKNOWN ({:x}) ", info & 0xF0);
            }
        }

        buf.reset();
        if let Err(e) = (desc.rm_desc)(&mut buf, &reader.v) {
            eprintln!("waldesc: rm_desc failed at {:X}/{:X}: {e:?}", (lsn >> 32) as u32, lsn as u32);
        }
        let _ = out.write_all(buf.as_bytes());

        buf.reset();
        rmgrdesc::xlogdesc::XLogRecGetBlockRefInfo(&reader.v, true, false, &mut buf, None)
            .expect("block ref info");
        let _ = out.write_all(buf.as_bytes());
    }
    let _ = out.flush();
}
