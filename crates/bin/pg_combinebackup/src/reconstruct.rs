//! C: src/bin/pg_combinebackup/reconstruct.c — reconstruct a full file from
//! an incremental file and a chain of prior backups.
//!
//! Incremental file format (native-endian, like C): magic 0xd3ae1f0d (u32),
//! num_blocks (u32), truncation_block_length (u32), then num_blocks sorted
//! relative block numbers (u32 each); the header is padded to a BLCKSZ
//! multiple iff num_blocks > 0; then the blocks, BLCKSZ each.

use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use manifest::{pg_checksum_type_name, PgChecksumContext, PgChecksumType};

use crate::copy_file::{copy_file, CopyMethod};
use crate::flog::{errno_message, log_debug, log_warning, pg_fatal};
use crate::load_manifest::ManifestData;
use crate::{BLCKSZ, RELSEG_SIZE};

/// C: INCREMENTAL_MAGIC (backup/basebackup_incremental.h).
pub const INCREMENTAL_MAGIC: u32 = 0xd3ae1f0d;

/// C: rfile.
struct Rfile {
    filename: PathBuf,
    file: File,
    header_length: usize,
    num_blocks: u32,
    relative_block_numbers: Vec<u32>,
    truncation_block_length: u32,
    num_blocks_read: u32,
    highest_offset_read: u64,
}

/// Allocate a `Vec` of `len` copies of `value`, reserving capacity fallibly so
/// that an oversized (attacker-influenced) request fails via the tool's fatal
/// path rather than aborting the process on allocation failure.
fn try_alloc_vec<T: Clone>(len: usize, value: T) -> Vec<T> {
    let mut v: Vec<T> = Vec::new();
    if v.try_reserve_exact(len).is_err() {
        pg_fatal!("out of memory");
    }
    v.resize(len, value);
    v
}

/// C: reconstruct_from_incremental_file. Returns the checksum payload to be
/// recorded in the output manifest (None when checksum_type is NONE), like
/// C's checksum_length/checksum_payload out-parameters.
#[allow(clippy::too_many_arguments)]
pub fn reconstruct_from_incremental_file(
    input_filename: &Path,
    output_filename: &Path,
    relative_path: &str,
    bare_file_name: &str,
    prior_backup_dirs: &[String],
    manifests: &[Option<ManifestData>],
    manifest_path: &[u8],
    mut checksum_type: PgChecksumType,
    copy_method: CopyMethod,
    debug: bool,
    dry_run: bool,
) -> Option<Vec<u8>> {
    let n_prior_backups = prior_backup_dirs.len();

    /* Sanity check the relative_path. */
    debug_assert!(relative_path.is_empty() || relative_path.ends_with('/'));

    /*
     * Every block must come either from the latest version of the file or
     * from one of the prior backups.
     */
    let mut source: Vec<Option<Rfile>> = Vec::with_capacity(1 + n_prior_backups);
    source.resize_with(1 + n_prior_backups, || None);

    /*
     * Use the information from the latest incremental file to figure out how
     * long the reconstructed file should be.
     */
    source[n_prior_backups] = Some(make_incremental_rfile(input_filename));
    let latest_idx = n_prior_backups;
    let block_length = find_reconstructed_block_length(source[latest_idx].as_ref().unwrap());
    let latest_truncation_block_length =
        source[latest_idx].as_ref().unwrap().truncation_block_length;

    /*
     * Bound the reconstructed length against a sane maximum. A relation
     * segment can hold at most RELSEG_SIZE blocks, so a larger value can only
     * come from a corrupt or malicious incremental file. Rejecting it here
     * caps the sourcemap/offsetmap allocations below (which are sized by
     * block_length) to a small, bounded amount and prevents multi-GiB
     * allocations driven by attacker-controlled block numbers.
     */
    if block_length > RELSEG_SIZE {
        pg_fatal!(
            "file \"{}\" has reconstructed block length {} in excess of segment size {}",
            input_filename.display(),
            block_length,
            RELSEG_SIZE
        );
    }

    /*
     * For each block in the output file: which source file, and at what
     * offset. sourcemap holds an index into `source`.
     */
    let mut sourcemap: Vec<Option<usize>> = try_alloc_vec(block_length as usize, None);
    let mut offsetmap: Vec<u64> = try_alloc_vec(block_length as usize, 0u64);
    let mut full_copy_possible = true;

    /*
     * Every block present in the newest incremental file is sourced from it.
     */
    {
        let latest = source[latest_idx].as_ref().unwrap();
        for i in 0..latest.num_blocks as usize {
            let b = latest.relative_block_numbers[i] as usize;
            /*
             * Real runtime bounds check (C uses Assert here, which is compiled
             * out in production). A block number at or beyond block_length
             * would index past sourcemap/offsetmap on a crafted file.
             */
            if b >= block_length as usize {
                pg_fatal!(
                    "file \"{}\" has out-of-range block number {}",
                    latest.filename.display(),
                    b
                );
            }
            sourcemap[b] = Some(latest_idx);
            offsetmap[b] = latest.header_length as u64 + (i as u64) * BLCKSZ as u64;
            full_copy_possible = false;
        }
    }

    let mut sidx = n_prior_backups;
    let mut copy_source_index: Option<usize> = None;

    loop {
        /*
         * Move to the next backup in the chain. If there are no more, then
         * we're done.
         */
        if sidx == 0 {
            break;
        }
        sidx -= 1;

        /*
         * Look for the full file in the previous backup. If not found, then
         * look for an incremental file instead.
         */
        let full_path = PathBuf::from(format!(
            "{}/{}{}",
            prior_backup_dirs[sidx], relative_path, bare_file_name
        ));
        let s = match make_rfile(&full_path, true) {
            Some(s) => s,
            None => {
                let inc_path = PathBuf::from(format!(
                    "{}/{}INCREMENTAL.{}",
                    prior_backup_dirs[sidx], relative_path, bare_file_name
                ));
                make_incremental_rfile(&inc_path)
            }
        };
        source[sidx] = Some(s);
        let s = source[sidx].as_ref().unwrap();

        /*
         * If header_length == 0, this is a full file; otherwise incremental.
         */
        if s.header_length == 0 {
            let sb_size = match s.file.metadata() {
                Ok(md) => md.len(),
                Err(e) => pg_fatal!(
                    "could not stat file \"{}\": {}",
                    s.filename.display(),
                    errno_message(&e)
                ),
            };

            /*
             * Since we found a full file, source all blocks from it that
             * exist in the file.
             */
            let blocklength = (sb_size / BLCKSZ as u64) as u32;
            for b in 0..latest_truncation_block_length {
                if sourcemap[b as usize].is_none() && b < blocklength {
                    sourcemap[b as usize] = Some(sidx);
                    offsetmap[b as usize] = b as u64 * BLCKSZ as u64;
                }
            }

            /*
             * If a full copy looks possible, check whether the resulting
             * file should be exactly as long as the source file is.
             */
            if full_copy_possible {
                let expected_length = latest_truncation_block_length as u64 * BLCKSZ as u64;
                if expected_length == sb_size {
                    copy_source_index = Some(sidx);
                }
            }

            /* We don't need to consider any further sources. */
            break;
        }

        /*
         * Another incremental file: source all blocks from it that we need
         * but don't yet have.
         */
        for i in 0..s.num_blocks as usize {
            let b = s.relative_block_numbers[i];
            if b < latest_truncation_block_length && sourcemap[b as usize].is_none() {
                sourcemap[b as usize] = Some(sidx);
                offsetmap[b as usize] = s.header_length as u64 + (i as u64) * BLCKSZ as u64;
                full_copy_possible = false;
            }
        }
    }

    /*
     * If a checksum of the required type already exists in the
     * backup_manifest for the relevant input directory, reuse it.
     */
    let mut reused_checksum: Option<Vec<u8>> = None;
    if let Some(ci) = copy_source_index {
        if checksum_type != PgChecksumType::None {
            if let Some(manifest) = manifests[ci].as_ref() {
                match manifest.files.get(manifest_path) {
                    None => {
                        /*
                         * The directory is out of sync with the
                         * backup_manifest, so emit a warning.
                         */
                        log_warning(&format!(
                            "manifest file \"{}/backup_manifest\" contains no entry for file \"{}\"",
                            prior_backup_dirs[ci],
                            String::from_utf8_lossy(manifest_path)
                        ));
                    }
                    Some(mfile) => {
                        if mfile.checksum_type == checksum_type {
                            reused_checksum =
                                Some(mfile.checksum_payload.clone().unwrap_or_default());
                            checksum_type = PgChecksumType::None;
                        }
                    }
                }
            }
        }
    }

    /* Prepare for checksum calculation, if required. */
    let mut checksum_ctx = PgChecksumContext::init(checksum_type);

    /*
     * Full-copy fast path, bottom-out error, or reconstruction.
     */
    if let Some(ci) = copy_source_index {
        let copy_src = source[ci].as_ref().unwrap().filename.clone();
        copy_file(&copy_src, output_filename, &mut checksum_ctx, copy_method, dry_run);
    } else if sidx == 0 && source[0].as_ref().unwrap().header_length != 0 {
        pg_fatal!(
            "full backup contains unexpected incremental file \"{}\"",
            source[0].as_ref().unwrap().filename.display()
        );
    } else {
        write_reconstructed_file(
            input_filename,
            output_filename,
            block_length,
            &mut source,
            &sourcemap,
            &offsetmap,
            &mut checksum_ctx,
            copy_method,
            debug,
            dry_run,
        );
        debug_reconstruction(&source, dry_run);
    }

    /* Save results of checksum calculation. */
    if checksum_type != PgChecksumType::None {
        let mut payload = [0u8; manifest::PG_CHECKSUM_MAX_LENGTH];
        let len = checksum_ctx.finalize(&mut payload);
        Some(payload[..len].to_vec())
    } else {
        reused_checksum
    }
}

/// C: debug_reconstruction.
fn debug_reconstruction(sources: &[Option<Rfile>], dry_run: bool) {
    for s in sources.iter().flatten() {
        /* If no data is needed from this file, we can ignore it. */
        if s.num_blocks_read == 0 {
            continue;
        }

        /* Debug logging. */
        if dry_run {
            log_debug(&format!(
                "would have read {} blocks from \"{}\"",
                s.num_blocks_read,
                s.filename.display()
            ));
        } else {
            log_debug(&format!(
                "read {} blocks from \"{}\"",
                s.num_blocks_read,
                s.filename.display()
            ));
        }

        /*
         * In dry-run mode we verify the file is long enough that the reads
         * would have succeeded.
         */
        if dry_run {
            let sb_size = match s.file.metadata() {
                Ok(md) => md.len(),
                Err(e) => pg_fatal!(
                    "could not stat file \"{}\": {}",
                    s.filename.display(),
                    errno_message(&e)
                ),
            };
            if sb_size < s.highest_offset_read {
                pg_fatal!(
                    "file \"{}\" is too short: expected {}, found {}",
                    s.filename.display(),
                    s.highest_offset_read,
                    sb_size
                );
            }
        }
    }
}

/// C: find_reconstructed_block_length.
fn find_reconstructed_block_length(s: &Rfile) -> u32 {
    let mut block_length = s.truncation_block_length;
    for i in 0..s.num_blocks as usize {
        let b = s.relative_block_numbers[i];
        if b >= block_length {
            /*
             * Use overflow-checked arithmetic: a crafted incremental file can
             * contain a block number of u32::MAX, and an unchecked `+ 1` would
             * wrap to 0 (release builds) or panic (debug builds), yielding a
             * bogus block_length that later drives out-of-bounds indexing.
             */
            block_length = b.checked_add(1).unwrap_or_else(|| {
                pg_fatal!(
                    "file \"{}\" has out-of-range block number {}",
                    s.filename.display(),
                    b
                )
            });
        }
    }
    block_length
}

/// C: make_incremental_rfile — read and validate the incremental header.
fn make_incremental_rfile(filename: &Path) -> Rfile {
    let mut rf = make_rfile(filename, false).expect("missing_ok=false never returns None");

    /* Read and validate magic number. */
    let magic = read_u32(&mut rf);
    if magic != INCREMENTAL_MAGIC {
        pg_fatal!(
            "file \"{}\" has bad incremental magic number (0x{:x}, expected 0x{:x})",
            filename.display(),
            magic,
            INCREMENTAL_MAGIC
        );
    }

    /* Read block count. */
    rf.num_blocks = read_u32(&mut rf);
    if rf.num_blocks > RELSEG_SIZE {
        pg_fatal!(
            "file \"{}\" has block count {} in excess of segment size {}",
            filename.display(),
            rf.num_blocks,
            RELSEG_SIZE
        );
    }

    /* Read truncation block length. */
    rf.truncation_block_length = read_u32(&mut rf);
    if rf.truncation_block_length > RELSEG_SIZE {
        pg_fatal!(
            "file \"{}\" has truncation block length {} in excess of segment size {}",
            filename.display(),
            rf.truncation_block_length,
            RELSEG_SIZE
        );
    }

    /* Read block numbers if there are any. */
    if rf.num_blocks > 0 {
        let n = rf.num_blocks as usize;
        let mut raw = vec![0u8; 4 * n];
        read_bytes(&mut rf, &mut raw);
        rf.relative_block_numbers = raw
            .chunks_exact(4)
            .map(|c| u32::from_ne_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
    }

    /* Remember length of header. */
    rf.header_length = 4 + 4 + 4 + 4 * rf.num_blocks as usize;

    /*
     * Round header length to a multiple of BLCKSZ, so that block contents
     * are properly aligned. Only when the file actually has data blocks.
     */
    if rf.num_blocks > 0 && rf.header_length % BLCKSZ != 0 {
        rf.header_length += BLCKSZ - (rf.header_length % BLCKSZ);
    }

    rf
}

/// C: make_rfile.
fn make_rfile(filename: &Path, missing_ok: bool) -> Option<Rfile> {
    match File::open(filename) {
        Ok(file) => Some(Rfile {
            filename: filename.to_path_buf(),
            file,
            header_length: 0,
            num_blocks: 0,
            relative_block_numbers: Vec::new(),
            truncation_block_length: 0,
            num_blocks_read: 0,
            highest_offset_read: 0,
        }),
        Err(e) if missing_ok && e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            filename.display(),
            errno_message(&e)
        ),
    }
}

/// C: read_bytes — a single read(2); short reads are fatal.
fn read_bytes(rf: &mut Rfile, buffer: &mut [u8]) {
    use std::io::Read;
    match rf.file.read(buffer) {
        Ok(rb) if rb == buffer.len() => {}
        Ok(rb) => pg_fatal!(
            "could not read file \"{}\": read {} of {}",
            rf.filename.display(),
            rb,
            buffer.len()
        ),
        Err(e) => pg_fatal!(
            "could not read file \"{}\": {}",
            rf.filename.display(),
            errno_message(&e)
        ),
    }
}

fn read_u32(rf: &mut Rfile) -> u32 {
    let mut b = [0u8; 4];
    read_bytes(rf, &mut b);
    u32::from_ne_bytes(b)
}

/// C: write_reconstructed_file.
#[allow(clippy::too_many_arguments)]
fn write_reconstructed_file(
    input_filename: &Path,
    output_filename: &Path,
    block_length: u32,
    source: &mut [Option<Rfile>],
    sourcemap: &[Option<usize>],
    offsetmap: &[u64],
    checksum_ctx: &mut PgChecksumContext,
    copy_method: CopyMethod,
    debug: bool,
    dry_run: bool,
) {
    /* Debugging output. */
    if debug {
        /* Basic information about the output file to be produced. */
        if dry_run {
            log_debug(&format!(
                "would reconstruct \"{}\" ({} blocks, checksum {})",
                output_filename.display(),
                block_length,
                pg_checksum_type_name(checksum_ctx.checksum_type())
            ));
        } else {
            log_debug(&format!(
                "reconstructing \"{}\" ({} blocks, checksum {})",
                output_filename.display(),
                block_length,
                pg_checksum_type_name(checksum_ctx.checksum_type())
            ));
        }

        /* Print out the plan for reconstructing this file. */
        let mut debug_buf = String::new();
        let mut start_of_range: u32 = 0;
        let mut current_block: u32 = 0;
        while current_block < block_length {
            let s = sourcemap[current_block as usize];

            /* Extend range, if possible. */
            if current_block + 1 < block_length && s == sourcemap[current_block as usize + 1] {
                current_block += 1;
                continue;
            }

            /* Add details about this range. */
            match s {
                None => {
                    if current_block == start_of_range {
                        debug_buf.push_str(&format!(" {current_block}:zero"));
                    } else {
                        debug_buf.push_str(&format!(" {start_of_range}-{current_block}:zero"));
                    }
                }
                Some(si) => {
                    let fname = source[si].as_ref().unwrap().filename.display().to_string();
                    if current_block == start_of_range {
                        debug_buf.push_str(&format!(
                            " {current_block}:{fname}@{}",
                            offsetmap[current_block as usize]
                        ));
                    } else {
                        debug_buf.push_str(&format!(
                            " {start_of_range}-{current_block}:{fname}@{}",
                            offsetmap[current_block as usize]
                        ));
                    }
                }
            }

            /* Begin new range. */
            current_block += 1;
            start_of_range = current_block;

            /* If the output is very long or we are done, dump it now. */
            if current_block == block_length || debug_buf.len() > 1024 {
                log_debug(&format!("reconstruction plan:{debug_buf}"));
                debug_buf.clear();
            }
        }
    }

    /* Open the output file, except in dry_run mode. */
    let mut wfd: Option<File> = None;
    if !dry_run {
        match crate::copy_file::open_excl_create(output_filename, true) {
            Ok(f) => wfd = Some(f),
            Err(e) => pg_fatal!(
                "could not open file \"{}\": {}",
                output_filename.display(),
                errno_message(&e)
            ),
        }
    }

    /* Read and write the blocks as required. */
    let mut zero_blocks: u32 = 0;
    let mut buffer = vec![0u8; BLCKSZ];
    for i in 0..block_length as usize {
        let si = sourcemap[i];

        /* Update accounting information. */
        match si {
            None => zero_blocks += 1,
            Some(sidx) => {
                let s = source[sidx].as_mut().unwrap();
                s.num_blocks_read += 1;
                s.highest_offset_read = s.highest_offset_read.max(offsetmap[i] + BLCKSZ as u64);
            }
        }

        /* Skip the rest of this in dry-run mode. */
        if dry_run {
            continue;
        }

        /* Read or zero-fill the block as appropriate. */
        let Some(sidx) = si else {
            /*
             * New block not mentioned in the WAL summary. Should have been
             * an uninitialized block, so just zero-fill it.
             */
            buffer.fill(0);
            write_block(wfd.as_mut().unwrap(), output_filename, &buffer, checksum_ctx);
            continue;
        };

        let s = source[sidx].as_ref().unwrap();
        if copy_method != CopyMethod::CopyFileRange {
            /*
             * Read the block from the correct source file, and then write it
             * out, possibly with a checksum update.
             */
            read_block(s, offsetmap[i], &mut buffer);
            write_block(wfd.as_mut().unwrap(), output_filename, &buffer, checksum_ctx);
        } else {
            /* use copy_file_range */
            copy_block_by_range(
                s,
                offsetmap[i],
                wfd.as_ref().unwrap(),
                input_filename,
                output_filename,
            );
            /*
             * When checksum calculation not needed, we're done, otherwise
             * read the block and pass it to the checksum calculation.
             */
            if checksum_ctx.checksum_type() == PgChecksumType::None {
                continue;
            }
            read_block(s, offsetmap[i], &mut buffer);
            checksum_ctx.update(&buffer);
        }
    }

    /* Debugging output. */
    if zero_blocks > 0 {
        if dry_run {
            log_debug(&format!("would have zero-filled {zero_blocks} blocks"));
        } else {
            log_debug(&format!("zero-filled {zero_blocks} blocks"));
        }
    }

    /* Close the output file. */
    if let Some(f) = wfd {
        if let Err(e) = crate::copy_file::close_file(f) {
            pg_fatal!(
                "could not close file \"{}\": {}",
                output_filename.display(),
                errno_message(&e)
            );
        }
    }
}

/// C: the copy_file_range branch of write_reconstructed_file's block loop.
#[cfg(target_os = "linux")]
fn copy_block_by_range(
    s: &Rfile,
    offset: u64,
    wfd: &File,
    input_filename: &Path,
    output_filename: &Path,
) {
    use std::os::fd::AsRawFd;
    let mut off: libc::off64_t = offset as libc::off64_t;
    let mut nwritten: usize = 0;
    /*
     * Retry until we've written all the bytes (the offset is updated by
     * copy_file_range, and so is the wfd file offset).
     */
    while BLCKSZ > nwritten {
        // SAFETY: fds open; off tracks the source offset across retries.
        let wb = unsafe {
            libc::copy_file_range(
                s.file.as_raw_fd(),
                &mut off,
                wfd.as_raw_fd(),
                std::ptr::null_mut(),
                BLCKSZ - nwritten,
                0,
            )
        };
        if wb < 0 {
            let e = std::io::Error::last_os_error();
            pg_fatal!(
                "error while copying file range from \"{}\" to \"{}\": {}",
                input_filename.display(),
                output_filename.display(),
                errno_message(&e)
            );
        }
        nwritten += wb as usize;
    }
}

#[cfg(not(target_os = "linux"))]
fn copy_block_by_range(
    _s: &Rfile,
    _offset: u64,
    _wfd: &File,
    _input_filename: &Path,
    _output_filename: &Path,
) {
    pg_fatal!("copy_file_range not supported on this platform");
}

/// C: write_block — one write(2), then checksum.
fn write_block(
    fd: &mut File,
    output_filename: &Path,
    buffer: &[u8],
    checksum_ctx: &mut PgChecksumContext,
) {
    match fd.write(buffer) {
        Ok(wb) if wb == buffer.len() => {}
        Ok(wb) => pg_fatal!(
            "could not write file \"{}\": wrote {} of {}",
            output_filename.display(),
            wb,
            buffer.len()
        ),
        Err(e) => pg_fatal!(
            "could not write file \"{}\": {}",
            output_filename.display(),
            errno_message(&e)
        ),
    }
    checksum_ctx.update(buffer);
}

/// C: read_block — one pread(2) of BLCKSZ.
fn read_block(s: &Rfile, off: u64, buffer: &mut [u8]) {
    match s.file.read_at(buffer, off) {
        Ok(rb) if rb == BLCKSZ => {}
        Ok(rb) => pg_fatal!(
            "could not read from file \"{}\", offset {}: read {} of {}",
            s.filename.display(),
            off,
            rb,
            BLCKSZ
        ),
        Err(e) => pg_fatal!(
            "could not read from file \"{}\": {}",
            s.filename.display(),
            errno_message(&e)
        ),
    }
}
