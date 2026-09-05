//! audit-18.6 b095 (a186-candidate-fp-smgr-md-ba612f982c71f6379d9b-1):
//! md.c:1815 _mdfd_getseg pads a short prior segment with a zero block from
//! palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, MCXT_ALLOC_ZERO) so the write is
//! legal on an O_DIRECT descriptor (debug_io_direct=data). The Rust port used
//! an unaligned stack array; pwritev on the O_DIRECT fd failed with EINVAL and
//! the discontiguous extension (hash index growth across a 1 GB boundary, WAL
//! replay into a high segment) raised a spurious "could not extend file".

mod common;

use types_core::primitive::{ForkNumber, INVALID_PROC_NUMBER};
use types_core::BLCKSZ;
use types_storage::bufpage::PG_IO_ALIGN_SIZE;
use types_storage::smgr::RELSEG_SIZE;
use types_storage::{RelFileLocator, RelFileLocatorBackend, IO_DIRECT_DATA};

// The caller-side buffers must be aligned too (C: PGIOAlignedBlock); only the
// md-internal pad block is under test.
#[repr(align(4096))]
struct Aligned([u8; BLCKSZ]);
const _: () = assert!(core::mem::align_of::<Aligned>() == PG_IO_ALIGN_SIZE);

#[test]
fn discontiguous_extend_pads_prior_segment_with_io_aligned_zero_block() {
    let dir = common::setup("direct_io_pad");
    fd::set_io_direct_flags(IO_DIRECT_DATA);
    let key = RelFileLocatorBackend {
        locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16386 },
        backend: INVALID_PROC_NUMBER,
    };
    let fork = ForkNumber::MAIN_FORKNUM;

    smgr::smgropen(key.locator, key.backend).unwrap();
    let block = Aligned([0x5Au8; BLCKSZ]);
    // Filesystems without O_DIRECT (tmpfs) refuse at open/first write: that
    // is the environment, not the port — report and stop rather than fake a
    // verdict either way.
    if let Err(e) = smgr::smgrcreate(key, fork, false) {
        println!("SKIP: O_DIRECT unsupported here ({})", e.message);
        let _ = std::fs::remove_dir_all(dir);
        return;
    }
    if let Err(e) = smgr::smgrextend(key, fork, 0, &block.0, false) {
        println!("SKIP: O_DIRECT aligned write refused here ({})", e.message);
        let _ = std::fs::remove_dir_all(dir);
        return;
    }
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), 1);

    // Block RELSEG_SIZE lives in segment 1; segment 0 (1 block) must first be
    // padded to RELSEG_SIZE with the zero block (sparse on disk). C: succeeds
    // under debug_io_direct=data.
    smgr::smgrextend(key, fork, RELSEG_SIZE, &block.0, false)
        .unwrap_or_else(|e| panic!("discontiguous extend under io_direct=data failed: {}", e.message));
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), RELSEG_SIZE + 1);

    fd::set_io_direct_flags(0);
    let _ = std::fs::remove_dir_all(dir);
}
