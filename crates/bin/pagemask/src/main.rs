//! pagemask — apply a resource manager's `rm_mask` to every page of a
//! relation file, so a pgrust-recovered relfile and a C-recovered relfile can
//! be compared byte-for-byte modulo the fields recovery legitimately rewrites
//! (page LSN, checksum, hint bits, unused space). This is the physical-plane
//! comparator of the pgrust-vs-C recovered-state differential (recovery D3):
//! run it on BOTH sides with the SAME rmgr, then `cmp` the outputs.
//!
//! The mask functions come from the real `RmgrTable` dispatch (rmgr::GetRmgr),
//! so this tool exercises exactly the callbacks that `wal_consistency_checking`
//! invokes in the server.
//!
//! Usage:
//!   pagemask <rmgr> <in-relfile> [out-file]
//! where <rmgr> is one of: heap heap2 btree hash gin gist seq brin spgist
//! generic. With no out-file, masked pages are written to stdout.
//!
//! All-zero and never-initialized (PageIsNew, pd_upper == 0) pages are emitted
//! verbatim: recovery never masks them and both engines leave them identical.

use std::io::{self, Read, Write};

use rmgr::{GetRmgr, RmgrData};
use types_core::{BlockNumber, RmgrIds, BLCKSZ};

// PageHeaderData field offsets (see types_storage::bufpage): pd_upper is the
// upper bound of the free area; a page with pd_upper == 0 is PageIsNew.
const PD_UPPER_OFF: usize = 14;

fn rmid_from_name(name: &str) -> Option<RmgrIds> {
    Some(match name {
        "heap" => RmgrIds::RM_HEAP_ID,
        "heap2" => RmgrIds::RM_HEAP2_ID,
        "btree" | "nbtree" => RmgrIds::RM_BTREE_ID,
        "hash" => RmgrIds::RM_HASH_ID,
        "gin" => RmgrIds::RM_GIN_ID,
        "gist" => RmgrIds::RM_GIST_ID,
        "seq" | "sequence" => RmgrIds::RM_SEQ_ID,
        "spgist" | "spg" => RmgrIds::RM_SPGIST_ID,
        "brin" => RmgrIds::RM_BRIN_ID,
        "generic" => RmgrIds::RM_GENERIC_ID,
        _ => return None,
    })
}

fn page_is_new(page: &[u8]) -> bool {
    let pd_upper = u16::from_ne_bytes([page[PD_UPPER_OFF], page[PD_UPPER_OFF + 1]]);
    pd_upper == 0
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 || args.len() > 4 {
        eprintln!("usage: pagemask <rmgr> <in-relfile> [out-file]");
        eprintln!("  rmgr: heap heap2 btree hash gin gist seq brin spgist generic");
        std::process::exit(2);
    }
    let rmid = match rmid_from_name(&args[1]) {
        Some(r) => r,
        None => {
            eprintln!("pagemask: unknown rmgr '{}'", args[1]);
            std::process::exit(2);
        }
    };
    let rmgr: &RmgrData = GetRmgr(rmid as u8 as _).expect("builtin rmgr");
    let mask = match rmgr.rm_mask {
        Some(m) => m,
        None => {
            eprintln!("pagemask: rmgr '{}' has no rm_mask (not maskable)", args[1]);
            std::process::exit(2);
        }
    };

    let mut data = Vec::new();
    std::fs::File::open(&args[2])
        .unwrap_or_else(|e| {
            eprintln!("pagemask: open {}: {e}", args[2]);
            std::process::exit(1);
        })
        .read_to_end(&mut data)
        .expect("read relfile");

    if data.len() % BLCKSZ != 0 {
        eprintln!(
            "pagemask: {} length {} is not a multiple of BLCKSZ {}",
            args[2],
            data.len(),
            BLCKSZ
        );
        std::process::exit(1);
    }

    let nblocks = data.len() / BLCKSZ;
    for blk in 0..nblocks {
        let page = &mut data[blk * BLCKSZ..(blk + 1) * BLCKSZ];
        // Skip pages recovery never touches / masks: all-zero and PageIsNew.
        if page.iter().all(|&b| b == 0) || page_is_new(page) {
            continue;
        }
        mask(page, blk as BlockNumber).unwrap_or_else(|e| {
            eprintln!("pagemask: rm_mask failed on block {blk}: {e:?}");
            std::process::exit(1);
        });
    }

    let out: Box<dyn Write> = if args.len() == 4 {
        Box::new(std::fs::File::create(&args[3]).expect("create out-file"))
    } else {
        Box::new(io::stdout().lock())
    };
    let mut out = io::BufWriter::new(out);
    out.write_all(&data).expect("write masked pages");
    out.flush().expect("flush");
}
