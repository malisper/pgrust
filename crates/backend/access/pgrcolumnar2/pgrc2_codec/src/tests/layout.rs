//! Size pins where structs price engagement (§5 M3-C; the issue-#69
//! template the format crate uses). The wire structs themselves are pinned
//! in `pgrc2_format`; these pin THIS crate's dispatch-priced shapes and the
//! frame-header constants the kernels hard-code.

use crate::arraydual::{ArrayElemFacts, ArraySplit};
use crate::election::{Demotion, DictArm, Election, FsstArm};
use pgrc2_format::abi::{CodecVtable, KernelKey};

#[test]
fn dispatch_priced_struct_sizes() {
    // The ABI pins (echoed from the format crate — a drift here means the
    // frozen ABI moved under us and this lane must stop and report).
    assert_eq!(core::mem::size_of::<KernelKey>(), 4);
    assert_eq!(core::mem::size_of::<CodecVtable>(), 56);
    // Election vocabulary: passed/copied per stream at seal — keep word-
    // scale (these are witnesses, not staging buffers).
    assert_eq!(core::mem::size_of::<Election>(), 24);
    assert_eq!(core::mem::size_of::<Demotion>(), 1);
    assert_eq!(core::mem::size_of::<DictArm>(), 48);
    assert_eq!(core::mem::size_of::<FsstArm>(), 48);
    assert_eq!(core::mem::size_of::<ArrayElemFacts>(), 8);
    assert_eq!(
        core::mem::size_of::<ArraySplit>(),
        2 * core::mem::size_of::<Vec<u64>>()
    );
}

#[test]
fn frame_header_constants() {
    // BYTE_FOR frame ref (spec §6.10; SB-3 widened the width ladder, the
    // frame ref is unchanged).
    assert_eq!(crate::bytefor::FRAME_REF_LEN, 8);
    // Wrapper vocabulary: LZ4 assigned, Zstd frozen-but-refused.
    assert_eq!(pgrc2_format::enc::Wrapper::Lz4.as_u8(), 1);
    assert_eq!(pgrc2_format::enc::Wrapper::Zstd.as_u8(), 2);
    // The geometry this crate's stack buffers are sized by.
    assert_eq!(pgrc2_format::geom::FRAME_VALUES, 1024);
    assert_eq!(pgrc2_format::geom::GRANULE_ROWS, 8192);
    assert_eq!(alp::VECTOR_SIZE, 1024);
    assert_eq!(alp::granule::GRANULE_VECTORS, 8);
    assert_eq!(alp::granule32::GRANULE_VECTORS, 8);
    // FSST wire vocabulary (SB-4/OD-5): escape byte, code-space ceiling,
    // symbol-length bound — the constants the decode validation hard-codes.
    assert_eq!(crate::fsst::FSST_ESCAPE, 255);
    assert_eq!(crate::fsst::FSST_MAX_SYMBOLS, 255);
    assert_eq!(crate::fsst::FSST_MAX_SYMBOL_LEN, 8);
    // The ENC 12 activation pin: id 12 IS Fsst (never a retrofitted slot).
    assert_eq!(pgrc2_format::enc::EncodingId::Fsst.as_u16(), 12);
    // The slot-table arithmetic the election prices with.
    assert_eq!(crate::fsst::frame_payload_bytes(1024, 0), 1025 * 4);
    assert_eq!(crate::fsst::frame_payload_bytes(3, 100), 16 + 100);
}
