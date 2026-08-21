//! The DictSpace ⇄ DictHandle seam (the M3-F seam v4's dicthandle module
//! doc names), rebuilt from v3 `lx_source/pgrc.rs:533-611` with the OD-9
//! delta: NO O(ndv) validation walk at publication.
//!
//! v3 licensed the infallible `DictSpace` faces by walking EVERY entry at
//! `open_validated` — which also faulted the entire payload, exactly the
//! whole-fault the SB-7 frame-lazy geometry exists to kill. v4's license
//! is layered instead (OD-9, AB-7.3):
//!
//! 1. the per-batch vectorized MAX-CODE guard bounds every published code
//!    (`GUARD_CODE_BOUND` demotes the batch on violation — checked gather,
//!    never an error);
//! 2. [`ScanDictSpace::prepare_frames`] faults exactly the frames the
//!    batch's codes address (typed errors surface HERE, before any
//!    infallible face is reachable);
//! 3. per-frame CRCs (SB-7) validate the faulted bytes.
//!
//! After 1+2, `entry_datum` on a published lane cannot fail: codes are
//! bounded and their frames resident.

use std::sync::Arc;

use datum::Datum;
use pgrc2_batch::{ColRep, DictEpoch, DictEpochKey, DictSpace};
use pgrc2_read::registry::PartPin;
use pgrc2_read::{DictHandle, OpenPart, ReadResult, SectionUnwrapper};

/// The scan-side dict provider: `pgrc2_batch::DictSpace` over the reader's
/// frame-lazy [`DictHandle`].
pub struct ScanDictSpace {
    handle: DictHandle,
    value_order: bool,
    epoch64: u64,
}

impl ScanDictSpace {
    /// Open the seam WITHOUT the v3 validation walk (OD-9) and WITHOUT a
    /// handle-held pin: pin lifetime is the CLAIM's (PC-2.4 — the worker's
    /// claim pin covers every dereference; part-advance drops this state).
    pub fn open(
        part: Arc<OpenPart>,
        unwrappers: &'static [&'static dyn SectionUnwrapper],
        attno: u32,
        path_ord: u32,
        value_order: bool,
    ) -> ReadResult<ScanDictSpace> {
        let handle = DictHandle::open(part, None, unwrappers, attno, path_ord)?;
        let epoch64 = handle.epoch64();
        Ok(ScanDictSpace {
            handle,
            value_order,
            epoch64,
        })
    }

    /// Fault the frames addressed by `codes` (frame-lazy: only touched
    /// frames commit). MUST run before publishing a lane over these codes
    /// — it is the fallible half of the infallible-face license.
    pub fn prepare_frames(&self, codes: &[u32]) -> ReadResult<()> {
        let mut last_frame = u32::MAX;
        for &c in codes {
            let f = self.handle.frame_of_code(c);
            if f != last_frame {
                self.handle.ensure_code(c)?;
                last_frame = f;
            }
        }
        Ok(())
    }

    pub fn epoch(&self) -> DictEpoch {
        DictEpoch(self.epoch64)
    }

    pub fn epoch_key(&self) -> DictEpochKey {
        self.handle.epoch_key()
    }

    pub fn handle(&self) -> &DictHandle {
        &self.handle
    }

    /// SB-7 residency witness passthrough (the census term).
    pub fn resident_payload_bytes(&self) -> u64 {
        self.handle.resident_payload_bytes()
    }

    /// Keep a claim-scoped pin alive for a lane published over this space
    /// (AB-4.3(a): the part-pin law covers the batch's life; the caller
    /// attaches the returned pin to its ClaimGuard).
    pub fn pin(&self) -> PartPin {
        PartPin::pin(self.handle.part())
    }
}

impl DictSpace for ScanDictSpace {
    fn ncodes(&self) -> u32 {
        self.handle.ncodes()
    }

    fn base_rep(&self) -> ColRep {
        // C6 dictionaries store detoasted plain images (spec §7/§7b), so
        // the inline proof holds unconditionally.
        ColRep::Varlena {
            inline_proven: true,
        }
    }

    fn entry_datum(&self, code: u32) -> Datum {
        let e = self
            .handle
            .entry(code)
            .expect("licensed at publish: max-code guard + prepare_frames");
        Datum::from_u64(e.image.as_ptr() as u64)
    }

    fn byte_len(&self, code: u32) -> u32 {
        // Index-only under every char-len form (the hot StrView-flip
        // caller must never trigger the Option-C load-time char walk).
        self.handle
            .byte_len_only(code)
            .expect("licensed at publish: max-code guard + resident index")
    }

    fn char_len(&self, code: u32) -> u32 {
        self.handle
            .lengths(code)
            .expect("licensed at publish: max-code guard + resident index")
            .1
    }

    fn code_order_is_value_order(&self) -> bool {
        self.value_order && self.handle.byte_rank_sorted()
    }
}
