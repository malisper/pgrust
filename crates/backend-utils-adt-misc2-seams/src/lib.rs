//! Seam declarations for the `backend-utils-adt-misc2` unit (its
//! `expandeddatum.c` expanded-TOAST-object surface).
//!
//! Callers (e.g. `backend-access-common-heaptuple`'s `heap_compute_data_size` /
//! `fill_val`) reach the expanded-object subsystem through these slots. The
//! owning unit installs them from its `init_seams()` when it lands; until then
//! a call panics loudly.

seam_core::seam!(
    /// `EOH_get_flat_size(DatumGetEOHP(datum))` (utils/adt/expandeddatum.c):
    /// the number of bytes the expanded object would occupy once flattened.
    ///
    /// `eoh_bytes` is the verbatim datum bytes of the expanded external pointer
    /// (`VARATT_IS_EXTERNAL_EXPANDED`). `Err` carries the expanded-object
    /// method's `ereport(ERROR)`s (e.g. the expanded-array `get_flat_size`
    /// raises `array size exceeds the maximum allowed`).
    pub fn eoh_get_flat_size(eoh_bytes: &[u8]) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `EOH_flatten_into(DatumGetEOHP(datum), data, data_length)`
    /// (utils/adt/expandeddatum.c): flatten the expanded object into `dest`
    /// (which is exactly `EOH_get_flat_size` bytes long). `Err` carries the
    /// expanded-object method's `ereport(ERROR)`s.
    pub fn eoh_flatten_into(eoh_bytes: &[u8], dest: &mut [u8]) -> types_error::PgResult<()>
);
