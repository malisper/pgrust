//! WAL-insertion buffer-registration vocabulary (`access/xloginsert.h`): the
//! `REGBUF_*` flags the generic-WAL and AM redo-emitting ports pass to the
//! `xloginsert` owner's `XLogRegisterBuffer`.

/// `#define REGBUF_FORCE_IMAGE 0x01` — force a full-page image.
pub const REGBUF_FORCE_IMAGE: u8 = 0x01;
/// `#define REGBUF_NO_IMAGE 0x02` — don't take a full-page image.
pub const REGBUF_NO_IMAGE: u8 = 0x02;
/// `#define REGBUF_WILL_INIT (0x04 | 0x02)` — page will be re-initialized at
/// replay (implies `REGBUF_NO_IMAGE`).
pub const REGBUF_WILL_INIT: u8 = 0x04 | 0x02;
/// `#define REGBUF_STANDARD 0x08` — page follows "standard" page layout (has
/// `pd_lower`/`pd_upper`, so the hole can be compressed out of the image).
pub const REGBUF_STANDARD: u8 = 0x08;
/// `#define REGBUF_KEEP_DATA 0x10` — include data even if a full-page image
/// is taken.
pub const REGBUF_KEEP_DATA: u8 = 0x10;
/// `#define REGBUF_NO_CHANGE 0x20` — intentionally register a clean buffer.
pub const REGBUF_NO_CHANGE: u8 = 0x20;
