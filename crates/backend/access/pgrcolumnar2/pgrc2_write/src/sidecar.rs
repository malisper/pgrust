//! Sidecar companion-file writer glue (M4-M's declared consumption of the
//! M3-A reserved slot vocabulary — `lanev3-m4-chunks.md` §2 M4-M row).
//!
//! The FIRST writer of the spec §16 envelope: `[SidecarFileHeader 48 B]
//! [payload][crc32c u32]` under the atomic tmp+rename+fsync discipline,
//! named per spec §12 (`part-<n>-<tag>-<gen>.pgrc2s`). This file writes the
//! frozen vocabulary EXACTLY as `pgrc2_format::sidecar` pins it — no format
//! amendment, `FORMAT_VERSION` untouched (payload-internal versioning is
//! the payload owner's; the envelope's `version` field IS the frozen format
//! version, enforced by `SidecarFileHeader::decode`).
//!
//! Posture (stated residuals, the M4-M lane report carries them):
//! - Companion-file-by-name only: no footer `SidecarDir` slot records are
//!   emitted (sealed parts are immutable, so footer listing is a
//!   build-at-seal rung — `seal.rs` is untouched by this glue).
//! - No publish-ordering coupling: derived sidecars are rebuildable by
//!   definition (refuse-and-rebuild), so a sidecar lost to a crash is a
//!   rebuild, never corruption. `publish.rs` is untouched.
//! - `recover_and_clean` does not classify `.pgrc2s` names: orphaned
//!   sidecars of reclaimed parts persist until the M5 compaction/GC rung.
//!
//! Derived-kind discipline (spec §16): `part_uuid` binds the part's
//! physical identity at build time — a copied or rewritten part has a new
//! identity and the reader treats the old sidecar as STALE (rebuild).

use pgrc2_format::dirlayout;
use pgrc2_format::sidecar::{SidecarFileHeader, SidecarKind, SIDECAR_MAGIC};
use pgrc2_format::wire::crc32c;
use pgrc2_format::FORMAT_VERSION;

use crate::wvfs::WriteVfs;
use crate::WriteResult;

/// Build the full companion-file image (header ‖ payload ‖ crc32c LE over
/// header+payload). Pure; the write path and the tests share it.
pub fn sidecar_file_image(
    kind: SidecarKind,
    part_uuid: [u8; 16],
    key_fingerprint: u64,
    payload: &[u8],
) -> Vec<u8> {
    let h = SidecarFileHeader {
        part_uuid,
        key_fingerprint,
        payload_len: payload.len() as u64,
        magic: SIDECAR_MAGIC,
        kind: kind.as_u16(),
        flags: 0,
        version: FORMAT_VERSION,
        pad: 0,
    };
    let mut out = Vec::with_capacity(48 + payload.len() + 4);
    h.encode_into(&mut out);
    out.extend_from_slice(payload);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Write one sidecar companion file into `dir` under the spec §13.3-shaped
/// durability discipline: whole image into a writer temp name, fsync file,
/// rename to the canonical §12 name, fsync dir. Returns the final file
/// name (not the path). `(fxid, seq)` name the temp file exactly like the
/// seal path's scratch discipline — a crash leaves only a `tmp-` name the
/// existing cleanup scan already reclaims.
#[allow(clippy::too_many_arguments)]
pub fn write_sidecar_file(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    part_no: u32,
    kind: SidecarKind,
    gen: u64,
    part_uuid: [u8; 16],
    key_fingerprint: u64,
    payload: &[u8],
    fxid: u64,
    seq: u32,
) -> WriteResult<String> {
    let image = sidecar_file_image(kind, part_uuid, key_fingerprint, payload);
    let tmp_name = dirlayout::temp_file_name(fxid, seq);
    let final_name = dirlayout::sidecar_file_name(part_no, kind, gen);
    let tmp_path = format!("{dir}/{tmp_name}");
    let final_path = format!("{dir}/{final_name}");
    let fd = vfs.create_rw(&tmp_path)?;
    vfs.pwrite_at(&fd, 0, &image)?;
    vfs.fsync_file(&fd)?;
    vfs.close_file(fd)?;
    vfs.rename_path(&tmp_path, &final_path)?;
    vfs.fsync_dir(dir)?;
    Ok(final_name)
}

/// ST-1 (OD-2): write the published parts' Stats companions — an EXPLICIT
/// POST-PUBLISH act, deliberately OUTSIDE `publish_parts`' §13.3 machinery
/// (this module's own doctrine: no publish coupling; the five-step crash
/// story and its op-boundary sweeps stay byte-identical). Derived class:
/// a companion is regenerable from its part (the sketches ride
/// `SealedPart.stats_payload`, a seal byproduct), so crash residue here is
/// a reclaimable `tmp-` name or a stale companion the derived-kind
/// refuse-and-rebuild law already covers. `pairs` = (part_no, payload) in
/// publish order — the caller zips `PublishOutcome::part_nos` with the
/// payloads it captured before publish cleared the writer.
pub fn publish_stats_sidecars(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    schema_fingerprint: u64,
    pairs: &[(u32, &[u8])],
    fxid: u64,
) -> WriteResult<()> {
    for (i, (part_no, payload)) in pairs.iter().enumerate() {
        if payload.is_empty() {
            continue;
        }
        write_sidecar_file(
            vfs,
            dir,
            *part_no,
            SidecarKind::Stats,
            1,
            [0u8; 16],
            schema_fingerprint,
            payload,
            fxid,
            0x8000_0000u32 | i as u32,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wvfs::MemVfs;
    use pgrc2_format::sidecar::SidecarFileHeader;
    use pgrc2_format::FormatError;

    const UUID: [u8; 16] = *b"part-uuid-16-byt";

    #[test]
    fn image_round_trips_through_the_frozen_validator() {
        let payload = b"memo payload bytes".to_vec();
        let image = sidecar_file_image(SidecarKind::Memo, UUID, 0xfeed_beef, &payload);
        let (h, body) = SidecarFileHeader::validate_file(&image).expect("valid image");
        assert_eq!(h.part_uuid, UUID);
        assert_eq!(h.key_fingerprint, 0xfeed_beef);
        assert_eq!(h.kind, SidecarKind::Memo.as_u16());
        assert_eq!(h.payload_len, payload.len() as u64);
        assert_eq!(body, &payload[..]);
    }

    #[test]
    fn write_is_atomic_named_and_durable_on_memvfs() {
        let mut vfs = MemVfs::default();
        vfs.mkdir_path("t").unwrap();
        let name = write_sidecar_file(
            &mut vfs,
            "t",
            3,
            SidecarKind::Memo,
            7,
            UUID,
            42,
            b"payload",
            99,
            0,
        )
        .expect("write ok");
        assert_eq!(name, "part-3-memo-7.pgrc2s");
        assert_eq!(
            dirlayout::parse_sidecar_file_name(&name),
            Some((3, SidecarKind::Memo, 7))
        );
        // The temp name is gone; only the canonical name remains.
        let names = vfs.list_dir("t").unwrap();
        assert_eq!(names, vec![name.clone()]);
        let bytes = vfs.read_full(&format!("t/{name}")).unwrap();
        let (h, body) = SidecarFileHeader::validate_file(&bytes).expect("durable image valid");
        assert_eq!(h.key_fingerprint, 42);
        assert_eq!(body, b"payload");
    }

    #[test]
    fn corrupt_teeth_every_field_refuses_typed() {
        let image = sidecar_file_image(SidecarKind::Memo, UUID, 1, b"xyz");
        // crc flip
        let mut c = image.clone();
        let n = c.len();
        c[n - 1] ^= 0xff;
        assert!(matches!(
            SidecarFileHeader::validate_file(&c),
            Err(FormatError::CrcMismatch { .. })
        ));
        // magic flip
        let mut m = image.clone();
        m[32] ^= 0xff; // magic starts after part_uuid(16)+fingerprint(8)+len(8)
        assert!(matches!(
            SidecarFileHeader::validate_file(&m),
            Err(FormatError::BadMagic { .. })
        ));
        // payload_len lie
        let mut l = image.clone();
        l[24] ^= 0x01;
        assert!(SidecarFileHeader::validate_file(&l).is_err());
        // truncation
        assert!(matches!(
            SidecarFileHeader::validate_file(&image[..20]),
            Err(FormatError::Truncated { .. })
        ));
        // payload byte flip lands in the crc
        let mut p = image.clone();
        p[48] ^= 0xff;
        assert!(matches!(
            SidecarFileHeader::validate_file(&p),
            Err(FormatError::CrcMismatch { .. })
        ));
    }
}
