//! Sidecar companion-file reader glue (M4-M's declared consumption of the
//! M3-A reserved slot vocabulary — the read half of the spec §16 envelope).
//!
//! Consult discipline for DERIVED sidecars, verbatim from the vocabulary:
//! absence is a first-class answer (older parts never wrote one — Absent,
//! not an error); ANY validation mismatch — magic, version, crc, kind,
//! part identity, key fingerprint — is STALE (refuse-and-rebuild), never a
//! query error; only real I/O faults surface as errors. The three verdicts
//! mirror `SidecarWitness` (Hit/Stale here; Built is the writer's).
//!
//! This glue returns the raw payload bytes: payload framing belongs to the
//! payload's owner (the memo payload is the lx-side vocabulary — this
//! crate stays payload-agnostic, exactly like the envelope).

use pgrc2_format::dirlayout;
use pgrc2_format::sidecar::{SidecarFileHeader, SidecarKind};

use crate::io::TableDirIo;
use crate::ReadResult;

/// One sidecar consult verdict.
#[derive(Debug)]
pub enum SidecarConsult {
    /// No companion file (older part, never built, or reclaimed) — the
    /// caller builds (and may persist) fresh.
    Absent,
    /// A file exists but refused validation — refuse-and-rebuild. The
    /// reason names the first failing check (witness vocabulary; the file
    /// is treated exactly like Absent for consumption).
    Stale(&'static str),
    /// A validated envelope: header facts plus the raw payload bytes.
    Hit {
        header: SidecarFileHeader,
        payload: Vec<u8>,
    },
}

/// Read + validate one sidecar companion file by its canonical §12 name.
///
/// `expect_uuid` binds the derived-kind identity law: the caller passes the
/// part's CURRENT physical identity, and an image built against any other
/// identity is `Stale("part identity")` — the structural staleness of a
/// rewritten/copied part. `expect_fingerprint` optionally binds the memo
/// key's 64-bit envelope fingerprint the same way.
pub fn read_sidecar(
    dir: &dyn TableDirIo,
    part_no: u32,
    kind: SidecarKind,
    gen: u64,
    expect_uuid: Option<[u8; 16]>,
    expect_fingerprint: Option<u64>,
) -> ReadResult<SidecarConsult> {
    let name = dirlayout::sidecar_file_name(part_no, kind, gen);
    let Some(bytes) = dir.read_file(&name)? else {
        return Ok(SidecarConsult::Absent);
    };
    let (header, payload) = match SidecarFileHeader::validate_file(&bytes) {
        Ok(v) => v,
        Err(_) => return Ok(SidecarConsult::Stale("envelope validation")),
    };
    if header.kind != kind.as_u16() {
        return Ok(SidecarConsult::Stale("kind mismatch"));
    }
    if let Some(uuid) = expect_uuid {
        if header.part_uuid != uuid {
            return Ok(SidecarConsult::Stale("part identity"));
        }
    }
    if let Some(fp) = expect_fingerprint {
        if header.key_fingerprint != fp {
            return Ok(SidecarConsult::Stale("key fingerprint"));
        }
    }
    Ok(SidecarConsult::Hit {
        header,
        payload: payload.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemTableDir;
    use pgrc2_format::sidecar::{SidecarFileHeader, SIDECAR_MAGIC};
    use pgrc2_format::wire::crc32c;
    use pgrc2_format::FORMAT_VERSION;

    const UUID: [u8; 16] = *b"uuid-of-the-part";

    /// A local mirror of the writer glue's pure image builder (the writer
    /// crate is not a dependency of the reader — the frozen format crate is
    /// the shared vocabulary, so the round-trip pin here builds the image
    /// from `pgrc2_format` primitives directly).
    fn image(kind: SidecarKind, uuid: [u8; 16], fp: u64, payload: &[u8]) -> Vec<u8> {
        let h = SidecarFileHeader {
            part_uuid: uuid,
            key_fingerprint: fp,
            payload_len: payload.len() as u64,
            magic: SIDECAR_MAGIC,
            kind: kind.as_u16(),
            flags: 0,
            version: FORMAT_VERSION,
            pad: 0,
        };
        let mut out = Vec::new();
        h.encode_into(&mut out);
        out.extend_from_slice(payload);
        let crc = crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn absent_is_a_first_class_answer() {
        let dir = MemTableDir::new();
        let v = read_sidecar(&dir, 1, SidecarKind::Memo, 1, None, None).unwrap();
        assert!(matches!(v, SidecarConsult::Absent));
    }

    #[test]
    fn hit_returns_header_facts_and_payload() {
        let mut dir = MemTableDir::new();
        dir.put(
            "part-5-memo-2.pgrc2s",
            image(SidecarKind::Memo, UUID, 77, b"the payload"),
        );
        let v = read_sidecar(&dir, 5, SidecarKind::Memo, 2, Some(UUID), Some(77)).unwrap();
        match v {
            SidecarConsult::Hit { header, payload } => {
                assert_eq!(header.part_uuid, UUID);
                assert_eq!(header.key_fingerprint, 77);
                assert_eq!(payload, b"the payload");
            }
            other => panic!("expected Hit, got {other:?}"),
        }
    }

    #[test]
    fn stale_teeth_identity_fingerprint_and_corruption() {
        let mut dir = MemTableDir::new();
        let good = image(SidecarKind::Memo, UUID, 77, b"p");
        // Part identity mismatch — the Law A structural staleness of a
        // rewritten part (new physical identity).
        dir.put("part-1-memo-1.pgrc2s", good.clone());
        let other_uuid = *b"a-DIFFERENT-part";
        let v = read_sidecar(&dir, 1, SidecarKind::Memo, 1, Some(other_uuid), None).unwrap();
        assert!(matches!(v, SidecarConsult::Stale("part identity")));
        // Fingerprint mismatch.
        let v = read_sidecar(&dir, 1, SidecarKind::Memo, 1, Some(UUID), Some(78)).unwrap();
        assert!(matches!(v, SidecarConsult::Stale("key fingerprint")));
        // Corruption (crc flip) — envelope validation.
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xff;
        dir.put("part-2-memo-1.pgrc2s", bad);
        let v = read_sidecar(&dir, 2, SidecarKind::Memo, 1, Some(UUID), Some(77)).unwrap();
        assert!(matches!(v, SidecarConsult::Stale("envelope validation")));
        // Kind mismatch: a trgm image at the memo name.
        dir.put(
            "part-3-memo-1.pgrc2s",
            image(SidecarKind::Trgm, UUID, 77, b"p"),
        );
        let v = read_sidecar(&dir, 3, SidecarKind::Memo, 1, None, None).unwrap();
        assert!(matches!(v, SidecarConsult::Stale("kind mismatch")));
    }
}
