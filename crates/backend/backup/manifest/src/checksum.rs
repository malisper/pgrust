
use crc32c::{fin_crc32c, pg_comp_crc32c, CRC32C_INIT};
use pg_sha2::{
    PgSha256Ctx, PgSha512Ctx, PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH,
    PG_SHA384_DIGEST_LENGTH, PG_SHA512_DIGEST_LENGTH,
};

pub const PG_CHECKSUM_MAX_LENGTH: usize = PG_SHA512_DIGEST_LENGTH;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PgChecksumType {
    None,
    Crc32c,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

pub use PgChecksumType::{
    Crc32c as CHECKSUM_TYPE_CRC32C, None as CHECKSUM_TYPE_NONE, Sha224 as CHECKSUM_TYPE_SHA224,
    Sha256 as CHECKSUM_TYPE_SHA256, Sha384 as CHECKSUM_TYPE_SHA384, Sha512 as CHECKSUM_TYPE_SHA512,
};

pub fn pg_checksum_type_name(ty: PgChecksumType) -> &'static str {
    match ty {
        PgChecksumType::None => "NONE",
        PgChecksumType::Crc32c => "CRC32C",
        PgChecksumType::Sha224 => "SHA224",
        PgChecksumType::Sha256 => "SHA256",
        PgChecksumType::Sha384 => "SHA384",
        PgChecksumType::Sha512 => "SHA512",
    }
}

enum Raw {
    None,
    Crc(u32),
    Sha256(Option<PgSha256Ctx>),
    Sha512(Option<PgSha512Ctx>),
}

pub struct PgChecksumContext {
    ty: PgChecksumType,
    raw: Raw,
}

impl PgChecksumContext {
    pub fn init(ty: PgChecksumType) -> Self {
        let raw = match ty {
            PgChecksumType::None => Raw::None,
            PgChecksumType::Crc32c => Raw::Crc(CRC32C_INIT),
            PgChecksumType::Sha224 => Raw::Sha256(Some(PgSha256Ctx::init_sha224())),
            PgChecksumType::Sha256 => Raw::Sha256(Some(PgSha256Ctx::init_sha256())),
            PgChecksumType::Sha384 => Raw::Sha512(Some(PgSha512Ctx::init_sha384())),
            PgChecksumType::Sha512 => Raw::Sha512(Some(PgSha512Ctx::init_sha512())),
        };
        Self { ty, raw }
    }

    pub fn checksum_type(&self) -> PgChecksumType {
        self.ty
    }

    pub fn update(&mut self, data: &[u8]) {
        match &mut self.raw {
            Raw::None => {}
            Raw::Crc(c) => *c = pg_comp_crc32c(*c, data),
            Raw::Sha256(ctx) => ctx.as_mut().expect("finalized").update(data),
            Raw::Sha512(ctx) => ctx.as_mut().expect("finalized").update(data),
        }
    }

    pub fn finalize(&mut self, output: &mut [u8]) -> usize {
        match (&mut self.raw, self.ty) {
            (Raw::None, _) => 0,
            (Raw::Crc(c), _) => {
                // native (little-endian) order, per C's memcpy(&c_crc32c).
                let v = fin_crc32c(*c).to_le_bytes();
                output[..4].copy_from_slice(&v);
                4
            }
            (Raw::Sha256(ctx), PgChecksumType::Sha224) => {
                let d = ctx.take().expect("finalized").final_sha224();
                output[..PG_SHA224_DIGEST_LENGTH].copy_from_slice(&d);
                PG_SHA224_DIGEST_LENGTH
            }
            (Raw::Sha256(ctx), _) => {
                let d = ctx.take().expect("finalized").final_sha256();
                output[..PG_SHA256_DIGEST_LENGTH].copy_from_slice(&d);
                PG_SHA256_DIGEST_LENGTH
            }
            (Raw::Sha512(ctx), PgChecksumType::Sha384) => {
                let d = ctx.take().expect("finalized").final_sha384();
                output[..PG_SHA384_DIGEST_LENGTH].copy_from_slice(&d);
                PG_SHA384_DIGEST_LENGTH
            }
            (Raw::Sha512(ctx), _) => {
                let d = ctx.take().expect("finalized").final_sha512();
                output[..PG_SHA512_DIGEST_LENGTH].copy_from_slice(&d);
                PG_SHA512_DIGEST_LENGTH
            }
        }
    }
}
