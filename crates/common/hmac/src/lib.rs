// hmac.c contract: init(key) hashes an over-block key down first, then folds
// ipad/opad per RFC 2104; update/final mirror the C incremental sequence.
// Monomorphized per hash (no fn-pointer table); infallible — C's error arms
// are OOM/EVP-failure only. Target builds use hmac_openssl.c; output identical.
// DIVERGENCE: C explicit_bzero's key material on free; ctxs here drop unwiped.

use pg_sha2::{PgSha256Ctx, PgSha512Ctx};

const HMAC_IPAD: u8 = 0x36;
const HMAC_OPAD: u8 = 0x5c;

pub trait HmacHash {
    const BLOCK_LENGTH: usize;
    const DIGEST_LENGTH: usize;
    type Digest: AsRef<[u8]>;
    fn init() -> Self;
    fn update(&mut self, data: &[u8]);
    fn finalize(self) -> Self::Digest;
}

macro_rules! hmac_hash {
    ($name:ident, $ctx:ty, $init:ident, $final:ident, $block:expr, $digest:expr) => {
        pub struct $name($ctx);
        impl HmacHash for $name {
            const BLOCK_LENGTH: usize = $block;
            const DIGEST_LENGTH: usize = $digest;
            type Digest = [u8; $digest];
            fn init() -> Self {
                $name(<$ctx>::$init())
            }
            fn update(&mut self, data: &[u8]) {
                self.0.update(data);
            }
            fn finalize(self) -> Self::Digest {
                self.0.$final()
            }
        }
    };
}

hmac_hash!(Sha224, PgSha256Ctx, init_sha224, final_sha224,
    pg_sha2::PG_SHA224_BLOCK_LENGTH, pg_sha2::PG_SHA224_DIGEST_LENGTH);
hmac_hash!(Sha256, PgSha256Ctx, init_sha256, final_sha256,
    pg_sha2::PG_SHA256_BLOCK_LENGTH, pg_sha2::PG_SHA256_DIGEST_LENGTH);
hmac_hash!(Sha384, PgSha512Ctx, init_sha384, final_sha384,
    pg_sha2::PG_SHA384_BLOCK_LENGTH, pg_sha2::PG_SHA384_DIGEST_LENGTH);
hmac_hash!(Sha512, PgSha512Ctx, init_sha512, final_sha512,
    pg_sha2::PG_SHA512_BLOCK_LENGTH, pg_sha2::PG_SHA512_DIGEST_LENGTH);

pub struct PgHmacCtx<H: HmacHash> {
    hash: H,
    k_opad: [u8; 128],
}

impl<H: HmacHash> PgHmacCtx<H> {
    pub fn init(key: &[u8]) -> Self {
        assert!(H::BLOCK_LENGTH <= 128);
        let mut k_ipad = [HMAC_IPAD; 128];
        let mut k_opad = [HMAC_OPAD; 128];

        let shrunk;
        let key: &[u8] = if key.len() > H::BLOCK_LENGTH {
            let mut h = H::init();
            h.update(key);
            shrunk = h.finalize();
            shrunk.as_ref()
        } else {
            key
        };
        for (i, &b) in key.iter().enumerate() {
            k_ipad[i] ^= b;
            k_opad[i] ^= b;
        }

        let mut hash = H::init();
        hash.update(&k_ipad[..H::BLOCK_LENGTH]);
        PgHmacCtx { hash, k_opad }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hash.update(data);
    }

    pub fn finalize(self) -> H::Digest {
        let inner = self.hash.finalize();
        let mut outer = H::init();
        outer.update(&self.k_opad[..H::BLOCK_LENGTH]);
        outer.update(inner.as_ref());
        outer.finalize()
    }
}

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; pg_sha2::PG_SHA256_DIGEST_LENGTH] {
    let mut ctx = PgHmacCtx::<Sha256>::init(key);
    ctx.update(msg);
    ctx.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    // RFC 4231 cases 1-4, 6, 7 (6/7 = over-block key, exercises key-shrink).
    #[test]
    fn rfc4231_hmac_sha256() {
        let case4_key: [u8; 25] = core::array::from_fn(|i| (i + 1) as u8);
        assert_eq!(
            hex(&hmac_sha256(&case4_key, &[0xcd; 50])),
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
        );
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            hex(&hmac_sha256(&[0xaa; 20], &[0xdd; 50])),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm."
            )),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"
        );
    }

    #[test]
    fn rfc4231_other_widths_case2() {
        let key = b"Jefe";
        let msg = b"what do ya want for nothing?";

        let mut c = PgHmacCtx::<Sha224>::init(key);
        c.update(msg);
        assert_eq!(hex(&c.finalize()), "a30e01098bc6dbbf45690f3a7e9e6d0f8bbea2a39e6148008fd05e44");

        let mut c = PgHmacCtx::<Sha384>::init(key);
        c.update(msg);
        assert_eq!(
            hex(&c.finalize()),
            "af45d2e376484031617f78d2b58a6b1b9c7ef464f5a01b47e42ec3736322445e8e2240ca5e69e2c78b3239ecfab21649"
        );

        let mut c = PgHmacCtx::<Sha512>::init(key);
        c.update(msg);
        assert_eq!(
            hex(&c.finalize()),
            "164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737"
        );
    }

    // RFC 4231 case 5: output truncated to 128 bits; assert the prefix.
    #[test]
    fn rfc4231_case5_truncated() {
        let key = [0x0cu8; 20];
        let msg = b"Test With Truncation";
        let mut c = PgHmacCtx::<Sha224>::init(&key);
        c.update(msg);
        assert_eq!(hex(&c.finalize()[..16]), "0e2aea68a90c8d37c988bcdb9fca6fa8");
        assert_eq!(hex(&hmac_sha256(&key, msg)[..16]), "a3b6167473100ee06e0c796c2955552b");
        let mut c = PgHmacCtx::<Sha384>::init(&key);
        c.update(msg);
        assert_eq!(hex(&c.finalize()[..16]), "3abf34c3503b2a23a46efc619baef897");
        let mut c = PgHmacCtx::<Sha512>::init(&key);
        c.update(msg);
        assert_eq!(hex(&c.finalize()[..16]), "415fad6271580a531d4179bc891d87a6");
    }

    // RFC 4231 cases 6/7 at the 128-byte-block widths (SHA-384/512 key-shrink).
    #[test]
    fn rfc4231_keyshrink_wide_blocks() {
        let key = [0xaau8; 131];
        let msg6: &[u8] = b"Test Using Larger Than Block-Size Key - Hash Key First";
        let msg7: &[u8] = b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.";

        let mut c = PgHmacCtx::<Sha384>::init(&key);
        c.update(msg6);
        assert_eq!(
            hex(&c.finalize()),
            "4ece084485813e9088d2c63a041bc5b44f9ef1012a2b588f3cd11f05033ac4c60c2ef6ab4030fe8296248df163f44952"
        );
        let mut c = PgHmacCtx::<Sha512>::init(&key);
        c.update(msg6);
        assert_eq!(
            hex(&c.finalize()),
            "80b24263c7c1a3ebb71493c1dd7be8b49b46d1f41b4aeec1121b013783f8f3526b56d037e05f2598bd0fd2215d6a1e5295e64f73f63f0aec8b915a985d786598"
        );
        let mut c = PgHmacCtx::<Sha384>::init(&key);
        c.update(msg7);
        assert_eq!(
            hex(&c.finalize()),
            "6617178e941f020d351e2f254e8fd32c602420feb0b8fb9adccebb82461e99c5a678cc31e799176d3860e6110c46523e"
        );
        let mut c = PgHmacCtx::<Sha512>::init(&key);
        c.update(msg7);
        assert_eq!(
            hex(&c.finalize()),
            "e37b6a775dc87dbaa4dfa9f96e5e3ffddebd71f8867289865df5a32d20cdc944b6022cac3c4982b10d5eeb55c3e4de15134676fb6de0446065c97440fa8c6a58"
        );
    }

    #[test]
    fn split_updates_match_one_shot() {
        let key = [0x0bu8; 20];
        let msg = b"Hi There";
        let mut c = PgHmacCtx::<Sha256>::init(&key);
        for b in msg.iter() {
            c.update(core::slice::from_ref(b));
        }
        assert_eq!(c.finalize(), hmac_sha256(&key, msg));
    }
}
