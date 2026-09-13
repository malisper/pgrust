//! digest() and hmac() over the linked OpenSSL EVP digests, as C openssl.c
//! px_find_digest (EVP_get_digestbyname) and px-hmac.c px_find_hmac do.

use std::ffi::CString;
use openssl_sys as ssl;

// OpenSSL 3.4+ evp.h maps EVP_MD_CTX_get_size (what C digest_result_size
// calls) onto this, which consults the context and reports -1 for an XOF
// with no output length set. openssl-sys only binds the EVP_MD-level form.
extern "C" {
    fn EVP_MD_CTX_get_size_ex(ctx: *const ssl::EVP_MD_CTX) -> core::ffi::c_int;
}

pub enum DigestError {
    NoHash,
    CipherInit,
}

/// openssl.c OSSLDigest: an EVP_MD_CTX initialised for one digest.
pub struct OsslDigest {
    md: *const ssl::EVP_MD,
    ctx: *mut ssl::EVP_MD_CTX,
}

impl OsslDigest {
    /// openssl.c:168 px_find_digest.
    pub fn find(name: &str) -> Result<OsslDigest, DigestError> {
        let cname = CString::new(name).map_err(|_| DigestError::NoHash)?;
        // SAFETY: cname is NUL-terminated for the lookup; the EVP_MD is a
        // static table entry OpenSSL owns; ctx is freed in Drop.
        unsafe {
            let md = ssl::EVP_get_digestbyname(cname.as_ptr());
            if md.is_null() {
                return Err(DigestError::NoHash);
            }
            let ctx = ssl::EVP_MD_CTX_new();
            if ctx.is_null() {
                return Err(DigestError::CipherInit);
            }
            if ssl::EVP_DigestInit_ex(ctx, md, core::ptr::null_mut()) == 0 {
                ssl::EVP_MD_CTX_free(ctx);
                return Err(DigestError::CipherInit);
            }
            Ok(OsslDigest { md, ctx })
        }
    }

    /// openssl.c:101 digest_result_size: negative (an XOF under OpenSSL 3.4+)
    /// is elog(ERROR).
    pub fn result_size(&self) -> Result<usize, &'static str> {
        // SAFETY: ctx is a live, initialised EVP_MD_CTX.
        let n = unsafe { EVP_MD_CTX_get_size_ex(self.ctx) };
        if n < 0 {
            return Err("EVP_MD_CTX_size() failed");
        }
        Ok(n as usize)
    }

    pub fn block_size(&self) -> Result<usize, &'static str> {
        // SAFETY: md is a live EVP_MD.
        let n = unsafe { ssl::EVP_MD_get_block_size(self.md) };
        if n < 0 {
            return Err("EVP_MD_CTX_block_size() failed");
        }
        Ok(n as usize)
    }

    pub fn reset(&mut self) {
        // SAFETY: ctx/md are live and owned by self.
        unsafe { ssl::EVP_DigestInit_ex(self.ctx, self.md, core::ptr::null_mut()) };
    }

    pub fn update(&mut self, data: &[u8]) {
        // SAFETY: ctx is live; data is a valid slice for its length.
        unsafe { ssl::EVP_DigestUpdate(self.ctx, data.as_ptr().cast(), data.len()) };
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = vec![0u8; ssl::EVP_MAX_MD_SIZE as usize];
        let mut n: u32 = 0;
        // SAFETY: out holds EVP_MAX_MD_SIZE bytes, the documented maximum.
        unsafe { ssl::EVP_DigestFinal_ex(self.ctx, out.as_mut_ptr(), &mut n) };
        out.truncate(n as usize);
        out
    }
}

impl Drop for OsslDigest {
    fn drop(&mut self) {
        // SAFETY: ctx was allocated by EVP_MD_CTX_new and is freed once.
        unsafe { ssl::EVP_MD_CTX_free(self.ctx) };
    }
}

pub enum HashError {
    /// pgcrypto.c:504 find_provider: `Cannot use "<name>": <px_strerror>`, 22023.
    Provider(String),
    /// openssl.c digest_* elog(ERROR)s: XX000.
    Internal(&'static str),
    Pg(Box<types_error::PgError>),
}

fn cannot_use(name: &str, what: &str) -> HashError {
    HashError::Provider(format!("Cannot use \"{name}\": {what}"))
}

fn find_digest(name: &str) -> Result<OsslDigest, HashError> {
    OsslDigest::find(name).map_err(|e| match e {
        DigestError::NoHash => cannot_use(name, "No such hash algorithm"),
        DigestError::CipherInit => cannot_use(name, "Cipher cannot be initialized"),
    })
}

pub fn digest(name: &str, data: &[u8]) -> Result<Vec<u8>, HashError> {
    // pgcrypto.c:504 find_provider: downcase_truncate_identifier first.
    let name = crate::provider_name(name).map_err(HashError::Pg)?;
    let mut md = find_digest(&name)?;
    md.result_size().map_err(HashError::Internal)?;
    md.update(data);
    Ok(md.finish())
}

// px-hmac.c px_find_hmac + px_hmac_* (RFC 2104) over the same EVP digests.
pub fn hmac(name: &str, key: &[u8], data: &[u8]) -> Result<Vec<u8>, HashError> {
    let name = crate::provider_name(name).map_err(HashError::Pg)?;
    let mut md = find_digest(&name)?;
    let b = md.block_size().map_err(HashError::Internal)?;
    if b < 2 {
        return Err(cannot_use(&name, "This hash algorithm is unusable for HMAC"));
    }
    md.result_size().map_err(HashError::Internal)?;

    let mut k0 = if key.len() > b {
        md.update(key);
        md.finish()
    } else {
        key.to_vec()
    };
    k0.resize(b, 0);

    let ipad: Vec<u8> = k0.iter().map(|&x| x ^ 0x36).collect();
    let opad: Vec<u8> = k0.iter().map(|&x| x ^ 0x5c).collect();

    md.reset();
    md.update(&ipad);
    md.update(data);
    let inner_digest = md.finish();

    md.reset();
    md.update(&opad);
    md.update(&inner_digest);
    Ok(md.finish())
}
