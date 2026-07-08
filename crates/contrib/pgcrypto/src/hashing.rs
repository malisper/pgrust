//! digest() and hmac() over PG's in-tree reference hashes (pg_md5/pg_sha1/
//! pg_sha2). Byte-identical to the C non-OpenSSL build.

// C's px_find_digest name -> (digest_len, hmac block_size).
struct HashAlgo {
    which: Which,
    digest_len: usize,
    block_size: usize,
}

enum Which {
    Md5,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

fn find_digest(name: &str) -> Option<HashAlgo> {
    let (which, digest_len, block_size) = match name.to_ascii_lowercase().as_str() {
        "md5" => (Which::Md5, 16, 64),
        "sha1" => (Which::Sha1, 20, 64),
        "sha224" => (Which::Sha224, 28, 64),
        "sha256" => (Which::Sha256, 32, 64),
        "sha384" => (Which::Sha384, 48, 128),
        "sha512" => (Which::Sha512, 64, 128),
        _ => return None,
    };
    Some(HashAlgo { which, digest_len, block_size })
}

fn hash_bytes(algo: &HashAlgo, data: &[u8]) -> Vec<u8> {
    match algo.which {
        Which::Md5 => pg_md5::pg_md5_binary(data).to_vec(),
        Which::Sha1 => pg_sha1::sha1(data).to_vec(),
        Which::Sha224 => pg_sha2::sha224(data).to_vec(),
        Which::Sha256 => pg_sha2::sha256(data).to_vec(),
        Which::Sha384 => pg_sha2::sha384(data).to_vec(),
        Which::Sha512 => pg_sha2::sha512(data).to_vec(),
    }
}

// C px_strerror(PXE_NO_HASH): Cannot use "<name>": No such hash algorithm.
fn no_hash(name: &str) -> String {
    format!("Cannot use \"{name}\": No such hash algorithm")
}

pub fn digest(name: &str, data: &[u8]) -> Result<Vec<u8>, String> {
    let algo = find_digest(name).ok_or_else(|| no_hash(name))?;
    Ok(hash_bytes(&algo, data))
}

// RFC 2104 HMAC (C px_find_hmac + px_hmac_* over the same reference hashes).
pub fn hmac(name: &str, key: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    let algo = find_digest(name).ok_or_else(|| no_hash(name))?;
    let b = algo.block_size;

    let mut k0 = if key.len() > b {
        hash_bytes(&algo, key)
    } else {
        key.to_vec()
    };
    k0.resize(b, 0);

    let ipad: Vec<u8> = k0.iter().map(|&x| x ^ 0x36).collect();
    let opad: Vec<u8> = k0.iter().map(|&x| x ^ 0x5c).collect();

    let mut inner = ipad;
    inner.extend_from_slice(data);
    let inner_digest = hash_bytes(&algo, &inner);

    let mut outer = opad;
    outer.extend_from_slice(&inner_digest);
    Ok(hash_bytes(&algo, &outer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    // Oracle: SELECT digest('abc','<algo>') on C 18.
    #[test]
    fn digests() {
        assert_eq!(hex(&digest("md5", b"abc").unwrap()), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(hex(&digest("sha1", b"abc").unwrap()), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            hex(&digest("sha256", b"abc").unwrap()),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn unknown_algo() {
        assert_eq!(digest("crc32", b"abc").unwrap_err(), "Cannot use \"crc32\": No such hash algorithm");
    }

    // RFC 2104 A.2 / C: SELECT hmac('Hi There', '\x0b'*20, 'md5') style.
    #[test]
    fn hmac_md5_rfc2104() {
        let key = [0x0bu8; 16];
        assert_eq!(
            hex(&hmac("md5", &key, b"Hi There").unwrap()),
            "9294727a3638bb1c13f48ef8158bfc9d"
        );
    }
}
