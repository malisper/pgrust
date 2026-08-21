//! Byte-substring search — the `contains_scalar` extraction from the
//! harness simd.rs (the only symbol the engine closure uses:
//! VarPredTerm::eval). The SSE2/NEON variants stay in the reference tree
//! until census heat argues for them (they served the handwritten
//! kernels, not the stencils).

pub fn contains_scalar(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || hay.len() < needle.len() {
        return false;
    }
    let first = needle[0];
    let end = hay.len() - needle.len();
    let mut i = 0;
    while i <= end {
        if hay[i] == first && &hay[i..i + needle.len()] == needle {
            return true;
        }
        i += 1;
    }
    false
}
