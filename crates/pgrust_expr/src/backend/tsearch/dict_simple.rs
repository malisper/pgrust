pub fn lexize_simple(token: &str) -> Option<String> {
    let normalized = token.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}
