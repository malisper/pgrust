use std::collections::HashSet;
use std::sync::OnceLock;

use rust_stemmers::{Algorithm, Stemmer};

fn english_stemmer() -> &'static Stemmer {
    static STEMMER: OnceLock<Stemmer> = OnceLock::new();
    STEMMER.get_or_init(|| Stemmer::create(Algorithm::English))
}

fn english_stopwords() -> &'static HashSet<&'static str> {
    static STOPWORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();
    STOPWORDS.get_or_init(|| {
        include_str!("english.stop")
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect()
    })
}

pub(crate) fn lexize_english(token: &str) -> Option<String> {
    let normalized = token.trim().to_ascii_lowercase();
    if normalized.is_empty() || english_stopwords().contains(normalized.as_str()) {
        return None;
    }
    Some(english_stemmer().stem(&normalized).to_string())
}
