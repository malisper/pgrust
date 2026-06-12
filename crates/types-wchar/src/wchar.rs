pub use types_core::PgWChar;

pub type pg_wchar = PgWChar;

/// Null-terminated PostgreSQL wide-character string.
#[derive(Debug, Eq, PartialEq)]
pub struct PgWCharStr<'a> {
    chars: &'a [PgWChar],
}

impl<'a> PgWCharStr<'a> {
    /// Creates a `PgWCharStr` when `chars` contains a terminating zero.
    pub fn from_slice(chars: &'a [PgWChar]) -> Option<Self> {
        chars.contains(&0).then_some(Self { chars })
    }

    /// Creates a `PgWCharStr` without checking for a terminating zero.
    ///
    /// Use this only when the caller has already guaranteed that `chars`
    /// contains a zero terminator.
    pub unsafe fn from_slice_unchecked(chars: &'a [PgWChar]) -> Self {
        Self { chars }
    }

    pub fn as_slice_with_nul(&self) -> &'a [PgWChar] {
        self.chars
    }

    pub fn len(&self) -> usize {
        self.chars
            .iter()
            .position(|&wchar| wchar == 0)
            .unwrap_or(self.chars.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
