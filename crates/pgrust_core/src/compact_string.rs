/// A string type that stores short strings inline (up to 22 bytes on the stack)
/// to avoid heap allocation. Falls back to standard String for longer strings.
#[derive(Clone, Hash)]
pub enum CompactString {
    Inline { len: u8, buf: [u8; 22] },
    Heap(String),
}

impl CompactString {
    pub fn new(s: &str) -> Self {
        if s.len() <= 22 {
            let mut buf = [0u8; 22];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            CompactString::Inline {
                len: s.len() as u8,
                buf,
            }
        } else {
            CompactString::Heap(s.to_owned())
        }
    }

    pub fn from_owned(s: String) -> Self {
        if s.len() <= 22 {
            let mut buf = [0u8; 22];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            CompactString::Inline {
                len: s.len() as u8,
                buf,
            }
        } else {
            CompactString::Heap(s)
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            CompactString::Inline { len, buf } => {
                // Safety: we only store valid UTF-8.
                unsafe { std::str::from_utf8_unchecked(&buf[..*len as usize]) }
            }
            CompactString::Heap(s) => s.as_str(),
        }
    }

    pub fn bytes(&self) -> impl Iterator<Item = u8> + '_ {
        self.as_str().bytes()
    }

    pub fn len(&self) -> usize {
        match self {
            CompactString::Inline { len, .. } => *len as usize,
            CompactString::Heap(s) => s.len(),
        }
    }
}

impl std::ops::Deref for CompactString {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Debug for CompactString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl std::fmt::Display for CompactString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.as_str(), f)
    }
}

impl PartialEq for CompactString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for CompactString {}

impl PartialOrd for CompactString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl From<&str> for CompactString {
    fn from(s: &str) -> Self {
        CompactString::new(s)
    }
}

impl From<String> for CompactString {
    fn from(s: String) -> Self {
        CompactString::from_owned(s)
    }
}
