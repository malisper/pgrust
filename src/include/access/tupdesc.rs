#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeAlign {
    Char,
    Short,
    Int,
    Double,
}

impl AttributeAlign {
    pub fn align_offset(self, off: usize) -> usize {
        match self {
            Self::Char => off,
            Self::Short => (off + 1) & !1,
            Self::Int => (off + 3) & !3,
            Self::Double => (off + 7) & !7,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeDesc {
    pub name: String,
    pub attlen: i16,
    pub attalign: AttributeAlign,
    pub nullable: bool,
}
