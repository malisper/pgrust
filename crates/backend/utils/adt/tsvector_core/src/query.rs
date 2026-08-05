// ts_type.h TSQuery item layout, shared by the tsquery crate and TS_execute.

pub const QI_VAL: i8 = 1;
pub const QI_OPR: i8 = 2;
pub const QI_VALSTOP: i8 = 3;

pub const OP_NOT: i8 = 1;
pub const OP_AND: i8 = 2;
pub const OP_OR: i8 = 3;
pub const OP_PHRASE: i8 = 4;

pub const OP_PRIORITY: [i32; 4] = [4, 2, 1, 3];

#[inline]
pub fn op_priority(op: i8) -> i32 {
    OP_PRIORITY[(op - 1) as usize]
}

pub const QUERYITEM_SIZE: usize = 12;
pub const HDRSIZETQ: usize = 8;
pub const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Operand {
    pub weight: u8,
    pub prefix: bool,
    pub valcrc: i32,
    pub length: usize,
    pub distance: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Operator {
    pub oper: i8,
    pub distance: i16,
    pub left: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Item {
    Val(Operand),
    Opr(Operator),
    ValStop,
}

impl Item {
    pub fn decode(raw: &[u8; QUERYITEM_SIZE]) -> Item {
        match raw[0] as i8 {
            QI_VAL => Item::Val(Operand {
                weight: raw[1],
                prefix: raw[2] != 0,
                valcrc: i32::from_ne_bytes(raw[4..8].try_into().unwrap()),
                length: {
                    let packed = u32::from_ne_bytes(raw[8..12].try_into().unwrap());
                    (packed & 0xfff) as usize
                },
                distance: {
                    let packed = u32::from_ne_bytes(raw[8..12].try_into().unwrap());
                    (packed >> 12) as usize
                },
            }),
            QI_OPR => Item::Opr(Operator {
                oper: raw[1] as i8,
                distance: i16::from_ne_bytes(raw[2..4].try_into().unwrap()),
                left: u32::from_ne_bytes(raw[4..8].try_into().unwrap()),
            }),
            QI_VALSTOP => Item::ValStop,
            other => panic!("unrecognized QueryItem type: {other}"),
        }
    }

    pub fn encode(self) -> [u8; QUERYITEM_SIZE] {
        let mut raw = [0u8; QUERYITEM_SIZE];
        match self {
            Item::Val(o) => {
                raw[0] = QI_VAL as u8;
                raw[1] = o.weight;
                raw[2] = o.prefix as u8;
                raw[4..8].copy_from_slice(&o.valcrc.to_ne_bytes());
                let packed = (o.length as u32 & 0xfff) | ((o.distance as u32) << 12);
                raw[8..12].copy_from_slice(&packed.to_ne_bytes());
            }
            Item::Opr(o) => {
                raw[0] = QI_OPR as u8;
                raw[1] = o.oper as u8;
                raw[2..4].copy_from_slice(&o.distance.to_ne_bytes());
                raw[4..8].copy_from_slice(&o.left.to_ne_bytes());
            }
            Item::ValStop => raw[0] = QI_VALSTOP as u8,
        }
        raw
    }
}

// A tsquery payload (varlena header stripped): int32 size, QueryItem[size],
// then NUL-terminated operand strings ('distance' offsets into that pool).
#[derive(Clone, Copy)]
pub struct TsQueryRef<'a> {
    pub payload: &'a [u8],
}

impl<'a> TsQueryRef<'a> {
    #[inline]
    pub fn size(self) -> usize {
        i32::from_ne_bytes(self.payload[0..4].try_into().unwrap()).max(0) as usize
    }

    #[inline]
    pub fn item(self, i: usize) -> Item {
        let off = 4 + i * QUERYITEM_SIZE;
        Item::decode(self.payload[off..off + QUERYITEM_SIZE].try_into().unwrap())
    }

    #[inline]
    pub fn operand_pool(self) -> &'a [u8] {
        &self.payload[4 + self.size() * QUERYITEM_SIZE..]
    }

    #[inline]
    pub fn operand_str(self, o: &Operand) -> &'a [u8] {
        &self.operand_pool()[o.distance..o.distance + o.length]
    }
}
