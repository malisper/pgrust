// Generated from postgres-18.3/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c.

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CodePair {
    pub code: u16,
    pub peer: u16,
}

pub const BIG5LEVEL1TOCNSPLANE1: [CodePair; 25] = [
    CodePair {
        code: 0xa140,
        peer: 0x2121,
    },
    CodePair {
        code: 0xa1f6,
        peer: 0x2258,
    },
    CodePair {
        code: 0xa1f7,
        peer: 0x2257,
    },
    CodePair {
        code: 0xa1f8,
        peer: 0x2259,
    },
    CodePair {
        code: 0xa2af,
        peer: 0x2421,
    },
    CodePair {
        code: 0xa3c0,
        peer: 0x4221,
    },
    CodePair {
        code: 0xa3e1,
        peer: 0x0000,
    },
    CodePair {
        code: 0xa440,
        peer: 0x4421,
    },
    CodePair {
        code: 0xacfe,
        peer: 0x5753,
    },
    CodePair {
        code: 0xacff,
        peer: 0x0000,
    },
    CodePair {
        code: 0xad40,
        peer: 0x5323,
    },
    CodePair {
        code: 0xafd0,
        peer: 0x5754,
    },
    CodePair {
        code: 0xbbc8,
        peer: 0x6b51,
    },
    CodePair {
        code: 0xbe52,
        peer: 0x6b50,
    },
    CodePair {
        code: 0xbe53,
        peer: 0x6f5c,
    },
    CodePair {
        code: 0xc1ab,
        peer: 0x7536,
    },
    CodePair {
        code: 0xc2cb,
        peer: 0x7535,
    },
    CodePair {
        code: 0xc2cc,
        peer: 0x7737,
    },
    CodePair {
        code: 0xc361,
        peer: 0x782e,
    },
    CodePair {
        code: 0xc3b9,
        peer: 0x7865,
    },
    CodePair {
        code: 0xc3ba,
        peer: 0x7864,
    },
    CodePair {
        code: 0xc3bb,
        peer: 0x7866,
    },
    CodePair {
        code: 0xc456,
        peer: 0x782d,
    },
    CodePair {
        code: 0xc457,
        peer: 0x7962,
    },
    CodePair {
        code: 0xc67f,
        peer: 0x0000,
    },
];

pub const CNSPLANE1TOBIG5LEVEL1: [CodePair; 26] = [
    CodePair {
        code: 0x2121,
        peer: 0xa140,
    },
    CodePair {
        code: 0x2257,
        peer: 0xa1f7,
    },
    CodePair {
        code: 0x2258,
        peer: 0xa1f6,
    },
    CodePair {
        code: 0x2259,
        peer: 0xa1f8,
    },
    CodePair {
        code: 0x234f,
        peer: 0x0000,
    },
    CodePair {
        code: 0x2421,
        peer: 0xa2af,
    },
    CodePair {
        code: 0x2571,
        peer: 0x0000,
    },
    CodePair {
        code: 0x4221,
        peer: 0xa3c0,
    },
    CodePair {
        code: 0x4242,
        peer: 0x0000,
    },
    CodePair {
        code: 0x4421,
        peer: 0xa440,
    },
    CodePair {
        code: 0x5323,
        peer: 0xad40,
    },
    CodePair {
        code: 0x5753,
        peer: 0xacfe,
    },
    CodePair {
        code: 0x5754,
        peer: 0xafd0,
    },
    CodePair {
        code: 0x6b50,
        peer: 0xbe52,
    },
    CodePair {
        code: 0x6b51,
        peer: 0xbbc8,
    },
    CodePair {
        code: 0x6f5c,
        peer: 0xbe53,
    },
    CodePair {
        code: 0x7535,
        peer: 0xc2cb,
    },
    CodePair {
        code: 0x7536,
        peer: 0xc1ab,
    },
    CodePair {
        code: 0x7737,
        peer: 0xc2cc,
    },
    CodePair {
        code: 0x782d,
        peer: 0xc456,
    },
    CodePair {
        code: 0x782e,
        peer: 0xc361,
    },
    CodePair {
        code: 0x7864,
        peer: 0xc3ba,
    },
    CodePair {
        code: 0x7865,
        peer: 0xc3b9,
    },
    CodePair {
        code: 0x7866,
        peer: 0xc3bb,
    },
    CodePair {
        code: 0x7962,
        peer: 0xc457,
    },
    CodePair {
        code: 0x7d4c,
        peer: 0x0000,
    },
];

pub const BIG5LEVEL2TOCNSPLANE2: [CodePair; 48] = [
    CodePair {
        code: 0xc940,
        peer: 0x2121,
    },
    CodePair {
        code: 0xc94a,
        peer: 0x0000,
    },
    CodePair {
        code: 0xc94b,
        peer: 0x212b,
    },
    CodePair {
        code: 0xc96c,
        peer: 0x214d,
    },
    CodePair {
        code: 0xc9be,
        peer: 0x214c,
    },
    CodePair {
        code: 0xc9bf,
        peer: 0x217d,
    },
    CodePair {
        code: 0xc9ed,
        peer: 0x224e,
    },
    CodePair {
        code: 0xcaf7,
        peer: 0x224d,
    },
    CodePair {
        code: 0xcaf8,
        peer: 0x2439,
    },
    CodePair {
        code: 0xd77a,
        peer: 0x3f6a,
    },
    CodePair {
        code: 0xd77b,
        peer: 0x387e,
    },
    CodePair {
        code: 0xdba7,
        peer: 0x3f6b,
    },
    CodePair {
        code: 0xddfc,
        peer: 0x4176,
    },
    CodePair {
        code: 0xddfd,
        peer: 0x4424,
    },
    CodePair {
        code: 0xe8a3,
        peer: 0x554c,
    },
    CodePair {
        code: 0xe976,
        peer: 0x5723,
    },
    CodePair {
        code: 0xeb5b,
        peer: 0x5a29,
    },
    CodePair {
        code: 0xebf1,
        peer: 0x554b,
    },
    CodePair {
        code: 0xebf2,
        peer: 0x5b3f,
    },
    CodePair {
        code: 0xecde,
        peer: 0x5722,
    },
    CodePair {
        code: 0xecdf,
        peer: 0x5c6a,
    },
    CodePair {
        code: 0xedaa,
        peer: 0x5d75,
    },
    CodePair {
        code: 0xeeeb,
        peer: 0x642f,
    },
    CodePair {
        code: 0xeeec,
        peer: 0x6039,
    },
    CodePair {
        code: 0xf056,
        peer: 0x5d74,
    },
    CodePair {
        code: 0xf057,
        peer: 0x6243,
    },
    CodePair {
        code: 0xf0cb,
        peer: 0x5a28,
    },
    CodePair {
        code: 0xf0cc,
        peer: 0x6337,
    },
    CodePair {
        code: 0xf163,
        peer: 0x6430,
    },
    CodePair {
        code: 0xf16b,
        peer: 0x6761,
    },
    CodePair {
        code: 0xf16c,
        peer: 0x6438,
    },
    CodePair {
        code: 0xf268,
        peer: 0x6934,
    },
    CodePair {
        code: 0xf269,
        peer: 0x6573,
    },
    CodePair {
        code: 0xf2c3,
        peer: 0x664e,
    },
    CodePair {
        code: 0xf375,
        peer: 0x6762,
    },
    CodePair {
        code: 0xf466,
        peer: 0x6935,
    },
    CodePair {
        code: 0xf4b5,
        peer: 0x664d,
    },
    CodePair {
        code: 0xf4b6,
        peer: 0x6962,
    },
    CodePair {
        code: 0xf4fd,
        peer: 0x6a4c,
    },
    CodePair {
        code: 0xf663,
        peer: 0x6a4b,
    },
    CodePair {
        code: 0xf664,
        peer: 0x6c52,
    },
    CodePair {
        code: 0xf977,
        peer: 0x7167,
    },
    CodePair {
        code: 0xf9c4,
        peer: 0x7166,
    },
    CodePair {
        code: 0xf9c5,
        peer: 0x7234,
    },
    CodePair {
        code: 0xf9c6,
        peer: 0x7240,
    },
    CodePair {
        code: 0xf9c7,
        peer: 0x7235,
    },
    CodePair {
        code: 0xf9d2,
        peer: 0x7241,
    },
    CodePair {
        code: 0xf9d6,
        peer: 0x0000,
    },
];

pub const CNSPLANE2TOBIG5LEVEL2: [CodePair; 49] = [
    CodePair {
        code: 0x2121,
        peer: 0xc940,
    },
    CodePair {
        code: 0x212b,
        peer: 0xc94b,
    },
    CodePair {
        code: 0x214c,
        peer: 0xc9be,
    },
    CodePair {
        code: 0x214d,
        peer: 0xc96c,
    },
    CodePair {
        code: 0x217d,
        peer: 0xc9bf,
    },
    CodePair {
        code: 0x224d,
        peer: 0xcaf7,
    },
    CodePair {
        code: 0x224e,
        peer: 0xc9ed,
    },
    CodePair {
        code: 0x2439,
        peer: 0xcaf8,
    },
    CodePair {
        code: 0x387e,
        peer: 0xd77b,
    },
    CodePair {
        code: 0x3f6a,
        peer: 0xd77a,
    },
    CodePair {
        code: 0x3f6b,
        peer: 0xdba7,
    },
    CodePair {
        code: 0x4424,
        peer: 0x0000,
    },
    CodePair {
        code: 0x4176,
        peer: 0xddfc,
    },
    CodePair {
        code: 0x4177,
        peer: 0x0000,
    },
    CodePair {
        code: 0x4424,
        peer: 0xddfd,
    },
    CodePair {
        code: 0x554b,
        peer: 0xebf1,
    },
    CodePair {
        code: 0x554c,
        peer: 0xe8a3,
    },
    CodePair {
        code: 0x5722,
        peer: 0xecde,
    },
    CodePair {
        code: 0x5723,
        peer: 0xe976,
    },
    CodePair {
        code: 0x5a28,
        peer: 0xf0cb,
    },
    CodePair {
        code: 0x5a29,
        peer: 0xeb5b,
    },
    CodePair {
        code: 0x5b3f,
        peer: 0xebf2,
    },
    CodePair {
        code: 0x5c6a,
        peer: 0xecdf,
    },
    CodePair {
        code: 0x5d74,
        peer: 0xf056,
    },
    CodePair {
        code: 0x5d75,
        peer: 0xedaa,
    },
    CodePair {
        code: 0x6039,
        peer: 0xeeec,
    },
    CodePair {
        code: 0x6243,
        peer: 0xf057,
    },
    CodePair {
        code: 0x6337,
        peer: 0xf0cc,
    },
    CodePair {
        code: 0x642f,
        peer: 0xeeeb,
    },
    CodePair {
        code: 0x6430,
        peer: 0xf163,
    },
    CodePair {
        code: 0x6438,
        peer: 0xf16c,
    },
    CodePair {
        code: 0x6573,
        peer: 0xf269,
    },
    CodePair {
        code: 0x664d,
        peer: 0xf4b5,
    },
    CodePair {
        code: 0x664e,
        peer: 0xf2c3,
    },
    CodePair {
        code: 0x6761,
        peer: 0xf16b,
    },
    CodePair {
        code: 0x6762,
        peer: 0xf375,
    },
    CodePair {
        code: 0x6934,
        peer: 0xf268,
    },
    CodePair {
        code: 0x6935,
        peer: 0xf466,
    },
    CodePair {
        code: 0x6962,
        peer: 0xf4b6,
    },
    CodePair {
        code: 0x6a4b,
        peer: 0xf663,
    },
    CodePair {
        code: 0x6a4c,
        peer: 0xf4fd,
    },
    CodePair {
        code: 0x6c52,
        peer: 0xf664,
    },
    CodePair {
        code: 0x7166,
        peer: 0xf9c4,
    },
    CodePair {
        code: 0x7167,
        peer: 0xf977,
    },
    CodePair {
        code: 0x7234,
        peer: 0xf9c5,
    },
    CodePair {
        code: 0x7235,
        peer: 0xf9c7,
    },
    CodePair {
        code: 0x7240,
        peer: 0xf9c6,
    },
    CodePair {
        code: 0x7241,
        peer: 0xf9d2,
    },
    CodePair {
        code: 0x7245,
        peer: 0x0000,
    },
];

pub const B1C4: [(u16, u16); 4] = [
    (0xc879, 0x2123),
    (0xc87b, 0x2124),
    (0xc87d, 0x212a),
    (0xc8a2, 0x2152),
];

pub const B2C3: [(u16, u16); 7] = [
    (0xf9d6, 0x4337),
    (0xf9d7, 0x4f50),
    (0xf9d8, 0x444e),
    (0xf9d9, 0x504a),
    (0xf9da, 0x2c5d),
    (0xf9db, 0x3d7e),
    (0xf9dc, 0x4b5c),
];
