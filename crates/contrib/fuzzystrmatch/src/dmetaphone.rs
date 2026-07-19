//! `dmetaphone.c` — Double Metaphone. Operates on the ASCII-uppercased byte
//! string padded with five trailing spaces, exactly as the C works on its
//! padded metastring; `length`/`last` are the pre-padding values.

use crate::ascii_upper;

fn get_at(s: &[u8], pos: isize) -> u8 {
    if pos < 0 || pos as usize >= s.len() {
        0
    } else {
        s[pos as usize]
    }
}

fn is_vowel(s: &[u8], pos: isize) -> bool {
    matches!(get_at(s, pos), b'A' | b'E' | b'I' | b'O' | b'U' | b'Y')
}

fn string_at(s: &[u8], start: isize, len: usize, tests: &[&[u8]]) -> bool {
    if start < 0 || start as usize >= s.len() {
        return false;
    }
    let start = start as usize;
    let Some(slice) = s.get(start..start + len) else {
        return false;
    };
    tests.iter().any(|t| {
        debug_assert_eq!(t.len(), len);
        *t == slice
    })
}

fn contains(s: &[u8], needle: &[u8]) -> bool {
    s.windows(needle.len()).any(|w| w == needle)
}

fn slavo_germanic(s: &[u8]) -> bool {
    contains(s, b"W") || contains(s, b"K") || contains(s, b"CZ") || contains(s, b"WITZ")
}

pub fn double_metaphone(input: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let length = input.len() as isize;
    let last = length - 1;

    let mut original: Vec<u8> = Vec::with_capacity(input.len() + 5);
    original.extend(input.iter().map(|&c| ascii_upper(c)));
    original.extend_from_slice(b"     ");
    let s = &original[..];

    let mut primary: Vec<u8> = Vec::new();
    let mut secondary: Vec<u8> = Vec::new();
    let mut current: isize = 0;

    if string_at(s, 0, 2, &[b"GN", b"KN", b"PN", b"WR", b"PS"]) {
        current += 1;
    }

    if get_at(s, 0) == b'X' {
        primary.push(b'S');
        secondary.push(b'S');
        current += 1;
    }

    while primary.len() < 4 || secondary.len() < 4 {
        if current >= length {
            break;
        }

        macro_rules! add {
            ($p:expr, $a:expr) => {{
                primary.extend_from_slice($p);
                secondary.extend_from_slice($a);
            }};
        }

        match get_at(s, current) {
            b'A' | b'E' | b'I' | b'O' | b'U' | b'Y' => {
                if current == 0 {
                    add!(b"A", b"A");
                }
                current += 1;
            }
            b'B' => {
                add!(b"P", b"P");
                current += if get_at(s, current + 1) == b'B' { 2 } else { 1 };
            }
            0xC7 => {
                // C with cedilla (Latin-1 byte, as in the C switch)
                add!(b"S", b"S");
                current += 1;
            }
            b'C' => {
                if current > 1
                    && !is_vowel(s, current - 2)
                    && string_at(s, current - 1, 3, &[b"ACH"])
                    && (get_at(s, current + 2) != b'I'
                        && (get_at(s, current + 2) != b'E'
                            || string_at(s, current - 2, 6, &[b"BACHER", b"MACHER"])))
                {
                    add!(b"K", b"K");
                    current += 2;
                } else if current == 0 && string_at(s, current, 6, &[b"CAESAR"]) {
                    add!(b"S", b"S");
                    current += 2;
                } else if string_at(s, current, 4, &[b"CHIA"]) {
                    add!(b"K", b"K");
                    current += 2;
                } else if string_at(s, current, 2, &[b"CH"]) {
                    if current > 0 && string_at(s, current, 4, &[b"CHAE"]) {
                        add!(b"K", b"X");
                    } else if current == 0
                        && (string_at(s, current + 1, 5, &[b"HARAC", b"HARIS"])
                            || string_at(s, current + 1, 3, &[b"HOR", b"HYM", b"HIA", b"HEM"]))
                        && !string_at(s, 0, 5, &[b"CHORE"])
                    {
                        add!(b"K", b"K");
                    } else if (string_at(s, 0, 4, &[b"VAN ", b"VON "])
                        || string_at(s, 0, 3, &[b"SCH"]))
                        || string_at(s, current - 2, 6, &[b"ORCHES", b"ARCHIT", b"ORCHID"])
                        || string_at(s, current + 2, 1, &[b"T", b"S"])
                        || ((string_at(s, current - 1, 1, &[b"A", b"O", b"U", b"E"])
                            || current == 0)
                            && string_at(
                                s,
                                current + 2,
                                1,
                                &[b"L", b"R", b"N", b"M", b"B", b"H", b"F", b"V", b"W", b" "],
                            ))
                    {
                        add!(b"K", b"K");
                    } else if current > 0 {
                        if string_at(s, 0, 2, &[b"MC"]) {
                            add!(b"K", b"K");
                        } else {
                            add!(b"X", b"K");
                        }
                    } else {
                        add!(b"X", b"X");
                    }
                    current += 2;
                } else if string_at(s, current, 2, &[b"CZ"])
                    && !string_at(s, current - 2, 4, &[b"WICZ"])
                {
                    add!(b"S", b"X");
                    current += 2;
                } else if string_at(s, current + 1, 3, &[b"CIA"]) {
                    add!(b"X", b"X");
                    current += 3;
                } else if string_at(s, current, 2, &[b"CC"])
                    && !(current == 1 && get_at(s, 0) == b'M')
                {
                    if string_at(s, current + 2, 1, &[b"I", b"E", b"H"])
                        && !string_at(s, current + 2, 2, &[b"HU"])
                    {
                        if (current == 1 && get_at(s, current - 1) == b'A')
                            || string_at(s, current - 1, 5, &[b"UCCEE", b"UCCES"])
                        {
                            add!(b"KS", b"KS");
                        } else {
                            add!(b"X", b"X");
                        }
                        current += 3;
                    } else {
                        add!(b"K", b"K");
                        current += 2;
                    }
                } else if string_at(s, current, 2, &[b"CK", b"CG", b"CQ"]) {
                    add!(b"K", b"K");
                    current += 2;
                } else if string_at(s, current, 2, &[b"CI", b"CE", b"CY"]) {
                    if string_at(s, current, 3, &[b"CIO", b"CIE", b"CIA"]) {
                        add!(b"S", b"X");
                    } else {
                        add!(b"S", b"S");
                    }
                    current += 2;
                } else {
                    add!(b"K", b"K");
                    if string_at(s, current + 1, 2, &[b" C", b" Q", b" G"]) {
                        current += 3;
                    } else if string_at(s, current + 1, 1, &[b"C", b"K", b"Q"])
                        && !string_at(s, current + 1, 2, &[b"CE", b"CI"])
                    {
                        current += 2;
                    } else {
                        current += 1;
                    }
                }
            }
            b'D' => {
                if string_at(s, current, 2, &[b"DG"]) {
                    if string_at(s, current + 2, 1, &[b"I", b"E", b"Y"]) {
                        add!(b"J", b"J");
                        current += 3;
                    } else {
                        add!(b"TK", b"TK");
                        current += 2;
                    }
                } else if string_at(s, current, 2, &[b"DT", b"DD"]) {
                    add!(b"T", b"T");
                    current += 2;
                } else {
                    add!(b"T", b"T");
                    current += 1;
                }
            }
            b'F' => {
                current += if get_at(s, current + 1) == b'F' { 2 } else { 1 };
                add!(b"F", b"F");
            }
            b'G' => {
                if get_at(s, current + 1) == b'H' {
                    if current > 0 && !is_vowel(s, current - 1) {
                        add!(b"K", b"K");
                        current += 2;
                    } else if current == 0 {
                        if get_at(s, current + 2) == b'I' {
                            add!(b"J", b"J");
                        } else {
                            add!(b"K", b"K");
                        }
                        current += 2;
                    } else if (current > 1 && string_at(s, current - 2, 1, &[b"B", b"H", b"D"]))
                        || (current > 2 && string_at(s, current - 3, 1, &[b"B", b"H", b"D"]))
                        || (current > 3 && string_at(s, current - 4, 1, &[b"B", b"H"]))
                    {
                        current += 2;
                    } else {
                        if current > 2
                            && get_at(s, current - 1) == b'U'
                            && string_at(s, current - 3, 1, &[b"C", b"G", b"L", b"R", b"T"])
                        {
                            add!(b"F", b"F");
                        } else if current > 0 && get_at(s, current - 1) != b'I' {
                            add!(b"K", b"K");
                        }
                        current += 2;
                    }
                } else if get_at(s, current + 1) == b'N' {
                    if current == 1 && is_vowel(s, 0) && !slavo_germanic(s) {
                        add!(b"KN", b"N");
                    } else if !string_at(s, current + 2, 2, &[b"EY"])
                        && get_at(s, current + 1) != b'Y'
                        && !slavo_germanic(s)
                    {
                        add!(b"N", b"KN");
                    } else {
                        add!(b"KN", b"KN");
                    }
                    current += 2;
                } else if string_at(s, current + 1, 2, &[b"LI"]) && !slavo_germanic(s) {
                    add!(b"KL", b"L");
                    current += 2;
                } else if current == 0
                    && (get_at(s, current + 1) == b'Y'
                        || string_at(
                            s,
                            current + 1,
                            2,
                            &[
                                b"ES", b"EP", b"EB", b"EL", b"EY", b"IB", b"IL", b"IN", b"IE",
                                b"EI", b"ER",
                            ],
                        ))
                {
                    add!(b"K", b"J");
                    current += 2;
                } else if (string_at(s, current + 1, 2, &[b"ER"]) || get_at(s, current + 1) == b'Y')
                    && !string_at(s, 0, 6, &[b"DANGER", b"RANGER", b"MANGER"])
                    && !string_at(s, current - 1, 1, &[b"E", b"I"])
                    && !string_at(s, current - 1, 3, &[b"RGY", b"OGY"])
                {
                    add!(b"K", b"J");
                    current += 2;
                } else if string_at(s, current + 1, 1, &[b"E", b"I", b"Y"])
                    || string_at(s, current - 1, 4, &[b"AGGI", b"OGGI"])
                {
                    if (string_at(s, 0, 4, &[b"VAN ", b"VON "]) || string_at(s, 0, 3, &[b"SCH"]))
                        || string_at(s, current + 1, 2, &[b"ET"])
                    {
                        add!(b"K", b"K");
                    } else if string_at(s, current + 1, 4, &[b"IER "]) {
                        add!(b"J", b"J");
                    } else {
                        add!(b"J", b"K");
                    }
                    current += 2;
                } else {
                    current += if get_at(s, current + 1) == b'G' { 2 } else { 1 };
                    add!(b"K", b"K");
                }
            }
            b'H' => {
                if (current == 0 || is_vowel(s, current - 1)) && is_vowel(s, current + 1) {
                    add!(b"H", b"H");
                    current += 2;
                } else {
                    current += 1;
                }
            }
            b'J' => {
                if string_at(s, current, 4, &[b"JOSE"]) || string_at(s, 0, 4, &[b"SAN "]) {
                    if (current == 0 && get_at(s, current + 4) == b' ')
                        || string_at(s, 0, 4, &[b"SAN "])
                    {
                        add!(b"H", b"H");
                    } else {
                        add!(b"J", b"H");
                    }
                    current += 1;
                } else {
                    if current == 0 && !string_at(s, current, 4, &[b"JOSE"]) {
                        add!(b"J", b"A");
                    } else if is_vowel(s, current - 1)
                        && !slavo_germanic(s)
                        && (get_at(s, current + 1) == b'A' || get_at(s, current + 1) == b'O')
                    {
                        add!(b"J", b"H");
                    } else if current == last {
                        add!(b"J", b"");
                    } else if !string_at(
                        s,
                        current + 1,
                        1,
                        &[b"L", b"T", b"K", b"S", b"N", b"M", b"B", b"Z"],
                    ) && !string_at(s, current - 1, 1, &[b"S", b"K", b"L"])
                    {
                        add!(b"J", b"J");
                    }
                    current += if get_at(s, current + 1) == b'J' { 2 } else { 1 };
                }
            }
            b'K' => {
                current += if get_at(s, current + 1) == b'K' { 2 } else { 1 };
                add!(b"K", b"K");
            }
            b'L' => {
                if get_at(s, current + 1) == b'L' {
                    if (current == length - 3
                        && string_at(s, current - 1, 4, &[b"ILLO", b"ILLA", b"ALLE"]))
                        || ((string_at(s, last - 1, 2, &[b"AS", b"OS"])
                            || string_at(s, last, 1, &[b"A", b"O"]))
                            && string_at(s, current - 1, 4, &[b"ALLE"]))
                    {
                        add!(b"L", b"");
                        current += 2;
                        continue;
                    }
                    current += 2;
                } else {
                    current += 1;
                }
                add!(b"L", b"L");
            }
            b'M' => {
                if (string_at(s, current - 1, 3, &[b"UMB"])
                    && (current + 1 == last || string_at(s, current + 2, 2, &[b"ER"])))
                    || get_at(s, current + 1) == b'M'
                {
                    current += 2;
                } else {
                    current += 1;
                }
                add!(b"M", b"M");
            }
            b'N' => {
                current += if get_at(s, current + 1) == b'N' { 2 } else { 1 };
                add!(b"N", b"N");
            }
            0xD1 => {
                // N with tilde (Latin-1 byte, as in the C switch)
                current += 1;
                add!(b"N", b"N");
            }
            b'P' => {
                if get_at(s, current + 1) == b'H' {
                    add!(b"F", b"F");
                    current += 2;
                } else {
                    current += if string_at(s, current + 1, 1, &[b"P", b"B"]) { 2 } else { 1 };
                    add!(b"P", b"P");
                }
            }
            b'Q' => {
                current += if get_at(s, current + 1) == b'Q' { 2 } else { 1 };
                add!(b"K", b"K");
            }
            b'R' => {
                if current == last
                    && !slavo_germanic(s)
                    && string_at(s, current - 2, 2, &[b"IE"])
                    && !string_at(s, current - 4, 2, &[b"ME", b"MA"])
                {
                    add!(b"", b"R");
                } else {
                    add!(b"R", b"R");
                }
                current += if get_at(s, current + 1) == b'R' { 2 } else { 1 };
            }
            b'S' => {
                if string_at(s, current - 1, 3, &[b"ISL", b"YSL"]) {
                    current += 1;
                } else if current == 0 && string_at(s, current, 5, &[b"SUGAR"]) {
                    add!(b"X", b"S");
                    current += 1;
                } else if string_at(s, current, 2, &[b"SH"]) {
                    if string_at(s, current + 1, 4, &[b"HEIM", b"HOEK", b"HOLM", b"HOLZ"]) {
                        add!(b"S", b"S");
                    } else {
                        add!(b"X", b"X");
                    }
                    current += 2;
                } else if string_at(s, current, 3, &[b"SIO", b"SIA"])
                    || string_at(s, current, 4, &[b"SIAN"])
                {
                    if !slavo_germanic(s) {
                        add!(b"S", b"X");
                    } else {
                        add!(b"S", b"S");
                    }
                    current += 3;
                } else if (current == 0 && string_at(s, current + 1, 1, &[b"M", b"N", b"L", b"W"]))
                    || string_at(s, current + 1, 1, &[b"Z"])
                {
                    add!(b"S", b"X");
                    current += if string_at(s, current + 1, 1, &[b"Z"]) { 2 } else { 1 };
                } else if string_at(s, current, 2, &[b"SC"]) {
                    if get_at(s, current + 2) == b'H' {
                        if string_at(s, current + 3, 2, &[b"OO", b"ER", b"EN", b"UY", b"ED", b"EM"])
                        {
                            if string_at(s, current + 3, 2, &[b"ER", b"EN"]) {
                                add!(b"X", b"SK");
                            } else {
                                add!(b"SK", b"SK");
                            }
                            current += 3;
                        } else {
                            if current == 0 && !is_vowel(s, 3) && get_at(s, 3) != b'W' {
                                add!(b"X", b"S");
                            } else {
                                add!(b"X", b"X");
                            }
                            current += 3;
                        }
                    } else if string_at(s, current + 2, 1, &[b"I", b"E", b"Y"]) {
                        add!(b"S", b"S");
                        current += 3;
                    } else {
                        add!(b"SK", b"SK");
                        current += 3;
                    }
                } else {
                    if current == last && string_at(s, current - 2, 2, &[b"AI", b"OI"]) {
                        add!(b"", b"S");
                    } else {
                        add!(b"S", b"S");
                    }
                    current += if string_at(s, current + 1, 1, &[b"S", b"Z"]) { 2 } else { 1 };
                }
            }
            b'T' => {
                if string_at(s, current, 4, &[b"TION"]) {
                    add!(b"X", b"X");
                    current += 3;
                } else if string_at(s, current, 3, &[b"TIA", b"TCH"]) {
                    add!(b"X", b"X");
                    current += 3;
                } else if string_at(s, current, 2, &[b"TH"]) || string_at(s, current, 3, &[b"TTH"])
                {
                    if string_at(s, current + 2, 2, &[b"OM", b"AM"])
                        || string_at(s, 0, 4, &[b"VAN ", b"VON "])
                        || string_at(s, 0, 3, &[b"SCH"])
                    {
                        add!(b"T", b"T");
                    } else {
                        add!(b"0", b"T");
                    }
                    current += 2;
                } else {
                    current += if string_at(s, current + 1, 1, &[b"T", b"D"]) { 2 } else { 1 };
                    add!(b"T", b"T");
                }
            }
            b'V' => {
                current += if get_at(s, current + 1) == b'V' { 2 } else { 1 };
                add!(b"F", b"F");
            }
            b'W' => {
                if string_at(s, current, 2, &[b"WR"]) {
                    add!(b"R", b"R");
                    current += 2;
                } else {
                    if current == 0
                        && (is_vowel(s, current + 1) || string_at(s, current, 2, &[b"WH"]))
                    {
                        if is_vowel(s, current + 1) {
                            add!(b"A", b"F");
                        } else {
                            add!(b"A", b"A");
                        }
                    }
                    if (current == last && is_vowel(s, current - 1))
                        || string_at(s, current - 1, 5, &[b"EWSKI", b"EWSKY", b"OWSKI", b"OWSKY"])
                        || string_at(s, 0, 3, &[b"SCH"])
                    {
                        add!(b"", b"F");
                        current += 1;
                    } else if string_at(s, current, 4, &[b"WICZ", b"WITZ"]) {
                        add!(b"TS", b"FX");
                        current += 4;
                    } else {
                        current += 1;
                    }
                }
            }
            b'X' => {
                if !(current == last
                    && (string_at(s, current - 3, 3, &[b"IAU", b"EAU"])
                        || string_at(s, current - 2, 2, &[b"AU", b"OU"])))
                {
                    add!(b"KS", b"KS");
                }
                current += if string_at(s, current + 1, 1, &[b"C", b"X"]) { 2 } else { 1 };
            }
            b'Z' => {
                if get_at(s, current + 1) == b'H' {
                    add!(b"J", b"J");
                    current += 2;
                } else {
                    if string_at(s, current + 1, 2, &[b"ZO", b"ZI", b"ZA"])
                        || (slavo_germanic(s) && current > 0 && get_at(s, current - 1) != b'T')
                    {
                        add!(b"S", b"TS");
                    } else {
                        add!(b"S", b"S");
                    }
                    current += if get_at(s, current + 1) == b'Z' { 2 } else { 1 };
                }
            }
            _ => {
                current += 1;
            }
        }
    }

    primary.truncate(4);
    secondary.truncate(4);
    (primary, secondary)
}
