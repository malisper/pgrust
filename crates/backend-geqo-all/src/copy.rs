//! `geqo_copy.c` — copies one gene to another.

use crate::Chromosome;

/// `geqo_copy(root, chromo1, chromo2, string_length)` — copy `chromo2`'s tour
/// (first `string_length` genes) and worth into `chromo1`.
pub fn geqo_copy(chromo1: &mut Chromosome, chromo2: &Chromosome, string_length: i32) {
    for i in 0..string_length as usize {
        chromo1.string[i] = chromo2.string[i];
    }
    chromo1.worth = chromo2.worth;
}
