//! geqo_copy.c — copies one gene string to another.

use super::Chromosome;

pub(super) fn geqo_copy(chromo1: &mut Chromosome, chromo2: &Chromosome, string_length: i32) {
    for i in 0..string_length as usize {
        chromo1.string[i] = chromo2.string[i];
    }
    chromo1.worth = chromo2.worth;
}
