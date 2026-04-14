use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub(crate) struct AnalyzeRng {
    state: u64,
}

impl AnalyzeRng {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn gen_range_u64(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockSampler {
    chosen_blocks: Vec<u32>,
    next_index: usize,
}

impl BlockSampler {
    pub(crate) fn new(nblocks: u32, target_blocks: u32, rng: &mut AnalyzeRng) -> Self {
        let target_blocks = target_blocks.min(nblocks);
        if target_blocks == 0 {
            return Self {
                chosen_blocks: Vec::new(),
                next_index: 0,
            };
        }

        let mut chosen = BTreeSet::new();
        while chosen.len() < target_blocks as usize {
            chosen.insert(rng.gen_range_u64(nblocks as u64) as u32);
        }

        Self {
            chosen_blocks: chosen.into_iter().collect(),
            next_index: 0,
        }
    }
}

impl Iterator for BlockSampler {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.chosen_blocks.get(self.next_index).copied()?;
        self.next_index += 1;
        Some(block)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReservoirSampler<T> {
    target_size: usize,
    seen: usize,
    sample: Vec<T>,
}

impl<T> ReservoirSampler<T> {
    pub(crate) fn new(target_size: usize) -> Self {
        Self {
            target_size,
            seen: 0,
            sample: Vec::with_capacity(target_size),
        }
    }

    pub(crate) fn push(&mut self, value: T, rng: &mut AnalyzeRng) {
        self.seen += 1;
        if self.target_size == 0 {
            return;
        }
        if self.sample.len() < self.target_size {
            self.sample.push(value);
            return;
        }
        let replacement = rng.gen_range_u64(self.seen as u64) as usize;
        if replacement < self.target_size {
            self.sample[replacement] = value;
        }
    }

    pub(crate) fn into_inner(self) -> Vec<T> {
        self.sample
    }

    pub(crate) fn seen(&self) -> usize {
        self.seen
    }
}

pub(crate) fn target_sample_rows(statistics_target: i16) -> usize {
    let target = if statistics_target <= 0 {
        100usize
    } else {
        statistics_target as usize
    };
    target.saturating_mul(300)
}

pub(crate) fn target_sample_blocks(
    nblocks: u32,
    sample_rows: usize,
    estimated_rows_per_block: usize,
) -> u32 {
    if nblocks == 0 || sample_rows == 0 {
        return 0;
    }
    let rows_per_block = estimated_rows_per_block.max(1);
    let estimated_blocks = sample_rows.div_ceil(rows_per_block) as u32;
    estimated_blocks.clamp(1, nblocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_sampler_returns_unique_sorted_blocks() {
        let mut rng = AnalyzeRng::new(123);
        let blocks = BlockSampler::new(20, 5, &mut rng).collect::<Vec<_>>();
        assert_eq!(blocks.len(), 5);
        assert_eq!(blocks, {
            let mut sorted = blocks.clone();
            sorted.sort_unstable();
            sorted.dedup();
            sorted
        });
    }

    #[test]
    fn reservoir_sampler_keeps_requested_size() {
        let mut rng = AnalyzeRng::new(42);
        let mut sampler = ReservoirSampler::new(10);
        for i in 0..1000 {
            sampler.push(i, &mut rng);
        }
        assert_eq!(sampler.seen(), 1000);
        assert_eq!(sampler.into_inner().len(), 10);
    }

    #[test]
    fn target_sample_rows_matches_postgres_shape() {
        assert_eq!(target_sample_rows(1), 300);
        assert_eq!(target_sample_rows(100), 30_000);
        assert_eq!(target_sample_rows(-1), 30_000);
    }

    #[test]
    fn target_sample_blocks_clamps_to_relation_size() {
        assert_eq!(target_sample_blocks(0, 1000, 10), 0);
        assert_eq!(target_sample_blocks(5, 1000, 200), 5);
        assert_eq!(target_sample_blocks(100, 3000, 100), 30);
    }
}
