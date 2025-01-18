use std::collections::BTreeMap;

#[derive(Default)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|v| *v += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|v| *v -= 1);
        if self.readers.get(&ts) == Some(&0) {
            self.readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().next().cloned()
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.keys().len()
    }
}
