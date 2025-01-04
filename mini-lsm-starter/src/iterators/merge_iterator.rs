use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Context, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            // Filter out invalid iterator.
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(size, iter)| HeapWrapper(size, iter))
            .collect();

        let current = iters.pop();

        MergeIterator { iters, current }
    }

    /// Fetch an iterator with smallest key into `self.current`.
    fn fetch(&mut self) {
        while let Some(iter) = self.iters.pop() {
            if iter.1.is_valid() {
                self.current = Some(iter);
                break;
            }
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(|iter| iter.1.key())
            .with_context(|| "calling `key()` on invalid merge iterator")
            .unwrap()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|iter| iter.1.value())
            .with_context(|| "calling `value()` on invalid merge iterator")
            .unwrap()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    /// Advance to next state.
    /// Stop if any inner iterator's `next()` return error.
    ///
    /// Here are the steps:
    /// - Remove all the keys inside the heap that equals the smallest key.
    /// - Calling next on `self.current` to discard the smallest key.
    /// - Push back `self.current` into the heap and get the smallest iterator out.
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let mut current = self
            .current
            .take()
            .with_context(|| "calling `next()` on invalid merge iterator")?;
        let smallest_key = current.1.key();

        // Advance until all iterators only contain keys that greater than previous key.
        while let Some(mut iter) = self.iters.pop() {
            // Skip invalid iterator
            if !iter.1.is_valid() {
                continue;
            }

            // The smallest key inside the heap is greater than `smallest_key`, can stop.
            if iter.1.key() > smallest_key {
                self.iters.push(iter);
                break;
            }

            // Advance the pointer and push back iterator if it is valid.
            iter.1.next()?;
            if iter.1.is_valid() {
                self.iters.push(iter);
            }
        }

        // Advance `current` to drop `smallest_key`, and push it back to the heap.
        current.1.next()?;
        if current.1.is_valid() {
            self.iters.push(current);
        }

        // fetch the next iterator out to `self.current`
        self.fetch();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let current_count = if self.current.is_some() { 1 } else { 0 };
        self.iters.len() + current_count
    }
}
