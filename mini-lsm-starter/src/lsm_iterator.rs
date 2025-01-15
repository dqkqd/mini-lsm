use std::ops::Bound;

use anyhow::{bail, Result};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeyBytes, KeySlice},
    mem_table::{map_bound, MemTableIterator},
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    // memtables and L0
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    // L1 sst
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    lower: Bound<KeyBytes>,
    upper: Bound<KeyBytes>,
    prev_key: Option<KeyBytes>,
    finished: bool,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            lower: map_bound(lower),
            upper: map_bound(upper),
            prev_key: None,
            finished: false,
        };
        iter.skip_lower_keys()?;
        iter.skip_deleted_keys()?;
        iter.check_finished();

        Ok(iter)
    }

    fn skip_deleted_keys(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            // TODO: is this correct?
            self.next()?;
        }
        Ok(())
    }

    fn skip_lower_keys(&mut self) -> Result<()> {
        while self.is_valid() {
            match &self.lower {
                Bound::Included(key) if self.key() < key.key_ref() => self.inner.next()?,
                Bound::Excluded(key) if self.key() <= key.key_ref() => self.inner.next()?,
                _ => break,
            }
        }
        Ok(())
    }

    fn skip_equal_keys(&mut self) -> Result<()> {
        if let Some(prev_key) = self.prev_key.as_ref() {
            while self.is_valid() && prev_key.key_ref() == self.key() {
                self.inner.next()?;
            }
        }
        Ok(())
    }

    fn check_finished(&mut self) -> bool {
        if self.finished || !self.inner.is_valid() {
            self.finished = true;
        } else {
            self.finished = match &self.upper {
                Bound::Included(key) if self.key() > key.key_ref() => true,
                Bound::Excluded(key) if self.key() >= key.key_ref() => true,
                _ => false,
            };
        }
        self.finished
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.finished && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.prev_key
                .replace(self.inner.key().to_key_vec().into_key_bytes());
        }
        self.inner.next()?;
        self.skip_lower_keys()?;
        self.skip_equal_keys()?;
        self.skip_deleted_keys()?;
        self.check_finished();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("calling `next()` on an errored iterator.")
        }

        if let Err(e) = self.iter.next() {
            self.has_errored = true;
            return Err(e);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
