use anyhow::Result;

use super::StorageIterator;

#[derive(Debug)]
enum IteratorState {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    state: IteratorState,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            state: IteratorState::A,
        };
        iter.state = iter.infer_state();
        Ok(iter)
    }

    fn infer_state(&self) -> IteratorState {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) if self.a.key() <= self.b.key() => IteratorState::A,
            (true, true) => IteratorState::B,
            (_, false) => IteratorState::A,
            (false, _) => IteratorState::B,
        }
    }

    /// Skip all the keys in b that equal the key in a
    fn skip_duplicated_keys(&mut self) -> Result<()> {
        // do not try to skip `b` if current iterator is not `a`, to avoid skipping valid keys.
        while matches!(self.state, IteratorState::A)
            && self.a.is_valid()
            && self.b.is_valid()
            && self.b.key() <= self.a.key()
        {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.state {
            IteratorState::A => self.a.key(),
            IteratorState::B => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.state {
            IteratorState::A => self.a.value(),
            IteratorState::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.skip_duplicated_keys()?;
        let _ = match self.state {
            IteratorState::A => self.a.next(),
            IteratorState::B => self.b.next(),
        };
        self.state = self.infer_state();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
