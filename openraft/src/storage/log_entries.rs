//! Log entries kept together with whatever accounts for their memory.

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;

/// Log entries read from a [`RaftLogReader`], and optionally the owner of their memory: a
/// [`RaftTypeConfig::EntriesOwner`].
///
/// A storage implementation that accounts for the memory its reads allocate, for example with a
/// reservation, returns the entries together with what holds that account
/// ([`LogEntries::owned`]). Openraft keeps the two together wherever the entries go, whether
/// iterated, applied, or sent to a follower in an [`AppendEntriesRequest`], and drops the owner
/// only after the entries' buffer is freed. An implementation that accounts for nothing returns
/// its `Vec` with `.into()`.
///
/// A follower's core takes a received request's entries as a bare `Vec`
/// ([`LogEntries::into_parts`]). Entries decoded from the network have no owner.
///
/// A clone has no owner: the cloner owns the clone's memory.
///
/// [`RaftLogReader`]: crate::storage::RaftLogReader
/// [`RaftTypeConfig::EntriesOwner`]: crate::RaftTypeConfig::EntriesOwner
/// [`AppendEntriesRequest`]: crate::raft::AppendEntriesRequest
pub struct LogEntries<E, O = ()> {
    // Declared first, so dropped first: the buffer is freed before the owner is dropped.
    entries: Vec<E>,
    owner: Option<O>,
}

impl<E, O> LogEntries<E, O> {
    /// Entries whose memory nothing accounts for.
    pub fn new(entries: Vec<E>) -> Self {
        Self { entries, owner: None }
    }

    /// Entries whose memory `owner` accounts for until their buffer is freed.
    pub fn owned(entries: Vec<E>, owner: O) -> Self {
        Self {
            entries,
            owner: Some(owner),
        }
    }

    /// Keeps the first `len` entries, dropping the rest. The buffer and its owner are kept.
    pub fn truncate(&mut self, len: usize) {
        self.entries.truncate(len);
    }

    /// The entries and their owner, apart. The caller drops the owner only after the entries'
    /// buffer is freed, or takes over accounting for it.
    pub fn into_parts(self) -> (Vec<E>, Option<O>) {
        (self.entries, self.owner)
    }
}

impl<E, O> Default for LogEntries<E, O> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl<E, O> From<Vec<E>> for LogEntries<E, O> {
    fn from(entries: Vec<E>) -> Self {
        Self::new(entries)
    }
}

impl<E, O> FromIterator<E> for LogEntries<E, O> {
    fn from_iter<I: IntoIterator<Item = E>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl<E, O> Deref for LogEntries<E, O> {
    type Target = [E];

    fn deref(&self) -> &[E] {
        &self.entries
    }
}

impl<E, O> DerefMut for LogEntries<E, O> {
    fn deref_mut(&mut self) -> &mut [E] {
        &mut self.entries
    }
}

impl<E: Clone, O> Clone for LogEntries<E, O> {
    fn clone(&self) -> Self {
        Self::new(self.entries.clone())
    }
}

impl<E: fmt::Debug, O> fmt::Debug for LogEntries<E, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.entries.iter()).finish()
    }
}

impl<E, O> IntoIterator for LogEntries<E, O> {
    type Item = E;
    type IntoIter = LogEntriesIter<E, O>;

    fn into_iter(self) -> LogEntriesIter<E, O> {
        LogEntriesIter {
            entries: self.entries.into_iter(),
            _owner: self.owner,
        }
    }
}

impl<'a, E, O> IntoIterator for &'a LogEntries<E, O> {
    type Item = &'a E;
    type IntoIter = std::slice::Iter<'a, E>;

    fn into_iter(self) -> std::slice::Iter<'a, E> {
        self.entries.iter()
    }
}

/// The owning iterator of [`LogEntries`]: it keeps their owner until the entries' buffer is
/// freed, when the iterator is dropped, however many entries were taken.
pub struct LogEntriesIter<E, O = ()> {
    // Declared first, so dropped first: the buffer is freed before the owner is dropped.
    entries: std::vec::IntoIter<E>,
    _owner: Option<O>,
}

impl<E, O> Iterator for LogEntriesIter<E, O> {
    type Item = E;

    fn next(&mut self) -> Option<E> {
        self.entries.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl<E, O> DoubleEndedIterator for LogEntriesIter<E, O> {
    fn next_back(&mut self) -> Option<E> {
        self.entries.next_back()
    }
}

impl<E, O> ExactSizeIterator for LogEntriesIter<E, O> {}

/// Serialized as the bare sequence of entries: the owner is local.
#[cfg(feature = "serde")]
impl<E: serde::Serialize, O> serde::Serialize for LogEntries<E, O> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.entries, serializer)
    }
}

/// Deserialized from the bare sequence of entries, with no owner.
#[cfg(feature = "serde")]
impl<'de, E: serde::Deserialize<'de>, O> serde::Deserialize<'de> for LogEntries<E, O> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Vec<E> as serde::Deserialize<'de>>::deserialize(deserializer).map(Self::new)
    }
}
