//! What [`Raft::with_raft_state`] and [`Raft::with_state_machine`] box to run a caller's
//! function on the core or in the state machine worker, each of a type sized before it is made:
//! so the boxes are charged to the node's funder before anything is allocated.
//!
//! [`Raft::with_raft_state`]: crate::Raft::with_raft_state
//! [`Raft::with_state_machine`]: crate::Raft::with_state_machine

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::OptionalSend;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::async_runtime::OneshotSender;
use crate::base::BoxFuture;

/// The size of what `make` returns: known from its type, before anything it is made from exists.
pub(super) fn size_of_output<A, B, R>(_make: fn(A, B) -> R) -> usize {
    size_of::<R>()
}

/// What the core runs for [`Raft::with_raft_state`]: `func`, its answer sent on `tx`.
///
/// [`Raft::with_raft_state`]: crate::Raft::with_raft_state
pub(super) fn state_reply<C, F, V, Tx>(func: F, tx: Tx) -> impl FnOnce(&RaftState<C>) + OptionalSend + 'static
where
    C: RaftTypeConfig,
    F: FnOnce(&RaftState<C>) -> V + OptionalSend + 'static,
    V: OptionalSend + 'static,
    Tx: OneshotSender<V> + 'static,
{
    move |state| {
        if tx.send(func(state)).is_err() {
            tracing::error!("with_raft_state: the caller is gone");
        }
    }
}

/// What the state machine worker runs for [`Raft::with_state_machine`]: `func`, its answer sent
/// on `tx` by a [`Reply`].
///
/// [`Raft::with_state_machine`]: crate::Raft::with_state_machine
pub(super) fn sm_reply<SM, F, V, Tx>(func: F, tx: Tx) -> impl FnOnce(&mut SM) -> BoxFuture<()> + OptionalSend + 'static
where
    SM: 'static,
    F: FnOnce(&mut SM) -> BoxFuture<V> + OptionalSend + 'static,
    V: OptionalSend + 'static,
    Tx: OneshotSender<V> + 'static,
{
    move |sm| {
        let reply: BoxFuture<'_, ()> = Box::pin(Reply {
            answer: func(sm),
            tx: Some(tx),
        });
        reply
    }
}

/// A state machine call's future, its answer sent on `tx` once ready: a named type, so its box is
/// sized before it is made.
pub(super) struct Reply<'a, V, Tx> {
    answer: BoxFuture<'a, V>,
    tx: Option<Tx>,
}

// No field is pinned in place: `answer` is pinned in its own box, and `tx` is moved out to send.
impl<V, Tx> Unpin for Reply<'_, V, Tx> {}

impl<V, Tx> Future for Reply<'_, V, Tx>
where
    V: OptionalSend,
    Tx: OneshotSender<V>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let answer = std::task::ready!(self.answer.as_mut().poll(cx));
        if let Some(tx) = self.tx.take()
            && tx.send(answer).is_err()
        {
            tracing::error!("with_state_machine: the caller is gone");
        }
        Poll::Ready(())
    }
}
