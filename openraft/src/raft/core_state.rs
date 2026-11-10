use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::errors::Fatal;
use crate::errors::Infallible;
use crate::raft::raft_inner::RaftInner;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::WatchReceiverOf;

/// The channel that stops RaftCore, and the one that stops its tick.
pub(in crate::raft) type StopChannels<C> = (
    OneshotSenderOf<C, ()>,
    OneshotReceiverOf<C, ()>,
    (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
);

/// The RaftCore task's join handle.
pub(in crate::raft) type CoreJoinHandle<C> = JoinHandleOf<C, Result<Infallible, Fatal<C>>>;

/// What spawns a built node's RaftCore and its background tasks, once: given the node's handle,
/// the receiver of the channel that stops RaftCore, and the channel that stops its tick, both
/// made, their blocks funded, by the start before it is called.
pub(in crate::raft) trait StartCore<C>: OptionalSend
where C: RaftTypeConfig
{
    fn start(
        self: Box<Self>,
        inner: &RaftInner<C>,
        core_stop: OneshotReceiverOf<C, ()>,
        tick_stop: (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
    ) -> CoreJoinHandle<C>;
}

impl<C, F> StartCore<C> for F
where
    C: RaftTypeConfig,
    F: FnOnce(
            &RaftInner<C>,
            OneshotReceiverOf<C, ()>,
            (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
        ) -> CoreJoinHandle<C>
        + OptionalSend,
{
    fn start(
        self: Box<Self>,
        inner: &RaftInner<C>,
        core_stop: OneshotReceiverOf<C, ()>,
        tick_stop: (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
    ) -> CoreJoinHandle<C> {
        (*self)(inner, core_stop, tick_stop)
    }
}

/// The running state of RaftCore
pub(in crate::raft) enum CoreState<C>
where C: RaftTypeConfig
{
    /// The RaftCore and its background tasks have not been spawned.
    Unstarted(Option<Box<dyn StartCore<C>>>),

    /// The RaftCore task is still running.
    Running(CoreJoinHandle<C>),

    /// The RaftCore task is waiting for a signal to finish joining.
    Joining(WatchReceiverOf<C, bool>),

    /// The RaftCore task has finished. The return value of the task is stored.
    Done(Result<Infallible, Fatal<C>>),
}

impl<C> CoreState<C>
where C: RaftTypeConfig
{
    /// Returns `true` if the RaftCore task is still running.
    #[allow(dead_code)]
    pub(in crate::raft) fn is_running(&self) -> bool {
        matches!(self, CoreState::Running(_))
    }
}
