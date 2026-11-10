use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::async_runtime::Charge;
use crate::errors::Fatal;
use crate::errors::Infallible;
use crate::errors::Unfunded;
use crate::raft::raft_inner::RaftInner;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;

/// The channel that stops RaftCore, and the one that stops its tick.
pub(in crate::raft) type StopChannels<C> = (
    OneshotSenderOf<C, ()>,
    OneshotReceiverOf<C, ()>,
    (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
);

/// The RaftCore task's join handle.
pub(in crate::raft) type CoreJoinHandle<C> = JoinHandleOf<C, Result<Infallible, Fatal<C>>>;

/// The channel that tells whoever waits for the RaftCore task to be joined that it has been:
/// made, its block funded, when the node starts.
pub(in crate::raft) type JoinWatch<C> = (WatchSenderOf<C, bool>, WatchReceiverOf<C, bool>);

/// What spawns a built node's RaftCore and its background tasks, once: given the node's handle,
/// the receiver of the channel that stops RaftCore, and the channel that stops its tick, both
/// made, their blocks funded, by the start before it is called. Each task is made from the share
/// of the node's charge it holds for it: refused as [`Unfunded`], what it holds freed and nothing
/// spawned, if the runtime cannot make one.
pub(in crate::raft) trait StartCore<C>: OptionalSend
where C: RaftTypeConfig
{
    fn start(
        self: Box<Self>,
        inner: &RaftInner<C>,
        core_stop: OneshotReceiverOf<C, ()>,
        tick_stop: (OneshotSenderOf<C, ()>, OneshotReceiverOf<C, ()>),
    ) -> Result<(CoreJoinHandle<C>, JoinWatch<C>), Unfunded>;
}

/// A built node's start, and the charge for the box it is held in, credited back after the box
/// is freed when it starts or the node is dropped unstarted; uncharged on a node made without a
/// funder.
pub(in crate::raft) struct Unstarted<C>
where C: RaftTypeConfig
{
    pub(in crate::raft) start: Box<dyn StartCore<C>>,
    pub(in crate::raft) charge: Option<Charge>,
}

/// The running state of RaftCore
pub(in crate::raft) enum CoreState<C>
where C: RaftTypeConfig
{
    /// The RaftCore and its background tasks have not been spawned.
    Unstarted(Option<Unstarted<C>>),

    /// The RaftCore task is still running, and the channel that tells it has been joined.
    Running(CoreJoinHandle<C>, JoinWatch<C>),

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
        matches!(self, CoreState::Running(..))
    }
}
