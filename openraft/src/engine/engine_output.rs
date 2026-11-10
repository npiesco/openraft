use std::collections::VecDeque;

use openraft_macros::since;

use crate::RaftTypeConfig;
use crate::async_runtime::Charge;
use crate::config::Config;
use crate::engine::Command;
use crate::engine::command_scheduler::CommandScheduler;
use crate::engine::pending_responds::PendingResponds;
use crate::engine::respond_command::PendingRespond;
use crate::errors::Unfunded;
use crate::proposer::funding::Funding;
use crate::raft_state::IOId;
use crate::storage::RaftStateMachine;
use crate::type_config::alias::LogIdOf;

/// The fixed few commands one event queues beside those it queues per target.
const EVENT_COMMANDS: usize = 32;

/// At most how many commands the Engine queues for one event, a message, a notification or the
/// routine actions after them, on a node whose membership has `targets` nodes: a fixed few, and
/// a replication command per target, twice.
pub(crate) const fn event_commands(targets: usize) -> usize {
    EVENT_COMMANDS + 2 * targets
}

/// What a node run with a funder is charged for its Engine's queues to keep taking events, with
/// memberships of at most `members` nodes and at most `responds` answers waiting on IO at once:
/// the command queue grown to room for two events' commands, and each of the four queues of
/// answers waiting on IO grown to room for all `responds`, each while the buffer it grew from,
/// up to half its size, is still held. Each queue's room is charged before it is reserved, and
/// credited back only once the buffer is freed. Refused room for an event, the event is left in
/// its channel, a batch of events is cut short there, and the event is taken once room is
/// charged, tried again every tick; refused room for an answer, the answer waits at the head of
/// the command queue until its IO is done or room is charged.
#[since(version = "0.10.0")]
pub const fn engine_queue_bytes<C, SM>(members: usize, responds: usize) -> usize
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    let commands = 3 * 2 * event_commands(members) * size_of::<Command<C, SM>>();
    let on_io = size_of::<PendingRespond<C, IOId<C>>>();
    let on_log = size_of::<PendingRespond<C, LogIdOf<C>>>();
    let respond = if on_io > on_log { on_io } else { on_log };
    commands + 4 * 3 * responds * respond
}

/// The entry of output from Engine to the runtime.
#[derive(Debug)]
pub(crate) struct EngineOutput<C, SM = ()>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Command queue that needs to be executed by `RaftRuntime`.
    pub(crate) commands: VecDeque<Command<C, SM>>,

    /// Pending responds waiting for IO conditions to be met before sending.
    pub(crate) pending_responds: PendingResponds<C>,

    /// The charge for the capacity of `commands`, taken before it was reserved; declared after
    /// it, so it is credited back only once that buffer is freed.
    commands_funding: Option<Charge>,
}

impl<C, SM> Default for EngineOutput<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    fn default() -> Self {
        Self {
            commands: VecDeque::new(),
            pending_responds: PendingResponds::default(),
            commands_funding: None,
        }
    }
}

impl<C, SM> EngineOutput<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Room for `room` more commands, its capacity charged to `funding` before it is reserved:
    /// refused as [`Unfunded`], nothing is allocated or charged. See [`Funding::reserve`].
    pub(crate) fn reserve(&mut self, funding: &Funding, room: usize) -> Result<(), Unfunded> {
        funding.reserve(&mut self.commands, &mut self.commands_funding, room)
    }

    /// Gives back the charge of the slots of the command queue, and of the queues of responds
    /// waiting on a condition, once few of them are taken, as [`Funding::shrink`] does.
    pub(crate) fn shrink(&mut self, funding: &Funding) {
        funding.shrink(&mut self.commands, &mut self.commands_funding);
        self.pending_responds.shrink(funding);
    }

    pub(crate) fn len(&self) -> usize {
        self.commands.len()
    }

    /// Push a command to the queue.
    pub(crate) fn push_command(&mut self, cmd: Command<C, SM>) {
        tracing::debug!("push command: {:?}", cmd);
        self.commands.push_back(cmd)
    }

    pub(crate) fn prepend_command(&mut self, cmd: Command<C, SM>) {
        tracing::debug!("push front command: {:?}", cmd);
        self.commands.push_front(cmd)
    }

    /// Put the command to the head of the queue or to a separate pending queue.
    ///
    /// This will be used when the command is not ready to be executed.
    ///
    /// Returns Ok if the cmd is put to a pending queue, means it is not put back, and other
    /// commands in the main queue can still be processed.
    ///
    /// A respond's room in its pending queue is charged to `funding` before it is reserved:
    /// refused, the respond is put back to the front of the command queue, into the slot it was
    /// popped from, as any other command is.
    pub(crate) fn postpone_command(&mut self, cmd: Command<C, SM>, funding: &Funding) -> Result<(), &'static str> {
        tracing::debug!("postpone command: {:?}", cmd);

        // For Respond command, put them to separate queue in order not to block other commands.
        // For other commands, put them back to the front of the command queue.
        let cmd = match cmd {
            Command::Respond { when, resp } => match self.pending_responds.push(when, resp, funding) {
                Ok(()) => return Ok(()),
                Err((when, resp)) => Command::Respond { when: Some(when), resp },
            },
            cmd => cmd,
        };
        self.commands.push_front(cmd);
        Err("Put back to the front of command queue")
    }

    /// Pop the first command to run from the queue.
    pub(crate) fn pop_command(&mut self) -> Option<Command<C, SM>> {
        self.commands.pop_front()
    }

    /// Iterate all queued commands.
    pub(crate) fn iter_commands(&self) -> impl Iterator<Item = &Command<C, SM>> {
        self.commands.iter()
    }

    /// Take all queued commands and clear the queue.
    #[cfg(test)]
    pub(crate) fn take_commands(&mut self) -> Vec<Command<C, SM>> {
        self.commands.drain(..).collect()
    }

    /// Clear all queued commands.
    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn clear_commands(&mut self) {
        self.commands.clear()
    }

    /// Reorganize commands for better I/O performance.
    ///
    /// Currently performs:
    /// - Merging consecutive `AppendEntries` commands from the same leader into one
    ///
    /// The batch size is limited by `config.max_in_batch_entries` to avoid high latency
    /// from writing very large batches to storage. A value of 0 means unlimited batching.
    pub(crate) fn sched_commands(&mut self, config: &Config) {
        let mut scheduler = CommandScheduler::new(config, self);
        scheduler.merge_front_append_entries();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Vote;
    use crate::engine::Condition;
    use crate::engine::Respond;
    use crate::engine::testing::UTConfig;
    use crate::engine::testing::log_id;
    use crate::proposer::funding::KEPT_SLOTS;
    use crate::proposer::funding::testing::Limit;
    use crate::raft::VoteResponse;
    use crate::type_config::TypeConfigExt;

    type C = UTConfig;

    #[test]
    fn test_command_room_is_charged_before_it_is_reserved() {
        let room = event_commands(3);
        let bytes = room * size_of::<Command<C>>();

        let short = Limit::new(bytes - 1);
        let mut output: EngineOutput<C> = EngineOutput::default();
        assert_eq!(Err(Unfunded), output.reserve(&short.funding(), room));
        assert_eq!(0, output.commands.capacity(), "refused, nothing is allocated");
        assert_eq!(0, short.held(), "refused, nothing is charged");

        let enough = Limit::new(bytes);
        output.reserve(&enough.funding(), room).unwrap();
        assert_eq!(room, output.commands.capacity());
        assert_eq!(bytes, enough.held());

        output.reserve(&enough.funding(), room).unwrap();
        assert_eq!(bytes, enough.held(), "room already reserved is not charged again");

        drop(output);
        assert_eq!(0, enough.held(), "credited back once the buffer is freed");
    }

    /// Its commands run, the queue gives back the charge of the slots they emptied: the smaller
    /// buffer is charged before it replaces the larger, and refused, the queue is left as it is.
    #[test]
    fn test_shrink_gives_back_the_charge_of_emptied_command_slots() {
        let room = 4 * KEPT_SLOTS;
        let slot = size_of::<Command<C>>();

        let full = Limit::new(room * slot);
        let mut output: EngineOutput<C> = EngineOutput::default();
        output.reserve(&full.funding(), room).unwrap();
        output.shrink(&full.funding());
        assert_eq!(
            room,
            output.commands.capacity(),
            "the smaller buffer refused, it is kept"
        );
        assert_eq!(room * slot, full.held());

        let limit = Limit::new(usize::MAX);
        let mut output: EngineOutput<C> = EngineOutput::default();
        output.reserve(&limit.funding(), room).unwrap();
        output.shrink(&limit.funding());
        assert_eq!(KEPT_SLOTS, output.commands.capacity());
        assert_eq!(KEPT_SLOTS * slot, limit.held(), "only the slots it keeps are charged");

        drop(output);
        assert_eq!(0, limit.held());
    }

    #[test]
    fn test_a_respond_refused_its_room_waits_in_front_of_the_command_queue() {
        let (tx, _rx) = UTConfig::<()>::oneshot();
        let resp = Respond::new(VoteResponse::new(Vote::new(1, 1), None, false), tx);
        let when = Some(Condition::Applied {
            log_id: log_id(1, 1, 3),
        });
        let bytes = size_of::<PendingRespond<C, LogIdOf<C>>>();

        let commands = Limit::new(usize::MAX);
        let mut output: EngineOutput<C> = EngineOutput::default();
        output.reserve(&commands.funding(), 1).unwrap();

        let short = Limit::new(bytes - 1);
        let refused = output.postpone_command(Command::Respond { when, resp }, &short.funding());
        assert!(refused.is_err());
        assert_eq!(1, output.len(), "put back to the front of the command queue");
        assert_eq!(
            0,
            output.pending_responds.on_apply.capacity(),
            "refused, nothing is allocated"
        );
        assert_eq!(0, short.held(), "refused, nothing is charged");

        let enough = Limit::new(bytes);
        let cmd = output.pop_command().unwrap();
        output.postpone_command(cmd, &enough.funding()).unwrap();
        assert_eq!(0, output.len());
        assert_eq!(1, output.pending_responds.on_apply.len());
        assert_eq!(bytes, enough.held());

        drop(output);
        assert_eq!(0, enough.held());
        assert_eq!(0, commands.held());
    }
}
