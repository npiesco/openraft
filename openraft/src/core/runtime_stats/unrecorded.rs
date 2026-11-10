//! What the core records runtime statistics into without the `runtime-stats` feature, which has
//! no way to read them: nothing, so the core neither allocates for them nor records them.

use std::marker::PhantomData;

use crate::Config;
use crate::RaftTypeConfig;
use crate::core::NotificationName;
use crate::core::raft_msg::RaftMsgName;
use crate::core::stage::Stage;
use crate::engine::CommandName;
use crate::type_config::alias::InstantOf;

/// A statistic that is not recorded.
pub(crate) struct Discarded;

impl Discarded {
    /// Records nothing of `value`.
    pub(crate) fn record(&mut self, value: u64) {
        let _ = value;
    }
}

/// The statistics [`RuntimeStats`](super::RuntimeStats) records, none of them recorded.
pub(crate) struct Unrecorded<C> {
    pub(crate) apply_batch: Discarded,
    pub(crate) append_batch: Discarded,
    pub(crate) raft_msg_per_run: Discarded,
    pub(crate) write_batch: Discarded,
    pub(crate) raft_msg_budget: Discarded,
    pub(crate) notification_budget: Discarded,
    pub(crate) raft_msg_usage_permille: Discarded,
    pub(crate) notification_usage_permille: Discarded,
    _instants: PhantomData<fn() -> C>,
}

impl<C> Unrecorded<C>
where C: RaftTypeConfig
{
    /// Nothing recorded, whatever `config` asks to keep.
    pub(crate) fn new(config: &Config) -> Self {
        let _ = config;
        Self {
            apply_batch: Discarded,
            append_batch: Discarded,
            raft_msg_per_run: Discarded,
            write_batch: Discarded,
            raft_msg_budget: Discarded,
            notification_budget: Discarded,
            raft_msg_usage_permille: Discarded,
            notification_usage_permille: Discarded,
            _instants: PhantomData,
        }
    }

    pub(crate) fn record_command(&mut self, name: CommandName) {
        let _ = name;
    }

    pub(crate) fn record_raft_msg(&mut self, name: RaftMsgName) {
        let _ = name;
    }

    pub(crate) fn record_notification(&mut self, name: NotificationName) {
        let _ = name;
    }

    pub(crate) fn record_log_stage_now(&mut self, stage: Stage, index: u64) {
        let _ = (stage, index);
    }

    pub(crate) fn record_log_stage(&mut self, stage: Stage, index: u64, now: InstantOf<C>) {
        let _ = (stage, index, now);
    }
}
