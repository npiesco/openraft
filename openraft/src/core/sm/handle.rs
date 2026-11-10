//! State machine control handle

use crate::RaftTypeConfig;
use crate::async_runtime::MpscSender;
use crate::async_runtime::MpscWeakSender;
use crate::async_runtime::SendError;
use crate::core::sm;
use crate::proposer::funding::Funding;
use crate::storage::RaftStateMachine;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::MpscWeakSenderOf;
use crate::type_config::alias::SmSnapshotOf;

/// State machine worker handle for sending command to it.
pub(crate) struct Handle<C, SM = ()>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    pub(in crate::core::sm) cmd_tx: MpscSenderOf<C, sm::Command<C, SM>>,

    #[allow(dead_code)]
    pub(in crate::core::sm) join_handle: JoinHandleOf<C, ()>,
}

impl<C, SM> Handle<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    pub(crate) async fn send(&mut self, cmd: sm::Command<C, SM>) -> Result<(), SendError<sm::Command<C, SM>>> {
        tracing::debug!("sending command to state machine worker: {:?}", cmd);
        self.cmd_tx.send(cmd).await
    }

    /// Create a [`SnapshotReader`] to get the current snapshot from the state machine, the
    /// channel it waits on for the snapshot charged to `funding`.
    pub(crate) fn new_snapshot_reader(&self, funding: Funding) -> SnapshotReader<C, SM> {
        SnapshotReader {
            cmd_tx: self.cmd_tx.downgrade(),
            funding,
        }
    }
}

/// A handle for retrieving a snapshot from the state machine.
pub(crate) struct SnapshotReader<C, SM = ()>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Weak command sender to the state machine worker.
    ///
    /// It is weak because the [`Worker`] watches the close event of this channel for shutdown.
    ///
    /// [`Worker`]: sm::worker::Worker
    cmd_tx: MpscWeakSenderOf<C, sm::Command<C, SM>>,

    /// What the channel each read waits on is charged to before it is made.
    funding: Funding,
}

impl<C, SM> SnapshotReader<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Get a snapshot from the state machine.
    ///
    /// If the state machine worker has shutdown, or the channel the read waits on is refused its
    /// charge (nothing made), it will return an error.
    /// If there is no snapshot available, it will return `Ok(None)`.
    pub(crate) async fn get_snapshot(&self) -> Result<Option<SmSnapshotOf<C, SM>>, &'static str> {
        let Ok((tx, rx)) = self.funding.oneshot::<C, _>() else {
            tracing::warn!("the channel to read the snapshot on is unfunded");
            return Err("the channel to read the snapshot on is unfunded");
        };

        let cmd = sm::Command::get_snapshot(tx);
        tracing::debug!("SnapshotReader sending command to sm::Worker: {:?}", cmd);

        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            tracing::info!("failed to upgrade cmd_tx, sm::Worker may have shutdown");
            return Err("failed to upgrade cmd_tx, sm::Worker may have shutdown");
        };

        // If fail to send command, cmd is dropped and tx will be dropped.
        cmd_tx.send(cmd).await.ok();

        let snapshot = match rx.await {
            Ok(x) => x,
            Err(_e) => {
                tracing::error!("failed to receive snapshot, sm::Worker may have shutdown");
                return Err("failed to receive snapshot, sm::Worker may have shutdown");
            }
        };

        Ok(snapshot)
    }
}
