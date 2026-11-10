use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::Vote;
use crate::batch::Batch;
use crate::core::ServerState;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::TargetProgress;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::entry::RaftEntry;
use crate::progress::entry::ProgressEntry;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::proposer::funding::leader_state_bytes;
use crate::proposer::funding::targets_bytes;
use crate::proposer::funding::testing::Limit;
use crate::raft_state::IOId;
use crate::replication::payload::Payload;
use crate::replication::replicate::Replicate;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

fn m01() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(2, 1),
    );
    eng.state.server_state = ServerState::Candidate;
    eng.state
        .membership_state
        .set_effective(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01()));

    eng.output.take_commands();
    eng
}

#[test]
fn test_become_leader() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.vote_handler().become_leader();

    let leader = eng.leader.as_ref().unwrap();
    assert_eq!(leader.noop_log_id, log_id(2, 1, 0));
    assert_eq!(leader.last_log_id(), Some(&log_id(2, 1, 0)));
    assert_eq!(*leader.committed_vote_ref(), Vote::new(2, 1).to_committed());

    assert_eq!(ServerState::Leader, eng.state.server_state);

    assert_eq!(eng.output.take_commands(), vec![
        Command::UpdateIOProgress {
            when: None,
            io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), None)
        },
        Command::RebuildReplicationStreams {
            leader_vote: Vote::new(2, 1).to_committed(),
            targets: vec![TargetProgress {
                target: 0,
                progress: ProgressEntry::empty(0, StreamId::new(1), 0),
            }],
            close_old_streams: true,
            funding: None,
        },
        Command::AppendEntries {
            committed_vote: Vote::new(2, 1).to_committed(),
            entries: Batch::of([EntryOf::<UTConfig>::new_blank(log_id(2, 1, 0))]),
            owner: None,
        },
        // Pipeline mode: ProgressEntry::empty(0) has matching.next_index()=0 == searching_end=0
        Command::Replicate {
            target: 0,
            req: Replicate {
                inflight_id: InflightId::new(1),
                payload: Payload::LogsSince { prev: None },
            }
        }
    ]);

    Ok(())
}

#[test]
fn test_become_leader_waits_for_previous_io() {
    let mut eng = eng();
    let previous_vote = Vote::new(2, 1);
    let previous_io = IOId::new(&previous_vote);
    eng.state.accept_log_io(previous_io.clone());

    eng.vote_handler().become_leader();

    let leader_vote = previous_vote.to_committed();
    let leader_io = IOId::new_log_io(leader_vote, None);
    let first_command = eng.output.pop_command();
    let expected = Some(Command::UpdateIOProgress {
        when: Some(Condition::IOFlushed { io_id: previous_io }),
        io_id: leader_io,
    });
    assert_eq!(first_command, expected);
}

#[test]
fn test_become_leader_holds_its_charge_until_it_is_freed() {
    let limit = Limit::new(usize::MAX);
    let mut eng = eng();
    eng.config.funding = limit.funding();

    eng.vote_handler().become_leader();
    assert!(eng.leader.is_some());
    // The log id list's room for the Leader's first log id, charged before the list grows, is
    // held by the engine with the list's buffer, not by the Leader.
    let log_ids = eng.state.log_ids.kept_bytes();
    assert_eq!(leader_state_bytes::<UTConfig>(&m01()) + log_ids, limit.held());

    eng.leader = None;
    let commands = eng.output.take_commands();
    let targets: usize = commands
        .iter()
        .map(|command| match command {
            Command::RebuildReplicationStreams { targets, .. } => targets.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(
        targets_bytes::<UTConfig>(targets) + log_ids,
        limit.held(),
        "the targets queued for the streams hold their part until they are freed, and the list its room"
    );

    drop(commands);
    drop(eng);
    assert_eq!(0, limit.held());
}

#[test]
fn test_become_leader_refused_its_funding_demotes_the_vote() {
    let limit = Limit::new(0);
    let mut eng = eng();
    eng.config.funding = limit.funding();

    eng.vote_handler().become_leader();

    assert!(eng.leader.is_none());
    assert_eq!(
        Vote::new(2, 1),
        *eng.state.vote_ref(),
        "demoted in memory, not committed"
    );
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(eng.output.take_commands(), vec![Command::FailPendingReads]);
    assert_eq!(0, limit.held());
}
