use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::MembershipState;
use crate::Vote;
use crate::batch::Batch;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::proposer::funding::testing::Limit;
use crate::raft_state::IOId;
use crate::raft_state::LogStateReader;
use crate::storage::LogEntries;
use crate::testing::blank_ent;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::StoredMembershipOf;
use crate::type_config::alias::VoteOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

fn m01() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn m23() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {2,3}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng: Engine<UTConfig> = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 2;
    let vote = VoteOf::<UTConfig>::new_committed(2, 1);
    let now = UTConfig::<()>::now();
    eng.state.vote.update(now, Duration::from_millis(500), vote);
    eng.state.log_ids.append(log_id(1, 1, 1));
    eng.state.log_ids.append(log_id(2, 1, 3));
    eng.state.membership_state = MembershipState::new(
        StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01()),
        StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23()),
    );
    eng.state.server_state = eng.calc_server_state();
    eng
}

#[test]
fn test_follower_append_entries_update_accepted() -> anyhow::Result<()> {
    let mut eng = eng();

    eng.output.clear_commands();

    eng.following_handler().append_entries(Some(log_id(2, 1, 3)), vec![
        //
        blank_ent::<UTConfig>(3, 1, 4),
        blank_ent::<UTConfig>(3, 1, 5),
    ]);

    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 1), //
            log_id(2, 1, 3),
            log_id(3, 1, 5),
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(
        Some(&IOId::new_log_io(Vote::new(2, 1).to_committed(), Some(log_id(3, 1, 5)))),
        eng.state.accepted_log_io()
    );
    assert_eq!(eng.output.take_commands(), vec![
        //
        Command::AppendEntries {
            committed_vote: Vote::new(2, 1).to_committed(),
            entries: Batch::of([blank_ent::<UTConfig>(3, 1, 4), blank_ent::<UTConfig>(3, 1, 5)]),
            owner: None,
        }
    ]);

    // Update to a new Leader and smaller log id
    {
        // Assume this node's Leader becomes T3-N1
        eng.state.vote = Leased::new(
            UTConfig::<()>::now(),
            Duration::from_millis(500),
            Vote::new_committed(3, 1),
        );
        eng.following_handler().append_entries(Some(log_id(2, 1, 3)), vec![
            //
            blank_ent::<UTConfig>(3, 1, 4),
        ]);
        assert_eq!(Some(&log_id(3, 1, 5)), eng.state.last_log_id());
        assert_eq!(
            Some(&IOId::new_log_io(Vote::new(3, 1).to_committed(), Some(log_id(3, 1, 4)))),
            eng.state.accepted_log_io()
        );
        assert_eq!(eng.output.take_commands(), vec![
            //
            Command::UpdateIOProgress {
                when: Some(Condition::IOFlushed {
                    io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), Some(log_id(3, 1, 5)))
                }),
                io_id: IOId::new_log_io(Vote::new(3, 1).to_committed(), Some(log_id(3, 1, 4))),
            }
        ]);
    }

    // Update to a smaller value is ignored.
    {
        eng.following_handler().append_entries(Some(log_id(2, 1, 3)), vec![]);
        assert_eq!(Some(&log_id(3, 1, 5)), eng.state.last_log_id());
        assert_eq!(
            Some(&IOId::new_log_io(Vote::new(3, 1).to_committed(), Some(log_id(3, 1, 4)))),
            eng.state.accepted_log_io()
        );

        assert_eq!(eng.output.take_commands(), vec![
            //
        ]);
    }

    Ok(())
}

/// An owned segment's owner travels with the entries the follower keeps, the one already present
/// dropped in place, into the command that appends them: the owner is dropped only after the
/// storage took the entries.
#[test]
fn test_follower_append_entries_keeps_the_owner_until_appended() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.output.clear_commands();

    eng.following_handler().append_entries(
        Some(log_id(1, 1, 2)),
        LogEntries::owned(vec![blank_ent::<UTConfig>(2, 1, 3), blank_ent::<UTConfig>(3, 1, 4)], ()),
    );
    eng.following_handler().append_entries(Some(log_id(3, 1, 4)), vec![blank_ent::<UTConfig>(3, 1, 5)]);

    let commands = eng.output.take_commands();
    let owners = commands
        .iter()
        .filter_map(|command| match command {
            Command::AppendEntries { entries, owner, .. } => Some((entries.len(), owner.is_some())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(vec![(1, true), (1, false)], owners);

    Ok(())
}

/// Room in the log id list for the leaders a follower's new entries add is charged before the
/// list grows. Refused, only the leading entries the room it has holds are taken, nothing is
/// charged, and the last one taken is returned for the Leader to send the rest after.
#[test]
fn test_follower_append_entries_takes_only_what_the_log_id_list_has_room_for() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(None, [log_id(1, 1, 1), log_id(2, 1, 3)]);
    assert_eq!(0, eng.state.log_ids.spare());

    let refusing = Limit::new(0);
    eng.config.funding = refusing.funding();

    let taken = eng.following_handler().append_entries(Some(log_id(2, 1, 3)), vec![
        blank_ent::<UTConfig>(3, 1, 4),
        blank_ent::<UTConfig>(3, 1, 5),
    ]);
    assert_eq!(Some(log_id(2, 1, 3)), taken, "a new leader's entries have no room");
    assert_eq!(&[log_id(1, 1, 1), log_id(2, 1, 3)], eng.state.log_ids.key_log_ids());
    assert_eq!(0, refusing.held());

    let taken = eng.following_handler().append_entries(Some(log_id(2, 1, 3)), vec![blank_ent::<UTConfig>(2, 1, 4)]);
    assert_eq!(Some(log_id(2, 1, 4)), taken, "the last leader's entry needs no room");

    let funding = Limit::new(1 << 20);
    eng.config.funding = funding.funding();
    let taken = eng.following_handler().append_entries(Some(log_id(2, 1, 4)), vec![blank_ent::<UTConfig>(3, 1, 5)]);
    assert_eq!(Some(log_id(3, 1, 5)), taken);
    assert_eq!(1, eng.state.log_ids.spare());
    assert_eq!(
        4 * size_of_val(&log_id(1, 1, 1)),
        funding.held(),
        "the whole capacity grown to"
    );

    eng.config.funding = refusing.funding();
    let taken = eng.following_handler().append_entries(Some(log_id(3, 1, 5)), vec![
        blank_ent::<UTConfig>(4, 1, 6),
        blank_ent::<UTConfig>(5, 1, 7),
    ]);
    assert_eq!(Some(log_id(4, 1, 6)), taken, "only the leader the room holds");
    assert_eq!(
        &[log_id(1, 1, 1), log_id(2, 1, 4), log_id(3, 1, 5), log_id(4, 1, 6)],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(0, refusing.held());

    drop(eng);
    assert_eq!(0, funding.held(), "credited back once the list is freed");

    Ok(())
}
