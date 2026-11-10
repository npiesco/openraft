use std::time::Duration;

use maplit::btreemap;
use maplit::btreeset;

use crate::Membership;
use crate::MembershipState;
use crate::RaftState;
use crate::Vote;
use crate::engine::testing::UTConfig;
use crate::errors::ForwardReason;
use crate::errors::ForwardToLeader;
use crate::proposer::funding::Funding;
use crate::proposer::funding::testing::Limit;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::LeaderIdOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::NodeIdOf;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;
use crate::vote::RaftLeaderIdExt;

fn log_id(term: u64, node_id: NodeIdOf<UTConfig<u64>>, index: u64) -> LogIdOf<UTConfig<u64>> {
    LogIdOf::<UTConfig<u64>>::new(LeaderIdOf::<UTConfig<u64>>::new_committed(term, node_id), index)
}

fn m12() -> Membership<u64, u64> {
    Membership::new_with_defaults(vec![btreeset! {1,2}], [])
}

#[test]
fn test_forward_to_leader_vote_not_committed() {
    let rs = RaftState::<UTConfig<u64>> {
        vote: Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new(1, 2)),
        membership_state: MembershipState::new(
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m12()),
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m12()),
        ),
        ..Default::default()
    };

    assert_eq!(ForwardToLeader::empty(), rs.forward_to_leader(&Funding::default()));
}

#[test]
fn test_forward_to_leader_not_a_member() {
    let rs = RaftState::<UTConfig<u64>> {
        vote: Leased::new(
            UTConfig::<()>::now(),
            Duration::from_millis(500),
            Vote::new_committed(1, 3),
        ),
        membership_state: MembershipState::new(
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m12()),
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m12()),
        ),
        ..Default::default()
    };

    assert_eq!(ForwardToLeader::empty(), rs.forward_to_leader(&Funding::default()));
}

#[test]
fn test_forward_to_leader_has_leader() {
    let m123 = || Membership::<u64, u64>::new(vec![btreeset! {1,2}], btreemap! {1=>4,2=>5,3=>6}).unwrap();

    let rs = RaftState::<UTConfig<u64>> {
        vote: Leased::new(
            UTConfig::<()>::now(),
            Duration::from_millis(500),
            Vote::new_committed(1, 3),
        ),
        membership_state: MembershipState::new(
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m123()),
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m123()),
        ),
        ..Default::default()
    };

    assert_eq!(ForwardToLeader::new(3, 6), rs.forward_to_leader(&Funding::default()));
}

#[test]
fn test_forward_to_leader_on_a_funded_node_names_the_leader_by_id_only() {
    let m123 = || Membership::<u64, u64>::new(vec![btreeset! {1,2}], btreemap! {1=>4,2=>5,3=>6}).unwrap();

    let rs = RaftState::<UTConfig<u64>> {
        vote: Leased::new(
            UTConfig::<()>::now(),
            Duration::from_millis(500),
            Vote::new_committed(1, 3),
        ),
        membership_state: MembershipState::new(
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m123()),
            StoredMembershipOf::<UTConfig<u64>>::new(Some(log_id(1, 0, 1)), m123()),
        ),
        ..Default::default()
    };

    let limit = Limit::new(0);
    assert_eq!(
        ForwardToLeader {
            leader_id: Some(3),
            leader_node: None,
            reason: ForwardReason::NotLeader,
        },
        rs.forward_to_leader(&limit.funding()),
        "no copy of the leader's node is made, so none is charged"
    );
    assert_eq!(0, limit.held());
}
