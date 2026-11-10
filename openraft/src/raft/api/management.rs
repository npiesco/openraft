use std::collections::TryReserveError;
use std::fmt::Debug;

use display_more::DisplayResultExt;
use maplit::btreemap;
use openraft_macros::since;

use crate::ChangeMembers;
use crate::LogIdOptionExt;
use crate::Membership;
use crate::RaftMetrics;
use crate::RaftTypeConfig;
use crate::batch::Batch;
use crate::core::raft_msg::RaftMsg;
use crate::core::raft_msg::membership_payloads::MembershipPayloads;
use crate::core::replication_lag;
use crate::entry::RaftPayload;
use crate::errors::ClientWriteError;
use crate::errors::InitializeError;
use crate::errors::RaftError;
use crate::errors::Unfunded;
use crate::impls::ProgressResponder;
use crate::membership::IntoNodes;
use crate::raft::ChangeMembershipOutcome;
use crate::raft::ChangeMembershipRequest;
use crate::raft::ClientWriteResult;
use crate::raft::Precondition;
use crate::raft::raft_inner::RaftInner;
use crate::type_config::alias::BatchOf;
use crate::type_config::alias::LogIdOf;

/// Provides management APIs for the Raft system.
///
/// This struct contains methods for managing the Raft cluster, including
/// membership changes and node additions.
#[since(version = "0.10.0")]
pub(crate) struct ManagementApi<'a, C>
where C: RaftTypeConfig
{
    inner: &'a RaftInner<C>,
}

impl<'a, C> ManagementApi<'a, C>
where C: RaftTypeConfig
{
    pub(in crate::raft) fn new(inner: &'a RaftInner<C>) -> Self {
        Self { inner }
    }

    #[since(version = "0.10.0")]
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn initialize<T>(&self, members: T) -> Result<Result<(), InitializeError<C>>, RaftError<C>>
    where T: IntoNodes<C::NodeId, C::Node> + Debug {
        self.initialize_with_payload(members, C::Payload::blank()).await
    }

    #[since(version = "0.10.0")]
    #[tracing::instrument(level = "debug", skip(self, payload))]
    pub(crate) async fn initialize_with_payload<T>(
        &self,
        members: T,
        payload: C::Payload,
    ) -> Result<Result<(), InitializeError<C>>, RaftError<C>>
    where
        T: IntoNodes<C::NodeId, C::Node> + Debug,
    {
        self.inner
            .call_core_oneshot(|tx| RaftMsg::Initialize {
                members: members.into_nodes(),
                payload,
                tx,
            })
            .await
    }

    #[since(version = "0.10.0")]
    #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<C::NodeId, C::Node>>,
        retain: bool,
        preconditions: BatchOf<C, Precondition<C>>,
    ) -> Result<ClientWriteResult<C>, RaftError<C>> {
        let request = ChangeMembershipRequest::new(members, retain).with_preconditions(preconditions);
        let result = self.change_membership_with_payload(request).await?;
        let result = result.map(|outcome| outcome.uniform);
        Ok(result)
    }

    pub(crate) async fn change_membership_with_payload(
        &self,
        request: ChangeMembershipRequest<C>,
    ) -> Result<Result<ChangeMembershipOutcome<C>, ClientWriteError<C>>, RaftError<C>> {
        let (changes, retain, preconditions, payload) = request.into_parts();
        let (joint_payload, uniform_payload) = match payload {
            Some((joint, uniform)) => (joint, uniform),
            None => (C::Payload::blank(), C::Payload::blank()),
        };

        tracing::info!(
            "change_membership: start to commit joint config: changes: {:?}, retain: {}",
            changes,
            retain
        );

        // Every channel of both steps is the caller's, all charged before any is made, and made
        // before anything is sent: refused, none is made, the change is not proposed, and a joint
        // entry never waits for a second step that could not be funded.
        let mut steps = self.inner.oneshot_charge::<ClientWriteResult<C>>(2)?;
        let mut unused = self.inner.oneshot_charge::<C::Payload>(1)?;
        // So is what the second step's preconditions take, before the first step is proposed:
        // whether they name a leader is known now ([`uniform_preconditions`]).
        let leader = names_leader(preconditions.as_ref());
        let uniform_held = self.inner.charge(uniform_preconditions_bytes::<C>(leader))?;
        let (tx, rx) = steps.channel()?;
        let tx = ProgressResponder::<C, _>::completing(tx);
        let (uniform_tx, uniform_rx) = steps.channel()?;

        tracing::debug!("change_membership: start",);

        // `RaftCore` computes the membership, so only `RaftCore` can tell whether this change
        // needs a joint membership. It spends `joint_payload` on a joint membership and
        // `uniform_payload` on a uniform one, and returns the payload it did not spend.
        let (unused_tx, unused_rx) = unused.channel()?;
        let payloads = MembershipPayloads::JointOrUniform {
            joint: joint_payload,
            uniform: uniform_payload,
            unused_tx,
        };

        // res is error if membership cannot be changed.
        // If no error, it will enter a joint state
        let client_write_result = self
            .inner
            .call_core(
                RaftMsg::ChangeMembership {
                    changes,
                    payloads,
                    retain,
                    // The caller's own, moved: the second step's are made from the first's entry.
                    preconditions,
                    held: None,
                    tx,
                },
                rx,
            )
            .await?;

        tracing::debug!(
            "change_membership: client_write_result: {}",
            client_write_result.display()
        );

        let resp = match client_write_result {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("the first step error: {}", e);
                return Ok(Err(e));
            }
        };

        tracing::debug!("res of first step: {}", resp);

        // Safe unwrap(): a change-membership entry's response carries its membership. Read in
        // place: the response is the outcome's.
        if !resp.membership.as_ref().unwrap().is_joint() {
            let outcome = ChangeMembershipOutcome {
                joint: None,
                uniform: resp,
            };
            return Ok(Ok(outcome));
        }

        // The first step spent `joint_payload` on the joint entry, so the payload it returned is
        // `uniform_payload`, for the entry this second step writes.
        let uniform_payload = self.inner.recv_msg(unused_rx).await?;

        tracing::debug!("committed a joint config: {} {:?}", resp.log_id, resp.membership);
        tracing::debug!("the second step is to change to uniform config");

        let preconditions = uniform_preconditions::<C>(&resp.log_id, leader).map_err(|_| Unfunded)?;

        let tx = ProgressResponder::<C, _>::completing(uniform_tx);

        // The second step, send a NOOP change to flatten the joint config.
        let changes = ChangeMembers::AddVoterIds(Default::default());
        let client_write_result = self
            .inner
            .call_core(
                RaftMsg::ChangeMembership {
                    changes,
                    payloads: MembershipPayloads::Uniform(uniform_payload),
                    retain,
                    preconditions,
                    held: uniform_held,
                    tx,
                },
                uniform_rx,
            )
            .await?;

        tracing::info!(
            "result of second step of change_membership: {}",
            client_write_result.display()
        );

        if let Err(e) = &client_write_result {
            tracing::error!("the second step error: {}", e);
        }

        let outcome = client_write_result.map(|uniform| ChangeMembershipOutcome {
            joint: Some(resp),
            uniform,
        });
        Ok(outcome)
    }

    #[since(version = "0.10.0")]
    #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn append_membership(
        &self,
        membership: Membership<C::NodeId, C::Node>,
        payload: C::Payload,
        preconditions: BatchOf<C, Precondition<C>>,
    ) -> Result<ClientWriteResult<C>, RaftError<C>> {
        tracing::info!("append_membership: proposed membership: {}", membership);

        // The channel is charged before the membership is bound, which allocates, and made only
        // once it is bound: refused either way, nothing is made for a call that is not sent.
        let mut charge = self.inner.oneshot_charge::<ClientWriteResult<C>>(1)?;

        // Bind the membership into the payload here, so the core has exactly one value to
        // validate and to write: the payload it receives.
        let payload = match payload.bind_membership(membership) {
            Ok(payload) => payload,
            Err(refused) => return Ok(Err(ClientWriteError::ChangeMembershipError(refused.into()))),
        };

        let (tx, rx) = charge.channel()?;
        let tx = ProgressResponder::<C, _>::completing(tx);

        let msg = RaftMsg::AppendMembership {
            payload,
            preconditions,
            tx,
        };

        Ok(self.inner.call_core(msg, rx).await?)
    }

    #[since(version = "0.10.0")]
    #[tracing::instrument(level = "debug", skip(self, id), fields(target=display(&id)))]
    pub(crate) async fn add_learner(
        &self,
        id: C::NodeId,
        node: C::Node,
        blocking: bool,
    ) -> Result<ClientWriteResult<C>, RaftError<C>> {
        let (tx, rx) = self.inner.oneshot()?;
        let tx = ProgressResponder::<C, _>::completing(tx);

        let msg = RaftMsg::ChangeMembership {
            changes: ChangeMembers::AddNodes(btreemap! {id.clone()=>node}),
            payloads: MembershipPayloads::Uniform(C::Payload::blank()),
            retain: true,
            preconditions: Batch::of([]),
            held: None,
            tx,
        };

        let client_write_result = self.inner.call_core(msg, rx).await?;

        let resp = match client_write_result {
            Ok(x) => x,
            Err(e) => return Ok(Err(e)),
        };

        if !blocking {
            return Ok(Ok(resp));
        }

        if self.inner.id == id {
            return Ok(Ok(resp));
        }

        // Otherwise, blocks until the replication to the new learner becomes up to date.

        // The log id of the membership that contains the added learner.
        let membership_log_id = &resp.log_id;

        let wait_res = self
            .inner
            .wait(None)
            .metrics(
                |metrics| match self.check_replication_upto_date(metrics, &id, Some(membership_log_id)) {
                    Ok(_matching) => true,
                    // keep waiting
                    Err(_) => false,
                },
                "wait new learner to become line-rate",
            )
            .await;

        tracing::info!(
            "waiting for replication to new learner: wait_res: {}",
            wait_res.display()
        );

        Ok(Ok(resp))
    }

    #[since(version = "0.10.0")]
    fn check_replication_upto_date(
        &self,
        metrics: &RaftMetrics<C>,
        node_id: &C::NodeId,
        membership_log_id: Option<&LogIdOf<C>>,
    ) -> Result<Option<LogIdOf<C>>, ()> {
        if metrics.membership_config.log_id().as_ref() < membership_log_id {
            // Waiting for the latest metrics to report.
            return Err(());
        }

        if metrics.membership_config.membership().get_node(node_id).is_none() {
            // This learner has been removed.
            return Ok(None);
        }

        let repl = match &metrics.replication {
            None => {
                // This node is no longer a leader.
                return Ok(None);
            }
            Some(x) => x,
        };

        let replication_metrics = repl;
        let target_metrics = match replication_metrics.get(node_id) {
            None => {
                // Maybe replication is not reported yet. Keep waiting.
                return Err(());
            }
            Some(x) => x,
        };

        let matched = target_metrics.clone();

        let distance = replication_lag(&matched.index(), &metrics.last_log_index);

        if distance <= self.inner.config.replication_lag_threshold {
            // replication became up to date.
            return Ok(matched);
        }

        // Not up to date, keep waiting.
        Err(())
    }
}

/// Whether `preconditions` require a committed leader, which a change's second step requires
/// again ([`uniform_preconditions`]). Read in place.
fn names_leader<C: RaftTypeConfig>(preconditions: &[Precondition<C>]) -> bool {
    preconditions.iter().any(|x| matches!(x, Precondition::CommittedLeaderId { .. }))
}

/// The bytes a change's second step's preconditions take ([`uniform_preconditions`]): none for
/// one alone, which a batch holds inline as [`InlineBatch`](crate::impls::InlineBatch) does, and
/// the vector of exactly two a batch keeps ([`Batch`]'s `From<Vec<T>>`) for both.
pub(crate) const fn uniform_preconditions_bytes<C: RaftTypeConfig>(leader: bool) -> usize {
    if leader { 2 * size_of::<Precondition<C>>() } else { 0 }
}

/// What a change's second, uniform step requires, made from the log id of the joint entry the
/// first step wrote, never copied from the caller's: that entry's membership is the effective
/// one, and, if the caller required a committed leader (`leader`), the leader that wrote it is
/// still established. That leader is the one the first step's preconditions required, which it
/// passed, so a leader change between the two steps still aborts the second. Any `LastLogId`
/// the caller required is no longer the last, as the joint entry follows it.
///
/// Both are made in a vector of exactly two, which [`uniform_preconditions_bytes`] charged
/// before the first step was proposed; refused by the allocator, nothing is made.
pub(crate) fn uniform_preconditions<C: RaftTypeConfig>(
    joint: &LogIdOf<C>,
    leader: bool,
) -> Result<BatchOf<C, Precondition<C>>, TryReserveError> {
    let membership = Precondition::LastMembershipLogId {
        last_membership_log_id: Some(joint.clone()),
    };
    if !leader {
        return Ok(Batch::of([membership]));
    }
    let mut both = Vec::new();
    both.try_reserve_exact(2)?;
    both.push(Precondition::CommittedLeaderId {
        committed_leader_id: joint.committed_leader_id().clone(),
    });
    both.push(membership);
    Ok(both.into())
}

#[cfg(test)]
mod tests {
    use super::names_leader;
    use super::uniform_preconditions;
    use super::uniform_preconditions_bytes;
    use crate::engine::testing::UTConfig;
    use crate::engine::testing::log_id;
    use crate::metered::metered;
    use crate::raft::Precondition;

    type C = UTConfig;

    /// A change's second step's preconditions are made from the joint entry in exactly the bytes
    /// charged for them before the first step is proposed: none for the joint entry's membership
    /// alone, and the vector of two a batch keeps, held until they are freed, when the caller
    /// required a leader too.
    #[test]
    fn test_uniform_preconditions_are_made_in_the_bytes_charged_for_them() {
        let joint = log_id(3, 1, 7);
        let membership = || Precondition::<C>::LastMembershipLogId {
            last_membership_log_id: Some(log_id(3, 1, 7)),
        };

        let (alone, meter) = metered(|| uniform_preconditions::<C>(&joint, false).unwrap());
        assert_eq!(0, uniform_preconditions_bytes::<C>(false));
        assert_eq!(0, meter.allocated, "one is held inline");
        assert_eq!(alone.as_ref(), [membership()]);

        let (both, meter) = metered(|| uniform_preconditions::<C>(&joint, true).unwrap());
        assert_eq!(uniform_preconditions_bytes::<C>(true), meter.allocated);
        assert_eq!(
            meter.allocated.cast_signed(),
            meter.kept,
            "what was charged is what they hold"
        );
        assert_eq!(both.as_ref(), [
            Precondition::CommittedLeaderId {
                committed_leader_id: log_id(3, 1, 7).leader_id,
            },
            membership(),
        ]);
        let ((), meter) = metered(|| drop(both));
        assert_eq!(
            -uniform_preconditions_bytes::<C>(true).cast_signed(),
            meter.kept,
            "and they free"
        );
    }

    /// Whether the caller's preconditions require a leader is read in place.
    #[test]
    fn test_a_required_leader_is_found_in_place() {
        let mut preconditions = vec![Precondition::<C>::LastLogId { last_log_id: None }];
        let (named, meter) = metered(|| names_leader(&preconditions));
        assert!(!named);
        preconditions.push(Precondition::CommittedLeaderId {
            committed_leader_id: log_id(3, 1, 7).leader_id,
        });
        let (also, again) = metered(|| names_leader(&preconditions));
        assert!(also);
        assert_eq!(0, meter.allocated + again.allocated);
    }
}
