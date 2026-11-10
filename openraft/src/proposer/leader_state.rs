use crate::Membership;
use crate::proposer::Candidate;
use crate::proposer::Leader;
use crate::type_config::alias::NodeIdOf;
use crate::type_config::alias::NodeOf;

/// The quorum set type used by `Leader`.
pub(crate) type LeaderQuorumSet<C> = Membership<NodeIdOf<C>, NodeOf<C>>;

/// Held inline, not boxed: a Leader's charge is credited back as it is dropped, which must not be
/// before the memory it is held in is freed.
pub(crate) type LeaderState<C> = Option<Leader<C, LeaderQuorumSet<C>>>;
pub(crate) type CandidateState<C> = Option<Candidate<C, LeaderQuorumSet<C>>>;
