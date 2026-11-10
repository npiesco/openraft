use openraft_macros::since;

/// The application's payload refused to bind the membership a proposal computed, so nothing was
/// proposed.
///
/// [`RaftPayload::bind_membership()`] refuses a membership it cannot account for, for example one
/// whose memory it cannot fund. OpenRaft then answers the membership change, append or
/// initialization that computed it with this error, and writes no entry.
///
/// [`RaftPayload::bind_membership()`]: crate::entry::RaftPayload::bind_membership
#[since(version = "0.10.0")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("the payload refused to bind the proposed membership")]
pub struct MembershipRefused;
