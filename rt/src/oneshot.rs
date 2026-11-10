//! Oneshot channel traits.

use std::future::Future;

use openraft_macros::since;

use crate::Charge;
use crate::OptionalSend;

/// A oneshot channel for sending a single value between asynchronous tasks.
pub trait Oneshot {
    /// Type of a `oneshot` sender.
    type Sender<T: OptionalSend>: OneshotSender<T>;
    /// Type of a `oneshot` receiver.
    #[since(version = "0.10.0", change = "remove the `OptionalSync` requirement")]
    type Receiver<T: OptionalSend>: OptionalSend + Future<Output = Result<T, Self::ReceiverError>> + Unpin;
    /// Type of a `oneshot` receiver error.
    type ReceiverError: std::error::Error + OptionalSend;

    /// Creates a new one-shot channel for sending single values.
    ///
    /// The function returns separate "send" and "receive" handles. The `Sender`
    /// handle is used by the producer to send the value. The `Receiver` handle is
    /// used by the consumer to receive the value.
    ///
    /// Each handle can be used on separate tasks.
    #[track_caller]
    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where T: OptionalSend;

    /// The bytes of the shared block a channel carrying `T` allocates, which a funded channel is
    /// charged before it is made: see [`charged_channel`](Self::charged_channel).
    ///
    /// `None` if the runtime cannot fund its channels, which by default it cannot.
    fn block_bytes<T>() -> Option<usize>
    where T: OptionalSend {
        None
    }

    /// Creates a one-shot channel like [`channel`](Self::channel), its shared block funded by
    /// `charge`, taken before for [`block_bytes`](Self::block_bytes), and held until both
    /// handles are dropped and the block is freed.
    ///
    /// `charge` is given back, with nothing allocated, if it does not hold the block's bytes or
    /// the runtime cannot fund its channels, which by default it cannot.
    fn charged_channel<T>(charge: Charge) -> Result<(Self::Sender<T>, Self::Receiver<T>), Charge>
    where T: OptionalSend {
        Err(charge)
    }
}

/// Send a value on a oneshot channel.
#[since(version = "0.10.0", change = "remove the `OptionalSync` requirement")]
pub trait OneshotSender<T>: OptionalSend + Sized
where T: OptionalSend
{
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent.
    ///
    /// This method consumes `self` as only one value may ever be sent on a `oneshot`
    /// channel. It is not marked async because sending a message to an `oneshot`
    /// channel never requires any form of waiting.  Because of this, the `send`
    /// method can be used in both synchronous and asynchronous code without
    /// problems.
    #[track_caller]
    fn send(self, t: T) -> Result<(), T>;
}
