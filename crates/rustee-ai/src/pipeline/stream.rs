//! Terminal-event normalization for provider streams.

use futures_util::{StreamExt, stream};

use crate::{AiEventStream, AiStreamEvent};

/// Yields a terminal event or stream error once, then stops polling its upstream stream.
///
/// Errors are terminal for the same reason: downstream stages must not process data after a
/// failed stream stage.
pub(super) fn stop_after_terminal_event<E>(stream: AiEventStream<E>) -> AiEventStream<E>
where
    E: Send + 'static,
{
    Box::pin(stream::unfold(
        (stream, false),
        |(mut stream, is_terminal)| async move {
            if is_terminal {
                return None;
            }

            let event = stream.next().await?;
            let is_terminal = event.is_err() || matches!(&event, Ok(AiStreamEvent::Completed(_)));
            Some((event, (stream, is_terminal)))
        },
    ))
}
