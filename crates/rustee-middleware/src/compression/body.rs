//! Streaming encoder composition and response-trailer preservation.

use std::{
    io,
    sync::{Arc, Mutex},
};

use async_compression::tokio::bufread::{BrotliEncoder, GzipEncoder};
use futures_util::StreamExt;
use http::HeaderMap;
use http_body::Frame;
use http_body_util::{BodyExt, BodyStream, StreamBody};
use rustee_core::{Body, BoxError};
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

use super::ContentCoding;

pub(super) fn compressed_body(body: Body, coding: ContentCoding) -> Body {
    let trailers = Arc::new(Mutex::<Option<HeaderMap>>::new(None));
    let input_trailers = Arc::clone(&trailers);
    let input = BodyStream::new(body).filter_map(move |frame| {
        let trailers = Arc::clone(&input_trailers);
        async move {
            match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(data) => Some(Ok(data)),
                    Err(frame) => match frame.into_trailers() {
                        Ok(values) => match trailers.lock() {
                            Ok(mut stored) => {
                                if let Some(existing) = stored.as_mut() {
                                    existing.extend(values);
                                } else {
                                    *stored = Some(values);
                                }
                                None
                            }
                            Err(_) => {
                                Some(Err(io::Error::other("response trailers lock poisoned")))
                            }
                        },
                        Err(_) => Some(Err(io::Error::other(
                            "response frame is neither data nor trailers",
                        ))),
                    },
                },
                Err(error) => Some(Err(io::Error::other(error))),
            }
        }
    });
    let reader = StreamReader::new(input);

    match coding {
        ContentCoding::Brotli => compressed_reader_body(BrotliEncoder::new(reader), trailers),
        ContentCoding::Gzip => compressed_reader_body(GzipEncoder::new(reader), trailers),
    }
}

fn compressed_reader_body<R>(reader: R, trailers: Arc<Mutex<Option<HeaderMap>>>) -> Body
where
    R: AsyncRead + Send + 'static,
{
    let stream = async_stream::stream! {
        let stream = ReaderStream::with_capacity(reader, 16 * 1024);
        futures_util::pin_mut!(stream);
        while let Some(chunk) = stream.next().await {
            yield chunk
                .map(Frame::data)
                .map_err(|error| -> BoxError { Box::new(error) });
        }
        match take_trailers(&trailers) {
            Ok(Some(values)) => yield Ok(Frame::trailers(values)),
            Ok(None) => {}
            Err(error) => yield Err(error),
        }
    };
    BodyExt::boxed_unsync(StreamBody::new(stream))
}

fn take_trailers(trailers: &Mutex<Option<HeaderMap>>) -> Result<Option<HeaderMap>, BoxError> {
    let mut stored = trailers.lock().map_err(|_| -> BoxError {
        Box::new(io::Error::other("response trailers lock poisoned"))
    })?;
    Ok(stored.take())
}
