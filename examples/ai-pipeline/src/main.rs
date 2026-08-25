//! Compile-checked OpenAI-backed Rustee AI pipeline example.

use std::{env, net::SocketAddr};

use rustee::{App, Error, Json, Result, State};
use rustee_ai::{AiPipeline, AiProvider, ChatMessage, ChatRequest, MessageRole};
use rustee_ai_openai::{OpenAiConfig, OpenAiResponsesProvider};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct SupportQuestion {
    message: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SupportReply {
    answer: String,
    model: String,
}

struct SupportState<P> {
    pipeline: AiPipeline<P>,
    model: String,
}

async fn answer<P>(
    State(state): State<SupportState<P>>,
    Json(question): Json<SupportQuestion>,
) -> Result<Json<SupportReply>>
where
    P: AiProvider,
{
    let request = ChatRequest::new(
        state.model.clone(),
        [
            ChatMessage::new(
                MessageRole::System,
                "Answer only from application-approved support policy.",
            )
            .map_err(|_| Error::internal())?,
            ChatMessage::new(MessageRole::User, question.message)
                .map_err(|_| Error::bad_request("message must not be blank"))?,
        ],
    )
    .map_err(|_| Error::bad_request("a support request needs one message"))?;
    let response = state
        .pipeline
        .complete(request)
        .await
        .map_err(|_| Error::internal())?;

    Ok(Json(SupportReply {
        answer: response.content().to_owned(),
        model: response.model().to_owned(),
    }))
}

fn app<P>(pipeline: AiPipeline<P>, model: impl Into<String>) -> App
where
    P: AiProvider,
{
    App::new()
        .with_state(SupportState {
            pipeline,
            model: model.into(),
        })
        .post("/support/answer", answer::<P>)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = OpenAiResponsesProvider::new(OpenAiConfig::new(env::var("OPENAI_API_KEY")?)?)?;
    let model = env::var("OPENAI_MODEL")?;
    if model.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "OPENAI_MODEL must not be blank",
        )
        .into());
    }
    rustee::serve(
        SocketAddr::from(([127, 0, 0, 1], 3004)),
        app(AiPipeline::new(provider), model),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rustee::StatusCode;
    use rustee_ai::{AiPipeline, ChatResponse, Usage};
    use rustee_ai_test::{RecordedAiOperation, RecordedAiProvider};
    use rustee_test::TestApp;

    use super::{SupportQuestion, SupportReply, app};

    #[tokio::test]
    async fn pipeline_endpoint_uses_the_configured_provider_model_and_returns_a_response() {
        let provider = RecordedAiProvider::new();
        provider.queue_completion(
            ChatResponse::new(
                "response-1",
                "gpt-test",
                "Reset your password from account settings.",
                [],
                Usage {
                    input_tokens: 7,
                    output_tokens: 11,
                },
            )
            .unwrap(),
        );
        let client = TestApp::new(app(AiPipeline::new(provider.clone()), "gpt-test"));

        let response = client
            .post("/support/answer")
            .unwrap()
            .json(&SupportQuestion {
                message: "How do I reset my password?".to_owned(),
            })
            .unwrap()
            .send()
            .await
            .unwrap();

        response.assert_status(StatusCode::OK).unwrap();
        assert_eq!(
            response.json::<SupportReply>().unwrap(),
            SupportReply {
                answer: "Reset your password from account settings.".to_owned(),
                model: "gpt-test".to_owned(),
            }
        );
        let records = provider.recorded_requests();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation(), RecordedAiOperation::Complete);
        assert_eq!(records[0].model(), "gpt-test");
        assert_eq!(records[0].message_count(), 2);
    }

    #[tokio::test]
    async fn provider_failures_are_normalized_before_the_http_response() {
        let client = TestApp::new(app(AiPipeline::new(RecordedAiProvider::new()), "gpt-test"));

        let response = client
            .post("/support/answer")
            .unwrap()
            .json(&SupportQuestion {
                message: "private support question".to_owned(),
            })
            .unwrap()
            .send()
            .await
            .unwrap();

        response
            .assert_status(StatusCode::INTERNAL_SERVER_ERROR)
            .unwrap();
        assert!(!response.text().unwrap().contains("no completion result"));
    }
}
