//! Opt-in `RabbitMQ` Management API topology-audit contract.

use std::time::Duration;

use lapin::{
    Connection, ConnectionProperties, ExchangeKind,
    options::{
        ExchangeDeclareOptions, ExchangeDeleteOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
};
use rustee_jobs::RetryPolicy;
use rustee_jobs_rabbitmq::{RabbitMqNativeRetryConfig, RabbitMqWorkerConfig};
use rustee_jobs_rabbitmq_management::{
    RabbitMqManagementConfig, RabbitMqTopologyAuditor, RabbitMqTopologyReport,
};
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

const RETRY_DELAY: Duration = Duration::from_millis(25);
const DELIVERY_LIMIT: u16 = 10;

struct Fixture {
    connection: Connection,
    queue: String,
    dead_letter_exchange: String,
    dead_letter_routing_key: String,
}

impl Fixture {
    async fn new() -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let queue = format!("rustee.management.contract.{suffix}.queue");
        let dead_letter_exchange = format!("rustee.management.contract.{suffix}.dlx");
        let dead_letter_routing_key = "dead-letter".to_owned();
        let connection = Connection::connect(&rabbitmq_url(), ConnectionProperties::default())
            .await
            .expect("connect to RabbitMQ");
        let fixture = Self {
            connection,
            queue,
            dead_letter_exchange,
            dead_letter_routing_key,
        };
        fixture.provision().await;
        fixture
    }

    async fn provision(&self) {
        let channel = self
            .connection
            .create_channel()
            .await
            .expect("create channel");
        channel
            .exchange_declare(
                self.dead_letter_exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("declare dead-letter exchange");

        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-queue-type".into(),
            AMQPValue::LongString("quorum".into()),
        );
        arguments.insert(
            "x-delayed-retry-type".into(),
            AMQPValue::LongString("failed".into()),
        );
        arguments.insert(
            "x-delayed-retry-min".into(),
            AMQPValue::LongInt(RETRY_DELAY.as_millis().try_into().expect("delay fits i32")),
        );
        arguments.insert(
            "x-delayed-retry-max".into(),
            AMQPValue::LongInt(RETRY_DELAY.as_millis().try_into().expect("delay fits i32")),
        );
        arguments.insert(
            "x-delivery-limit".into(),
            AMQPValue::LongInt(i32::from(DELIVERY_LIMIT)),
        );
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(self.dead_letter_exchange.as_str().into()),
        );
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(self.dead_letter_routing_key.as_str().into()),
        );
        channel
            .queue_declare(
                self.queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                arguments,
            )
            .await
            .expect("declare quorum source queue");
    }

    fn worker_config(&self) -> RabbitMqWorkerConfig {
        RabbitMqWorkerConfig::new(
            self.queue.clone(),
            format!("{}.worker", self.queue),
            RabbitMqNativeRetryConfig::new(RETRY_DELAY, RETRY_DELAY)
                .expect("valid native retry range"),
            self.dead_letter_exchange.clone(),
            self.dead_letter_routing_key.clone(),
        )
        .expect("valid worker configuration")
    }

    async fn cleanup(&self) {
        let channel = self
            .connection
            .create_channel()
            .await
            .expect("create cleanup channel");
        let _ = channel
            .queue_delete(self.queue.as_str().into(), QueueDeleteOptions::default())
            .await;
        let _ = channel
            .exchange_delete(
                self.dead_letter_exchange.as_str().into(),
                ExchangeDeleteOptions::default(),
            )
            .await;
    }
}

fn rabbitmq_url() -> String {
    std::env::var("RUSTEE_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".to_owned())
}

fn management_url() -> Url {
    std::env::var("RUSTEE_RABBITMQ_MANAGEMENT_URL").map_or_else(
        |_| Url::parse("http://127.0.0.1:15672/").expect("valid loopback URL"),
        |value| Url::parse(&value).expect("valid RabbitMQ management URL"),
    )
}

async fn audit_until_visible(auditor: &RabbitMqTopologyAuditor) -> RabbitMqTopologyReport {
    let mut last_error = None;
    for attempt in 0..40 {
        match auditor.audit().await {
            Ok(report) => return report,
            Err(error) => last_error = Some(error),
        }
        if attempt < 39 {
            sleep(Duration::from_millis(50)).await;
        }
    }
    panic!("RabbitMQ management snapshot never matched: {last_error:?}");
}

#[tokio::test]
#[ignore = "requires RabbitMQ 4.3 Management API; CI provisions one"]
async fn audits_actual_quorum_topology_from_management_api() {
    let fixture = Fixture::new().await;
    let auditor = RabbitMqTopologyAuditor::new(
        RabbitMqManagementConfig::new(management_url(), "guest", "guest", "/")
            .expect("valid loopback management configuration"),
        fixture.worker_config(),
        RetryPolicy {
            max_deliveries: 3,
            initial_backoff: RETRY_DELAY,
            max_backoff: RETRY_DELAY,
        },
    )
    .expect("management auditor");

    let report = audit_until_visible(&auditor).await;
    fixture.cleanup().await;

    assert_eq!(report.delivery_limit(), DELIVERY_LIMIT);
}
