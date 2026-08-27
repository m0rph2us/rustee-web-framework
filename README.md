# Rustee

Rustee is a Rust web framework for building services that can grow from a small HTTP API into an operationally rigorous system without turning the core into a bundle of provider assumptions.

It combines a small, typed, Tower-compatible HTTP foundation with focused optional crates for persistence, authentication, background delivery, event streaming, AI application integration, and operational concerns. Tokio and Hyper power the core runtime; applications keep ownership of their infrastructure clients, topology, credentials, and deployment policy.

## Quick Start

Rustee currently builds from this workspace and requires Rust `1.94.1` or newer.

```sh
git clone https://github.com/m0rph2us/rustee-web-framework.git
cd rustee-web-framework
cargo +1.94.1 run --locked -p hello-world
```

The hello-world example listens on `127.0.0.1:3000`. More runnable examples are available in [`examples/`](examples/) and described in [`docs/examples.html`](docs/examples.html).

```rust
use std::net::SocketAddr;

use rustee::App;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = App::new().get("/", || async { "Hello from Rustee" });
    rustee::serve(SocketAddr::from(([127, 0, 0, 1], 3000)), app).await
}
```

## Design

- A compact, typed core for routing, extraction, response construction, server lifecycle, configuration, and Tower middleware composition.
- Optional integration crates rather than hidden defaults for SQLx, Redis, MongoDB, authentication, jobs, Kafka, RabbitMQ, SQS, OpenAPI, and AI or MCP workflows.
- Explicit operational contracts for timeouts, redaction, retries, shutdown, idempotency, observability, and failure recovery.
- At-least-once delivery whenever a crash boundary can create duplicates; application handlers own idempotent external effects.
- A strict separation between reusable framework capability and deployment-specific qualification for providers, credentials, topology, outages, and recovery.

## Workspace Areas

| Area | Rustee provides | Application or operator owns |
| --- | --- | --- |
| HTTP | Routing, extractors, response types, server lifecycle, middleware | Domain handlers and service policy |
| Data | SQLx, Redis, and MongoDB adapters | Client lifecycle, schemas, migrations, topology, and tenant policy |
| Security | Principal normalization, JWT, sessions, OIDC, API-key boundaries | Identity-provider configuration, key rotation, and authorization policy |
| Delivery | Jobs, outbox, Redis, NATS, RabbitMQ, SQS, Kafka, retry, and scheduling contracts | Broker selection, ACLs, duplicate handling, outage response, and external effects |
| AI and MCP | Provider, tool, RAG, audit, evaluation, cache, batch, and OAuth boundaries | Models, prompts, data policy, approvals, credentials, spending, and live-provider operation |
| Operations | Configuration, tracing, metrics, OpenTelemetry, edge-delivery guidance, and release evidence | Deployment, exporters, alerting, TLS, CDN, secrets, and incident response |

## Documentation

The canonical project documentation is HTML under [`docs/`](docs/), not this README. Start with the [documentation index](docs/index.html), then follow the focused documents for the area you are using.

- [Design philosophy](docs/design-philosophy.html) and [architecture](docs/architecture.html)
- [Database](docs/database.html), [Redis](docs/redis.html), and [MongoDB](docs/mongodb.html)
- [Authentication](docs/authentication.html), [queues](docs/queues.html), and [events](docs/events.html)
- [AI integration](docs/ai.html), [testing](docs/testing.html), and [operations](docs/operations.html)
- [Release plan](docs/release-plan.html), [support matrix](docs/support-matrix.html), and [qualification register](docs/qualification-register.html)
- [Architecture decision records](docs/adr/index.html) and [AI maintenance context](docs/ai-context.html)

## Verification

Run the ordinary workspace checks with the tracked dependency graph:

```sh
cargo +1.94.1 check --locked --workspace --all-targets
cargo +1.94.1 test --locked --workspace --all-targets
```

Optional provider and topology qualifications are deliberately separate from ordinary local tests. Review the [support matrix](docs/support-matrix.html) and [qualification register](docs/qualification-register.html) before making deployment or provider support claims.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
