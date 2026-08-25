//! Opt-in `PostgreSQL` contract tests. Run with a disposable database at `RUSTEE_DATABASE_URL`.

use std::time::{Duration, Instant};

use rustee_sqlx::{
    ConnectError, POSTGRES_TENANT_SETTING, PoolConfig, TenantContext, begin_tenant_transaction,
    connect, readiness,
};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".to_owned())
}

#[tokio::test]
#[ignore = "requires a stopped PostgreSQL server; CI controls the container lifecycle"]
async fn initial_pool_connect_is_bounded_and_does_not_render_database_credentials() {
    if std::env::var("RUSTEE_SQLX_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }
    let config = PoolConfig {
        max_connections: 1,
        min_connections: 0,
        acquire_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
    };
    let started = Instant::now();
    let error = connect(&database_url(), config).await.unwrap_err();
    assert!(matches!(
        error,
        ConnectError::Sqlx(_) | ConnectError::Timeout(_)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped PostgreSQL connection exceeded the pool deadline"
    );
    let rendered = error.to_string();
    assert!(!rendered.contains("rustee:rustee"));
    assert!(!rendered.contains("127.0.0.1"));
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn pool_connects_and_executes_the_readiness_query() {
    let pool = connect(&database_url(), PoolConfig::default())
        .await
        .unwrap();
    readiness(&pool, Duration::from_secs(1)).await.unwrap();
    let answer: i32 = sqlx::query_scalar("SELECT 40 + 2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(answer, 42);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn tenant_transaction_enforces_postgres_row_level_security() {
    let pool = connect(&database_url(), PoolConfig::default())
        .await
        .unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let table = format!("tenant_contract_{suffix}");
    let role = format!("tenant_contract_{suffix}");
    let policy = format!("tenant_policy_{suffix}");

    sqlx::query(&format!(
        "CREATE TABLE {table} (tenant_id text NOT NULL, value text NOT NULL)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE ROLE {role} NOLOGIN"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("GRANT SELECT, INSERT ON {table} TO {role}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE POLICY {policy} ON {table} FOR ALL TO {role} USING (tenant_id = current_setting('{POSTGRES_TENANT_SETTING}', true)) WITH CHECK (tenant_id = current_setting('{POSTGRES_TENANT_SETTING}', true))"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let acme = TenantContext::new("acme").unwrap();
    let mut transaction = begin_tenant_transaction(&pool, &acme).await.unwrap();
    sqlx::query(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let configured_tenant: String = sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(POSTGRES_TENANT_SETTING)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(configured_tenant, "acme");
    sqlx::query(&format!(
        "INSERT INTO {table} (tenant_id, value) VALUES ($1, $2)"
    ))
    .bind("acme")
    .bind("visible only to acme")
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let beta = TenantContext::new("beta").unwrap();
    let mut transaction = begin_tenant_transaction(&pool, &beta).await.unwrap();
    sqlx::query(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let beta_visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(beta_visible, 0);
    assert!(
        sqlx::query(&format!(
            "INSERT INTO {table} (tenant_id, value) VALUES ($1, $2)"
        ))
        .bind("acme")
        .bind("must be rejected")
        .execute(&mut *transaction)
        .await
        .is_err()
    );
    transaction.rollback().await.unwrap();

    let mut transaction = begin_tenant_transaction(&pool, &acme).await.unwrap();
    sqlx::query(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let acme_visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(acme_visible, 1);
    transaction.commit().await.unwrap();

    sqlx::query(&format!("DROP TABLE {table}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
