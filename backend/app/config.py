from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str = "postgresql+psycopg://airflow:airflow@postgres:5432/airflow"
    kafka_bootstrap_servers: str = "redpanda:9092"
    kafka_topic: str = "ecommerce.webhooks"
    kafka_group_id: str = "ecommerce-order-processor"
    connections_topic: str = "connections.mock"
    connections_group_id: str = "connections-persistor"
    external_erp_url: str = "http://mock-erp:9000/orders"
    max_pool_size: int = 10
    max_overflow: int = 5
    ingest_max_bytes: int = 262144
    outbox_poll_seconds: float = 1.0


settings = Settings()
