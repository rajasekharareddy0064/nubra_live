from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "nubra-realtime-backend"
    environment: str = "dev"
    log_level: str = "INFO"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/nubra"

    # Realtime mode (no Postgres): tick fan-out + in-memory 3m candles. Set use_database=true for legacy DB workers.
    use_database: bool = False
    use_redis: bool = False
    strike_radius: int = 15
    candle_interval_minutes: int = 3
    market_timezone: str = "Asia/Kolkata"
    subscribe_sdk_ohlcv: bool = False
    #: If true, subscribe to Nubra realtime ``data_type="option"`` using
    #: ``NIFTY:YYYYMMDD`` (see Nubra option-chain docs). Heavier than
    #: ref-id orderbook alone; set false if you hit subscription limits.
    subscribe_sdk_option_chain: bool = True

    queue_maxsize: int = 100000
    db_batch_size: int = 500
    db_flush_interval_ms: int = 1000

    # Nubra runtime knobs. Keep optional so scaffolding can boot.
    nubra_env: str = "UAT"
    nubra_exchange: str = "NSE"
    enable_nubra_socket: bool = False
    initial_nifty_price: float = 22000.0

    # Legacy TOTP flag, retained for backward compatibility only.
    # Session-only authentication ignores this value at runtime — the
    # SDK is always constructed with totp_login=False and tokens are
    # injected directly from auth_data.db.*. Refresh the session by
    # running setup_totp.py from a real terminal.
    nubra_use_totp: bool = True


settings = Settings()
