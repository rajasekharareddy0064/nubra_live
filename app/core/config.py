from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib.parse import quote_plus


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "nubra-realtime-backend"
    environment: str = "dev"
    log_level: str = "INFO"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str | None = None
    db_host: str = "localhost"
    db_name: str = "nubra"
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_port: int = 5432
    db_schema: str = "public"

    # Realtime mode (no Postgres): tick fan-out + in-memory 3m candles. Set use_database=true for legacy DB workers.
    use_database: bool = False
    use_redis: bool = False
    strike_radius: int = 15
    #: ATM ±N strikes for options_data / chain view / backfill (order book uses same).
    option_emit_radius: int = 10
    candle_interval_minutes: int = 3
    market_timezone: str = "Asia/Kolkata"
    subscribe_sdk_ohlcv: bool = False
    #: If true, subscribe to Nubra realtime ``data_type="option"`` using
    #: ``NIFTY:YYYYMMDD`` (see Nubra option-chain docs). Heavier than
    #: ref-id orderbook alone; set false if you hit subscription limits.
    subscribe_sdk_option_chain: bool = True

    #: Poll Nubra historical_data() after each closed 3m bar and diff vs live.
    #: Default off so we do not burn the 60 req/min historical quota until enabled.
    enable_historical_compare: bool = False
    historical_compare_settle_seconds: int = 12
    historical_price_abs_tol: float = 0.05
    #: Tick OHLC vs official REST for NIFTY is typically a few points, not paise.
    historical_index_price_abs_tol: float = 10.0
    historical_volume_rel_tol: float = 0.02

    queue_maxsize: int = 100000
    db_batch_size: int = 500
    db_flush_interval_ms: int = 1000

    # Nubra runtime knobs. Keep optional so scaffolding can boot.
    nubra_env: str = "UAT"
    nubra_exchange: str = "NSE"
    enable_nubra_socket: bool = False
    initial_nifty_price: float = 22000.0

    # Simulation mode: replays sample data through the pipeline without
    # connecting to the live Nubra WebSocket. Enabled by setting
    # SIMULATION_MODE=true or MARKET_MODE=SIMULATION.
    simulation_mode: bool = False
    market_mode: str = "LIVE"
    simulation_speed: float = 1.0  # 1x, 5x, 10x, 0=instant
    sample_data_dir: str = "sample_data"

    # Instrument cache: three-level fallback (GCS → local CSV → SDK)
    instrument_cache_bucket: str = "stock-anaysis-cache"
    instrument_cache_file: str = "instrument_master_cache.csv"
    instrument_download_timeout: int = 60

    # Legacy TOTP flag, retained for backward compatibility only.
    # Session-only authentication ignores this value at runtime — the
    # SDK is always constructed with totp_login=False and tokens are
    # injected directly from auth_data.db.*. Refresh the session by
    # running setup_totp.py from a real terminal.
    nubra_use_totp: bool = True

    @property
    def is_simulation(self) -> bool:
        return self.simulation_mode or self.market_mode.upper() == "SIMULATION"

    @property
    def database_dsn(self) -> str:
        if self.postgres_dsn:
            return self.postgres_dsn
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        host = self.db_host
        database = quote_plus(self.db_name)
        return f"postgresql://{user}:{password}@{host}:{self.db_port}/{database}"


settings = Settings()
