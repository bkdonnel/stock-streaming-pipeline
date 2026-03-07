import os
from dotenv import load_dotenv

load_dotenv()


def _get_polygon_api_key() -> str:
    """
    Use Databricks Secrets when running on a cluster, fall back to .env for local development.
    """
    try:
        return dbutils.secrets.get(scope="stock-pipeline", key="polygon-api-key")  # noqa: F821
    except NameError:
        return os.getenv("POLYGON_API_KEY", "")


class Config:
    POLYGON_API_KEY: str = _get_polygon_api_key()
    STOCK_SYMBOLS: list[str] = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

    # Fully qualified Unity Catalog table names (catalog: bootcamp_students, user-prefixed schemas)
    BRONZE_TABLE: str = "bootcamp_students.bd_bronze.stock_prices"
    SILVER_TABLE: str = "bootcamp_students.bd_silver.stock_prices"
    GOLD_TABLE: str = "bootcamp_students.bd_gold.stock_indicators"

    # Indicator periods
    SMA_SHORT: int = 20
    SMA_LONG: int = 50
    EMA_SHORT: int = 12
    EMA_LONG: int = 26
    RSI_PERIOD: int = 14
    MACD_FAST: int = 12
    MACD_SLOW: int = 26
    MACD_SIGNAL: int = 9
    BB_PERIOD: int = 20
    BB_STD_DEV: int = 2

    @classmethod
    def validate(cls) -> None:
        if not cls.POLYGON_API_KEY:
            raise ValueError(
                "POLYGON_API_KEY is not set. "
                "Add it via Databricks Secrets (scope: stock-pipeline, key: polygon-api-key) "
                "or in your local .env file."
            )
