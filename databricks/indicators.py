import pandas as pd


def calculate_sma(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].rolling(window=period, min_periods=1).mean()


def calculate_ema(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].ewm(span=period, adjust=False).mean()


def calculate_rsi(df: pd.DataFrame, period: int = 14, column: str = "close") -> pd.Series:
    delta = df[column].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    column: str = "close",
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = df[column].ewm(span=fast, adjust=False).mean()
    ema_slow = df[column].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    std_dev: int = 2,
    column: str = "close",
) -> tuple[pd.Series, pd.Series, pd.Series]:
    sma = df[column].rolling(window=period, min_periods=1).mean()
    rolling_std = df[column].rolling(window=period, min_periods=1).std()
    upper = sma + (rolling_std * std_dev)
    lower = sma - (rolling_std * std_dev)
    return upper, sma, lower


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    df["sma_20"] = calculate_sma(df, 20)
    df["sma_50"] = calculate_sma(df, 50)
    df["ema_12"] = calculate_ema(df, 12)
    df["ema_26"] = calculate_ema(df, 26)
    df["rsi_14"] = calculate_rsi(df, 14)
    df["macd_line"], df["macd_signal"], df["macd_histogram"] = calculate_macd(df)
    df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger_bands(df)
    return df
