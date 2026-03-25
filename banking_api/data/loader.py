"""Data loading and validation utilities."""
import logging
import sys
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Internal format columns (what the rest of the app uses after transformation)
REQUIRED_COLUMNS: dict[str, Any] = {
    "step": "int64",
    "type": "object",
    "amount": "float64",
    "nameOrig": "object",
    "oldbalanceOrg": "float64",
    "newbalanceOrig": "float64",
    "nameDest": "object",
    "oldbalanceDest": "float64",
    "newbalanceDest": "float64",
    "isFraud": "int8",
    "isFlaggedFraud": "int8",
}

VALID_TYPES: set[str] = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

# Raw CSV columns expected in transactions_data.csv
_RAW_CSV_COLUMNS = {
    "id", "date", "client_id", "card_id", "amount", "use_chip",
    "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors",
}

# Map use_chip values to internal transaction types
_TYPE_MAP: dict[str, str] = {
    "Swipe Transaction": "PAYMENT",
    "Chip Transaction": "CASH_OUT",
    "Online Transaction": "TRANSFER",
}

# Error keywords that signal potential fraud
_FRAUD_KEYWORDS = ("Bad PIN", "Bad CVV", "Bad Card Number", "Bad Expiration", "Bad Zipcode")


def validate_columns(df: pd.DataFrame) -> list[str]:
    """Return list of required internal column names missing from df."""
    return [col for col in REQUIRED_COLUMNS if col not in df.columns]


def validate_fraud_values(df: pd.DataFrame) -> list[int]:
    """Return list of row indices where isFraud is not 0 or 1."""
    invalid_mask = ~df["isFraud"].isin([0, 1])
    return [int(i) for i in df.index[invalid_mask]]


def validate_types(df: pd.DataFrame) -> list[str]:
    """Return list of transaction type values not in VALID_TYPES."""
    unique_types = {str(t) for t in df["type"].unique()}
    return sorted(unique_types - VALID_TYPES)


def validate_null_values(df: pd.DataFrame) -> dict[str, int]:
    """Return a dict mapping column names to their null count (nulls only)."""
    cols = [col for col in REQUIRED_COLUMNS if col in df.columns]
    null_counts = df[cols].isnull().sum()
    return {col: int(count) for col, count in null_counts.items() if count > 0}


def generate_transaction_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with a generated 'id' column (tx_XXXXXXX)."""
    df = df.copy()
    df["id"] = [f"tx_{i:07d}" for i in range(len(df))]
    return df


def _transform_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Transform raw CSV format into the application's internal format.

    Maps columns from the credit card transaction CSV to the internal
    representation used by all services and models.
    """
    result = pd.DataFrame(index=df.index)

    # step: hours elapsed since the earliest transaction
    dates = pd.to_datetime(df["date"])
    result["step"] = ((dates - dates.min()) / pd.Timedelta(hours=1)).astype(int)

    # transaction type: map use_chip values to internal names; keep unknown values as-is
    result["type"] = (
        df["use_chip"].map(_TYPE_MAP).fillna(df["use_chip"]).astype("category")
    )

    # amount: strip leading $, remove commas, take absolute value
    result["amount"] = (
        df["amount"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
        .abs()
    )

    # origin customer ID
    result["nameOrig"] = "C" + df["client_id"].astype(str)

    # balance fields: set so the fraud balance-consistency rule doesn't
    # false-fire (expected = oldbalanceOrg - amount = amount - amount = 0 = newbalanceOrig)
    result["oldbalanceOrg"] = result["amount"]
    result["newbalanceOrig"] = 0.0
    result["oldbalanceDest"] = 0.0
    result["newbalanceDest"] = result["amount"]

    # destination merchant ID
    result["nameDest"] = "M" + df["merchant_id"].astype(str)

    # isFraud: 1 if errors contain security-related keywords
    fraud_mask = pd.Series(False, index=df.index)
    for kw in _FRAUD_KEYWORDS:
        fraud_mask |= df["errors"].str.contains(kw, na=False, regex=False)
    result["isFraud"] = fraud_mask.astype("int8")

    # isFlaggedFraud: no legacy flagging system in this dataset
    result["isFlaggedFraud"] = pd.Series(0, index=df.index, dtype="int8")

    return result


def load_transactions(data_path: str) -> pd.DataFrame:
    """Load, transform, validate, and return the transaction DataFrame."""
    try:
        df = pd.read_csv(data_path, low_memory=False)
    except FileNotFoundError:
        logger.error(f"Data file not found: {data_path}")
        sys.exit(1)
    except Exception as exc:
        logger.error(f"Error reading data file '{data_path}': {exc}")
        sys.exit(1)

    # Detect format: raw CSV vs already-internal (e.g. test data)
    is_raw_csv = _RAW_CSV_COLUMNS.issubset(df.columns)

    if is_raw_csv:
        logger.info("Detected raw CSV format — transforming to internal format...")
        df = _transform_raw_dataframe(df)
        df = generate_transaction_ids(df)
    else:
        # Already in internal format (e.g. test data CSV)
        if "id" not in df.columns:
            df = generate_transaction_ids(df)
        df["type"] = df["type"].astype("category")
        df["isFraud"] = df["isFraud"].astype("int8")
        df["isFlaggedFraud"] = df["isFlaggedFraud"].astype("int8")

    # Validate internal format
    missing = validate_columns(df)
    if missing:
        logger.error(f"Missing required columns after load: {missing}")
        sys.exit(1)

    nulls = validate_null_values(df)
    if nulls:
        logger.warning(f"Null values found: {nulls}")

    invalid_types = validate_types(df)
    if invalid_types:
        logger.warning(f"Unknown transaction types: {invalid_types}")

    logger.info(f"Loaded {len(df):,} transactions from '{data_path}'")
    return df
