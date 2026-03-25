"""DataFrame-backed implementation of DataAccessLayer."""
from typing import Any, Optional

import pandas as pd

from banking_api.data.base import DataAccessLayer


class DataFrameDAL(DataAccessLayer):
    """In-memory data access layer backed by a pandas DataFrame."""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.copy()
        # Timeline is built lazily per customer on first access
        self._timeline: dict[str, list[tuple[int, int]]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        """Convert a raw row dict to a properly typed transaction dict."""
        return {
            "id": str(row["id"]),
            "step": int(row["step"]),
            "type": str(row["type"]),
            "amount": float(row["amount"]),
            "nameOrig": str(row["nameOrig"]),
            "oldbalanceOrg": float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest": str(row["nameDest"]),
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"]),
            "isFraud": int(row["isFraud"]),
            "isFlaggedFraud": int(row["isFlaggedFraud"]),
        }

    # ------------------------------------------------------------------
    # DataAccessLayer interface
    # ------------------------------------------------------------------

    def get_dataframe(self) -> pd.DataFrame:
        return self._df

    def get_all_transactions(
        self,
        page: int,
        limit: int,
        filters: Optional[dict[str, Any]] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        df = self._df

        if filters:
            if "type" in filters:
                df = df[df["type"] == filters["type"]]
            if "isFraud" in filters:
                df = df[df["isFraud"] == filters["isFraud"]]
            if "min_amount" in filters:
                df = df[df["amount"] >= filters["min_amount"]]
            if "max_amount" in filters:
                df = df[df["amount"] <= filters["max_amount"]]

        total = len(df)
        offset = (page - 1) * limit
        page_df = df.iloc[offset: offset + limit]

        transactions = [
            self._row_to_dict(row.to_dict())
            for _, row in page_df.iterrows()
        ]
        return transactions, total

    def get_transaction_by_id(
        self, transaction_id: str
    ) -> Optional[dict[str, Any]]:
        mask = self._df["id"] == transaction_id
        if not mask.any():
            return None
        row = self._df[mask].iloc[0]
        return self._row_to_dict(row.to_dict())

    def search_transactions(
        self, criteria: dict[str, Any]
    ) -> list[dict[str, Any]]:
        df = self._df

        if "type" in criteria:
            df = df[df["type"] == criteria["type"]]
        if "isFraud" in criteria:
            df = df[df["isFraud"] == criteria["isFraud"]]
        if "amount_range" in criteria:
            min_amt, max_amt = criteria["amount_range"]
            df = df[(df["amount"] >= min_amt) & (df["amount"] <= max_amt)]

        return [
            self._row_to_dict(row.to_dict())
            for _, row in df.head(100).iterrows()
        ]

    def get_unique_types(self) -> list[str]:
        return sorted(str(t) for t in self._df["type"].unique())

    def get_recent_transactions(self, n: int) -> list[dict[str, Any]]:
        df = self._df.nlargest(n, "step")
        return [self._row_to_dict(row.to_dict()) for _, row in df.iterrows()]

    def delete_transaction(self, transaction_id: str) -> bool:
        mask = self._df["id"] == transaction_id
        if not mask.any():
            return False
        self._df = self._df[~mask].reset_index(drop=True)
        return True

    def get_transactions_by_customer(
        self, customer_id: str, as_origin: bool = True
    ) -> list[dict[str, Any]]:
        col = "nameOrig" if as_origin else "nameDest"
        df = self._df[self._df[col] == customer_id]
        return [self._row_to_dict(row.to_dict()) for _, row in df.iterrows()]

    def get_all_customers(
        self, page: int, limit: int
    ) -> tuple[list[str], int]:
        unique_customers = sorted(str(c) for c in self._df["nameOrig"].unique())
        total = len(unique_customers)
        offset = (page - 1) * limit
        return unique_customers[offset: offset + limit], total

    def get_customer_stats(
        self, customer_id: str
    ) -> Optional[dict[str, Any]]:
        df = self._df[self._df["nameOrig"] == customer_id]
        if df.empty:
            return None
        return {
            "id": customer_id,
            "transactions_count": len(df),
            "avg_amount": float(df["amount"].mean()),
            "fraudulent": bool(df["isFraud"].any()),
        }

    def get_top_customers(self, n: int) -> list[dict[str, Any]]:
        agg = (
            self._df.groupby("nameOrig", observed=True)
            .agg(
                transactions_count=("amount", "count"),
                total_amount=("amount", "sum"),
                avg_amount=("amount", "mean"),
            )
            .reset_index()
        )
        top = agg.nlargest(n, "total_amount")
        return [
            {
                "id": str(row["nameOrig"]),
                "transactions_count": int(row["transactions_count"]),
                "total_amount": float(row["total_amount"]),
                "avg_amount": float(row["avg_amount"]),
            }
            for _, row in top.iterrows()
        ]

    def get_customer_timeline(
        self, customer_id: str
    ) -> list[tuple[int, int]]:
        if customer_id not in self._timeline:
            mask = self._df["nameOrig"] == customer_id
            sub = self._df[mask][["step"]]
            entries = sorted(
                (int(step), int(idx))
                for idx, step in zip(sub.index, sub["step"])
            )
            self._timeline[customer_id] = entries
        return self._timeline[customer_id]
