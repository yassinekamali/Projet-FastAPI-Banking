"""Abstract base class for the data access layer."""
from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd


class DataAccessLayer(ABC):
    """Abstract interface for transaction data access."""

    @abstractmethod
    def get_all_transactions(
        self,
        page: int,
        limit: int,
        filters: Optional[dict[str, Any]] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return a paginated list of transactions and the total count."""
        ...

    @abstractmethod
    def get_transaction_by_id(
        self, transaction_id: str
    ) -> Optional[dict[str, Any]]:
        """Return a single transaction by ID, or None if not found."""
        ...

    @abstractmethod
    def search_transactions(
        self, criteria: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Return up to 100 transactions matching the given criteria."""
        ...

    @abstractmethod
    def get_unique_types(self) -> list[str]:
        """Return a sorted list of unique transaction type strings."""
        ...

    @abstractmethod
    def get_recent_transactions(self, n: int) -> list[dict[str, Any]]:
        """Return the N transactions with the highest step value."""
        ...

    @abstractmethod
    def delete_transaction(self, transaction_id: str) -> bool:
        """Delete a transaction by ID. Returns True on success."""
        ...

    @abstractmethod
    def get_transactions_by_customer(
        self, customer_id: str, as_origin: bool = True
    ) -> list[dict[str, Any]]:
        """Return all transactions for a customer (as sender or recipient)."""
        ...

    @abstractmethod
    def get_all_customers(
        self, page: int, limit: int
    ) -> tuple[list[str], int]:
        """Return a paginated list of unique customer IDs and the total count."""
        ...

    @abstractmethod
    def get_customer_stats(
        self, customer_id: str
    ) -> Optional[dict[str, Any]]:
        """Return aggregated stats for a customer, or None if not found."""
        ...

    @abstractmethod
    def get_top_customers(self, n: int) -> list[dict[str, Any]]:
        """Return the top N customers by total transaction volume."""
        ...

    @abstractmethod
    def get_customer_timeline(
        self, customer_id: str
    ) -> list[tuple[int, int]]:
        """Return a sorted list of (step, row_index) tuples for a customer."""
        ...

    @abstractmethod
    def get_dataframe(self) -> pd.DataFrame:
        """Return the underlying DataFrame."""
        ...
