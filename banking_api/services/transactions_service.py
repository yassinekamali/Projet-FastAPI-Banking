"""Transaction service module.

This module provides business logic for transaction operations
including listing, filtering, searching, and deletion.
"""
import os
from typing import Any, Optional

from fastapi import Request

from banking_api.data.base import DataAccessLayer
from banking_api.exceptions import (
    DeleteNotAllowedError,
    TransactionNotFoundError,
)


def _get_dal(request: Request) -> DataAccessLayer:
    """Get the data access layer from app state.

    Parameters
    ----------
    request : Request
        The FastAPI request object.

    Returns
    -------
    DataAccessLayer
        The data access layer instance.
    """
    dal: DataAccessLayer = request.app.state.dal
    return dal


def get_transactions(
    request: Request,
    page: int = 1,
    limit: int = 10,
    type_filter: Optional[str] = None,
    is_fraud: Optional[int] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None
) -> tuple[list[dict[str, Any]], int]:
    """Get paginated list of transactions with optional filters.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    page : int, optional
        Page number (1-indexed), by default 1.
    limit : int, optional
        Items per page, by default 10.
    type_filter : str, optional
        Filter by transaction type.
    is_fraud : int, optional
        Filter by fraud status (0 or 1).
    min_amount : float, optional
        Minimum transaction amount.
    max_amount : float, optional
        Maximum transaction amount.

    Returns
    -------
    tuple[list[dict[str, Any]], int]
        List of transactions and total count.
    """
    dal = _get_dal(request)
    filters: dict[str, Any] = {}

    if type_filter:
        filters["type"] = type_filter
    if is_fraud is not None:
        filters["isFraud"] = is_fraud
    if min_amount is not None:
        filters["min_amount"] = min_amount
    if max_amount is not None:
        filters["max_amount"] = max_amount

    return dal.get_all_transactions(page, limit, filters if filters else None)


def get_transaction_by_id(request: Request, transaction_id: str) -> dict[str, Any]:
    """Get a single transaction by ID.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    transaction_id : str
        The unique transaction identifier.

    Returns
    -------
    dict[str, Any]
        Transaction data.

    Raises
    ------
    TransactionNotFoundError
        If transaction is not found.
    """
    dal = _get_dal(request)
    transaction = dal.get_transaction_by_id(transaction_id)

    if transaction is None:
        raise TransactionNotFoundError(transaction_id)

    return transaction


def search_transactions(
    request: Request,
    criteria: dict[str, Any]
) -> list[dict[str, Any]]:
    """Search transactions using multiple criteria.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    criteria : dict[str, Any]
        Search criteria including type, isFraud, amount_range.

    Returns
    -------
    list[dict[str, Any]]
        List of matching transactions.
    """
    dal = _get_dal(request)
    return dal.search_transactions(criteria)


def get_transaction_types(request: Request) -> list[str]:
    """Get list of unique transaction types.

    Parameters
    ----------
    request : Request
        The FastAPI request object.

    Returns
    -------
    list[str]
        List of transaction type names.
    """
    dal = _get_dal(request)
    return dal.get_unique_types()


def get_recent_transactions(request: Request, n: int = 10) -> list[dict[str, Any]]:
    """Get the N most recent transactions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    n : int, optional
        Number of transactions to return, by default 10.

    Returns
    -------
    list[dict[str, Any]]
        List of recent transactions.
    """
    dal = _get_dal(request)
    return dal.get_recent_transactions(n)


def delete_transaction(request: Request, transaction_id: str) -> bool:
    """Delete a transaction by ID (test mode only).

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    transaction_id : str
        The unique transaction identifier.

    Returns
    -------
    bool
        True if deletion was successful.

    Raises
    ------
    DeleteNotAllowedError
        If not in test mode.
    TransactionNotFoundError
        If transaction is not found.
    """
    # Check test mode
    test_mode = os.getenv("TEST_MODE", "0").lower() in ("1", "true", "yes")
    if not test_mode:
        raise DeleteNotAllowedError()

    dal = _get_dal(request)

    # Verify transaction exists
    if dal.get_transaction_by_id(transaction_id) is None:
        raise TransactionNotFoundError(transaction_id)

    return dal.delete_transaction(transaction_id)


def get_transactions_by_customer(
    request: Request,
    customer_id: str,
    as_origin: bool = True
) -> list[dict[str, Any]]:
    """Get transactions for a specific customer.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    customer_id : str
        The customer identifier.
    as_origin : bool, optional
        If True, get transactions where customer is sender.
        If False, get transactions where customer is recipient.

    Returns
    -------
    list[dict[str, Any]]
        List of transactions.
    """
    dal = _get_dal(request)
    return dal.get_transactions_by_customer(customer_id, as_origin)
