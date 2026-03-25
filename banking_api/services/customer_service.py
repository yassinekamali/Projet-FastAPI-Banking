"""Customer service module.

This module provides business logic for customer-related operations
including profile retrieval and top customer rankings.
"""
from fastapi import Request

from banking_api.data.base import DataAccessLayer
from banking_api.exceptions import CustomerNotFoundError
from banking_api.models.customer import Customer, TopCustomer


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


def get_customers(
    request: Request,
    page: int = 1,
    limit: int = 10
) -> tuple[list[str], int]:
    """Get paginated list of customers.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    page : int, optional
        Page number (1-indexed), by default 1.
    limit : int, optional
        Items per page, by default 10.

    Returns
    -------
    tuple[list[str], int]
        List of customer IDs and total count.
    """
    dal = _get_dal(request)
    return dal.get_all_customers(page, limit)


def get_customer_by_id(request: Request, customer_id: str) -> Customer:
    """Get a customer profile by ID.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    customer_id : str
        The customer identifier.

    Returns
    -------
    Customer
        Customer profile data.

    Raises
    ------
    CustomerNotFoundError
        If customer is not found.
    """
    dal = _get_dal(request)
    stats = dal.get_customer_stats(customer_id)

    if stats is None:
        raise CustomerNotFoundError(customer_id)

    return Customer(
        id=stats["id"],
        transactions_count=stats["transactions_count"],
        avg_amount=round(stats["avg_amount"], 2),
        fraudulent=stats["fraudulent"]
    )


def get_top_customers(request: Request, n: int = 10) -> list[TopCustomer]:
    """Get top N customers by total transaction volume.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    n : int, optional
        Number of customers to return, by default 10.

    Returns
    -------
    list[TopCustomer]
        List of top customers with their statistics.
    """
    dal = _get_dal(request)
    top_customers_data = dal.get_top_customers(n)

    return [
        TopCustomer(
            id=c["id"],
            transactions_count=c["transactions_count"],
            total_amount=round(c["total_amount"], 2),
            avg_amount=round(c["avg_amount"], 2)
        )
        for c in top_customers_data
    ]
