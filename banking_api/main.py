"""Banking Transactions API - Main Application.

This module initializes the FastAPI application, configures the lifespan
context manager for startup/shutdown, and registers all routers.
"""
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from banking_api.data.dataframe_dal import DataFrameDAL
from banking_api.data.loader import load_transactions
from banking_api.exceptions import (
    BankingAPIError,
    CustomerNotFoundError,
    DeleteNotAllowedError,
    InvalidSearchCriteriaError,
    TransactionNotFoundError,
)
from banking_api.routers import customers, fraud, stats, system, transactions
from banking_api.services.fraud_detection_service import compute_fraud_stats
from banking_api.services.stats_service import compute_all_stats

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_data_path() -> str:
    """Get the appropriate data file path based on mode.

    Returns
    -------
    str
        Path to the transaction data CSV file.
    """
    test_mode = os.getenv("TEST_MODE", "0").lower() in ("1", "true", "yes")

    if test_mode:
        relative_path = os.getenv("TEST_DATA_PATH", "data/test_transactions_data.csv")
    else:
        relative_path = os.getenv("DATA_PATH", "data/transactions_data.csv")

    # Resolve relative to project root (parent of banking_api package)
    project_root = Path(__file__).parent.parent
    return str(project_root / relative_path)


def build_customer_timeline(dal: DataFrameDAL) -> dict[str, list[tuple[int, int]]]:
    """Build customer transaction timeline for rapid transaction detection.

    Parameters
    ----------
    dal : DataFrameDAL
        The data access layer instance.

    Returns
    -------
    dict[str, list[tuple[int, int]]]
        Mapping of customer IDs to sorted list of (step, index) tuples.
    """
    df = dal.get_dataframe()[["nameOrig", "step"]].copy()
    df["_idx"] = df.index

    timeline: dict[str, list[tuple[int, int]]] = {}
    for customer_id, group in df.groupby("nameOrig", observed=True):
        timeline[str(customer_id)] = sorted(
            zip(group["step"].astype(int), group["_idx"].astype(int))
        )

    return timeline


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan context manager.

    Handles startup initialization (data loading, precomputation)
    and shutdown cleanup.

    Parameters
    ----------
    app : FastAPI
        The FastAPI application instance.

    Yields
    ------
    None
    """
    # Startup
    logger.info("Starting Banking Transactions API...")

    test_mode = os.getenv("TEST_MODE", "0").lower() in ("1", "true", "yes")
    logger.info(f"Test mode: {'enabled' if test_mode else 'disabled'}")

    # Record start time
    app.state.start_time = datetime.now()
    app.state.load_time = datetime.now()

    # Load transaction data
    data_path = get_data_path()
    logger.info(f"Loading data from: {data_path}")

    try:
        df = load_transactions(data_path)
    except SystemExit:
        logger.error("Failed to load transaction data. Exiting.")
        sys.exit(1)

    # Initialize DAL
    app.state.dal = DataFrameDAL(df)
    logger.info(f"Initialized DAL with {len(df):,} transactions")

    # Build customer timeline for fraud detection
    logger.info("Building customer timeline index...")
    app.state.customer_timeline = build_customer_timeline(app.state.dal)
    logger.info(f"Built timeline for {len(app.state.customer_timeline):,} customers")

    # Precompute statistics
    logger.info("Precomputing statistics...")
    stats_data = compute_all_stats(df)
    fraud_stats = compute_fraud_stats(df)

    app.state.cached_stats = {
        **stats_data,
        **fraud_stats
    }
    logger.info("Statistics precomputed and cached")

    logger.info("Banking Transactions API started successfully!")

    yield

    # Shutdown
    logger.info("Shutting down Banking Transactions API...")


# Create FastAPI application
app = FastAPI(
    title="Banking Transactions API",
    description=(
        "A REST API for banking transaction data with fraud detection capabilities. "
        "Provides endpoints for transaction queries, statistics, fraud analysis, "
        "and customer profiles."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)


# Exception handlers
@app.exception_handler(TransactionNotFoundError)
async def transaction_not_found_handler(
    request: Request,
    exc: TransactionNotFoundError
) -> JSONResponse:
    """Handle TransactionNotFoundError exceptions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    exc : TransactionNotFoundError
        The exception instance.

    Returns
    -------
    JSONResponse
        404 error response.
    """
    return JSONResponse(
        status_code=404,
        content={"detail": exc.message}
    )


@app.exception_handler(CustomerNotFoundError)
async def customer_not_found_handler(
    request: Request,
    exc: CustomerNotFoundError
) -> JSONResponse:
    """Handle CustomerNotFoundError exceptions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    exc : CustomerNotFoundError
        The exception instance.

    Returns
    -------
    JSONResponse
        404 error response.
    """
    return JSONResponse(
        status_code=404,
        content={"detail": exc.message}
    )


@app.exception_handler(InvalidSearchCriteriaError)
async def invalid_search_handler(
    request: Request,
    exc: InvalidSearchCriteriaError
) -> JSONResponse:
    """Handle InvalidSearchCriteriaError exceptions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    exc : InvalidSearchCriteriaError
        The exception instance.

    Returns
    -------
    JSONResponse
        400 error response.
    """
    return JSONResponse(
        status_code=400,
        content={"detail": exc.message}
    )


@app.exception_handler(DeleteNotAllowedError)
async def delete_not_allowed_handler(
    request: Request,
    exc: DeleteNotAllowedError
) -> JSONResponse:
    """Handle DeleteNotAllowedError exceptions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    exc : DeleteNotAllowedError
        The exception instance.

    Returns
    -------
    JSONResponse
        403 error response.
    """
    return JSONResponse(
        status_code=403,
        content={"detail": exc.message}
    )


@app.exception_handler(BankingAPIError)
async def generic_banking_error_handler(
    request: Request,
    exc: BankingAPIError
) -> JSONResponse:
    """Handle generic BankingAPIError exceptions.

    Parameters
    ----------
    request : Request
        The FastAPI request object.
    exc : BankingAPIError
        The exception instance.

    Returns
    -------
    JSONResponse
        500 error response.
    """
    return JSONResponse(
        status_code=500,
        content={"detail": exc.message}
    )


# Register routers
app.include_router(transactions.router)
app.include_router(stats.router)
app.include_router(fraud.router)
app.include_router(customers.router)
app.include_router(system.router)


# Root endpoint
@app.get("/", tags=["Root"])
def root() -> dict[str, str]:
    """Root endpoint with API information.

    Returns
    -------
    dict[str, str]
        Welcome message and documentation links.
    """
    return {
        "message": "Welcome to the Banking Transactions API",
        "docs": "/docs",
        "redoc": "/redoc"
    }


def run_server() -> None:
    """Run the API server using uvicorn.

    This function is the entry point for the console script.
    """
    uvicorn.run(
        "banking_api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


if __name__ == "__main__":
    run_server()
