"""Dependency-safe console entry points for optional CopeTech services."""

from __future__ import annotations

from importlib import import_module


def run_service() -> None:
    """Start the optional API service or explain how to install it."""

    try:
        app_module = import_module("copetech_sec.app")
    except ModuleNotFoundError as exc:
        if exc.name in {
            "boto3",
            "fastapi",
            "pandas",
            "uvicorn",
            "yfinance",
        }:
            raise SystemExit(
                "The CopeTech SEC API requires optional service dependencies. "
                "Install them with: pip install 'copetech-edgar[service]'"
            ) from None
        raise
    app_module.run()
