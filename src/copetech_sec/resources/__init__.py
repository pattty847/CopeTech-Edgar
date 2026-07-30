"""Stable public resource namespaces for :class:`copetech_sec.EdgarClient`."""

from .companies import CompaniesResource
from .filings import FilingsResource
from .financials import FinancialsResource
from .funds import FundsResource
from .institutions import InstitutionsResource
from .ownership import OwnershipResource
from .xbrl import XbrlResource

__all__ = [
    "CompaniesResource",
    "FilingsResource",
    "FinancialsResource",
    "FundsResource",
    "InstitutionsResource",
    "OwnershipResource",
    "XbrlResource",
]
