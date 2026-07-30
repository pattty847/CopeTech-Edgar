"""Pure ownership-event classifications used by signal aggregation."""

from __future__ import annotations

from datetime import datetime
from typing import Any


SIGNAL_CLASS_MAP = {
    "P": "open_market_buy",
    "S": "open_market_sell",
    "F": "tax_sale",
    "M": "option_exercise",
    "A": "award_or_grant",
    "G": "gift",
    "D": "gift",
    "C": "derivative_conversion",
    "W": "derivative_conversion",
}
ECONOMIC_INTENT_MAP = {
    "open_market_buy": "bullish",
    "open_market_sell": "bearish",
    "tax_sale": "neutral",
    "option_exercise": "neutral",
    "award_or_grant": "compensation",
    "gift": "neutral",
    "derivative_conversion": "neutral",
    "planned_sale_10b5_1": "bearish",
    "holding": "neutral",
    "other": "neutral",
}


class OwnershipSignalClassifier:
    def classify(self, transaction: dict[str, Any]) -> str:
        transaction_code = str(transaction.get("transaction_code") or "").upper()
        price = transaction.get("price_per_share")
        is_derivative = bool(transaction.get("is_derivative"))
        if transaction.get("is_holding"):
            return "holding"
        if transaction_code == "S" and self.indicates_10b5_1_plan(transaction):
            return "planned_sale_10b5_1"
        if transaction_code == "P" and not is_derivative and price not in (None, 0, 0.0):
            return "open_market_buy"
        if transaction_code == "S" and not is_derivative and price not in (None, 0, 0.0):
            return "open_market_sell"
        return SIGNAL_CLASS_MAP.get(transaction_code, "other")

    @staticmethod
    def indicates_10b5_1_plan(transaction: dict[str, Any]) -> bool:
        if transaction.get("rule_10b5_1_plan"):
            return True
        notes = " ".join(
            str(note)
            for note in transaction.get("footnotes") or []
        ).lower()
        return "10b5-1" in notes or "10b5‑1" in notes

    @staticmethod
    def economic_intent(signal_class: str) -> str:
        return ECONOMIC_INTENT_MAP.get(signal_class, "neutral")

    @staticmethod
    def role_weight(role: str) -> float:
        normalized = (role or "").lower()
        if "chief executive" in normalized or "ceo" in normalized:
            return 1.0
        if "chief financial" in normalized or "cfo" in normalized:
            return 0.9
        if (
            "president" in normalized
            or "chief operating" in normalized
            or "coo" in normalized
        ):
            return 0.85
        if "director" in normalized:
            return 0.7
        if "10% owner" in normalized:
            return 0.65
        if "officer" in normalized:
            return 0.75
        return 0.5

    @staticmethod
    def safe_date(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d")
        except ValueError:
            return None
