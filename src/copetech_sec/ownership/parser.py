"""Safe Ownership XML parser producing source-faithful normalized rows."""

from __future__ import annotations

import logging
from typing import Any
from xml.etree.ElementTree import Element, tostring

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

from .normalization import TRANSACTION_CODE_MAP, resolve_direction


def _decimal(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = str(raw).strip().replace(",", "").replace("$", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        logging.debug("Non-numeric ownership value %r; treating as missing.", raw)
        return None


def _text(node: Element | None, path: str) -> str | None:
    if node is None:
        return None
    value = node.findtext(path)
    stripped = value.strip() if value is not None else ""
    return stripped or None


def _flag(node: Element | None, tag: str) -> bool:
    if node is None:
        return False
    raw = node.findtext(tag)
    if raw is None:
        raw = node.findtext(f"{tag}/value")
    return (raw or "").strip().lower() in {"1", "true", "y", "yes"}


class OwnershipXmlParser:
    """Parse one Form 3/4/5 XML document without acquisition or signal logic."""

    def parse(self, xml_content: str) -> list[dict[str, Any]]:
        try:
            root = ET.fromstring(xml_content)
            issuer = root.find(".//issuer")
            footnotes = self._footnotes(root)
            owners = self._reporting_owners(root)
            primary_owner = owners[0]
            shared_fields = {
                "ticker": _text(issuer, "issuerTradingSymbol") or "N/A",
                "issuer_cik": _text(issuer, "issuerCik") or "N/A",
                "issuer_name": _text(issuer, "issuerName") or "N/A",
                "document_type": _text(root, "documentType"),
                "period_of_report": _text(root, "periodOfReport"),
                "reporting_owners": owners,
                "owner_count": len(owners),
                "is_joint_filing": len(owners) > 1,
                "rule_10b5_1_plan": _flag(root, "aff10b5One"),
                "remarks": _text(root, "remarks"),
                "not_subject_to_section_16": _flag(
                    root,
                    "notSubjectToSection16",
                ),
            }
            rows: list[dict[str, Any]] = []
            for path, is_derivative, is_holding in (
                (".//nonDerivativeTransaction", False, False),
                (".//nonDerivativeHolding", False, True),
                (".//derivativeTransaction", True, False),
                (".//derivativeHolding", True, True),
            ):
                for node in root.findall(path):
                    try:
                        rows.append(
                            self._row(
                                node,
                                owner=primary_owner,
                                shared_fields=shared_fields,
                                is_derivative=is_derivative,
                                is_holding=is_holding,
                                footnotes=footnotes,
                            )
                        )
                    except Exception as exc:
                        logging.warning(
                            "Error parsing ownership row (%s): %s - XML: %s",
                            path,
                            exc,
                            tostring(node, encoding="unicode")[:200],
                        )
            return rows
        except (ET.ParseError, DefusedXmlException) as exc:
            logging.error(
                "XML Parse Error in ownership form: %s - Content length: %s",
                exc,
                len(xml_content),
            )
            return []

    @staticmethod
    def _footnotes(root: Element) -> dict[str, str]:
        notes: dict[str, str] = {}
        for note in root.findall(".//footnotes/footnote"):
            note_id = (note.get("id") or "").strip()
            text = " ".join((note.text or "").split())
            if note_id and text:
                notes[note_id] = text
        return notes

    @staticmethod
    def _row_footnote_ids(node: Element) -> list[str]:
        return [
            ref.get("id").strip()
            for ref in node.iter("footnoteId")
            if (ref.get("id") or "").strip()
        ]

    @staticmethod
    def _reporting_owners(root: Element) -> list[dict[str, Any]]:
        owners: list[dict[str, Any]] = []
        for owner_node in root.findall(".//reportingOwner"):
            identity = owner_node.find("reportingOwnerId")
            relationship = owner_node.find("reportingOwnerRelationship")
            positions: list[str] = []
            officer_title = None
            if _flag(relationship, "isDirector"):
                positions.append("Director")
            if _flag(relationship, "isOfficer"):
                officer_title = _text(relationship, "officerTitle")
                positions.append(
                    f"Officer ({officer_title})"
                    if officer_title
                    else "Officer"
                )
            if _flag(relationship, "isTenPercentOwner"):
                positions.append("10% Owner")
            if _flag(relationship, "isOther"):
                other = _text(relationship, "otherText")
                positions.append(f"Other ({other})" if other else "Other")
            owners.append(
                {
                    "owner_cik": _text(identity, "rptOwnerCik") or "N/A",
                    "owner_name": _text(identity, "rptOwnerName") or "N/A",
                    "owner_position": ", ".join(positions) or "N/A",
                    "owner_is_director": _flag(relationship, "isDirector"),
                    "owner_is_officer": _flag(relationship, "isOfficer"),
                    "owner_is_ten_percent_owner": _flag(
                        relationship,
                        "isTenPercentOwner",
                    ),
                    "owner_officer_title": officer_title,
                }
            )
        if not owners:
            logging.warning(
                "Ownership document contained no <reportingOwner> block."
            )
            owners.append(
                {
                    "owner_cik": "N/A",
                    "owner_name": "N/A",
                    "owner_position": "N/A",
                    "owner_is_director": False,
                    "owner_is_officer": False,
                    "owner_is_ten_percent_owner": False,
                    "owner_officer_title": None,
                }
            )
        return owners

    def _row(
        self,
        node: Element,
        *,
        owner: dict[str, Any],
        shared_fields: dict[str, Any],
        is_derivative: bool,
        is_holding: bool,
        footnotes: dict[str, str],
    ) -> dict[str, Any]:
        amounts = node.find("transactionAmounts")
        post = node.find("postTransactionAmounts")
        transaction_code = _text(
            node,
            "transactionCoding/transactionCode",
        )
        acquired_disposed_code = _text(
            amounts,
            "transactionAcquiredDisposedCode/value",
        )
        shares = _decimal(_text(amounts, "transactionShares/value"))
        price = _decimal(_text(amounts, "transactionPricePerShare/value"))
        conversion_price = _decimal(
            _text(node, "conversionOrExercisePrice/value")
        )
        is_acquisition, is_disposition = resolve_direction(
            acquired_disposed_code,
            transaction_code,
        )
        value = (
            shares * price
            if shares is not None and price is not None
            else None
        )
        footnote_ids = self._row_footnote_ids(node)
        return {
            **shared_fields,
            "owner_cik": owner["owner_cik"],
            "owner_name": owner["owner_name"],
            "owner_position": owner["owner_position"],
            "owner_is_director": owner["owner_is_director"],
            "owner_is_officer": owner["owner_is_officer"],
            "owner_is_ten_percent_owner": owner[
                "owner_is_ten_percent_owner"
            ],
            "owner_officer_title": owner["owner_officer_title"],
            "security_title": _text(node, "securityTitle/value") or "N/A",
            "transaction_date": _text(node, "transactionDate/value") or "N/A",
            "transaction_code": transaction_code or "N/A",
            "transaction_type": TRANSACTION_CODE_MAP.get(
                transaction_code or "",
                "Unknown",
            ),
            "transaction_form_type": _text(
                node,
                "transactionCoding/transactionFormType",
            ),
            "equity_swap_involved": _flag(
                node.find("transactionCoding"),
                "equitySwapInvolved",
            ),
            "acq_disp_code": acquired_disposed_code or "N/A",
            "is_acquisition": is_acquisition,
            "is_disposition": is_disposition,
            "shares": shares,
            "price_per_share": price,
            "value": value,
            "conversion_exercise_price": conversion_price,
            "exercise_date": _text(node, "exerciseDate/value"),
            "expiration_date": _text(node, "expirationDate/value"),
            "underlying_title": _text(
                node,
                "underlyingSecurity/underlyingSecurityTitle/value",
            ),
            "underlying_shares": _decimal(
                _text(
                    node,
                    "underlyingSecurity/underlyingSecurityShares/value",
                )
            ),
            "shares_owned_after": _decimal(
                _text(post, "sharesOwnedFollowingTransaction/value")
            ),
            "direct_indirect": _text(
                node,
                "ownershipNature/directOrIndirectOwnership/value",
            )
            or "N/A",
            "indirect_ownership_nature": _text(
                node,
                "ownershipNature/natureOfOwnership/value",
            ),
            "is_derivative": is_derivative,
            "is_holding": is_holding,
            "footnote_ids": footnote_ids,
            "footnotes": [
                footnotes[footnote_id]
                for footnote_id in footnote_ids
                if footnote_id in footnotes
            ],
        }
