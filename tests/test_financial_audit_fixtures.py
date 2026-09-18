from __future__ import annotations

import copy
import json

from analysis.financial_series_audit.fixtures import (
    build_fixture_artifacts,
    build_fixture_artifacts_from_path,
    canonical_json_sha256,
    write_fixture_artifacts,
)


def _concept(label: str, entries: list[dict], *, unit: str = "USD") -> dict:
    return {
        "label": label,
        "description": f"Recorded {label}",
        "units": {unit: entries},
    }


def _fact(
    value: int,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str = "10-K",
) -> dict:
    return {
        "start": f"{int(end[:4]) - 1}-01-01",
        "end": end,
        "val": value,
        "accn": accession,
        "fy": int(end[:4]),
        "fp": "FY",
        "form": form,
        "filed": filed,
    }


def _company_facts() -> dict:
    return {
        "cik": 1818874,
        "entityName": "SoFi Technologies, Inc.",
        "facts": {
            "custom": {
                "UnmappedDisclosure": _concept(
                    "Unmapped disclosure",
                    [_fact(99, "2023-12-31", "2024-02-27", "custom-1")],
                )
            },
            "us-gaap": {
                "DebtInstrumentCarryingAmount": _concept(
                    "Gross debt carrying amount",
                    [_fact(5_259_584_000, "2023-12-31", "2024-02-27", "debt-1")],
                ),
                "Revenues": _concept(
                    "Revenue",
                    [
                        _fact(800, "2022-12-31", "2023-02-28", "old"),
                        _fact(1_000, "2023-12-31", "2024-02-27", "original"),
                        _fact(1_005, "2023-12-31", "2024-03-15", "amendment", form="10-K/A"),
                        _fact(1_000, "2023-12-31", "2025-02-27", "comparative"),
                    ],
                ),
            },
        },
    }


def test_fixture_keeps_accepted_and_control_concepts_with_all_target_repeats():
    artifacts = build_fixture_artifacts(
        _company_facts(),
        symbol="sofi",
        source_url="https://data.sec.gov/example.json",
        retrieved_at="2026-09-15T00:00:00Z",
        target_period_ends=["2023-12-31"],
        controls=[("us-gaap", "DebtInstrumentCarryingAmount")],
    )

    facts = artifacts.company_facts["facts"]
    assert set(facts) == {"us-gaap"}
    assert set(facts["us-gaap"]) == {"Revenues", "DebtInstrumentCarryingAmount"}
    revenues = facts["us-gaap"]["Revenues"]["units"]["USD"]
    assert [entry["accn"] for entry in revenues] == [
        "original",
        "amendment",
        "comparative",
    ]
    assert artifacts.manifest["sourceFactCount"] == 6
    assert artifacts.manifest["fixtureFactCount"] == 4


def test_inventory_preserves_visibility_into_every_source_concept():
    artifacts = build_fixture_artifacts(
        _company_facts(),
        symbol="SOFI",
        source_url="https://data.sec.gov/example.json",
        retrieved_at="2026-09-15T00:00:00Z",
        target_period_ends=["2023-12-31"],
    )

    indexed = {
        (item["taxonomy"], item["concept"]): item
        for item in artifacts.concept_inventory["concepts"]
    }
    assert ("custom", "UnmappedDisclosure") in indexed
    assert indexed[("us-gaap", "Revenues")]["factCount"] == 4
    assert indexed[("us-gaap", "Revenues")]["forms"] == ["10-K", "10-K/A"]
    assert "UnmappedDisclosure" not in artifacts.company_facts["facts"].get("custom", {})


def test_artifacts_and_manifest_hashes_are_deterministic_across_source_order():
    first = _company_facts()
    reordered = copy.deepcopy(first)
    reordered["facts"] = dict(reversed(list(reordered["facts"].items())))
    revenues = reordered["facts"]["us-gaap"]["Revenues"]["units"]["USD"]
    revenues.reverse()
    arguments = {
        "symbol": "SOFI",
        "source_url": "https://data.sec.gov/example.json",
        "retrieved_at": "2026-09-15T00:00:00Z",
        "target_period_ends": ["2023-12-31"],
        "controls": [("us-gaap", "DebtInstrumentCarryingAmount")],
    }

    left = build_fixture_artifacts(first, **arguments)
    right = build_fixture_artifacts(reordered, **arguments)

    assert left.company_facts == right.company_facts
    assert left.concept_inventory == right.concept_inventory
    assert left.manifest == right.manifest
    assert left.manifest["fixtureContentSha256"] == canonical_json_sha256(
        left.company_facts
    )
    assert left.manifest["conceptInventoryContentSha256"] == canonical_json_sha256(
        left.concept_inventory
    )


def test_path_entrypoint_reads_only_the_explicit_local_payload(tmp_path):
    source = tmp_path / "companyfacts.json"
    source.write_text(json.dumps(_company_facts()), encoding="utf-8")

    artifacts = build_fixture_artifacts_from_path(
        source,
        symbol="SOFI",
        source_url="https://data.sec.gov/example.json",
        retrieved_at="2026-09-15T00:00:00Z",
        target_period_ends=["2023-12-31"],
    )

    assert artifacts.manifest["symbol"] == "SOFI"
    assert artifacts.manifest["sourceContentSha256"] == canonical_json_sha256(
        _company_facts()
    )


def test_fixture_writer_creates_the_three_reviewable_artifacts(tmp_path):
    artifacts = build_fixture_artifacts(
        _company_facts(),
        symbol="SOFI",
        source_url="https://data.sec.gov/example.json",
        retrieved_at="2026-09-15T00:00:00Z",
        target_period_ends=["2023-12-31"],
    )

    destination = write_fixture_artifacts(tmp_path / "sofi", artifacts)

    assert {path.name for path in destination.iterdir()} == {
        "companyfacts.json",
        "concept-index.json",
        "manifest.json",
    }
    assert json.loads((destination / "manifest.json").read_text())["symbol"] == "SOFI"
