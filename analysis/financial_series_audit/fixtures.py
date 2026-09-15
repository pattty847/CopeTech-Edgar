"""Offline construction of compact, provenance-preserving Company Facts fixtures.

The audit acquires SEC payloads separately.  This module only accepts an already-local
JSON document, so fixture generation cannot consume SEC rate-limit capacity or make a
test suite depend on the network.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from copetech_sec.financial_metrics import METRIC_REGISTRY


ConceptKey = tuple[str, str]
FIXTURE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CompanyFactsFixtureArtifacts:
    """The three reviewable artifacts produced from one local SEC payload."""

    company_facts: dict[str, Any]
    concept_inventory: dict[str, Any]
    manifest: dict[str, Any]


def write_fixture_artifacts(
    output_dir: str | Path,
    artifacts: CompanyFactsFixtureArtifacts,
) -> Path:
    """Write the deterministic fixture bundle to an explicit directory."""

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    _write_json(destination / "companyfacts.json", artifacts.company_facts)
    _write_json(destination / "concept-index.json", artifacts.concept_inventory)
    _write_json(destination / "manifest.json", artifacts.manifest)
    return destination


def load_local_company_facts(path: str | Path) -> dict[str, Any]:
    """Load a Company Facts payload from disk without any network fallback."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Company Facts fixture source must be a JSON object")
    _facts_mapping(payload)
    return payload


def accepted_concepts() -> frozenset[ConceptKey]:
    """Return every taxonomy/concept pair accepted by the metric registry."""

    return frozenset(
        (taxonomy, concept)
        for definition in METRIC_REGISTRY.values()
        for taxonomy, concept in definition.concepts
    )


def minimize_company_facts(
    payload: dict[str, Any],
    *,
    target_period_ends: Iterable[str],
    controls: Iterable[ConceptKey] = (),
) -> dict[str, Any]:
    """Keep accepted/control facts for target periods, including every repeat.

    Selection is deliberately by economic period end, not filing fiscal metadata.
    Every matching entry remains present, including amendments and comparative facts
    repeated by later filings.  The production resolver, rather than the recorder,
    must prove that it can choose between them.
    """

    targets = _target_period_ends(target_period_ends)
    selected = accepted_concepts() | _concept_keys(controls)
    minimized_facts: dict[str, dict[str, Any]] = {}

    for taxonomy, taxonomy_facts in sorted(_facts_mapping(payload).items()):
        if not isinstance(taxonomy_facts, dict):
            raise TypeError(f"facts.{taxonomy} must be an object")
        minimized_taxonomy: dict[str, Any] = {}
        for concept, concept_body in sorted(taxonomy_facts.items()):
            if (taxonomy, concept) not in selected:
                continue
            if not isinstance(concept_body, dict):
                raise TypeError(f"facts.{taxonomy}.{concept} must be an object")
            units = concept_body.get("units") or {}
            if not isinstance(units, dict):
                raise TypeError(f"facts.{taxonomy}.{concept}.units must be an object")
            minimized_units: dict[str, list[dict[str, Any]]] = {}
            for unit, entries in sorted(units.items()):
                if not isinstance(entries, list):
                    raise TypeError(
                        f"facts.{taxonomy}.{concept}.units.{unit} must be an array"
                    )
                retained = [
                    dict(entry)
                    for entry in entries
                    if isinstance(entry, dict) and str(entry.get("end") or "") in targets
                ]
                if retained:
                    minimized_units[unit] = sorted(retained, key=_fact_sort_key)
            if minimized_units:
                minimized_taxonomy[concept] = {
                    **{
                        key: value
                        for key, value in sorted(concept_body.items())
                        if key != "units"
                    },
                    "units": minimized_units,
                }
        if minimized_taxonomy:
            minimized_facts[taxonomy] = minimized_taxonomy

    return {
        "cik": payload.get("cik"),
        "entityName": payload.get("entityName"),
        "facts": minimized_facts,
    }


def build_concept_inventory(payload: dict[str, Any]) -> dict[str, Any]:
    """Describe every source concept, including concepts outside current mappings."""

    concepts: list[dict[str, Any]] = []
    for taxonomy, taxonomy_facts in sorted(_facts_mapping(payload).items()):
        if not isinstance(taxonomy_facts, dict):
            raise TypeError(f"facts.{taxonomy} must be an object")
        for concept, concept_body in sorted(taxonomy_facts.items()):
            if not isinstance(concept_body, dict):
                raise TypeError(f"facts.{taxonomy}.{concept} must be an object")
            units = concept_body.get("units") or {}
            if not isinstance(units, dict):
                raise TypeError(f"facts.{taxonomy}.{concept}.units must be an object")
            entries = [
                entry
                for unit_entries in units.values()
                if isinstance(unit_entries, list)
                for entry in unit_entries
                if isinstance(entry, dict)
            ]
            period_ends = sorted(
                {str(entry["end"]) for entry in entries if entry.get("end")}
            )
            concepts.append(
                {
                    "taxonomy": taxonomy,
                    "concept": concept,
                    "label": concept_body.get("label"),
                    "units": sorted(str(unit) for unit in units),
                    "forms": sorted(
                        {str(entry["form"]) for entry in entries if entry.get("form")}
                    ),
                    "factCount": len(entries),
                    "firstPeriodEnd": period_ends[0] if period_ends else None,
                    "lastPeriodEnd": period_ends[-1] if period_ends else None,
                }
            )
    return {
        "schemaVersion": FIXTURE_SCHEMA_VERSION,
        "cik": payload.get("cik"),
        "entityName": payload.get("entityName"),
        "concepts": concepts,
    }


def build_fixture_artifacts(
    payload: dict[str, Any],
    *,
    symbol: str,
    source_url: str,
    retrieved_at: str,
    target_period_ends: Iterable[str],
    controls: Iterable[ConceptKey] = (),
) -> CompanyFactsFixtureArtifacts:
    """Build a minimized fixture, complete inventory, and deterministic manifest."""

    targets = _target_period_ends(target_period_ends)
    control_keys = _concept_keys(controls)
    minimized = minimize_company_facts(
        payload,
        target_period_ends=targets,
        controls=control_keys,
    )
    inventory = build_concept_inventory(payload)
    registry_keys = accepted_concepts()
    manifest = {
        "schemaVersion": FIXTURE_SCHEMA_VERSION,
        "symbol": symbol.upper(),
        "cik": payload.get("cik"),
        "entityName": payload.get("entityName"),
        "sourceUrl": source_url,
        "retrievedAt": retrieved_at,
        "targetPeriodEnds": sorted(targets),
        "controlConcepts": _concept_key_records(control_keys),
        "acceptedConceptRegistrySha256": canonical_json_sha256(
            _concept_key_records(registry_keys)
        ),
        "sourceContentSha256": canonical_json_sha256(
            _canonical_company_facts_payload(payload)
        ),
        "fixtureContentSha256": canonical_json_sha256(minimized),
        "conceptInventoryContentSha256": canonical_json_sha256(inventory),
        "sourceFactCount": _fact_count(payload),
        "fixtureFactCount": _fact_count(minimized),
    }
    return CompanyFactsFixtureArtifacts(minimized, inventory, manifest)


def build_fixture_artifacts_from_path(
    source_path: str | Path,
    **kwargs: Any,
) -> CompanyFactsFixtureArtifacts:
    """Build fixture artifacts from an explicitly supplied local file."""

    return build_fixture_artifacts(load_local_company_facts(source_path), **kwargs)


def canonical_json_sha256(value: Any) -> str:
    """Hash JSON content independently of object insertion order or whitespace."""

    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _facts_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        raise TypeError("Company Facts payload must contain a facts object")
    return facts


def _target_period_ends(values: Iterable[str]) -> frozenset[str]:
    targets = frozenset(str(value).strip() for value in values if str(value).strip())
    if not targets:
        raise ValueError("target_period_ends must contain at least one date")
    return targets


def _concept_keys(values: Iterable[ConceptKey]) -> frozenset[ConceptKey]:
    keys: set[ConceptKey] = set()
    for value in values:
        if len(value) != 2 or not all(str(part).strip() for part in value):
            raise ValueError("concept keys must be (taxonomy, concept) pairs")
        keys.add((str(value[0]).strip(), str(value[1]).strip()))
    return frozenset(keys)


def _concept_key_records(values: Iterable[ConceptKey]) -> list[dict[str, str]]:
    return [
        {"taxonomy": taxonomy, "concept": concept}
        for taxonomy, concept in sorted(values)
    ]


def _fact_sort_key(entry: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(entry.get("end") or ""),
        str(entry.get("start") or ""),
        str(entry.get("filed") or ""),
        str(entry.get("accn") or ""),
        str(entry.get("form") or ""),
        str(entry.get("fy") or ""),
        str(entry.get("fp") or ""),
        str(entry.get("frame") or ""),
        json.dumps(entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
    )


def _fact_count(payload: dict[str, Any]) -> int:
    return sum(
        len(entries)
        for taxonomy_facts in _facts_mapping(payload).values()
        if isinstance(taxonomy_facts, dict)
        for concept_body in taxonomy_facts.values()
        if isinstance(concept_body, dict)
        for entries in (concept_body.get("units") or {}).values()
        if isinstance(entries, list)
    )


def _canonical_company_facts_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize unordered SEC fact collections before hashing source content."""

    canonical = {key: value for key, value in payload.items() if key != "facts"}
    canonical_facts: dict[str, dict[str, Any]] = {}
    for taxonomy, taxonomy_facts in sorted(_facts_mapping(payload).items()):
        if not isinstance(taxonomy_facts, dict):
            raise TypeError(f"facts.{taxonomy} must be an object")
        canonical_taxonomy: dict[str, Any] = {}
        for concept, concept_body in sorted(taxonomy_facts.items()):
            if not isinstance(concept_body, dict):
                raise TypeError(f"facts.{taxonomy}.{concept} must be an object")
            units = concept_body.get("units") or {}
            if not isinstance(units, dict):
                raise TypeError(f"facts.{taxonomy}.{concept}.units must be an object")
            canonical_units: dict[str, Any] = {}
            for unit, entries in sorted(units.items()):
                if not isinstance(entries, list):
                    raise TypeError(
                        f"facts.{taxonomy}.{concept}.units.{unit} must be an array"
                    )
                dictionaries = [entry for entry in entries if isinstance(entry, dict)]
                other_entries = [entry for entry in entries if not isinstance(entry, dict)]
                canonical_units[unit] = [
                    *sorted(dictionaries, key=_fact_sort_key),
                    *sorted(
                        other_entries,
                        key=lambda value: json.dumps(
                            value,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ),
                    ),
                ]
            canonical_taxonomy[concept] = {
                **{
                    key: value
                    for key, value in sorted(concept_body.items())
                    if key != "units"
                },
                "units": canonical_units,
            }
        canonical_facts[taxonomy] = canonical_taxonomy
    canonical["facts"] = canonical_facts
    return canonical
