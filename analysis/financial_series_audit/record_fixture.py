"""Create a minimized regression fixture from an already-local SEC payload."""

from __future__ import annotations

import argparse
from pathlib import Path

from .fixtures import build_fixture_artifacts_from_path, write_fixture_artifacts


def _concept_key(raw: str) -> tuple[str, str]:
    taxonomy, separator, concept = raw.partition(":")
    if not separator or not taxonomy or not concept:
        raise argparse.ArgumentTypeError("concept must use taxonomy:ConceptName")
    return taxonomy, concept


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Local full Company Facts JSON")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--retrieved-at", required=True, help="UTC acquisition timestamp")
    parser.add_argument("--source-url", help="Defaults to the official Company Facts URL")
    parser.add_argument("--period-end", action="append", required=True)
    parser.add_argument("--control-concept", action="append", type=_concept_key, default=[])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tests/fixtures/sec/companyfacts"),
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    from .fixtures import load_local_company_facts

    payload = load_local_company_facts(args.source)
    cik = f"{int(payload['cik']):010d}"
    source_url = args.source_url or (
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    )
    artifacts = build_fixture_artifacts_from_path(
        args.source,
        symbol=args.symbol,
        source_url=source_url,
        retrieved_at=args.retrieved_at,
        target_period_ends=args.period_end,
        controls=args.control_concept,
    )
    destination = write_fixture_artifacts(
        args.output_dir / args.symbol.lower(), artifacts
    )
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
