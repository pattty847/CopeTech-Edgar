from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PHASE0_DIR = Path(__file__).resolve().parent
DB_PATH = PHASE0_DIR / "phase0.duckdb"
BACKTEST_CSV = PHASE0_DIR / "backtest_results.csv"
REPORT_PATH = PHASE0_DIR / "REPORT.md"
START_DATE = "2024-01-01"
END_DATE = "2026-01-01"


@dataclass(frozen=True)
class ManagerSeed:
    cik: str
    display_name: str
    archetype_seed: str


MANAGER_SET = [
    # Verified against SEC submissions JSON on 2026-05-03.
    ManagerSeed("0001067983", "Berkshire Hathaway Inc", "slow_whale"),
    ManagerSeed("0001446194", "Susquehanna International Group, LLP", "market_maker_noise_reference"),
    ManagerSeed("0001423053", "Citadel Advisors LLC", "market_maker_hedge_fund_hybrid"),
    ManagerSeed("0000923093", "Tudor Investment Corp Et Al", "discretionary_fast_money"),
    ManagerSeed("0001135730", "Coatue Management LLC", "tech_growth_concentration"),
    ManagerSeed("0001167483", "Tiger Global Management LLC", "high_turnover_growth"),
    ManagerSeed("0001336528", "Pershing Square Capital Management, L.P.", "concentrated_activist"),
    ManagerSeed("0001040273", "Third Point LLC", "event_driven_activist"),
]
