from datetime import datetime, timezone

import pytest

from src.onchain import hyperliquid as hl


def _row(*, interval_h=8, age_ms=0, **overrides):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    row = {
        "fundingRate": "0.0001",
        "fundingTime": str(now_ms + 60_000),
        "nextFundingTime": str(now_ms + 60_000 + interval_h * 3_600_000),
        "ts": str(now_ms - age_ms),
    }
    row.update(overrides)
    return row


def test_okx_funding_annualizes_actual_contract_interval():
    now_ms = 1_000_000_000
    eight_hour = {"fundingRate": "0.0001", "fundingTime": str(now_ms),
                  "nextFundingTime": str(now_ms + 8 * 3_600_000), "ts": str(now_ms)}
    four_hour = {**eight_hour, "nextFundingTime": str(now_ms + 4 * 3_600_000)}

    assert hl._okx_funding_ann(eight_hour, now_ms=now_ms) == pytest.approx(10.95)
    assert hl._okx_funding_ann(four_hour, now_ms=now_ms) == pytest.approx(21.9)


@pytest.mark.parametrize(
    "row",
    [
        _row(nextFundingTime=""),
        _row(interval_h=0),
        _row(interval_h=25),
        _row(age_ms=hl.OKX_FUNDING_MAX_AGE_MS + 1),
    ],
)
def test_okx_funding_rejects_missing_invalid_or_stale_period(row):
    assert hl._okx_funding_ann(row) is None


def test_okx_funding_map_omits_unverifiable_interval():
    valid = _row(interval_h=4)
    invalid = _row(nextFundingTime="")

    got = hl.okx_funding_map(
        ["GOOD", "BAD"],
        fetch=lambda url: {"data": [valid if "GOOD" in url else invalid]},
    )

    assert got["GOOD"] == pytest.approx(21.9)
    assert "BAD" not in got
