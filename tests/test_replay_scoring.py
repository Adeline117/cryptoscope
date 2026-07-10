"""A constant verdict must never be published as a lift.

SIREN replayed at 5 blocks across 25 days returned `distributing conf75` at every
one. Precision came out 2/5 = 40% against a 37% chance base rate, i.e. lift 1.08 —
which reads as "slight edge" but is arithmetically zero information: a classifier
that always says the same thing cannot time anything, and its precision is the base
rate by construction.
"""

from src.backtest import replay_operator as ro


def _s(token, verdict, ret, days):
    return {"token": token, "chain": "bsc", "verdict": verdict, "ret": ret,
            "days_ago": days, "confidence": 75}


def test_constant_verdict_suppresses_lift(monkeypatch):
    monkeypatch.setattr("src.pipeline.evidence.base_rate",
                        lambda *a, **k: {"available": True, "p": 0.37})
    samples = [_s("0xsiren", "distributing", r, d)
               for r, d in [(0.03, 25), (-0.03, 20), (-0.08, 15), (-0.14, 10), (-0.01, 5)]]
    out = ro.score(samples)
    assert out["n"] == 5 and out["hits"] == 2
    assert out["constant_verdict"] is True
    assert "lift" not in out, "a constant classifier must not publish a lift"
    assert "零信息" in out["invalid_as_timing_signal"]


def test_varying_verdict_keeps_lift(monkeypatch):
    monkeypatch.setattr("src.pipeline.evidence.base_rate",
                        lambda *a, **k: {"available": True, "p": 0.30})
    samples = [_s("0xa", "distributing", -0.10, 20),
               _s("0xa", "loaded_accumulating", 0.12, 15),
               _s("0xa", "distributing", -0.09, 10),
               _s("0xa", "indeterminate_emptied", 0.01, 5)]
    out = ro.score(samples)
    assert out.get("constant_verdict") is not True
    assert "lift" in out
    assert out["n"] == 3          # indeterminate is non-directional, excluded


def test_unpriced_samples_are_excluded_not_zeroed():
    samples = [_s("0xa", "distributing", None, 20), _s("0xa", "distributing", -0.09, 10)]
    out = ro.score(samples)
    assert out["n"] == 1, "a sample with no price must be dropped, not scored as 0%"


def test_no_directional_verdicts_returns_no_precision():
    out = ro.score([_s("0xa", "indeterminate_emptied", -0.2, 10)])
    assert out["n"] == 0 and "precision" not in out
