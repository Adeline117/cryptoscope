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


def test_first_sample_is_never_a_transition():
    """We don't know the state before the replay window opened. Treating unknown->X as
    an event would make every token's first observation a signal."""
    s = [_s("0xa", "distributing", -0.1, 20), _s("0xa", "distributing", -0.1, 10)]
    for i, x in enumerate(s):
        x["block"] = 100 + i
    assert ro.transitions(s) == []


def test_transition_is_detected_chronologically():
    a = _s("0xa", "loaded_dormant", 0.0, 20); a["block"] = 100
    b = _s("0xa", "distributing", -0.09, 10); b["block"] = 200
    # deliberately out of order — must sort by block, not list order
    t = ro.transitions([b, a])
    assert len(t) == 1
    assert t[0]["from_verdict"] == "loaded_dormant" and t[0]["to_verdict"] == "distributing"


def test_no_directional_transition_is_not_no_edge(monkeypatch):
    """A constant verdict yields zero transitions. That means NOTHING WAS MEASURED —
    it must not be reported as 'no edge'."""
    s = [_s("0xa", "distributing", -0.1, 20), _s("0xa", "distributing", 0.02, 10)]
    for i, x in enumerate(s):
        x["block"] = 100 + i
    out = ro.score_transitions(s)
    assert out["n_directional"] == 0
    assert "无跃迁" in out["note"] and "不是" in out["note"]
    assert "precision" not in out and "lift" not in out


def test_transition_scoring_uses_target_verdict_direction(monkeypatch):
    monkeypatch.setattr("src.pipeline.evidence.base_rate",
                        lambda *a, **k: {"available": True, "p": 0.30})
    a = _s("0xa", "loaded_dormant", 0.0, 30); a["block"] = 100
    b = _s("0xa", "distributing", -0.09, 20); b["block"] = 200   # short flip, hit
    c = _s("0xa", "loaded_accumulating", 0.11, 10); c["block"] = 300  # long flip, hit
    out = ro.score_transitions([a, b, c])
    assert out["n_directional"] == 2 and out["hits"] == 2
    assert "fragile" in out          # expected 0.6 < 2.0


def test_single_sample_never_reports_a_precision(monkeypatch):
    """n=1 printed `precision: 1.0` — the embryo of the next fake 44%. It happened for
    real: deep replay points had no price data, leaving one scored sample."""
    monkeypatch.setattr("src.pipeline.evidence.base_rate",
                        lambda *a, **k: {"available": True, "p": 0.37})
    s = [_s("0xa", "distributing", None, 120), _s("0xa", "distributing", None, 96),
         _s("0xa", "distributing", -0.17, 24)]
    for i, x in enumerate(s):
        x["block"] = 100 + i
    out = ro.score(s)
    assert out["n"] == 1
    assert "precision" not in out and "lift" not in out
    assert "insufficient" in out
    assert out["unpriced_dropped"] == 2
