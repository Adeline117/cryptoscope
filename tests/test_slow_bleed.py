"""Slow-bleed alarm: cumulative vs-baseline distribution that dribbles below the
per-window magnitude gate (the SKYAI audit finding) must still alert — but arm
silently on first evaluation (pre-existing drop is not news), never fire on
reflection tokens, and re-arm per BLEED_STEP so it can't spam every tick."""

from src.pipeline.operator_sentinel import BLEED_STEP


def _bleed_fired(t: dict, cb: float, fired_keys: set[str] | None = None):
    """Mirror of the sentinel's slow-bleed block (pure logic replica for testing)."""
    base = t.get("baseline", {})
    fired = []
    bb = base.get("cluster_balance")
    if (cb is not None and bb and bb > 0 and t.get("balanceof_reliable", True)
            and "庄在卖" not in (fired_keys or set())):
        drop_pct = (bb - cb) / bb * 100
        armed = t.get("bleed_alerted_pct")
        if armed is None:
            t["bleed_alerted_pct"] = max(0.0, drop_pct)
        elif drop_pct >= armed + BLEED_STEP:
            fired.append("慢滴漏")
            t["bleed_alerted_pct"] = drop_pct
    return fired


def test_first_evaluation_arms_without_alert():
    t = {"baseline": {"cluster_balance": 100.0}, "balanceof_reliable": True}
    assert _bleed_fired(t, cb=76.0) == []          # SIREN-like -24%: old news, no alert
    assert t["bleed_alerted_pct"] == 24.0          # armed at current drop


def test_dribble_below_step_stays_silent_then_fires_at_step():
    t = {"baseline": {"cluster_balance": 100.0}, "balanceof_reliable": True,
         "bleed_alerted_pct": 0.0}
    assert _bleed_fired(t, cb=99.7) == []          # -0.3% (SKYAI-like) silent
    assert _bleed_fired(t, cb=96.0) == []          # -4% still under step
    assert _bleed_fired(t, cb=94.9) == ["慢滴漏"]   # -5.1% crosses the 5% step
    assert abs(t["bleed_alerted_pct"] - 5.1) < 1e-9


def test_rearms_per_step_no_spam():
    t = {"baseline": {"cluster_balance": 100.0}, "balanceof_reliable": True,
         "bleed_alerted_pct": 5.1}
    assert _bleed_fired(t, cb=94.0) == []          # -6%: within the armed step
    assert _bleed_fired(t, cb=89.0) == ["慢滴漏"]   # -11%: next step fires


def test_reflection_token_never_fires():
    t = {"baseline": {"cluster_balance": 100.0}, "balanceof_reliable": False,
         "bleed_alerted_pct": 0.0}
    assert _bleed_fired(t, cb=50.0) == []


def test_suppressed_when_sell_alert_already_fired():
    t = {"baseline": {"cluster_balance": 100.0}, "balanceof_reliable": True,
         "bleed_alerted_pct": 0.0}
    assert _bleed_fired(t, cb=80.0, fired_keys={"庄在卖"}) == []
