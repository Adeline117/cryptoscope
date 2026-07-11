"""Number-honesty guard — the alert table can never again be read as a hit rate.

Class-2 error this session: "44% short hit rate" was 40 duplicate SIREN alert rows
from a single 17-minute episode, counted as 40 independent trials. The mechanism was
a bare SUM(hit)/COUNT over the raw `alerts` audit-trail table. `log_alert`'s own
docstring warns "Never compute a hit rate off this table directly; go through
evidence.episodes()" — yet `hit_rate_report()` did exactly that.

This is deliberately NARROW. A blanket "no win_rate outside evidence.py" would be
wrong: `win_rate` is a legitimate field for Kelly sizing, per-wallet skill, and
paper-trade journals — different numbers, not the alert hit-rate. The class-2 risk
is specific: presenting a percentage off the raw alerts table as a hit rate. So the
guard is specific — it freezes `hit_rate_report()` as a labeled RAW-count audit
tally that points to evidence.report(), and asserts the raw-row percentage mechanism
is gone from outcome_tracker.
"""

from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"


def test_hit_rate_report_carries_audit_disclaimer():
    # The output a human actually reads must announce that these are raw fire counts,
    # not independent trials — so it can't be quoted as a hit rate out of context.
    from src.pipeline.outcome_tracker import hit_rate_report
    out = hit_rate_report()
    assert "原始审计计数" in out, "hit_rate_report lost its raw-tally disclaimer"
    assert "evidence.report()" in out, "must point to the honest measurement path"


def test_hit_rate_report_emits_no_bare_direction_percentage():
    # The old fake format was "short 24h: 1/16 命中 (6%)". A percentage on a
    # per-direction raw-row line is exactly the number that got quoted as 44%.
    from src.pipeline.outcome_tracker import hit_rate_report
    out = hit_rate_report()
    for line in out.splitlines():
        if re.search(r"\b(long|short)\b.*\d+h:", line):
            assert "%" not in line, \
                f"per-direction raw line emits a percentage (class-2 fake): {line!r}"


def test_outcome_tracker_source_has_no_raw_rowcount_percentage():
    # Freeze the fix at the source: the specific 44%-generating expressions
    # (dividing a hit-flag SUM by a raw row COUNT into a percentage) must stay gone.
    src = (SRC / "pipeline" / "outcome_tracker.py").read_text()
    # strip comments/docstring mentions so the guard keys on live code, not prose
    banned = [r"hits\s*/\s*n\s*\*\s*100", r"命中 \(\{?[^)]*\*\s*100"]
    for pat in banned:
        assert not re.search(pat, src), \
            f"outcome_tracker reintroduced a raw-row hit-rate percentage: /{pat}/"


def test_evidence_remains_the_hit_rate_authority():
    # Positive anchor: the ONE place a hit-rate percentage is legitimately emitted is
    # evidence, and it ships with a Wilson CI (episode-deduped), never bare.
    ev = (SRC / "pipeline" / "evidence.py").read_text()
    assert "Wilson" in ev and "episode" in ev.lower(), \
        "evidence.py must remain the episode-deduped, CI-carrying hit-rate path"
