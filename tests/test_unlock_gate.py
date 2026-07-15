"""An unverified unlock calendar must never create an automatic short signal."""


def test_unlock_directional_signal_is_explicitly_parked():
    import src.signals.signal_generator as sg
    assert sg.UNLOCK_DIRECTIONAL_SIGNAL_ENABLED is False
    assert sg.check_token_unlocks() == []


def test_legacy_directional_generator_is_parked_before_detectors_run(monkeypatch):
    import src.signals.signal_generator as sg

    def must_not_run():
        raise AssertionError("legacy detector reached a user-facing signal path")

    monkeypatch.setattr(sg, "check_token_unlocks", must_not_run)
    monkeypatch.setattr(sg, "check_funding_reversion", must_not_run)
    monkeypatch.setattr(sg, "check_dexscreener_boosts", must_not_run)

    assert sg.LEGACY_DIRECTIONAL_SIGNAL_ENABLED is False
    assert sg.generate_signals() == []
