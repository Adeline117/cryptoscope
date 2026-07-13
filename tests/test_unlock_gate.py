"""An unverified unlock calendar must never create an automatic short signal."""


def test_unlock_directional_signal_is_explicitly_parked():
    import src.signals.signal_generator as sg
    assert sg.UNLOCK_DIRECTIONAL_SIGNAL_ENABLED is False
    assert sg.check_token_unlocks() == []
