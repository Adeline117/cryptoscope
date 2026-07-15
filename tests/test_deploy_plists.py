"""Always-on services must point at the repository venv and remain resource-bounded."""
from __future__ import annotations

import plistlib
from pathlib import Path


def test_hyperliquid_launch_agent_is_read_only_stream_process():
    root = Path(__file__).resolve().parents[1]
    path = root / "deploy" / "com.cryptoscope.hyperliquid.plist"
    with path.open("rb") as handle:
        config = plistlib.load(handle)
    assert config["Label"] == "com.cryptoscope.hyperliquid"
    assert config["ProgramArguments"][-2:] == ["-m", "src.pipeline.hyperliquid_stream"]
    assert config["RunAtLoad"] is True and config["KeepAlive"] is True
    assert config["SoftResourceLimits"]["NumberOfFiles"] <= 512
