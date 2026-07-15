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


def test_solana_launch_agent_is_independent_and_resource_bounded():
    root = Path(__file__).resolve().parents[1]
    path = root / "deploy" / "com.cryptoscope.solana-launches.plist"
    with path.open("rb") as handle:
        config = plistlib.load(handle)
    assert config["Label"] == "com.cryptoscope.solana-launches"
    assert config["ProgramArguments"][-2:] == [
        "-m", "src.pipeline.solana_launch_stream"]
    assert config["RunAtLoad"] is True and config["KeepAlive"] is True
    assert config["SoftResourceLimits"]["NumberOfFiles"] <= 512
    assert config["StandardErrorPath"].endswith("solana-launches.err.log")


def test_evm_factory_agent_is_independent_and_resource_bounded():
    root = Path(__file__).resolve().parents[1]
    path = root / "deploy" / "com.cryptoscope.evm-factories.plist"
    with path.open("rb") as handle:
        config = plistlib.load(handle)
    assert config["Label"] == "com.cryptoscope.evm-factories"
    assert config["ProgramArguments"][-2:] == [
        "-m", "src.pipeline.evm_factory_stream"]
    assert config["RunAtLoad"] is True and config["KeepAlive"] is True
    assert config["SoftResourceLimits"]["NumberOfFiles"] <= 512
    assert config["StandardErrorPath"].endswith("evm-factories.err.log")
