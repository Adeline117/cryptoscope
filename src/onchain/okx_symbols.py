"""Deterministic Hyperliquid-to-OKX base-symbol candidates."""
from __future__ import annotations


def candidates(symbol: str) -> tuple[str, ...]:
    """Prefer an exact OKX symbol, then known HL multiplier-prefix aliases."""
    exact = str(symbol or "").strip().upper()
    if not exact:
        return ()
    ordered = [exact]
    if exact.startswith("K") and len(exact) > 1:
        ordered.append(exact[1:])
    if exact.startswith("1000") and len(exact) > 4:
        ordered.append(exact[4:])
    return tuple(dict.fromkeys(ordered))
