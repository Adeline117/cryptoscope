"""Deterministic Hyperliquid-to-OKX base-symbol candidates."""
from __future__ import annotations


# Explicit token migrations only. Never add a ticker that merely looks similar: every
# alias can turn a delta-neutral pair into two unrelated assets.
_MIGRATED_BASES: dict[str, tuple[str, ...]] = {
    "MATIC": ("POL",),
}


def candidates(symbol: str) -> tuple[str, ...]:
    """Prefer exact, then verified migration and multiplier-prefix aliases."""
    exact = str(symbol or "").strip().upper()
    if not exact:
        return ()
    ordered = [exact]
    ordered.extend(_MIGRATED_BASES.get(exact, ()))
    if exact.startswith("K") and len(exact) > 1:
        ordered.append(exact[1:])
    if exact.startswith("1000") and len(exact) > 4:
        ordered.append(exact[4:])
    return tuple(dict.fromkeys(ordered))
