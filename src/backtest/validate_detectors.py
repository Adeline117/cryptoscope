"""Case-based detector validation — measure whether the accuracy upgrades actually
work on KNOWN-outcome cases, not just in isolation.

The honest gap this closes: most upgrades were verified negatively ("doesn't
false-fire"). This runs each detector against labeled ground-truth cases (our own
post-mortems) and tabulates hits/misses — a small-N confusion matrix, N disclosed.
It is NOT a 95%-precision claim; it's "on our N known cases, X are classified
correctly, and here are the misses." Run:  python -m src.backtest.validate_detectors
"""

from __future__ import annotations

import src.config  # noqa: F401  (auto-loads .env)

# (label, token, chain, wallets, ground_truth) — the operator wallets are the
# post-mortem clusters; ground_truth is what the system SHOULD conclude.
CASES = {
    "MAME (证伪:4天新币/分发器/刷币)": {
        "token": "0xe92F7Fe3EAf61DF28b7B75f3FaAB199333c42302", "chain": "bsc",
        "wallets": ["0x095d868f05d2c93c2677e80b8dfd0966455b5181",
                    "0x79f5b4950d49ae3a0b8c22d6233341150f8cdee0"],
        "truth": "false_positive",   # must NOT read as a proven operator
    },
    "ESPORTS (团队多签内幕砸盘)": {
        "token": "0xF39e4b21c84e737Df08e2C3b32541d856f508E48", "chain": "bsc",
        "wallets": ["0x49a0c2366936b115d6877438175ee4b97d6dab7c",
                    "0x1552160b3c36492f17cc9c468a082f9e6cc1f1e3"],
        "truth": "team_custody",     # multisig/treasury, not a trading operator
    },
    "SIREN (真操盘,已派发)": {
        "token": "0x997A58129890bBdA032231A52eD1ddC845fc18e1", "chain": "bsc",
        "wallets": ["0x91dca37856240e5e1906222ec79278b16420dc92",
                    "0x0d0707963952f2fba59dd06f2b425ace40b492fe"],
        "truth": "real_operator",
    },
    "EVAA (真操盘,撤池)": {
        "token": "0xaa036928c9c0Df07d525B55ea8EE690Bb5a628C1", "chain": "bsc",
        "wallets": ["0xd5da17a84314194e348649c89a65143a061f7190",
                    "0x024ee8dc380ad17d955b07149725d518b5cbba67"],
        "truth": "real_operator",
    },
    "PUMPCADE (团队分配,同实体)": {
        "token": "Eg2ymQ2aQqjMcibnmTt8erC6Tvk9PVpJZCxvVPJz2agu", "chain": "solana",
        "wallets": ["4fWfKUDJJzmnUByfBhQ5o3SKG1eMwNwGJGsRxH5mEern",
                    "31iYiwMjW2ycykhZ3B7hEnyLfGugLixymoKMEk1YERvN"],
        "truth": "team_allocation",
    },
}

BLUECHIPS = ["LINK", "WBTC", "USDT", "RLUSD", "weETH", "wstETH", "DAI"]
MEMES = ["SIREN", "ESPORTS", "MAME", "PEPE"]


def _check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return bool(ok)


def run() -> dict:
    from src.onchain.entity_classify import classify_cluster
    from src.onchain.token_registry import is_non_operator
    from src.pipeline.operator_sentinel import _distribution_history, _token_age_days

    passed = total = 0

    print("=== 1. 蓝筹排除(is_non_operator)===")
    for s in BLUECHIPS:
        total += 1; passed += _check(f"{s} 应排除", is_non_operator(s), "excluded" if is_non_operator(s) else "NOT excluded")
    for s in MEMES:
        total += 1; passed += _check(f"{s} 应放行", not is_non_operator(s), "passed" if not is_non_operator(s) else "wrongly excluded")

    print("=== 2. 年龄门(MAME太新应'不可判', 老币可判)===")
    for name, c in CASES.items():
        if c["chain"] != "bsc":
            continue
        dh = _distribution_history(c["token"], c["chain"], c["wallets"])
        prof = dh.get("profile", "")
        if c["truth"] == "false_positive":
            ok = prof.startswith("?")            # young/insufficient → unjudgeable
            total += 1; passed += _check(f"{name} 派发履历应'不可判'", ok, prof)
        elif c["truth"] == "real_operator":
            ok = "聪明庄" in prof                  # old real operator → has distribution history
            total += 1; passed += _check(f"{name} 应判'聪明庄'", ok, prof)

    print("=== 3. 实体分类(团队簇=托管非交易庄)===")
    for name, c in CASES.items():
        if c["chain"] != "bsc":
            continue
        cc = classify_cluster(c["wallets"], c["chain"])
        eoa_share = cc.get("eoa_share_of_members", 1)
        if c["truth"] == "team_custody":
            ok = eoa_share < 0.5                  # mostly multisig/contract
            total += 1; passed += _check(f"{name} 应识别为团队托管", ok, f"{cc['summary']} eoa={eoa_share}")
        elif c["truth"] == "real_operator":
            ok = eoa_share >= 0.5                 # mostly trading EOAs
            total += 1; passed += _check(f"{name} 应识别为交易EOA簇", ok, f"{cc['summary']} eoa={eoa_share}")

    print("=== 4. Solana bundle(团队分配=同实体)===")
    for name, c in CASES.items():
        if c["chain"] not in ("solana", "sol"):
            continue
        try:
            from src.onchain.solana_bundle import shared_fee_payer
            edges = shared_fee_payer(c["wallets"])
            ok = len(edges) > 0
            total += 1; passed += _check(f"{name} 应确认同实体", ok, f"{len(edges)} 条同feePayer/bundle边")
        except Exception as e:
            total += 1; _check(f"{name} bundle检查", False, f"err {str(e)[:50]}")

    print(f"\n=== 混淆(小N，N={total}）: {passed}/{total} 正确 ({passed/max(total,1)*100:.0f}%) ===")
    print("注：N小，非统计显著；这是'已知案例上系统分类对不对'的诚实回放。")
    return {"passed": passed, "total": total}


if __name__ == "__main__":
    run()
