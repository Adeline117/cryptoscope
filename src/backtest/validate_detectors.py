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
        # 2026-07-01: second wallet swapped — 0x0d0707… turned out to be Gate.io's
        # hot wallet (Dune label), silently sitting in this case as an "operator EOA".
        "wallets": ["0x91dca37856240e5e1906222ec79278b16420dc92",
                    "0x7467a1ff2f66933057776ebf8a985613904ece0b"],
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


def run(live: bool = True) -> dict:
    """Run the labeled-case validation. `live=True` (default = existing behavior)
    also runs the LIVE balanceOf ground-truth gate, which needs archive RPC + network.
    CI passes `live=False` to run only the deterministic, secretless pure-function
    assertions (sections 1-6); the 24/7 scheduler runs `live=True` so the chain
    arbitrates the holder snapshots every cycle."""
    from src.onchain.entity_classify import classify_cluster
    from src.onchain.token_registry import is_non_operator
    from src.pipeline.operator_sentinel import _distribution_history, _token_age_days

    passed = total = 0

    print("=== 1. 蓝筹排除(is_non_operator)===")
    for s in BLUECHIPS:
        total += 1; passed += _check(f"{s} 应排除", is_non_operator(s), "excluded" if is_non_operator(s) else "NOT excluded")
    for s in MEMES:
        total += 1; passed += _check(f"{s} 应放行", not is_non_operator(s), "passed" if not is_non_operator(s) else "wrongly excluded")

    # Sections 2/3/5 hit the network (distribution_history / classify_cluster /
    # shared_fee_payer make RPC + API calls) and are rate-limit flaky — a 429 once
    # FAILed section 5 here. They are gated on `live` so CI (offline, secretless)
    # runs only the truly pure sections 1/4/6 and never goes red for a non-code
    # reason. The 24/7 scheduler runs them with keys.
    if live:
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
            elif c["truth"] == "team_custody":
                # The RIGHT dump signal for our BSC universe: our micro-caps sell INTO
                # the DEX pool (not CEX deposits — verified via Dune all-history). So
                # the fire we validate is cluster distribution, not cex_flow. ESPORTS
                # team cluster dumped -> distribution_history must register it.
                ok = "聪明庄" in prof
                total += 1; passed += _check(f"{name} 砸盘应被派发履历抓到", ok, prof)

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

    print("=== 4. CEX分类(is_cex_address, 纯函数)===")
    from src.onchain.cex_flow import is_cex_address
    known_binance = "0x4aefa39caeadd662ae31ab0ce7c8c2c9c0a013e8"  # Dune-verified BSC Binance
    total += 1; passed += _check("已知Binance热钱包应=CEX", is_cex_address(known_binance, "bsc"),
                                  "True" if is_cex_address(known_binance, "bsc") else "False")
    dead = "0x000000000000000000000000000000000000dead"
    total += 1; passed += _check("随机地址应≠CEX", not is_cex_address(dead, "bsc"),
                                  "False" if not is_cex_address(dead, "bsc") else "True")
    # NOTE: cex_flow *firing* on a real operator→CEX dump is NOT yet positively
    # validated. The ESPORTS custody cluster (multisig+contract) never deposits
    # directly — it disperses to EOAs that deposit. Validating a fire needs those
    # downstream EOAs; documented gap, not a passing assertion here.

    if live:
        print("=== 5. Solana bundle(团队分配=同实体)===")
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

    print("=== 6. cluster_confidence(纯函数,装弹操盘应打高分)===")
    # The gap this closes: all our KNOWN cases are post-distribution → score low, so
    # we'd never SEEN confidence rank a loaded operator high. It can't be shown on a
    # live loaded operator (we have none on hand), but the scoring LOGIC is validated
    # here: a loaded coordinated cluster scores high, dispersed/uncoordinated low.
    from src.pipeline.anomaly_screener import _cluster_confidence as _cc
    loaded = _cc(dict(largest_entity_pct=30, concentration_gap=25,
                      dominant_cluster_wallets=list(range(15)), funder_complete=True))
    siren_like = _cc(dict(largest_entity_pct=20, concentration_gap=15,
                          dominant_cluster_wallets=list(range(14)), funder_complete=True))
    dispersed = _cc(dict(largest_entity_pct=8, concentration_gap=2,
                         dominant_cluster_wallets=list(range(2)), funder_complete=True))
    whale = _cc(dict(largest_entity_pct=25, concentration_gap=0,
                     dominant_cluster_wallets=[1], funder_complete=True))
    capped = _cc(dict(largest_entity_pct=30, concentration_gap=25,
                      dominant_cluster_wallets=list(range(15)), funder_complete=False))
    total += 1; passed += _check("装弹操盘应≥80", loaded >= 80, str(loaded))
    total += 1; passed += _check("SIREN型真庄应≥60", siren_like >= 60, str(siren_like))
    total += 1; passed += _check("派发后/分散应≤30", dispersed <= 30, str(dispersed))
    total += 1; passed += _check("单一巨鲸(无协同)应≤40", whale <= 40, str(whale))
    total += 1; passed += _check("funder不全应封顶≤45", capped <= 45, str(capped))
    total += 1; passed += _check("区分:装弹>SIREN型>分散", loaded > siren_like > dispersed,
                                 f"{loaded}>{siren_like}>{dispersed}")

    # ---- LIVE GROUND-TRUTH GATE: holder data must match the chain ----
    # The ghost-holder bug (ETH holder lists rebuilt from the token's EARLIEST
    # transfers, returned as current) fabricated two sentinel verdicts and survived
    # for months. It was found by accident. No unit test can catch it, because the
    # data source itself lies — only the chain can arbitrate. This gate asks the
    # chain, on a real token, every run. Needs archive RPC + network → gated on
    # `live` so CI (secretless) skips it; the 24/7 scheduler runs it.
    live_failed = 0
    if live:
        try:
            from src.onchain.evm_archive import ArchiveRPC
            from src.onchain.holder_snapshot import fetch_holders_evm
            for sym, tok, cid, chain in [("WOO(eth)", "0x4691937a7508860f876c9c0a2a617e7d9e945d4b", 1, "ethereum"),
                                         ("SIREN(bsc)", "0x997a58129890bbda032231a52ed1ddc845fc18e1", 56, "bsc")]:
                hs = fetch_holders_evm(tok, chain_id=cid, max_pages=3) or []
                rpc = ArchiveRPC(chain)
                agree = 0
                for h in hs[:3]:
                    real = rpc.balance_of(tok, h["address"])
                    if real is not None and abs(real - float(h["balance"])) < max(1.0, 0.02 * float(h["balance"])):
                        agree += 1
                total += 1
                ok = agree >= 2
                if not ok:
                    live_failed += 1
                passed += _check(f"{sym} holder快照与链上一致(top3)", ok,
                                 f"{agree}/3 一致 — 不一致=数据源在撒谎(幽灵余额)")
        except Exception as e:
            total += 1
            live_failed += 1
            passed += _check("holder链上一致性网关", False, f"无法执行: {str(e)[:50]}")

    print(f"\n=== 混淆(小N，N={total}）: {passed}/{total} 正确 ({passed/max(total,1)*100:.0f}%) ===")
    print("注：N小，非统计显著；这是'已知案例上系统分类对不对'的诚实回放。")
    return {"passed": passed, "total": total, "live": live, "live_failed": live_failed}


if __name__ == "__main__":
    import sys
    # CI / offline use: `--no-live` runs only the deterministic pure-function
    # assertions (no RPC, no keys). Default keeps the full live chain-arbitration.
    res = run(live="--no-live" not in sys.argv)
    if res["passed"] < res["total"]:
        sys.exit(1)   # non-zero exit → CI red / scheduler can detect failure
