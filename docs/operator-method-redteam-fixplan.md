I've read the full target and verified every cited line against the actual code. The findings hold up — the circular `member_set` (line 456), the `set`→`list` frontier ordering (318-319), the cumulative cap (326), the dead `amt * 0` (333), the age-`None` short-circuit (416), the `max(sum_exit_in, 1)` floor (444), the 0.3–0.7 dead-zone (180/182), and the contract-only candidate filter (174) are all real.

Here is the prioritized implementation checklist, ordered by correctness-impact ÷ effort.

---

# FIX PLAN — src/onchain/operator_id.py

## TIER 1 — do first (S effort, each kills a confirmed ground-truth miss)

### F1 — Deterministic `_rotation_frontier` (ordered worklist + weight-priority cap)
- **Hole:** lines 318-319 `seen = set(...); frontier = list(seen)` destroys the caller's inflow order; PYTHONHASHSEED randomizes `list(seen)`; the cumulative `visited_edges >= max_wallets` cap (326) then stops the walk after a *hash-random* subset. **Failure input (EVAA):** 8 emptied wallets → 40 L1 wallets, one of which dumped 30M (`sold`), 39 parked (25M). Run A walks the dumper inside the first 25 → `sold>parked` → `distributing_or_churn` (conf 45). Run B it falls past the cap → `parked>sold` → `present_rotating_confirmed` (conf ~75). Same chain state, opposite verdict.
- **Change:** keep an explicit ordered worklist (`seen` for membership only); replace the count cap with a **weight-priority** cap — at each level sort candidates by `(-amount_received, address)` and take top-K, so the dropped tail is the lowest-value wallets that can't move the sold/parked comparison. Amount is already in `_wallet_outflow_map`. Also delete the `sold += amt * 0` dead branch (333) — resolve contracts explicitly (staking/vault = parked-if-withdrawable, else own bucket) so `parked > sold` isn't computed over a silently shrunk denominator. Return `wallets_walked` list for auditability.
- **Protects:** EVAA (deterministic `present_rotating`).
- **Effort:** S.

### F2 — On-chain-derived age + explicit age-unknown branch + loaded-before-gate
- **Hole:** line 407 `_token_age_days` is a flaky network call; line 416 `age is not None` makes the whole youth gate evaporate on a timeout, dropping a 5-day token into the operator taxonomy. Separately, the gate runs *before* the loaded checks (434/444), so a fast-loaded 9-day operator (`conf 40, dom 8, lg 22%`) is forced to `too_young_to_judge`.
- **Change:** derive age from the earliest transfer you already fetch — `_early_inflow_moralis` (ASC) / `_events_etherscan` see the first transfer's block; one `getBlockByNumber` via the existing `ArchiveRPC` → reproducible age. Treat unknown age as its own flagged branch (`age_unverified`), never a silent fall-through. Evaluate the verified-loaded path (`dom>=5 & lg>=10 & _cluster_holds_onchain`) **before** the youth gate — a verified live loaded stack is judgeable at any age; youth only blocks the *exited/distribute* lifecycle inference.
- **Protects:** CX (stays too-young deterministically), and stops FN-6 (young loaded operator no longer missed) + determinism-#2 flip.
- **Effort:** S.

### F3 — Real supply + coordination gate on the historical holding path (line 444)
- **Hole:** line 444 `len(holding) >= 3 and sum_hold >= max(sum_exit_in, 1)` — no percent-of-supply floor; `max(…,1)` means **3 retail diamond-hands holding 1 token** clear it → `loaded_live_operator` conf 63. This is the MAME-shaped retail false positive with no size gate.
- **Change:** add `totalSupply` selector next to `token_decimals` (line 396, one RPC call) and require `sum_hold / total_supply >= ~8%`; replace `max(sum_exit_in, 1)` with a real floor; require ≥2 of the holding wallets to share a non-CEX/non-disperser root funder (reuse the funder map already computed for `dominant_cluster_wallets`).
- **Protects:** MAME / generic-retail (no false loaded); keeps BASED (genuine 12%+ shared-funder cluster still passes).
- **Effort:** S (M if wiring a fresh funder lookup here).

### F4 — Close the 0.3–0.7 "distributed" dead-zone
- **Hole:** lines 180/182 — `exited` only if `dist>=0.7`, `holding` only if `dist<0.3`. An operator that sold 60% and holds a loaded 40% (`dist=0.60`) is in neither list → `exited==[] and holding==[]` → falls to line 527 → **`treasury` conf 60 (NON_PROMOTABLE)**. SIREN slow-bleed with a big residual lands here and is labeled passive long-hold.
- **Change:** add a middle band `0.3 <= dist < 0.7` → `partial_distributing`, routed into the **same `_exit_destinations` referee** (452-518) the `exited` wallets already use. The destination trace exists; it's just gated behind `dist>=0.7`.
- **Protects:** SIREN (mid-distribution correctly reaches `distributing`, becomes promotable).
- **Effort:** S/M.

---

## TIER 2 — high leverage, M effort

### F5 — `member_set` = funder-verified cluster, not the early cohort (the circularity fix)
- **Hole:** line 456 `member_set = {early exited} | {early holding}` = the top-40 early-inflow wallets with **zero same-entity proof**. `_exit_destinations` scores `to in member_set → move_member`, so any transfer between two *unrelated* early snipers counts as "internal rotation" → `moved_internal >= 0.5` fires `_rotation_frontier` → `present_rotating_confirmed`. This is the MAME false-positive mechanism, and it's definitional (member = "early", not "same entity").
- **Change:** the strong entity signal is **already computed and thrown away** — `conc["dominant_cluster_wallets"]` (line 392) is the shared-funder cluster. Intersect: `member_set = {a for a in early_cohort if a in dominant_cluster_wallets}` (or share its root funder). Cohort-internal churn between unrelated snipers then falls to `move_eoa` → `indeterminate_emptied`/`distributing`, not rotation. Route the `_rotation_frontier` parked-terminal test through the same membership (only count `parked_in_wallets` if the wallet is funder-linked to the seed) — this also fixes the OTC-buyer-parked → false `present_rotating` case (H4).
- **Protects:** MAME (stops being a false positive); makes EVAA `present_rotating` only fire when the rotated stack lands in funder-confirmed operator wallets.
- **Effort:** M.

### F6 — Harden the historical candidate filter (line 174) + subtract pairs/infra
- **Hole:** line 174 skips only confident `"contract"`. `classify_address` returns `"unknown"` on a getCode flake (and for everything on BSC when ArchiveRPC is down) → an old LP/bridge/migration lock passes, lands in `exited` with huge `total_in` + `net~0`, and its outflow to the new pool reads as `exited_by_selling` conf 85. `_historical_ledger` also never subtracts `_token_pairs`/`_infra` at all.
- **Change:** skip candidates whose type ∈ `("contract","unknown")` for the *exited* path (a real trading operator is confidently `eoa`), AND explicitly subtract `_token_pairs(token,chain)` + `_infra(chain)` routers/bridges/disperse. **Nuance to preserve FN-3:** do *not* blanket-drop `multisig`/Safe — keep Safes as operator candidates in both the ledger and the current-graph share; only exclude router/pair/known-infra.
- **Protects:** guards against LP-migration/bridge false `exited_by_selling`; keeps Safe-held operators visible.
- **Effort:** M.

### F7 — Pin `toBlock` + termination-based completeness guard (busy-ETH)
- **Hole:** line 63 `toBlock=latest` is re-evaluated per page, so the tip moves mid-walk; a sell landing between pages splits a top-40 wallet's in/out across the boundary → transient `net<0` → guard (146) flips the whole historical dimension to `available=False` → `unknown`. And on >60k-transfer tokens the `max_pages=60` ceiling truncates history → guard trips **every** busy token (FN-4) — exactly the tokens operators target.
- **Change:** fetch head block once, use fixed `toBlock=head - CONFIRMATIONS` for every page (frozen, replayable window). Re-key the completeness guard off a real invariant — "did the final page genuinely terminate (`len(res)<1000`)" — with a small tolerance for reflection/fee-on-transfer negatives, instead of a transient negative net. When the ETH full-walk is still incomplete, fall back to the BSC-style `_early_inflow_moralis` + RPC balance path already in the file rather than refusing.
- **Protects:** busy ETH operators (BASED-shaped on ETH) stay judgeable and deterministic.
- **Effort:** M.

---

## TIER 3 — actionability + remaining determinism (M/L)

### F8 — Split `loaded_live_operator` into `loaded_accumulating` vs `loaded_dormant`
- **Hole:** lines 434-451 are pure static stock checks (`balanceOf>0`); confidence *scales with holding size* (`45 + int(lg)`), which is backwards for actionability. A 6-month-flat fossil and a cluster that added +40% this week emit the identical "拉盘候选 (pump candidate)" string. This is the BASED failure: correct state label, useless trade signal.
- **Change:** call the already-written-but-uncalled `operator_curve_evm(token, cluster_wallets, chain, from_block=block(now−30d), n_points=8)` (evm_archive.py). Compute net-30d Δ. `Δ > +2% supply` → `loaded_accumulating` (only actionable long); `|Δ| ≤ 2%` → `loaded_dormant` (state only, drop the "拉盘候选" string); `Δ < −2%` → route to distributing. Drive confidence off velocity/recency, not `lg`.
- **Protects:** BASED (correctly `loaded_dormant`, not a false buy signal).
- **Effort:** M/L.

### F9 — Convergence-bounded exit walk (kill the sliding DESC recency window)
- **Hole:** `_exit_destinations`/`_wallet_outflow_map` walk `order=DESC, max_pages=6` = only the ~600 *most recent* transfers. For an emptied early operator the sells happened early; as later dust accrues, the 600-window slides off the real exit → SIREN's `distributing` decays to `indeterminate_emptied` with wall-clock.
- **Change:** you already know `expected_out ≈ total_in − max(net_now,0)`. Page until cumulative resolved outflow of *this token* reaches `expected_out` within tolerance, then stop (convergence bound, not recency slice); large `max_pages` only as backstop; flag `resolved=False` if not reached.
- **Protects:** SIREN (`distributing` stays stable over time).
- **Effort:** M.

### F10 — `same_entity` co-held-basket oracle (the MM/serial-degen killer)
- **Hole:** a market-maker EOA passes all three candidate exclusions (H3) → misread as `exited_by_selling`; serial-degen wallets are flagged but not excluded (weakness #3); shared-funder alone can't separate an MM desk from an operator (FP-6).
- **Change:** add `same_entity(a,b,chain)` combining hardened shared-funder (require batch-funded ≥3, survive `_funder_is_disperser`, raise `min_batch_funder` off 2) with a **co-held low-cap token basket** (Jaccard of each wallet's `GET {wallet}/erc20?chain=` via `moralis_client`). Small idiosyncratic basket overlap = operator fingerprint; huge generic basket = MM/degen → tag `is_market_maker` and drop in the line-174 filter. Route H1/H4/H3 membership through this.
- **Protects:** MAME + MM false positives, structurally.
- **Effort:** L.

---

## TIER 4 — polish (S each)

- **F11 — Remaining-ammo + price/liquidity terminal gate.** Emit `remaining_operator_float_pct = sum_hold / total_supply` on distributing verdicts (SIREN 45%-left vs 3%-left are opposite trades). Add a cheap terminal caveat using DexScreener fields already fetched in `_token_pairs` (currently discarded `liquidity.usd`, `volume.h24`, `priceChange`): if `liquidity < X and volume ≈ 0 and post-collapse` → tag "event已发生" and cap confidence (kills post-dump-flat misfire, HOLE 5 / weakness #6).
- **F12 — `loaded_single_operator` branch (FN-2) + hysteresis.** Add `lg >= 15 and _cluster_holds_onchain and dom < 5` → loaded, so a lone/two-wallet operator isn't dumped to `treasury`. Quantize noisy inputs before the cliffs (round `conf` to 5, `sold_frac` to 0.05) and emit a `borderline` caveat within ±margin, refusing promotion rather than committing a category (stops 54↔56 and 0.50-boundary flips).
- **F13 — Deterministic funder tie-break (upstream, anomaly_screener:582).** `sorted(multi, key=lambda f: (by_funder_pre[f], f))` so which 12 funders get disperser-profiled is reproducible; gate `loaded`/`live` on the `funder_complete` flag.

---

## UPSTREAM DEPENDENCY (not in target file, but gates every verdict)
**FP-1/FP-2 — balance-only clustering.** `entity_clustering._similar_balance_groups` merges ≥3 addresses equal to 3 sig-figs with **no funder corroboration** → an equal-tier airdrop of 40 wallets fabricates one 20% "entity" → `cluster_confidence 83` → `live_operator`/`loaded_live_operator` at the top of the tree (lines 430/434). No fix inside operator_id.py can undo this because the signal arrives pre-merged. **Required:** in `cluster_addresses`, demote the similar-balance edge to a corroborator — only union a similar-balance group if members *also* share a non-disperser funder; and make `_cluster_holds_onchain` re-confirm ≥2 live holders share a root funder before the line-434 loaded path may fire. Highest FP leverage in the whole system; flag it to whoever owns anomaly_screener/entity_clustering.

---

## RESIDUAL GAPS after all fixes (honest unknowns)

1. **OTC / off-DEX sells are still unprovable.** A stack sent to a fresh EOA that is *not* funder-linked is correctly no longer called "parked/rotating," but selling-to-a-buyer's-wallet vs self-custody-move remains genuinely indeterminate on-chain without CEX-deposit labels. Best achievable is `indeterminate_emptied` with a caveat — not a resolved sell/hold.
2. **Unlabeled regional CEX / new launchpad funders.** The disperser guard is a blocklist; a brand-new onramp contract that hasn't been labeled will still merge two retail wallets (or fail to strip an exchange). Funder-grounding reduces but doesn't eliminate this until the label set catches up.
3. **Co-held-basket threshold is uncalibrated.** F10's Jaccard cutoff and "small/idiosyncratic" basket-size gate are new hand-set numbers with no outcome data yet — same class of un-calibrated constant as the 55/85 confidences. They need a labeled backtest before being trusted.
4. **Confidence numbers remain un-backtested.** Even after tying confidence to velocity/recency (F8) and adding hysteresis (F12), no fix here calibrates the numbers against realized outcomes (did the "loaded_accumulating" actually mark up?). That requires an outcome-labeled dataset the codebase doesn't yet have.
5. **BSC completeness is still a proxy.** `_early_inflow_moralis` only sees the early window; F(5) catches late accumulators only via the current-top-holder inflow scan (an added call), and if Moralis quota is exhausted the historical dimension is genuinely blank → `unknown` remains the honest answer, not a defect to fix.
6. **`operator_curve_evm` cost/coverage on BSC.** F8's velocity split depends on archive-node balance-at-block; if the BSC archive RPC is unavailable, `loaded_accumulating` vs `loaded_dormant` collapses back to a static check on that chain.

**Suggested build order:** F1 → F2 → F3 → F4 (Tier 1, one PR each, each maps to a named ground-truth case) → F5, F6, F7 → then Tier 3/4. The upstream similar-balance fix should be scheduled in parallel since it gates the entire `conf>=55` top branch.