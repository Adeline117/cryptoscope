The repo confirms the verifiers' load-bearing facts: `src/onchain/operator_id.py` already exists (target file), `data/cex_labels_bsc.json` is present but there is **no** `cex_labels_eth.json` and **no** `infra_<chain>.json`, and both curve files store raw token counts in `*_pct` fields (BASED `share_peak_pct: 11490243.74`, SIREN `65650333.96` — not percentages). In-repo modules I map to below: `moralis_client`, `cex_addresses`, `cex_flow`, `funder_graph`, `entity_classify`, `entity_clustering`, `holder_snapshot`, `evm_archive`, `token_identity`. Here is the synthesized final spec.

---

# `identify_operator(token) -> Verdict` — FINAL VERIFIED SPEC (v2, no-Dune)

Every fix from the five adversarial passes (EVAA, CX, BASED, SIREN, MAME) is folded in. Changes vs the proposed spec are tagged `[EVAA-n]`, `[CX-n]`, `[BASED-n]`, `[SIREN-n]`, `[MAME-n]`.

## 0. Output contract (extended)

```
Verdict = {
  status: none | too-young-to-judge | dispersed | treasury-only | accumulating |
          loaded-live-operator | distributing | exited-by-selling |
          present-but-rotating-wallets | indeterminate-emptied,
  linkage_confidence:  0.0–1.0,      # how sure the entity is one actor
  lifecycle_confidence:0.0–1.0,      # how sure the status is right
  entities: [ {members, edges:[(a,b,type,strength)], funder, funder_verdict,
               net_to_pool, held_now, per_member_peak_block} ],
  evidence: { per-signal booleans+numbers that fired, each with provenance },
  unknowns: [ explicit strings ],    # MUST be non-empty unless a HIGH/HIGH verdict
  bounds: { window, transfers_pulled, snapshot_block, per_wallet_peak_blocks,
            rpc_ok:bool, transfer_feed_covers_drain:bool, pair_liq_completeness:float,
            token_age_days, nonzero_sample_counts }
}
```
Two confidences reported separately `[BASED-5]`. Persisted verdicts carry `token_age`, `transfers_pulled`, `snapshot_block`, sample counts, tier inline `[MAME-7]`.

### Structural invariants (enforced in code, not convention)
- **INV-1 (anti-snapshot-blindness):** candidate set = live-top-holders ∪ oldest-first early buyers. `none` only after ASC ran non-truncated-and-empty. **INV-1 does NOT apply when `token_age < HISTORY_WINDOW (14d)`** — youth routes to `too-young-to-judge` *before* the operator taxonomy, so a busy young token is never cornered into an operator label `[MAME-2]`.
- **INV-2 (anti-balance→0):** no path writes `exited-by-selling` from a balance delta. Exit needs destination-proof AND market-corroboration, AND-gated across stages.
- **INV-3 (anti-false-present) `[CX-5]`:** `present-but-rotating-wallets` requires a **positive MOVE proof** (destination re-converges to a cluster member, or destination itself buys/holds long-term / shares the cohort fingerprint). Mere *absence* of a sell proof → `indeterminate-emptied`, never present-rotating.
- **INV-4 (data-degradation is UNKNOWN, never optimism) `[SIREN-3/6]`:** if RPC `balanceOf` is unavailable → balances = UNKNOWN and `none/loaded/accumulating/treasury-only` are **forbidden** (LOW + `"live balances unavailable"`). If the transfer feed cannot cover a wallet's drain window → destination = UNRESOLVED (not MOVE). The Moralis `owners` snapshot may **never** substitute for a balance.

---

## STAGE 0 — Token skeleton + exclusion sets + AGE GATE

- DexScreener `/tokens/{addr}` → **all** `pairAddress/dexId/quoteToken/pairCreatedAt(=t0)/liquidity/priceChange/priceUsd`. Store every pair.
- `token_age_days = now − t0`. **AGE GATE `[MAME-1]`:** if `token_age < 14d`, set a hard flag `YOUNG=true`; all history-dependent verdicts (`loaded-live-operator/distributing/exited-by-selling/operator-grade accumulating`) are capped at LOW and carry `"token <14d: pump→distribute lifecycle uncomputable"`. If additionally no mature-history signal exists → `too-young-to-judge` and STOP before Stage 6 operator logic.
- **Pair-completeness** `[EVAA-6]`: `pair_liq_completeness = Σ(known-pair liquidity)/DexScreener total DEX liq`. Record it; <~0.9 makes any "no-Swap → MOVE" leg UNKNOWN.
- Build exclusion sets, chain-keyed, into new `data/research/labels/infra_<chain>.json` (create it — absent today): `PAIRS` (DexScreener ∪ `token0()/token1()` probe), `ROUTERS`, `AGGREGATORS`, `BRIDGES`, `DISPERSE` (`0xD152…2150`), `BURN` (`0x0…0`,`0x…dEaD`), `CEX` = `cex_labels_bsc.json` (632) + on ETH the still-missing `cex_labels_eth.json` (build it `[CX-1]`) + Etherscan-V2 name-tags.

---

## STAGE 1 — Candidate reconstruction (price-curve-driven early window)

1. **Early cohort (ASC):** Moralis `transfers order=ASC`, but the window is **derived from the price curve, not a fixed 72h/5k** `[EVAA-3]`: extend ASC until cumulative net-accumulation is captured **up to the run-up inflection** (first sustained price rise). If truncated before the inflection → **downgrade confidence AND refuse `none`** (not merely emit a string). Net-position per recipient over the window; top ~20 net accumulators. Record each accumulator's `from` addresses `[BASED-4]`.
2. **Live holders:** Moralis `owners` top ~100 — **discovery only, never balances** (SIREN ghost).

`CANDIDATES = top20_early ∪ top100_live` minus obvious PAIRS/BURN/BRIDGE.

---

## STAGE 2 — Entity-eligibility gate (runs on every candidate AND funder, cheapest-decisive first)

1. `eth_getCode` (multicall). Present → EIP-7702 delegate prefix `0xef0100` = EOA-with-delegate (continue); Safe `getThreshold/getOwners` = **MULTISIG → operator-eligible**; `token0/token1/getReserves` = LP_PAIR; vesting/staking/locker = TREASURY_CONTRACT (→ `treasury-only` only); else INFRA (excluded).
2. In BURN/ROUTER/BRIDGE/DISPERSE/AGGREGATOR → EXCLUDED.
3. In PAIRS → LP_PAIR (sell-destination marker).
4. In CEX cache / name-tag → CEX; **as a shared funder the link is VOID**.
5. **Multi-signal wallet classifier `[BASED-6]/[MAME-4]` — gate on counterparty diversity + two-sided flow, NOT raw token count:**
   - **CEX/MM:** distinct counterparties >~100–500 **or** >10k lifetime tx **or** two-sided balanced pool flow across many unrelated tokens. Kept in the entity pass, not silently dropped `[EVAA-8]`; flagged per-wallet `"MM-vs-operator unresolved"`; cluster-level structure may override toward operator.
   - **Serial-degen/churn (new, kills MAME) `[MAME-4]`:** recently-born wallet + traded >~15–20 unrelated micro-cap memes + two-sided flow + short hold times → `serial-degen, not operator`. This bin sits an order of magnitude below the CEX "hundreds" threshold.
   - A wallet holding many tokens but with **few counterparties + buy-only flow + long holds** = operator matrix wallet, NOT MM (BASED shape).
   - **CEX-adjacency test, fan-out-independent `[SIREN-2]`:** flag any wallet whose **first inbound is a single transfer transitively one-hop from the CEX cluster**; and any wallet co-holding ≥3 unrelated known-妖币 whose per-token first-funders differ → `possibly-CEX-unconfirmed`, **barred from being load-bearing** (entity can't reach MEDIUM/HIGH if its net-sold depends on such a member). Catches the low-footprint MEXC/Gate inventory sub-wallet the cache misses.
6. **Genesis-source discriminator `[BASED-4]/[MAME-5]`:** BOUGHT = position arrived **from PAIRS OR from any contract over many irregular swap-sized txns** (resolve the intermediary by probing whether it touches PAIRS — a real router does; BASED's `0x238a3588`). AIRDROP/GRANT = uniform one-shot from deployer/disperse (low weight). **from-pool is necessary-not-sufficient** — pair with age + hold-behavior + token diversity; young+churny+pool-bought → "launch-week degen" branch, not operator-eligible.

Survivors = unlabeled EOAs / verified multisigs, bought-from-pool, not serial-degen, not CEX-adjacent. Every exclusion writes its reason; every unresolved BSC/ETH wallet writes `"possibly-CEX-unconfirmed"` rather than being promoted.

---

## STAGE 3 — Entity clustering (edges are strength-weighted hypotheses)

Per survivor: Moralis native transfers ASC → first-funder. Edges:

- **F1 shared clean funder — GUARDED, chain-aware `[BASED-1]/[MAME-3]/[SIREN-2]/[CX-4]`:** cluster on a shared first-funder ONLY IF funder passes Stage-2 AND is low-fan-out **EOA _or verified-Safe-multisig_** (contract-ness alone never excludes — the SIREN killer was *high fan-out + CEX label*, not contract-ness; this un-breaks BASED's `0xfd09a9cc` multisig). **Fan-out counts ALL assets** (native+stablecoin+token) and lifetime distinct out-counterparties — a 120+-USDT hub with 15 BNB recipients is a disperser (kills MAME). "High fraction bought this token" gets an explicit numerator/denominator (≥50% of funded addrs bought within window), recorded. **On any chain lacking a CEX cache (ETH today), F1 is capped at MEDIUM and is non-load-bearing without a second independent edge**; F1 always requires a *positive* non-CEX proof, not merely "absent from cache."
- **F2 direct transfer A→B — de-circularized `[EVAA-1]`:** a lone A→B to a featureless fresh EOA = `MOVE-unresolved (self-custody vs rotation)` at LOW, **never** HIGH and **never** auto-makes B a member. F2 is same-entity/HIGH only with an **independent qualifier on B**: B also bought-from-pool early, OR B shares timing+size with a third member, OR the link sits in a ≥3-node / convergence structure.
- **T1 split into two edges `[BASED-2]`:** (a) *tight-window co-load* (buys within a tight block/hour window, size CV<0.3); (b) ***same-early-epoch uniform-target-size independent buys*** — slow Sybil accumulation to a uniform target over days is itself a signal (BASED accumulated over ~20d). Each is medium; needs a second edge.
- **B1 co-held basket — STRONG (new, the BASED 铁证) `[BASED-3/5]`:** survivors co-holding ≥~5 identical unrelated low-cap tokens = same-entity; strength scales with count and token rarity. Cheap — reuses Moralis `owners`/wallet-token data. Basket M≥~8 is a HIGH-tier edge alongside funding/direct-transfer/convergence.
- **Convergence sink — STRONG:** survivors send to a common collector = rotation re-pool endpoint.
- **Rotation-frontier fingerprint `[EVAA-2]`:** cluster on shared **downstream sinks** and shared behavioral fingerprint (same buy-block cohort AND same emptying-block cohort) — a medium edge that, aggregated across ≥N wallets, is itself promotable when the funder is voided (exactly EVAA's situation). A voided funder caps entity linkage at MEDIUM — never emit a coordinated verdict off timing alone.

Connected components = entities. **Lifecycle judged per entity, never per wallet.**

---

## STAGE 4 — Exact live balances (RPC, block-pinned)

`eth_call balanceOf` per member at a pinned `snapshot_block`; overrides all snapshots. Per member: held-now and whether emptied since its **peak-balance block** (`argmax(balance)`, recorded). **INV-4:** RPC down → balances UNKNOWN, optimistic verdicts forbidden.

---

## STAGE 5 — Sell-vs-Move referee (block-anchored, recursive, cumulative)

For every emptying member, pull outflows anchored **from its peak-balance block forward, bounded by tx-count not calendar days** `[CX-6]/[SIREN-6]` (not the now-anchored 90d window). If the feed can't cover the drain window → destination UNRESOLVED, and the referee is **forbidden from `present-but-rotating`** `[SIREN-6]`.

Classify each `to` (re-run Stage-2 gate):
```
to ∈ PAIRS/ROUTERS/AGGREGATORS  → SELL(dex)  [needs block-anchored Swap + reserve/price]
to ∈ CEX (deposit-forwarding)   → SELL(cex-pending) → distribution-intent
to ∈ BRIDGE                     → EXIT-CHAIN (unknown)
to = cluster member, OR fresh EOA WITH POSITIVE ROTATION PROOF → MOVE   [INV-3]
to = fresh EOA that only received and sits → ? (indeterminate), NOT MOVE  [CX-1]
to = round-trips back to the funding hot wallet → ? "return-to-funder,
                                   sale-vs-consolidation unresolved"  [EVAA-5]
else                            → UNKNOWN (never default to exited/moved)
```
- **Full-receipt sell discovery `[CX-3]/[EVAA-6]`:** don't only match enumerated PAIRS — scan the suspect tx receipt for **any `Swap` topic0 from any address** and any token `Transfer` into an address that passes a `token0/token1/getReserves` probe. Catches sells through unindexed pools. If `pair_liq_completeness < ~0.9`, a "no-Swap" result is UNKNOWN, not MOVE.
- **Market corroboration is block-anchored `[EVAA-7]/[CX-2]/[SIREN-1]`:** for any drain whose block is **outside** the DexScreener trailing `priceChange` window, the cheap proxy is **forbidden**; use `getReserves(N−1 vs N)`/Swap logs **at the drain tx block**. Proxy admissible only for a drain inside the current window. This is the single highest-leverage SIREN fix — it stops a post-dump flat-price review from misfiring the EVAA verdict.
- **Cumulative corroboration `[SIREN-4]`:** for staged bleed, evaluate cumulative net-token-into-pool vs cumulative reserve/price change across the whole drain window — not per-tx (each small sell fails the per-tx bump test).
- **Rotation-frontier recursion `[EVAA-4]`:** feed MOVE destinations back into Stages 3–5 as new members, bounded depth ≤2–3; compute the entity's net-to-pool across the **whole rotation frontier**, so a rotate-then-dump isn't laundered into permanent present-rotating.
- **Cluster-level netting:** aggregate net-flow-to-pool over the whole entity; sell-A + rebuy-B with price supported nets ≈0 = not distribution. Reflection/tax tokens: net buys vs sells, ignore fee dust.
- **CEX deposit vs hot-wallet round-trip `[EVAA-5]`:** deposit addr (forwards ~100% onward, rests ≈0, verified by a **look-ahead re-poll after N blocks** `[CX-1]`) → SELL(cex-pending) → `distributing`. Round-trip to the *funding* hot wallet is anomalous → `unknown`, not distributing.

**Referee rule (EVAA gate):** drained-to-0 + no Swap on complete pair set + reserves flat + price held (block-anchored) + destination resolves to cluster members / long-term holders → MOVE → `present-but-rotating`. A "sold" claim contradicting intact block-anchored price → cap LOW, flag contradiction, never narrate.

---

## STAGE 6 — Verdict assignment (per entity)

Curve series **normalized to share-of-supply before any threshold** `[BASED data-integrity]` (the `*_pct` fields are raw token counts today).

| Verdict | Exact condition |
|---|---|
| **too-young-to-judge** `[MAME]` | `token_age<14d` and no mature-history signal. Reached BEFORE operator taxonomy; non-promotable. |
| **none** | `token_age≥14d` AND non-truncated ASC ran AND no survivor entity after Stage 2. |
| **dispersed** | deployer/disperse fan-out to uniform fresh wallets that CHURN (two-sided, price volatile), no re-convergence, no from-pool accumulation (MAME airdrop shape). |
| **treasury-only** | entity accumulated AND **zero pool/CEX/bridge outflow across the whole window** ("no sell-venue outflow ever observed"); contract → scheduled linear/step release, price-uncorrelated. |
| **accumulating** | cohort≥threshold AND (uniform CV<0.3 OR guarded low-fan-out shared funder OR basket B1) AND monotone buy-only flow AND float concentration rising while price flat. |
| **loaded-live-operator** | `accumulating` AND positions HELD flat AND price not yet run AND ~zero pool/CEX outflow (BASED). |
| **distributing** | entity net-sold into pool (block-anchored reserve↑/price↓, cumulative) OR confirmed CEX-deposit outflow, staged/ongoing, balances nonzero, correlated with price decline / thinning liq (SIREN). |
| **exited-by-selling** | `distributing` conditions AND balances now ~0 AND cumulative sell destination-proven (pool Swap or CEX deposit) AND market-corroborated (block-anchored). INV-2. |
| **present-but-rotating-wallets** | members emptied to ~0 AND Stage-5 referee = **positive** MOVE proof (destinations re-converge to cluster / hold long-term / share cohort fingerprint), no Swap on complete pair set, block-anchored price+reserves intact (EVAA). INV-3. |
| **indeterminate-emptied** `[CX-5]` | members drained BUT destination AND market both UNRESOLVED (feed doesn't cover drain / RPC down / no positive MOVE proof / no sell proof). NOT present, NOT exited → triggers re-pull, no sentinel. |

---

## (2) Per-verdict confidence & evidence rule table

| Verdict | Min evidence to emit | linkage_conf | lifecycle_conf | Hard caps / downgrades |
|---|---|---|---|---|
| too-young-to-judge | `token_age<14d` | n/a | LOW (fixed) | non-promotable |
| none | ASC non-truncated+empty, age≥14d | n/a | MED–HIGH | LOW if ASC truncated before inflection (then `none` forbidden anyway) |
| dispersed | deployer fan-out + churn + price-volatile | MED | MED | — |
| treasury-only | accumulate + zero sell-venue outflow entire window | per-edge | MED–HIGH | LOW if window truncated or RPC down (INV-4) |
| accumulating | cohort + ≥1 edge + buy-only + concentration↑ | edge-tier | MED–HIGH | LOW if YOUNG or <4 nonzero samples |
| loaded-live-operator | accumulating + held-flat + ~0 outflow | edge-tier | HIGH if B1/funding/convergence; else MED | LOW if YOUNG |
| distributing | net-sold block-anchored OR CEX-deposit confirmed | edge-tier | MED–HIGH | LOW if only trailing-proxy price avail |
| exited-by-selling | distributing + balances~0 + destination-proof + market-corrob | edge-tier | HIGH only if both signals block-anchored & agree | cap LOW on any destination/price contradiction |
| present-but-rotating | emptied + **positive MOVE proof** + block-anchored price/reserve intact + complete pairs | MED if funder voided; HIGH only via B1/convergence/qualified-F2 | MED–HIGH | LOW if pair-incompleteness or feed gap; forbidden if feed can't cover drain |
| indeterminate-emptied | emptied + unresolved dest/market | LOW | LOW | always LOW; re-pull flag |

**Edge → linkage tier:** HIGH = ≥1 guarded funding edge (EOA/verified-multisig, all-asset low-fan-out, non-CEX) OR qualified direct A↔B OR convergence sink OR basket B1(M≥8). MEDIUM = timing+uniform-size only, OR voided-funder cohort via rotation-frontier fingerprint, OR CEX heuristic (not cache) drove exclusion. LOW = any load-bearing input unresolved.

**Hard downgrade triggers (all cap at LOW) `[MAME-6]/[SIREN]`:** `token_age<14d`; entity-history-window <14d; any per-entity derived signal from **<4 nonzero balance samples** (kills the phantom-71%); destination-vs-price contradiction; RPC/feed degradation; pair-incompleteness on a MOVE leg. Timing-only cluster caps at MEDIUM.

---

## (3) Residual GAPS — cases it still cannot resolve (explicit unknowns)

1. **Off-chain coordination (TG/Discord) invisible** — coordination inferred from on-chain uniformity/basket only; intent never confirmed.
2. **CEX off-chain sale size** — deposit proves intent, not execution/size → systematic under-call of `exited-by-selling` to `distributing` on CEX-route exits (SIREN). Documented, not a bug: `"CEX-route exit confirmable only to deposit; execution/size unverifiable without exchange labels."`
3. **BSC/ETH MM-vs-operator without labels** — frequently unresolvable; carried as per-wallet unknown, cluster structure may override but never forces a class.
4. **Funder via bridge/privacy/CEX-internal** — `"funder unresolved; absence of shared funder ≠ absence of operator."`
5. **Rotation vs benign self-custody at N=1** — a lone A→B to a featureless fresh EOA is genuinely ambiguous → `MOVE-unresolved` LOW; only re-convergence / cohort fingerprint / basket promotes it. This is the irreducible EVAA-class residue when the funder is voided and wallets rotate to disjoint fresh addresses.
6. **Return-to-funder round-trips** — sale-vs-consolidation unresolved; carried as unknown, not distributing.
7. **Sub-wallet seeding vs real buyers** — fan-out to fresh wallets with no subsequent pool/CEX outflow → `?`.
8. **Bounded pulls** — `transfers_pulled/window/snapshot_block/per_wallet_peak_blocks` declared; slow pre-window accumulation and inter-sample sells are declared limits.
9. **`indeterminate-emptied` is a real terminal state, not a failure** — when RPC/feed degrade or no positive MOVE/SELL proof exists, the honest answer is "cannot classify → re-pull," and the system must not launder it into either present or exited.
10. **ETH without a CEX cache** — until `cex_labels_eth.json` exists, every ETH shared-funder edge is non-load-bearing (MEDIUM cap); low-throughput ETH CEX withdrawal addresses can still masquerade as operator funders (the "family root" risk is only *mitigated*, not closed, on ETH).

---

## (4) Implementation checklist — stage → data call → in-repo module

**Pre-work (data/artifacts):**
- [ ] Create `data/research/labels/infra_bsc.json` + `infra_eth.json` (routers/aggregators/bridges/disperse/burn). — new, beside `cex_addresses.py`.
- [ ] Build `data/cex_labels_eth.json` (currently absent) from Etherscan-V2 name-tags. `[CX-1]`
- [ ] Normalize `_run_*.curve.json` `*_pct` fields to share-of-supply before any Stage-6 threshold consumes them. `[BASED]`
- [ ] Scrub CEX wallets (`0x0d0707…`Gate, `0x4982085c…`MEXC) from `siren.json`/`bsc_0x997a…json` operators lists. `[SIREN hygiene]`

| Stage | Data call | Module |
|---|---|---|
| 0 skeleton+age+completeness | DexScreener `/tokens/{addr}`; `token0/token1` probe | `token_identity.py`, `holder_snapshot.py` |
| 0 exclusion sets | load `infra_<chain>.json` + `cex_labels_<chain>.json` + Etherscan-V2 tags | `cex_addresses.py`, `entity_classify.py` |
| 1 early cohort (curve-driven ASC) | Moralis `transfers order=ASC`, window→price inflection | `moralis_client.py` + new `_early_window_from_curve()` |
| 1 live holders | Moralis `owners` top100 (discovery only) | `holder_snapshot.py` |
| 2 code/type | `eth_getCode` multicall; Safe/vesting/pair selector probes | `entity_classify.py` (extend) |
| 2 multi-signal + serial-degen + CEX-adjacency | Moralis `getWalletStats` + 1 transfers page; token-holdings; one-hop-from-CEX check | new `wallet_profile.py` / extend `cex_flow.py` |
| 2 genesis-source (pool-vs-deployer, router-probe) | recipient `from` set from Stage 1; `getReserves` probe on intermediary | new `genesis_source.py` |
| 3 F1 all-asset fan-out guard | Moralis native+ERC20 out-transfers of funder; distinct counterparties | `funder_graph.py` (extend to all-asset) |
| 3 F2 qualified / T1a+T1b / B1 basket / convergence / rotation-fingerprint | Moralis native ASC first-inbound; buy-block+empty-block cohorts; co-held token sets | `entity_clustering.py`, `graph_cluster.py` (add B1, fingerprint) |
| 4 exact balances | `eth_call balanceOf` at pinned block; peak-block = `argmax` | `evm_archive.py` |
| 5 outflows (peak-anchored, tx-bounded) | Moralis `from=wallet order=DESC` from peak block | `exit_monitor.py` (re-anchor off `now`) |
| 5 full-receipt Swap discovery | `eth_getLogs`/receipt Swap topic0 + `token0/token1/getReserves` probe on any `to` | `evm_archive.py`, `cex_flow.py` |
| 5 block-anchored + cumulative corroboration | `getReserves(N−1 vs N)` at drain block; cumulative net-to-pool | `evm_archive.py` + new `market_corroborate.py` |
| 5 CEX deposit look-ahead | re-poll `to` transfers after N blocks | `cex_flow.py` |
| 5 rotation-frontier recursion (depth≤3) | recurse Stages 3–5 on MOVE destinations | `operator_id.py` orchestrator |
| 6 verdict + confidence + provenance | pure function over entities | `operator_id.py` |

**Process gate `[MAME-7]`:** `identify_operator` is the **only** path to sentinel registration — no `register()` bypass. LOW / `too-young-to-judge` / `indeterminate-emptied` are **non-promotable**. Every persisted verdict carries `token_age, transfers_pulled, snapshot_block, nonzero_sample_counts, tier` inline.

**Cost bound:** unchanged O(candidates × emptying_txs) plus bounded rotation-frontier recursion (≤3 deep, only on MOVE destinations) — still feasible on busy old tokens.

The two highest-leverage new functions remain the all-asset funder fan-out classifier (`funder_graph`) and the from-pool-vs-deployer genesis discriminator (`genesis_source`); the two highest-leverage new *edges* are **B1 co-held basket** (solves BASED) and the **rotation-frontier fingerprint** (solves EVAA once the funder is voided); the single highest-leverage referee fix is **block-anchored (not trailing-proxy) corroboration** (solves SIREN's post-dump misfire).