# 妖币侦测系统升级 — 可靠性 + 验证层(2026-06)

## 为什么做(根因)
系统反复给出**错误结论**,要靠人工逐个抓:SIREN「48%巨鲸」是冻结快照的幽灵、多次「0转账」是 getLogs 静默失败的假零、ESPORTS 被误判「无庄死盘」(实为团队多签借 KuCoin 上市内幕砸盘)、PUMPCADE 把团队整数分配当「上膛庄」、MAME 被当「聪明庄埋伏」(实为 4 天新币 + 分发器供血 + 刷币钱包,「71%派发履历」是 `_distribution_history` 把币诞生前的 0 余额当真实持仓算出的假象)。

**根因(一句话):系统只会「算信号」,没有「数据可信度/验证层」。** 它把"空 getLogs / None 余额 / 过期快照 / 纯结构聚类 / 蓝筹托管 / 新币的派发履历"直接当事实下结论,从不问"这个数据/判断可信吗"。

## 错误类型 → 系统级防御(本次升级)
| 犯过的错 | 根因 | 现在的防御 |
|---|---|---|
| MAME 假"聪明庄" | 派发履历无年龄闸门 | `_distribution_history` age-gate(非零样本<4 或首个持仓样本<14d → "?不可判");注册体检门(币龄/funder扇出/degen钱包/实体类型) |
| ESPORTS"无庄/卡仓"误判 | 结构≠行为,不分实体 | `entity_classify`:burn/cex/multisig/contract/eoa;团队多签+金库 → "团队托管控制,非交易operator" |
| 反复"0转账/已减仓" | RPC 失败被当 0 | 状态化取数:`get_transfer_logs.logs_complete`、`combined_balance_at(strict=True)`→None、netflow 不完整→None(不结论) |
| SIREN"48%幽灵" | 过期快照当现值 | `snapshot_freshness` 强制执行:`_build_series` feed 冻结→不构建信号 |
| 观察名单塞满 LINK/WBTC/USDT | 无蓝筹排除 | `token_registry.is_non_operator`:稳定币/wrapped/LST/蓝筹一律 veto(screener + hunt + backtest 共用) |
| 幻象庄在卖 | balanceOf 漏 decimals | `ArchiveRPC.balance_of` 按 `token_decimals` 缩放(非硬编码 /1e18) |
| 庄在买即崩 | `cpr` 未定义 | `cpr = cur.get("price")` |
| getLogs 漏近期转账 | 块时间硬编码 3s | `ArchiveRPC.seconds_per_block()` 实测(BSC ~0.45s);窗口按时间(~2天)封顶 |
| BSC"数据黑洞" | .env 非全路径加载 | `config.py` import 即自动加载 .env(Moralis/Covalent/Etherscan keys 全路径可用) |

## 新增模块
- `src/onchain/token_registry.py` — 非操盘币种注册表(稳定币/wrapped/LST/蓝筹),单一真相源。
- `src/onchain/entity_classify.py` — 持有人实体分类(burn/cex/multisig/contract/eoa + `is_operator_candidate`);`classify_cluster` 给簇构成。
- `src/onchain/token_identity.py` — 项目身份(官网/社交/币龄/名称 → real_project / anon_meme / unknown,带 age-gate 防误判老币)。
- `src/onchain/catalyst_feed.py` — 临近催化剂(DefiLlama 解锁 + CoinGecko 定价;CEX 上市为诚实 stub,不造假)。

## 关键改动(按子系统)
- **数据层** `evm_archive.py`:decimals 缩放、`seconds_per_block`、`logs_complete`、`combined_balance_at(strict)`。
- **配置** `config.py`:import 时自动加载 .env(python-dotenv 或手写兜底,不覆盖已设值)。
- **哨兵** `operator_sentinel.py`:cpr 修复、动态块时间、`_distribution_history` age-gate、netflow 状态化、`_registration_sanity` 体检门(年龄+funder扇出+degen+实体类型+身份+催化剂)。
- **发现层** `operator_hunt.py`:共享跳过名单、`auto_promote()` 严格自动晋升(过全部硬门才注册哨兵);`anomaly_screener.effective_concentration_signal` 返回 `dominant_cluster_wallets`。
- **二级** `anomaly_screener.py`:蓝筹 veto、watchlist `effective_top_pct` 修正(存真集中度非 score)。
- **校准** `calibrate_weights.py`:`generate_labels()` 从 alert_outcomes 价格涨跌派生 pump/dud 标签;`_load_labels` 冲突时优先 price-derived(灭旧错标如 siren.json=pump)。

## 自动晋升硬门(auto_promote,严格)
全部满足才注册:① shape=隐藏簇/混合 ② funder 解析且非分发器(扇出≤40)③ 币龄≥14d ④ 簇主体是交易 EOA(非多签/金库)⑤ 身份非匿名meme ⑥ 有派发履历(聪明庄)。MAME 类在 ③④⑤ 被拒。

## 纪律(记忆固化)
- **新币的派生指标一律"不可判"**,绝不当确定信号;承重断言前过"廉价体检三件套"(币龄/钱包多样性/funder扇出)。
- **观测 vs 解释分离**:出口前点名无聊替代解释、跑区分检验或降置信度(别把观测脑补成精彩故事)。
- **失败≠0**:RPC/数据失败返回"未知",不返回 0、不下"无活动"结论。
- **结构≠庄家**:钱包聚类只证明协同,贴"庄家"需行为证据(拉过/派发过)。

## 如何验证
```
.venv/bin/python -m pytest tests/ -q          # 全套(含 test_reliability_upgrades.py 29项 + test_snapshot_freshness.py)
.venv/bin/python -m src.ops.health             # 数据源健康 + stale 哨兵
```
回归 6 个哨兵应满足:无幻象庄在卖;ESPORTS=团队多签出货、PUMPCADE=分配盘(非上膛庄);蓝筹被排除;BSC 转账经 Moralis/Covalent 可靠取到。

## 未做 / 延后
- `check_run` 每标的 try/except(单币报错不中止整轮)——延后(避免与 operator_sentinel 的活跃编辑撞车)。
- 权重校准实际生效——需积累 ≥5 pump + ≥5 dud 标签(管道已就绪,随运行自动激活)。
- funder 部分覆盖率细化(`funder_complete` 已做全有/全无 gate)。
