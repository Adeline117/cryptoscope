# Launch 前向优势验证协议 v3

状态：**已预注册，计划从 2026-08-03 00:00 UTC 起仅前向收集；当前必须先完成来源
burn-in 并 armed**。实现身份常量以 `src/contract/launch_protocol.py` 为准。

## 能回答和不能回答的问题

该协议只检验：在同一前向候选宇宙和同期市场日中，冻结的 Launch 筛选规则是否
比 `WATCH` 对照产生更高的成本后 24 小时纸面效用。通过只代表“值得进入人工小额
真实成交实验的前向纸面迹象”，不代表实盘优势、可复制利润或自动交易授权。

它不能证明：路由报价一定成交、预注册网络费情景等于实际费用、失效线一定执行、
极端行情可退出，也不能把模拟盈利升级为 A4 真实成交证据。该协议检验的是
`SMALL_PROBE` 与 `WATCH` 的发现规则差异，不是最终 A3 成交策略的实盘 edge。

## 冻结项

- 协议 ID：`launch-forward-spa-v3`；事件 `cohort_version=6`；当前协议链仅
  `solana`。EVM Launch 仍可发现和展示，但在完整成本/执行合同建立前不进入 v6。
- 起点：`2026-08-03T00:00:00Z`。更早事件与 v1-v5 永久只作描述。v5 在
  decision-clock、冻结池与追加式价格证据上线前已封存，绝不把旧结果补标进 v6。
- 入组时钟：`detected_at`、`decision_at` 和入口观察都必须位于起点后；账本
  `created_at` 必须在决策后 300 秒内。起点前发现的 backlog 永久隔离，不能在水合后
  补进 v6。起点前必须 armed；若来源门未通过，v6 在边界处永久 breach，不能事后
  改起点、恢复同一 cohort 或回填。
- 试验组：首次冻结决策为 `SMALL_PROBE`；对照组：首次冻结决策为 `WATCH`。
- selector 快照：池创建时钟、流动性、FDV、5 分钟成交额和买卖笔数必须在发现时
  冻结；协议用固定公式重算试验组/对照组、仓位上限和 route 成本，不能信任标签。
- 入口时钟：DEX Screener 精确 token/pair 的首次决策报价观察；必须满足
  `detected_at <= entry_observation.observed_at == decision_at`。
- 成本合同：`discovery_outcome`、版本 1、完整纸面模型。包含冻结仓位下的
  constant-product 路由/DEX/impact 模型和 `$2` 往返 Solana 网络费预注册上限情景。
  `$2` 是保守纸面情景，不是 Solana 官方费率、实测费用或实际成交。
- 主终点：入口观察价起算，读取同一冻结池、同一代币的 24h 已闭合 USD candle，
  再扣冻结的完整纸面成本；经 1% 残值下限转换为 log 增长效用。
- 价格真相：只能使用 append-only `outcome_price_observations`；必须复核事件、周期、
  token、pool、entry/cost hash、candle/target/retrieval 时钟与 deterministic ID。可变
  outcome JSON 仅作缓存，不能单独进入优势样本。
- 固定 look：每组 100、200、400、800、1600、3200 个按入口观察时钟排序的前缀。

规则、终点、起点或成本方法发生任何变化都必须升 cohort/protocol 版本，不能用
新规则重新标记旧事件。

## 来源入组与不可恢复闸门

页面看到一条 websocket 日志不等于完整候选宇宙。每个 v6 事件必须同时存在：

1. 实时 RPC 的 `live_ws` 原始观察和完整 hydration 身份；
2. 不同主机的独立 archive RPC 对同一 finalized slot 区间的原始观察和 hydration；
3. `missing_live=0`、`extra_live=0` 的 `sealed_clean` epoch，且 signature、slot、mint、
   raw hash、identity hash、provider、genesis hash 和对账时钟完全一致；
4. 入账前再次从 append-only SQLite 读取完全相同的 reconciliation proof，不能只信
   事件 JSON 内自洽的 proof。

协议 admission 还要求最近连续 1,440 个 clean epoch、证据年龄不超过 300 秒、sealed
和实时 cursor 相对 epoch 末端的 finalized lag 都不超过 256 slots，并要求实时流与
maintenance 流均 live 且没有 open gap。仅把同一 RPC 换端口不算独立来源。

闸门状态只能按 `scheduled → armed → open` 前进；边界后最多允许 180 秒激活。
未按时 armed+ready 或 open 后任何 readiness 失败都会进入终态 `breached`。公共页面
必须把来源 readiness、持久 admission、纸面 selector 证据和当前人工验证窗分开显示；
任一层缺失都按 blocked，不能用绿色或 A3 掩盖。

## 放行硬门

1. 当前固定前缀的 24h 结果必须全部结算为价格或明确不可得；仍 pending 时不看结果。
2. 固定前缀两组的 24h 主结果都必须 100% 可结算。rug、退市、死池最容易导致结局依赖缺失，因此任何 unavailable/invalid 都阻断正向 edge 判定；不能用剩余样本制造通过。
3. 至少 14 个同时拥有试验和对照的 UTC 日；每组至少 80% 的有效事件位于共享日。
   主检验从首个活跃 UTC 日到最后活跃 UTC 日填满连续日历；任一组某日没有候选时按
   持有现金、log 效用 0 计入，双空仓日也保留为 0，不得压缩成“活跃日序列”。固定
   前缀的全事件平均 log 效用还必须同时优于 WATCH 与现金。
4. 连续 UTC 日历内每日先求平均 log 效用，再用 `arch==8.0.0` 的 SPA/Reality Check
   做 10,000 次 stationary bootstrap，固定 3 个连续日 block 和随机种子。
5. 六次 look 共用 5% family-wise alpha；每次只可用 `0.05/6`。使用最保守的
   SPA `upper` p-value，并要求日均 log 效用差至少 0.02。

任何依赖缺失、数值异常、覆盖不全、同期市场日不足或 p-value 未越过门槛都 fail
closed 为“不可判”。固定 look 的效应不优于 WATCH 时显示“无edge/负”。

## 持续验证和真实成交

看板刷新不能创建新 look：样本数位于两个预注册边界之间时，仍使用上一个固定
前缀。到达下一个边界后重新检验，结论可以降级。即使 v6 通过，也只能显示“发现
规则存在前向纸面 selector edge 迹象”；`real_edge_n=0`、
`execution_edge_eligible=false` 且自动交易永远关闭。A3 仍只是人工小额验证窗；只有
带可核验双向真实成交、完整实际 all-in 成本和退出证据的独立账本才能进入 A4，且
不能与本协议的纸面样本混为一谈。
