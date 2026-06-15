# 妖币庄簇标注 — 手动标注说明

目标:用真实的庄地址簇(从 Arkham 免费网页复制)验证方法,并最终训练自动认庄模型。

## 怎么标(每个币 ~2 分钟)

1. 在 Arkham 网页找到目标币的「控盘者 / Whale」实体页面
2. 复制 `_TEMPLATE.json`,改名为 `<symbol>.json`(如 `siren.json`)
3. 填:
   - `symbol` / `token`(合约地址) / `chain`(bsc/ethereum/solana/base)
   - `outcome`:`pump`(拉过盘)或 `dud`(横死,看着像吸筹但没起来)← **两类都要,各标几个**
   - `max_return`:大概涨了几倍(拉盘组),横死组填 1 或更低
   - `operators`:把 Arkham 那个实体名下的地址粘进来(一行一个字符串)
4. 删掉 `_instructions` 行

## 为什么要两类

只标赢家 = 幸存者偏差,证明不了"能预测"。**必须有横死组对照**:看吸筹判定能不能把"拉盘的"和"看着像吸筹但没起来的"分开。建议各标 5-10 个。

## 标完跑

```
python -m src.backtest.run_labeled_validation
```

→ 对每个币用真实庄簇重建持仓曲线、判定吸筹,输出「赢家吸筹率 vs 横死吸筹率」。
若赢家显著更常吸筹 → 方法证伪通过,值得训自动认庄模型。

## 链覆盖(数据现实)

- **bsc / ethereum / base / arbitrum / optimism**:免费 archive 可重建持仓曲线 ✅
- **solana**:免费可重建 ✅
- 任意链只要给了庄簇地址,持仓曲线都能免费跑(balanceOf 历史)
