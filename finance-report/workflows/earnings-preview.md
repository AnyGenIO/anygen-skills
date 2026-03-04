# 财报预演

标的即将发财报时，提前做情景推演。

## 所需数据能力

- 财报日历 — `python3 scripts/cli.py upcoming_earnings '{"symbol":"XXX"}'`
- 一致预期 — `python3 scripts/cli.py consensus_estimates '{"symbol":"XXX"}'`
- 隐含波动 — `python3 scripts/cli.py options_implied_move '{"symbol":"XXX"}'`
- 历史价格 — `python3 scripts/cli.py price_history '{"symbol":"XXX","period":"6mo"}'`
- 基本面指标 — `python3 scripts/cli.py fundamentals '{"symbol":"XXX"}'`
- 技术指标 — `python3 scripts/cli.py technicals '{"symbol":"XXX"}'`
- 新闻 — `python3 scripts/cli.py news '{"symbol":"XXX"}'`

## 分析框架

### 1. 一致预期
获取 Revenue/EPS 预期。

### 2. 情景矩阵
| 情景 | Revenue | EPS | 关键驱动 | 预期股价反应 |
|------|---------|-----|---------|------------|
| Bull | Beat >X% | Beat >X% | | +X% |
| Base | In-line | In-line | | ±X% |
| Bear | Miss >X% | Miss | | -X% |

注：Beat/Miss 的幅度阈值根据该标的历史波动和行业特点动态判断。

### 3. 期权市场信号
获取隐含波动 vs 历史均值。
可用历史价格回看近 4 次财报窗口实际波动，验证历史平均 move。

### 4. 仓位建议
- 财报前是否调仓？

## 深度分析
加载 `references/earnings-preview.md`。
