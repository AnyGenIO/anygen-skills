# Finance Report — 投研PDF报告生成器

## 何时使用

用户要求深度分析、财报分析、赛道扫描、出报告时激活。

## 场景路由

| 用户说 | Workflow |
|--------|----------|
| "分析XX" / "研究XX" | `references/initiating-coverage.md` |
| "XX财报分析" | `workflows/earnings-analysis.md` |
| "XX财报预演" | `workflows/earnings-preview.md` |
| "XX赛道扫描" / "XX vs YY" | `workflows/sector-scan.md` |
| "算XX估值" | `references/valuation-methodologies.md` |

## 执行流程

1. 按对应workflow收集数据（用fin_*工具）
2. 按方法论分析（反偏见、证据分级、变异视角）
3. 生成HTML报告 → 渲染PDF
4. 发送PDF文件

## PDF生成

详见 `workflows/pdf-output.md`。
- CSS样式: `templates/report-style.css`
- 品牌配置: `config/output.yaml`

## 方法论

| 框架 | 参考文件 |
|------|---------|
| 反偏见清单 | `references/anti-bias.md` |
| 证据分级T1/T2/T3 | `references/evidence-hierarchy.md` |
| 变异视角 | `references/variant-view.md` |

## 底线

1. 每个数字来自fin_*工具，不编造
2. 关键判断标注[T1][T2][T3]
3. 每份报告Headline不同，反映当次核心变化
