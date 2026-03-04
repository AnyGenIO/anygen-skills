# PDF 报告生成（Institutional v10.2）

## 核心原则

`templates/report-style.css` 是组件库，不是固定模板。  
先形成分析结论，再选择组件表达结论。

目标：输出应接近 sell-side 正式研报，而非仪表盘或演示稿。

## 视觉纪律（专业度底线）

- **字体纪律**：正文与标题使用同一 sans 家族（机构出版风），避免花哨字体混搭
- **色彩纪律**：全篇仅 1 个品牌主色（`accent`）+ 语义色（`pos/neg/warn`），禁止渐变滥用
- **层级纪律**：靠字号、字重、边线建立层级，不靠大面积底色
- **密度纪律**：优先信息密度与可扫描性；同页尽量保持 2-4 个信息块

## 唯一固定元素

每份报告必须包含：
1. `page-header`
2. `page-footer`

其余结构、篇幅、组件组合由场景决定。

## 组件体系

### 基础组件（所有报告）

- `cover` / `cover-minimal`
- `section` / `subsection`
- `text` / `list`
- `callout` / `callout-key`
- `table` / `mini-table` / `fin-table`
- `kpi-strip` / `stat-row` / `scenario-row`
- `exhibit-title` / `exhibit-source` / `footnote`

### 长文增强组件（深度/首次覆盖优先）

- `toc`
- `view-panel`
- `layout-2col`
- `valuation-bridge`
- `timeline`
- `risk-matrix`
- `disclosure-box`

### 研究语义组件（提升“像真分析师”）

- `consensus-gap`（一致预期 vs 我们观点）
- `evidence-ladder`（证据分层，支持 `badge-t1/t2/t3`）
- `thesis-grid`（多条投资论点并列）
- `assump-table`（关键假设与敏感性）
- `trigger-board`（催化剂/监控触发器）

深度报告要求：至少使用 1 个研究语义组件。

### 新增图表组件（纯 HTML + CSS）

- `chart`（图表容器）
- `bar-chart`（横向对比）
- `tornado`（敏感性/正负驱动）
- `waterfall`（增减拆解）
- `heatmap`（多维强弱）
- `sparkline`（趋势线）

## 图表选型规则（必须按分析问题选）

- 比较横截面大小：`bar-chart`
- 展示驱动拆解（A 到 B）：`waterfall`
- 展示上下行弹性：`tornado`
- 展示多维相对强弱：`heatmap`
- 展示时间序列方向：`sparkline` + `table`

禁止：为了“好看”堆图。图表必须回答明确问题。

## 图表填值规范

- `bar-fill` 宽度用内联 `style="width: xx%"`
- `wf-bar` 高度用内联 `style="height: xxpx"`
- `heat-cell` 使用 `heat-l1` ~ `heat-l5` 体现强弱
- 每个图必须有 `chart-title` + `chart-source`
- 图中数字与正文/表格口径一致（单位、币种、期间一致）

## 图表最小示例

```html
<div class="chart">
  <div class="chart-head">
    <div class="chart-title">Exhibit 3: Segment Growth Comparison</div>
    <div class="chart-subtitle">FY25E YoY</div>
  </div>
  <div class="bar-chart">
    <div class="bar-row">
      <div class="bar-label">Data Center</div>
      <div class="bar-track"><div class="bar-fill pos" style="width: 78%"></div></div>
      <div class="bar-value">+24.7%</div>
    </div>
    <div class="bar-row">
      <div class="bar-label">Client</div>
      <div class="bar-track"><div class="bar-fill alt" style="width: 42%"></div></div>
      <div class="bar-value">+13.1%</div>
    </div>
  </div>
  <div class="chart-source">Source: Company filings, trade-master estimates</div>
</div>
```

## 生成流程

1. 读取 `config/output.yaml`
2. 读取 `templates/report-style.css`
3. 根据分析场景选择组件（不套固定骨架）
4. 生成 HTML：
   - CSS 内联到 `<style>`
   - 数字优先使用 `num`/右对齐
   - 表格/图表必须带来源行
5. 输出 HTML 到 `{save_dir}/report-YYYY-MM-DD-HHMM.html`
6. PDF 引擎渲染
7. 发送 PDF

## 场景化组合建议

| 场景 | 推荐组合 | 页数 |
|------|----------|------|
| Flash Note | `cover-minimal` + `callout-key` + 1 个 `table`/`bar-chart` | 1-2 |
| 财报分析 | `cover` + `rating-box` + `kpi-strip` + `consensus-gap` + `scenario-row` + `waterfall` | 3-5 |
| 首次覆盖 | `cover` + `toc` + `view-panel` + `thesis-grid` + `layout-2col` + `fin-table` + `assump-table` + `valuation-bridge` + `risk-matrix` + `chart` | 10-20 |
| 赛道扫描 | `cover` + `table` + `heatmap` + `bar-chart` + `evidence-ladder` + `callout-key` | 4-8 |
| 宏观策略 | `cover-minimal` + `timeline` + `sparkline` + `scenario-row` | 2-5 |

## 写作约束（防 AI 痕迹）

- 标题必须是观点句，不是主题词
- 每段只传达一个结论 + 证据 + 含义
- 禁止空泛措辞（如“值得关注”“保持观察”）
- 关键判断后标注证据层级 `[T1]/[T2]/[T3]`，长文优先用 `evidence-ladder` 显式呈现
- 写明核心假设与失效条件

## 数据纪律

- 所有数字必须来自工具结果
- 缺失数据标 `N/A`，并解释缺失原因
- 同一指标口径全篇一致
- 同比/环比/绝对值必须清晰标注
- 表格和图表必须标来源（`table-source` / `exhibit-source` / `chart-source`）

## 合规纪律

- 涉及评级/目标价/持仓时，正文给依据，文末用 `disclosure-box` 披露
- 引用管理层原话必须使用 `quote` + `quote-attr`

## 分页纪律

- 默认自动分页，不滥用 `.page-break`
- `.no-break` 仅用于小模块
- 大表自然跨页，保持表头重复
- **页面填充率检查**：若某页内容高度明显不足（约 <70%），优先移除最近的手动 `.page-break`
- 除封面/附录外，禁止连续两页都出现“大面积空白底部”
- 禁止“为凑页数而分页”；页数由内容密度自然决定（通常 2-5 页，深度覆盖可更长）

## 发布前验收（全部通过才可发送）

1. Headline 是否具备“本次特异性”？
2. 每个核心结论是否有数据/事件支撑？
3. 是否给出至少一个反证或关键风险？
4. 图表是否都回答了明确问题（不是装饰）？
5. 是否包含至少一个“研究语义组件”（`consensus-gap` / `evidence-ladder` / `assump-table` 等）？
6. 是否所有表图均标注来源与单位？
7. 是否存在口径切换或数字冲突？
8. 披露与免责声明是否完整？

## PDF 发送规则

必须使用绝对路径，前缀来自 `config/output.yaml` 的 `absolute_prefix`。

1. 复制 PDF 到 `{workspace_dir}/`
2. 按 `MEDIA:{absolute_prefix}/{workspace_dir}/filename.pdf` 发送
3. 路径必须是绝对路径

## PDF 引擎

渲染引擎与参数全部从 `config/output.yaml` 读取，不在 workflow 内硬编码。
