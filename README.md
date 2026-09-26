# var-rh-eth

This is a separate Variational + Lighter on Robinhood Chain deployment. It does not change or share runtime state with the Lighter mainnet project.

- Lighter API: `https://api.rh.lighter.xyz`
- Lighter WebSocket: `wss://api.rh.lighter.xyz/stream`
- Signing chain ID: `466324`
- Variational forwarder ports: `8866` (WS), `8867` (REST), `8868` (commands)

Use a separate project directory, `.env`, Variational browser profile, and Robinhood Chain Lighter account. Do not copy the mainnet `.env` or `log/` state into this project. The existing strategy thresholds were not calibrated against Robinhood Chain execution; verify market metadata, account positions, and fills with a small controlled run before increasing size. This code change does not start the strategy or submit orders.

邀请链接：
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT)（直升 Bronze，获得 12% 积分加成）
- Robinhood Chain Lighter: [https://robinhoodchain.lighter.xyz](https://robinhoodchain.lighter.xyz)

English version is below.

## 中文

### 概述
`var-rh-eth` 是一个基于 Chrome 插件转发的运行时工具，用于：
1. 跟踪 Variational 订单生命周期，
2. 在终端展示实时看板，
3. 可选地在 Lighter 自动对冲。

交易对会从 Variational 的 REST/WS 消息中自动识别，不需要手动输入 ticker。

### 核心功能
- 记录 Variational/Lighter 的订单关键信息（成交、价格、方向、价差）。
- Rich 终端看板实时展示双边盘口、价差百分比和最近订单。
- 支持 `observe / dry-run / paper / live` 四种模式，默认 `observe` 更安全。
- 支持页面重连与交易资产自动切换（切换后自动重置对应历史窗口）。
- 内置最小 `risk_guard`，可限制对冲计划的最大数量和最大价格偏离，并在 dashboard / CSV / JSONL 中标记失败原因与阶段轨迹。

### 项目结构
- `main.py`：主程序
- `variational/listener.py`：本地接收与监控解析
- `chrome_extension/`：CDP 转发插件

### 环境准备
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows（PowerShell）
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

如需运行 `live` 模式，请创建 `.env` 并填入 Lighter 的真实下单凭证：
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

说明：
- `observe` 不需要 Lighter 交易凭证。
- `dry-run` 需要 Lighter 行情可用，但不发送真实订单。
- `paper` 需要 Lighter 行情可用，会用真实行情自动模拟开平仓并写入 `opportunities.jsonl`，但不发送真实订单。
- `live` 才需要完整的 Lighter 交易凭证。

如需在 Lighter WebSocket 新旧逻辑切换期间临时强制使用旧的应用层 ping/pong 逻辑，可额外设置：
```bash
LIGHTER_WS_SERVER_PINGS=true
```
不设置时默认使用新的兼容模式：客户端依赖 WebSocket protocol ping frame 保活，同时仍兼容旧服务端发出的 `ping` 消息。

### 加载 Chrome 插件
1. 打开 `chrome://extensions`
2. 在右上角开启 `Developer mode`
3. 在左上角点击 `Load unpacked`，选择：
`var-rh-eth/chrome_extension`

### 运行
```bash
python main.py
```

默认等价于观察模式：
```bash
python main.py --mode observe
```

模拟对冲计划但不发送真实订单：
```bash
python main.py --mode dry-run
```

自动模拟机会开平仓但不发送真实订单：
```bash
python main.py --mode paper
```

`paper` 第一版使用 5 分钟跨平台价差中位数作为基准：当前价差相对中位数偏离足够大时模拟开仓，价差回到中位数附近或超过最长持仓时间时模拟平仓。模拟结果会追加写入 `log/opportunities.jsonl`。

常用 paper 参数：
```bash
python main.py --mode paper --paper-notional-usd 30 --paper-entry-deviation-bps 3 --paper-exit-deviation-bps 0.5
```

以更严格的风控阈值运行 dry-run：
```bash
python main.py --mode dry-run --risk-guard-max-base-amount 200 --risk-guard-max-price-deviation-bps 150
```

真实 Lighter 对冲必须显式二次确认：
```bash
python main.py --mode live --confirm-live
```

Python 脚本开始运行后，打开 Variational 的交易页面，
打开 Chrome 插件列表，点击 “Variational CDP Forwarder” -> 点击 `Start`

切换看板语言为英文：
```bash
python main.py --lang en
```

### 风控参数
- `--risk-guard-max-base-amount`
  - 作用：限制生成 Lighter 对冲计划时允许的最大 `base_amount`
  - 默认值：`1000`
- `--risk-guard-max-price-deviation-bps`
  - 作用：限制对冲计划价格相对 Variational 成交价的最大偏离（单位：bps）
  - 默认值：`500`

当触发风控拦截时：
- dashboard 中对应订单会高亮显示
- `order_metrics.jsonl` 会记录 `failure_reason`、`processing_stage`、`stage_history`、`stage_flow_text`
- `trade_records.csv` 会记录 `processing_stage`、`stage_history`、`failure_reason`

### 输出日志
默认目录：`./log`
- `runtime.log`（程序运行日志）
- `order_metrics.jsonl`
- `trade_records.csv`（当前交易记录快照，dashboard 刷新时按最新状态覆盖写）

第一轮 `BTC` 真实小额校准实验方案见：`docs/btc-live-calibration-plan.md`

说明：终端仅用于显示 dashboard。程序不会落盘原始 REST/WS 消息，只会写运行日志、订单指标日志和交易记录 CSV 快照。

其中：
- `order_metrics.jsonl` 适合查看逐条事件快照，例如 `variational_fill`、`lighter_dry_run_plan`、`lighter_error`
- `trade_records.csv` 适合查看每笔订单的最新聚合状态
- dashboard 会显示当前 `mode`、`health`、`risk_guard` 阈值，以及最近订单的阶段轨迹 `Stage Flow`

### 最小复盘
推荐直接从 `order_metrics.jsonl` 做事件流复盘，因为 `trade_records.csv` 是最新快照，旧样本会被覆盖。

按当天事件流统计指定资产：

```powershell
.\.venv\Scripts\python.exe .\tools\analyze_trade_records.py --assets SOL,BTC,ETH --date 2026-05-23
```

如果只想看当前 CSV 快照：

```powershell
.\.venv\Scripts\python.exe .\tools\analyze_trade_records.py --source csv --assets SOL,BTC,ETH
```

默认读取 `log/order_metrics.jsonl`，按 `trade_key` 聚合成每笔 live 尝试的最终状态，输出每个资产的：
- 样本数
- 已完成 `lighter_filled` 数量
- 平均/中位 `live_fill_latency_ms`
- 平均 `live_edge_bps`
- 平均 `fill_diff_var_minus_lighter`
- 平均 `plan_vs_lighter_fill_diff`
- 失败原因统计，例如 `live_cooldown_active`、`hedge_base_amount_exceeds_risk_limit`

分析 `paper` 自动模拟机会：

```powershell
.\.venv\Scripts\python.exe .\tools\analyze_opportunities.py --fee-bps 0.5 --recent 10
```

默认读取 `log/opportunities.jsonl`，按 `opportunity_id` 聚合 `paper_entered` / `paper_closed`，输出总机会数、已平仓数、平均/中位 PnL、持仓时间、退出原因和按方向统计。`--fee-bps` 是粗略手续费敏感性检查，按开仓双边和平仓双边共 4 条腿估算。

---

## English

Referral Links:
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT) (instant Bronze tier + 12% points bonus)
- Robinhood Chain Lighter: [https://robinhoodchain.lighter.xyz](https://robinhoodchain.lighter.xyz)

### Overview
`var-rh-eth` is a Chrome-extension-assisted runtime for:
1. tracking Variational order lifecycle,
2. showing a terminal dashboard,
3. optionally auto-hedging on Lighter.

Ticker is auto-derived from incoming Variational REST/WS messages.

### Core Features
- Tracks key Variational/Lighter order data (fills, prices, direction, spread).
- Rich terminal dashboard for live two-venue quotes, spread percentages, and recent orders.
- Supports `observe / dry-run / paper / live` modes, with `observe` as the safe default.
- Handles page reconnects and automatic asset switching (with related history reset on switch).
- Includes a minimal `risk_guard` that can cap hedge-plan size and price deviation, while exposing failure reason and stage flow in the dashboard / CSV / JSONL outputs.

### Repository Layout
- `main.py`: main runtime
- `variational/listener.py`: local receiver + monitor parsing
- `chrome_extension/`: CDP forwarder extension

### Setup
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows (PowerShell)
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

If you plan to use `live` mode, create `.env` with Lighter trading credentials:
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

Notes:
- `observe` does not require Lighter trading credentials.
- `dry-run` needs Lighter market data but does not send real orders.
- `paper` needs Lighter market data and auto-simulates entries/exits into `opportunities.jsonl`, but does not send real orders.
- `live` is the only mode that requires full Lighter trading credentials.

If you need to temporarily force Lighter's legacy application-level ping/pong behavior during the rollout window, you can also set:
```bash
LIGHTER_WS_SERVER_PINGS=true
```
When unset, the runtime uses the forward-compatible path: it keeps the socket alive with WebSocket protocol ping frames and still responds to legacy server `ping` messages.

### Load Chrome Extension
1. Open `chrome://extensions`
2. Enable `Developer mode` (top-right)
3. Click `Load unpacked` (top-left), then choose:
`var-rh-eth/chrome_extension`

### Run
```bash
python main.py
```

Default is observe mode:
```bash
python main.py --mode observe
```

Simulate hedge plans without sending real orders:
```bash
python main.py --mode dry-run
```

Auto-simulate opportunity entries/exits without sending real orders:
```bash
python main.py --mode paper
```

The first `paper` version uses the 5-minute cross-exchange spread median as the baseline. It simulates entry when the current spread deviates enough from that median, and simulates exit when the spread returns near the median or exceeds the maximum holding time. Results are appended to `log/opportunities.jsonl`.

Common paper parameters:
```bash
python main.py --mode paper --paper-notional-usd 30 --paper-entry-deviation-bps 3 --paper-exit-deviation-bps 0.5
```

Run dry-run with stricter risk-guard thresholds:
```bash
python main.py --mode dry-run --risk-guard-max-base-amount 200 --risk-guard-max-price-deviation-bps 150
```

Real Lighter hedge requires explicit second confirmation:
```bash
python main.py --mode live --confirm-live
```

After the Python script starts, open the Variational trading page,
open the Chrome extensions list, click `Variational CDP Forwarder`, then click `Start`.

Switch dashboard language to Chinese:
```bash
python main.py --lang zh
```

### Risk Guard Options
- `--risk-guard-max-base-amount`
  - Caps the maximum Lighter `base_amount` allowed when building a hedge plan
  - Default: `1000`
- `--risk-guard-max-price-deviation-bps`
  - Caps the maximum allowed deviation between hedge plan price and Variational fill price, in bps
  - Default: `500`

When a risk guard blocks a hedge plan:
- the affected order is highlighted in the dashboard
- `order_metrics.jsonl` records `failure_reason`, `processing_stage`, `stage_history`, and `stage_flow_text`
- `trade_records.csv` records `processing_stage`, `stage_history`, and `failure_reason`

### Output Logs
Default path: `./log`
- `runtime.log` (runtime log messages)
- `order_metrics.jsonl`
- `trade_records.csv` (current trade-record snapshot, overwritten on dashboard refresh with latest state)

Note: the terminal is reserved for the dashboard. Raw REST/WS payloads are not persisted; only runtime logs, order-metrics logs, and trade-record CSV snapshots are written.

In practice:
- `order_metrics.jsonl` is best for step-by-step event snapshots such as `variational_fill`, `lighter_dry_run_plan`, and `lighter_error`
- `trade_records.csv` is best for the latest aggregated state of each tracked order
- the dashboard shows current `mode`, `health`, `risk_guard` thresholds, and per-order `Stage Flow`
