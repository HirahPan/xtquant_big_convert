# 日频本地数据仓

`bigqmt-data` 将已缓存于大 QMT 的不复权日线导出到独立的 Parquet + DuckDB 数据仓；它不会调用账户、委托或下单接口，也不会默认触发历史数据下载。

## 安装

```powershell
python -m pip install -e "D:\QMT\local-data-lake"
```

运行时仍会只读引用 `D:\QMT\vendor\xtquant_big_convert-main` 的 QMT 兼容桥接层，但数据仓实现、测试、文档和依赖均在本目录，不修改上游 vendor 源码。

默认目录为 `D:\QMT-data`。该目录是运行数据，不能提交到 Git。

## 首次导出与日终增量

```powershell
# 使用 QMT 当前沪深 A 股、沪深 ETF 板块；只读取终端本地缓存。
# --start 只是最早允许日期；每只标的先查询上市日，再从 max(上市日, --start) 拉取。
bigqmt-data bootstrap --start 20150101 --end 20260905

# 每个交易日 20:00 后运行；日期应为最新完整交易日。
bigqmt-data sync --start 20260905 --end 20260905
```

如需纳入 QMT 当前板块列表外的退市标的，传入一行一个 `600000.SH` 格式代码的 `--symbols-file`。同步会按版本暂存数据，只有显式 `commit` 成功后才切换为当前版本；失败时旧版本保持可用。

## 进度日志与断点续传

首次导出先逐只读取上市日期，不会以 `19900101` 或 `count=0` 对全部标的发起宽泛请求。日线数据按 `--chunk-size`（默认 100 只）提交：一批完成后才写入检查点。因此终端重启、脚本中断或机器重启后，以相同参数再次执行原命令会自动从最后成功批次继续。

文件均位于独立数据目录，默认为：

- `D:\QMT-data\logs\daily_bootstrap.progress.jsonl`：首次导出的结构化进度日志；
- `D:\QMT-data\logs\daily_bootstrap.checkpoint.json`：首次导出的恢复检查点；
- `D:\QMT-data\logs\daily_sync.progress.jsonl` / `daily_sync.checkpoint.json`：日终增量对应文件。

成功完成后检查点会自动删除，日志保留。若有意改变日期范围或标的范围并放弃旧进度，传入 `--reset-checkpoint`；这只会删除该命令对应的检查点，不会删除已提交的 Parquet 数据。

## AKShare 交叉校验

```powershell
bigqmt-data validate-akshare --date 20260905
```

默认每天抽样 120 只股票和 30 只 ETF。QMT 是唯一主数据；AKShare 仅以不复权日线比较共同存在的交易日。价格超过 `0.001` 且相对偏差超过 `0.01%`，或成交量/成交额偏差超过 `1%` 时，该标的日期进入隔离区。AKShare 超时和限流仅记为 `unverified`，不会改写 QMT 数据。

初次建库可扩大样本并校验完整历史区间：

```powershell
bigqmt-data validate-akshare --start 20150101 --end 20260905 --stock-sample 500 --etf-sample 100
```

## 导出给本地回测器

```powershell
bigqmt-data export-csv --start 20250101 --end 20251231 --output D:\QMT-data\exports\bars.csv
```

CSV 与 `bigqmt_backtest.CsvBarFeed` 兼容。回测交易、涨跌停和资金计算应使用原始价格；研究指标可通过 `DataLake.get_bars(price_basis="front_ratio", asof=...)` 使用仅依赖截至该时点 `preClose` 的动态等比前复权价格。
