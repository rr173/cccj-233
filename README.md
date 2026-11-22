# handover — 扫描台绑定表离线交接工具

把扫描台绑定表从旧版逐台交接给只接受逐台提交的旧版接收器。每个扫描台
同时占用一个**工位号**和一个**通道号**，两列各自唯一；接收器不支持整表
替换、推迟唯一性检查或临时造号。工具分两部分：

1. **规划器**：为指定的旧表和目标表编排一份逐台变更工单（或证明无解并
   给出闭合等待环）；
2. **接收库**：把工单逐条落到 SQLite，中途停止后按工单身份和已提交步号
   继续。

纯 Python 3 标准库，无第三方依赖。

## 输入格式

绑定表为 JSON Lines，每行恰好三个键，值均为非空字符串（区分大小写）：

```json
{"id": "A", "ws": "WS-1", "ch": "CH-1"}
```

两份表各自要求 id / ws / ch 不重复。临时离线清单是一个 JSON 数组，只
允许两版均存在且绑定有变的 id：

```json
["A", "B"]
```

## 用法

```bash
# 1. 编排工单（失败时不产出工单文件）
python3 -m handover plan --old old.jsonl --target new.jsonl \
    --offline offline.json --out order.json
#    退出码 0=成功；3=源资料不合法；4=合法但无法迁移（打印等待环报告）

# 2. 初始化接收库（装入旧表；仅空库可执行）
python3 -m handover init --db recv.db --table old.jsonl

# 3. 逐条提交 / 续接到完成
python3 -m handover submit --db recv.db --order order.json --seq 0
python3 -m handover run    --db recv.db --order order.json

# 4. 查看当前绑定与已提交进度
python3 -m handover status --db recv.db --dump
```

`run` 的两个测试钩子用于模拟崩溃：`--crash-before-seq K`（提交第 K 步
前停止）和 `--crash-after-commit-seq K`（第 K 步提交成功但应答丢失）。

## 允许的逐台操作

| op       | 变更前            | 变更后            | 适用对象             |
|----------|-------------------|-------------------|----------------------|
| insert   | 无行              | online(ws,ch)     | 仅新版存在的设备     |
| delete   | online(ws,ch)     | 无行              | 仅旧版存在的设备     |
| update   | online(ws,ch)     | online(ws',ch')   | 两版均在且绑定有变   |
| offline  | online(ws,ch)     | offline（行保留） | 获准临时离线的设备   |
| online   | offline           | online(ws',ch')   | 获准临时离线的设备   |

不写半套绑定；不用删除重插冒充原地迁移；不借用任何不属于该设备目标的
号码；绑定不变的设备完全不动。

## 规划器

把每台待迁移设备看作等待者：它的目标工位号/通道号当前被谁占用，就等
谁。用就绪堆 + 每个号码的等待者索引做增量调度，每落一步 O(log n)，
**不会每步重扫全部设备**；只有在无人能动时才做一次环检测。

- 无环：总有设备两列目标都空闲，直接写定目标绑定。
- 有环：环上若有获准离线的设备，将其一次性清空两个号码（行和 ID 保
  留），环即解除；它稍后在目标号码空闲时一次性写定目标绑定。
- 环上无任何获准离线的设备：判定**合法但无法迁移**，输出闭合等待环报
  告。由于等待边只会随迁移消失、不会新增，此判定是完备的——只要存在
  合法顺序，规划器必然给出工单。

等待环报告的每条边指出某台设备需要的哪一列号码被哪台设备占用，并内嵌
编排时刻的绑定快照；`handover.verify_cycle_report` 可据此复核为什么
这些设备都无法先动。工单操作顺序是稳定的：同一输入内容重排行序，产出
的工单逐字节相同。

## 工单格式（handover-order/1）

```json
{
  "format": "handover-order/1",
  "initial_hash": "…", "final_hash": "…", "order_hash": "…",
  "steps": [
    {"seq": 0, "id": "B", "op": "offline",
     "before": {"state": "online", "ws": "WS-2", "ch": "CH-2"},
     "after":  {"state": "offline"}}
  ]
}
```

每步带扫描台 ID 和准确的变更前后状态，`absent`（无行）与 `offline`
（行在、号码已清空）明确区分。`order_hash` 覆盖全部内容，是工单身份。

## 接收库语义

- `bindings(id PK, ws UNIQUE, ch UNIQUE)`：真实唯一键始终开启；离线行
  的两列为 NULL（SQLite 唯一索引视 NULL 互不相同）。
- 初次接受工单前核对整张初始绑定表（`initial_hash`）；之后按
  `order_hash` + 已提交步号继续。
- 每次只提交下一步；业务变更与进度（`meta` + `steps` 表）在同一事务
  落地，要么都落要么都不落。
- 已提交步骤重送：按步号+步骤哈希核验后为幂等空操作；跳步、别份工单
  的同号步、与已提交内容不符的重送一律拒绝且不改库。
- 最后一步落地时整库核对 `final_hash`；`status` 可读出当前绑定与进度。

## 扫描归属台账（handover-scan-ledger/1）

接收器能回答一单交接结束后工位归谁，却回答不了交接当天某次扫描该计
入哪个通道：扫描日志只有发生时刻和工位号，而工位可能在交接中空出、
稍后又被另一台扫描台接手。台账（`handover/ledger.py`）把一单**已完
工**交接的逐步生效时刻和当天扫描合并成一份可就地更正的 SQLite 账
本，建一次底账后可以处理多次时间更正，不重导整天扫描，也不改动旧
接收器与工单格式。

### 建账（一次）

```bash
python3 -m handover ledger-build --db ledger.db --snapshot old.jsonl \
    --order order.json --receiver recv.db --times times.json \
    --scans scans.jsonl
```

- 接收库以**只读**方式打开，仅作证据：工单身份（`order_hash`）、初
  始快照摘要（`initial_hash`）与接收库保存的完整步号及步骤摘要必须
  全部对得上；未提交完成的工单不是历史，建账拒绝（
  `receiver_incomplete`）。目标路径已存在即拒绝（`ledger_exists`）：
  底账只建一次；建账先写临时文件再就位，失败不留半成品。
- 时刻表为 `handover-step-times/1`：`{"format":…,"order_hash":…,
  "times":[{"seq":0,"ts":…},…]}`。每个工单步号恰好一个实际生效时
  刻，按原步号非递减。
- 扫描为 JSON Lines，每行恰好 `{"id":…,"ws":…,"ts":…}`：扫描记录
  ID 唯一、工位号非空、时刻为非负整数 Unix 秒（UTC 日期不超过
  9999-12-31）。

归属规则：第一步之前用初始绑定；同一秒有多步时，该秒的扫描看到所有
步骤按原序生效后的状态，而非某个中间状态；已离线或无人占用的工位归
为**未归属**，不沿用上一次占用者。

### 更正（多次，就地）

```bash
python3 -m handover ledger-correct --db ledger.db --correction corr.json
#    测试钩子 --crash-before-commit：写入后、COMMIT 前杀死进程（退出码 12）
python3 -m handover ledger-status --db ledger.db [--scans]
```

更正文件为 `handover-time-correction/1`：`{"format":…,"order_hash":…,
"changes":[{"seq":1,"ts":…},…]}`。整组校验：不存在的或重复的步号、
非法时刻、更正后倒序，都拒绝且不留任何变化；更正只能引用既有步号，
不能增删步骤或改步骤内容。落地时只检查被改步骤涉及的工位、以及各步
新旧时刻之间 `[较早, 较晚)` 窗口内的扫描（同一工位相交窗口合并；原
先未归属的扫描同样在候选范围），不触碰别的工位或窗口外的历史，更不
替换整份台账。归属未变的明细不动；归属有变的，移走原计数、记入新计
数。更正后的时刻、被改明细与净计数随结果一起返回，并存入
`corrections` 表。整个更正是一个事务：出错或提交前中断，重开仍是旧
结果；重送已生效的相同更正按内容哈希判重，是幂等空操作。

### 读取（无需再扫扫描明细）

```sql
-- 每条扫描的归属（未归属的 device/ch 为 NULL）
SELECT scan_id, ws, ts, device, ch FROM scans ORDER BY ts, scan_id;
SELECT scan_id, ws, ts, device, ch FROM scans WHERE scan_id = 'S06';

-- 按扫描发生的 UTC 日期和通道汇总的计数：未归属单列一组，零计数组不出现
SELECT day, COALESCE(ch, '(unattributed)') AS channel, n
FROM counts ORDER BY day, channel;

-- 当前逐步生效时刻
SELECT seq, id, op, ts FROM steps ORDER BY seq;

-- 已应用的更正：新时刻、窗口、被改明细、净计数
SELECT hash, summary FROM corrections ORDER BY rowid;
```

更正的候选查询固定走 `scans(ws, ts)` 索引做范围查找（`EXPLAIN QUERY
PLAN` 显示 `SEARCH scans USING INDEX scans_ws_ts`，而非全表扫描），
因此更正访问量随窗口内记录数增长，不随底账总量增长——测试用 SQLite
虚拟机指令计数（而非耗时）证明了这一点。

## 演练与测试

```bash
python3 run_demo.py        # A/B/C 等待环演练：只许 A 离线时识别 B<->C 死锁；
                           # 再许 B 离线后规划、崩溃、续接并跑到目标表，
                           # 并证明每个已提交前缀满足两个唯一约束、共有行
                           # 未被删后重插（rowid 不变）
python3 test_handover.py   # 测试套件：输入校验、无解/不合法区分、等待环复核、
                           # 定向用例（新增/退出/双资源冲突/换序/大小写）、
                           # 独立 BFS 枚举对拍（不调用规划器）、千级长链+
                           # 大量互换组的工作量、跨进程崩溃/重送/续接、
                           # 接收库拒绝非法写入
python3 run_ledger_demo.py # 台账演练：工位先空出后换主的一单交接建账，
                           # 边界/同秒/未归属扫描逐条核对，两次时间更正
                           # （含一次写入后、提交前崩溃重开），重送判重
python3 test_ledger.py     # 台账测试：建账证据链与输入校验、与独立重放
                           # 对拍的逐条归属与分组计数、窗口化更正与窗口合
                           # 并、整组拒绝不留痕、多次更正与幂等重送、崩溃
                           # 回滚、以及用 VM 指令计数证明更正访问只随窗口
                           # 内记录增长（20 倍底账无关扫描下访问数不变）
```

所有测试数据均在运行时生成于临时目录。
