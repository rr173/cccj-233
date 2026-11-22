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
```

所有测试数据均在运行时生成于临时目录。
