# task26100105 — 离线工单归档库

Python + SQLite 实现的离线工单归档库：把符合条件的工单从在线库迁移到本地
JSON 文件，进程被强制终止（kill -9）后可通过 `Recover()` 自动续跑。

## 文件

- `ticket_archive.py` — 库本体（`TicketArchive`：`import_tickets` / `archive` / `recover` / `get_ticket`）。
- `runner.py` — 故障注入测试用的子进程入口（通过 `TICKET_ARCHIVE_FAILPOINT` 环境变量在指定故障点 `os._exit`）。
- `tests/test_ticket_archive.py` — 自动化测试。

## 持久化边界

- **在线表 `tickets`**：保存尚未归档的完整工单（id、status、closed_at、JSON 正文）。
- **批次元数据 `batches` / `batch_selection`**：`batch_id` 首次登记时冻结选集
  （在线记录数严格大于 100000 时，选取 `status=closed` 且 `closed_at` 严格早于
  `now - 90*24h` 的工单；否则为空选集），只持久化工单 id 与批次参数，不保存正文。
- **定位索引 `archive_index`**：已归档工单 id → 正式文件名，仅供 `GetTicket` 定位，不含正文。
- **归档目录**：只有以 `.json` 结尾的文件是正式归档，每个文件是可独立解析的
  JSON 数组，最多 1000 条。写入时先写 `.tmp` 临时文件，`fsync` 后 `os.replace`
  原子改名；正式文件一旦可见就不再改写。

## 恢复语义

- 每个分块的提交顺序：发布正式文件 → （故障点）→ 同事务写入定位索引并删除在线记录。
  因此任意时刻崩溃，工单要么仍在线、要么已在完整可读的正式文件中，允许短暂重叠，
  绝不丢失或仅存于日志。
- 重新打开存储后，若存在未完成的批次，除 `recover()` 外的操作一律拒绝，
  直到恢复成功。
- `recover()` 只完成已冻结的选集：正式文件已存在则跳过重写（绝不改写可见文件），
  仅补齐索引与删除；文件缺失则从在线记录重新生成。即使部分删除已使在线记录数
  降到阈值以下，剩余冻结选集仍会全部完成。恢复可反复被终止、反复重入。
- 同一 `batch_id` 完成后再次调用 `archive` 直接返回持久化的结果
  （正式文件路径列表与工单总数），不会吸收后来导入的工单；空选集批次同样如此。

## 运行测试

```sh
python3 -m pytest tests/ -v
```
