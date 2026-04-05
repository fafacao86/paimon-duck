# Paimon-CPP Compaction StringView 优化计划

## 1. 目标

本方案聚焦 **mergetree compaction 内部链路**，利用 Arrow C++ 的
`StringView/BinaryView` 减少大字符串在 compaction 过程中的重复拷贝。

目标范围：

- Parquet compaction 读取阶段按需返回 `STRING_VIEW/BINARY_VIEW`
- `InternalRow` / `ColumnarUtils` 能透明访问 view 类型
- compaction 写出阶段使用 `StringViewBuilder/BinaryViewBuilder`
- 不修改 catalog / schema 持久化格式
- 不改变普通 scan / 非 compaction 写路径的默认行为

## 2. Arrow 版本前提

当前仓库锁定 Arrow `17.0.0`，不足以使用 Parquet 官方的
`ArrowReaderProperties::set_binary_type(...)` 能力。

本次实现将 Arrow 基线提升到 **21.0.0**，原因如下：

- 从 Arrow `21.0.0` 起，Parquet C++ 已提供 `set_binary_type`
- Parquet schema 映射已支持：
  - `STRING -> String / LargeString / StringView`
  - `BYTE_ARRAY -> Binary / LargeBinary / BinaryView`
- 这已经满足 compaction-only StringView 优化，不必将版本前提写成 `24+`

升级验收项：

- `third_party/versions.txt` 更新到 `21.0.0`
- `cmake_modules/arrow.diff` 需要重新验证哪些 patch 仍然必要
- 现有 parquet / orc / compaction 相关测试需要重新回归

## 3. 当前拷贝热点

长字符串在 compaction 中的热点路径大致如下：

1. Parquet 读出普通 `StringArray/BinaryArray`
2. `InternalRow::GetString()` / `GetBinary()` 可能 materialize 到 `Bytes`
3. `RowToArrowArrayConverter` 用 `BinaryBuilder` 再拷贝一遍字符串数据

现状里字符串比较已经较优：

- `FieldsComparator` 使用 `GetStringView()`
- merge/sort 阶段不是主要拷贝来源

真正值得优化的是：

- Parquet 读取结果的 Arrow 表示
- compaction 写出时 builder 的数据复制

## 4. 实现设计

### 4.1 Compaction-only Reader 开关

在 parquet reader 侧新增内部选项：

- `parquet.read.as-binary-view`

只在 compaction 创建 `ReadContext` 时打开该选项：

- `MergeTreeCompactRewriter::Create()` 会向读取 options 注入：
  - `parquet.read.enable-pre-buffer=false`
  - `parquet.read.as-binary-view=true`

`ParquetFileBatchReader::CreateArrowReaderProperties()` 中按该选项设置：

- `arrow_reader_props.set_binary_type(arrow::Type::BINARY_VIEW)`

这样可以让 compaction 读取阶段优先得到：

- Parquet string 列 -> `StringViewArray`
- Parquet binary 列 -> `BinaryViewArray`

兼容性说明：

- 如果未来 writer 开启 `ArrowWriterProperties::store_schema()`，reader 端的
  `set_binary_type` 可能会被存储的 Arrow schema 覆盖
- 当前 paimon-cpp 默认并未开启 `store_schema()`，因此该方案可行

### 4.2 Compaction 专用输出 Schema

不修改通用 `DataField` / table schema 的产出规则。

在 `MergeTreeCompactRewriter` 内部新增一份 **compaction-only write schema**：

- `STRING -> STRING_VIEW`
- `BINARY -> BINARY_VIEW`
- 递归处理 `LIST / MAP / STRUCT`
- `_SEQUENCE_NUMBER`、`_VALUE_KIND` 保持原类型

该 schema 仅用于：

- compaction 的 `KeyValueMetaProjectionConsumer`
- compaction 文件写出时的 Arrow writer schema

以下内容保持不变：

- table schema JSON
- DataField 元数据定义
- 对外暴露的普通读写 schema

### 4.3 中间访问层支持 View 类型

#### ColumnarUtils

`ColumnarUtils::GetView()` 扩展支持：

- `STRING_VIEW`
- `BINARY_VIEW`
- `DICTIONARY<..., STRING_VIEW>`
- `DICTIONARY<..., BINARY_VIEW>`

同时保留原有：

- `STRING / LARGE_STRING`
- `BINARY / LARGE_BINARY`

#### InternalRow

`InternalRow::CreateFieldGetter()` 扩展支持：

- `STRING_VIEW` 走字符串 getter 分支
- `BINARY_VIEW` 走二进制 getter 分支

这样原有 `use_view=true` 的调用方不需要区分底层到底是普通 string 还是 view。

#### FieldsComparator

`FieldsComparator` 补充支持：

- `STRING_VIEW`
- `BINARY_VIEW`

虽然当前比较阶段已经主要依赖 `GetStringView()`，但这一步可以避免未来
value schema 或 sequence 字段 schema 切到 view 类型后出现不支持类型的问题。

### 4.4 Compaction 写出改为 View Builder

`RowToArrowArrayConverter` 扩展支持：

- `Reserve()` 支持 `STRING_VIEW/BINARY_VIEW`
- `Accumulate()` 支持 `STRING_VIEW/BINARY_VIEW`
- `AppendField()` 支持：
  - `StringViewBuilder`
  - `BinaryViewBuilder`

行为约束：

- 普通 `STRING/BINARY` 保持现状，继续走 `StringBuilder/BinaryBuilder`
- 只有目标 schema 本身是 `STRING_VIEW/BINARY_VIEW` 时才走 view builder

因此：

- `KeyValueMetaProjectionConsumer`
- `KeyValueProjectionConsumer`

无需改调用方式，仍然只依赖 `AppendField(use_view=true, ...)`。

### 4.5 输出兼容性

compaction 写出的 Parquet 文件仍保持标准 Parquet 语义：

- string 列仍是 `BYTE_ARRAY + STRING logical type`
- binary 列仍是 `BYTE_ARRAY`

本次优化改变的是 **Arrow 内存表示**，不是磁盘格式。

因此兼容性策略为：

- 普通读路径默认仍读取为 `STRING/BINARY`
- 只有 compaction 内部 reader 显式开启 view 选项时才读取为 view
- 不默认开启 `store_schema()`

## 5. 代码落点

本次实现的主要改动点：

- `src/paimon/core/mergetree/compact/merge_tree_compact_rewriter.{h,cpp}`
  - 生成 compaction-only write schema
  - 向 compaction 读上下文注入 `parquet.read.as-binary-view=true`
- `src/paimon/format/parquet/parquet_file_batch_reader.cpp`
  - 在 reader properties 中按选项打开 `BINARY_VIEW`
- `src/paimon/format/parquet/parquet_format_defs.h`
  - 新增内部选项常量
- `src/paimon/common/data/columnar/columnar_utils.h`
  - 支持 `STRING_VIEW/BINARY_VIEW` 与字典 view
- `src/paimon/common/data/internal_row.cpp`
  - 支持 `STRING_VIEW/BINARY_VIEW`
- `src/paimon/core/utils/fields_comparator.cpp`
  - 支持 view 类型比较
- `src/paimon/core/io/row_to_arrow_array_converter.h`
  - 支持 `StringViewBuilder/BinaryViewBuilder`
- `third_party/versions.txt`
  - Arrow 升级到 `21.0.0`

## 6. 测试计划

### 单元测试

- `ColumnarUtils`：
  - 普通 `STRING/BINARY`
  - `STRING_VIEW/BINARY_VIEW`
  - `DICTIONARY<STRING_VIEW>`
- `KeyValueMetaProjectionConsumer`：
  - 目标 schema 为 `STRING_VIEW/BINARY_VIEW`
  - 输出 batch 的字段类型正确
  - 输出数据内容正确
- `ParquetFileBatchReader`：
  - 打开 `parquet.read.as-binary-view=true` 后
  - 读取 string/binary 列得到 `STRING_VIEW/BINARY_VIEW`

### 集成回归

- 现有 compaction 测试继续通过
- 现有 parquet reader/writer 测试继续通过
- data evolution / projection 路径不发生行为回归

## 7. 已知边界

- 本次没有把 StringView 扩散到普通 scan 路径
- 本次没有修改 catalog/schema 存储格式
- 本次没有默认开启 `ArrowWriterProperties::store_schema()`
- Arrow 升级后的 `arrow.diff` 是否还能直接复用，需要在依赖构建阶段继续验证
