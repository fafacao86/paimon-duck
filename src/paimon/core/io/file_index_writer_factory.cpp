/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/io/file_index_writer_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/status.h"

namespace paimon {

namespace {

Status AddIndexWritersForColumns(FileIndexFormat::Writer* writer,
                                 const std::shared_ptr<arrow::Schema>& write_schema,
                                 const std::vector<std::string>& columns,
                                 const std::string& index_type,
                                 const std::shared_ptr<MemoryPool>& pool) {
    for (const auto& col_name : columns) {
        auto field = write_schema->GetFieldByName(col_name);
        if (!field) {
            continue;  // Skip columns not in write_schema
        }

        auto col_schema = arrow::schema({field});
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*col_schema, &c_schema));
        ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });

        PAIMON_ASSIGN_OR_RAISE(auto indexer, FileIndexerFactory::Get(index_type, {}));
        if (!indexer) {
            continue;  // Index type not registered (e.g. paimon_file_index not linked), skip
        }
        PAIMON_ASSIGN_OR_RAISE(auto sub_writer, indexer->CreateWriter(&c_schema, pool));
        PAIMON_RETURN_NOT_OK(writer->AddIndexWriter(col_name, index_type, std::move(sub_writer),
                                                    arrow::struct_({field})));
    }
    return Status::OK();
}

}  // namespace

Result<std::unique_ptr<FileIndexFormat::Writer>> FileIndexWriterFactory::Create(
    const std::shared_ptr<arrow::Schema>& write_schema, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& pool) {
    const auto& bitmap_cols = options.GetFileIndexBitmapColumns();
    const auto& bsi_cols = options.GetFileIndexBsiColumns();
    const auto& bloom_cols = options.GetFileIndexBloomFilterColumns();
    if (bitmap_cols.empty() && bsi_cols.empty() && bloom_cols.empty()) {
        return std::unique_ptr<FileIndexFormat::Writer>();
    }

    auto writer = FileIndexFormat::CreateWriter();

    PAIMON_RETURN_NOT_OK(
        AddIndexWritersForColumns(writer.get(), write_schema, bitmap_cols, "bitmap", pool));
    PAIMON_RETURN_NOT_OK(
        AddIndexWritersForColumns(writer.get(), write_schema, bsi_cols, "bsi", pool));
    PAIMON_RETURN_NOT_OK(
        AddIndexWritersForColumns(writer.get(), write_schema, bloom_cols, "bloom-filter", pool));

    return writer;
}

}  // namespace paimon
