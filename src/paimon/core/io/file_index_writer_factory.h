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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/core/core_options.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;

/// Factory for creating FileIndexFormat::Writer from table options.
/// Registers index writers for each column in bitmap, bsi, and bloom-filter columns.
class FileIndexWriterFactory {
 public:
    FileIndexWriterFactory() = delete;

    /// Creates a FileIndexFormat::Writer configured from CoreOptions.
    /// Supports bitmap, bsi, and bloom-filter index types. If no index columns are
    /// configured, returns nullptr.
    ///
    /// @param write_schema Schema of the data being written (must contain the indexed columns).
    /// @param options CoreOptions with file-index.bitmap.columns, file-index.bsi.columns,
    ///                file-index.bloom-filter.columns.
    /// @param pool Memory pool for allocations.
    /// @return A configured writer, or an empty writer if no index columns are configured.
    static Result<std::unique_ptr<FileIndexFormat::Writer>> Create(
        const std::shared_ptr<arrow::Schema>& write_schema, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
