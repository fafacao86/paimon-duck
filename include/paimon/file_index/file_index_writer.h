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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowArray;

namespace paimon {
/// Interface for writing file-level index data from Arrow batches.
class PAIMON_EXPORT FileIndexWriter {
 public:
    virtual ~FileIndexWriter() = default;

    /// Adds a batch of data to the index writer.
    ///
    /// @param batch Pointer to a C ArrowArray derived from an Arrow struct array containing
    ///              the indexed field.
    ///
    /// @note Ownership of `batch` is transferred to the callee.
    ///       Implementations must either consume the array (for example by importing or moving it)
    ///       or release it on failure. Callers must not access or release `batch` after this call.
    ///
    /// @return `Status::OK()` on success; otherwise, an error indicating failure
    ///         (e.g. schema mismatch or invalid input batch).
    virtual Status AddBatch(::ArrowArray* batch) = 0;

    /// Serializes the built index into a byte buffer.
    ///
    /// @note This method returns the complete serialized form of the index after all batches
    ///       have been added. It should be called only after all `AddBatch()` calls complete.
    ///
    /// @return A pool-managed unique pointer to the serialized index bytes,
    ///         or an error if serialization fails.
    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const = 0;
};
}  // namespace paimon
