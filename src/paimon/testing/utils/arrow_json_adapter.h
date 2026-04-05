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

#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/type.h>
#include <arrow/util/config.h>

// Arrow version detection
#if ARROW_VERSION_MAJOR >= 24
// Arrow 24+: Use arrow::ipc::internal::json::ArrayFromJSON
#include <arrow/ipc/json_simple.h>

#elif ARROW_VERSION_MAJOR >= 17
// Arrow 17-23: Use arrow::ArrayFromJSON from testing/gtest_util.h
#include <arrow/testing/gtest_util.h>

#else
#error "Unsupported Arrow version"
#endif

namespace paimon {
namespace testing {

#if ARROW_VERSION_MAJOR >= 24
// Arrow 24+
inline std::shared_ptr<arrow::Array> ArrayFromJSON(
    const std::shared_ptr<arrow::DataType>& type,
    std::string_view json) {
    return arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
}

inline arrow::Status ChunkedArrayFromJSON(
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<std::string>& json,
    std::shared_ptr<arrow::ChunkedArray>* out) {
    return arrow::ipc::internal::json::ChunkedArrayFromJSON(type, json, out);
}

#else
// Arrow 17-23
inline std::shared_ptr<arrow::Array> ArrayFromJSON(
    const std::shared_ptr<arrow::DataType>& type,
    std::string_view json) {
    return arrow::ArrayFromJSON(type, json);
}

inline arrow::Status ChunkedArrayFromJSON(
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<std::string>& json,
    std::shared_ptr<arrow::ChunkedArray>* out) {
    *out = arrow::ChunkedArrayFromJSON(type, json);
    return arrow::Status::OK();
}

#endif

}  // namespace testing
}  // namespace paimon
