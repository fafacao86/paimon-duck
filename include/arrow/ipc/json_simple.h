#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/json/from_string.h>

namespace arrow::ipc::internal::json {

class ArrayFromJSONCompat {
public:
    explicit ArrayFromJSONCompat(arrow::Result<std::shared_ptr<arrow::Array>> result)
        : result_(std::move(result)) {}

    bool ok() const {
        return result_.ok();
    }

    arrow::Status status() const {
        return result_.status();
    }

    std::shared_ptr<arrow::Array> ValueUnsafe() && {
        return std::move(result_).ValueUnsafe();
    }

    std::shared_ptr<arrow::Array> ValueOrDie() const {
        return result_.ValueOrDie();
    }

    std::shared_ptr<arrow::Array> ValueOr(std::shared_ptr<arrow::Array> default_value) const {
        if (result_.ok()) {
            return result_.ValueOrDie();
        }
        return default_value;
    }

    operator std::shared_ptr<arrow::Array>() const {
        return result_.ValueOrDie();
    }

    arrow::Array* operator->() const {
        return result_.ValueOrDie().get();
    }

private:
    arrow::Result<std::shared_ptr<arrow::Array>> result_;
};

inline ArrayFromJSONCompat ArrayFromJSON(const std::shared_ptr<arrow::DataType>& type,
                                         std::string_view json) {
    return ArrayFromJSONCompat(arrow::json::ArrayFromJSONString(type, json));
}

inline ArrayFromJSONCompat ArrayFromJSON(const std::shared_ptr<arrow::DataType>& type,
                                         const char* json) {
    return ArrayFromJSONCompat(arrow::json::ArrayFromJSONString(type, json));
}

inline arrow::Status ChunkedArrayFromJSON(const std::shared_ptr<arrow::DataType>& type,
                                          const std::vector<std::string>& json,
                                          std::shared_ptr<arrow::ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(*out, arrow::json::ChunkedArrayFromJSONString(type, json));
    return arrow::Status::OK();
}

}  // namespace arrow::ipc::internal::json
