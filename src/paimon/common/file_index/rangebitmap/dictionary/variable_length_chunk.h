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

#include <functional>
#include <memory>
#include <vector>

#include "paimon/common/file_index/rangebitmap/dictionary/chunk.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class VariableLengthChunk : public Chunk {
 public:
    static constexpr int8_t CURRENT_VERSION = 1;

    /// Constructor for creating a new chunk
    VariableLengthChunk(const Literal& key, int32_t code, int32_t offset,
                        int32_t limited_serialized_size_in_bytes,
                        std::shared_ptr<KeyFactory::KeySerializer> serializer,
                        std::function<int(const Literal&, const Literal&)> comparator);

    /// Constructor for reading from serialized data
    VariableLengthChunk(const std::shared_ptr<InputStream>& input_stream, int32_t keys_base_offset,
                        std::shared_ptr<KeyFactory::KeyDeserializer> deserializer,
                        std::function<int(const Literal&, const Literal&)> comparator);

    Result<bool> TryAdd(const Literal& key) override;
    Result<Literal> GetKey(int32_t index) const override;
    const Literal& Key() const override {
        return key_;
    }
    int32_t Code() const override {
        return code_;
    }
    Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(MemoryPool* pool) const override;
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeKeys(MemoryPool* pool) const override;

    static Result<std::unique_ptr<VariableLengthChunk>> Deserialize(
        const std::shared_ptr<InputStream>& input_stream, int32_t keys_base_offset,
        std::shared_ptr<KeyFactory::KeyDeserializer> deserializer,
        std::function<int(const Literal&, const Literal&)> comparator);

 private:
    int8_t version_;
    Literal key_;
    int32_t code_;
    int32_t offset_;
    int32_t size_;
    int32_t current_offset_;
    std::vector<Literal> keys_;
    std::vector<int32_t> offsets_;
    std::shared_ptr<KeyFactory::KeySerializer> serializer_;
    std::shared_ptr<KeyFactory::KeyDeserializer> deserializer_;
    std::function<int(const Literal&, const Literal&)> comparator_;
    std::shared_ptr<InputStream> input_stream_;
    int32_t keys_base_offset_;
};

}  // namespace paimon