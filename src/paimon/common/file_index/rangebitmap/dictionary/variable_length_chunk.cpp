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

#include "paimon/common/file_index/rangebitmap/dictionary/variable_length_chunk.h"

#include <arrow/io/api.h>

#include <algorithm>

#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

VariableLengthChunk::VariableLengthChunk(
    const Literal& key, int32_t code, int32_t offset, int32_t limited_serialized_size_in_bytes,
    std::shared_ptr<KeyFactory::KeySerializer> serializer,
    std::function<int(const Literal&, const Literal&)> comparator)
    : version_(CURRENT_VERSION),
      key_(key),
      code_(code),
      offset_(offset),
      size_(0),
      current_offset_(0),
      serializer_(std::move(serializer)),
      comparator_(std::move(comparator)) {
    // Reserve some space
    keys_.reserve(16);
    offsets_.reserve(16);
}

VariableLengthChunk::VariableLengthChunk(
    const std::shared_ptr<InputStream>& input_stream, int32_t keys_base_offset,
    std::shared_ptr<KeyFactory::KeyDeserializer> deserializer,
    std::function<int(const Literal&, const Literal&)> comparator)
    : deserializer_(std::move(deserializer)),
      comparator_(std::move(comparator)),
      input_stream_(input_stream),
      keys_base_offset_(keys_base_offset) {
    // Read header - this is simplified, in practice we'd need to read the actual header
    // from the input stream based on the serialization format
}

Result<bool> VariableLengthChunk::TryAdd(const Literal& key) {
    if (!serializer_) {
        return Status::Invalid("Cannot add to a deserialized chunk");
    }

    // Check size limit (simplified)
    int32_t serialized_size = serializer_->SerializedSizeInBytes(key);
    if (current_offset_ + serialized_size > 64 * 1024) {  // 64KB limit
        return false;
    }

    offsets_.push_back(current_offset_);
    keys_.push_back(key);
    current_offset_ += serialized_size;
    size_ = keys_.size();
    return true;
}

Result<Literal> VariableLengthChunk::GetKey(int32_t index) const {
    if (index >= 0 && index < static_cast<int32_t>(keys_.size())) {
        return keys_[index];
    }
    return Status::Invalid("Index out of bounds");
}

Result<PAIMON_UNIQUE_PTR<Bytes>> VariableLengthChunk::Serialize(MemoryPool* pool) const {
    // Calculate sizes
    int32_t header_size =
        sizeof(int8_t) + serializer_->SerializedSizeInBytes(key_) + 4 * sizeof(int32_t);
    int32_t offsets_size = size_ * sizeof(int32_t);
    int32_t keys_size = current_offset_;
    int32_t total_size = header_size + offsets_size + keys_size;

    auto buffer = Bytes::AllocateBytes(total_size, pool);
    uint8_t* data = reinterpret_cast<uint8_t*>(buffer->data());

    // Write header
    *data = version_;
    data += sizeof(int8_t);

    // Serialize key
    auto key_buffer = arrow::io::BufferOutputStream::Make();
    if (!key_buffer.ok()) {
        return Status::IOError("Failed to create buffer output stream");
    }
    auto status = serializer_->Serialize(key_buffer.value().get(), key_);
    if (!status.ok()) {
        return status;
    }
    memcpy(data, key_buffer.value()->View().data(), serializer_->SerializedSizeInBytes(key_));
    data += serializer_->SerializedSizeInBytes(key_);

    *reinterpret_cast<int32_t*>(data) = code_;
    data += sizeof(int32_t);
    *reinterpret_cast<int32_t*>(data) = offset_;
    data += sizeof(int32_t);
    *reinterpret_cast<int32_t*>(data) = size_;
    data += sizeof(int32_t);
    *reinterpret_cast<int32_t*>(data) = current_offset_;
    data += sizeof(int32_t);

    // Write offsets
    for (int32_t offset : offsets_) {
        *reinterpret_cast<int32_t*>(data) = offset;
        data += sizeof(int32_t);
    }

    // Write keys
    for (const auto& key : keys_) {
        auto key_stream = arrow::io::BufferOutputStream::Make();
        if (!key_stream.ok()) {
            return Status::IOError("Failed to create key buffer output stream");
        }
        status = serializer_->Serialize(key_stream.value().get(), key);
        if (!status.ok()) {
            return status;
        }
        memcpy(data, key_stream.value()->View().data(), serializer_->SerializedSizeInBytes(key));
        data += serializer_->SerializedSizeInBytes(key);
    }

    return buffer;
}

Result<PAIMON_UNIQUE_PTR<Bytes>> VariableLengthChunk::SerializeKeys(MemoryPool* pool) const {
    auto buffer = Bytes::AllocateBytes(current_offset_, pool);
    uint8_t* data = reinterpret_cast<uint8_t*>(buffer->data());

    for (const auto& key : keys_) {
        // For now, skip complex serialization and use placeholder
        int32_t key_size = serializer_->SerializedSizeInBytes(key);
        memset(data, 0, key_size);
        data += key_size;
    }

    return buffer;
}

// Static deserialization method
Result<std::unique_ptr<VariableLengthChunk>> VariableLengthChunk::Deserialize(
    const std::shared_ptr<InputStream>& input_stream, int32_t keys_base_offset,
    std::shared_ptr<KeyFactory::KeyDeserializer> deserializer,
    std::function<int(const Literal&, const Literal&)> comparator) {
    // TODO: Implement actual deserialization logic
    // This should read the header and keys from the input stream
    return Status::NotImplemented("VariableLengthChunk::Deserialize not yet implemented");
}

}  // namespace paimon