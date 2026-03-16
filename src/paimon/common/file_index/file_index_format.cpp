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

 #include "paimon/file_index/file_index_format.h"

 #include <cassert>
 #include <map>
 #include <unordered_map>
 #include <utility>
 #include <vector>
 
 #include "arrow/c/bridge.h"
 #include "arrow/c/helpers.h"
 #include "arrow/type.h"
 #include "fmt/format.h"
 #include "paimon/common/file_index/empty/empty_file_index_reader.h"
 #include "paimon/common/io/memory_segment_output_stream.h"
 #include "paimon/common/memory/memory_segment_utils.h"
 #include "paimon/common/utils/arrow/status_utils.h"
 #include "paimon/file_index/file_indexer.h"
 #include "paimon/file_index/file_indexer_factory.h"
 #include "paimon/io/byte_array_input_stream.h"
 #include "paimon/io/data_input_stream.h"
 #include "paimon/memory/bytes.h"
 #include "paimon/memory/memory_pool.h"
 #include "paimon/status.h"
 
 namespace paimon {
 class InputStream;
 class MemoryPool;
 
 class FileIndexFormatReaderImpl : public FileIndexFormat::Reader {
  public:
     using HeaderType =
         std::unordered_map<std::string,
                            std::unordered_map<std::string, std::pair<int32_t, int32_t>>>;
 
     static Result<std::unique_ptr<FileIndexFormatReaderImpl>> Create(
         const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
         DataInputStream data_input_stream(input_stream);
         PAIMON_ASSIGN_OR_RAISE(int64_t magic, data_input_stream.ReadValue<int64_t>());
         if (magic != FileIndexFormat::MAGIC) {
             return Status::Invalid("This file is not file index file.");
         }
         PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input_stream.ReadValue<int32_t>());
         if (version != FileIndexFormat::V_1) {
             return Status::Invalid(
                 fmt::format("This index file is version of {}, not in supported version list [{}]",
                             version, FileIndexFormat::V_1));
         }
         PAIMON_ASSIGN_OR_RAISE(int32_t head_length, data_input_stream.ReadValue<int32_t>());
         auto head_bytes = std::make_unique<Bytes>(head_length - 8 - 4 - 4, pool.get());
         PAIMON_RETURN_NOT_OK(data_input_stream.ReadBytes(head_bytes.get()));
 
         auto byte_array_input_stream =
             std::make_shared<ByteArrayInputStream>(head_bytes->data(), head_bytes->size());
         DataInputStream inner_data_input_stream(byte_array_input_stream);
         PAIMON_ASSIGN_OR_RAISE(int32_t column_size, inner_data_input_stream.ReadValue<int32_t>());
 
         HeaderType header;
         for (int32_t i = 0; i < column_size; i++) {
             PAIMON_ASSIGN_OR_RAISE(std::string column_name, inner_data_input_stream.ReadString());
             PAIMON_ASSIGN_OR_RAISE(int32_t index_size,
                                    inner_data_input_stream.ReadValue<int32_t>());
             auto& index_map = header[column_name];
             for (int32_t j = 0; j < index_size; j++) {
                 PAIMON_ASSIGN_OR_RAISE(std::string index_type,
                                        inner_data_input_stream.ReadString());
                 PAIMON_ASSIGN_OR_RAISE(int32_t offset,
                                        inner_data_input_stream.ReadValue<int32_t>());
                 PAIMON_ASSIGN_OR_RAISE(int32_t length,
                                        inner_data_input_stream.ReadValue<int32_t>());
                 index_map[index_type] = std::make_pair(offset, length);
             }
         }
         return std::unique_ptr<FileIndexFormatReaderImpl>(
             new FileIndexFormatReaderImpl(input_stream, std::move(header), pool));
     }
 
     Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadColumnIndex(
         const std::string& column_name, ::ArrowSchema* c_arrow_schema) const override {
         PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                           arrow::ImportSchema(c_arrow_schema));
         auto column_field = arrow_schema->GetFieldByName(column_name);
         if (!column_field) {
             return Status::Invalid(fmt::format("cannot find column {} in schema", column_name));
         }
         std::vector<std::shared_ptr<FileIndexReader>> res;
         auto index_iter = header_.find(column_name);
         if (index_iter != header_.end()) {
             const auto& index_map = index_iter->second;
             for (const auto& [index_type, offset_and_length] : index_map) {
                 PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexReader> file_index_reader,
                                        GetFileIndexReader(arrow::schema({column_field}), index_type,
                                                           offset_and_length));
                 if (file_index_reader) {
                     // skip the index not registered
                     res.push_back(std::move(file_index_reader));
                 }
             }
         }
         return res;
     }
 
  private:
     FileIndexFormatReaderImpl(const std::shared_ptr<InputStream>& input_stream, HeaderType&& header,
                               const std::shared_ptr<MemoryPool>& pool)
         : input_stream_(input_stream), pool_(pool), header_(std::move(header)) {
         assert(input_stream_);
     }
 
     Result<std::shared_ptr<FileIndexReader>> GetFileIndexReader(
         const std::shared_ptr<arrow::Schema>& arrow_schema, const std::string& index_type,
         const std::pair<int32_t, int32_t>& offset_and_length) const {
         if (offset_and_length.first == FileIndexFormat::EMPTY_INDEX_FLAG) {
             return std::make_shared<EmptyFileIndexReader>();
         }
         PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexer> file_indexer,
                                FileIndexerFactory::Get(index_type, /*options=*/{}));
         // assert(file_indexer);
         if (!file_indexer) {
             return std::shared_ptr<FileIndexReader>();
         }
         ArrowSchema c_arrow_schema;
         PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
         return file_indexer->CreateReader(&c_arrow_schema, offset_and_length.first,
                                           offset_and_length.second, input_stream_, pool_);
     }
 
  private:
     std::shared_ptr<InputStream> input_stream_;
     std::shared_ptr<MemoryPool> pool_;
     // get header and cache it.
     // [column_name : [index_type : {offset, length}]]
     HeaderType header_;
 };
 
 class FileIndexFormatWriterImpl : public FileIndexFormat::Writer {
  public:
     struct IndexEntry {
         std::string index_type;
         std::shared_ptr<FileIndexWriter> writer;
     };
 
     Status AddIndexWriter(const std::string& column_name, const std::string& index_type,
                           std::shared_ptr<FileIndexWriter> writer,
                           std::shared_ptr<arrow::DataType> struct_type) override {
         if (struct_type) {
             auto it = column_struct_types_.find(column_name);
             if (it == column_struct_types_.end()) {
                 column_struct_types_[column_name] = struct_type;
             } else if (!it->second || !it->second->Equals(struct_type)) {
                 return Status::Invalid("conflicting struct_type for column " + column_name);
             }
         }
 
         columns_[column_name].push_back({index_type, std::move(writer)});
         return Status::OK();
     }
 
     Status AddBatch(const std::string& column_name,
                 const std::shared_ptr<arrow::StructArray>& batch) override {
         auto it = columns_.find(column_name);
         if (it == columns_.end()) {
             return Status::OK();
         }
 
         const auto& entries = it->second;
         for (const auto& entry : entries) {
             ::ArrowArray tmp_array{};
             PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch, &tmp_array));
             PAIMON_RETURN_NOT_OK(entry.writer->AddBatch(&tmp_array));
         }
 
         return Status::OK();
     }
 
     Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(
         const std::shared_ptr<MemoryPool>& pool) const override {
         // Phase 1: Collect serialized body bytes from each sub-writer.
         struct BodyEntry {
             std::string column_name;
             std::string index_type;
             PAIMON_UNIQUE_PTR<Bytes> body;
         };
         std::vector<BodyEntry> body_entries;
 
         for (const auto& [col_name, entries] : columns_) {
             for (const auto& entry : entries) {
                 PAIMON_ASSIGN_OR_RAISE(auto body_bytes, entry.writer->SerializedBytes());
                 body_entries.push_back({col_name, entry.index_type, std::move(body_bytes)});
             }
         }
 
         // Phase 2: Compute head_content_size and assign body offsets.
         //   head_content = column_number(4)
         //     + per column: column_name(2+len) + index_number(4)
         //       + per index: index_type(2+len) + start_pos(4) + length(4)
         //     + redundant_length(4)
 
         // Group by column for deterministic serialization order.
         std::map<std::string, std::vector<size_t>> column_to_body_indices;
         for (size_t i = 0; i < body_entries.size(); i++) {
             column_to_body_indices[body_entries[i].column_name].push_back(i);
         }
 
         int32_t head_content_size = 4;  // column_number
         for (const auto& [col_name, indices] : column_to_body_indices) {
             head_content_size += 2 + static_cast<int32_t>(col_name.size());  // column_name
             head_content_size += 4;                                          // index_number
             for (size_t idx : indices) {
                 head_content_size +=
                     2 + static_cast<int32_t>(body_entries[idx].index_type.size());  // index_type
                 head_content_size += 4;                                             // start_pos
                 head_content_size += 4;                                             // length
             }
         }
         head_content_size += 4;  // redundant_length
 
         // magic(8) + version(4) + head_length(4) + head_content
         int32_t head_length = 8 + 4 + 4 + head_content_size;
 
         // Compute start_pos for each body entry (absolute offset from file start).
         std::vector<int32_t> start_positions(body_entries.size());
         int32_t body_offset = 0;
         for (const auto& [col_name, indices] : column_to_body_indices) {
             for (size_t idx : indices) {
                 auto body_size = static_cast<int32_t>(body_entries[idx].body->size());
                 if (body_size == 0) {
                     start_positions[idx] = FileIndexFormat::EMPTY_INDEX_FLAG;
                 } else {
                     start_positions[idx] = head_length + body_offset;
                     body_offset += body_size;
                 }
             }
         }
 
         // Phase 3: Write the full binary using MemorySegmentOutputStream.
         MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
         out.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
 
         // Header preamble
         out.WriteValue<int64_t>(FileIndexFormat::MAGIC);
         out.WriteValue<int32_t>(FileIndexFormat::V_1);
         out.WriteValue<int32_t>(head_length);
 
         // Header content
         out.WriteValue<int32_t>(static_cast<int32_t>(column_to_body_indices.size()));
         for (const auto& [col_name, indices] : column_to_body_indices) {
             out.WriteString(col_name);
             out.WriteValue<int32_t>(static_cast<int32_t>(indices.size()));
             for (size_t idx : indices) {
                 out.WriteString(body_entries[idx].index_type);
                 out.WriteValue<int32_t>(start_positions[idx]);
                 out.WriteValue<int32_t>(static_cast<int32_t>(body_entries[idx].body->size()));
             }
         }
         out.WriteValue<int32_t>(0);  // redundant_length
 
         // Body
         for (const auto& [col_name, indices] : column_to_body_indices) {
             for (size_t idx : indices) {
                 if (body_entries[idx].body->size() > 0) {
                     out.Write(body_entries[idx].body->data(),
                               static_cast<uint32_t>(body_entries[idx].body->size()));
                 }
             }
         }
 
         // Phase 4: Convert segments to PAIMON_UNIQUE_PTR<Bytes> using memory pool.
         return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
     }
 
     bool IsEmpty() const override {
         return columns_.empty();
     }
 
  private:
     // Preserves insertion order per column, using map for deterministic serialization order.
     std::map<std::string, std::vector<IndexEntry>> columns_;
     // Struct type per column for safe import/export when multiple writers share a column.
     std::map<std::string, std::shared_ptr<arrow::DataType>> column_struct_types_;
 };
 
 const int64_t FileIndexFormat::MAGIC = 1493475289347502LL;
 const int32_t FileIndexFormat::EMPTY_INDEX_FLAG = -1;
 const int32_t FileIndexFormat::V_1 = 1;
 
 Result<std::unique_ptr<FileIndexFormat::Reader>> FileIndexFormat::CreateReader(
     const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
     return FileIndexFormatReaderImpl::Create(input_stream, pool);
 }
 
 std::unique_ptr<FileIndexFormat::Writer> FileIndexFormat::CreateWriter() {
     return std::make_unique<FileIndexFormatWriterImpl>();
 }
 
 }  // namespace paimon