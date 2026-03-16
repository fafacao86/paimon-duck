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
 #include <string>
 #include <vector>
 
 #include "arrow/type_fwd.h"
 #include "paimon/file_index/file_index_reader.h"
 #include "paimon/file_index/file_index_writer.h"
 #include "paimon/memory/bytes.h"
 #include "paimon/memory/memory_pool.h"
 #include "paimon/result.h"
 #include "paimon/visibility.h"
 
 struct ArrowSchema;
 struct ArrowArray;
 
 namespace paimon {
 class InputStream;
 class MemoryPool;
 
 /// Defines the on-disk format and versioning for Paimon file-level indexes.
 /// File index file format. Put all column and offset in the header.
 ///
 /// <pre>
 ///  _______________________________________    _____________________
 /// |     magic    | version | head length |
 /// |--------------------------------------|
 /// |            column number             |
 /// |--------------------------------------|
 /// |   column 1        |  index number    |
 /// |--------------------------------------|
 /// |  index name 1 | start pos | length   |
 /// |--------------------------------------|
 /// |  index name 2 | start pos | length   |
 /// |--------------------------------------|
 /// |  index name 3 | start pos | length   |
 /// |--------------------------------------|            HEAD
 /// |   column 2        |  index number    |
 /// |--------------------------------------|
 /// |  index name 1 | start pos | length   |
 /// |--------------------------------------|
 /// |  index name 2 | start pos | length   |
 /// |--------------------------------------|
 /// |  index name 3 | start pos | length   |
 /// |--------------------------------------|
 /// |                 ...                  |
 /// |--------------------------------------|
 /// |                 ...                  |
 /// |--------------------------------------|
 /// |  redundant length | redundant bytes  |
 /// |--------------------------------------|    ---------------------
 /// |                BODY                  |
 /// |                BODY                  |
 /// |                BODY                  |             BODY
 /// |                BODY                  |
 /// |______________________________________|    _____________________
 ///
 /// magic:               8 bytes long
 /// version:             4 bytes int
 /// head length:         4 bytes int
 /// column number:       4 bytes int
 /// column x:            var bytes utf (length + bytes)
 /// index number:        4 bytes int (how many column items below)
 /// index name x:        var bytes utf
 /// start pos:           4 bytes int
 /// length:              4 bytes int
 /// redundant length:    4 bytes int (for compatibility with later versions, in this version,
 ///                      content is zero)
 /// redundant bytes:     var bytes (for compatibility with later version, in this version, is empty)
 /// BODY:                column index bytes + column index bytes + column index bytes + .......
 /// </pre>
 ///
 class PAIMON_EXPORT FileIndexFormat {
  public:
     class Reader;
     class Writer;
 
     /// Creates a `Reader` to parse a index file (may contain multiple indexes) from the given input
     /// stream.
     ///
     /// @param input_stream Input stream containing serialized index data.
     /// @param pool Memory pool for temporary allocations during reading.
     /// @return A unique pointer to a `Reader` on success, or an error if the stream is invalid
     ///         (e.g., wrong magic, unsupported version, or corrupted data).
     static Result<std::unique_ptr<Reader>> CreateReader(
         const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool);
 
     /// Creates a `Writer` for building a complete file index (header + body).
     static std::unique_ptr<Writer> CreateWriter();
 
  public:
     static const int64_t MAGIC;
     static const int32_t EMPTY_INDEX_FLAG;
     static const int32_t V_1;
 };
 
 /// Reader for file index file.
 class FileIndexFormat::Reader {
  public:
     virtual ~Reader() = default;
     /// Reads index data for a specific column from the index file.
     ///
     /// @param column_name Name of the column to retrieve index data for.
     /// @param arrow_schema Arrow schema that must contain a field corresponding to `column_name`.
     /// @return A vector of shared pointers to FileIndexReader objects, each corresponding to a
     ///         different index type; or an error if the column is not indexed or the index is
     ///         malformed.
     virtual Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadColumnIndex(
         const std::string& column_name, ::ArrowSchema* arrow_schema) const = 0;
 };
 
 /// Writer for file index file. Collects per-column index writers, feeds them data,
 /// and serializes the complete file index binary (magic + version + header + body).
 class FileIndexFormat::Writer {
  public:
     virtual ~Writer() = default;
 
     /// Registers a sub-writer for a specific (column, index_type) pair.
     ///
     /// @param column_name  Name of the column this index covers.
     /// @param index_type   Identifier of the index type (e.g. "bitmap", "bsi").
     /// @param writer       The concrete FileIndexWriter that builds the index body bytes.
     /// @param struct_type  Arrow struct type wrapping the single indexed field, used internally
     ///                     to safely duplicate the ArrowArray when multiple writers share a column.
     virtual Status AddIndexWriter(const std::string& column_name, const std::string& index_type,
                                   std::shared_ptr<FileIndexWriter> writer,
                                   std::shared_ptr<arrow::DataType> struct_type) = 0;
     /// Feeds a single-column batch to all sub-writers registered for the given column.
     ///
     /// @param column_name Name of the column whose sub-writers should receive this batch.
     /// @param batch       A shared Arrow StructArray containing the column data. The struct is
     ///                    expected to wrap the indexed field as a single child field.
     ///
     /// @note This method borrows the shared batch and does not take ownership away from the caller.
     ///       If no sub-writer is registered for the column, the batch is ignored.
     ///       For each registered sub-writer, this writer may export an independent ArrowArray
     ///       instance from the shared batch and forward it to the sub-writer.
     ///       The caller retains ownership of the shared_ptr and may continue to use it after
     ///       this call returns.
     ///
     /// @return `Status::OK()` on success; otherwise, an error indicating failure.
     virtual Status AddBatch(const std::string& column_name,
                             const std::shared_ptr<arrow::StructArray>& batch) = 0;
 
     /// Serializes the complete file index (magic + version + header + body) into a Bytes buffer.
     ///
     /// @param pool Memory pool for output allocation.
     /// @return The serialized bytes, or an error if any sub-writer fails to serialize.
     virtual Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(
         const std::shared_ptr<MemoryPool>& pool) const = 0;
 
     /// Returns true if no sub-writers have been registered.
     virtual bool IsEmpty() const = 0;
 };
 
 }  // namespace paimon