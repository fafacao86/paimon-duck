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

 #include "paimon/core/io/data_file_writer.h"

 #include <cassert>
 
 #include "arrow/c/abi.h"
 #include "arrow/c/bridge.h"
 #include "arrow/c/helpers.h"
 #include "arrow/type.h"
 #include "paimon/common/utils/arrow/status_utils.h"
 #include "paimon/common/utils/long_counter.h"
 #include "paimon/common/utils/path_util.h"
 #include "paimon/common/utils/scope_guard.h"
 #include "paimon/core/io/data_file_path_factory.h"
 #include "paimon/core/stats/simple_stats.h"
 #include "paimon/core/stats/simple_stats_converter.h"
 #include "paimon/format/format_stats_extractor.h"
 #include "paimon/fs/file_system.h"
 
 namespace paimon {
 class MemoryPool;
 
 DataFileWriter::DataFileWriter(
     const std::string& compression, std::function<Status(::ArrowArray*, ::ArrowArray*)> converter,
     int64_t schema_id, const std::shared_ptr<LongCounter>& seq_num_counter, FileSource file_source,
     const std::shared_ptr<FormatStatsExtractor>& stats_extractor, bool is_external_path,
     const std::optional<std::vector<std::string>>& write_cols,
     const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::Schema>& write_schema,
     std::unique_ptr<FileIndexFormat::Writer> file_index_writer,
     int64_t file_index_in_manifest_threshold)
     : SingleFileWriter(compression, converter),
       pool_(pool),
       schema_id_(schema_id),
       is_external_path_(is_external_path),
       seq_num_counter_(seq_num_counter),
       file_source_(file_source),
       stats_extractor_(stats_extractor),
       write_cols_(write_cols),
       write_schema_(write_schema),
       file_index_writer_(std::move(file_index_writer)),
       file_index_in_manifest_threshold_(file_index_in_manifest_threshold) {}
 
 Status DataFileWriter::Write(ArrowArray* batch) {
     int64_t record_count = batch->length;
 
     if (file_index_writer_ && write_schema_ && !file_index_writer_->IsEmpty()) {
         PAIMON_RETURN_NOT_OK(FeedFileIndexWriter(batch));
     }
 
     PAIMON_RETURN_NOT_OK(SingleFileWriter::Write(batch));
     seq_num_counter_->Add(record_count);
     return Status::OK();
 }
 
 Status DataFileWriter::FeedFileIndexWriter(::ArrowArray* batch_array) {
     auto struct_type = arrow::struct_(write_schema_->fields());
     PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                       arrow::ImportArray(batch_array, struct_type));
     auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
 
     for (int32_t i = 0; i < write_schema_->num_fields(); i++) {
         auto field = write_schema_->field(i);
         auto col = struct_array->field(i);
         PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> col_struct,
                                           arrow::StructArray::Make({col}, {field->name()}));
         PAIMON_RETURN_NOT_OK(file_index_writer_->AddBatch(field->name(), col_struct));
     }
 
     PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, batch_array));
     return Status::OK();
 }
 
 Result<std::shared_ptr<DataFileMeta>> DataFileWriter::GetResult() {
     PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ColumnStats>> field_stats, GetFieldStats());
     PAIMON_ASSIGN_OR_RAISE(SimpleStats stats,
                            SimpleStatsConverter::ToBinary(field_stats, pool_.get()));
     // TODO(xinyu.lxy): do not support write value stats cols & first_row_id for now
     std::optional<std::string> final_path;
     if (is_external_path_) {
         PAIMON_ASSIGN_OR_RAISE(Path external_path, PathUtil::ToPath(path_));
         final_path = external_path.ToString();
     }
 
     std::shared_ptr<Bytes> embedded_index;
     std::vector<std::optional<std::string>> extra_files;
     if (file_index_writer_ && !file_index_writer_->IsEmpty()) {
         PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> index_bytes,
                                file_index_writer_->Serialize(pool_));
         if (static_cast<int64_t>(index_bytes->size()) <= file_index_in_manifest_threshold_) {
             embedded_index = std::shared_ptr<Bytes>(std::move(index_bytes));
         } else {
             std::string index_file_name =
                 PathUtil::GetName(path_) + DataFilePathFactory::INDEX_PATH_SUFFIX;
             std::string index_path =
                 PathUtil::JoinPath(PathUtil::GetParentDirPath(path_), index_file_name);
             PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                                    fs_->Create(index_path, /*overwrite=*/false));
             PAIMON_RETURN_NOT_OK(
                 out->Write(index_bytes->data(), static_cast<uint32_t>(index_bytes->size())));
             PAIMON_RETURN_NOT_OK(out->Flush());
             PAIMON_RETURN_NOT_OK(out->Close());
             extra_files.push_back(index_file_name);
         }
     }
 
     return DataFileMeta::ForAppend(
         PathUtil::GetName(path_), output_bytes_, RecordCount(), stats,
         seq_num_counter_->GetValue() - RecordCount(), seq_num_counter_->GetValue() - 1, schema_id_,
         extra_files, embedded_index, file_source_, /*value_stats_cols=*/std::nullopt, final_path,
         /*first_row_id=*/std::nullopt, write_cols_);
 }
 
 Result<std::vector<std::shared_ptr<ColumnStats>>> DataFileWriter::GetFieldStats() {
     if (!closed_) {
         return Status::Invalid("Cannot access metric unless the writer is closed.");
     }
     if (stats_extractor_ == nullptr) {
         assert(false);
         return Status::Invalid("simple stats extractor is null pointer.");
     }
     return stats_extractor_->Extract(fs_, path_, pool_);
 }
 
 }  // namespace paimon