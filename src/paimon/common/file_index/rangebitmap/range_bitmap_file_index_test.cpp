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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

#include <gtest/gtest.h>

#include <memory>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RangeBitmapFileIndexTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        fs_ = std::make_shared<LocalFileSystem>();
    }

    void TearDown() override {
        index_buffer_.reset();
        pool_.reset();
        fs_.reset();
    }

    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReader(
        const std::string& index_file_path) {
        // Read the index file
        PAIMON_ASSIGN_OR_RAISE(auto file, fs_->Open(index_file_path));
        PAIMON_ASSIGN_OR_RAISE(auto file_size, file->Length());
        auto buffer = std::make_shared<Bytes>(file_size, pool_.get());
        PAIMON_ASSIGN_OR_RAISE(auto bytes_read, file->Read(buffer->data(), file_size));
        if (static_cast<uint64_t>(bytes_read) != file_size) {
            return Status::IOError("Failed to read complete index file");
        }

        // Keep buffer alive for the lifetime of the reader; ByteArrayInputStream only holds raw
        // pointers.
        index_buffer_ = buffer;

        // Create input stream
        const auto input_stream =
            std::make_shared<ByteArrayInputStream>(buffer->data(), buffer->size());

        // Always use FileIndexFormat - it extracts the correct offset from the header
        PAIMON_RETURN_NOT_OK(input_stream->Seek(0, SeekOrigin::FS_SEEK_SET));
        PAIMON_ASSIGN_OR_RAISE(auto file_index_reader,
                               FileIndexFormat::CreateReader(input_stream, pool_));

        // Create arrow schema for BIGINT type
        const auto& arrow_type = arrow::int64();
        auto schema = arrow::schema({arrow::field("pid", arrow_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        if (!arrow::ExportSchema(*schema, c_schema.get()).ok()) {
            return Status::Invalid("Failed to create Arrow schema");
        }

        // ReadColumnIndex extracts the offset and length from the FileIndexFormat header
        // and passes them to CreateReader - no need to manually specify start=0
        PAIMON_ASSIGN_OR_RAISE(const auto index_readers,
                               file_index_reader->ReadColumnIndex("pid", c_schema.get()));

        // Find the RangeBitmap reader (it already has the correct offset set up internally)
        for (const auto& reader : index_readers) {
            if (auto range_bitmap_reader =
                    std::dynamic_pointer_cast<RangeBitmapFileIndexReader>(reader)) {
                return range_bitmap_reader;
            }
        }

        return Status::NotExist("RangeBitmap index not found in FileIndexFormat");
    }

    static void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                     const std::vector<int32_t>& expected) {
        const auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        const RoaringBitmap32 expected_bitmap = RoaringBitmap32::From(expected);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), expected_bitmap)
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << expected_bitmap.ToString();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<Bytes> index_buffer_;
};

// Helper function to create writer, serialize, and create reader
template<typename ArrowBuilder, typename ValueType>
Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderForTest(
    RangeBitmapFileIndexTest* test,
    const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::vector<ValueType>& test_data,
    std::shared_ptr<Bytes>& serialized_bytes_out) {
    // Create Arrow array from test data
    auto builder = std::make_shared<ArrowBuilder>();
    auto status = builder->AppendValues(test_data);
    if (!status.ok()) {
        return Status::Invalid("Failed to append values: " + status.ToString());
    }
    std::shared_ptr<arrow::Array> arrow_array;
    status = builder->Finish(&arrow_array);
    if (!status.ok()) {
        return Status::Invalid("Failed to finish builder: " + status.ToString());
    }

    // Create ArrowArray C struct
    auto c_array = std::make_unique<::ArrowArray>();
    status = arrow::ExportArray(*arrow_array, c_array.get());
    if (!status.ok()) {
        return Status::Invalid("Failed to export array: " + status.ToString());
    }

    // Create schema for the field
    auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

    // Create writer
    PAIMON_ASSIGN_OR_RAISE(auto writer,
                           RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, test->pool_));

    // Add the batch
    PAIMON_RETURN_NOT_OK(writer->AddBatch(c_array.get()));

    // Get serialized payload
    PAIMON_ASSIGN_OR_RAISE(auto serialized_bytes, writer->SerializedBytes());
    if (!serialized_bytes || serialized_bytes->size() == 0) {
        return Status::Invalid("Serialized bytes is empty");
    }

    // Convert to shared_ptr to ensure lifetime extends beyond function return
    serialized_bytes_out = std::shared_ptr<Bytes>(serialized_bytes.release());

    // Read payload
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes_out->data(), serialized_bytes_out->size());
    PAIMON_ASSIGN_OR_RAISE(
        auto reader, RangeBitmapFileIndexReader::Create(
                         arrow_type, /*start=*/0, static_cast<int32_t>(serialized_bytes_out->size()),
                         input_stream, test->pool_));
    return reader;
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_BigInt) {
    std::vector<int64_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int64();

    // Create Arrow array from test data
    auto builder = std::make_shared<arrow::Int64Builder>();
    ASSERT_TRUE(builder->AppendValues(test_data).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder->Finish(&arrow_array).ok());

    // Create ArrowArray C struct
    auto c_array = std::make_unique<::ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*arrow_array, c_array.get()).ok());

    // Create schema
    auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

    // Create writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, pool_));

    // Add the batch
    ASSERT_OK(writer->AddBatch(c_array.get()));

    // Get serialized payload
    ASSERT_OK_AND_ASSIGN(auto serialized_bytes_unique, writer->SerializedBytes());
    ASSERT_TRUE(serialized_bytes_unique);
    ASSERT_GT(serialized_bytes_unique->size(), 0);

    // Convert to shared_ptr to ensure lifetime extends throughout test
    std::shared_ptr<Bytes> serialized_bytes(serialized_bytes_unique.release());

    // Read payload
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes->data(), serialized_bytes->size());
    ASSERT_OK_AND_ASSIGN(
        auto reader, RangeBitmapFileIndexReader::Create(
                         arrow_type, /*start=*/0, static_cast<int32_t>(serialized_bytes->size()),
                         input_stream, pool_));
    ASSERT_TRUE(reader);

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int64_t>(10))));
    CheckResult(eq_10_result, {0, 2});  // positions 0 and 2 have value 10

    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int64_t>(20))));
    CheckResult(eq_20_result, {1, 4});  // positions 1 and 4 have value 20

    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int64_t>(30))));
    CheckResult(eq_30_result, {3});  // position 3 has value 30

    ASSERT_OK_AND_ASSIGN(auto eq_40_result, reader->VisitEqual(Literal(static_cast<int64_t>(40))));
    CheckResult(eq_40_result, {5});  // position 5 has value 40

    ASSERT_OK_AND_ASSIGN(auto eq_50_result, reader->VisitEqual(Literal(static_cast<int64_t>(50))));
    CheckResult(eq_50_result, {6});  // position 6 has value 50

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result,
                         reader->VisitGreaterThan(Literal(static_cast<int64_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result,
                         reader->VisitLessThan(Literal(static_cast<int64_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35: 10, 20, 10, 30, 20

    ASSERT_OK_AND_ASSIGN(auto gte_20_result,
                         reader->VisitGreaterOrEqual(Literal(static_cast<int64_t>(20))));
    CheckResult(gte_20_result, {1, 3, 4, 5, 6});  // values >= 20: 20, 30, 20, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lte_40_result,
                         reader->VisitLessOrEqual(Literal(static_cast<int64_t>(40))));
    CheckResult(lte_40_result, {0, 1, 2, 3, 4, 5});  // values <= 40: 10, 20, 10, 30, 20, 40

    // Test IN queries
    std::vector<Literal> in_values = {Literal(static_cast<int64_t>(10)),
                                      Literal(static_cast<int64_t>(30))};
    ASSERT_OK_AND_ASSIGN(auto in_result, reader->VisitIn(in_values));
    CheckResult(in_result, {0, 2, 3});  // positions with values 10 or 30

    // Test NOT IN queries
    ASSERT_OK_AND_ASSIGN(auto not_in_result, reader->VisitNotIn(in_values));
    CheckResult(not_in_result, {1, 4, 5, 6});  // positions with values NOT 10 or 30

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});  // no null values

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);  // all positions are not null
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_Int) {
    std::vector<int32_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int32();

    std::shared_ptr<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(this, arrow_type, test_data, serialized_bytes)));

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int32_t>(10))));
    CheckResult(eq_10_result, {0, 2});

    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int32_t>(20))));
    CheckResult(eq_20_result, {1, 4});

    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int32_t>(30))));
    CheckResult(eq_30_result, {3});

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result, reader->VisitGreaterThan(Literal(static_cast<int32_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(static_cast<int32_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_SmallInt) {
    std::vector<int16_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int16();

    std::shared_ptr<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int16Builder, int16_t>(this, arrow_type, test_data, serialized_bytes)));

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int16_t>(10))));
    CheckResult(eq_10_result, {0, 2});

    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int16_t>(20))));
    CheckResult(eq_20_result, {1, 4});

    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int16_t>(30))));
    CheckResult(eq_30_result, {3});

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result, reader->VisitGreaterThan(Literal(static_cast<int16_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(static_cast<int16_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_TinyInt) {
    std::vector<int8_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int8();

    std::shared_ptr<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int8Builder, int8_t>(this, arrow_type, test_data, serialized_bytes)));

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int8_t>(10))));
    CheckResult(eq_10_result, {0, 2});

    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int8_t>(20))));
    CheckResult(eq_20_result, {1, 4});

    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int8_t>(30))));
    CheckResult(eq_30_result, {3});

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result, reader->VisitGreaterThan(Literal(static_cast<int8_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(static_cast<int8_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_Boolean) {
    std::vector<bool> test_data = {true, false, true, true, false, true, false};
    const auto& arrow_type = arrow::boolean();

    // Create Arrow array from test data
    auto builder = std::make_shared<arrow::BooleanBuilder>();
    ASSERT_TRUE(builder->AppendValues(test_data).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder->Finish(&arrow_array).ok());

    // Create ArrowArray C struct
    auto c_array = std::make_unique<::ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*arrow_array, c_array.get()).ok());

    // Create schema
    auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

    // Create writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, pool_));

    // Add the batch
    ASSERT_OK(writer->AddBatch(c_array.get()));

    // Get serialized payload
    ASSERT_OK_AND_ASSIGN(auto serialized_bytes_unique, writer->SerializedBytes());
    ASSERT_TRUE(serialized_bytes_unique);
    ASSERT_GT(serialized_bytes_unique->size(), 0);

    // Convert to shared_ptr to ensure lifetime extends throughout test
    std::shared_ptr<Bytes> serialized_bytes(serialized_bytes_unique.release());

    // Read payload
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes->data(), serialized_bytes->size());
    ASSERT_OK_AND_ASSIGN(
        auto reader, RangeBitmapFileIndexReader::Create(
                         arrow_type, /*start=*/0, static_cast<int32_t>(serialized_bytes->size()),
                         input_stream, pool_));
    ASSERT_TRUE(reader);

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_true_result, reader->VisitEqual(Literal(true)));
    CheckResult(eq_true_result, {0, 2, 3, 5});  // positions with value true

    ASSERT_OK_AND_ASSIGN(auto eq_false_result, reader->VisitEqual(Literal(false)));
    CheckResult(eq_false_result, {1, 4, 6});  // positions with value false

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_Float) {
    std::vector<float> test_data = {10.5f, 20.3f, 10.5f, 30.7f, 20.3f, 40.1f, 50.9f};
    const auto& arrow_type = arrow::float32();

    // Create Arrow array from test data
    auto builder = std::make_shared<arrow::FloatBuilder>();
    ASSERT_TRUE(builder->AppendValues(test_data).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder->Finish(&arrow_array).ok());

    // Create ArrowArray C struct
    auto c_array = std::make_unique<::ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*arrow_array, c_array.get()).ok());

    // Create schema
    auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

    // Create writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, pool_));

    // Add the batch
    ASSERT_OK(writer->AddBatch(c_array.get()));

    // Get serialized payload
    ASSERT_OK_AND_ASSIGN(auto serialized_bytes_unique, writer->SerializedBytes());
    ASSERT_TRUE(serialized_bytes_unique);
    ASSERT_GT(serialized_bytes_unique->size(), 0);

    // Convert to shared_ptr to ensure lifetime extends throughout test
    std::shared_ptr<Bytes> serialized_bytes(serialized_bytes_unique.release());

    // Read payload
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes->data(), serialized_bytes->size());
    ASSERT_OK_AND_ASSIGN(
        auto reader, RangeBitmapFileIndexReader::Create(
                         arrow_type, /*start=*/0, static_cast<int32_t>(serialized_bytes->size()),
                         input_stream, pool_));
    ASSERT_TRUE(reader);

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_5_result, reader->VisitEqual(Literal(10.5f)));
    CheckResult(eq_10_5_result, {0, 2});  // positions with value 10.5

    ASSERT_OK_AND_ASSIGN(auto eq_20_3_result, reader->VisitEqual(Literal(20.3f)));
    CheckResult(eq_20_3_result, {1, 4});  // positions with value 20.3

    ASSERT_OK_AND_ASSIGN(auto eq_30_7_result, reader->VisitEqual(Literal(30.7f)));
    CheckResult(eq_30_7_result, {3});  // position with value 30.7

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result, reader->VisitGreaterThan(Literal(25.0f)));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25.0: 30.7, 40.1, 50.9

    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(35.0f)));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35.0

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndex_Date) {
    // Date is stored as int32_t (days since epoch)
    std::vector<int32_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::date32();

    // Create Arrow array from test data
    auto builder = std::make_shared<arrow::Date32Builder>();
    ASSERT_TRUE(builder->AppendValues(test_data).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder->Finish(&arrow_array).ok());

    // Create ArrowArray C struct
    auto c_array = std::make_unique<::ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*arrow_array, c_array.get()).ok());

    // Create schema
    auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

    // Create writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, pool_));

    // Add the batch
    ASSERT_OK(writer->AddBatch(c_array.get()));

    // Get serialized payload
    ASSERT_OK_AND_ASSIGN(auto serialized_bytes_unique, writer->SerializedBytes());
    ASSERT_TRUE(serialized_bytes_unique);
    ASSERT_GT(serialized_bytes_unique->size(), 0);

    // Convert to shared_ptr to ensure lifetime extends throughout test
    std::shared_ptr<Bytes> serialized_bytes(serialized_bytes_unique.release());

    // Read payload
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(serialized_bytes->data(), serialized_bytes->size());
    ASSERT_OK_AND_ASSIGN(
        auto reader, RangeBitmapFileIndexReader::Create(
                         arrow_type, /*start=*/0, static_cast<int32_t>(serialized_bytes->size()),
                         input_stream, pool_));
    ASSERT_TRUE(reader);

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(FieldType::DATE, 10)));
    CheckResult(eq_10_result, {0, 2});  // positions with value 10

    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(FieldType::DATE, 20)));
    CheckResult(eq_20_result, {1, 4});  // positions with value 20

    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(FieldType::DATE, 30)));
    CheckResult(eq_30_result, {3});  // position with value 30

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result, reader->VisitGreaterThan(Literal(FieldType::DATE, 25)));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(FieldType::DATE, 35)));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35

    // Test null checks
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

}  // namespace paimon::test