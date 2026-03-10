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

#include <utility>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/helpers.h"
#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"
#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FileIndexFormatTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::Schema>& schema) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(FileIndexFormatTest, TestCreateEmptyFileIndexReader) {
    auto schema = arrow::schema({arrow::field("c1", arrow::utf8())});
    std::vector<char> index_file_bytes = {0,  5,  78, 78, -48, 26, 53,  -82, 0,   0,   0,   1,
                                          0,  0,  0,  47, 0,   0,  0,   1,   0,   2,   99,  49,
                                          0,  0,  0,  1,  0,   5,  101, 109, 112, 116, 121, -1,
                                          -1, -1, -1, 0,  0,   0,  0,   0,   0,   0,   0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_file_bytes.data(), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                         reader->ReadColumnIndex("c1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, index_file_readers.size());
    auto* empty_reader = dynamic_cast<EmptyFileIndexReader*>(index_file_readers[0].get());
    ASSERT_TRUE(empty_reader);
}

TEST_F(FileIndexFormatTest, TestSimple) {
    auto schema =
        arrow::schema({arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::int32()),
                       arrow::field("non-exist", arrow::int32())});
    std::vector<uint8_t> index_file_bytes = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,  0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98, 105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   131, 0,   2,   102, 49,  0,  0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   227, 0,   0,   0,  74,  0,   2,   102, 50,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,  0,   1,   45,  0,   0,
        0,   76,  0,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,  0,   5,   0,   0,   0,
        0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,  0,   4,   76,  117, 99,
        121, 255, 255, 255, 251, 0,   0,   0,   3,   66,  111, 98,  0,  0,   0,   20,  0,   0,
        0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 0,   0,  0,   4,   84,  111, 110,
        121, 0,   0,   0,   40,  58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   1,   0,   16,
        0,   0,   0,   0,   0,   7,   0,   58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   1,
        0,   16,  0,   0,   0,   1,   0,   5,   0,   58,  48,  0,   0,  1,   0,   0,   0,   0,
        0,   1,   0,   16,  0,   0,   0,   3,   0,   6,   0,   1,   0,  0,   0,   8,   0,   0,
        0,   2,   0,   0,   0,   0,   20,  0,   0,   0,   0,   0,   0,  0,   10,  0,   0,   0,
        22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,  16,  0,   0,   0,   4,
        0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   4,   0,   16,
        0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,  1,   0,   0,   0,   8,
        0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   0,  0,   0,   0,   0,   0,
        0,   0,   1,   0,   0,   0,   22,  58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   2,
        0,   16,  0,   0,   0,   2,   0,   3,   0,   6,   0,   58,  48, 0,   0,   1,   0,   0,
        0,   0,   0,   3,   0,   16,  0,   0,   0,   0,   0,   1,   0,  4,   0,   5,   0};
    auto input_stream = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<char*>(index_file_bytes.data()), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f1", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,2,3,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(50)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f2", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(0)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(1)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,4,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_TRUE(result);
            ASSERT_EQ("{7}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("non-exist", CreateArrowSchema(schema).get()));
        ASSERT_TRUE(index_file_readers.empty());
    }
}

// NOLINTNEXTLINE(google-readability-function-size)
TEST_F(FileIndexFormatTest, TestBitmapIndexWithTimestamp) {
    auto schema = arrow::schema({
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Tokyo")),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Tokyo")),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Tokyo")),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Tokyo")),
    });

    auto fs = std::make_shared<LocalFileSystem>();
    std::string file_name = GetDataDir() +
                            "orc/timestamp_index.db/timestamp_index/"
                            "bucket-0/data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc.index";
    std::string index_file_bytes;
    ASSERT_OK(fs->ReadFile(file_name, &index_file_bytes));
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_file_bytes.data(), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    auto check_second = [&](const std::string& field_name) {
        // data: second
        // 1745542802000lms, 0ns
        // 1745542902000lms, 0ns
        // 1745542602000lms, 0ns
        // -1745000lms, 0ns
        // -1765000lms, 0ns
        // null
        // 1745542802000lms, 0ns
        // -1725000lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                        Literal(Timestamp(-1745000l, 0)),
                                                        Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                      Literal(Timestamp(-1745000l, 0)),
                                                      Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745000l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_milli = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 0ns
        // 1745542902001lms, 0ns
        // 1745542602001lms, 0ns
        // -1745001lms, 0ns
        // -1765001lms, 0ns
        // null
        // 1745542802001lms, 0ns
        // -1725001lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                        Literal(Timestamp(-1745001l, 0)),
                                                        Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                      Literal(Timestamp(-1745001l, 0)),
                                                      Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    auto check_micro = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1000ns
        // 1745542902001lms, 1000ns
        // 1745542602001lms, 1000ns
        // -1745001lms, 1000ns
        // -1765001lms, 1000ns
        // null
        // 1745542802001lms, 1000ns
        // -1725001lms, 1000ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_nano = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1001ns
        // 1745542902001lms, 1001ns
        // 1745542602001lms, 1001ns
        // -1745001lms, 1001ns
        // -1765001lms, 1001ns
        // null
        // 1745542802001lms, 1001ns
        // -1725001lms, 1001ns

        // as timestamp is normalized by micro seconds, there is a loss of precision in the
        // nanosecond part
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    check_second("ts_sec");
    check_second("ts_tz_sec");

    check_milli("ts_milli");
    check_milli("ts_tz_milli");

    check_micro("ts_micro");
    check_micro("ts_tz_micro");

    check_nano("ts_nano");
    check_nano("ts_tz_nano");
}

TEST_F(FileIndexFormatTest, TestWriterAndReader) {
    auto col1_field = arrow::field("col1", arrow::int32());
    auto col2_field = arrow::field("col2", arrow::int32());
    auto full_schema = arrow::schema({col1_field, col2_field});

    auto writer = FileIndexFormat::CreateWriter();
    ASSERT_TRUE(writer->IsEmpty());

    // Register bitmap sub-writer for col1
    {
        ArrowSchema c_col1_schema;
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({col1_field}), &c_col1_schema).ok());
        BitmapFileIndex bitmap_indexer({});
        ASSERT_OK_AND_ASSIGN(auto col1_writer, bitmap_indexer.CreateWriter(&c_col1_schema, pool_));
        writer->AddIndexWriter("col1", "bitmap", std::move(col1_writer),
                               arrow::struct_({col1_field}));
    }

    // Register bitmap sub-writer for col2
    {
        ArrowSchema c_col2_schema;
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({col2_field}), &c_col2_schema).ok());
        BitmapFileIndex bitmap_indexer({});
        ASSERT_OK_AND_ASSIGN(auto col2_writer, bitmap_indexer.CreateWriter(&c_col2_schema, pool_));
        writer->AddIndexWriter("col2", "bitmap", std::move(col2_writer),
                               arrow::struct_({col2_field}));
    }

    ASSERT_FALSE(writer->IsEmpty());

    // Feed col1 data: [1, 2, 1, null, 2, 1, 3, null]
    {
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder
                        .AppendValues({1, 2, 1, 0, 2, 1, 3, 0},
                                      {true, true, true, false, true, true, true, false})
                        .ok());
        std::shared_ptr<arrow::Array> col1_array;
        ASSERT_TRUE(builder.Finish(&col1_array).ok());
        auto col1_struct = arrow::StructArray::Make({col1_array}, {"col1"}).ValueOrDie();
        ArrowArray c_col1_array;
        ASSERT_TRUE(arrow::ExportArray(*col1_struct, &c_col1_array).ok());
        ASSERT_OK(writer->AddBatch("col1", &c_col1_array));
    }

    // Feed col2 data: [10, 10, 20, 10, 20, null, 30, 30]
    {
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder
                        .AppendValues({10, 10, 20, 10, 20, 0, 30, 30},
                                      {true, true, true, true, true, false, true, true})
                        .ok());
        std::shared_ptr<arrow::Array> col2_array;
        ASSERT_TRUE(builder.Finish(&col2_array).ok());
        auto col2_struct = arrow::StructArray::Make({col2_array}, {"col2"}).ValueOrDie();
        ArrowArray c_col2_array;
        ASSERT_TRUE(arrow::ExportArray(*col2_struct, &c_col2_array).ok());
        ASSERT_OK(writer->AddBatch("col2", &c_col2_array));
    }

    // Serialize
    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    ASSERT_NE(nullptr, bytes);
    ASSERT_GT(bytes->size(), 0u);

    // Read back
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    // Verify col1
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("col1", CreateArrowSchema(full_schema).get()));
        ASSERT_EQ(1, readers.size());
        auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
        ASSERT_TRUE(bm);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(1)));
            ASSERT_EQ("{0,2,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(2)));
            ASSERT_EQ("{1,4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(3)));
            ASSERT_EQ("{6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
            ASSERT_EQ("{3,7}", result->ToString());
        }
    }

    // Verify col2
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("col2", CreateArrowSchema(full_schema).get()));
        ASSERT_EQ(1, readers.size());
        auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
        ASSERT_TRUE(bm);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(10)));
            ASSERT_EQ("{0,1,3}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(20)));
            ASSERT_EQ("{2,4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(30)));
            ASSERT_EQ("{6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
    }

    // Non-existent column should return empty
    {
        auto schema_with_extra =
            arrow::schema({col1_field, col2_field, arrow::field("non-exist", arrow::int32())});
        ASSERT_OK_AND_ASSIGN(
            auto readers,
            reader->ReadColumnIndex("non-exist", CreateArrowSchema(schema_with_extra).get()));
        ASSERT_TRUE(readers.empty());
    }
}

// Helper: register a bitmap sub-writer for the given field into the writer.
// Consumes the exported schema via BitmapFileIndex::CreateWriter (which calls ImportSchema).
static void RegisterBitmapWriter(FileIndexFormat::Writer* writer, const std::string& col_name,
                                 const std::shared_ptr<arrow::DataType>& arrow_type,
                                 const std::shared_ptr<MemoryPool>& pool,
                                 const std::string& index_type = "bitmap") {
    auto col_field = arrow::field(col_name, arrow_type);
    auto field_schema = arrow::schema({col_field});
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*field_schema, &c_schema).ok());

    BitmapFileIndex bitmap_indexer({});
    ASSERT_OK_AND_ASSIGN(auto sub_writer, bitmap_indexer.CreateWriter(&c_schema, pool));
    writer->AddIndexWriter(col_name, index_type, std::move(sub_writer),
                           arrow::struct_({col_field}));
}

// Helper: build a single-column int32 struct ArrowArray and call writer->AddBatch.
static void AddInt32Batch(FileIndexFormat::Writer* writer, const std::string& col_name,
                          const std::vector<int32_t>& values, const std::vector<bool>& validity) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues(values, validity).ok());
    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder.Finish(&arr).ok());
    auto struct_arr = arrow::StructArray::Make({arr}, {col_name}).ValueOrDie();
    ArrowArray c_arr;
    ASSERT_TRUE(arrow::ExportArray(*struct_arr, &c_arr).ok());
    ASSERT_OK(writer->AddBatch(col_name, &c_arr));
}

TEST_F(FileIndexFormatTest, TestWriterHeaderBytesExact) {
    // Single column "a" (1 byte name), single "bitmap" (6 bytes) index, 3 rows.
    // Expected head_length = 8+4+4 + (4 + (2+1) + 4 + (2+6) + 4+4 + 4) = 8+4+4+31 = 47.
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "a", arrow::int32(), pool_);
    AddInt32Batch(writer.get(), "a", {1, 2, 1}, {true, true, true});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    ASSERT_GE(bytes->size(), 47u);

    auto* data = reinterpret_cast<const uint8_t*>(bytes->data());
    auto read_be_int64 = [&](int32_t off) -> int64_t {
        int64_t v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | data[off + i];
        return v;
    };
    auto read_be_int32 = [&](int32_t off) -> int32_t {
        return (static_cast<int32_t>(data[off]) << 24) |
               (static_cast<int32_t>(data[off + 1]) << 16) |
               (static_cast<int32_t>(data[off + 2]) << 8) | static_cast<int32_t>(data[off + 3]);
    };
    auto read_be_int16 = [&](int32_t off) -> int16_t {
        return static_cast<int16_t>((static_cast<int16_t>(data[off]) << 8) | data[off + 1]);
    };

    // magic (bytes 0..7)
    ASSERT_EQ(FileIndexFormat::MAGIC, read_be_int64(0));
    // version (bytes 8..11)
    ASSERT_EQ(FileIndexFormat::V_1, read_be_int32(8));
    // head_length (bytes 12..15)
    ASSERT_EQ(47, read_be_int32(12));
    // column_count (bytes 16..19)
    ASSERT_EQ(1, read_be_int32(16));
    // col_name_len (bytes 20..21) = 1
    ASSERT_EQ(1, read_be_int16(20));
    // col_name (byte 22) = 'a'
    ASSERT_EQ(static_cast<uint8_t>('a'), data[22]);
    // index_count (bytes 23..26) = 1
    ASSERT_EQ(1, read_be_int32(23));
    // index_type_len (bytes 27..28) = 6 ("bitmap")
    ASSERT_EQ(6, read_be_int16(27));
    // index_type (bytes 29..34) = "bitmap"
    ASSERT_EQ('b', static_cast<char>(data[29]));
    ASSERT_EQ('i', static_cast<char>(data[30]));
    ASSERT_EQ('t', static_cast<char>(data[31]));
    ASSERT_EQ('m', static_cast<char>(data[32]));
    ASSERT_EQ('a', static_cast<char>(data[33]));
    ASSERT_EQ('p', static_cast<char>(data[34]));
    // start_pos (bytes 35..38) = 47 (= head_length, first body)
    ASSERT_EQ(47, read_be_int32(35));
    // body length (bytes 39..42) > 0
    int32_t body_len = read_be_int32(39);
    ASSERT_GT(body_len, 0);
    // redundant_length (bytes 43..46) = 0
    ASSERT_EQ(0, read_be_int32(43));
    // total size = head_length + body_len
    ASSERT_EQ(static_cast<size_t>(47 + body_len), bytes->size());
}

TEST_F(FileIndexFormatTest, TestWriterOffsetAndLengthConsistency) {
    // Two columns "col1" and "col2" (map order: col1 < col2 lexicographically).
    // head_content = 4 + 2*(2+4 + 4 + 2+6 + 4+4) + 4 = 4 + 2*26 + 4 = 60
    // head_length = 8+4+4+60 = 76
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "col1", arrow::int32(), pool_);
    RegisterBitmapWriter(writer.get(), "col2", arrow::int32(), pool_);
    AddInt32Batch(writer.get(), "col1", {1, 2, 3}, {true, true, true});
    AddInt32Batch(writer.get(), "col2", {10, 20, 30}, {true, true, true});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));

    auto* data = reinterpret_cast<const uint8_t*>(bytes->data());
    auto read_be_int32 = [&](int32_t off) -> int32_t {
        return (static_cast<int32_t>(data[off]) << 24) |
               (static_cast<int32_t>(data[off + 1]) << 16) |
               (static_cast<int32_t>(data[off + 2]) << 8) | static_cast<int32_t>(data[off + 3]);
    };

    static constexpr int32_t EXPECTED_HEAD_LENGTH = 76;
    // head_length at [12..15]
    ASSERT_EQ(EXPECTED_HEAD_LENGTH, read_be_int32(12));
    // column_count at [16..19] = 2
    ASSERT_EQ(2, read_be_int32(16));

    // col1 start_pos at [38..41], length at [42..45]
    int32_t col1_start = read_be_int32(38);
    int32_t col1_len = read_be_int32(42);
    // col2 start_pos at [64..67], length at [68..71]
    int32_t col2_start = read_be_int32(64);
    int32_t col2_len = read_be_int32(68);
    // redundant_length at [72..75] = 0
    ASSERT_EQ(0, read_be_int32(72));

    ASSERT_GT(col1_len, 0);
    ASSERT_GT(col2_len, 0);
    // col1 body starts immediately after the header
    ASSERT_EQ(EXPECTED_HEAD_LENGTH, col1_start);
    // col2 body starts right after col1 body
    ASSERT_EQ(col1_start + col1_len, col2_start);
    // total bytes = header + col1 body + col2 body
    ASSERT_EQ(static_cast<size_t>(col2_start + col2_len), bytes->size());
}

TEST_F(FileIndexFormatTest, TestWriterEmptyWriter) {
    // A writer with no sub-writers should serialize to a valid 24-byte stream
    // and the resulting reader should return an empty vector for any queried column.
    auto writer = FileIndexFormat::CreateWriter();
    ASSERT_TRUE(writer->IsEmpty());

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    ASSERT_NE(nullptr, bytes);
    // magic(8) + version(4) + head_length(4) + column_count=0(4) + redundant=0(4) = 24
    ASSERT_EQ(24u, bytes->size());

    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema({arrow::field("col1", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("col1", CreateArrowSchema(schema).get()));
    ASSERT_TRUE(readers.empty());
}

TEST_F(FileIndexFormatTest, TestWriterMultipleBatches) {
    // Split [1,2,3,1,2,3,null] into 3 batches and verify cumulative row indices.
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "c", arrow::int32(), pool_);

    // Batch 1: [1, 2, 3] (rows 0-2)
    AddInt32Batch(writer.get(), "c", {1, 2, 3}, {true, true, true});
    // Batch 2: [1, 2, 3] (rows 3-5)
    AddInt32Batch(writer.get(), "c", {1, 2, 3}, {true, true, true});
    // Batch 3: [null] (row 6)
    AddInt32Batch(writer.get(), "c", {0}, {false});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema({arrow::field("c", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("c", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
    ASSERT_TRUE(bm);
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(1)));
        ASSERT_EQ("{0,3}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(2)));
        ASSERT_EQ("{1,4}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(3)));
        ASSERT_EQ("{2,5}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
        ASSERT_EQ("{6}", result->ToString());
    }
}

TEST_F(FileIndexFormatTest, TestWriterAddBatchToUnregisteredColumn) {
    // AddBatch to a column with no registered sub-writer should be a no-op (Status::OK).
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "col1", arrow::int32(), pool_);

    // Build a valid batch for an unregistered column
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({5, 6, 7}, {true, true, true}).ok());
    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder.Finish(&arr).ok());
    auto struct_arr = arrow::StructArray::Make({arr}, {"unregistered"}).ValueOrDie();
    ArrowArray c_arr;
    ASSERT_TRUE(arrow::ExportArray(*struct_arr, &c_arr).ok());

    // Should succeed and not touch the registered col1 data
    ASSERT_OK(writer->AddBatch("unregistered", &c_arr));

    // Feed col1 with valid data
    AddInt32Batch(writer.get(), "col1", {1, 2, 3}, {true, true, true});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema(
        {arrow::field("col1", arrow::int32()), arrow::field("unregistered", arrow::int32())});
    // col1 should be present
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("col1", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, readers.size());
    }
    // unregistered should be absent (empty vector)
    {
        ASSERT_OK_AND_ASSIGN(
            auto readers, reader->ReadColumnIndex("unregistered", CreateArrowSchema(schema).get()));
        ASSERT_TRUE(readers.empty());
    }
}

TEST_F(FileIndexFormatTest, TestWriterAddBatchToUnregisteredColumnReleasesBatch) {
    // Empty writer: no AddIndexWriter called. AddBatch for any column should release the batch.
    auto writer = FileIndexFormat::CreateWriter();

    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({5, 6, 7}, {true, true, true}).ok());
    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder.Finish(&arr).ok());
    auto struct_arr = arrow::StructArray::Make({arr}, {"any_column"}).ValueOrDie();
    ArrowArray c_arr;
    ASSERT_TRUE(arrow::ExportArray(*struct_arr, &c_arr).ok());

    ASSERT_OK(writer->AddBatch("any_column", &c_arr));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_arr));
}

TEST_F(FileIndexFormatTest, TestWriterInt64Column) {
    // int64 (BIGINT) column bitmap round-trip.
    auto writer = FileIndexFormat::CreateWriter();

    auto col_field = arrow::field("bigcol", arrow::int64());
    {
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({col_field}), &c_schema).ok());
        BitmapFileIndex bitmap_indexer({});
        ASSERT_OK_AND_ASSIGN(auto sub_writer, bitmap_indexer.CreateWriter(&c_schema, pool_));
        writer->AddIndexWriter("bigcol", "bitmap", std::move(sub_writer),
                               arrow::struct_({col_field}));
    }

    // Feed data: [100, 200, null, 100, 300] (5 rows)
    {
        arrow::Int64Builder builder;
        ASSERT_TRUE(
            builder.AppendValues({100LL, 200LL, 0LL, 100LL, 300LL}, {true, true, false, true, true})
                .ok());
        std::shared_ptr<arrow::Array> arr;
        ASSERT_TRUE(builder.Finish(&arr).ok());
        auto struct_arr = arrow::StructArray::Make({arr}, {"bigcol"}).ValueOrDie();
        ArrowArray c_arr;
        ASSERT_TRUE(arrow::ExportArray(*struct_arr, &c_arr).ok());
        ASSERT_OK(writer->AddBatch("bigcol", &c_arr));
    }

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema({col_field});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("bigcol", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
    ASSERT_TRUE(bm);
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(static_cast<int64_t>(100))));
        ASSERT_EQ("{0,3}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(static_cast<int64_t>(200))));
        ASSERT_EQ("{1}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(static_cast<int64_t>(300))));
        ASSERT_EQ("{4}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
        ASSERT_EQ("{2}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNotNull());
        ASSERT_EQ("{0,1,3,4}", result->ToString());
    }
}

TEST_F(FileIndexFormatTest, TestWriterAllNullColumn) {
    // All-null column: null_bitmap covers all rows, id_to_bitmap is empty.
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "nullcol", arrow::int32(), pool_);
    // Feed [null, null, null] (3 rows)
    AddInt32Batch(writer.get(), "nullcol", {0, 0, 0}, {false, false, false});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema({arrow::field("nullcol", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("nullcol", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
    ASSERT_TRUE(bm);
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
        ASSERT_EQ("{0,1,2}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNotNull());
        ASSERT_EQ("{}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(1)));
        ASSERT_EQ("{}", result->ToString());
    }
}

TEST_F(FileIndexFormatTest, TestWriterNotEqualInNotIn) {
    // Verify VisitNotEqual, VisitIn, VisitNotIn, VisitIsNotNull predicates.
    // Data: [10, 20, 30, 10, 20, null] (6 rows)
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "v", arrow::int32(), pool_);
    AddInt32Batch(writer.get(), "v", {10, 20, 30, 10, 20, 0},
                  {true, true, true, true, true, false});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema = arrow::schema({arrow::field("v", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("v", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
    ASSERT_TRUE(bm);
    {
        // NotEqual(10): rows with 20, 30, 20 → {1,2,4}; null row excluded
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitNotEqual(Literal(10)));
        ASSERT_EQ("{1,2,4}", result->ToString());
    }
    {
        // In([10, 30]): rows with 10 or 30 → {0,2,3}
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIn({Literal(10), Literal(30)}));
        ASSERT_EQ("{0,2,3}", result->ToString());
    }
    {
        // NotIn([10, 30]): rows not containing 10 or 30; null excluded → {1,4}
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitNotIn({Literal(10), Literal(30)}));
        ASSERT_EQ("{1,4}", result->ToString());
    }
    {
        // IsNotNull: all non-null rows → {0,1,2,3,4}
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNotNull());
        ASSERT_EQ("{0,1,2,3,4}", result->ToString());
    }
}

TEST_F(FileIndexFormatTest, TestWriterMultipleIndexTypesPerColumn) {
    class CountingZeroBodyWriter : public FileIndexWriter {
     public:
        explicit CountingZeroBodyWriter(MemoryPool* pool) : pool_(pool) {}

        Status AddBatch(::ArrowArray* /*batch*/) override {
            ++add_batch_calls_;
            return Status::OK();
        }

        Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override {
            return Bytes::AllocateBytes(0, pool_);
        }

        int add_batch_calls() const {
            return add_batch_calls_;
        }

     private:
        MemoryPool* pool_;
        int add_batch_calls_ = 0;
    };

    // Same column "c" registered with two index types:
    //   - bitmap: regular bitmap reader
    //   - empty-index-test: zero-body writer (EMPTY_INDEX_FLAG -> EmptyFileIndexReader)
    // This verifies that multi-writer AddBatch fan-out works and both entries are readable.
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "c", arrow::int32(), pool_, "bitmap");
    auto c_field = arrow::field("c", arrow::int32());
    auto counting_empty_writer = std::make_shared<CountingZeroBodyWriter>(pool_.get());
    writer->AddIndexWriter("c", "empty-index-test", counting_empty_writer,
                           arrow::struct_({c_field}));

    // Also register a second column to test cross-column correctness.
    RegisterBitmapWriter(writer.get(), "d", arrow::int32(), pool_);

    // Feed data to column "c": [1, 2, 1, null] (4 rows)
    AddInt32Batch(writer.get(), "c", {1, 2, 1, 0}, {true, true, true, false});
    // Feed data to column "d": [10, 20] (2 rows)
    AddInt32Batch(writer.get(), "d", {10, 20}, {true, true});
    ASSERT_EQ(1, counting_empty_writer->add_batch_calls());

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema =
        arrow::schema({arrow::field("c", arrow::int32()), arrow::field("d", arrow::int32())});

    // Column "c" should have 2 readers:
    //   - bitmap reader with expected filtering semantics
    //   - empty reader from EMPTY_INDEX_FLAG entry
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("c", CreateArrowSchema(schema).get()));
        ASSERT_EQ(2, readers.size());

        int bitmap_readers = 0;
        int empty_readers = 0;
        for (const auto& r : readers) {
            if (auto* bm = dynamic_cast<BitmapFileIndexReader*>(r.get())) {
                ++bitmap_readers;
                {
                    ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(1)));
                    ASSERT_EQ("{0,2}", result->ToString());
                }
                {
                    ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(2)));
                    ASSERT_EQ("{1}", result->ToString());
                }
                {
                    ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
                    ASSERT_EQ("{3}", result->ToString());
                }
                continue;
            }
            if (dynamic_cast<EmptyFileIndexReader*>(r.get())) {
                ++empty_readers;
            }
        }
        ASSERT_EQ(1, bitmap_readers);
        ASSERT_EQ(1, empty_readers);
    }

    // Column "d" should have 1 reader.
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("d", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, readers.size());
        auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
        ASSERT_TRUE(bm);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(10)));
            ASSERT_EQ("{0}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(20)));
            ASSERT_EQ("{1}", result->ToString());
        }
    }
}

// ZeroBodyWriter: returns 0 bytes from SerializedBytes() to trigger EMPTY_INDEX_FLAG.
class ZeroBodyWriter : public FileIndexWriter {
 public:
    explicit ZeroBodyWriter(MemoryPool* pool) : pool_(pool) {}
    Status AddBatch(::ArrowArray* /*batch*/) override {
        return Status::OK();
    }
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override {
        return Bytes::AllocateBytes(0, pool_);
    }

 private:
    MemoryPool* pool_;
};

// FailingAddBatchWriter: AddBatch returns error. Must release batch when rejecting.
class FailingAddBatchWriter : public FileIndexWriter {
 public:
    explicit FailingAddBatchWriter(MemoryPool* pool) : pool_(pool) {}
    Status AddBatch(::ArrowArray* batch) override {
        ArrowArrayRelease(batch);
        return Status::Invalid("injected failure for memory test");
    }
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override {
        return Bytes::AllocateBytes(0, pool_);
    }

 private:
    MemoryPool* pool_;
};

TEST_F(FileIndexFormatTest, TestWriterAddBatchMultiWriterFailureReleasesResources) {
    // Column "c" has two sub-writers: first succeeds, second fails.
    // Verifies tmp_array and original batch are properly released on failure path.
    auto writer = FileIndexFormat::CreateWriter();
    auto c_field = arrow::field("c", arrow::int32());

    writer->AddIndexWriter("c", "ok", std::make_shared<ZeroBodyWriter>(pool_.get()),
                           arrow::struct_({c_field}));
    writer->AddIndexWriter("c", "fail", std::make_shared<FailingAddBatchWriter>(pool_.get()),
                           arrow::struct_({c_field}));

    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2, 3}, {true, true, true}).ok());
    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder.Finish(&arr).ok());
    auto struct_arr = arrow::StructArray::Make({arr}, {"c"}).ValueOrDie();
    ArrowArray c_arr;
    ASSERT_TRUE(arrow::ExportArray(*struct_arr, &c_arr).ok());

    Status st = writer->AddBatch("c", &c_arr);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.ToString().find("injected failure") != std::string::npos);
    ASSERT_TRUE(ArrowArrayIsReleased(&c_arr));
}

TEST_F(FileIndexFormatTest, TestWriterEmptyIndexEntry) {
    // "col1" uses ZeroBodyWriter (triggers EMPTY_INDEX_FLAG), "col2" has real bitmap data.
    // Verifies that EMPTY_INDEX_FLAG is written and read back as EmptyFileIndexReader.
    auto writer = FileIndexFormat::CreateWriter();

    // col1: zero-body writer, type "zero" (4 chars)
    writer->AddIndexWriter("col1", "zero", std::make_shared<ZeroBodyWriter>(pool_.get()), nullptr);

    // col2: normal bitmap writer
    RegisterBitmapWriter(writer.get(), "col2", arrow::int32(), pool_);
    AddInt32Batch(writer.get(), "col2", {5, 10}, {true, true});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));

    // Byte-level checks.
    // head_content = 4 + (2+4)+4+(2+4)+4+4 + (2+4)+4+(2+6)+4+4 + 4
    //             = 4 + 24 + 26 + 4 = 58
    // head_length = 8+4+4+58 = 74
    auto* data = reinterpret_cast<const uint8_t*>(bytes->data());
    auto read_be_int32 = [&](int32_t off) -> int32_t {
        return (static_cast<int32_t>(data[off]) << 24) |
               (static_cast<int32_t>(data[off + 1]) << 16) |
               (static_cast<int32_t>(data[off + 2]) << 8) | static_cast<int32_t>(data[off + 3]);
    };

    // col1 start_pos at [36..39] = -1 (EMPTY_INDEX_FLAG), length at [40..43] = 0
    ASSERT_EQ(FileIndexFormat::EMPTY_INDEX_FLAG, read_be_int32(36));
    ASSERT_EQ(0, read_be_int32(40));
    // col2 start_pos at [62..65] = 74 (= head_length), length at [66..69] > 0
    ASSERT_EQ(74, read_be_int32(62));
    ASSERT_GT(read_be_int32(66), 0);

    // Reader checks.
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));

    auto schema =
        arrow::schema({arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::int32())});

    // col1 → EmptyFileIndexReader
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("col1", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, readers.size());
        auto* empty = dynamic_cast<EmptyFileIndexReader*>(readers[0].get());
        ASSERT_TRUE(empty);
    }
    // col2 → BitmapFileIndexReader with valid data
    {
        ASSERT_OK_AND_ASSIGN(auto readers,
                             reader->ReadColumnIndex("col2", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, readers.size());
        auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
        ASSERT_TRUE(bm);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(5)));
            ASSERT_EQ("{0}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(10)));
            ASSERT_EQ("{1}", result->ToString());
        }
    }
}

TEST_F(FileIndexFormatTest, CppWriterJavaReaderCompatibility) {
    // Verify byte-level compatibility with Java DataOutputStream format:
    //   writeLong(MAGIC) → big-endian int64
    //   writeInt(V_1) → big-endian int32
    //   writeUTF(str) → int16 length + UTF-8 bytes (identical to C++ WriteString)
    //   start_pos = absolute offset from file start
    //
    // Config: column "col1" (4 chars), index "bitmap" (6 chars), data [1,1,2,2,null] (5 rows)
    // head_length = 8+4+4 + (4 + (2+4) + 4 + (2+6) + 4+4 + 4) = 16 + 34 = 50
    auto writer = FileIndexFormat::CreateWriter();
    RegisterBitmapWriter(writer.get(), "col1", arrow::int32(), pool_);
    AddInt32Batch(writer.get(), "col1", {1, 1, 2, 2, 0}, {true, true, true, true, false});

    ASSERT_OK_AND_ASSIGN(auto bytes, writer->Serialize(pool_));
    ASSERT_GE(bytes->size(), 50u);

    auto* data = reinterpret_cast<const uint8_t*>(bytes->data());
    auto read_be_int64 = [&](int32_t off) -> int64_t {
        int64_t v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | data[off + i];
        return v;
    };
    auto read_be_int32 = [&](int32_t off) -> int32_t {
        return (static_cast<int32_t>(data[off]) << 24) |
               (static_cast<int32_t>(data[off + 1]) << 16) |
               (static_cast<int32_t>(data[off + 2]) << 8) | static_cast<int32_t>(data[off + 3]);
    };
    auto read_be_int16 = [&](int32_t off) -> int16_t {
        return static_cast<int16_t>((static_cast<int16_t>(data[off]) << 8) | data[off + 1]);
    };

    // Java MAGIC = 0x00054E4ED01A35AE = 1493475289347502L
    ASSERT_EQ(0x00, data[0]);
    ASSERT_EQ(0x05, data[1]);
    ASSERT_EQ(0x4E, data[2]);
    ASSERT_EQ(0x4E, data[3]);
    ASSERT_EQ(0xD0, data[4]);
    ASSERT_EQ(0x1A, data[5]);
    ASSERT_EQ(0x35, data[6]);
    ASSERT_EQ(0xAE, data[7]);
    ASSERT_EQ(FileIndexFormat::MAGIC, read_be_int64(0));

    // Java Version.V_1.version() = 1
    ASSERT_EQ(1, read_be_int32(8));

    // head_length = 50
    ASSERT_EQ(50, read_be_int32(12));

    // column_count = 1
    ASSERT_EQ(1, read_be_int32(16));

    // Java writeUTF("col1") → [0x00, 0x04, 'c', 'o', 'l', '1']
    ASSERT_EQ(4, read_be_int16(20));
    ASSERT_EQ('c', static_cast<char>(data[22]));
    ASSERT_EQ('o', static_cast<char>(data[23]));
    ASSERT_EQ('l', static_cast<char>(data[24]));
    ASSERT_EQ('1', static_cast<char>(data[25]));

    // index_count = 1
    ASSERT_EQ(1, read_be_int32(26));

    // Java writeUTF("bitmap") → [0x00, 0x06, 'b', 'i', 't', 'm', 'a', 'p']
    ASSERT_EQ(6, read_be_int16(30));
    ASSERT_EQ('b', static_cast<char>(data[32]));
    ASSERT_EQ('i', static_cast<char>(data[33]));
    ASSERT_EQ('t', static_cast<char>(data[34]));
    ASSERT_EQ('m', static_cast<char>(data[35]));
    ASSERT_EQ('a', static_cast<char>(data[36]));
    ASSERT_EQ('p', static_cast<char>(data[37]));

    // start_pos = headLength (Java: start + headLength, where start=0 for first body)
    ASSERT_EQ(50, read_be_int32(38));

    // body length > 0
    int32_t body_len = read_be_int32(42);
    ASSERT_GT(body_len, 0);

    // redundant_length = 0 (Java: REDUNDANT_LENGTH = 0)
    ASSERT_EQ(0, read_be_int32(46));

    // total file size = head_length + body
    ASSERT_EQ(static_cast<size_t>(50 + body_len), bytes->size());

    // Semantic check: Reader can parse and return correct query results.
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    auto schema = arrow::schema({arrow::field("col1", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("col1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    auto* bm = dynamic_cast<BitmapFileIndexReader*>(readers[0].get());
    ASSERT_TRUE(bm);
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(1)));
        ASSERT_EQ("{0,1}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitEqual(Literal(2)));
        ASSERT_EQ("{2,3}", result->ToString());
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, bm->VisitIsNull());
        ASSERT_EQ("{4}", result->ToString());
    }
}

}  // namespace paimon::test
