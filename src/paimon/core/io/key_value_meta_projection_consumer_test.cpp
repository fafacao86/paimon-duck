/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/core/io/key_value_meta_projection_consumer.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

KeyValue CreateKeyValue(int64_t sequence_number, std::string_view str_value,
                        std::string_view binary_value, const std::shared_ptr<MemoryPool>& pool) {
    auto key = std::make_shared<GenericRow>(0);
    auto value = std::make_unique<GenericRow>(2);
    auto binary_bytes = std::make_shared<Bytes>(std::string(binary_value), pool.get());
    value->SetField(0, str_value);
    value->SetField(1, binary_bytes);
    value->AddDataHolder(binary_bytes);
    return KeyValue(RowKind::Insert(), sequence_number, /*level=*/0, key, std::move(value));
}

}  // namespace

TEST(KeyValueMetaProjectionConsumerTest, TestStringAndBinaryViewSchema) {
    auto pool = GetDefaultPool();
    auto target_schema = arrow::schema(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()),
         DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()),
         arrow::field("f0", arrow::utf8_view()), arrow::field("f1", arrow::binary_view())});

    ASSERT_OK_AND_ASSIGN(auto consumer, KeyValueMetaProjectionConsumer::Create(target_schema, pool));

    std::vector<KeyValue> rows;
    rows.emplace_back(CreateKeyValue(/*sequence_number=*/7, "alpha", "aa", pool));
    rows.emplace_back(CreateKeyValue(/*sequence_number=*/9, "longer-string", "payload", pool));

    ASSERT_OK_AND_ASSIGN(auto batch, consumer->NextBatch(rows));

    ArrowSchema c_schema {};
    ASSERT_TRUE(arrow::ExportSchema(*target_schema, &c_schema).ok());
    auto array = arrow::ImportArray(batch.batch.get(), &c_schema).ValueOrDie();
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);

    ASSERT_EQ(struct_array->field(2)->type_id(), arrow::Type::type::STRING_VIEW);
    ASSERT_EQ(struct_array->field(3)->type_id(), arrow::Type::type::BINARY_VIEW);

    auto string_array = std::static_pointer_cast<arrow::StringViewArray>(struct_array->field(2));
    auto binary_array = std::static_pointer_cast<arrow::BinaryViewArray>(struct_array->field(3));
    ASSERT_EQ(string_array->GetView(0), "alpha");
    ASSERT_EQ(string_array->GetView(1), "longer-string");
    ASSERT_EQ(binary_array->GetView(0), "aa");
    ASSERT_EQ(binary_array->GetView(1), "payload");
}

}  // namespace paimon::test
