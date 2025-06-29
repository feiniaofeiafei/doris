// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/es/es_scroll_parser.h"

#include <absl/strings/substitute.h>
#include <cctz/time_zone.h>
#include <glog/logging.h>
#include <rapidjson/allocators.h>
#include <rapidjson/encodings.h>
#include <stdint.h>
#include <string.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstdlib>
#include <ostream>
#include <string>

#include "common/status.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_ID = "_id";

// get the original json data type
std::string json_type_to_string(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNumberType:
        return "Number";
    case rapidjson::kStringType:
        return "Varchar/Char";
    case rapidjson::kArrayType:
        return "Array";
    case rapidjson::kObjectType:
        return "Object";
    case rapidjson::kNullType:
        return "Null Type";
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
        return "True/False";
    default:
        return "Unknown Type";
    }
}

// transfer rapidjson::Value to string representation
std::string json_value_to_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> temp_writer(scratch_buffer);
    value.Accept(temp_writer);
    return scratch_buffer.GetString();
}

static const std::string ERROR_INVALID_COL_DATA =
        "Data source returned inconsistent column data. "
        "Expected value of type {} based on column metadata. This likely indicates a "
        "problem with the data source library.";
static const std::string ERROR_MEM_LIMIT_EXCEEDED =
        "DataSourceScanNode::$0() failed to allocate "
        "$1 bytes for $2.";
static const std::string ERROR_COL_DATA_IS_ARRAY =
        "Data source returned an array for the type $0"
        "based on column metadata.";
static const std::string INVALID_NULL_VALUE =
        "Invalid null value occurs: Non-null column `$0` contains NULL";

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type, is_array)                    \
    do {                                                                     \
        if (col.IsArray() == is_array) {                                     \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document slice is : " << json_value_to_string(col);     \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col, type)                         \
    do {                                                                     \
        if (!col.IsNumber()) {                                               \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document value is: " << json_value_to_string(col);      \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type)                       \
    do {                                                                        \
        if (result != StringParser::PARSE_SUCCESS) {                            \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                     \
    do {                                                                 \
        std::stringstream ss;                                            \
        ss << "Expected value of type: " << type_to_string(type)         \
           << "; but found type: " << json_type_to_string(col.GetType()) \
           << "; Document slice is : " << json_value_to_string(col);     \
        return Status::RuntimeError(ss.str());                           \
    } while (false)

template <typename T>
Status get_int_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                     bool pure_doc_value) {
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && !col.Empty()) {
        RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col[0], type);
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    if (sizeof(T) < 16) {
        *reinterpret_cast<T*>(slot) = v;
    } else {
        DCHECK(sizeof(T) == 16);
        memcpy(slot, &v, sizeof(v));
    }

    return Status::OK();
}

template <PrimitiveType T>
Status get_date_value_int(const rapidjson::Value& col, PrimitiveType type, bool is_date_str,
                          typename PrimitiveTypeTraits<T>::ColumnItemType* slot,
                          const cctz::time_zone& time_zone) {
    constexpr bool is_datetime_v1 = T == TYPE_DATE || T == TYPE_DATETIME;
    typename PrimitiveTypeTraits<T>::CppType dt_val;
    if (is_date_str) {
        const std::string str_date = col.GetString();
        int str_length = col.GetStringLength();
        bool success = false;
        if (str_length > 19) {
            std::chrono::system_clock::time_point tp;
            // time_zone suffix pattern
            // Z/+08:00/-04:30
            RE2 time_zone_pattern(R"([+-]\d{2}:\d{2}|Z)");
            bool ok = false;
            std::string fmt;
            re2::StringPiece value;
            if (time_zone_pattern.Match(str_date, 0, str_date.size(), RE2::UNANCHORED, &value, 1)) {
                // with time_zone info
                // YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DDTHH:MM:SS+08:00
                // or 2022-08-08T12:10:10.000Z or YYYY-MM-DDTHH:MM:SS-08:00
                fmt = "%Y-%m-%dT%H:%M:%E*S%Ez";
                cctz::time_zone ctz;
                // find time_zone by time_zone suffix string
                TimezoneUtils::find_cctz_time_zone(value.as_string(), ctz);
                ok = cctz::parse(fmt, str_date, ctz, &tp);
            } else {
                // without time_zone info
                // 2022-08-08T12:10:10.000
                fmt = "%Y-%m-%dT%H:%M:%E*S";
                // If the time without time_zone info, ES will assume it is UTC time.
                // So we parse it in Doris with UTC time zone.
                ok = cctz::parse(fmt, str_date, cctz::utc_time_zone(), &tp);
            }
            if (ok) {
                // The local time zone can change by session variable `time_zone`
                // We should use the user specified time zone, not the actual system local time zone.
                success = true;
                dt_val.from_unixtime(std::chrono::system_clock::to_time_t(tp), time_zone);
            }
        } else if (str_length == 19) {
            // YYYY-MM-DDTHH:MM:SS
            if (*(str_date.c_str() + 10) == 'T') {
                std::chrono::system_clock::time_point tp;
                const bool ok =
                        cctz::parse("%Y-%m-%dT%H:%M:%S", str_date, cctz::utc_time_zone(), &tp);
                if (ok) {
                    success = true;
                    dt_val.from_unixtime(std::chrono::system_clock::to_time_t(tp), time_zone);
                }
            } else {
                // YYYY-MM-DD HH:MM:SS
                success = dt_val.from_date_str(str_date.c_str(), str_length);
            }

        } else if (str_length == 13) {
            // string long like "1677895728000"
            int64_t time_long = std::atol(str_date.c_str());
            if (time_long > 0) {
                success = true;
                dt_val.from_unixtime(time_long / 1000, time_zone);
            }
        } else {
            // YYYY-MM-DD or others
            success = dt_val.from_date_str(str_date.c_str(), str_length);
        }

        if (!success) {
            RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
        }

    } else {
        dt_val.from_unixtime(col.GetInt64() / 1000, time_zone);
    }
    if constexpr (is_datetime_v1) {
        if (type == TYPE_DATE) {
            dt_val.cast_to_date();
        } else {
            dt_val.to_datetime();
        }
    }

    *reinterpret_cast<typename PrimitiveTypeTraits<T>::ColumnItemType*>(slot) =
            binary_cast<typename PrimitiveTypeTraits<T>::CppType,
                        typename PrimitiveTypeTraits<T>::ColumnItemType>(
                    *reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(&dt_val));
    return Status::OK();
}

template <PrimitiveType T>
Status get_date_int(const rapidjson::Value& col, PrimitiveType type, bool pure_doc_value,
                    typename PrimitiveTypeTraits<T>::ColumnItemType* slot,
                    const cctz::time_zone& time_zone) {
    // this would happend just only when `enable_docvalue_scan = false`, and field has timestamp format date from _source
    if (col.IsNumber()) {
        // ES process date/datetime field would use millisecond timestamp for index or docvalue
        // processing date type field, if a number is encountered, Doris On ES will force it to be processed according to ms
        // Doris On ES needs to be consistent with ES, so just divided by 1000 because the unit for from_unixtime is seconds
        return get_date_value_int<T>(col, type, false, slot, time_zone);
    } else if (col.IsArray() && pure_doc_value && !col.Empty()) {
        // this would happened just only when `enable_docvalue_scan = true`
        // ES add default format for all field after ES 6.4, if we not provided format for `date` field ES would impose
        // a standard date-format for date field as `2020-06-16T00:00:00.000Z`
        // At present, we just process this string format date. After some PR were merged into Doris, we would impose `epoch_mills` for
        // date field's docvalue
        if (col[0].IsString()) {
            return get_date_value_int<T>(col[0], type, true, slot, time_zone);
        }
        // ES would return millisecond timestamp for date field, divided by 1000 because the unit for from_unixtime is seconds
        return get_date_value_int<T>(col[0], type, false, slot, time_zone);
    } else {
        // this would happened just only when `enable_docvalue_scan = false`, and field has string format date from _source
        RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
        RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
        return get_date_value_int<T>(col, type, true, slot, time_zone);
    }
}
template <PrimitiveType T>
Status fill_date_int(const rapidjson::Value& col, PrimitiveType type, bool pure_doc_value,
                     vectorized::IColumn* col_ptr, const cctz::time_zone& time_zone) {
    typename PrimitiveTypeTraits<T>::ColumnItemType data;
    RETURN_IF_ERROR((get_date_int<T>(col, type, pure_doc_value, &data, time_zone)));
    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&data)), 0);
    return Status::OK();
}

template <typename T>
Status get_float_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                       bool pure_doc_value) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && !col.Empty()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    *reinterpret_cast<T*>(slot) = v;

    return Status::OK();
}

template <typename T>
Status insert_float_value(const rapidjson::Value& col, PrimitiveType type,
                          vectorized::IColumn* col_ptr, bool pure_doc_value, bool nullable) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && !col.Empty() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&v)), 0);

    return Status::OK();
}

template <typename T>
Status insert_int_value(const rapidjson::Value& col, PrimitiveType type,
                        vectorized::IColumn* col_ptr, bool pure_doc_value, bool nullable) {
    if (col.IsNumber()) {
        T value;
        // ES allows inserting float and double in int/long types.
        // To parse these numbers in Doris, we direct cast them to int types.
        if (col.IsDouble()) {
            value = static_cast<T>(col.GetDouble());
        } else if (col.IsFloat()) {
            value = static_cast<T>(col.GetFloat());
        } else {
            value = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        }
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    auto parse_and_insert_data = [&](const rapidjson::Value& col_value) -> Status {
        StringParser::ParseResult result;
        std::string val = col_value.GetString();
        // ES allows inserting numbers and characters containing decimals in numeric types.
        // To parse these numbers in Doris, we remove the decimals here.
        size_t pos = val.find('.');
        if (pos != std::string::npos) {
            val = val.substr(0, pos);
        }
        size_t len = val.length();
        T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
        RETURN_ERROR_IF_PARSING_FAILED(result, col_value, type);

        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&v)), 0);
        return Status::OK();
    };

    if (pure_doc_value && col.IsArray() && !col.Empty()) {
        if (col[0].IsNumber()) {
            T value = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
            return Status::OK();
        } else {
            RETURN_ERROR_IF_COL_IS_ARRAY(col[0], type, true);
            RETURN_ERROR_IF_COL_IS_NOT_STRING(col[0], type);
            return parse_and_insert_data(col[0]);
        }
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
    return parse_and_insert_data(col);
}

template <PrimitiveType T>
Status handle_value(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                    typename PrimitiveTypeTraits<T>::ColumnItemType& val) {
    if constexpr (T == TYPE_TINYINT || T == TYPE_SMALLINT || T == TYPE_INT || T == TYPE_BIGINT ||
                  T == TYPE_LARGEINT) {
        RETURN_IF_ERROR(get_int_value<typename PrimitiveTypeTraits<T>::ColumnItemType>(
                col, sub_type, &val, pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_FLOAT) {
        RETURN_IF_ERROR(get_float_value<float>(col, sub_type, &val, pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_DOUBLE) {
        RETURN_IF_ERROR(get_float_value<double>(col, sub_type, &val, pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_STRING || T == TYPE_CHAR || T == TYPE_VARCHAR) {
        RETURN_ERROR_IF_COL_IS_ARRAY(col, sub_type, true);
        if (!col.IsString()) {
            val = json_value_to_string(col);
        } else {
            val = col.GetString();
        }
        return Status::OK();
    }
    if constexpr (T == TYPE_BOOLEAN) {
        if (col.IsBool()) {
            val = col.GetBool();
            return Status::OK();
        }

        if (col.IsNumber()) {
            val = col.GetInt();
            return Status::OK();
        }

        bool is_nested_str = false;
        if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsBool()) {
            val = col[0].GetBool();
            return Status::OK();
        } else if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsString()) {
            is_nested_str = true;
        } else if (pure_doc_value && col.IsArray()) {
            return Status::InternalError(ERROR_INVALID_COL_DATA, "BOOLEAN");
        }

        const rapidjson::Value& str_col = is_nested_str ? col[0] : col;
        const std::string& str_val = str_col.GetString();
        size_t val_size = str_col.GetStringLength();
        StringParser::ParseResult result;
        val = StringParser::string_to_bool(str_val.c_str(), val_size, &result);
        RETURN_ERROR_IF_PARSING_FAILED(result, str_col, sub_type);
        return Status::OK();
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Un-supported type: {}", type_to_string(T));
}

template <PrimitiveType T>
Status process_single_column(const rapidjson::Value& col, PrimitiveType sub_type,
                             bool pure_doc_value, vectorized::Array& array) {
    typename PrimitiveTypeTraits<T>::ColumnItemType val;
    RETURN_IF_ERROR(handle_value<T>(col, sub_type, pure_doc_value, val));
    array.push_back(vectorized::Field::create_field<T>(val));
    return Status::OK();
}

template <PrimitiveType T>
Status process_column_array(const rapidjson::Value& col, PrimitiveType sub_type,
                            bool pure_doc_value, vectorized::Array& array) {
    for (const auto& sub_col : col.GetArray()) {
        RETURN_IF_ERROR(process_single_column<T>(sub_col, sub_type, pure_doc_value, array));
    }
    return Status::OK();
}

template <PrimitiveType T>
Status process_column(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                      vectorized::Array& array) {
    if (!col.IsArray()) {
        return process_single_column<T>(col, sub_type, pure_doc_value, array);
    } else {
        return process_column_array<T>(col, sub_type, pure_doc_value, array);
    }
}

template <PrimitiveType T>
Status process_date_column(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                           vectorized::Array& array, const cctz::time_zone& time_zone) {
    if (!col.IsArray()) {
        typename PrimitiveTypeTraits<T>::ColumnItemType data;
        RETURN_IF_ERROR((get_date_int<T>(col, sub_type, pure_doc_value, &data, time_zone)));
        array.push_back(vectorized::Field::create_field<T>(data));
    } else {
        for (const auto& sub_col : col.GetArray()) {
            typename PrimitiveTypeTraits<T>::ColumnItemType data;
            RETURN_IF_ERROR((get_date_int<T>(sub_col, sub_type, pure_doc_value, &data, time_zone)));
            array.push_back(vectorized::Field::create_field<T>(data));
        }
    }
    return Status::OK();
}

Status ScrollParser::parse_column(const rapidjson::Value& col, PrimitiveType sub_type,
                                  bool pure_doc_value, vectorized::Array& array,
                                  const cctz::time_zone& time_zone) {
    switch (sub_type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return process_column<TYPE_STRING>(col, sub_type, pure_doc_value, array);
    case TYPE_TINYINT:
        return process_column<TYPE_TINYINT>(col, sub_type, pure_doc_value, array);
    case TYPE_SMALLINT:
        return process_column<TYPE_SMALLINT>(col, sub_type, pure_doc_value, array);
    case TYPE_INT:
        return process_column<TYPE_INT>(col, sub_type, pure_doc_value, array);
    case TYPE_BIGINT:
        return process_column<TYPE_BIGINT>(col, sub_type, pure_doc_value, array);
    case TYPE_LARGEINT:
        return process_column<TYPE_LARGEINT>(col, sub_type, pure_doc_value, array);
    case TYPE_FLOAT:
        return process_column<TYPE_FLOAT>(col, sub_type, pure_doc_value, array);
    case TYPE_DOUBLE:
        return process_column<TYPE_DOUBLE>(col, sub_type, pure_doc_value, array);
    case TYPE_BOOLEAN:
        return process_column<TYPE_BOOLEAN>(col, sub_type, pure_doc_value, array);
    // date/datetime v2 is the default type for catalog table,
    // see https://github.com/apache/doris/pull/16304
    // No need to support date and datetime types.
    case TYPE_DATEV2: {
        return process_date_column<TYPE_DATEV2>(col, sub_type, pure_doc_value, array, time_zone);
    }
    case TYPE_DATETIMEV2: {
        return process_date_column<TYPE_DATETIMEV2>(col, sub_type, pure_doc_value, array,
                                                    time_zone);
    }
    default:
        LOG(ERROR) << "Do not support Array type: " << sub_type;
        return Status::InternalError("Unsupported type");
    }
}

ScrollParser::ScrollParser(bool doc_value_mode) : _size(0), _line_index(0) {}

ScrollParser::~ScrollParser() = default;

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    // rely on `_size !=0 ` to determine whether scroll ends
    _size = 0;
    _document_node.Parse(scroll_result.c_str(), scroll_result.length());
    if (_document_node.HasParseError()) {
        return Status::InternalError("Parsing json error, json is: {}", scroll_result);
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll response:" << scroll_result;
        return Status::InternalError("Document has not a scroll id field");
    }

    if (!exactly_once) {
        const rapidjson::Value& scroll_node = _document_node[FIELD_SCROLL_ID];
        _scroll_id = scroll_node.GetString();
    }
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value& outer_hits_node = _document_node[FIELD_HITS];
    // if has no inner hits, there has no data in this index
    if (!outer_hits_node.HasMember(FIELD_INNER_HITS)) {
        return Status::OK();
    }
    const rapidjson::Value& inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
    }
    _inner_hits_node.CopyFrom(inner_hits_node, _document_node.GetAllocator());
    // how many documents contains in this batch
    _size = _inner_hits_node.Size();
    return Status::OK();
}

int ScrollParser::get_size() const {
    return _size;
}

const std::string& ScrollParser::get_scroll_id() {
    return _scroll_id;
}

Status ScrollParser::fill_columns(const TupleDescriptor* tuple_desc,
                                  std::vector<vectorized::MutableColumnPtr>& columns,
                                  bool* line_eof,
                                  const std::map<std::string, std::string>& docvalue_context,
                                  const cctz::time_zone& time_zone) {
    *line_eof = true;

    if (_size <= 0 || _line_index >= _size) {
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    bool pure_doc_value = false;
    if (obj.HasMember("fields")) {
        pure_doc_value = true;
    }
    // obj may be neither have `_source` nor `fields` field.
    const rapidjson::Value* line = nullptr;
    if (obj.HasMember(FIELD_SOURCE)) {
        line = &obj[FIELD_SOURCE];
    } else if (obj.HasMember("fields")) {
        line = &obj["fields"];
    }

    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];
        auto col_ptr = columns[i].get();

        if (!slot_desc->is_materialized()) {
            continue;
        }
        if (slot_desc->col_name() == FIELD_ID) {
            // actually this branch will not be reached, this is guaranteed by Doris FE.
            if (pure_doc_value) {
                return Status::RuntimeError("obtain `_id` is not supported in doc_values mode");
            }
            // obj[FIELD_ID] must not be NULL
            std::string _id = obj[FIELD_ID].GetString();
            size_t len = _id.length();

            col_ptr->insert_data(const_cast<const char*>(_id.data()), len);
            continue;
        }

        const char* col_name = pure_doc_value ? docvalue_context.at(slot_desc->col_name()).c_str()
                                              : slot_desc->col_name().c_str();

        if (line == nullptr || line->FindMember(col_name) == line->MemberEnd()) {
            if (slot_desc->is_nullable()) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);
                nullable_column->insert_data(nullptr, 0);
                continue;
            } else {
                std::string details = absl::Substitute(INVALID_NULL_VALUE, col_name);
                return Status::RuntimeError(details);
            }
        }

        const rapidjson::Value& col = (*line)[col_name];

        auto type = slot_desc->type()->get_primitive_type();

        // when the column value is null, the subsequent type casting will report an error
        if (col.IsNull() && slot_desc->is_nullable()) {
            col_ptr->insert_data(nullptr, 0);
            continue;
        } else if (col.IsNull() && !slot_desc->is_nullable()) {
            std::string details = absl::Substitute(INVALID_NULL_VALUE, col_name);
            return Status::RuntimeError(details);
        }
        switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            // sometimes elasticsearch user post some not-string value to Elasticsearch Index.
            // because of reading value from _source, we can not process all json type and then just transfer the value to original string representation
            // this may be a tricky, but we can work around this issue
            std::string val;
            if (pure_doc_value) {
                if (col.Empty()) {
                    break;
                } else if (!col[0].IsString()) {
                    val = json_value_to_string(col[0]);
                } else {
                    val = col[0].GetString();
                }
            } else {
                RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
                if (!col.IsString()) {
                    val = json_value_to_string(col);
                } else {
                    val = col.GetString();
                }
            }
            size_t val_size = val.length();
            col_ptr->insert_data(const_cast<const char*>(val.data()), val_size);
            break;
        }

        case TYPE_TINYINT: {
            RETURN_IF_ERROR(insert_int_value<int8_t>(col, type, col_ptr, pure_doc_value,
                                                     slot_desc->is_nullable()));
            break;
        }

        case TYPE_SMALLINT: {
            RETURN_IF_ERROR(insert_int_value<int16_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_INT: {
            RETURN_IF_ERROR(insert_int_value<int32_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_BIGINT: {
            RETURN_IF_ERROR(insert_int_value<int64_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_LARGEINT: {
            RETURN_IF_ERROR(insert_int_value<__int128>(col, type, col_ptr, pure_doc_value,
                                                       slot_desc->is_nullable()));
            break;
        }

        case TYPE_DOUBLE: {
            RETURN_IF_ERROR(insert_float_value<double>(col, type, col_ptr, pure_doc_value,
                                                       slot_desc->is_nullable()));
            break;
        }

        case TYPE_FLOAT: {
            RETURN_IF_ERROR(insert_float_value<float>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_BOOLEAN: {
            if (col.IsBool()) {
                int8_t val = col.GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            if (col.IsNumber()) {
                int8_t val = col.GetInt();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            bool is_nested_str = false;
            if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsBool()) {
                int8_t val = col[0].GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            } else if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsString()) {
                is_nested_str = true;
            } else if (pure_doc_value && col.IsArray()) {
                return Status::InternalError(ERROR_INVALID_COL_DATA, "BOOLEAN");
            }

            const rapidjson::Value& str_col = is_nested_str ? col[0] : col;

            RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);

            const std::string& val = str_col.GetString();
            size_t val_size = str_col.GetStringLength();
            StringParser::ParseResult result;
            bool b = StringParser::string_to_bool(val.c_str(), val_size, &result);
            RETURN_ERROR_IF_PARSING_FAILED(result, str_col, type);
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&b)), 0);
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value data;

            if (col.IsDouble()) {
                data.assign_from_double(col.GetDouble());
            } else {
                std::string val;
                if (pure_doc_value) {
                    if (col.Empty()) {
                        break;
                    } else if (!col[0].IsString()) {
                        val = json_value_to_string(col[0]);
                    } else {
                        val = col[0].GetString();
                    }
                } else {
                    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
                    if (!col.IsString()) {
                        val = json_value_to_string(col);
                    } else {
                        val = col.GetString();
                    }
                }
                data.parse_from_str(val.data(), val.length());
            }
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&data)), 0);
            break;
        }

        case TYPE_DATE:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATE>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATETIME:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATETIME>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATEV2:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATEV2>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATETIMEV2: {
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATETIMEV2>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        }
        case TYPE_ARRAY: {
            vectorized::Array array;
            const auto& sub_type =
                    assert_cast<const vectorized::DataTypeArray*>(
                            vectorized::remove_nullable(tuple_desc->slots()[i]->type()).get())
                            ->get_nested_type()
                            ->get_primitive_type();
            RETURN_IF_ERROR(parse_column(col, sub_type, pure_doc_value, array, time_zone));
            col_ptr->insert(vectorized::Field::create_field<TYPE_ARRAY>(array));
            break;
        }
        case TYPE_JSONB: {
            JsonBinaryValue jsonb_value;
            RETURN_IF_ERROR(jsonb_value.from_json_string(json_value_to_string(col)));
            vectorized::JsonbField json(jsonb_value.value(), jsonb_value.size());
            col_ptr->insert(vectorized::Field::create_field<TYPE_JSONB>(json));
            break;
        }
        default: {
            LOG(ERROR) << "Unsupported data type: " << type_to_string(type);
            DCHECK(false);
            break;
        }
        }
    }

    *line_eof = false;
    return Status::OK();
}

} // namespace doris
