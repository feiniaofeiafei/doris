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

#pragma once

#include <fmt/format.h>
#include <gen_cpp/Status_types.h>

#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/status.h"
#include "util/defer_op.h"

namespace doris {

inline thread_local int enable_thread_catch_bad_alloc = 0;
class Exception : public std::exception {
public:
    Exception() : _code(ErrorCode::OK) {}
    Exception(int code, const std::string_view& msg);
    Exception(const Status& status) : Exception(status.code(), status.msg()) {}

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    Exception(int code, const std::string_view& fmt, Args&&... args)
            : Exception(code, fmt::format(fmt, std::forward<Args>(args)...)) {}

    int code() const { return _code; }
    std::string message() const { return _err_msg ? _err_msg->_msg : ""; }

    const std::string& to_string() const;

    const char* what() const noexcept override { return to_string().c_str(); }

    Status to_status() const { return {code(), _err_msg->_msg, _err_msg->_stack}; }

private:
    int _code;
    struct ErrMsg {
        std::string _msg;
        std::string _stack;
    };
    std::unique_ptr<ErrMsg> _err_msg;
    mutable std::string _cache_string;
};

inline const std::string& Exception::to_string() const {
    if (!_cache_string.empty()) {
        return _cache_string;
    }
    fmt::memory_buffer buf;
    fmt::format_to(buf, "[E{}] {}", _code, _err_msg ? _err_msg->_msg : "");
    if (_err_msg && !_err_msg->_stack.empty()) {
        fmt::format_to(buf, "\n{}", _err_msg->_stack);
    }
    _cache_string = fmt::to_string(buf);
    return _cache_string;
}

} // namespace doris

#define RETURN_IF_CATCH_EXCEPTION(stmt)                                                          \
    do {                                                                                         \
        try {                                                                                    \
            doris::enable_thread_catch_bad_alloc++;                                              \
            Defer defer {[&]() { doris::enable_thread_catch_bad_alloc--; }};                     \
            { stmt; }                                                                            \
        } catch (const doris::Exception& e) {                                                    \
            if (e.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {                                \
                return Status::MemoryLimitExceeded(fmt::format(                                  \
                        "PreCatch error code:{}, {}, __FILE__:{}, __LINE__:{}, __FUNCTION__:{}", \
                        e.code(), e.to_string(), __FILE__, __LINE__, __PRETTY_FUNCTION__));      \
            }                                                                                    \
            return Status::Error<false>(e.code(), e.to_string());                                \
        }                                                                                        \
    } while (0)

#define RETURN_IF_ERROR_OR_CATCH_EXCEPTION(stmt)                                                 \
    do {                                                                                         \
        try {                                                                                    \
            doris::enable_thread_catch_bad_alloc++;                                              \
            Defer defer {[&]() { doris::enable_thread_catch_bad_alloc--; }};                     \
            {                                                                                    \
                Status _status_ = (stmt);                                                        \
                if (UNLIKELY(!_status_.ok())) {                                                  \
                    return _status_;                                                             \
                }                                                                                \
            }                                                                                    \
        } catch (const doris::Exception& e) {                                                    \
            if (e.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {                                \
                return Status::MemoryLimitExceeded(fmt::format(                                  \
                        "PreCatch error code:{}, {}, __FILE__:{}, __LINE__:{}, __FUNCTION__:{}", \
                        e.code(), e.to_string(), __FILE__, __LINE__, __PRETTY_FUNCTION__));      \
            }                                                                                    \
            return Status::Error<false>(e.code(), e.to_string());                                \
        }                                                                                        \
    } while (0)

#define ASSIGN_STATUS_IF_CATCH_EXCEPTION(stmt, status_)                                          \
    do {                                                                                         \
        try {                                                                                    \
            doris::enable_thread_catch_bad_alloc++;                                              \
            Defer defer {[&]() { doris::enable_thread_catch_bad_alloc--; }};                     \
            { stmt; }                                                                            \
        } catch (const doris::Exception& e) {                                                    \
            if (e.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {                                \
                status_ = Status::MemoryLimitExceeded(fmt::format(                               \
                        "PreCatch error code:{}, {}, __FILE__:{}, __LINE__:{}, __FUNCTION__:{}", \
                        e.code(), e.to_string(), __FILE__, __LINE__, __PRETTY_FUNCTION__));      \
            } else {                                                                             \
                status_ = e.to_status();                                                         \
            }                                                                                    \
        }                                                                                        \
    } while (0);

#define HANDLE_EXCEPTION_IF_CATCH_EXCEPTION(stmt, exception_handler)                             \
    do {                                                                                         \
        try {                                                                                    \
            doris::enable_thread_catch_bad_alloc++;                                              \
            Defer defer {[&]() { doris::enable_thread_catch_bad_alloc--; }};                     \
            {                                                                                    \
                Status _status_ = (stmt);                                                        \
                if (UNLIKELY(!_status_.ok())) {                                                  \
                    exception_handler(doris::Exception());                                       \
                    return _status_;                                                             \
                }                                                                                \
            }                                                                                    \
        } catch (const doris::Exception& e) {                                                    \
            exception_handler(e);                                                                \
            if (e.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {                                \
                return Status::MemoryLimitExceeded(fmt::format(                                  \
                        "PreCatch error code:{}, {}, __FILE__:{}, __LINE__:{}, __FUNCTION__:{}", \
                        e.code(), e.to_string(), __FILE__, __LINE__, __PRETTY_FUNCTION__));      \
            }                                                                                    \
            return Status::Error<false>(e.code(), e.to_string());                                \
        }                                                                                        \
    } while (0);
