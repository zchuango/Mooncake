/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Log macros for mooncake async logging.
 */
#pragma once

#include <sstream>
#include <cstring>

#include "trace.h"
#include "rate_limiter.h"
#include <spdlog/spdlog.h>

namespace mooncake {

/**
 * @brief Internal log stream builder - RAII object that logs on destruction.
 */
class LogStream {
public:
    LogStream(spdlog::logger *logger, spdlog::level::level_enum level, const char *file, int line)
        : logger_(logger), level_(level)
    {
        loc_.filename = file;
        loc_.line = static_cast<size_t>(line);
    }

    ~LogStream()
    {
        if (logger_ && !skip_) {
            logger_->log(loc_, level_, stream_.str());
        }
    }

    std::ostream &Stream()
    {
        return stream_;
    }

    void SetSkip()
    {
        skip_ = true;
    }

private:
    spdlog::logger *logger_;
    spdlog::level::level_enum level_;
    spdlog::source_loc loc_;
    std::ostringstream stream_;
    bool skip_ = false;
};

/**
 * @brief Check if log should be admitted based on rate limiting and trace sampling.
 */
inline bool ShouldLog(spdlog::level::level_enum level)
{
    (void)level;  // Currently unused, reserved for level-based filtering

    // For now, check rate limiting if enabled
    uint64_t traceHash = Trace::Instance().GetTraceHash();

    // If rate limiting is enabled, check ShouldLog
    if (RateLimiter::Instance().IsEnabled()) {
        auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        if (!RateLimiter::Instance().ShouldLog(traceHash, nowMs)) {
            return false;
        }
    }

    // Check request sampling decision if trace is marked for sampling
    if (Trace::Instance().IsRequestLogTrace()) {
        bool admitted = false;
        if (Trace::Instance().GetRequestSampleDecision(admitted)) {
            return admitted;
        }
    }

    return true;
}

}  // namespace mooncake

// User-facing log macros
#define LOG_DEBUG                                                           \
    if (!mooncake::ShouldLog(spdlog::level::debug)) {                       \
    } else                                                                  \
        mooncake::LogStream(spdlog::default_logger().get(),                \
                            spdlog::level::debug, __FILE__, __LINE__).Stream()

#define LOG_INFO                                                            \
    if (!mooncake::ShouldLog(spdlog::level::info)) {                        \
    } else                                                                  \
        mooncake::LogStream(spdlog::default_logger().get(),                \
                            spdlog::level::info, __FILE__, __LINE__).Stream()

#define LOG_WARNING                                                         \
    if (!mooncake::ShouldLog(spdlog::level::warn)) {                        \
    } else                                                                  \
        mooncake::LogStream(spdlog::default_logger().get(),                \
                            spdlog::level::warn, __FILE__, __LINE__).Stream()

#define LOG_ERROR                                                          \
    if (!mooncake::ShouldLog(spdlog::level::err)) {                        \
    } else                                                                 \
        mooncake::LogStream(spdlog::default_logger().get(),               \
                            spdlog::level::err, __FILE__, __LINE__).Stream()

// Conditional logging
#define LOG_IF(severity, condition)                                        \
    if (!(condition)) {                                                    \
    } else if (!mooncake::ShouldLog(spdlog::level::severity)) {            \
    } else                                                                 \
        mooncake::LogStream(spdlog::default_logger().get(),               \
                            spdlog::level::severity, __FILE__, __LINE__).Stream()

// CHECK macro - logs and aborts if condition is false
#define CHECK(condition)                                                   \
    if (condition) {                                                      \
    } else                                                                 \
        mooncake::LogStream(spdlog::default_logger().get(),               \
                            spdlog::level::err, __FILE__, __LINE__).Stream() \
            << "CHECK FAILED: " #condition " -- "
