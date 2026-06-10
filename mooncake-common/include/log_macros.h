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

#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <iosfwd>
#include <sstream>

#include "trace.h"

namespace mooncake {

enum class LogLevel {
    kDebug,
    kInfo,
    kWarning,
    kError,
    kFatal,
};

/**
 * @brief Internal log stream builder - RAII object that logs on destruction.
 */
class LogStream {
public:
    LogStream(LogLevel level, const char *file, int line, bool fatal = false,
              int saved_errno = 0, bool to_console = false);
    LogStream(const LogStream &) = delete;
    LogStream &operator=(const LogStream &) = delete;
    LogStream(LogStream &&other) noexcept;
    LogStream &operator=(LogStream &&other) = delete;

    ~LogStream();

    std::ostream &Stream()
    {
        return stream_;
    }

    void SetSkip()
    {
        skip_ = true;
    }

private:
    LogLevel level_;
    const char *file_;
    int line_;
    std::ostringstream stream_;
    bool skip_ = false;
    bool fatal_ = false;
    int saved_errno_ = 0;
    bool to_console_ = false;
};

class LogStreamVoidify {
public:
    void operator&(std::ostream &)
    {
    }
};

/**
 * @brief Check if log should be admitted based on rate limiting and trace sampling.
 */
bool ShouldLog(LogLevel level);
bool ShouldVLog(int level);
int GetLogVerbosity();
void SetLogVerbosity(int verbosity);

LogStream MakeLogStream(LogLevel level, const char *file, int line,
                        bool fatal = false, int saved_errno = 0);

// Builds a LogStream that emits via ConsoleLogger() (terminal + file). Backs
// the CLOG() macros.
LogStream MakeConsoleLogStream(LogLevel level, const char *file, int line,
                               bool fatal = false, int saved_errno = 0);

}  // namespace mooncake

#define MOONCAKE_SPDLOG_LEVEL_DEBUG mooncake::LogLevel::kDebug
#define MOONCAKE_SPDLOG_LEVEL_INFO mooncake::LogLevel::kInfo
#define MOONCAKE_SPDLOG_LEVEL_WARNING mooncake::LogLevel::kWarning
#define MOONCAKE_SPDLOG_LEVEL_WARN mooncake::LogLevel::kWarning
#define MOONCAKE_SPDLOG_LEVEL_ERROR mooncake::LogLevel::kError
#define MOONCAKE_SPDLOG_LEVEL_ERR mooncake::LogLevel::kError
#define MOONCAKE_SPDLOG_LEVEL_FATAL mooncake::LogLevel::kFatal

#define MOONCAKE_LOG_IS_FATAL_DEBUG false
#define MOONCAKE_LOG_IS_FATAL_INFO false
#define MOONCAKE_LOG_IS_FATAL_WARNING false
#define MOONCAKE_LOG_IS_FATAL_WARN false
#define MOONCAKE_LOG_IS_FATAL_ERROR false
#define MOONCAKE_LOG_IS_FATAL_ERR false
#define MOONCAKE_LOG_IS_FATAL_FATAL true

#define MOONCAKE_LOG_STREAM_IF(level, fatal, saved_errno, condition)       \
    (!(condition) || (!mooncake::ShouldLog(level) && !(fatal)))            \
        ? (void)0                                                          \
        : mooncake::LogStreamVoidify() &                                   \
              mooncake::MakeLogStream(level, __FILE__, __LINE__, fatal,    \
                                      saved_errno)                         \
                  .Stream()

#define MOONCAKE_LOG_STREAM(level, fatal, saved_errno)                     \
    MOONCAKE_LOG_STREAM_IF(level, fatal, saved_errno, true)

// User-facing log macros
#define LOG_DEBUG MOONCAKE_LOG_STREAM(mooncake::LogLevel::kDebug, false, 0)
#define LOG_INFO MOONCAKE_LOG_STREAM(mooncake::LogLevel::kInfo, false, 0)
#define LOG_WARNING MOONCAKE_LOG_STREAM(mooncake::LogLevel::kWarning, false, 0)
#define LOG_WARN LOG_WARNING
#define LOG_ERROR MOONCAKE_LOG_STREAM(mooncake::LogLevel::kError, false, 0)
#define LOG_FATAL MOONCAKE_LOG_STREAM(mooncake::LogLevel::kFatal, true, 0)

#define PLOG_DEBUG MOONCAKE_LOG_STREAM(mooncake::LogLevel::kDebug, false, errno)
#define PLOG_INFO MOONCAKE_LOG_STREAM(mooncake::LogLevel::kInfo, false, errno)
#define PLOG_WARNING MOONCAKE_LOG_STREAM(mooncake::LogLevel::kWarning, false, errno)
#define PLOG_WARN PLOG_WARNING
#define PLOG_ERROR MOONCAKE_LOG_STREAM(mooncake::LogLevel::kError, false, errno)
#define PLOG_FATAL MOONCAKE_LOG_STREAM(mooncake::LogLevel::kFatal, true, errno)

#define LOG(severity)                                                      \
    MOONCAKE_LOG_STREAM(MOONCAKE_SPDLOG_LEVEL_##severity,                  \
                        MOONCAKE_LOG_IS_FATAL_##severity, 0)
#define PLOG(severity)                                                     \
    MOONCAKE_LOG_STREAM(MOONCAKE_SPDLOG_LEVEL_##severity,                  \
                        MOONCAKE_LOG_IS_FATAL_##severity, errno)
#define VLOG(level)                                                        \
    MOONCAKE_LOG_STREAM_IF(mooncake::LogLevel::kInfo, false, 0,            \
                           mooncake::ShouldVLog(level))

// Conditional logging
#define LOG_IF(severity, condition)                                        \
    MOONCAKE_LOG_STREAM_IF(MOONCAKE_SPDLOG_LEVEL_##severity,               \
                           MOONCAKE_LOG_IS_FATAL_##severity, 0, condition)

#define PLOG_IF(severity, condition)                                       \
    MOONCAKE_LOG_STREAM_IF(MOONCAKE_SPDLOG_LEVEL_##severity,               \
                           MOONCAKE_LOG_IS_FATAL_##severity, errno,        \
                           condition)

#define VLOG_IF(level, condition)                                          \
    MOONCAKE_LOG_STREAM_IF(mooncake::LogLevel::kInfo, false, 0,            \
                           (condition) && mooncake::ShouldVLog(level))

#define MOONCAKE_CONCAT_INNER(x, y) x##y
#define MOONCAKE_CONCAT(x, y) MOONCAKE_CONCAT_INNER(x, y)

#define LOG_EVERY_N(severity, n)                                           \
    static std::atomic<uint64_t> MOONCAKE_CONCAT(_mooncake_log_every_n_,    \
                                                 __LINE__){0};             \
    LOG_IF(severity,                                                       \
           (MOONCAKE_CONCAT(_mooncake_log_every_n_, __LINE__)              \
                .fetch_add(1, std::memory_order_relaxed) %                 \
            static_cast<uint64_t>(n)) == 0)

// CHECK macro - logs and aborts if condition is false
#define CHECK(condition)                                                   \
    if (condition) {                                                       \
    } else                                                                 \
        mooncake::MakeLogStream(mooncake::LogLevel::kFatal, __FILE__,      \
                                __LINE__, true, 0).Stream()                \
            << "CHECK FAILED: " #condition " -- "

#define LOG_ASSERT(condition) CHECK(condition)

// ---------------------------------------------------------------------------
// spdlog-branch macro family (ported during the lx/supercache_dev merge),
// kept in the function-macro form DLOG(severity) / CLOG(severity) to match
// LOG(severity) / VLOG(level).
//
//   DLOG(severity): "detail" logs, additionally gated by MC_LOG_DETAIL_ENABLE.
//                   LOG_IF already applies the per-level ShouldLog() check, so
//                   the combined semantics equal the old ShouldDetailLog().
//   CLOG(severity): "critical/console" logs that ALWAYS emit regardless of
//                   MC_LOG_ENABLE, rate-limiting and request sampling, honoring
//                   only spdlog's own level filter. Matches the spdlog-branch
//                   console logger whose output must reach terminal+file even
//                   when MC_LOG_ENABLE is off.
//
// TRACE has no dedicated LogLevel in this tree; it is aliased to DEBUG.
// ---------------------------------------------------------------------------
#define MOONCAKE_SPDLOG_LEVEL_TRACE mooncake::LogLevel::kDebug
#define MOONCAKE_LOG_IS_FATAL_TRACE false

namespace mooncake {
bool DetailLogEnabledFromEnv();
bool ShouldLogAlways(LogLevel level);
}  // namespace mooncake

#define DLOG(severity) LOG_IF(severity, ::mooncake::DetailLogEnabledFromEnv())

#define MOONCAKE_CLOG_STREAM(level, fatal)                                   \
    !::mooncake::ShouldLogAlways(level)                                      \
        ? (void)0                                                            \
        : ::mooncake::LogStreamVoidify() &                                   \
              ::mooncake::MakeConsoleLogStream(level, __FILE__, __LINE__,     \
                                               fatal, 0)                     \
                  .Stream()

#define CLOG(severity)                                                       \
    MOONCAKE_CLOG_STREAM(MOONCAKE_SPDLOG_LEVEL_##severity,                   \
                         MOONCAKE_LOG_IS_FATAL_##severity)
