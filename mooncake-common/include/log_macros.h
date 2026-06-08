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

#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <cstring>
#include <cerrno>
#include <atomic>
#include <string>

#include "trace.h"
#include "rate_limiter.h"
#include <spdlog/spdlog.h>

// Forward declaration: trace-id helper lives in mooncake_logging.h. Forward
// declaring (instead of including) keeps glog out of every logging TU, while
// letting the LogStream destructor stamp the current trace id into the payload
// on the *producing* thread — the only reliable place for an async logger.
namespace mooncake {
namespace logging {
uint64_t CurrentTraceId();
}  // namespace logging
}  // namespace mooncake

namespace mooncake {

bool DetailLogEnabledFromEnv();

// Returns the always-on stderr console logger (created by Logger::Init), or
// nullptr before init. Backs the CLOG_* macros, whose output must always reach
// the terminal regardless of MC_LOG_ENABLE / MC_LOG_DIR.
spdlog::logger *ConsoleLogger();

// Build the "trace_id[<id>] " prefix on the calling thread. Mirrors the legacy
// MC_LOG / AsyncLogMessage output format so log consumers stay unchanged.
inline std::string TraceIdPrefix()
{
    uint64_t tid = ::mooncake::logging::CurrentTraceId();
    if (tid != 0) {
        return "trace_id[" + std::to_string(tid) + "] ";
    }
    return "trace_id[none] ";
}

/**
 * @brief Internal log stream builder - RAII object that logs on destruction.
 */
class LogStream {
public:
    LogStream(spdlog::logger *logger, spdlog::level::level_enum level, const char *file, int line,
              bool with_trace_prefix = true)
        : logger_(logger), level_(level), with_trace_prefix_(with_trace_prefix)
    {
        loc_.filename = file;
        loc_.line = static_cast<size_t>(line);
    }

    ~LogStream()
    {
        if (logger_ && !skip_) {
            // Pass the message via the non-formatting string_view overload so
            // that '{' / '}' in payloads (e.g. JSON) are never parsed by fmt.
            if (with_trace_prefix_) {
                logger_->log(loc_, level_, TraceIdPrefix() + stream_.str());
            } else {
                logger_->log(loc_, level_, stream_.str());
            }
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
    bool with_trace_prefix_ = true;
};

/**
 * @brief Fatal log stream - logs at critical, flushes, then aborts the process.
 *
 * Mirrors glog LOG(FATAL) / MC_LOG(FATAL) semantics: the message is always
 * emitted (no admission gate) and the process terminates once the temporary
 * is destroyed at the end of the full expression.
 */
class FatalLogStream {
public:
    FatalLogStream(const char *file, int line)
    {
        loc_.filename = file;
        loc_.line = static_cast<size_t>(line);
    }

    [[noreturn]] ~FatalLogStream()
    {
        auto *logger = spdlog::default_logger().get();
        if (logger) {
            logger->log(loc_, spdlog::level::critical,
                        TraceIdPrefix() + stream_.str());
            logger->flush();
        }
        std::abort();
    }

    std::ostream &Stream()
    {
        return stream_;
    }

private:
    spdlog::source_loc loc_;
    std::ostringstream stream_;
};

/**
 * @brief errno-aware log stream (glog PLOG parity).
 *
 * Snapshots errno at construction — the very first thing after the macro is
 * entered — so later stream operations cannot clobber it. On destruction the
 * textual strerror of the saved errno is appended after the user's message,
 * exactly like glog's PLOG. Emission still respects the target logger's level.
 */
class PlogStream {
public:
    PlogStream(spdlog::logger *logger, spdlog::level::level_enum level, const char *file, int line)
        : saved_errno_(errno), logger_(logger), level_(level)
    {
        loc_.filename = file;
        loc_.line = static_cast<size_t>(line);
    }

    ~PlogStream()
    {
        if (logger_ && logger_->should_log(level_)) {
            logger_->log(loc_, level_,
                         TraceIdPrefix() + stream_.str() + ": " +
                             std::strerror(saved_errno_));
        }
    }

    std::ostream &Stream()
    {
        return stream_;
    }

private:
    int saved_errno_;
    spdlog::logger *logger_;
    spdlog::level::level_enum level_;
    spdlog::source_loc loc_;
    std::ostringstream stream_;
};

/**
 * @brief Check if log should be admitted based on rate limiting and trace sampling.
 */
inline bool ShouldLog(spdlog::level::level_enum level)
{
    // Level gate first: lets disabled DEBUG/TRACE (mapped from MC_VLOG) skip
    // building the stream entirely, preserving the old VLOG_IS_ON cheapness.
    auto *logger = spdlog::default_logger().get();
    if (logger && !logger->should_log(level)) {
        return false;
    }

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

inline bool ShouldDetailLog(spdlog::level::level_enum level)
{
    return DetailLogEnabledFromEnv() && ShouldLog(level);
}

// Voidify: lowers a LogStream's ostream& back to void with an operator& whose
// precedence sits between << and ?:. This makes the LOG_* macros expand to a
// single conditional expression, so `if (c) LOG_X << ...; else ...;` is parsed
// correctly (no dangling-else). Same idiom glog uses for LOG_IF.
class LogVoidify {
public:
    LogVoidify() = default;
    void operator&(std::ostream &) {}
};

}  // namespace mooncake

// User-facing log macros. Expand to one conditional expression (dangling-else
// safe). The trailing `<< args` binds inside the false branch via << / & / ?:
// precedence (<<  >  &  >  ?:).
#define MC_LOG_STREAM_AT(spdlog_level)                                      \
    mooncake::LogVoidify() &                                                \
        mooncake::LogStream(spdlog::default_logger().get(), spdlog_level,   \
                            __FILE__, __LINE__).Stream()

#define LOG_TRACE                                                           \
    !mooncake::ShouldLog(spdlog::level::trace)                              \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::trace)

#define LOG_DEBUG                                                           \
    !mooncake::ShouldLog(spdlog::level::debug)                              \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::debug)

#define LOG_INFO                                                            \
    !mooncake::ShouldLog(spdlog::level::info)                               \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::info)

#define LOG_WARNING                                                         \
    !mooncake::ShouldLog(spdlog::level::warn)                               \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::warn)

#define LOG_ERROR                                                           \
    !mooncake::ShouldLog(spdlog::level::err)                                \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::err)

#define DLOG_TRACE                                                          \
    !mooncake::ShouldDetailLog(spdlog::level::trace)                        \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::trace)

#define DLOG_DEBUG                                                          \
    !mooncake::ShouldDetailLog(spdlog::level::debug)                        \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::debug)

#define DLOG_INFO                                                           \
    !mooncake::ShouldDetailLog(spdlog::level::info)                         \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::info)

#define DLOG_WARNING                                                        \
    !mooncake::ShouldDetailLog(spdlog::level::warn)                         \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::warn)

#define DLOG_ERROR                                                          \
    !mooncake::ShouldDetailLog(spdlog::level::err)                          \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(spdlog::level::err)

// Fatal: always emitted, then aborts the process (glog LOG(FATAL) parity).
// Unconditional, so already a single expression — dangling-else safe as-is.
#define LOG_FATAL                                                          \
    mooncake::FatalLogStream(__FILE__, __LINE__).Stream()

// NOTE: LOG_IF and CHECK macros were intentionally removed — their names
// collide with glog's macros of the same name, and glog is still included by
// files that use raw LOG()/CHECK(). Reintroduce as MC_-prefixed variants if
// a spdlog-backed conditional/check is ever needed.

// ---------------------------------------------------------------------------
// MC_-prefixed variants of glog-only macros (PLOG / LOG_IF / LOG_EVERY_N).
// MC_ prefix avoids colliding with glog's same-named macros in TUs that still
// include <glog/logging.h>. All three target the default (file) logger and are
// therefore gated by MC_LOG_ENABLE / MC_LOG_LEVEL exactly like LOG_*.
// ---------------------------------------------------------------------------

// PLOG parity: appends ": <strerror(errno)>". No ShouldLog pre-gate so errno is
// snapshotted (in PlogStream's ctor) before anything can clobber it; emission
// still honors the logger level inside PlogStream's destructor.
#define MC_PLOG_ERROR                                                       \
    mooncake::PlogStream(spdlog::default_logger().get(), spdlog::level::err, \
                         __FILE__, __LINE__).Stream()

#define MC_PLOG_WARNING                                                     \
    mooncake::PlogStream(spdlog::default_logger().get(),                    \
                         spdlog::level::warn, __FILE__, __LINE__).Stream()

// Conditional log (glog LOG_IF parity). Single conditional expression, so it is
// dangling-else safe. Usage: MC_LOG_IF(spdlog::level::info, cond) << msg;
#define MC_LOG_IF(level, cond)                                              \
    (!(cond) || !mooncake::ShouldLog(level))                               \
        ? (void)0                                                           \
        : MC_LOG_STREAM_AT(level)

// Throttled log (glog LOG_EVERY_N parity). Expands to multiple statements, so —
// like glog's own LOG_EVERY_N — it must be used as a standalone statement, not
// inside an unbraced if/else. Each expansion gets its own counter via __LINE__.
#define MC_LOG_EVERY_N_IMPL(level, n, counter)                             \
    static std::atomic<uint64_t> counter{0};                               \
    if ((counter.fetch_add(1, std::memory_order_relaxed) % (n)) == 0)      \
    MC_LOG_STREAM_AT(level)
#define MC_LOG_EVERY_N_CAT(a, b) a##b
#define MC_LOG_EVERY_N_NAME(line) MC_LOG_EVERY_N_CAT(_mc_log_occ_, line)
#define MC_LOG_EVERY_N(level, n)                                            \
    MC_LOG_EVERY_N_IMPL(level, n, MC_LOG_EVERY_N_NAME(__LINE__))

// ---------------------------------------------------------------------------
// CLOG_*: always-on console (stderr) logging, INDEPENDENT of the file logger.
// Routes to the dedicated console logger (mooncake::ConsoleLogger()) which is
// NOT gated by MC_LOG_ENABLE / MC_LOG_DIR — used for init banners and benchmark
// output that must always reach the terminal. No trace-id prefix (clean lines).
// ---------------------------------------------------------------------------
#define CLOG_AT(level)                                                      \
    mooncake::LogVoidify() &                                                \
        mooncake::LogStream(mooncake::ConsoleLogger(), level, __FILE__,     \
                            __LINE__, /*with_trace_prefix=*/false).Stream()

#define CLOG_INFO    CLOG_AT(spdlog::level::info)
#define CLOG_WARNING CLOG_AT(spdlog::level::warn)
#define CLOG_ERROR   CLOG_AT(spdlog::level::err)
