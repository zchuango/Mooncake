#include "log_macros.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <string>

#include <spdlog/spdlog.h>

#include "rate_limiter.h"
#include "trace.h"

namespace mooncake {

namespace {

std::atomic<int> g_log_verbosity{0};

// MC_LOG_ENABLE master kill switch (ported from the spdlog branch).
// Default ON when unset/empty; only an explicit falsy value turns logging off.
// Cached because the env var is a process-level setting.
bool LogEnabledFromEnv()
{
    static const bool enabled = []() {
        const char *value = std::getenv("MC_LOG_ENABLE");
        if (value == nullptr || *value == '\0') {
            return true;
        }
        std::string lowered(value);
        std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                       [](unsigned char ch) { return std::tolower(ch); });
        return !(lowered == "off" || lowered == "0" || lowered == "false" ||
                 lowered == "no");
    }();
    return enabled;
}

spdlog::level::level_enum ToSpdlogLevel(LogLevel level)
{
    switch (level) {
        case LogLevel::kDebug:
            return spdlog::level::debug;
        case LogLevel::kInfo:
            return spdlog::level::info;
        case LogLevel::kWarning:
            return spdlog::level::warn;
        case LogLevel::kError:
            return spdlog::level::err;
        case LogLevel::kFatal:
            return spdlog::level::critical;
    }
    return spdlog::level::info;
}

}  // namespace

LogStream::LogStream(LogLevel level, const char *file, int line, bool fatal,
                     int saved_errno)
    : level_(level),
      file_(file),
      line_(line),
      fatal_(fatal),
      saved_errno_(saved_errno)
{
}

LogStream::LogStream(LogStream &&other) noexcept
    : level_(other.level_),
      file_(other.file_),
      line_(other.line_),
      stream_(std::move(other.stream_)),
      skip_(other.skip_),
      fatal_(other.fatal_),
      saved_errno_(other.saved_errno_)
{
    other.skip_ = true;
    other.fatal_ = false;
}

LogStream::~LogStream()
{
    auto logger = spdlog::default_logger();
    if (logger && !skip_) {
        auto msg = stream_.str();
        if (saved_errno_ != 0) {
            msg += ": ";
            msg += std::strerror(saved_errno_);
        }

        spdlog::source_loc loc;
        loc.filename = file_;
        loc.line = static_cast<size_t>(line_);
        auto spdlog_level = ToSpdlogLevel(level_);
        logger->log(loc, spdlog_level,
                    spdlog::string_view_t(msg.data(), msg.size()));
        if (fatal_) {
            logger->flush();
        }
    }
    if (fatal_) {
        std::abort();
    }
}

// MC_LOG_DETAIL_ENABLE secondary switch, backs the DLOG() detail-log macros.
// Default OFF; only an explicit truthy value enables detail logs.
bool DetailLogEnabledFromEnv()
{
    static const bool enabled = []() {
        const char *value = std::getenv("MC_LOG_DETAIL_ENABLE");
        if (value == nullptr || *value == '\0') {
            return false;
        }
        std::string lowered(value);
        std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                       [](unsigned char ch) { return std::tolower(ch); });
        return lowered == "on" || lowered == "1" || lowered == "true" ||
               lowered == "yes";
    }();
    return enabled;
}

bool ShouldLog(LogLevel level)
{
    if (!LogEnabledFromEnv()) {
        return false;
    }
    auto spdlog_level = ToSpdlogLevel(level);
    auto logger = spdlog::default_logger();
    if (logger && !logger->should_log(spdlog_level)) {
        return false;
    }

    uint64_t traceHash = Trace::Instance().GetTraceHash();
    if (RateLimiter::Instance().IsEnabled()) {
        auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        if (!RateLimiter::Instance().ShouldLog(traceHash, nowMs)) {
            return false;
        }
    }

    if (Trace::Instance().IsRequestLogTrace()) {
        bool admitted = false;
        if (Trace::Instance().GetRequestSampleDecision(admitted)) {
            return admitted;
        }
    }

    return true;
}

// Backs the CLOG(severity) macros: always emit regardless of MC_LOG_ENABLE,
// rate-limiting and request sampling; honor only spdlog's own level filter.
bool ShouldLogAlways(LogLevel level)
{
    auto spdlog_level = ToSpdlogLevel(level);
    auto logger = spdlog::default_logger();
    return !(logger && !logger->should_log(spdlog_level));
}

bool ShouldVLog(int level)
{
    return level <= g_log_verbosity.load(std::memory_order_relaxed);
}

int GetLogVerbosity()
{
    return g_log_verbosity.load(std::memory_order_relaxed);
}

void SetLogVerbosity(int verbosity)
{
    g_log_verbosity.store(std::max(0, verbosity), std::memory_order_relaxed);
}

LogStream MakeLogStream(LogLevel level, const char *file, int line, bool fatal,
                        int saved_errno)
{
    return LogStream(level, file, line, fatal, saved_errno);
}

}  // namespace mooncake
