#pragma once

#include <cstdint>
#include <memory>
#include <sstream>

#include <glog/logging.h>

namespace mooncake::logging {

uint64_t NewTraceId();
uint64_t CurrentTraceId();
bool IsMooncakeLogEnabled();
bool ShouldLog(google::LogSeverity severity);
bool ShouldVLog(int level);
void ApplyMooncakeLogEnableToGlog();

class ScopedTraceId {
   public:
    explicit ScopedTraceId(uint64_t trace_id);
    ~ScopedTraceId();

    ScopedTraceId(const ScopedTraceId&) = delete;
    ScopedTraceId& operator=(const ScopedTraceId&) = delete;

   private:
    uint64_t previous_trace_id_;
};

class AsyncLogMessage {
   public:
    AsyncLogMessage(const char* file, int line, google::LogSeverity severity,
                    bool enabled);
    ~AsyncLogMessage();

    AsyncLogMessage(const AsyncLogMessage&) = delete;
    AsyncLogMessage& operator=(const AsyncLogMessage&) = delete;

    std::ostream& stream();

   private:
    const char* file_;
    int line_;
    google::LogSeverity severity_;
    bool enabled_;
    uint64_t trace_id_;
    std::ostringstream stream_;
};

void FlushAsyncLogs();

}  // namespace mooncake::logging

// NoOpStream: dummy stream that discards everything when logging is disabled
class NoOpStream {
   public:
    template <typename T>
    NoOpStream& operator<<(const T&) {
        return *this;
    }
};

#define MC_LOG(severity)                                                      \
    ([&]() -> std::ostream& {                                                \
        if (!mooncake::logging::ShouldLog(google::severity)) {               \
            static NoOpStream dev_null;                                       \
            return dev_null;                                                 \
        }                                                                    \
        static mooncake::logging::AsyncLogMessage __msg__(                    \
            __FILE__, __LINE__, google::severity, true);                     \
        return __msg__.stream();                                              \
    }())

#define MC_VLOG(level)                                                        \
    ([&]() -> std::ostream& {                                                \
        if (!mooncake::logging::ShouldVLog(level)) {                         \
            static NoOpStream dev_null;                                       \
            return dev_null;                                                 \
        }                                                                    \
        static mooncake::logging::AsyncLogMessage __msg__(                    \
            __FILE__, __LINE__, google::INFO, true);                          \
        return __msg__.stream();                                              \
    }())
