#include "mooncake_logging.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>

// The async MC_LOG/MC_VLOG logging path that used to live here (lock-free MPSC
// ring buffer, AsyncLogMessage, glog integration) has been retired in favor of
// the spdlog-backed LOG_* macros (see log_macros.h / logger.cpp). Only the
// thread-local trace-id helpers remain, since they are still used to propagate
// a trace id across async boundaries and to stamp every LOG_* line.

namespace mooncake::logging {
namespace {

thread_local uint64_t current_trace_id = 0;

uint64_t SteadyClockNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

uint64_t NewTraceId() {
    // Use pointer address as high-entropy base — far better than PID alone
    static const uint64_t process_seed = [] {
        uint64_t ptr_val = reinterpret_cast<uint64_t>(&process_seed);
        return (ptr_val << 32) ^ (SteadyClockNs() & 0x0000FFFFFFFFFFFFULL);
    }();
    static std::atomic<uint64_t> counter{1};
    return process_seed ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CurrentTraceId() { return current_trace_id; }

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(current_trace_id) {
    current_trace_id = trace_id;
}

ScopedTraceId::~ScopedTraceId() { current_trace_id = previous_trace_id_; }

}  // namespace mooncake::logging
