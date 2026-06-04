#pragma once

#include <cstdint>

// glog is still pulled in here on purpose: raw LOG()/CHECK() call sites across
// the codebase historically relied on this header to make glog available
// transitively. The async MC_LOG path that used to live here has been replaced
// by the spdlog-backed LOG_* macros in log_macros.h; only the trace-id helpers
// remain.
#include <glog/logging.h>

namespace mooncake::logging {

// Trace-id helpers. Thread-local current trace id, propagated across async
// boundaries by callers and stamped into every LOG_* line (see log_macros.h).
uint64_t NewTraceId();
uint64_t CurrentTraceId();

class ScopedTraceId {
   public:
    explicit ScopedTraceId(uint64_t trace_id);
    ~ScopedTraceId();

    ScopedTraceId(const ScopedTraceId&) = delete;
    ScopedTraceId& operator=(const ScopedTraceId&) = delete;

   private:
    uint64_t previous_trace_id_;
};

}  // namespace mooncake::logging
