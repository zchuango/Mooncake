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
 * Description: Simplified trace context for request tracing and log sampling.
 */
#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

enum class LogLevel;

/**
 * @brief Simplified trace context for request tracking.
 *
 * Provides thread-local trace ID storage and hash computation for rate limiting.
 * This is a simplified version of datasystem's Trace class.
 */
class Trace {
public:
    Trace(const Trace &other) = delete;
    Trace &operator=(const Trace &) = delete;
    ~Trace() = default;

    /**
     * @brief Singleton instance accessor.
     */
    static Trace &Instance();

    /**
     * @brief Set trace ID for current thread.
     * @param[in] traceId The trace ID string.
     */
    void SetTraceID(const std::string &traceId);

    /**
     * @brief Set trace ID from a 64-bit integer (for efficiency).
     * @param[in] traceId The trace ID as integer.
     */
    void SetTraceID(uint64_t traceId);

    /**
     * @brief Get trace ID as string.
     * @return Current trace ID, empty if not set.
     */
    std::string GetTraceID() const;

    /**
     * @brief Get 64-bit FNV-1a hash of current trace ID.
     * @return Hash value, 0 if trace ID is empty.
     */
    uint64_t GetTraceHash() const;

    /**
     * @brief Clear trace ID for current thread.
     */
    void Invalidate();

    /**
     * @brief Check if request log sampling is enabled for current trace.
     */
    bool IsRequestLogTrace() const
    {
        return requestLogTrace_;
    }

    /**
     * @brief Enable/disable request log sampling for current trace.
     */
    void SetRequestLogTrace(bool enabled = true)
    {
        requestLogTrace_ = enabled;
    }

    /**
     * @brief Get request sample decision.
     * @param[out] admitted Output admitted flag.
     * @return true if decision exists.
     */
    bool GetRequestSampleDecision(bool &admitted) const
    {
        if (!requestSampleDecisionValid_) {
            return false;
        }
        admitted = requestSampleDecisionAdmitted_;
        return true;
    }

    /**
     * @brief Set request sample decision.
     */
    void SetRequestSampleDecision(bool valid, bool admitted)
    {
        requestSampleDecisionValid_ = valid;
        requestSampleDecisionAdmitted_ = admitted;
    }

private:
    Trace() = default;

    // FNV-1a hash computation
    static uint64_t FnvHash(const std::string &str);

    // Thread-local trace ID storage
    thread_local static std::string currentTraceID_;
    thread_local static uint64_t currentTraceHash_;

    // Sampling control
    bool requestLogTrace_ = false;
    bool requestSampleDecisionValid_ = false;
    bool requestSampleDecisionAdmitted_ = false;
};

}  // namespace mooncake

namespace mooncake::logging {

uint64_t NewTraceId();
uint64_t CurrentTraceId();
bool ShouldLog(LogLevel level);
void ApplyMooncakeLogEnableToGlog();

class ScopedTraceId {
public:
    explicit ScopedTraceId(uint64_t trace_id);
    ~ScopedTraceId();

    ScopedTraceId(const ScopedTraceId &) = delete;
    ScopedTraceId &operator=(const ScopedTraceId &) = delete;

private:
    std::string previous_trace_id_;
};

}  // namespace mooncake::logging
