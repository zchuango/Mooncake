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

#pragma once

#include <atomic>
#include <cstdint>
#include <unordered_map>
#include <mutex>

namespace mooncake {

/**
 * @brief Rate limiter for log sampling per trace.
 *
 * Uses a sliding window approach: each traceHash gets a per-second budget.
 * When rateLimit is 0, all logs are admitted (no limiting).
 */
class RateLimiter {
public:
    RateLimiter(const RateLimiter &other) = delete;
    RateLimiter &operator=(const RateLimiter &) = delete;
    ~RateLimiter() = default;

    /**
     * @brief Singleton instance.
     */
    static RateLimiter &Instance();

    /**
     * @brief Set the rate limit (logs per second per trace).
     * @param[in] rate Max logs per second, 0 means unlimited.
     */
    void SetRate(int32_t rate);

    /**
     * @brief Get current rate limit.
     */
    int32_t GetRate() const;

    /**
     * @brief Check if a log should be admitted for given trace.
     * @param[in] traceHash The trace identifier hash.
     * @param[in] nowMs Current timestamp in milliseconds.
     * @return true if log should be recorded, false if dropped.
     */
    bool ShouldLog(uint64_t traceHash, int64_t nowMs);

    /**
     * @brief Check if rate limiting is enabled.
     */
    bool IsEnabled() const;

private:
    RateLimiter() = default;

    // Per-trace window: (lastWindowMs, countInWindow)
    struct Window {
        int64_t lastWindowMs = 0;
        int32_t count = 0;
    };

    // Clean up stale windows periodically
    void CleanupStaleWindowsLocked(int64_t nowMs);

    std::atomic<int32_t> rate_{ 0 };  // 0 = unlimited
    std::atomic<int64_t> lastCleanupMs_{ 0 };

    // Thread-safe map
    std::unordered_map<uint64_t, Window> windows_;
    std::mutex mutex_;
};

}  // namespace mooncake
