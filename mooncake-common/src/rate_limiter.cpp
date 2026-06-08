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

#include "rate_limiter.h"
#include <chrono>

namespace mooncake {

constexpr int64_t WINDOW_SIZE_MS = 1000;        // 1 second window
constexpr int64_t CLEANUP_INTERVAL_MS = 60000;  // Cleanup every 60 seconds

RateLimiter &RateLimiter::Instance()
{
    static RateLimiter instance;
    return instance;
}

void RateLimiter::SetRate(int32_t rate)
{
    rate_.store(rate, std::memory_order_relaxed);
}

int32_t RateLimiter::GetRate() const
{
    return rate_.load(std::memory_order_relaxed);
}

bool RateLimiter::IsEnabled() const
{
    return rate_.load(std::memory_order_relaxed) > 0;
}

bool RateLimiter::ShouldLog(uint64_t traceHash, int64_t nowMs)
{
    int32_t limit = rate_.load(std::memory_order_relaxed);
    if (limit <= 0) {
        return true;  // No limiting
    }

    std::lock_guard<std::mutex> lock(mutex_);
    CleanupStaleWindowsLocked(nowMs);
    Window &win = windows_[traceHash];

    // Check if we need to reset the window for a new second
    int64_t windowStart = nowMs / WINDOW_SIZE_MS;
    int64_t lastWindowStart = win.lastWindowMs / WINDOW_SIZE_MS;

    if (windowStart > lastWindowStart) {
        // New second, reset counter
        win.lastWindowMs = nowMs;
        win.count = 1;
        return true;
    }

    // Within same second window
    if (win.count < limit) {
        ++win.count;
        return true;
    }

    return false;  // Rate exceeded
}

void RateLimiter::CleanupStaleWindowsLocked(int64_t nowMs)
{
    int64_t lastCleanup = lastCleanupMs_.load(std::memory_order_relaxed);
    if (nowMs - lastCleanup < CLEANUP_INTERVAL_MS) {
        return;
    }

    // Only one thread does cleanup
    if (!lastCleanupMs_.compare_exchange_strong(lastCleanup, nowMs, std::memory_order_relaxed)) {
        return;
    }

    int64_t staleThreshold = nowMs - WINDOW_SIZE_MS * 2;

    for (auto it = windows_.begin(); it != windows_.end(); ) {
        if (it->second.lastWindowMs < staleThreshold) {
            it = windows_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace mooncake
