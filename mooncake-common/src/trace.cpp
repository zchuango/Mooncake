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

#include "trace.h"
#include "log_config.h"
#include "logger.h"
#include "log_macros.h"

#include <atomic>
#include <cstring>
#include <unistd.h>

namespace mooncake {

// Thread-local storage for trace context
thread_local std::string Trace::currentTraceID_;
thread_local uint64_t Trace::currentTraceHash_ = 0;

Trace &Trace::Instance()
{
    static Trace instance;
    return instance;
}

void Trace::SetTraceID(const std::string &traceId)
{
    currentTraceID_ = traceId;
    currentTraceHash_ = FnvHash(traceId);
}

void Trace::SetTraceID(uint64_t traceId)
{
    currentTraceID_ = std::to_string(traceId);
    currentTraceHash_ = traceId;  // Use the integer value directly as hash
}

std::string Trace::GetTraceID() const
{
    return currentTraceID_;
}

uint64_t Trace::GetTraceHash() const
{
    return currentTraceHash_;
}

void Trace::Invalidate()
{
    currentTraceID_.clear();
    currentTraceHash_ = 0;
    requestLogTrace_ = false;
    requestSampleDecisionValid_ = false;
}

// FNV-1a 64-bit hash
uint64_t Trace::FnvHash(const std::string &str)
{
    if (str.empty()) {
        return 0;
    }

    uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
    for (char c : str) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ULL;  // FNV prime
    }
    return hash;
}

}  // namespace mooncake

namespace mooncake::logging {

uint64_t NewTraceId()
{
    static std::atomic<uint64_t> counter{1};
    const uint64_t pid = static_cast<uint64_t>(getpid()) & 0xffff;
    return (pid << 48) ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CurrentTraceId()
{
    const auto trace_id = Trace::Instance().GetTraceID();
    if (trace_id.empty()) {
        return 0;
    }
    try {
        return std::stoull(trace_id);
    } catch (...) {
        return Trace::Instance().GetTraceHash();
    }
}

bool ShouldLog(LogLevel level)
{
    return mooncake::ShouldLog(level);
}

void ApplyMooncakeLogEnableToGlog()
{
    if (!Logger::Instance().IsInitialized()) {
        Logger::Instance().Init(LogConfig());
    }
}

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(Trace::Instance().GetTraceID())
{
    Trace::Instance().SetTraceID(trace_id);
}

ScopedTraceId::~ScopedTraceId()
{
    if (previous_trace_id_.empty()) {
        Trace::Instance().Invalidate();
    } else {
        Trace::Instance().SetTraceID(previous_trace_id_);
    }
}

}  // namespace mooncake::logging
