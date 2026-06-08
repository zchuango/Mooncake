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

#include "logger.h"
#include "rate_limiter.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <stdexcept>
#include <map>
#include <filesystem>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>

namespace mooncake {

static std::map<std::string, spdlog::level::level_enum> LEVEL_MAP = {
    { "TRACE", spdlog::level::trace },
    { "DEBUG", spdlog::level::debug },
    { "INFO", spdlog::level::info },
    { "WARNING", spdlog::level::warn },
    { "ERROR", spdlog::level::err },
    { "CRITICAL", spdlog::level::critical },
    { "FATAL", spdlog::level::critical },
    { "OFF", spdlog::level::off }
};

class Logger::Impl {
public:
    bool Init(const LogConfig &config)
    {
        spdlog::drop_all();

        // Create log directory if not exists
        std::error_code ec;
        std::filesystem::create_directories(config.logDir, ec);
        if (ec) {
            return false;
        }

        // Initialize async thread pool
        if (!spdlog::thread_pool()) {
            spdlog::init_thread_pool(config.asyncQueueSize, config.asyncThreads);
        }

        // Create rotating file sink
        auto fileSink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            config.logDir + "/" + config.fileName + ".log",
            config.maxSizeMB * 1024 * 1024,
            config.maxFiles);

        // Create async logger
        logger_ = std::make_shared<spdlog::async_logger>(
            "mooncake",
            fileSink,
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::overrun_oldest);

        spdlog::initialize_logger(logger_);
        logger_->set_pattern(config.pattern, spdlog::pattern_time_type::local);

        auto levelIt = LEVEL_MAP.find(config.level);
        auto level = (levelIt != LEVEL_MAP.end()) ? levelIt->second : spdlog::level::info;
        logger_->set_level(level);

        // Set as default logger
        spdlog::set_default_logger(logger_);

        // Periodic background flush (the field was previously unused).
        if (config.flushIntervalSecs > 0) {
            spdlog::flush_every(std::chrono::seconds(config.flushIntervalSecs));
        }

        // Configure rate limiter
        RateLimiter::Instance().SetRate(config.rateLimit);

        initialized_ = true;
        return true;
    }

    void Shutdown()
    {
        if (logger_) {
            logger_->flush();
        }
        spdlog::shutdown();
        initialized_ = false;
    }

    std::shared_ptr<spdlog::logger> GetLogger()
    {
        return logger_;
    }

    void SetLevel(const std::string &levelStr)
    {
        auto it = LEVEL_MAP.find(levelStr);
        if (it != LEVEL_MAP.end() && logger_) {
            logger_->set_level(it->second);
        }
    }

    bool IsInitialized() const
    {
        return initialized_;
    }

private:
    std::shared_ptr<spdlog::logger> logger_;
    bool initialized_ = false;
};

Logger::Logger() : pImpl_(std::make_unique<Impl>())
{
}

Logger::~Logger()
{
    if (pImpl_->IsInitialized()) {
        Shutdown();
    }
}

Logger &Logger::Instance()
{
    static Logger instance;
    return instance;
}

void Logger::Init(const LogConfig &config)
{
    pImpl_->Init(config);
}

void Logger::Shutdown()
{
    pImpl_->Shutdown();
}

std::shared_ptr<spdlog::logger> Logger::GetSpdlogger()
{
    return pImpl_->GetLogger();
}

void Logger::SetLevel(const std::string &level)
{
    pImpl_->SetLevel(level);
}

bool Logger::IsInitialized() const
{
    return pImpl_->IsInitialized();
}

namespace {

std::string UpperString(const char *value)
{
    std::string text(value ? value : "");
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return std::toupper(ch); });
    return text;
}

// MC_LOG_ENABLE semantics preserved from the legacy glog path: default OFF;
// only an explicit truthy value turns logging on.
bool LogEnabledFromEnv()
{
    const char *value = std::getenv("MC_LOG_ENABLE");
    if (value == nullptr || *value == '\0') {
        return false;
    }
    std::string lowered(value);
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return !(lowered == "off" || lowered == "0" || lowered == "false" ||
             lowered == "no");
}

}  // namespace

bool DetailLogEnabledFromEnv()
{
    const char *value = std::getenv("MC_LOG_DETAIL_ENABLE");
    if (value == nullptr || *value == '\0') {
        return false;
    }
    std::string lowered(value);
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return lowered == "on" || lowered == "1" || lowered == "true" ||
           lowered == "yes";
}

LogConfig LogConfigFromEnv()
{
    LogConfig config;

    // Directory: honor MC_LOG_DIR, else match the legacy glog default.
    if (const char *dir = std::getenv("MC_LOG_DIR")) {
        config.logDir = dir;
    } else {
        config.logDir = "/var/log/mooncake";
    }

    if (const char *level = std::getenv("MC_LOG_LEVEL")) {
        config.level = UpperString(level);
    }

    if (const char *maxSize = std::getenv("MC_LOG_MAX_SIZE")) {
        config.maxSizeMB = static_cast<uint32_t>(std::atoi(maxSize));
    }

    if (const char *bufSecs = std::getenv("MC_LOG_BUFFER_SECS")) {
        config.flushIntervalSecs = std::atoi(bufSecs);
    } else {
        config.flushIntervalSecs = 3;  // legacy FLAGS_logbufsecs default
    }

    // Total kill switch: when disabled, set level OFF so spdlog drops all logs.
    if (!LogEnabledFromEnv()) {
        config.level = "OFF";
    }

    return config;
}

}  // namespace mooncake
