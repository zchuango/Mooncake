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
#include "log_macros.h"
#include "rate_limiter.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <cstdlib>
#include <stdexcept>
#include <map>
#include <filesystem>
#include <algorithm>
#include <cctype>
#include <string>

namespace mooncake {

static std::map<std::string, spdlog::level::level_enum> LEVEL_MAP = {
    { "DEBUG", spdlog::level::debug },
    { "INFO", spdlog::level::info },
    { "WARNING", spdlog::level::warn },
    { "ERROR", spdlog::level::err }
};

// Build a LogConfig from MC_LOG_* environment variables. Declared in logger.h
// and used by service entry points (master.cpp, real_client_main.cpp, ...).
// NOTE: deliberately does NOT lower the level when MC_LOG_ENABLE is off — that
// kill switch is enforced centrally in ShouldLog() for LOG_*/DLOG, while CLOG
// must still emit, so we must not suppress logging via spdlog's level here.
LogConfig LogConfigFromEnv()
{
    LogConfig config;
    if (const char *dir = std::getenv("MC_LOG_DIR"); dir && *dir != '\0') {
        config.logDir = dir;
    }
    if (const char *level = std::getenv("MC_LOG_LEVEL"); level && *level != '\0') {
        std::string upper(level);
        std::transform(upper.begin(), upper.end(), upper.begin(),
                       [](unsigned char ch) { return std::toupper(ch); });
        config.level = upper;
    }
    if (const char *maxSize = std::getenv("MC_LOG_MAX_SIZE");
        maxSize && *maxSize != '\0') {
        config.maxSizeMB = static_cast<uint32_t>(std::atoi(maxSize));
    }
    if (const char *bufSecs = std::getenv("MC_LOG_BUFFER_SECS");
        bufSecs && *bufSecs != '\0') {
        config.flushIntervalSecs = std::atoi(bufSecs);
    }
    return config;
}

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

        // Configure rate limiter
        RateLimiter::Instance().SetRate(config.rateLimit);

        int verbosity = config.verbosity;
        if (const char *env = std::getenv("MC_VLOG_LEVEL")) {
            verbosity = std::atoi(env);
        }
        SetLogVerbosity(verbosity);

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

    void Flush()
    {
        if (logger_) {
            logger_->flush();
        }
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

void Logger::Flush()
{
    pImpl_->Flush();
}

void Logger::SetLevel(const std::string &level)
{
    pImpl_->SetLevel(level);
}

bool Logger::IsInitialized() const
{
    return pImpl_->IsInitialized();
}

}  // namespace mooncake
