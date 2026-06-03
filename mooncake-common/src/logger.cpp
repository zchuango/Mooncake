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
#include "config.h"
#include "rate_limiter.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <stdexcept>
#include <map>
#include <filesystem>

namespace mooncake {

static std::map<std::string, spdlog::level::level_enum> LEVEL_MAP = {
    { "DEBUG", spdlog::level::debug },
    { "INFO", spdlog::level::info },
    { "WARNING", spdlog::level::warn },
    { "ERROR", spdlog::level::err }
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

}  // namespace mooncake
