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

#include <memory>
#include <string>
#include "log_config.h"

namespace mooncake {

/**
 * @brief Build a LogConfig from environment variables, preserving the legacy
 *        MC_LOG_* operational knobs (MC_LOG_ENABLE / MC_LOG_DIR / MC_LOG_LEVEL /
 *        MC_LOG_MAX_SIZE / MC_LOG_BUFFER_SECS). MC_LOG_ENABLE defaults to on.
 */
LogConfig LogConfigFromEnv();

/**
 * @brief Check whether detailed client-side logs are enabled via
 *        MC_LOG_DETAIL_ENABLE. Only explicit truthy values enable it.
 */
bool DetailLogEnabledFromEnv();

/**
 * @brief Logger singleton for mooncake async logging.
 *
 * Initializes spdlog async logger with rotating file sink.
 * Use Logger::Instance().Init(config) to initialize before logging.
 */
class Logger {
public:
    Logger(const Logger &other) = delete;
    Logger &operator=(const Logger &) = delete;
    ~Logger();

    /**
     * @brief Singleton instance accessor.
     */
    static Logger &Instance();

    /**
     * @brief Initialize logger with config.
     * @param[in] config Log configuration (uses defaults if empty).
     */
    void Init(const LogConfig &config = LogConfig());

    /**
     * @brief Shutdown logger and flush pending logs.
     */
    void Shutdown();

    /**
     * @brief Flush pending log messages.
     */
    void Flush();

    /**
     * @brief Set log level at runtime.
     * @param[in] level String level: DEBUG, INFO, WARNING, ERROR.
     */
    void SetLevel(const std::string &level);

    /**
     * @brief Check if logger is initialized.
     */
    bool IsInitialized() const;

private:
    Logger();

    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

}  // namespace mooncake
