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

#include <cstdint>
#include <string>

namespace mooncake {

/**
 * @brief Log configuration structure.
 *
 * All settings have reasonable defaults; initialization can be done with zero args.
 * Environment variable override: MOONCAKE_LOG_DIR, MOONCAKE_LOG_LEVEL, MOONCAKE_LOG_FILE.
 */
struct LogConfig {
    /** Log output directory */
    std::string logDir = "/tmp/mooncake/logs";

    /** Log level: DEBUG, INFO, WARNING, ERROR */
    std::string level = "INFO";

    /** Log message pattern (spdlog format) */
    std::string pattern = "%Y-%m-%d %H:%M:%S.%6f | pid=%P tid=%t | %^%L%$ | %s:%# | %v";

    /** Log file base name (produces name.log, name.log.1, ...) */
    std::string fileName = "app";

    /** Maximum size per log file in MB before rotation */
    uint32_t maxSizeMB = 100;

    /** Maximum number of rotated log files to keep */
    uint32_t maxFiles = 5;

    /** Async queue capacity (messages) */
    uint32_t asyncQueueSize = 65536;

    /** Number of async worker threads */
    uint32_t asyncThreads = 2;

    /** Periodic flush interval in seconds */
    int flushIntervalSecs = 5;

    /** Rate limit: max logs per second per trace (0 = unlimited) */
    int32_t rateLimit = 0;
};

}  // namespace mooncake
