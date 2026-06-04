#include "mooncake_logging.h"
#include "logger.h"
#include "log_macros.h"
#include "log_config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

void setenv_test(const char* key, const char* val) {
    if (val) {
        setenv(key, val, 1);
    } else {
        unsetenv(key);
    }
}

std::string read_file(const std::string& path) {
    std::ifstream f(path);
    if (!f) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Initialize the spdlog logger against a clean temp directory and return the
// path to the log file it writes to.
std::string init_logger_to(const std::string& dir, const std::string& level) {
    std::string cmd = "rm -rf " + dir + " && mkdir -p " + dir;
    (void)system(cmd.c_str());

    LogConfig config;
    config.logDir = dir;
    config.level = level;
    config.fileName = "app";
    config.flushIntervalSecs = 0;  // no background flush; we flush explicitly
    Logger::Instance().Init(config);
    return dir + "/app.log";
}

// ===========================================================================
// Trace-id helpers (the only survivors of mooncake_logging.cpp)
// ===========================================================================
TEST(LoggingTraceId, UniqueIds) {
    uint64_t id1 = logging::NewTraceId();
    uint64_t id2 = logging::NewTraceId();
    uint64_t id3 = logging::NewTraceId();
    EXPECT_NE(id1, id2);
    EXPECT_NE(id2, id3);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id1, 0u);
}

TEST(LoggingTraceId, ScopedTraceId) {
    EXPECT_EQ(logging::CurrentTraceId(), 0u);
    uint64_t tid = logging::NewTraceId();
    {
        logging::ScopedTraceId _(tid);
        EXPECT_EQ(logging::CurrentTraceId(), tid);
    }
    EXPECT_EQ(logging::CurrentTraceId(), 0u);
}

// ===========================================================================
// LogConfigFromEnv: legacy MC_LOG_* knobs map onto LogConfig
// ===========================================================================
TEST(LogConfigEnv, MapsKnobs) {
    setenv_test("MC_LOG_ENABLE", "on");
    setenv_test("MC_LOG_DIR", "/tmp/mooncake_ut_cfg");
    setenv_test("MC_LOG_LEVEL", "warning");
    setenv_test("MC_LOG_MAX_SIZE", "42");
    setenv_test("MC_LOG_BUFFER_SECS", "7");

    LogConfig config = LogConfigFromEnv();
    EXPECT_EQ(config.logDir, "/tmp/mooncake_ut_cfg");
    EXPECT_EQ(config.level, "WARNING");  // upper-cased for the level map
    EXPECT_EQ(config.maxSizeMB, 42u);
    EXPECT_EQ(config.flushIntervalSecs, 7);
}

TEST(LogConfigEnv, DisabledForcesOff) {
    setenv_test("MC_LOG_ENABLE", "off");
    setenv_test("MC_LOG_DIR", nullptr);
    setenv_test("MC_LOG_LEVEL", nullptr);
    EXPECT_EQ(LogConfigFromEnv().level, "OFF");
}

TEST(LogConfigEnv, DefaultDisabledWhenUnset) {
    setenv_test("MC_LOG_ENABLE", nullptr);
    // MC_LOG_ENABLE defaults to off → level OFF (legacy behavior preserved).
    EXPECT_EQ(LogConfigFromEnv().level, "OFF");
}

// ===========================================================================
// spdlog file output carries the trace_id prefix
// ===========================================================================
TEST(LoggingOutput, WritesFileWithTraceId) {
    std::string path = init_logger_to("/tmp/mooncake_ut_out", "INFO");

    uint64_t tid = logging::NewTraceId();
    {
        logging::ScopedTraceId _(tid);
        LOG_INFO << "spdlog_marker_hello";
    }
    Logger::Instance().Shutdown();  // flush + join worker → file is durable

    std::string content = read_file(path);
    EXPECT_NE(content.find("spdlog_marker_hello"), std::string::npos)
        << "log file should contain the message";
    EXPECT_NE(content.find("trace_id[" + std::to_string(tid) + "]"),
              std::string::npos)
        << "log line should be stamped with the current trace id";
}

TEST(LoggingOutput, NoTraceIdShowsNone) {
    std::string path = init_logger_to("/tmp/mooncake_ut_none", "INFO");
    LOG_INFO << "no_trace_marker";
    Logger::Instance().Shutdown();

    std::string content = read_file(path);
    EXPECT_NE(content.find("trace_id[none]"), std::string::npos);
}

TEST(LoggingOutput, LevelGateDropsDebugWhenInfo) {
    std::string path = init_logger_to("/tmp/mooncake_ut_lvl", "INFO");
    LOG_DEBUG << "debug_should_be_dropped";
    LOG_ERROR << "error_should_pass";
    Logger::Instance().Shutdown();

    std::string content = read_file(path);
    EXPECT_EQ(content.find("debug_should_be_dropped"), std::string::npos);
    EXPECT_NE(content.find("error_should_pass"), std::string::npos);
}

// ===========================================================================
// LOG_FATAL terminates the process (glog LOG(FATAL) parity)
// ===========================================================================
TEST(LoggingFatal, Aborts) {
    EXPECT_DEATH({ LOG_FATAL << "fatal_boom"; }, "");
}

}  // namespace
}  // namespace mooncake
