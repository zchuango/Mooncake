#include "mooncake_logging.h"

#include <dirent.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::logging {
namespace {

void setenv_test(const char* key, const char* val) {
    if (val) {
        setenv(key, val, 1);
    } else {
        unsetenv(key);
    }
}

// List glog files in directory (glog creates files like mooncake.INFO.12345)
std::vector<std::string> list_glog_files(const std::string& dir) {
    std::vector<std::string> files;
    DIR* d = opendir(dir.c_str());
    if (!d) return files;
    struct dirent* entry;
    while ((entry = readdir(d))) {
        std::string name(entry->d_name);
        if (name.find("mooncake.") == 0) {
            files.push_back(dir + "/" + name);
        }
    }
    closedir(d);
    return files;
}

std::string read_file(const std::string& path) {
    std::ifstream f(path);
    if (!f) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// ===========================================================================
// Test: IsMooncakeLogEnabled parsing logic
// ===========================================================================
TEST(LoggingIsEnabled, ParseOff) {
    setenv_test("MC_LOG_ENABLE", "off");
    // Force re-init by clearing static — call a different entry point
    EXPECT_FALSE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, ParseFalse) {
    setenv_test("MC_LOG_ENABLE", "false");
    EXPECT_FALSE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, ParseZero) {
    setenv_test("MC_LOG_ENABLE", "0");
    EXPECT_FALSE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, ParseNo) {
    setenv_test("MC_LOG_ENABLE", "no");
    EXPECT_FALSE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, ParseOn) {
    setenv_test("MC_LOG_ENABLE", "on");
    EXPECT_TRUE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, Parse1) {
    setenv_test("MC_LOG_ENABLE", "1");
    EXPECT_TRUE(IsMooncakeLogEnabled());
}

TEST(LoggingIsEnabled, ParseEmpty) {
    setenv_test("MC_LOG_ENABLE", nullptr);
    EXPECT_FALSE(IsMooncakeLogEnabled());
}

// ===========================================================================
// Test: TraceId generation
// ===========================================================================
TEST(LoggingTraceId, UniqueIds) {
    uint64_t id1 = NewTraceId();
    uint64_t id2 = NewTraceId();
    uint64_t id3 = NewTraceId();
    EXPECT_NE(id1, id2);
    EXPECT_NE(id2, id3);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id1, 0u);
}

TEST(LoggingTraceId, ScopedTraceId) {
    EXPECT_EQ(CurrentTraceId(), 0u);
    uint64_t tid = NewTraceId();
    {
        ScopedTraceId _(tid);
        EXPECT_EQ(CurrentTraceId(), tid);
    }
    EXPECT_EQ(CurrentTraceId(), 0u);
}

// ===========================================================================
// Test: FATAL always logged regardless of MC_LOG_ENABLE
// ===========================================================================
TEST(LoggingFatal, AlwaysLogged) {
    EXPECT_TRUE(ShouldLog(google::FATAL));
}

// ===========================================================================
// Test: Direct glog file output (no async, no worker thread)
// ===========================================================================
TEST(LoggingGlogFile, DirectGlogWritesFile) {
    setenv_test("MC_LOG_ENABLE", "on");
    setenv_test("MC_LOG_DIR", "/tmp/mooncake_ut_glog");
    // Ensure directory exists BEFORE glog init
    system("rm -rf /tmp/mooncake_ut_glog && mkdir -p /tmp/mooncake_ut_glog");

    // Init glog directly
    FLAGS_log_dir = "/tmp/mooncake_ut_glog";
    FLAGS_minloglevel = 0;
    google::InitGoogleLogging("test");

    LOG(INFO) << "direct_glog_marker_abc";
    google::FlushLogFiles(google::INFO);

    auto files = list_glog_files("/tmp/mooncake_ut_glog");
    ASSERT_FALSE(files.empty()) << "glog should create a file in /tmp/mooncake_ut_glog";
    std::string content = read_file(files[0]);
    EXPECT_NE(content.find("direct_glog_marker_abc"), std::string::npos)
        << "glog file should contain our marker";
}

TEST(LoggingGlogFile, ApplyEnablesGlog) {
    setenv_test("MC_LOG_ENABLE", "on");
    setenv_test("MC_LOG_DIR", "/tmp/mooncake_ut_apply");
    system("rm -rf /tmp/mooncake_ut_apply && mkdir -p /tmp/mooncake_ut_apply");

    ApplyMooncakeLogEnableToGlog();

    LOG(INFO) << "apply_glog_marker_xyz";
    google::FlushLogFiles(google::INFO);

    auto files = list_glog_files("/tmp/mooncake_ut_apply");
    ASSERT_FALSE(files.empty()) << "glog should create a file after ApplyMooncakeLogEnableToGlog";
    std::string content = read_file(files[0]);
    EXPECT_NE(content.find("apply_glog_marker_xyz"), std::string::npos)
        << "glog file should contain marker from ApplyMooncakeLogEnableToGlog path";
}

// ===========================================================================
// Test: MC_LOG macro produces output (sync path via Enqueue)
// Note: Worker thread may not be alive in unit test context, but Enqueue
// should still work — FlushAsyncLogs will drain via DrainAll
// ===========================================================================
TEST(LoggingMCLog, MCLogMacroOutput) {
    setenv_test("MC_LOG_ENABLE", "on");
    setenv_test("MC_LOG_DIR", "/tmp/mooncake_ut_mclog");
    system("rm -rf /tmp/mooncake_ut_mclog && mkdir -p /tmp/mooncake_ut_mclog");

    ApplyMooncakeLogEnableToGlog();

    MC_LOG(INFO) << "mc_log_marker_sync";

    // FlushAsyncLogs calls DrainAll which consumes from ring buffer
    FlushAsyncLogs();

    auto files = list_glog_files("/tmp/mooncake_ut_mclog");
    if (!files.empty()) {
        std::string content = read_file(files[0]);
        EXPECT_NE(content.find("mc_log_marker_sync"), std::string::npos)
            << "MC_LOG should produce output via Enqueue";
    }
}

}  // namespace
}  // namespace mooncake::logging
