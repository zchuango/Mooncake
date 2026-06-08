#include "log_macros.h"
#include "logger.h"
#include "rate_limiter.h"
#include "trace.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

std::string ReadFile(const std::filesystem::path &path)
{
    std::ifstream file(path);
    std::stringstream stream;
    stream << file.rdbuf();
    return stream.str();
}

LogConfig TestConfig(const std::filesystem::path &dir,
                     const std::string &name = "logging_test")
{
    LogConfig config;
    config.logDir = dir.string();
    config.fileName = name;
    config.level = "DEBUG";
    config.asyncQueueSize = 1024;
    config.asyncThreads = 1;
    config.rateLimit = 0;
    return config;
}

TEST(LoggingSpdlogFile, WritesFile)
{
    auto dir = std::filesystem::temp_directory_path() / "mooncake_ut_spdlog";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Logger::Instance().Init(TestConfig(dir));
    LOG_INFO << "spdlog_marker_abc";
    spdlog::default_logger()->flush();
    Logger::Instance().Shutdown();

    auto content = ReadFile(dir / "logging_test.log");
    EXPECT_NE(content.find("spdlog_marker_abc"), std::string::npos);
}

TEST(LoggingMacros, CompatibilityMacrosCompileAndLog)
{
    auto dir = std::filesystem::temp_directory_path() / "mooncake_ut_macros";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Logger::Instance().Init(TestConfig(dir));
    LOG(INFO) << "log_info_marker";
    LOG_WARNING << "log_warning_marker";
    MC_LOG_INFO << "mc_log_info_marker";
    PLOG(ERROR) << "plog_error_marker";
    spdlog::default_logger()->flush();
    Logger::Instance().Shutdown();

    auto content = ReadFile(dir / "logging_test.log");
    EXPECT_NE(content.find("log_info_marker"), std::string::npos);
    EXPECT_NE(content.find("log_warning_marker"), std::string::npos);
    EXPECT_NE(content.find("mc_log_info_marker"), std::string::npos);
    EXPECT_NE(content.find("plog_error_marker"), std::string::npos);
}

TEST(LoggingTrace, ThreadLocalTrace)
{
    Trace::Instance().SetTraceID("trace-a");
    EXPECT_EQ(Trace::Instance().GetTraceID(), "trace-a");
    EXPECT_NE(Trace::Instance().GetTraceHash(), 0u);

    Trace::Instance().SetTraceID(42);
    EXPECT_EQ(Trace::Instance().GetTraceID(), "42");
    EXPECT_EQ(Trace::Instance().GetTraceHash(), 42u);

    Trace::Instance().Invalidate();
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
    EXPECT_EQ(Trace::Instance().GetTraceHash(), 0u);
}

TEST(LoggingRateLimiter, LimitsPerTrace)
{
    RateLimiter::Instance().SetRate(2);
    Trace::Instance().SetTraceID("rate-limit-trace");

    EXPECT_TRUE(ShouldLog(spdlog::level::info));
    EXPECT_TRUE(ShouldLog(spdlog::level::info));
    EXPECT_FALSE(ShouldLog(spdlog::level::info));

    RateLimiter::Instance().SetRate(0);
    Trace::Instance().Invalidate();
}

}  // namespace
}  // namespace mooncake

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
