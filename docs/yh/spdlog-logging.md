# Mooncake spdlog 日志系统：功能与实现

本文介绍 spdlog 在 Mooncake 中承担的日志能力，并结合源码讲解每个功能的实现原理。

> 代码位置：`mooncake-common/include/{log_config.h, logger.h, log_macros.h, trace.h, rate_limiter.h, mooncake_logging.h}` 与 `mooncake-common/src/{logger.cpp, trace.cpp, rate_limiter.cpp, mooncake_logging.cpp}`。

---

## 1. 概述：spdlog 提供了哪些功能

spdlog 是 Mooncake 的**业务日志主干**，对外暴露 `LOG_INFO / LOG_WARNING / LOG_ERROR / LOG_DEBUG / LOG_TRACE / LOG_FATAL` 一组流式宏。它提供下列能力：

| 功能 | 关键实现 | 作用 |
|---|---|---|
| **异步日志输出** | `spdlog::async_logger` + 全局线程池 | 业务线程只入队，磁盘 IO 甩给后台线程，热路径不阻塞 |
| **流式日志宏** | `LOG_*` + `LogStream`（RAII） | `LOG_INFO << a << b;` 风格，析构时落一条日志 |
| **trace_id 注入** | `LogStream` 析构 + `CurrentTraceId()` | 每行日志带 `trace_id[...]`，串联一次请求 |
| **滚动文件** | `rotating_file_sink_mt` | 按大小滚动、保留固定份数 |
| **级别门控** | `ShouldLog()` + `logger->should_log()` | 关掉的级别连字符串都不构建 |
| **致命日志** | `FatalLogStream`（`[[noreturn]]`） | `LOG_FATAL` 落日志 + flush + `abort()` |
| **环境变量配置** | `LogConfigFromEnv()` | 运维用 `MC_LOG_*` 调目录/级别/滚动/刷新 |
| **限流 / trace 采样**（默认关闭） | `RateLimiter` + `Trace` | 预留的按 trace 限流与采样能力 |

整体分两层：

- **基础设施层**：`Logger`（封装 spdlog）、`Trace`、`RateLimiter`、`LogConfig`。
- **使用层**：`log_macros.h` 里的 `LOG_*` 宏 + `LogStream`。

另外 `mooncake_logging.h/.cpp` 只保留了 **trace-id 辅助**（`NewTraceId / CurrentTraceId / ScopedTraceId`），原来基于 glog 的 `MC_LOG` 异步实现已退役。

---

## 2. 功能详解与实现

### 2.1 异步日志输出（核心）

实现在 `Logger::Impl::Init()`（`mooncake-common/src/logger.cpp`）：

```cpp
// ① 全局线程池：一个环形消息队列 + N 个后台写线程
if (!spdlog::thread_pool()) {
    spdlog::init_thread_pool(config.asyncQueueSize, config.asyncThreads);
}

// ② 滚动文件 sink
auto fileSink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
    config.logDir + "/" + config.fileName + ".log",
    config.maxSizeMB * 1024 * 1024, config.maxFiles);

// ③ 异步 logger：绑定线程池 + sink + 溢出策略
logger_ = std::make_shared<spdlog::async_logger>(
    "mooncake", fileSink, spdlog::thread_pool(),
    spdlog::async_overflow_policy::overrun_oldest);   // ← 有损溢出

spdlog::initialize_logger(logger_);
logger_->set_pattern(config.pattern, spdlog::pattern_time_type::local);
logger_->set_level(level);
spdlog::set_default_logger(logger_);                  // ← LOG_* 通过它取 logger

// ④ 周期性后台刷盘
if (config.flushIntervalSecs > 0) {
    spdlog::flush_every(std::chrono::seconds(config.flushIntervalSecs));
}
```

数据流（生产者—消费者）：

```
  业务线程 (生产者)                      后台 worker 线程 (消费者)
  ─────────────────                     ──────────────────────────
  LOG_INFO << "x=" << v
   │ 1. 在【调用线程】格式化成 buffer
   │ 2. 拷成 async_msg 入环形队列 ─────►  3. 出队
   │ 3. 立即返回(不碰磁盘)                4. 套 pattern(时间/级别/行号)
   ▼                                     5. 写文件 / 按大小滚动
  继续干活                                (真正 IO 在这里)
```

要点：
- **`overrun_oldest`（有损）**：队列满时覆盖最旧消息，生产者**永不阻塞**；另一选择是 `block`（满了等待，无损但卡业务线程）。
- **职责切分**：用户消息的参数拼接在调用线程；加时间戳/级别/源码位置的 pattern 渲染 + 写盘在后台线程。
- `set_default_logger()` 是关键——`LOG_*` 宏内部用 `spdlog::default_logger()` 取这个异步 logger。**没调用 `Logger::Init()` 之前**，default logger 是 spdlog 自带的同步 stdout logger。

启动入口（3 处）在 `master.cpp` / `real_client_main.cpp` / `stress_cluster_bench.cpp`：

```cpp
mooncake::Logger::Instance().Init(mooncake::LogConfigFromEnv());
```

退出刷盘由 `Logger` 单例析构自动完成（`~Logger()` → `Shutdown()` → `logger_->flush()` + `spdlog::shutdown()`）。

---

### 2.2 流式 `LOG_*` 宏与 `LogStream`

`log_macros.h` 里 `LogStream` 是个 RAII 对象：构造时记录级别与源码位置，往内部 `ostringstream` 流式写入，**析构时落一条日志**。

```cpp
class LogStream {
public:
    LogStream(spdlog::logger *logger, spdlog::level::level_enum level,
              const char *file, int line) : logger_(logger), level_(level) {
        loc_.filename = file;
        loc_.line = static_cast<size_t>(line);
    }
    ~LogStream() {
        if (logger_ && !skip_) {
            // 非格式化的 string_view 重载：payload 里的 { } 不会被 fmt 解析
            logger_->log(loc_, level_, TraceIdPrefix() + stream_.str());
        }
    }
    std::ostream &Stream() { return stream_; }
private:
    spdlog::logger *logger_;
    spdlog::level::level_enum level_;
    spdlog::source_loc loc_;
    std::ostringstream stream_;
    bool skip_ = false;
};
```

宏定义采用 glog 同款的 **`?:` + `Voidify`** 写法（而非 `if/else`），保证是**单个条件表达式**、**dangling-else 安全**：

```cpp
class LogVoidify {            // operator& 优先级位于 << 和 ?: 之间
public:
    void operator&(std::ostream &) {}
};

#define MC_LOG_STREAM_AT(spdlog_level)                                  \
    mooncake::LogVoidify() &                                            \
        mooncake::LogStream(spdlog::default_logger().get(), spdlog_level,\
                            __FILE__, __LINE__).Stream()

#define LOG_INFO                                          \
    !mooncake::ShouldLog(spdlog::level::info)             \
        ? (void)0                                         \
        : MC_LOG_STREAM_AT(spdlog::level::info)
```

为什么是这个写法？运算符优先级是 `<<` > `&` > `?:`，于是：

```
LOG_INFO << "x" << v;
↓ 展开
!ShouldLog(info) ? (void)0 : LogVoidify() & LogStream(...).Stream() << "x" << v;
↓ 解析
!ShouldLog(info) ? (void)0 : ( LogVoidify() & ((Stream() << "x") << v) );
```

两个分支都是 `void`，整体是一个表达式语句。这样 `if (c) LOG_INFO << ...; else ...;` 不会发生 else 错配（dangling-else）。

> 历史踩坑：迁移前那套 `if(!x){}else STREAM` 形式在 852 处替换后会引入 dangling-else 隐患，已改为上面的表达式形式。

---

### 2.3 trace_id 注入（异步下的正确做法）

每行日志前缀 `trace_id[<id>]`，由 `LogStream` 析构时在**调用线程**拼接：

```cpp
inline std::string TraceIdPrefix() {
    uint64_t tid = ::mooncake::logging::CurrentTraceId();
    if (tid != 0) return "trace_id[" + std::to_string(tid) + "] ";
    return "trace_id[none] ";
}
```

trace id 本身是线程局部变量，由 `ScopedTraceId` 在请求入口设置（`mooncake-common/src/mooncake_logging.cpp`）：

```cpp
thread_local uint64_t current_trace_id = 0;
uint64_t CurrentTraceId() { return current_trace_id; }

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(current_trace_id) { current_trace_id = trace_id; }
ScopedTraceId::~ScopedTraceId() { current_trace_id = previous_trace_id_; }
```

用法（如 `real_client.cpp`）：

```cpp
mooncake::logging::ScopedTraceId trace(mooncake::logging::NewTraceId());
```

**为什么在 message 里拼、而不用 spdlog 的 MDC？**
spdlog 的 MDC / 自定义 flag 都从 `thread_local` 读值，而 pattern 渲染发生在**后台 worker 线程**——读不到生产者线程的 MDC，会丢 id / 串号。`log_msg` 也没有可携带的自定义上下文字段。所以异步下唯一可靠做法是在**生产者线程记录时把 trace_id 固化进 payload**，正好 `LogStream` 析构在调用线程执行。

---

### 2.4 级别门控（关掉的级别零开销）

`ShouldLog()` 在宏的条件位置先做级别判断，关掉的 `LOG_DEBUG/LOG_TRACE` 连 `stream_ << ...` 都不会执行：

```cpp
inline bool ShouldLog(spdlog::level::level_enum level) {
    auto *logger = spdlog::default_logger().get();
    if (logger && !logger->should_log(level)) return false;   // ← 级别门控
    // 限流 / trace 采样（默认关闭，见 2.7）
    ...
    return true;
}
```

这保留了旧 `MC_VLOG` 的 `VLOG_IS_ON` 廉价语义：级别没开就不付出字符串构建成本。

---

### 2.5 滚动文件

由 `rotating_file_sink_mt` 实现（见 2.1 的 `fileSink`）：写到 `<logDir>/<fileName>.log`，单文件超过 `maxSizeMB` 滚动为 `app.log.1 / app.log.2 ...`，最多保留 `maxFiles` 份。

---

### 2.6 致命日志 `LOG_FATAL`

对齐 glog `LOG(FATAL)` 语义：**总是输出 + 终止进程**。用单独的 `FatalLogStream`，析构 `[[noreturn]]`：

```cpp
class FatalLogStream {
public:
    FatalLogStream(const char *file, int line) { loc_.filename = file; loc_.line = line; }
    [[noreturn]] ~FatalLogStream() {
        auto *logger = spdlog::default_logger().get();
        if (logger) {
            logger->log(loc_, spdlog::level::critical, TraceIdPrefix() + stream_.str());
            logger->flush();                 // 终止前确保落盘
        }
        std::abort();
    }
    std::ostream &Stream() { return stream_; }
};

#define LOG_FATAL  mooncake::FatalLogStream(__FILE__, __LINE__).Stream()
```

`[[noreturn]]` 让编译器知道该语句之后不可达（和 glog 同款），避免「函数控制流到末尾」的告警。

---

### 2.7 环境变量配置

`LogConfig`（`log_config.h`）是所有可调项，`LogConfigFromEnv()`（`logger.cpp`）把运维熟悉的 `MC_LOG_*` 映射进去：

```cpp
LogConfig LogConfigFromEnv() {
    LogConfig config;
    if (const char *dir = std::getenv("MC_LOG_DIR")) config.logDir = dir;
    else config.logDir = "/var/log/mooncake";              // 兼容旧 glog 默认
    if (const char *level = std::getenv("MC_LOG_LEVEL")) config.level = UpperString(level);
    if (const char *m = std::getenv("MC_LOG_MAX_SIZE")) config.maxSizeMB = std::atoi(m);
    if (const char *b = std::getenv("MC_LOG_BUFFER_SECS")) config.flushIntervalSecs = std::atoi(b);
    else config.flushIntervalSecs = 3;
    if (!LogEnabledFromEnv()) config.level = "OFF";        // 总开关：默认关闭
    return config;
}
```

| 环境变量 | 映射字段 | 默认 | 说明 |
|---|---|---|---|
| `MC_LOG_ENABLE` | 总开关 | **off** | 非 on/1/true 时把 level 设为 `OFF`，spdlog 丢弃全部日志 |
| `MC_LOG_DIR` | `logDir` | `/var/log/mooncake` | 日志目录 |
| `MC_LOG_LEVEL` | `level` | `INFO` | `TRACE/DEBUG/INFO/WARNING/ERROR/OFF` |
| `MC_LOG_MAX_SIZE` | `maxSizeMB` | 100 | 单文件滚动阈值(MB) |
| `MC_LOG_BUFFER_SECS` | `flushIntervalSecs` | 3 | 后台周期刷盘间隔(秒) |

> 注意：`MC_LOG_ENABLE` **默认关闭**，是从旧 glog 路径保留下来的行为；生产/测试需显式 `MC_LOG_ENABLE=on`。

---

### 2.8 限流与 trace 采样（预留能力，默认关闭）

`ShouldLog()` 里还接了两个开关，目前默认不生效：

- **`RateLimiter`**（`rate_limiter.cpp`）：按 trace 哈希的滑动窗口限流，`rateLimit` 为每秒每 trace 的预算，0 = 不限。`LogConfig.rateLimit` 默认 0。
- **`Trace` 请求采样**（`trace.cpp`）：`IsRequestLogTrace()` + `GetRequestSampleDecision()`，需要在请求入口 `Trace::SetTraceID()` 并标记采样后才生效。

```cpp
// ShouldLog 内（节选）
if (RateLimiter::Instance().IsEnabled()) {
    if (!RateLimiter::Instance().ShouldLog(traceHash, nowMs)) return false;
}
if (Trace::Instance().IsRequestLogTrace()) {
    bool admitted = false;
    if (Trace::Instance().GetRequestSampleDecision(admitted)) return admitted;
}
```

这部分是后续增强点：要启用得在请求入口接 `Trace`，并把 `LogConfig.rateLimit` 配成正值。

---

## 3. 一条日志的完整生命周期

以 `LOG_INFO << "key=" << key;` 为例：

1. **宏展开**为 `!ShouldLog(info) ? (void)0 : LogVoidify() & LogStream(...).Stream() << "key=" << key;`
2. **`ShouldLog(info)`**：default logger 级别是否 ≥ info；（可选）限流/采样判定。关了就走 `(void)0`，啥都不构建。
3. **构造 `LogStream`**，记录 `info` 级别 + `__FILE__:__LINE__`。
4. **`<<` 链**把 `"key="`、`key` 拼进内部 `ostringstream`（调用线程）。
5. **语句结束，`LogStream` 析构**：取 `CurrentTraceId()` 拼 `trace_id[..] ` 前缀，调 `logger_->log(loc, info, msg)`。
6. **spdlog 异步**：消息拷进环形队列，**调用线程立即返回**。
7. **后台 worker** 出队，按 `pattern` 渲染（时间/级别/`源文件:行号`/正文），写入 `<logDir>/app.log`，必要时滚动。

最终一行形如：

```
2026-06-04 14:23:01.123456 | I | real_client.cpp:825 | trace_id[8231…] Mounting CXL segment: 4096 bytes, 0x...
```
（pattern：`%Y-%m-%d %H:%M:%S.%6f | %^%L%$ | %s:%# | %v`）

---

## 4. 与旧 `MC_LOG` / glog 的关系

- **旧 `MC_LOG`/`MC_VLOG`（glog 异步 + 手写无锁环形缓冲）已退役**；852 处调用全部改成 `LOG_*`。
- `mooncake_logging.h/.cpp` 只剩 trace-id 辅助（`NewTraceId/CurrentTraceId/ScopedTraceId`），仍被十几处用于跨异步边界传递 trace id。
- **glog 没有移除**：`mooncake-ep / mooncake-pg / mooncake-integration / benchmarks` 仍直接用原始 `LOG()` / `CHECK()`，所以 glog 继续链接；`mooncake_logging.h` 仍 `#include <glog/logging.h>` 以维持这些调用点的传递可用性。
- 由此，`log_macros.h` 里**移除了 `CHECK`/`LOG_IF`**（与 glog 同名宏冲突），只保留不冲突的 `LOG_*` 级别宏。

---

## 5. 快速使用指南

```cpp
#include "logger.h"       // Logger / LogConfigFromEnv
#include "log_macros.h"   // LOG_*

int main() {
    mooncake::Logger::Instance().Init(mooncake::LogConfigFromEnv());  // 启动一次
    LOG_INFO << "started, pid=" << getpid();
    LOG_ERROR << "bad thing: " << err;
    // LOG_FATAL << "unrecoverable";   // 落盘后 abort()
    // 退出时 Logger 单例析构自动 flush + shutdown
}
```

运行：

```bash
MC_LOG_ENABLE=on MC_LOG_DIR=/tmp/mc MC_LOG_LEVEL=INFO ./mooncake_master
tail -f /tmp/mc/app.log      # 每行带 trace_id[...]
```
