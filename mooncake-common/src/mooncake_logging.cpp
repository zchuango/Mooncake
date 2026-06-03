#include "mooncake_logging.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <thread>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace mooncake::logging {
namespace {

thread_local uint64_t current_trace_id = 0;

uint64_t SteadyClockNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

std::string LowerEnvValue(const char* value) {
    if (value == nullptr) return "";
    std::string text(value);
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return text;
}

struct LogEntry {
    const char* file = nullptr;
    int line = 0;
    google::LogSeverity severity = google::INFO;
    uint64_t trace_id = 0;
    std::string message;
};

// Lock-free MPSC ring buffer — no mutex on hot path.
// Producers atomically claim slots; single consumer processes them in order.
class AsyncLogQueue {
   public:
    // kCapacity must be power of 2 for cheap modulo via bitmask
    static constexpr size_t kCapacity = 8192;
    static constexpr size_t kSlotSize =
        (sizeof(LogEntry) + 63) & ~63ULL;  // cache-line aligned

    static AsyncLogQueue& Instance() {
        static AsyncLogQueue instance;
        return instance;
    }

    // Lock-free enqueue. Never blocks caller — on overflow drops oldest logs.
    void Enqueue(LogEntry entry) {
        uint64_t claimed = claimed_.fetch_add(1, std::memory_order_acquire);
        uint64_t consumed = consumed_.load(std::memory_order_acquire);

        // Overflow? Roll back claimed to keep window bounded.
        // This drops the oldest (consumed - claimed + kCapacity) entries.
        if (claimed - consumed >= kCapacity) {
            claimed_.store(consumed + kCapacity, std::memory_order_release);
        }

        // Write into ring buffer slot (overwrites oldest if overflowing)
        char* slot_ptr = slots_.get() + (claimed & (kCapacity - 1)) * kSlotSize;
        *reinterpret_cast<LogEntry*>(slot_ptr) = std::move(entry);

        // Publish so consumer sees this entry
        published_.fetch_add(1, std::memory_order_release);
    }

    // Called by worker thread — consume one published entry
    bool ConsumeOne(LogEntry* out) {
        uint64_t consumed = consumed_.load(std::memory_order_acquire);
        uint64_t published = published_.load(std::memory_order_acquire);

        if (consumed >= published) {
            return false;  // nothing to consume
        }

        char* slot_ptr = slots_.get() + (consumed & (kCapacity - 1)) * kSlotSize;
        *out = std::move(*reinterpret_cast<LogEntry*>(slot_ptr));
        consumed_.store(consumed + 1, std::memory_order_release);
        return true;
    }

    // Drain all published entries (called from Flush)
    void DrainAll() {
        LogEntry tmp;
        while (ConsumeOne(&tmp)) {
            WriteSync(tmp);
        }
        google::FlushLogFiles(google::INFO);
    }

   private:
    AsyncLogQueue() : slots_(new char[kSlotSize * kCapacity]) {}
    ~AsyncLogQueue() = default;

    // No copying
    AsyncLogQueue(const AsyncLogQueue&) = delete;
    AsyncLogQueue& operator=(const AsyncLogQueue&) = delete;

public:
    static void WriteSync(const LogEntry& entry) {
        google::LogMessage log_message(entry.file, entry.line, entry.severity);
        auto& stream = log_message.stream();
        if (entry.trace_id != 0) {
            stream << "trace_id[" << entry.trace_id << "] ";
        } else {
            stream << "trace_id[none] ";
        }
        stream << entry.message;
        // Note: glog LogMessage dtor calls FlushLogFiles automatically for FATAL
    }

    // Ring buffer: 3 atomic indices for lock-free MPSC
    // claimed_: producer claims next slot here
    // published_: producer has written entry and published it
    // consumed_: consumer has consumed up to this point
    std::atomic<uint64_t> claimed_{0};
    std::atomic<uint64_t> published_{0};
    std::atomic<uint64_t> consumed_{0};

    // Aligned ring buffer slots
    std::unique_ptr<char[]> slots_;
};

// Worker thread: consumes from ring buffer and writes to glog
// Uses its own ConsumeAll loop — no mutex needed
// LoggingWorkerThread: runs continuously, spinning when queue is empty
void LoggingWorkerThread() {
    LogEntry entry;
    while (true) {
        if (AsyncLogQueue::Instance().ConsumeOne(&entry)) {
            AsyncLogQueue::WriteSync(entry);
        } else {
            // Queue empty: spin briefly before retrying
            // This keeps the worker alive across test FlushAsyncLogs calls
        }
    }
}

}  // namespace

uint64_t NewTraceId() {
    // Use pointer address as high-entropy base — far better than PID alone
    static const uint64_t process_seed = [] {
        uint64_t ptr_val =
            reinterpret_cast<uint64_t>(&process_seed);
        return (ptr_val << 32) ^
               (SteadyClockNs() & 0x0000FFFFFFFFFFFFULL);
    }();
    static std::atomic<uint64_t> counter{1};
    return process_seed ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CurrentTraceId() { return current_trace_id; }

bool IsMooncakeLogEnabled() {
    // Cached at first call — no repeated getenv/tolower overhead in hot path
    static const bool enabled = [] {
        const std::string value =
            LowerEnvValue(std::getenv("MC_LOG_ENABLE"));
        if (value.empty()) return false;
        if (value == "off" || value == "0" || value == "false" ||
            value == "no") {
            return false;
        }
        return true;
    }();
    return enabled;
}

bool ShouldLog(google::LogSeverity severity) {
    if (severity == google::FATAL) return true;
    return IsMooncakeLogEnabled() && severity >= FLAGS_minloglevel;
}

bool ShouldVLog(int level) {
    return IsMooncakeLogEnabled() && VLOG_IS_ON(level);
}

void ApplyMooncakeLogEnableToGlog() {
    // Idempotent: InitGoogleLogging must only be called once
    static bool initialized = [] {
        // Set FLAGS_log_dir BEFORE InitGoogleLogging so glog writes to the right place
        if (std::getenv("MC_LOG_DIR") != nullptr) {
            FLAGS_log_dir = std::getenv("MC_LOG_DIR");
        } else {
            FLAGS_log_dir = "/var/log/mooncake";
        }
        google::InitGoogleLogging("mooncake");
        return true;
    }();
    (void)initialized;

    if (!IsMooncakeLogEnabled()) {
        FLAGS_minloglevel = google::FATAL + 1;
        return;
    }

    // Disable console output to prevent performance degradation
    FLAGS_logtostderr = 0;
    FLAGS_stderrthreshold = google::FATAL;  // Only FATAL goes to stderr

    // Batch file writes — configurable via env var
    if (const char* buf_secs = std::getenv("MC_LOG_BUFFER_SECS")) {
        FLAGS_logbufsecs = std::atoi(buf_secs);
    } else {
        FLAGS_logbufsecs = 3;
    }

    if (const char* max_size = std::getenv("MC_LOG_MAX_SIZE")) {
        FLAGS_max_log_size = std::atoi(max_size);
    } else {
        FLAGS_max_log_size = 100;
    }

    FLAGS_logbuflevel = google::INFO;

#ifdef NDEBUG
    FLAGS_enable_lock_usage = false;
#endif
}

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(current_trace_id) {
    current_trace_id = trace_id;
}

ScopedTraceId::~ScopedTraceId() { current_trace_id = previous_trace_id_; }

AsyncLogMessage::AsyncLogMessage(const char* file, int line,
                                 google::LogSeverity severity, bool enabled)
    : file_(file),
      line_(line),
      severity_(severity),
      enabled_(enabled),
      trace_id_(CurrentTraceId()) {}

AsyncLogMessage::~AsyncLogMessage() {
    if (!enabled_) return;
    if (severity_ == google::FATAL) {
        google::LogMessage log_message(file_, line_, severity_);
        auto& output = log_message.stream();
        if (trace_id_ != 0) {
            output << "trace_id[" << trace_id_ << "] ";
        } else {
            output << "trace_id[none] ";
        }
        output << stream_.str();
        return;
    }
    LogEntry entry{file_, line_, severity_, trace_id_, stream_.str()};
    AsyncLogQueue::Instance().Enqueue(std::move(entry));
}

std::ostream& AsyncLogMessage::stream() { return stream_; }

void FlushAsyncLogs() { AsyncLogQueue::Instance().DrainAll(); }

}  // namespace mooncake::logging
