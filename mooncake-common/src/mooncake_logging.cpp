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

// LogEntry stored in the ring buffer slots
// All fields are plain POD so memcpy is safe for move-assignment
struct LogEntry {
    const char* file = nullptr;
    int line = 0;
    google::LogSeverity severity = google::INFO;
    uint64_t trace_id = 0;
    std::string message;  // heap-allocated, OK
};

// Lock-free MPSC ring buffer
// Uses atomic indices (claim/consume model) — no mutex on hot path
class AsyncLogQueue {
   public:
    static constexpr size_t kCapacity = 8192;  // must be power of 2 for mod

    static AsyncLogQueue& Instance() {
        static AsyncLogQueue instance;
        return instance;
    }

    // Lock-free enqueue. Never blocks the caller — on overflow, drops oldest entries.
    // Returns true if enqueued (or dropped due to overflow), always non-blocking.
    bool Enqueue(LogEntry entry) {
        // Claim a slot atomically
        uint64_t claimed = claimed_.fetch_add(1, std::memory_order_acquire);
        uint64_t consumed = consumed_.load(std::memory_order_acquire);

        // Overflow? Roll back claimed to keep claimed - consumed <= kCapacity.
        // This effectively makes oldest entries get overwritten (lossy behavior).
        if (claimed - consumed >= kCapacity) {
            claimed_.store(consumed + kCapacity, std::memory_order_release);
            // Do NOT fall back to sync write — caller never blocks
        }

        // Slot index in ring buffer (modulo power-of-2 is free: idx & (kCapacity-1))
        size_t slot_idx = claimed & (kCapacity - 1);
        slots_[slot_idx] = std::move(entry);

        // Publish so consumer sees this entry
        published_.fetch_add(1, std::memory_order_release);
        return true;  // always succeeds, never blocks
    }

    void Flush() {
        // Wait for consumer to drain all published entries
        uint64_t claimed = claimed_.load(std::memory_order_acquire);
        while (published_.load(std::memory_order_acquire) < claimed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        google::FlushLogFiles(google::INFO);
    }

   private:
    static constexpr size_t kDummySize =
        (sizeof(LogEntry) + 63) & ~63ULL;  // cache-line align each slot

    AsyncLogQueue() : slots_(new char[kDummySize * kCapacity]) {}

    ~AsyncLogQueue() { delete[] slots_; }

    // No copying
    AsyncLogQueue(const AsyncLogQueue&) = delete;
    AsyncLogQueue& operator=(const AsyncLogQueue&) = delete;

    // Get slot pointer — slots are cache-line aligned to prevent false sharing
    LogEntry* slot(size_t idx) {
        return reinterpret_cast<LogEntry*>(slots_.get() + idx * kDummySize);
    }

    void WorkerLoop() {
        while (true) {
            // Claim one published entry to consume (acquire)
            uint64_t consumed = consumed_.load(std::memory_order_acquire);
            uint64_t published = published_.load(std::memory_order_acquire);

            if (consumed >= published) {
                if (stopped_) break;
                // Sleep and spin
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }

            size_t slot_idx = consumed & (kCapacity - 1);
            LogEntry* e = slot(consumed);

            // Process entry
            WriteSync(*e);

            // Advance consumer past this slot (release)
            consumed_.store(consumed + 1, std::memory_order_release);

            if (consumed_ == stopped_) break;
        }
    }

    void EnsureStarted() {
        std::call_once(start_once_, [this] {
            worker_ = std::thread([this] { WorkerLoop(); });
            std::atexit([] { AsyncLogQueue::Instance().Stop(); });
        });
    }

    void Stop() {
        stopped_ = true;
        if (worker_.joinable()) worker_.join();
        google::FlushLogFiles(google::INFO);
    }

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

    std::once_flag start_once_;
    std::thread worker_;

    // Lock-free MPSC ring buffer state
    std::atomic<uint64_t> claimed_{0};     // producer claims next slot here
    std::atomic<uint64_t> published_{0}; // producer publishes entry here
    std::atomic<uint64_t> consumed_{0};    // consumer consumes from here
    bool stopped_ = false;

    // Ring buffer slots — cache-line aligned to avoid false sharing
    // Use unique_ptr<char[]> instead of new[] to avoid leak
    std::unique_ptr<char[]> slots_;
};

}  // namespace

uint64_t NewTraceId() {
    // Use pointer address as high-entropy base — far better than PID alone
    // which is typically a small integer with many leading zeros in 64-bit space
    static const uint64_t process_seed = [] {
        uint64_t ptr_val =
            reinterpret_cast<uint64_t>(&process_seed);  // address of static var
        return (ptr_val << 32) ^  // high bits from object address
               (SteadyClockNs() &
                0x0000FFFFFFFFFFFFULL);  // low 48 bits from wall clock
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
    // Set default log directory before anything else
    if (std::getenv("MC_LOG_DIR") == nullptr) {
        FLAGS_log_dir = "/var/log/mooncake";
    }

    if (!IsMooncakeLogEnabled()) {
        FLAGS_minloglevel = google::FATAL + 1;
        return;
    }

    // Disable console output to prevent performance degradation
    // File-only logging significantly reduces I/O overhead
    FLAGS_logtostderr = 0;
    FLAGS_stderrthreshold = google::FATAL;  // Only FATAL goes to stderr

    // Batch file writes for higher throughput — configurable via env var
    // Default 3s buffer when unset; set MC_LOG_BUFFER_SECS=30 for
    // high-throughput scenarios to minimize I/O overhead
    if (const char* buf_secs = std::getenv("MC_LOG_BUFFER_SECS")) {
        FLAGS_logbufsecs = std::atoi(buf_secs);
    } else {
        FLAGS_logbufsecs = 3;
    }

    // Set adequate log file size before rotation — configurable via env var
    // Default 100MB when unset; set MC_LOG_MAX_SIZE=200 for longer retention
    if (const char* max_size = std::getenv("MC_LOG_MAX_SIZE")) {
        FLAGS_max_log_size = std::atoi(max_size);
    } else {
        FLAGS_max_log_size = 100;
    }

    // Buffer INFO and above to reduce I/O frequency
    FLAGS_logbuflevel = google::INFO;

    // Disable lock usage tracking overhead — only available in NDEBUG builds
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
    // Lossy: Enqueue always returns true, never blocks or falls back to sync write
    AsyncLogQueue::Instance().Enqueue(std::move(entry));
}

std::ostream& AsyncLogMessage::stream() { return stream_; }

void FlushAsyncLogs() { AsyncLogQueue::Instance().Flush(); }

}  // namespace mooncake::logging
