#include <numa.h>
#include <sched.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <cstdlib>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mooncake_logging.h"
#include "dummy_client.h"
#include "real_client.h"
#include "shm_helper.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;

static void bindToSocket(int socket_id) {
    if (numa_available() < 0) return;
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (socket_id < 0 || socket_id >= numa_num_configured_nodes())
        socket_id = 0;
    struct bitmask* cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_possible_cpus = numa_num_possible_cpus();
    int nr_cpus = 0;
    for (int cpu = 0; cpu < nr_possible_cpus; ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) &&
            numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            ++nr_cpus;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus > 0) {
        if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
            PLOG(WARNING) << "Failed to set CPU affinity for NUMA socket "
                          << socket_id;
        }
    }
}

static std::string FormatBytes(size_t bytes) {
    if (bytes == 0) return "0 B";
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int i = static_cast<int>(std::floor(std::log2(bytes) / 10));
    if (i > 4) i = 4;
    double val = static_cast<double>(bytes) / std::pow(1024, i);
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << val << " " << units[i];
    return oss.str();
}

static std::vector<std::string> DiscoverSegmentsFromMaster(
    const std::string& master_host, int master_admin_port) {
    std::vector<std::string> segments;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        LOG(ERROR) << "Failed to create socket for discovering segments";
        return segments;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_admin_port);

    std::string host = master_host;
    size_t colon_pos = host.find(':');
    if (colon_pos != std::string::npos) {
        host = host.substr(0, colon_pos);
    }

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        LOG(ERROR) << "Invalid master host: " << host;
        close(sockfd);
        return segments;
    }

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "Failed to connect to master admin at " << host << ":"
                   << master_admin_port;
        close(sockfd);
        return segments;
    }

    std::string request = "GET /get_all_segments HTTP/1.0\r\nHost: " + host +
                          "\r\nConnection: close\r\n\r\n";
    if (send(sockfd, request.c_str(), request.size(), 0) < 0) {
        LOG(ERROR) << "Failed to send HTTP request to master";
        close(sockfd);
        return segments;
    }

    std::string response;
    char buf[4096];
    ssize_t n;
    while ((n = recv(sockfd, buf, sizeof(buf), 0)) > 0) {
        response.append(buf, n);
    }
    close(sockfd);

    size_t header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response from master";
        return segments;
    }

    std::string header = response.substr(0, header_end);
    size_t status_pos = header.find(' ');
    if (status_pos == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response header from master";
        return segments;
    }
    size_t status_code_start = status_pos + 1;
    size_t status_code_end = header.find(' ', status_code_start);
    if (status_code_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP status line from master";
        return segments;
    }
    std::string status_code =
        header.substr(status_code_start, status_code_end - status_code_start);
    if (status_code != "200") {
        LOG(ERROR) << "HTTP request failed with status " << status_code
                   << " from master at " << master_host << ":"
                   << master_admin_port;
        return segments;
    }

    std::string body = response.substr(header_end + 4);
    std::istringstream iss(body);
    std::string line;
    // Use an unordered_set to track already-seen segments and avoid duplicates
    std::unordered_set<std::string> seen;
    while (std::getline(iss, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n' ||
                                 line.back() == ' ' || line.back() == '\t')) {
            line.pop_back();
        }
        if (!line.empty() && seen.insert(line).second) {
            segments.push_back(line);
        }
    }

    return segments;
}

// Resolve the effective list of peer RPC addresses for client_rpc_bench.
// --peer_rpc_addrs (multi-peer, comma-separated) takes precedence over
// --peer_rpc_addr (single-peer fallback).
std::vector<std::string> ParsePeerRpcAddrs() {
    std::vector<std::string> addrs;
    if (!FLAGS_peer_rpc_addrs.empty()) {
        std::istringstream iss(FLAGS_peer_rpc_addrs);
        std::string a;
        while (std::getline(iss, a, ',')) {
            size_t s = a.find_first_not_of(" \t");
            size_t e = a.find_last_not_of(" \t");
            if (s != std::string::npos && e != std::string::npos) {
                addrs.push_back(a.substr(s, e - s + 1));
            }
        }
    } else if (!FLAGS_peer_rpc_addr.empty()) {
        addrs.push_back(FLAGS_peer_rpc_addr);
    }
    return addrs;
}
}  // namespace

DEFINE_string(local_hostname, "localhost",
              "Local hostname (with optional port, e.g. node1:12345)");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server URL");
DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(protocol, "tcp", "Transport protocol: tcp, rdma, ub");
DEFINE_string(device_name, "", "RDMA/UB device name (comma-separated)");
DEFINE_uint64(global_segment_size, 4 * GB, "Global segment size in bytes");
DEFINE_uint64(local_buffer_size, 512 * MB, "Local buffer size in bytes");
DEFINE_bool(enable_ssd_offload, false, "Enable SSD offload on this client");
DEFINE_string(ssd_offload_path, "", "SSD offload directory path");

DEFINE_string(scenario, "local_memory",
              "Benchmark scenario: local_memory, remote_memory, local_disk, "
              "remote_disk, segment_write, segment_read, client_rpc_bench, "
              "list_segments");
// Peer RPC server address for client_rpc_bench. Must be the
// "local_rpc_addr" (ip:port) of the RealClient that hosts the
// offload_rpc_server_ registered with batch_get_offload_object /
// release_offload_buffer. The peer must be started with
// --enable_ssd_offload=true so the offload_rpc_server_ is up.
DEFINE_string(peer_rpc_addr, "",
              "[client_rpc_bench] Address (ip:port) of the peer RealClient's "
              "offload_rpc_server_, e.g. \"10.0.0.2:17888\". The peer is "
              "expected to have --enable_ssd_offload=true. Single-peer "
              "fallback; superseded by --peer_rpc_addrs when that flag is "
              "set.");
// Comma-separated list of peer RPC addresses (ip:port). When non-empty,
// takes precedence over --peer_rpc_addr. In each round of the timed
// loop the bench issues one batch_get_offload_object RPC to each alive
// peer in this list, and reports per-peer statistics in addition to the
// aggregate. Each peer must be started with --enable_ssd_offload=true.
DEFINE_string(peer_rpc_addrs, "",
              "[client_rpc_bench] Comma-separated list of peer RealClient "
              "offload_rpc_server_ addresses (ip:port), e.g. "
              "\"10.0.0.2:17888,10.0.0.3:17888,...\". When non-empty, takes "
              "precedence over --peer_rpc_addr. Each peer is expected to "
              "have --enable_ssd_offload=true. Per-peer statistics are "
              "reported alongside an AGGREGATE summary.");
// Optional: if set, we will call batch_get_offload_object with this single
// key and --ssd_value_size. The peer's FileStorage must already hold this
// key in its SSD directory (use scenario=local_disk on the peer first, or
// any other writer that has offloaded to the peer's SSD). When unset, the
// RPC is issued with empty keys (no SSD I/O on the peer); this still
// measures a real client-to-client RPC round-trip, but the response
// contains an empty pointers vector.
DEFINE_string(ssd_key, "",
              "[client_rpc_bench] Key that the peer has in its SSD. When "
              "set, the bench actually reads from the peer's SSD; the "
              "response will carry a real pointer (memory address) to the "
              "data buffer on the peer. Leave empty to send an empty "
              "request (no SSD I/O).");
DEFINE_int64(ssd_value_size, 4096,
             "[client_rpc_bench] Size in bytes of the SSD-backed object to "
             "request from the peer. Only used when --ssd_key is set.");
DEFINE_string(role, "writer",
              "Node role: writer (prefill data) or reader (benchmark reads)");
DEFINE_string(client_type, "real",
              "Underlying client implementation: real (RealClient) or dummy "
              "(DummyClient that connects to a remote RealClient via RPC + "
              "IPC). The dummy client requires a real client to be running "
              "with its RPC server reachable at --dummy_server_address and "
              "its IPC server listening on --dummy_ipc_socket_path.");
DEFINE_string(dummy_server_address, "127.0.0.1:12345",
              "[dummy client] RealClient RPC server address (IP:port) that "
              "the DummyClient connects to.");
DEFINE_string(dummy_ipc_socket_path, "/tmp/mooncake_dummy.sock",
              "[dummy client] Abstract-namespace Unix socket path used by "
              "DummyClient to register SHM with the real client. Should "
              "match the --ipc_socket_path of the real client.");
DEFINE_uint64(dummy_mem_pool_size, 0,
              "[dummy client] Memory pool size in bytes allocated inside "
              "DummyClient. 0 = reuse --global_segment_size.");
DEFINE_uint64(value_size, 4 * MB, "Size of each value in bytes");
DEFINE_uint64(num_keys, 100, "Number of keys to write/read");
DEFINE_uint64(batch_size, 32, "Batch size for put/get operations");
DEFINE_uint64(num_threads, 1, "Number of concurrent reader threads");
DEFINE_uint64(warmup_keys, 5, "Number of warmup keys (not counted in stats)");
DEFINE_uint64(wait_seconds, 5,
              "Seconds to wait before reading (for remote scenarios)");
DEFINE_bool(verify, true, "Verify data integrity after read");
DEFINE_uint64(replica_num, 1, "Number of replicas for each object");
DEFINE_bool(hard_pin, false,
            "Pin objects to prevent eviction during benchmark");

DEFINE_string(segments, "",
              "Comma-separated segment names for segment_write/segment_read "
              "scenarios. Use segment 'name' (typically hostname), NOT "
              "IP:port. Leave empty to auto-discover from master.");
DEFINE_uint64(master_admin_port, 9003,
              "Master admin HTTP port for auto-discovering segments");
DEFINE_uint64(read_segment_nums, 0,
              "Number of segments to read from in segment_read scenario (0 = "
              "read from all segments)");
DEFINE_uint64(duration, 0,
              "Duration in seconds for continuous reading in segment_read "
              "scenario (0 = read num_keys once)");
DEFINE_uint64(statis_interval, 5,
              "Statistics print interval in seconds for segment_read scenario");

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}

inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToMs(int64_t ns) {
    return static_cast<double>(ns) / 1000000.0;
}
inline double NanosToSec(int64_t ns) { return static_cast<double>(ns) / 1e9; }

struct ThreadResult {
    std::vector<int64_t> latencies_ns;
    size_t total_bytes = 0;
    size_t total_keys = 0;     // number of keys processed
    size_t total_queries = 0;  // number of API calls (get_into / batch_get_into)
    size_t failed_ops = 0;
};

class BenchmarkStats {
   public:
    void InitThreads(size_t n, size_t expected_per_thread) {
        thread_results_.resize(n);
        expected_per_thread_ = expected_per_thread;
    }

    ThreadResult& GetThreadResult(size_t tid) { return thread_results_[tid]; }

    void StartTimer() { start_ = Clock::now(); }
    void StopTimer() { end_ = Clock::now(); }

    double WallSeconds() const {
        return NanosToSec(ElapsedNanos(start_, end_));
    }

    void Finalize() {
        merged_latencies_ns_.clear();
        total_bytes_ = 0;
        total_keys_ = 0;
        total_queries_ = 0;
        total_failed_ = 0;

        for (auto& tr : thread_results_) {
            merged_latencies_ns_.insert(merged_latencies_ns_.end(),
                                        tr.latencies_ns.begin(),
                                        tr.latencies_ns.end());
            total_bytes_ += tr.total_bytes;
            total_keys_ += tr.total_keys;
            total_queries_ += tr.total_queries;
            total_failed_ += tr.failed_ops;
        }
        std::sort(merged_latencies_ns_.begin(), merged_latencies_ns_.end());
    }

    double PercentileUs(double p) const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double rank = (p / 100.0) * (merged_latencies_ns_.size() - 1);
        size_t lo = static_cast<size_t>(rank);
        size_t hi = std::min(lo + 1, merged_latencies_ns_.size() - 1);
        double frac = rank - lo;
        int64_t ns_val =
            static_cast<int64_t>(merged_latencies_ns_[lo] * (1.0 - frac) +
                                 merged_latencies_ns_[hi] * frac);
        return NanosToUs(ns_val);
    }

    double MeanLatencyUs() const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double sum = static_cast<double>(std::accumulate(
            merged_latencies_ns_.begin(), merged_latencies_ns_.end(),
            int64_t(0)));
        return NanosToUs(sum / static_cast<double>(merged_latencies_ns_.size()));
    }

    double ThroughputMBps() const {
        double wall = WallSeconds();
        return (wall > 0) ? (static_cast<double>(total_bytes_) / MB) / wall : 0;
    }

    double KeysPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_keys_) / wall : 0;
    }

    double QueriesPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_queries_) / wall : 0;
    }

    void Print(const std::string& title) const {
        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  " << title << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        double wall = WallSeconds();
        std::cout << "  Wall time:        " << wall << " s\n";
        std::cout << "  Total queries:    " << total_queries_
                  << " (failed: " << total_failed_ << ")\n";
        std::cout << "  Total keys:       " << total_keys_ << "\n";
        std::cout << "  Total data:       " << FormatBytes(total_bytes_)
                  << "\n";
        std::cout << "  Throughput:       " << ThroughputMBps() << " MB/s";
        if (ThroughputMBps() > 1024) {
            std::cout << " (" << ThroughputMBps() / 1024 << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Keys/sec:         " << KeysPerSec() << "\n";
        std::cout << "  Queries/sec:      " << QueriesPerSec() << "\n";

        if (!merged_latencies_ns_.empty()) {
            size_t n = merged_latencies_ns_.size();
            std::cout << "\n  Latency (us)      [n=" << n << ", per-query]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.front()) << "\n";
            std::cout << "    Avg:   " << std::setw(12) << MeanLatencyUs()
                      << "\n";
            std::cout << "    P50:   " << std::setw(12) << PercentileUs(50)
                      << "\n";
            std::cout << "    P90:   " << std::setw(12) << PercentileUs(90)
                      << "\n";
            std::cout << "    P99:   " << std::setw(12) << PercentileUs(99);
            if (n < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12) << PercentileUs(99.9);
            if (n < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.back()) << "\n";
        }
        std::cout << "========================================"
                  << "========================================\n\n";
    }

    size_t total_bytes() const { return total_bytes_; }
    size_t total_keys() const { return total_keys_; }
    size_t total_queries() const { return total_queries_; }
    size_t total_failed() const { return total_failed_; }

   private:
    std::vector<ThreadResult> thread_results_;
    std::vector<int64_t> merged_latencies_ns_;
    size_t total_bytes_ = 0;
    size_t total_keys_ = 0;
    size_t total_queries_ = 0;
    size_t total_failed_ = 0;
    size_t expected_per_thread_ = 0;
    Clock::time_point start_;
    Clock::time_point end_;
};

class StressBenchmark {
   public:
    StressBenchmark()
        : client_(nullptr),
          primary_dummy_client_(nullptr),
          main_buffer_client_(nullptr),
          buffer_(nullptr),
          buffer_size_(0) {}

    ~StressBenchmark() {
        // Early return if nothing was set up (covers both real and dummy
        // modes - in dummy mode client_ stays null but primary_dummy_client_
        // and dummy_clients_ may be populated).
        if (!client_ && !primary_dummy_client_ && dummy_clients_.empty()) {
            return;
        }

        // Unregister and free per-thread buffers. For dummy mode each
        // thread owns its own DummyClient and its own SHM, so we must
        // unregister the buffer with the same client that registered it
        // (otherwise the real client side will not find the matching
        // registration). For real mode all threads share client_, so
        // unregister is a no-op for repeated calls.
        for (size_t t = 0; t < thread_buffers_.size(); ++t) {
            auto& tb = thread_buffers_[t];
            if (!tb.ptr) continue;
            std::shared_ptr<mooncake::PyClient> thread_client;
            if (is_dummy_ && t < dummy_clients_.size() &&
                dummy_clients_[t]) {
                thread_client = dummy_clients_[t];
            } else {
                thread_client = client_;
            }
            if (thread_client) {
                try {
                    thread_client->unregister_buffer(tb.ptr);
                } catch (...) {
                    LOG(WARNING)
                        << "Failed to unregister thread " << t
                        << " buffer, ignoring";
                }
            }
            FreeBuffer(tb.ptr, tb.size);
            tb.ptr = nullptr;
        }
        thread_buffers_.clear();

        // Unregister and free the main buffer (allocated by this class).
        if (buffer_ && main_buffer_client_) {
            try {
                main_buffer_client_->unregister_buffer(buffer_);
            } catch (...) {
                LOG(WARNING) << "Failed to unregister main buffer, ignoring";
            }
        }
        FreeBuffer(buffer_, buffer_size_);
        buffer_ = nullptr;

        // Tear down per-thread DummyClients (one per reader thread). Each
        // call to DummyClient::tearDownAll() also stops the ping thread and
        // closes the IPC / RPC channels belonging to that client.
        for (auto& dc : dummy_clients_) {
            if (!dc) continue;
            try {
                dc->tearDownAll();
            } catch (...) {
                LOG(WARNING) << "Failed to tearDownAll per-thread dummy "
                                "client, ignoring";
            }
        }
        dummy_clients_.clear();

        // Tear down the primary DummyClient (writer's client).
        if (primary_dummy_client_) {
            try {
                primary_dummy_client_->tearDownAll();
            } catch (...) {
                LOG(WARNING) << "Failed to tearDownAll primary dummy "
                                "client, ignoring";
            }
            primary_dummy_client_.reset();
        }

        // Tear down the RealClient (no-op in dummy mode).
        if (client_) {
            try {
                client_->tearDownAll();
            } catch (...) {
                LOG(WARNING) << "Failed to tearDownAll real client, ignoring";
            }
            client_ = nullptr;
        }
        main_buffer_client_ = nullptr;
    }

    int Setup() {
        if (FLAGS_client_type == "dummy") {
            is_dummy_ = true;
            // Build the primary DummyClient that owns the main buffer. The
            // single-threaded writer, warmup, and verify paths use this
            // client and operate on buffer_. Per-thread DummyClients (one
            // per reader thread) are created later in AllocateThreadBuffers.
            // local_buffer_size is set to 0 so the primary client does not
            // pre-allocate an extra SHM segment - we will allocate buffer_
            // ourselves via ShmHelper and register it explicitly.
            primary_dummy_client_ =
                CreateDummyClient(/*local_buffer_size=*/0);
            if (!primary_dummy_client_) {
                return -1;
            }
            main_buffer_client_ = primary_dummy_client_;
            client_ = nullptr;
            LOG(INFO) << "DummyClient (primary, for writer) setup succeeded "
                      << "(server=" << FLAGS_dummy_server_address
                      << ", ipc=" << FLAGS_dummy_ipc_socket_path << ")";
        } else if (FLAGS_client_type == "real") {
            is_dummy_ = false;
            auto real = mooncake::RealClient::create();
            int ret = real->setup_real(
                FLAGS_local_hostname, FLAGS_metadata_server,
                FLAGS_global_segment_size, FLAGS_local_buffer_size,
                FLAGS_protocol, FLAGS_device_name, FLAGS_master_server, nullptr,
                "", FLAGS_enable_ssd_offload, FLAGS_ssd_offload_path);
            if (ret != 0) {
                LOG(ERROR) << "RealClient setup_real failed, ret=" << ret;
                return ret;
            }
            client_ = real;
            main_buffer_client_ = real;
            primary_dummy_client_ = nullptr;
            LOG(INFO) << "RealClient setup succeeded"
                      << (FLAGS_enable_ssd_offload
                              ? " (SSD offload enabled)"
                              : "");
        } else {
            LOG(ERROR) << "Unknown --client_type: " << FLAGS_client_type
                       << " (expected 'real' or 'dummy')";
            return -1;
        }

        buffer_size_ = FLAGS_batch_size * FLAGS_value_size;
        buffer_ = AllocateBuffer(buffer_size_);
        if (!buffer_) {
            LOG(ERROR) << "Failed to allocate buffer of " << buffer_size_
                       << " bytes ("
                       << (is_dummy_ ? "ShmHelper" : "numa") << ")";
            // Tear down the primary dummy client immediately: it has an
            // open RPC/IPC connection to the real client and a background
            // ping thread; leaving it around would leak those resources
            // (the destructor would still clean up, but we want the
            // failure to be self-contained).
            if (primary_dummy_client_) {
                primary_dummy_client_->tearDownAll();
                primary_dummy_client_.reset();
            }
            main_buffer_client_ = nullptr;
            return -1;
        }
        std::memset(buffer_, 0, buffer_size_);

        // Register the main buffer with the client that owns it: the
        // primary dummy client in dummy mode, or the shared real client
        // in real mode. If registration fails, free the buffer and the
        // primary dummy client immediately so we don't leave a half-open
        // RPC/IPC connection to the real client.
        int ret =
            main_buffer_client_->register_buffer(buffer_, buffer_size_);
        if (ret != 0) {
            LOG(ERROR) << "register_buffer (main) failed, ret=" << ret;
            FreeBuffer(buffer_, buffer_size_);
            buffer_ = nullptr;
            if (primary_dummy_client_) {
                primary_dummy_client_->tearDownAll();
                primary_dummy_client_.reset();
            }
            main_buffer_client_ = nullptr;
            return ret;
        }
        LOG(INFO) << "Registered main buffer of " << buffer_size_ / MB
                  << " MB";
        return 0;
    }

    int RunWriter() {
        LOG(INFO) << "=== WRITER MODE ===";
        LOG(INFO) << "Writing " << FLAGS_num_keys << " keys, each "
                  << FLAGS_value_size / MB << " MB";

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;

        size_t written = 0;
        size_t failed = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);

            auto t0 = Clock::now();
            int ret = main_buffer_client_->put_from(key, buffer_, FLAGS_value_size, config);
            auto t1 = Clock::now();

            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key
                           << " ret=" << ret;
                ++failed;
                continue;
            }
            ++written;

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                double elapsed_us = NanosToUs(ElapsedNanos(t0, t1));
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " last_latency=" << elapsed_us << " us";
            }
        }

        LOG(INFO) << "Write complete: " << written << " succeeded, " << failed
                  << " failed";
        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return (failed > 0) ? -1 : 0;
    }

    int RunReader() {
        LOG(INFO) << "=== READER MODE ===";
        LOG(INFO) << "Scenario: " << FLAGS_scenario;
        LOG(INFO) << "Reading " << FLAGS_num_keys << " keys with "
                  << FLAGS_num_threads
                  << " threads, batch_size=" << FLAGS_batch_size;

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        if (FLAGS_scenario == "remote_memory" ||
            FLAGS_scenario == "remote_disk") {
            LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                      << " seconds for writer to finish prefill...";
            std::this_thread::sleep_for(
                std::chrono::seconds(FLAGS_wait_seconds));
        }

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }
        system("ubdiag clear");

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "READ BENCHMARK [" + FLAGS_scenario + "]";
        stats.Print(title);

        if (FLAGS_verify) {
            int v = VerifyData();
            if (v != 0) {
                LOG(ERROR) << "Data verification FAILED";
            } else {
                LOG(INFO) << "Data verification PASSED";
            }
        }

        return 0;
    }

    int RunLocalMemory() {
        LOG(INFO) << "=== LOCAL MEMORY BENCHMARK ===";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;

        LOG(INFO) << "Phase 1: Writing " << FLAGS_num_keys << " keys...";
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);
            int ret = main_buffer_client_->put_from(key, buffer_, FLAGS_value_size, config);
            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key;
                return ret;
            }
            if ((i + 1) % 50 == 0) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys;
            }
        }
        LOG(INFO) << "Write phase complete";

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent reads with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL MEMORY READ BENCHMARK");

        if (FLAGS_verify) {
            int v = VerifyData();
            LOG_IF(INFO, v == 0) << "Data verification PASSED";
            LOG_IF(ERROR, v != 0) << "Data verification FAILED";
        }

        return 0;
    }

    int RunLocalDisk() {
        LOG(INFO) << "=== LOCAL DISK BENCHMARK ===";
        LOG(INFO) << "NOTE: Disk reads require Master with enable_offload=true "
                  << "and client with enable_ssd_offload=true";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;

        LOG(INFO) << "Phase 1: Writing " << FLAGS_num_keys
                  << " keys (data may be offloaded to SSD)...";
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);
            int ret = main_buffer_client_->put_from(key, buffer_, FLAGS_value_size, config);
            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key;
                return ret;
            }
            if ((i + 1) % 50 == 0) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys;
            }
        }
        LOG(INFO) << "Write phase complete";

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for offload/eviction to complete...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent disk reads with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL DISK READ BENCHMARK");

        if (FLAGS_verify) {
            int v = VerifyData();
            LOG_IF(INFO, v == 0) << "Data verification PASSED";
            LOG_IF(ERROR, v != 0) << "Data verification FAILED";
        }

        return 0;
    }

    static std::vector<std::string> ParseSegments() {
        std::vector<std::string> segments;
        std::istringstream iss(FLAGS_segments);
        std::string seg;
        while (std::getline(iss, seg, ',')) {
            size_t start = seg.find_first_not_of(" \t");
            size_t end = seg.find_last_not_of(" \t");
            if (start != std::string::npos && end != std::string::npos) {
                segments.push_back(seg.substr(start, end - start + 1));
            }
        }
        return segments;
    }

    static std::string MakeSegmentKey(const std::string& segment, size_t idx) {
        static const char* kSpecialChars = ".:-/\\[]{}()@#$%^&*+=|<>,;!?`'\"~";
        std::string sanitized = segment;
        for (char& c : sanitized) {
            if (std::strchr(kSpecialChars, c) != nullptr || std::isspace(c)) {
                c = '_';
            }
        }
        return "seg_" + sanitized + "_key_" + std::to_string(idx);
    }

    int RunSegmentWrite() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        LOG(INFO) << "=== SEGMENT WRITE MODE ===";
        LOG(INFO) << "Writing to " << segments.size() << " segments, "
                  << FLAGS_num_keys << " keys per segment (interleaved), each "
                  << FLAGS_value_size / MB << " MB";

        std::vector<size_t> seg_written(segments.size(), 0);
        std::vector<size_t> seg_failed(segments.size(), 0);
        std::vector<mooncake::ReplicateConfig> configs(segments.size());
        for (size_t s = 0; s < segments.size(); ++s) {
            configs[s].replica_num = FLAGS_replica_num;
            configs[s].with_hard_pin = FLAGS_hard_pin;
            configs[s].preferred_segments = {segments[s]};
        }

        size_t total_written = 0;
        size_t total_failed = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < segments.size(); ++s) {
                const auto& segment = segments[s];
                std::string key = MakeSegmentKey(segment, i);
                FillBuffer(i);

                auto t0 = Clock::now();
                int ret = main_buffer_client_->put_from(key, buffer_, FLAGS_value_size,
                                            configs[s]);
                auto t1 = Clock::now();

                if (ret != 0) {
                    LOG(ERROR) << "put_from failed for key=" << key
                               << " segment=" << segment << " ret=" << ret;
                    ++seg_failed[s];
                    continue;
                }
                ++seg_written[s];
            }

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " keys to all " << segments.size() << " segments";
            }
        }

        for (size_t s = 0; s < segments.size(); ++s) {
            total_written += seg_written[s];
            total_failed += seg_failed[s];
            LOG(INFO) << "Segment [" << s << "] " << segments[s]
                      << " complete: " << seg_written[s] << " succeeded, "
                      << seg_failed[s] << " failed";
        }

        LOG(INFO) << "All segments write complete: " << total_written
                  << " succeeded, " << total_failed << " failed";

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return (total_failed > 0) ? -1 : 0;
    }

    int RunSegmentRead() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        size_t read_segment_nums = FLAGS_read_segment_nums;
        if (read_segment_nums == 0 || read_segment_nums > segments.size()) {
            read_segment_nums = segments.size();
        }

        std::vector<std::string> read_segments(
            segments.begin(), segments.begin() + read_segment_nums);

        LOG(INFO) << "=== SEGMENT READ MODE ===";
        LOG(INFO) << "Reading from " << read_segment_nums << " segments ("
                  << read_segment_nums << " nodes)";
        for (size_t s = 0; s < read_segments.size(); ++s) {
            LOG(INFO) << "  Segment [" << s << "]: " << read_segments[s];
        }
        LOG(INFO) << "Keys per segment: " << FLAGS_num_keys;
        LOG(INFO) << "Duration: "
                  << (FLAGS_duration > 0 ? std::to_string(FLAGS_duration) + "s"
                                         : "single pass");
        LOG(INFO) << "Stats interval: " << FLAGS_statis_interval << "s";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        std::vector<std::string> all_keys;
        for (size_t s = 0; s < read_segments.size(); ++s) {
            for (size_t i = 0; i < FLAGS_num_keys; ++i) {
                all_keys.push_back(MakeSegmentKey(read_segments[s], i));
            }
        }
        LOG(INFO) << "Total keys to read: " << all_keys.size();

        size_t warmup_end =
            std::min(static_cast<size_t>(FLAGS_warmup_keys), all_keys.size());
        if (warmup_end > 0) {
            LOG(INFO) << "Warmup: reading " << warmup_end << " keys...";
            for (size_t i = 0; i < warmup_end; ++i) {
                int64_t ret =
                    main_buffer_client_->get_into(all_keys[i], buffer_, FLAGS_value_size);
                if (ret < 0) {
                    LOG(WARNING)
                        << "Warmup get_into failed for key=" << all_keys[i]
                        << " ret=" << ret;
                }
            }
            LOG(INFO) << "Warmup complete";
        }

        if (FLAGS_duration == 0) {
            return RunSegmentReadSinglePass(read_segments, all_keys);
        }
        return RunSegmentReadDuration(read_segments, all_keys);
    }

    int RunSegmentReadSinglePass(const std::vector<std::string>& read_segments,
                                 const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Single-pass read with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          all_keys.size() / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads =
            LaunchReadWorkers(FLAGS_num_threads, all_keys.size(), stats,
                              start_latch, done_latch, [&all_keys](size_t idx) {
                                  return all_keys[idx % all_keys.size()];
                              });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "SEGMENT READ BENCHMARK [segments=" +
                            std::to_string(read_segments.size()) + "]";
        stats.Print(title);
        return 0;
    }

    struct IntervalLatencyStats {
        std::vector<int64_t> latencies_ns;
        int64_t min_latency_ns = std::numeric_limits<int64_t>::max();
        int64_t max_latency_ns = 0;
        double p50_latency_ns = 0;
        double p90_latency_ns = 0;
        double p99_latency_ns = 0;
        double p999_latency_ns = 0;
        double p9999_latency_ns = 0;
        double avg_latency_ns = 0;
        double throughput_mbps = 0;
        double keys_per_sec = 0;
        double queries_per_sec = 0;

        void Finalize() {
            if (latencies_ns.empty()) return;
            std::sort(latencies_ns.begin(), latencies_ns.end());
            min_latency_ns = latencies_ns.front();
            max_latency_ns = latencies_ns.back();
            size_t n = latencies_ns.size();
            avg_latency_ns =
                std::accumulate(latencies_ns.begin(), latencies_ns.end(), 0.0) /
                n;
            auto percentile = [&](double p) -> double {
                if (n == 0) return 0;
                size_t idx = static_cast<size_t>(p / 100.0 * (n - 1));
                return static_cast<double>(latencies_ns[idx]);
            };
            p50_latency_ns = percentile(50);
            p90_latency_ns = percentile(90);
            if (n >= 100) {
                p99_latency_ns = latencies_ns[static_cast<size_t>(n * 0.99)];
            }
            if (n >= 1000) {
                p999_latency_ns = latencies_ns[static_cast<size_t>(n * 0.999)];
            }
            // P99.99 needs n >= 10000 to be statistically meaningful.
            // When n < 10000 we fall back to the maximum sample so
            // downstream consumers see a non-zero number (it would
            // otherwise be silently 0.0, which is misleading). The
            // print code already tags it with "(n<10000)" so the
            // user knows it's an upper-bound proxy, not a true
            // percentile.
            if (n >= 10000) {
                p9999_latency_ns =
                    latencies_ns[static_cast<size_t>(n * 0.9999)];
            } else {
                p9999_latency_ns = latencies_ns.back();
            }
        }

        void Aggregate(const IntervalLatencyStats& other) {
            min_latency_ns = std::min(min_latency_ns, other.min_latency_ns);
            max_latency_ns = std::max(max_latency_ns, other.max_latency_ns);
            p50_latency_ns = std::max(p50_latency_ns, other.p50_latency_ns);
            p90_latency_ns = std::max(p90_latency_ns, other.p90_latency_ns);
            p99_latency_ns = std::max(p99_latency_ns, other.p99_latency_ns);
            p999_latency_ns = std::max(p999_latency_ns, other.p999_latency_ns);
            p9999_latency_ns =
                std::max(p9999_latency_ns, other.p9999_latency_ns);
            throughput_mbps += other.throughput_mbps;
            keys_per_sec += other.keys_per_sec;
            queries_per_sec += other.queries_per_sec;
            // Weighted average: accumulate sum and count
            total_latency_sum_ns +=
                other.avg_latency_ns *
                static_cast<double>(other.latencies_ns.size());
            total_samples += other.latencies_ns.size();
            if (total_samples > 0) {
                avg_latency_ns =
                    total_latency_sum_ns / static_cast<double>(total_samples);
            }
        }

        size_t total_samples = 0;
        double total_latency_sum_ns = 0;
    };

    int RunSegmentReadDuration(const std::vector<std::string>& read_segments,
                               const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Duration-based continuous read with " << FLAGS_num_threads
                  << " threads for " << FLAGS_duration << "s, stats every "
                  << FLAGS_statis_interval << "s";

        std::atomic<bool> stop_flag{false};
        std::atomic<size_t> global_keys{0};
        std::atomic<size_t> global_queries{0};
        std::atomic<size_t> global_bytes{0};
        std::atomic<size_t> global_failed{0};

        std::vector<std::vector<int64_t>> thread_latencies(FLAGS_num_threads);
        std::vector<std::mutex> latency_mutexes(FLAGS_num_threads);

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;

        size_t total_keys = all_keys.size();
        size_t keys_per_thread =
            (total_keys + FLAGS_num_threads - 1) / FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            // Per-thread client: each reader thread uses its own
            // DummyClient in dummy mode (isolated SHM + RPC), or the
            // shared RealClient in real mode.
            std::shared_ptr<mooncake::PyClient> thread_client;
            if (is_dummy_) {
                thread_client = dummy_clients_[t];
            } else {
                thread_client = client_;
            }
            threads.emplace_back(
                [&, t, keys_per_thread, total_keys, thread_client]() {
                    bindToSocket(t % NR_SOCKETS);
                    char* my_buf = thread_buffers_[t].ptr;

                    start_latch.arrive_and_wait();

                    size_t key_offset = t * keys_per_thread;
                    size_t key_idx = key_offset;

                    if (FLAGS_batch_size <= 1) {
                        while (!stop_flag.load(std::memory_order_relaxed)) {
                            const std::string& key =
                                all_keys[key_idx % total_keys];
                            auto t0 = Clock::now();
                            int64_t ret = thread_client->get_into(
                                key, my_buf, FLAGS_value_size);
                            auto t1 = Clock::now();
                            int64_t latency_ns = ElapsedNanos(t0, t1);

                            {
                                std::lock_guard<std::mutex> lock(
                                    latency_mutexes[t]);
                                thread_latencies[t].push_back(latency_ns);
                            }

                            if (ret < 0) {
                                global_failed.fetch_add(
                                    1, std::memory_order_relaxed);
                            } else {
                                global_bytes.fetch_add(
                                    static_cast<size_t>(ret),
                                    std::memory_order_relaxed);
                            }
                            global_keys.fetch_add(1, std::memory_order_relaxed);
                            global_queries.fetch_add(1, std::memory_order_relaxed);
                            ++key_idx;
                        }
                    } else {
                        size_t per_key_buf = FLAGS_value_size;
                        while (!stop_flag.load(
                            std::memory_order_relaxed)) {
                            std::vector<std::string> keys;
                            std::vector<void*> bufs;
                            std::vector<size_t> sizes;
                            keys.reserve(FLAGS_batch_size);
                            bufs.reserve(FLAGS_batch_size);
                            sizes.reserve(FLAGS_batch_size);

                            for (size_t b = 0; b < FLAGS_batch_size; ++b) {
                                const std::string& key =
                                    all_keys[key_idx % total_keys];
                                keys.push_back(key);
                                bufs.push_back(my_buf + b * per_key_buf);
                                sizes.push_back(FLAGS_value_size);
                                ++key_idx;
                            }

                            auto t0 = Clock::now();
                            auto results = thread_client->batch_get_into(
                                keys, bufs, sizes);
                            auto t1 = Clock::now();
                            int64_t latency_ns = ElapsedNanos(t0, t1);

                            {
                                std::lock_guard<std::mutex> lock(
                                    latency_mutexes[t]);
                                for (size_t k = 0; k < results.size(); ++k) {
                                    thread_latencies[t].push_back(
                                        latency_ns / FLAGS_batch_size);
                                }
                            }

                            for (size_t k = 0; k < results.size(); ++k) {
                                if (results[k] < 0) {
                                    global_failed.fetch_add(
                                        1, std::memory_order_relaxed);
                                } else {
                                    global_bytes.fetch_add(
                                        static_cast<size_t>(results[k]),
                                        std::memory_order_relaxed);
                                }
                                global_keys.fetch_add(1, std::memory_order_relaxed);
                            }
			    global_queries.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                });
        }

        auto bench_start = Clock::now();
        auto bench_end = bench_start + std::chrono::seconds(FLAGS_duration);
        auto next_statis =
            bench_start + std::chrono::seconds(FLAGS_statis_interval);

        size_t prev_keys = 0;
        size_t prev_queries = 0;
        size_t prev_bytes = 0;
        size_t prev_failed = 0;
        auto prev_time = bench_start;

        std::vector<IntervalLatencyStats> interval_stats_list;

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  SEGMENT READ DURATION BENCHMARK [segments="
                  << read_segments.size() << "]\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        while (Clock::now() < bench_end) {
            auto now = Clock::now();
            if (now >= next_statis) {
                size_t cur_keys = global_keys.load(std::memory_order_relaxed);
                size_t cur_queries =
                    global_queries.load(std::memory_order_relaxed);
                size_t cur_bytes = global_bytes.load(std::memory_order_relaxed);
                size_t cur_failed =
                    global_failed.load(std::memory_order_relaxed);

                double interval_sec = NanosToSec(ElapsedNanos(prev_time, now));
                size_t interval_keys = cur_keys - prev_keys;
                size_t interval_queries = cur_queries - prev_queries;
                size_t interval_bytes = cur_bytes - prev_bytes;
                size_t interval_failed = cur_failed - prev_failed;

                double interval_throughput_mbps =
                    (interval_sec > 0)
                        ? (static_cast<double>(interval_bytes) / MB) /
                              interval_sec
                        : 0;
                double interval_keys_per_sec =
                    (interval_sec > 0)
                        ? static_cast<double>(interval_keys) / interval_sec
                        : 0;
                double interval_queries_per_sec =
                    (interval_sec > 0)
                        ? static_cast<double>(interval_queries) / interval_sec
                        : 0;

                IntervalLatencyStats interval_stats;
                interval_stats.throughput_mbps = interval_throughput_mbps;
                interval_stats.keys_per_sec = interval_keys_per_sec;
                interval_stats.queries_per_sec = interval_queries_per_sec;
                for (size_t t = 0; t < FLAGS_num_threads; ++t) {
                    std::lock_guard<std::mutex> lock(latency_mutexes[t]);
                    interval_stats.latencies_ns.insert(
                        interval_stats.latencies_ns.end(),
                        thread_latencies[t].begin(), thread_latencies[t].end());
                    thread_latencies[t].clear();
                }
                interval_stats.Finalize();
                interval_stats_list.push_back(interval_stats);

                double total_sec = NanosToSec(ElapsedNanos(bench_start, now));
                double total_throughput_mbps =
                    (total_sec > 0)
                        ? (static_cast<double>(cur_bytes) / MB) / total_sec
                        : 0;
                double total_keys_per_sec =
                    (total_sec > 0) ? static_cast<double>(cur_keys) / total_sec
                                    : 0;
                double total_queries_per_sec =
                    (total_sec > 0)
                        ? static_cast<double>(cur_queries) / total_sec
                        : 0;

                std::cout << "  [t=" << std::setw(6) << total_sec << "s]"
                          << "  interval: " << interval_throughput_mbps
                          << " MB/s, " << interval_keys_per_sec << " keys/s, "
                          << interval_queries_per_sec << " qps"
                          << " (failed=" << interval_failed << ")"
                          << "  lat[us]: avg="
                          << NanosToUs(interval_stats.avg_latency_ns)
                          << ", P50=" << NanosToUs(interval_stats.p50_latency_ns)
                          << ", P90=" << NanosToUs(interval_stats.p90_latency_ns)
                          << ", P99="
                          << NanosToUs(interval_stats.p99_latency_ns)
                          << "  total: " << cur_queries << " queries, "
                          << cur_keys << " keys, "
                          << total_throughput_mbps << " MB/s, "
                          << total_keys_per_sec << " keys/s, "
                          << total_queries_per_sec << " qps"
                          << " (failed=" << cur_failed << ")\n";

                prev_keys = cur_keys;
                prev_queries = cur_queries;
                prev_bytes = cur_bytes;
                prev_failed = cur_failed;
                prev_time = now;
                next_statis += std::chrono::seconds(FLAGS_statis_interval);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        stop_flag.store(true, std::memory_order_relaxed);
        for (auto& th : threads) {
            th.join();
        }

        auto final_time = Clock::now();
        double total_sec = NanosToSec(ElapsedNanos(bench_start, final_time));
        size_t final_keys = global_keys.load(std::memory_order_relaxed);
        size_t final_queries = global_queries.load(std::memory_order_relaxed);
        size_t final_bytes = global_bytes.load(std::memory_order_relaxed);
        size_t final_failed = global_failed.load(std::memory_order_relaxed);

        double final_throughput_mbps =
            (total_sec > 0)
                ? (static_cast<double>(final_bytes) / MB) / total_sec
                : 0;
        double final_keys_per_sec =
            (total_sec > 0) ? static_cast<double>(final_keys) / total_sec : 0;
        double final_queries_per_sec =
            (total_sec > 0) ? static_cast<double>(final_queries) / total_sec
                            : 0;

        IntervalLatencyStats overall;
        for (const auto& stats : interval_stats_list) {
            overall.Aggregate(stats);
        }
        double avg_throughput_mbps =
            !interval_stats_list.empty()
                ? overall.throughput_mbps / interval_stats_list.size()
                : 0;
        size_t total_latency_samples = overall.total_samples;

        std::cout << "\n  FINAL SUMMARY\n";
        std::cout << "  Total time:       " << total_sec << " s\n";
        std::cout << "  Total queries:    " << final_queries
                  << " (failed: " << final_failed << ")\n";
        std::cout << "  Total keys:       " << final_keys << "\n";
        std::cout << "  Total data:       " << FormatBytes(final_bytes) << "\n";
        std::cout << "  Throughput:       " << final_throughput_mbps
                  << " MB/s (avg: " << avg_throughput_mbps << " MB/s)";
        if (final_throughput_mbps > 1024) {
            std::cout << " (" << final_throughput_mbps / 1024 << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Keys/sec:         " << final_keys_per_sec << "\n";
        std::cout << "  Queries/sec:      " << final_queries_per_sec << "\n";

        if (total_latency_samples > 0) {
            std::cout << "\n  Latency (us)      [n=" << total_latency_samples
                      << ", per-query]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(overall.min_latency_ns) << "\n";
            std::cout << "    Avg:   " << std::setw(12)
                      << NanosToUs(overall.avg_latency_ns) << "\n";
            std::cout << "    P50:   " << std::setw(12)
                      << NanosToUs(overall.p50_latency_ns) << "\n";
            std::cout << "    P90:   " << std::setw(12)
                      << NanosToUs(overall.p90_latency_ns) << "\n";
            std::cout << "    P99:   " << std::setw(12)
                      << NanosToUs(overall.p99_latency_ns);
            if (total_latency_samples < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12)
                      << NanosToUs(overall.p999_latency_ns);
            if (total_latency_samples < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    P9999: " << std::setw(12)
                      << NanosToUs(overall.p9999_latency_ns);
            if (total_latency_samples < 10000) std::cout << "  (n<10000)";
            std::cout << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(overall.max_latency_ns) << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n\n";

        return 0;
    }

    int RunListSegments() {
        LOG(INFO) << "Discovering segments from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;

        auto segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server, static_cast<int>(FLAGS_master_admin_port));

        if (segments.empty()) {
            LOG(ERROR) << "No segments discovered from master. "
                       << "Check master connectivity at " << FLAGS_master_server
                       << ":" << FLAGS_master_admin_port;
            return -1;
        }

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  DISCOVERED SEGMENTS [count=" << segments.size()
                  << "]\n";
        std::cout << "========================================"
                  << "========================================\n";

        for (size_t i = 0; i < segments.size(); ++i) {
            std::cout << "  [" << std::setw(4) << i << "] " << segments[i]
                      << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  Total segments: " << segments.size() << "\n";
        std::cout << "========================================"
                  << "========================================\n\n";

        return 0;
    }

    // ------------------------------------------------------------------------
    // client_rpc_bench: measure the round-trip latency of a single
    // client-to-client RPC, with no master RPC, no SSD I/O, and no P2P
    // data transfer.
    //
    // Multi-peer variant: in each round of the timed loop the bench
    // visits every alive peer sequentially and reports per-peer
    // statistics in addition to the AGGREGATE summary. Use
    // --peer_rpc_addrs=ip1:port,ip2:port,... (preferred) or
    // --peer_rpc_addr=ip:port (single-peer fallback) to point at the
    // peer(s). Each peer must be started with
    // --enable_ssd_offload=true so its offload_rpc_server_ is up.
    //
    // Implementation: spin up a standalone mooncake::ClientRequester in
    // this process, and use it to call
    //   RealClient::batch_get_offload_object
    // (the only client-side RPC handler that is already registered to the
    // peer's offload_rpc_server_) with EMPTY keys and sizes. The peer:
    //   1. receives the RPC request
    //   2. runs BatchGet({},{}) on its FileStorage -- with empty inputs
    //      this is O(1) bookkeeping, no SSD read, no allocation loop
    //   3. replies with BatchGetOffloadObjectResponse{ batch_id,
    //      pointers=<empty>, transfer_engine_addr=<peer segment endpoint>,
    //      gc_ttl_ms }
    // We time the full call and treat the response's
    // transfer_engine_addr as the "peer memory address" requested by the
    // user.
    //
    // Why NOT use get_into / batch_get_into / execute_ranged_read /
    // isExist: all of those go through the master (stage 1) or perform
    // real data I/O (stages 2-5), so they do not isolate a single RPC
    // hop.
    //
    // Prerequisite: every peer must be started with
    // --enable_ssd_offload=true so its offload_rpc_server_ is up and
    // the batch_get_offload_object handler is registered.
    // ------------------------------------------------------------------------
    int RunClientRpcBench() {
        LOG(INFO) << "=== CLIENT-TO-CLIENT RPC BENCHMARK (single "
                     "ClientRequester, batch_get_offload_object, multi-peer) ===";

        if (FLAGS_num_threads == 0) {
            LOG(ERROR) << "--num_threads must be > 0";
            return -1;
        }

        auto peer_addrs = ParsePeerRpcAddrs();
        if (peer_addrs.empty()) {
            LOG(ERROR)
                << "Provide --peer_rpc_addrs=ip1:port,ip2:port,... or "
                   "--peer_rpc_addr=ip:port for client_rpc_bench. Each peer "
                   "must be started with --enable_ssd_offload=true.";
            return -1;
        }
        LOG(INFO) << "Targeting " << peer_addrs.size() << " peer(s):";
        for (size_t i = 0; i < peer_addrs.size(); ++i) {
            LOG(INFO) << "  peer[" << i << "]: " << peer_addrs[i];
        }

        // One ClientRequester shared by all worker threads; the
        // underlying coro_rpc client pool is thread-safe.
        auto requester = std::make_shared<mooncake::ClientRequester>();

        // Build the request payload ONCE and reuse across all peers and
        // all iterations so every measured call hits exactly the same
        // path. When --ssd_key is set, the request actually reads from
        // the peer's SSD storage; otherwise it is an empty request (no
        // SSD I/O).
        std::vector<std::string> req_keys;
        std::vector<int64_t> req_sizes;
        if (!FLAGS_ssd_key.empty()) {
            req_keys.push_back(FLAGS_ssd_key);
            req_sizes.push_back(FLAGS_ssd_value_size);
            LOG(INFO) << "Will request real SSD read: key=\""
                      << FLAGS_ssd_key << "\" size="
                      << FLAGS_ssd_value_size << "B. Make sure the peer "
                      << "has this key in its SSD (write it first via "
                      << "scenario=local_disk or similar on the peer).";
        } else {
            LOG(INFO) << "Will request empty payload (no SSD I/O on peer). "
                      << "Set --ssd_key=<peer_ssd_key> to actually read SSD.";
        }

        // Per-peer warmup. Peers whose warmup fully fails are kept in
        // the list and marked warmup_ok=false so the timed loop skips
        // them. We only abort if EVERY peer fails warmup.
        std::vector<PeerBenchState> peer_states(peer_addrs.size());
        for (size_t p = 0; p < peer_addrs.size(); ++p) {
            peer_states[p].peer_addr = peer_addrs[p];
            size_t warmup_n = static_cast<size_t>(
                std::max<int64_t>(1, FLAGS_warmup_keys));
            size_t warmup_done = 0;
            for (size_t i = 0; i < warmup_n; ++i) {
                auto r = requester->batch_get_offload_object(
                    peer_addrs[p], req_keys, req_sizes);
                if (r) {
                    ++warmup_done;
                    if (peer_states[p].peer_seen_addr.empty()) {
                        peer_states[p].peer_seen_addr =
                            r->transfer_engine_addr;
                    }
                }
            }
            peer_states[p].warmup_ok = (warmup_done > 0);
            LOG(INFO) << "Peer[" << p << "] " << peer_addrs[p]
                      << " warmup " << warmup_done << "/" << warmup_n
                      << ", transfer_engine_addr=\""
                      << peer_states[p].peer_seen_addr << "\"";
            if (!peer_states[p].warmup_ok) {
                LOG(ERROR) << "Peer[" << p << "] " << peer_addrs[p]
                           << " warmup failed; this peer will be skipped.";
            }
        }

        size_t alive_peers = 0;
        for (const auto& ps : peer_states) {
            if (ps.warmup_ok) ++alive_peers;
        }
        if (alive_peers == 0) {
            LOG(ERROR) << "All peer warmups failed. Aborting. Check that "
                          "the peers are reachable and were started with "
                          "--enable_ssd_offload=true.";
            return -1;
        }
        LOG(INFO) << alive_peers << "/" << peer_states.size()
                  << " peer(s) ready for the timed loop.";

        if (FLAGS_duration == 0) {
            return RunClientRpcBenchSinglePass(peer_states, req_keys,
                                               req_sizes, requester);
        }
        return RunClientRpcBenchDuration(peer_states, req_keys, req_sizes,
                                         requester);
    }

    int RunClientRpcBenchSinglePass(
        std::vector<PeerBenchState>& peer_states,
        const std::vector<std::string>& req_keys,
        const std::vector<int64_t>& req_sizes,
        const std::shared_ptr<mooncake::ClientRequester>& requester) {
        const size_t num_peers = peer_states.size();
        const size_t per_thread = std::max<size_t>(1, FLAGS_num_keys);
        const size_t per_thread_rpcs = per_thread * num_peers;

        LOG(INFO)
            << "Single-pass mode: " << FLAGS_num_threads << " threads x "
            << per_thread << " rounds x " << num_peers
            << " peers (skipping warmup-failed) = up to "
            << (FLAGS_num_threads * per_thread_rpcs)
            << " total RPCs. Measured: single client-to-client RPC RTT.";

        for (auto& ps : peer_states) {
            if (ps.warmup_ok) {
                ps.stats.InitThreads(FLAGS_num_threads, per_thread);
            }
        }

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;
        threads.reserve(FLAGS_num_threads);

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            threads.emplace_back([&, t]() {
                bindToSocket(t % NR_SOCKETS);
                for (auto& ps : peer_states) {
                    if (ps.warmup_ok) {
                        ps.stats.GetThreadResult(t).latencies_ns.reserve(
                            per_thread);
                    }
                }
                start_latch.arrive_and_wait();

                for (size_t k = 0; k < per_thread; ++k) {
                    // One round = one RPC per alive peer, sequentially.
                    for (size_t p = 0; p < num_peers; ++p) {
                        if (!peer_states[p].warmup_ok) continue;
                        const std::string& addr = peer_states[p].peer_addr;

                        // --- single client-to-client RPC: send
                        //     request, receive
                        //     BatchGetOffloadObjectResponse ---
                        auto t0 = Clock::now();
                        auto ret = requester->batch_get_offload_object(
                            addr, req_keys, req_sizes);
                        auto t1 = Clock::now();

                        int64_t lat_ns = ElapsedNanos(t0, t1);
                        ThreadResult& tr =
                            peer_states[p].stats.GetThreadResult(t);
                        tr.latencies_ns.push_back(lat_ns);

                        if (!ret) {
                            ++tr.failed_ops;
                            LOG_EVERY_N(ERROR, 100)
                                << "batch_get_offload_object RPC to "
                                << addr << " failed: "
                                << mooncake::toString(ret.error());
                        } else {
                            // Each successful response carries a
                            // BatchGetOffloadObjectResponse. "Bytes" in
                            // the stats sheet is the response size,
                            // which is a good proxy for "amount of data
                            // round-tripped" -- not real payload bytes.
                            tr.total_bytes +=
                                static_cast<size_t>(
                                    ret->pointers.size() * sizeof(uint64_t)) +
                                ret->transfer_engine_addr.size() +
                                sizeof(ret->batch_id) +
                                sizeof(ret->gc_ttl_ms);
                            // Sanity: the response must have been served
                            // by the peer we asked for. Print the first
                            // successful peer's address per thread for
                            // end-to-end verification.
                            if (k == 0 &&
                                peer_states[p].peer_seen_addr !=
                                    ret->transfer_engine_addr) {
                                LOG(WARNING)
                                    << "  [t=" << t << " p=" << p
                                    << "] peer transfer_engine_addr "
                                    << "mismatch: expected=\""
                                    << peer_states[p].peer_seen_addr
                                    << "\" got=\""
                                    << ret->transfer_engine_addr << "\"";
                            }
                        }
                        ++tr.total_keys;
                        ++tr.total_queries;
                    }
                }

                done_latch.arrive_and_wait();
            });
        }

        done_latch.wait();
        for (auto& th : threads) th.join();

        // Per-peer prints, then AGGREGATE.
        for (auto& ps : peer_states) {
            if (!ps.warmup_ok) continue;
            ps.stats.Finalize();
            std::string title = "CLIENT-TO-CLIENT RPC BENCHMARK [peer=" +
                                ps.peer_addr + "]";
            ps.stats.Print(title);
        }
        BenchmarkStats agg = MergePeerStats(peer_states);
        agg.Print(
            "CLIENT-TO-CLIENT RPC BENCHMARK [AGGREGATE across all alive "
            "peers]");

        return 0;
    }

    int RunClientRpcBenchDuration(
        std::vector<PeerBenchState>& peer_states,
        const std::vector<std::string>& req_keys,
        const std::vector<int64_t>& req_sizes,
        const std::shared_ptr<mooncake::ClientRequester>& requester) {
        const size_t num_peers = peer_states.size();
        LOG(INFO) << "Duration mode: " << FLAGS_num_threads
                  << " threads continuously fire client-to-client RPCs to "
                  << num_peers << " peer(s) for " << FLAGS_duration
                  << "s, stats every " << FLAGS_statis_interval
                  << "s. keys=" << req_keys.size() << ", sizes="
                  << req_sizes.size() << " ("
                  << (req_keys.empty()
                          ? "empty payload -> no SSD I/O on peer"
                          : "real SSD read on peer")
                  << "). Measured: single client-to-client RPC RTT.";

        for (auto& ps : peer_states) {
            if (ps.warmup_ok) {
                ps.InitDuration(FLAGS_num_threads);
            }
        }

        std::atomic<bool> stop_flag{false};
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;
        threads.reserve(FLAGS_num_threads);

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            threads.emplace_back([&, t]() {
                bindToSocket(t % NR_SOCKETS);
                start_latch.arrive_and_wait();

                if (FLAGS_batch_size <= 1) {
                    // Single-RPC-per-iteration path. Each round
                    // visits every alive peer sequentially and times
                    // each RPC individually; the latency distribution
                    // is the steady-state RTT of one client-to-client
                    // RPC to that peer.
                    while (
                        !stop_flag.load(std::memory_order_relaxed)) {
                        for (size_t p = 0; p < num_peers; ++p) {
                            if (!peer_states[p].warmup_ok) continue;
                            const std::string& addr = peer_states[p].peer_addr;
                            auto t0 = Clock::now();
                            auto ret = requester->batch_get_offload_object(
                                addr, req_keys, req_sizes);
                            auto t1 = Clock::now();
                            int64_t lat_ns = ElapsedNanos(t0, t1);
                            PeerBenchState& ps = peer_states[p];
                            {
                                std::lock_guard<std::mutex> lk(
                                    ps.latency_mutexes[t]);
                                ps.thread_latencies[t].push_back(lat_ns);
                            }
                            if (!ret) {
                                ps.global_failed.fetch_add(
                                    1, std::memory_order_relaxed);
                            } else {
                                ps.global_bytes.fetch_add(
                                    static_cast<size_t>(
                                        ret->pointers.size() *
                                        sizeof(uint64_t)) +
                                        ret->transfer_engine_addr.size() +
                                        sizeof(ret->batch_id) +
                                        sizeof(ret->gc_ttl_ms),
                                    std::memory_order_relaxed);
                            }
                            ps.global_keys.fetch_add(
                                1, std::memory_order_relaxed);
                            ps.global_queries.fetch_add(
                                1, std::memory_order_relaxed);
                        }
                    }
                } else {
                    // Batch path: each "batch" is FLAGS_batch_size
                    // rounds, and each round visits all alive peers.
                    // We time the whole batch and split equally across
                    // (batch_size * num_peers) RPCs for the per-RPC
                    // latency samples, then attribute FLAGS_batch_size
                    // samples (= per_rpc_ns each) to every alive peer
                    // to match the original single-peer semantics
                    // (one sample per RPC issued to that peer).
                    while (
                        !stop_flag.load(std::memory_order_relaxed)) {
                        auto t0 = Clock::now();
                        for (size_t b = 0; b < FLAGS_batch_size; ++b) {
                            for (size_t p = 0; p < num_peers; ++p) {
                                if (!peer_states[p].warmup_ok) continue;
                                const std::string& addr =
                                    peer_states[p].peer_addr;
                                auto ret = requester->batch_get_offload_object(
                                    addr, req_keys, req_sizes);
                                PeerBenchState& ps = peer_states[p];
                                if (!ret) {
                                    ps.global_failed.fetch_add(
                                        1, std::memory_order_relaxed);
                                } else {
                                    ps.global_bytes.fetch_add(
                                        static_cast<size_t>(
                                            ret->pointers.size() *
                                            sizeof(uint64_t)) +
                                            ret->transfer_engine_addr.size() +
                                            sizeof(ret->batch_id) +
                                            sizeof(ret->gc_ttl_ms),
                                        std::memory_order_relaxed);
                                }
                                ps.global_keys.fetch_add(
                                    1, std::memory_order_relaxed);
                            }
                        }
                        auto t1 = Clock::now();
                        int64_t batch_lat_ns = ElapsedNanos(t0, t1);
                        int64_t per_rpc_ns = batch_lat_ns /
                                             (FLAGS_batch_size * num_peers);
                        for (auto& ps : peer_states) {
                            if (!ps.warmup_ok) continue;
                            std::lock_guard<std::mutex> lk(
                                ps.latency_mutexes[t]);
                            for (size_t b = 0; b < FLAGS_batch_size; ++b) {
                                ps.thread_latencies[t].push_back(per_rpc_ns);
                            }
                        }
                        for (auto& ps : peer_states) {
                            if (!ps.warmup_ok) continue;
                            ps.global_queries.fetch_add(
                                1, std::memory_order_relaxed);
                        }
                    }
                }
            });
        }

        auto bench_start = Clock::now();
        auto bench_end = bench_start + std::chrono::seconds(FLAGS_duration);
        auto next_statis =
            bench_start + std::chrono::seconds(FLAGS_statis_interval);

        // Per-peer interval state (one prev_* set per peer so the
        // per-peer interval deltas are computed independently).
        std::vector<size_t> prev_keys(num_peers, 0);
        std::vector<size_t> prev_queries(num_peers, 0);
        std::vector<size_t> prev_bytes(num_peers, 0);
        std::vector<size_t> prev_failed(num_peers, 0);
        std::vector<Clock::time_point> prev_time(num_peers, bench_start);
        std::vector<IntervalLatencyStats> interval_stats_list(num_peers);

        // Aggregate interval state.
        size_t prev_keys_agg = 0;
        size_t prev_queries_agg = 0;
        size_t prev_bytes_agg = 0;
        size_t prev_failed_agg = 0;
        auto prev_time_agg = bench_start;
        std::vector<IntervalLatencyStats> interval_agg_list;

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  CLIENT-TO-CLIENT RPC DURATION BENCHMARK "
                     "[batch_get_offload_object, " << num_peers
                  << " peer(s)]\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        while (Clock::now() < bench_end) {
            auto now = Clock::now();
            if (now >= next_statis) {
                // ---- Per-peer interval print ----
                for (size_t p = 0; p < num_peers; ++p) {
                    if (!peer_states[p].warmup_ok) continue;
                    PeerBenchState& ps = peer_states[p];
                    size_t cur_keys =
                        ps.global_keys.load(std::memory_order_relaxed);
                    size_t cur_queries =
                        ps.global_queries.load(std::memory_order_relaxed);
                    size_t cur_bytes =
                        ps.global_bytes.load(std::memory_order_relaxed);
                    size_t cur_failed =
                        ps.global_failed.load(std::memory_order_relaxed);

                    double interval_sec =
                        NanosToSec(ElapsedNanos(prev_time[p], now));
                    size_t interval_keys = cur_keys - prev_keys[p];
                    size_t interval_queries =
                        cur_queries - prev_queries[p];
                    size_t interval_bytes = cur_bytes - prev_bytes[p];
                    size_t interval_failed =
                        cur_failed - prev_failed[p];

                    double interval_mbps =
                        (interval_sec > 0)
                            ? (static_cast<double>(interval_bytes) / MB) /
                                  interval_sec
                            : 0;
                    double interval_qps =
                        (interval_sec > 0)
                            ? static_cast<double>(interval_queries) /
                                  interval_sec
                            : 0;
                    double interval_kps =
                        (interval_sec > 0)
                            ? static_cast<double>(interval_keys) /
                                  interval_sec
                            : 0;

                    IntervalLatencyStats iv;
                    iv.throughput_mbps = interval_mbps;
                    iv.queries_per_sec = interval_qps;
                    iv.keys_per_sec = interval_kps;
                    for (size_t tt = 0; tt < FLAGS_num_threads; ++tt) {
                        std::lock_guard<std::mutex> lk(
                            ps.latency_mutexes[tt]);
                        iv.latencies_ns.insert(
                            iv.latencies_ns.end(),
                            ps.thread_latencies[tt].begin(),
                            ps.thread_latencies[tt].end());
                        ps.thread_latencies[tt].clear();
                    }
                    iv.Finalize();
                    interval_stats_list[p].Aggregate(iv);

                    double total_sec =
                        NanosToSec(ElapsedNanos(bench_start, now));
                    double total_mbps =
                        (total_sec > 0)
                            ? (static_cast<double>(cur_bytes) / MB) /
                                  total_sec
                            : 0;
                    double total_qps =
                        (total_sec > 0)
                            ? static_cast<double>(cur_queries) / total_sec
                            : 0;

                    std::cout << "  [t=" << std::setw(6) << total_sec
                              << "s]"
                              << "  peer[" << ps.peer_addr
                              << "]  interval: " << interval_mbps
                              << " MB/s, " << interval_qps << " qps"
                              << " (failed=" << interval_failed << ")"
                              << "  lat[us]: avg="
                              << NanosToUs(iv.avg_latency_ns)
                              << ", P50=" << NanosToUs(iv.p50_latency_ns)
                              << ", P99=" << NanosToUs(iv.p99_latency_ns)
                              << "  total: " << cur_queries << " queries, "
                              << cur_keys << " keys, " << total_mbps
                              << " MB/s, " << total_qps << " qps"
                              << " (failed=" << cur_failed << ")\n";

                    prev_keys[p] = cur_keys;
                    prev_queries[p] = cur_queries;
                    prev_bytes[p] = cur_bytes;
                    prev_failed[p] = cur_failed;
                    prev_time[p] = now;
                }

                // ---- Aggregate interval print ----
                {
                    size_t cur_keys = 0, cur_queries = 0, cur_bytes = 0,
                           cur_failed = 0;
                    for (const auto& ps : peer_states) {
                        if (!ps.warmup_ok) continue;
                        cur_keys +=
                            ps.global_keys.load(std::memory_order_relaxed);
                        cur_queries +=
                            ps.global_queries.load(std::memory_order_relaxed);
                        cur_bytes +=
                            ps.global_bytes.load(std::memory_order_relaxed);
                        cur_failed +=
                            ps.global_failed.load(std::memory_order_relaxed);
                    }
                    double interval_sec =
                        NanosToSec(ElapsedNanos(prev_time_agg, now));
                    size_t interval_keys = cur_keys - prev_keys_agg;
                    size_t interval_queries =
                        cur_queries - prev_queries_agg;
                    size_t interval_bytes = cur_bytes - prev_bytes_agg;
                    size_t interval_failed =
                        cur_failed - prev_failed_agg;

                    double interval_mbps =
                        (interval_sec > 0)
                            ? (static_cast<double>(interval_bytes) / MB) /
                                  interval_sec
                            : 0;
                    double interval_qps =
                        (interval_sec > 0)
                            ? static_cast<double>(interval_queries) /
                                  interval_sec
                            : 0;
                    double interval_kps =
                        (interval_sec > 0)
                            ? static_cast<double>(interval_keys) /
                                  interval_sec
                            : 0;

                    IntervalLatencyStats iv;
                    iv.throughput_mbps = interval_mbps;
                    iv.queries_per_sec = interval_qps;
                    iv.keys_per_sec = interval_kps;
                    for (auto& ps : peer_states) {
                        if (!ps.warmup_ok) continue;
                        for (size_t tt = 0; tt < FLAGS_num_threads; ++tt) {
                            std::lock_guard<std::mutex> lk(
                                ps.latency_mutexes[tt]);
                            iv.latencies_ns.insert(
                                iv.latencies_ns.end(),
                                ps.thread_latencies[tt].begin(),
                                ps.thread_latencies[tt].end());
                            ps.thread_latencies[tt].clear();
                        }
                    }
                    iv.Finalize();
                    interval_agg_list.push_back(iv);

                    double total_sec =
                        NanosToSec(ElapsedNanos(bench_start, now));
                    double total_mbps =
                        (total_sec > 0)
                            ? (static_cast<double>(cur_bytes) / MB) /
                                  total_sec
                            : 0;
                    double total_qps =
                        (total_sec > 0)
                            ? static_cast<double>(cur_queries) / total_sec
                            : 0;

                    std::cout << "  [t=" << std::setw(6) << total_sec
                              << "s]"
                              << "  AGGREGATE   interval: " << interval_mbps
                              << " MB/s, " << interval_qps << " qps"
                              << " (failed=" << interval_failed << ")"
                              << "  lat[us]: avg="
                              << NanosToUs(iv.avg_latency_ns)
                              << ", P50=" << NanosToUs(iv.p50_latency_ns)
                              << ", P99=" << NanosToUs(iv.p99_latency_ns)
                              << "  total: " << cur_queries << " queries, "
                              << cur_keys << " keys, " << total_mbps
                              << " MB/s, " << total_qps << " qps"
                              << " (failed=" << cur_failed << ")\n";

                    prev_keys_agg = cur_keys;
                    prev_queries_agg = cur_queries;
                    prev_bytes_agg = cur_bytes;
                    prev_failed_agg = cur_failed;
                    prev_time_agg = now;
                }

                next_statis += std::chrono::seconds(FLAGS_statis_interval);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        stop_flag.store(true, std::memory_order_relaxed);
        for (auto& th : threads) th.join();

        // ---- Per-peer final summary ----
        auto per_peer_final_time = Clock::now();
        for (size_t p = 0; p < num_peers; ++p) {
            if (!peer_states[p].warmup_ok) continue;
            PeerBenchState& ps = peer_states[p];
            double total_sec =
                NanosToSec(ElapsedNanos(bench_start, per_peer_final_time));
            size_t final_keys =
                ps.global_keys.load(std::memory_order_relaxed);
            size_t final_queries =
                ps.global_queries.load(std::memory_order_relaxed);
            size_t final_bytes =
                ps.global_bytes.load(std::memory_order_relaxed);
            size_t final_failed =
                ps.global_failed.load(std::memory_order_relaxed);

            double final_mbps =
                (total_sec > 0)
                    ? (static_cast<double>(final_bytes) / MB) / total_sec
                    : 0;
            double final_qps =
                (total_sec > 0)
                    ? static_cast<double>(final_queries) / total_sec
                    : 0;
            double final_kps =
                (total_sec > 0)
                    ? static_cast<double>(final_keys) / total_sec
                    : 0;

            const IntervalLatencyStats& overall = interval_stats_list[p];

            std::cout << "\n  FINAL SUMMARY [peer=" << ps.peer_addr << "]\n";
            std::cout << "  Total time:       " << total_sec << " s\n";
            std::cout << "  Total queries:    " << final_queries
                      << " (failed: " << final_failed << ")\n";
            std::cout << "  Total keys:       " << final_keys << "\n";
            std::cout << "  Total data:       " << FormatBytes(final_bytes)
                      << "  (response bytes, not payload)\n";
            std::cout << "  Throughput:       " << final_mbps << " MB/s";
            if (final_mbps > 1024)
                std::cout << " (" << final_mbps / 1024 << " GB/s)";
            std::cout << "\n";
            std::cout << "  Keys/sec:         " << final_kps << "\n";
            std::cout << "  Queries/sec:      " << final_qps << "\n";

            if (overall.total_samples > 0) {
                std::cout << "\n  Latency (us)      [n="
                          << overall.total_samples << ", per-RPC]\n";
                std::cout << "    Min:   " << std::setw(12)
                          << NanosToUs(overall.min_latency_ns) << "\n";
                std::cout << "    Avg:   " << std::setw(12)
                          << NanosToUs(overall.avg_latency_ns) << "\n";
                std::cout << "    P50:   " << std::setw(12)
                          << NanosToUs(overall.p50_latency_ns) << "\n";
                std::cout << "    P90:   " << std::setw(12)
                          << NanosToUs(overall.p90_latency_ns) << "\n";
                std::cout << "    P99:   " << std::setw(12)
                          << NanosToUs(overall.p99_latency_ns) << "\n";
                std::cout << "    P999:  " << std::setw(12)
                          << NanosToUs(overall.p999_latency_ns);
                if (overall.total_samples < 1000) std::cout << "  (n<1000)";
                std::cout << "\n";
                std::cout << "    P9999: " << std::setw(12)
                          << NanosToUs(overall.p9999_latency_ns);
                if (overall.total_samples < 10000)
                    std::cout << "  (n<10000)";
                std::cout << "\n";
                std::cout << "    Max:   " << std::setw(12)
                          << NanosToUs(overall.max_latency_ns) << "\n";
            }
        }

        // ---- Aggregate final summary ----
        {
            size_t final_keys = 0, final_queries = 0, final_bytes = 0,
                   final_failed = 0;
            for (const auto& ps : peer_states) {
                if (!ps.warmup_ok) continue;
                final_keys +=
                    ps.global_keys.load(std::memory_order_relaxed);
                final_queries +=
                    ps.global_queries.load(std::memory_order_relaxed);
                final_bytes +=
                    ps.global_bytes.load(std::memory_order_relaxed);
                final_failed +=
                    ps.global_failed.load(std::memory_order_relaxed);
            }
            double total_sec =
                NanosToSec(ElapsedNanos(bench_start, per_peer_final_time));
            double final_mbps =
                (total_sec > 0)
                    ? (static_cast<double>(final_bytes) / MB) / total_sec
                    : 0;
            double final_qps =
                (total_sec > 0)
                    ? static_cast<double>(final_queries) / total_sec
                    : 0;
            double final_kps =
                (total_sec > 0)
                    ? static_cast<double>(final_keys) / total_sec
                    : 0;

            IntervalLatencyStats overall;
            for (const auto& s : interval_agg_list) overall.Aggregate(s);

            std::cout << "\n  FINAL SUMMARY [AGGREGATE across all alive "
                         "peers]\n";
            std::cout << "  Total time:       " << total_sec << " s\n";
            std::cout << "  Total queries:    " << final_queries
                      << " (failed: " << final_failed << ")\n";
            std::cout << "  Total keys:       " << final_keys << "\n";
            std::cout << "  Total data:       " << FormatBytes(final_bytes)
                      << "  (response bytes, not payload)\n";
            std::cout << "  Throughput:       " << final_mbps << " MB/s";
            if (final_mbps > 1024)
                std::cout << " (" << final_mbps / 1024 << " GB/s)";
            std::cout << "\n";
            std::cout << "  Keys/sec:         " << final_kps << "\n";
            std::cout << "  Queries/sec:      " << final_qps << "\n";

            if (overall.total_samples > 0) {
                std::cout << "\n  Latency (us)      [n="
                          << overall.total_samples << ", per-RPC]\n";
                std::cout << "    Min:   " << std::setw(12)
                          << NanosToUs(overall.min_latency_ns) << "\n";
                std::cout << "    Avg:   " << std::setw(12)
                          << NanosToUs(overall.avg_latency_ns) << "\n";
                std::cout << "    P50:   " << std::setw(12)
                          << NanosToUs(overall.p50_latency_ns) << "\n";
                std::cout << "    P90:   " << std::setw(12)
                          << NanosToUs(overall.p90_latency_ns) << "\n";
                std::cout << "    P99:   " << std::setw(12)
                          << NanosToUs(overall.p99_latency_ns) << "\n";
                std::cout << "    P999:  " << std::setw(12)
                          << NanosToUs(overall.p999_latency_ns);
                if (overall.total_samples < 1000) std::cout << "  (n<1000)";
                std::cout << "\n";
                std::cout << "    P9999: " << std::setw(12)
                          << NanosToUs(overall.p9999_latency_ns);
                if (overall.total_samples < 10000)
                    std::cout << "  (n<10000)";
                std::cout << "\n";
                std::cout << "    Max:   " << std::setw(12)
                          << NanosToUs(overall.max_latency_ns) << "\n";
            }
        }

        std::cout << "  Note: timed region is exactly the "
                     "batch_get_offload_object RPC round-trip; no master, "
                     "no P2P data, no SSD read.\n";
        std::cout << "========================================"
                  << "========================================\n\n";
        return 0;
    }

    int Run() {
        if (FLAGS_scenario == "local_memory") {
            return RunLocalMemory();
        } else if (FLAGS_scenario == "local_disk") {
            return RunLocalDisk();
        } else if (FLAGS_scenario == "segment_write") {
            return RunSegmentWrite();
        } else if (FLAGS_scenario == "segment_read") {
            return RunSegmentRead();
        } else if (FLAGS_scenario == "list_segments") {
            return RunListSegments();
        } else if (FLAGS_scenario == "client_rpc_bench") {
            return RunClientRpcBench();
        } else if (FLAGS_scenario == "remote_memory" ||
                   FLAGS_scenario == "remote_disk") {
            if (FLAGS_role == "writer") {
                return RunWriter();
            } else {
                return RunReader();
            }
        } else {
            LOG(ERROR) << "Unknown scenario: " << FLAGS_scenario;
            return -1;
        }
    }

   private:
    // Per-peer benchmark state used by RunClientRpcBench* in multi-peer
    // mode. Each peer owns its own per-thread results (single-pass) and
    // its own per-thread latencies / global counters (duration), so the
    // stats for every peer can be reported independently and then
    // merged into an AGGREGATE summary.
    struct PeerBenchState {
        std::string peer_addr;
        // transfer_engine_addr echoed back from the peer's first
        // successful warmup RPC; used for end-to-end sanity checking.
        std::string peer_seen_addr;
        // Set to true if the per-peer warmup completed at least one
        // successful RPC. Peers that fail warmup are kept in the list
        // and skipped at timed-loop time.
        bool warmup_ok = false;

        // Single-pass mode: per-thread results aggregated by
        // BenchmarkStats::Finalize / Print.
        BenchmarkStats stats;

        // Duration mode: per-thread latency samples and global
        // counters. The worker loop visits every alive peer in
        // sequence within each round, and writes each peer's samples /
        // counters into its own PeerBenchState.
        std::vector<std::vector<int64_t>> thread_latencies;
        std::vector<std::mutex> latency_mutexes;
        std::atomic<size_t> global_keys{0};
        std::atomic<size_t> global_queries{0};
        std::atomic<size_t> global_bytes{0};
        std::atomic<size_t> global_failed{0};

        void InitDuration(size_t n_threads) {
            thread_latencies.assign(n_threads, {});
            latency_mutexes.assign(n_threads, {});
            global_keys.store(0);
            global_queries.store(0);
            global_bytes.store(0);
            global_failed.store(0);
        }
    };

    // Merge per-peer per-thread results into a single BenchmarkStats
    // for the AGGREGATE print. Only peers with warmup_ok == true are
    // included. Called only by RunClientRpcBenchSinglePass.
    static BenchmarkStats MergePeerStats(
        const std::vector<PeerBenchState>& peer_states) {
        BenchmarkStats agg;
        if (FLAGS_num_threads == 0) return agg;
        agg.InitThreads(FLAGS_num_threads, /*expected_per_thread=*/0);
        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            ThreadResult& agg_tr = agg.GetThreadResult(t);
            for (const auto& ps : peer_states) {
                if (!ps.warmup_ok) continue;
                const ThreadResult& pr_tr = ps.stats.GetThreadResult(t);
                agg_tr.latencies_ns.insert(agg_tr.latencies_ns.end(),
                                           pr_tr.latencies_ns.begin(),
                                           pr_tr.latencies_ns.end());
                agg_tr.total_bytes += pr_tr.total_bytes;
                agg_tr.total_keys += pr_tr.total_keys;
                agg_tr.total_queries += pr_tr.total_queries;
                agg_tr.failed_ops += pr_tr.failed_ops;
            }
        }
        agg.Finalize();
        return agg;
    }

    static std::string MakeKey(size_t idx) {
        return "bench_key_" + std::to_string(idx);
    }

    void FillBuffer(size_t seed) {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(buffer_);
        size_t num_words = FLAGS_value_size / sizeof(uint64_t);
        uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
        for (size_t w = 0; w < num_words; ++w) {
            pattern = (pattern ^ (pattern >> 30)) * 0xBF58476D1CE4E5B9ULL;
            pattern = (pattern ^ (pattern >> 27)) * 0x94D049BB133111EBULL;
            ptr[w] = pattern ^ (pattern >> 31);
        }
    }

    bool CheckBuffer(size_t seed, const void* data, size_t size) const {
        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(data);
        size_t num_words = size / sizeof(uint64_t);
        uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
        for (size_t w = 0; w < num_words; ++w) {
            pattern = (pattern ^ (pattern >> 30)) * 0xBF58476D1CE4E5B9ULL;
            pattern = (pattern ^ (pattern >> 27)) * 0x94D049BB133111EBULL;
            uint64_t expected = pattern ^ (pattern >> 31);
            if (ptr[w] != expected) {
                LOG(ERROR) << "Checksum mismatch at word " << w
                           << " for seed=" << seed << " expected=" << std::hex
                           << expected << " got=" << ptr[w] << std::dec;
                return false;
            }
        }
        return true;
    }

    int DoWarmup() {
        if (FLAGS_warmup_keys == 0) return 0;
        LOG(INFO) << "Warmup: reading " << FLAGS_warmup_keys << " keys...";

        size_t warmup_end = std::min(static_cast<size_t>(FLAGS_warmup_keys),
                                     static_cast<size_t>(FLAGS_num_keys));
        for (size_t i = 0; i < warmup_end; ++i) {
            std::string key = MakeKey(i);
            int64_t ret = main_buffer_client_->get_into(key, buffer_, FLAGS_value_size);
            if (ret < 0) {
                LOG(WARNING) << "Warmup get_into failed for key=" << key
                             << " ret=" << ret;
            }
        }
        LOG(INFO) << "Warmup complete";
        return 0;
    }

    void BatchReadWorker(size_t tid, size_t my_keys, size_t key_offset,
                         BenchmarkStats& stats, std::latch& start_latch,
                         std::latch& done_latch,
                         const std::function<std::string(size_t)>& key_func,
                         std::shared_ptr<mooncake::PyClient> thread_client) {
        bindToSocket(tid % NR_SOCKETS);

        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_keys);

        char* my_buf = thread_buffers_[tid].ptr;

        start_latch.arrive_and_wait();

        size_t keys = 0;
        size_t queries = 0;
        size_t failed = 0;
        size_t bytes = 0;

        if (FLAGS_batch_size <= 1) {
            for (size_t i = 0; i < my_keys; ++i) {
                size_t key_idx = key_offset + i;
                std::string key = key_func(key_idx);

                auto t0 = Clock::now();
                int64_t ret =
                    thread_client->get_into(key, my_buf, FLAGS_value_size);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);

                if (ret < 0) {
                    ++failed;
                    LOG_EVERY_N(ERROR, 100)
                        << "get_into failed key=" << key << " ret=" << ret;
                } else {
                    bytes += static_cast<size_t>(ret);
                }
                result.latencies_ns.push_back(lat_ns);
                ++keys;
                ++queries;
            }
        } else {
            size_t per_key_buf = FLAGS_value_size;
            size_t i = 0;
            while (i < my_keys) {
                std::vector<std::string> key_list;
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                size_t batch_end = std::min(i + FLAGS_batch_size, my_keys);
                key_list.reserve(batch_end - i);
                bufs.reserve(batch_end - i);
                sizes.reserve(batch_end - i);

                for (size_t j = i; j < batch_end; ++j) {
                    size_t key_idx = key_offset + j;
                    key_list.push_back(key_func(key_idx));
                    bufs.push_back(my_buf + (j - i) * per_key_buf);
                    sizes.push_back(FLAGS_value_size);
                }

                auto t0 = Clock::now();
                auto results =
                    thread_client->batch_get_into(key_list, bufs, sizes);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);
                result.latencies_ns.push_back(lat_ns);

                for (size_t k = 0; k < results.size(); ++k) {
                    if (results[k] < 0) {
                        ++failed;
                    } else {
                        bytes += static_cast<size_t>(results[k]);
                    }
                    ++keys;
                }
                ++queries;

                i = batch_end;
            }
        }

        result.total_bytes = bytes;
        result.total_keys = keys;
        result.total_queries = queries;
        result.failed_ops = failed;

        done_latch.arrive_and_wait();
    }

    std::vector<std::thread> LaunchReadWorkers(
        size_t num_threads, size_t total_keys, BenchmarkStats& stats,
        std::latch& start_latch, std::latch& done_latch,
        const std::function<std::string(size_t)>& key_func) {
        std::vector<std::thread> threads;
        size_t keys_per_thread = total_keys / num_threads;
        size_t remainder = total_keys % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            // Per-thread client: a separate DummyClient for dummy mode
            // (one per reader thread, isolated SHM and RPC), or the
            // shared RealClient for real mode.
            std::shared_ptr<mooncake::PyClient> thread_client;
            if (is_dummy_) {
                thread_client = dummy_clients_[t];
            } else {
                thread_client = client_;
            }

            threads.emplace_back(
                [&, t, my_keys, key_offset, thread_client]() {
                    BatchReadWorker(t, my_keys, key_offset, stats,
                                    start_latch, done_latch, key_func,
                                    thread_client);
                });
        }
        return threads;
    }

    std::vector<std::string> DiscoverSegmentsIfNeeded(
        const std::string& context) {
        auto segments = ParseSegments();
        if (!segments.empty()) {
            return segments;
        }

        LOG(INFO) << context << ", auto-discovering from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;
        segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server, static_cast<int>(FLAGS_master_admin_port));
        if (segments.empty()) {
            LOG(ERROR) << "No segments discovered from master. "
                       << "Check master connectivity.";
        }
        return segments;
    }

    int VerifyData() {
        LOG(INFO) << "Verifying data integrity for " << FLAGS_num_keys
                  << " keys...";
        int errors = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            int64_t ret = main_buffer_client_->get_into(key, buffer_, FLAGS_value_size);
            if (ret < 0) {
                LOG(ERROR) << "Verify: get_into failed for key=" << key;
                ++errors;
                continue;
            }
            if (!CheckBuffer(i, buffer_, static_cast<size_t>(ret))) {
                LOG(ERROR) << "Verify: data mismatch for key=" << key;
                ++errors;
            }
        }

        LOG(INFO) << "Verification complete: " << errors << " errors out of "
                  << FLAGS_num_keys << " keys";
        return errors > 0 ? -1 : 0;
    }

    // Real client (single, used for real mode only).
    std::shared_ptr<mooncake::PyClient> client_;
    // For dummy mode: a dedicated DummyClient that owns the main buffer
    // (buffer_). The single-threaded writer, warmup, and verify paths all
    // operate on buffer_ via this client.
    std::shared_ptr<mooncake::DummyClient> primary_dummy_client_;
    // For dummy mode: one DummyClient per reader thread. Each thread's SHM
    // segment and RPC channel are isolated to that thread's DummyClient, so
    // multiple readers can run get_into / batch_get_into concurrently
    // without contending on a single client (the per-thread setup matches
    // the original Go test pattern that creates an independent store per
    // goroutine / process).
    std::vector<std::shared_ptr<mooncake::DummyClient>> dummy_clients_;
    // The client that owns the main buffer (buffer_). For real mode this is
    // client_; for dummy mode this is primary_dummy_client_. Use this in
    // single-threaded writer / warmup / verify code paths.
    std::shared_ptr<mooncake::PyClient> main_buffer_client_;
    char* buffer_;
    size_t buffer_size_;
    // True if the underlying client is a DummyClient. DummyClient::register_buffer
    // requires the memory to be inside a ShmHelper-managed segment (memfd+mmap),
    // because the address+fd is later passed to the real client via IPC. A
    // plain numa_alloc_local buffer would be rejected with "Buffer is not
    // in any registered shared memory". Track this so Setup / destructor /
    // AllocateThreadBuffers can pick the right allocator.
    bool is_dummy_ = false;

    // Build and connect a single DummyClient. Returns nullptr on failure.
    // Per-thread dummy clients are created with local_buffer_size=0 so they
    // do not pre-allocate a SHM segment; AllocateThreadBuffers will then
    // allocate the per-thread buffer via ShmHelper and register it. This
    // gives every thread its own (SHM, RPC, IPC) triple and avoids sharing
    // one client across threads.
    std::shared_ptr<mooncake::DummyClient> CreateDummyClient(size_t local_buffer_size = 0) {
        size_t mem_pool = FLAGS_dummy_mem_pool_size > 0
                              ? FLAGS_dummy_mem_pool_size
                              : FLAGS_global_segment_size;
        if (FLAGS_dummy_server_address.empty() ||
            FLAGS_dummy_ipc_socket_path.empty()) {
            LOG(ERROR)
                << "Dummy client requires non-empty --dummy_server_address "
                   "and --dummy_ipc_socket_path";
            return nullptr;
        }
        auto dummy = std::make_shared<mooncake::DummyClient>();
        int ret = dummy->setup_dummy(mem_pool, local_buffer_size,
                                     FLAGS_dummy_server_address,
                                     FLAGS_dummy_ipc_socket_path);
        if (ret != 0) {
            LOG(ERROR) << "DummyClient setup_dummy failed, ret=" << ret;
            return nullptr;
        }
        return dummy;
    }

    // Returns a NUMA-local buffer for RealClient, or a ShmHelper segment for
    // DummyClient. Caller owns the buffer and must release it with
    // FreeBuffer().
    char* AllocateBuffer(size_t size, int numa_node = -1) {
        if (is_dummy_) {
            try {
                return static_cast<char*>(
                    mooncake::ShmHelper::getInstance()->allocate(size));
            } catch (const std::exception& e) {
                LOG(ERROR) << "ShmHelper::allocate(" << size
                           << ") failed: " << e.what();
                return nullptr;
            }
        }
        if (numa_node >= 0) {
            return reinterpret_cast<char*>(numa_alloc_onnode(size, numa_node));
        }
        return reinterpret_cast<char*>(numa_alloc_local(size));
    }

    // Counterpart of AllocateBuffer.
    void FreeBuffer(char* ptr, size_t size) {
        if (!ptr) return;
        if (is_dummy_) {
            if (mooncake::ShmHelper::getInstance()->free(ptr) != 0) {
                LOG(WARNING) << "ShmHelper::free(" << ptr << ") failed";
            }
            return;
        }
        numa_free(ptr, size);
    }

    struct ThreadBuffer {
        char* ptr = nullptr;
        size_t size = 0;
        int numa_node = -1;
    };
    std::vector<ThreadBuffer> thread_buffers_;

    int AllocateThreadBuffers(size_t num_threads) {
        thread_buffers_.resize(num_threads);
        dummy_clients_.clear();
        dummy_clients_.reserve(num_threads);
        size_t per_buf_size = FLAGS_batch_size * FLAGS_value_size;
        for (size_t t = 0; t < num_threads; ++t) {
            int node = t % NR_SOCKETS;
            thread_buffers_[t].size = per_buf_size;
            thread_buffers_[t].numa_node = node;

            // Pick (or create) the client that will own this thread's
            // buffer. For dummy mode, every thread gets its own
            // DummyClient so its SHM and RPC channel are isolated; for
            // real mode all threads share the single RealClient.
            std::shared_ptr<mooncake::PyClient> thread_client;
            if (is_dummy_) {
                auto dc = CreateDummyClient(/*local_buffer_size=*/0);
                if (!dc) {
                    LOG(ERROR) << "Failed to create DummyClient for thread "
                               << t
                               << " (will roll back " << t
                               << " already-allocated thread(s))";
                    RollbackThreadBuffers(t);
                    return -1;
                }
                dummy_clients_.push_back(dc);
                thread_client = dc;
            } else {
                thread_client = client_;
            }

            thread_buffers_[t].ptr = AllocateBuffer(per_buf_size, node);
            if (!thread_buffers_[t].ptr) {
                LOG(ERROR) << "Failed to allocate buffer for thread " << t
                           << " on NUMA node " << node << " ("
                           << (is_dummy_ ? "ShmHelper" : "numa")
                           << "); will roll back " << t
                           << " already-allocated thread(s)";
                // The buffer failed to allocate, so this thread's dummy
                // client owns no buffer; tear it down to release its
                // RPC/IPC connection.
                if (is_dummy_ && !dummy_clients_.empty()) {
                    try {
                        dummy_clients_.back()->tearDownAll();
                    } catch (...) {
                        LOG(WARNING)
                            << "Failed to tearDownAll dummy client for "
                            << "thread " << t << ", ignoring";
                    }
                    dummy_clients_.pop_back();
                }
                RollbackThreadBuffers(t);
                return -1;
            }
            std::memset(thread_buffers_[t].ptr, 0, per_buf_size);

            int ret = thread_client->register_buffer(
                thread_buffers_[t].ptr, per_buf_size);
            if (ret != 0) {
                LOG(ERROR) << "register_buffer failed for thread " << t
                           << " on NUMA node " << node
                           << " (is_dummy=" << is_dummy_
                           << "); will roll back " << t
                           << " already-allocated thread(s)";
                // Try to unregister with the same client (best effort).
                try {
                    thread_client->unregister_buffer(
                        thread_buffers_[t].ptr);
                } catch (...) {
                    LOG(WARNING) << "Best-effort unregister after "
                                    "register_buffer failure for thread "
                                 << t << " failed, ignoring";
                }
                FreeBuffer(thread_buffers_[t].ptr, per_buf_size);
                thread_buffers_[t].ptr = nullptr;
                if (is_dummy_ && !dummy_clients_.empty()) {
                    try {
                        dummy_clients_.back()->tearDownAll();
                    } catch (...) {
                        LOG(WARNING)
                            << "Failed to tearDownAll dummy client for "
                            << "thread " << t << ", ignoring";
                    }
                    dummy_clients_.pop_back();
                }
                RollbackThreadBuffers(t);
                return ret;
            }
        }
        LOG(INFO) << "Allocated " << num_threads << " thread buffers, each "
                  << per_buf_size / MB << " MB ("
                  << (is_dummy_ ? "per-thread ShmHelper, "
                                : "NUMA-aware, ")
                  << NR_SOCKETS << " sockets)";
        return 0;
    }

    // Roll back all thread buffers in [0, count). Called when
    // AllocateThreadBuffers fails partway through: we have to unregister
    // each already-registered buffer with its owning dummy client (so the
    // real client side releases the IPC fd) and free the SHM, then
    // tearDownAll each dummy client (so its RPC + ping thread exit).
    // After this call, thread_buffers_ and dummy_clients_ are empty.
    void RollbackThreadBuffers(size_t count) {
        for (size_t t = 0; t < count; ++t) {
            auto& tb = thread_buffers_[t];
            if (!tb.ptr) continue;
            std::shared_ptr<mooncake::PyClient> thread_client;
            if (is_dummy_ && t < dummy_clients_.size() &&
                dummy_clients_[t]) {
                thread_client = dummy_clients_[t];
            } else {
                thread_client = client_;
            }
            if (thread_client) {
                try {
                    thread_client->unregister_buffer(tb.ptr);
                } catch (...) {
                    LOG(WARNING) << "Rollback: failed to unregister "
                                    "thread "
                                 << t << " buffer, ignoring";
                }
            }
            FreeBuffer(tb.ptr, tb.size);
            tb.ptr = nullptr;
        }
        // Free any DummyClient entries left (those whose buffers were
        // never allocated or whose buffers we already cleaned up
        // inline). tearDownAll stops the ping thread and closes the
        // IPC / RPC channels.
        for (auto& dc : dummy_clients_) {
            if (!dc) continue;
            try {
                dc->tearDownAll();
            } catch (...) {
                LOG(WARNING) << "Rollback: failed to tearDownAll dummy "
                                "client, ignoring";
            }
        }
        dummy_clients_.clear();
        thread_buffers_.clear();
    }
};

int main(int argc, char* argv[]) {
    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(argv[0]);
    }
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (std::getenv("MC_LOG_DIR") == nullptr) {
        FLAGS_logtostderr = true;
    }
    mooncake::logging::ApplyMooncakeLogEnableToGlog();

    LOG(INFO) << "Mooncake Stress Cluster Benchmark";
    LOG(INFO) << "  Scenario:       " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:       " << FLAGS_protocol;
    LOG(INFO) << "  Client type:    " << FLAGS_client_type;
    if (FLAGS_client_type == "dummy") {
        LOG(INFO) << "  Dummy server:   " << FLAGS_dummy_server_address;
        LOG(INFO) << "  Dummy IPC path: " << FLAGS_dummy_ipc_socket_path;
    }
    LOG(INFO) << "  Value size:     " << FLAGS_value_size / MB << " MB";
    LOG(INFO) << "  Num keys:       " << FLAGS_num_keys;
    LOG(INFO) << "  Batch size:     " << FLAGS_batch_size;
    LOG(INFO) << "  Num threads:    " << FLAGS_num_threads;
    LOG(INFO) << "  Hard pin:       " << (FLAGS_hard_pin ? "yes" : "no");
    LOG(INFO) << "  SSD offload:    "
              << (FLAGS_enable_ssd_offload ? "yes" : "no");
    if (!FLAGS_segments.empty()) {
        LOG(INFO) << "  Segments:       " << FLAGS_segments;
    } else {
        LOG(INFO) << "  Segments:       auto-discover from master";
    }
    LOG(INFO) << "  Master admin:   " << FLAGS_master_admin_port;
    LOG(INFO) << "  Read seg nums:  " << FLAGS_read_segment_nums;
    LOG(INFO) << "  Duration:       " << FLAGS_duration << "s";
    LOG(INFO) << "  Stats interval: " << FLAGS_statis_interval << "s";

    size_t total_data = FLAGS_num_keys * FLAGS_value_size;
    if (total_data > FLAGS_global_segment_size * 9.5 / 10) {
        LOG(WARNING) << "Total data (" << total_data / MB << " MB) may exceed "
                     << "95% of segment (" << FLAGS_global_segment_size / MB
                     << " MB). Master eviction may delete objects. "
                     << "Consider increasing --global_segment_size or "
                     << "decreasing --num_keys, or use --hard_pin=true.";
    }

    StressBenchmark bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Benchmark setup failed";
        return ret;
    }

    ret = bench.Run();
    return ret;
}
