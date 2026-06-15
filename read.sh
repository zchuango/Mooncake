#!/bin/bash
set -euo pipefail

export LD_LIBRARY_PATH=/usr/local/lib/python3.11/site-packages/nvidia/cuda_runtime/lib:$LD_LIBRARY_PATH
export MC_STORE_CLIENT_SETUP_RETRIES=3
export MC_STORE_CLIENT_METRIC_BANDWIDTH=1
export MC_TCP_BIND_ADDRESS=141.61.84.245
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.17.0/24,141.61.11.0/24,141.61.84.0/24"
export MC_LOG_ENABLE=on
export MC_LOG_LEVEL=INFO
# export MC_LOG_DIR=/var/log/mooncake
export MC_LOG_DETAIL_ENABLE=off
export MC_LOG_MAX_SIZE=100
export MC_LOG_BUFFER_SECS=3
export MC_HIFREQ_LOG_SAMPLE_RATE=0.1

BENCH=./build/mooncake-store/benchmarks/stress_cluster_bench

echo "============================================"
echo "  READ BENCHMARK START: $(date)"
echo "============================================"

# Run benchmark: stdout=summary, stderr=glog
# Use temp files so we never lose the summary even if the terminal scrolls away
READ_STDOUT=$(mktemp)
READ_STDERR=$(mktemp)

set +e
$BENCH \
        --role=reader \
        --global-segment-size=0 \
        --local-buffer-size=1073741824 \
        --local-hostname=141.61.84.245 \
        --master-server=141.61.84.245:50060 \
        --master_admin_port=9010 \
        --device-name=bonding_dev_0 \
        --metadata-server='http://141.61.84.245:8020/metadata' \
        --scenario=segment_read \
        --num-keys=1000 \
        --protocol=ub \
        --verify=false \
        --num_threads=16 \
        --batch-size=16 \
        --duration=20 \
        >"$READ_STDOUT" 2>"$READ_STDERR"
READ_EXIT=$?
set -e

echo ""
echo "============================================"
echo "  READ BENCHMARK SUMMARY"
echo "============================================"
echo "  Exit code: $READ_EXIT"
echo "  End time:  $(date)"

if [ $READ_EXIT -ne 0 ]; then
    echo "  STATUS: FAILED"
    echo ""
    echo "  Last 15 lines of stderr:"
    echo "  ------------------------"
    tail -15 "$READ_STDERR"
    echo "  ------------------------"
else
    echo "  STATUS: PASSED"
fi

# Print the benchmark's own formatted summary (it goes to stdout)
echo ""
echo "============================================"
echo "  BENCHMARK OUTPUT (stdout)"
echo "============================================"
cat "$READ_STDOUT"
echo "============================================"

# Extract key metrics for quick glance
echo ""
echo "============================================"
echo "  KEY METRICS"
echo "============================================"
grep -E '(Wall time|Total ops|Throughput|Ops/sec|Mean|P50|P99|P999)' "$READ_STDOUT" || echo "  (no metrics found)"

# Check for common error patterns in stderr
if grep -qi 'segfault\|SIGSEGV\|signal\|fatal\|corrupted\|double free' "$READ_STDERR"; then
    echo ""
    echo "  *** CRITICAL ERRORS DETECTED IN STDERR ***"
    grep -i 'segfault\|SIGSEGV\|signal\|fatal\|corrupted\|double free' "$READ_STDERR"
fi

rm -f "$READ_STDOUT" "$READ_STDERR"

echo ""
echo "============================================"
echo "  READ BENCHMARK COMPLETE: $(date)"
echo "============================================"
