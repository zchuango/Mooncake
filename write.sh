#!/bin/bash
set -euo pipefail

export MC_STORE_CLIENT_SETUP_RETRIES=3
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.17.0/24,141.61.11.0/24,141.61.84.0/24"
export MC_STORE_CLIENT_METRIC_BANDWIDTH=0
export MC_TCP_BIND_ADDRESS=141.61.84.245
export MC_SLICE_SIZE=1048576
export MC_WORKERS_PER_CTX=4
export MC_MAX_WR=32
export MC_URMA_TRANS_MODE=RM
export MC_LOG_ENABLE=on
export MC_LOG_LEVEL=INFO
# export MC_LOG_DIR=/var/log/mooncake
export MC_LOG_DETAIL_ENABLE=off
export MC_LOG_MAX_SIZE=100
export MC_LOG_BUFFER_SECS=3
export MC_HIFREQ_LOG_SAMPLE_RATE=0.1

BENCH=./build/mooncake-store/benchmarks/stress_cluster_bench

echo "============================================"
echo "  WRITE BENCHMARK START: $(date)"
echo "============================================"

WRITE_STDOUT=$(mktemp)
WRITE_STDERR=$(mktemp)

set +e
$BENCH \
        --metadata-server='http://141.61.84.245:8020/metadata' \
        --master-server='141.61.84.245:50060' \
        --local-hostname=$MC_TCP_BIND_ADDRESS \
        --master_admin_port=9010 \
        --global-segment-size=0 \
        --local-buffer-size=536870912 \
        --device-name=udmac0d1e2 \
        --scenario=segment_write \
        --num-keys=1000 \
        --protocol=ub \
        --verify=false \
        --num_threads=32 \
        --batch-size=32 \
        >"$WRITE_STDOUT" 2>"$WRITE_STDERR"
WRITE_EXIT=$?
set -e

sleep 5

echo ""
echo "============================================"
echo "  WRITE BENCHMARK SUMMARY"
echo "============================================"
echo "  Exit code: $WRITE_EXIT"
echo "  End time:  $(date)"

if [ $WRITE_EXIT -ne 0 ]; then
    echo "  STATUS: FAILED"
    echo ""
    echo "  Last 15 lines of stderr:"
    echo "  ------------------------"
    tail -15 "$WRITE_STDERR"
    echo "  ------------------------"
else
    echo "  STATUS: PASSED"
fi

# Print benchmark output
echo ""
echo "============================================"
echo "  BENCHMARK OUTPUT (stdout)"
echo "============================================"
cat "$WRITE_STDOUT"
echo "============================================"

# Extract write progress from logs
echo ""
echo "============================================"
echo "  KEY METRICS"
echo "============================================"
grep -E '(succeeded|failed|complete|Written|All segments)' "$WRITE_STDOUT" "$WRITE_STDERR" 2>/dev/null || echo "  (check log for details)"

# Check for errors
if grep -qi 'segfault\|SIGSEGV\|signal\|fatal\|corrupted\|double free' "$WRITE_STDERR"; then
    echo ""
    echo "  *** CRITICAL ERRORS DETECTED IN STDERR ***"
    grep -i 'segfault\|SIGSEGV\|signal\|fatal\|corrupted\|double free' "$WRITE_STDERR"
fi

rm -f "$WRITE_STDOUT" "$WRITE_STDERR"

echo ""
echo "============================================"
echo "  WRITE BENCHMARK COMPLETE: $(date)"
echo "============================================"
