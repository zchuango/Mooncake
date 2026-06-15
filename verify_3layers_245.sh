#!/bin/bash
# ============================================================================
# Mooncake UbDiag 三层分发 — 245 aarch64 全验证脚本
# ============================================================================
# 一键运行：bash verify_3layers_245.sh
#
# 自动完成：
#   1. 三层分发各编译一遍（Layer1 submodule / Layer2 system / Layer3 mock）
#   2. 每层启动 master → client → write.sh → read.sh 全流程
#   3. 每层输出 summary（Wall time / Throughput / Ops/sec / Latency）
#   4. 最终汇总三层结果
# ============================================================================

set -euo pipefail

# ---- 配置 ----
PROJECT_DIR="/home/q00913006/project/mooncake-qyf"
RESULTS_DIR="$PROJECT_DIR/verify_3layers_$(date +%Y%m%d_%H%M%S)"
SCRIPTS_DIR="$PROJECT_DIR"
UBDIAG_SRC="$PROJECT_DIR/extern/ubdiag"
UBDIAG_INSTALL_PREFIX="$HOME/.local/ubdiag_layer2"

# cmake 参数（与 245 之前编译一致）
CMAKE_ARGS=(
    -DWITH_STORE=ON -DWITH_P2P_STORE=OFF
    -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF
    -DUSE_CUDA=OFF -DUSE_REDIS=OFF -DUSE_ETCD=OFF -DSTORE_USE_ETCD=OFF
    -DENABLE_OB_MEMORY=OFF -DENABLE_OB_CACHE=OFF -DENABLE_MEMPOINT=OFF
    -DENABLE_PERFLOG=ON -DUSE_UB=ON
)

# ---- 辅助函数 ----

kill_mooncake() {
    echo "  [cleanup] Killing mooncake processes..."
    pkill -f mooncake_master 2>/dev/null || true
    pkill -f mooncake_client 2>/dev/null || true
    pkill -f stress_cluster_bench 2>/dev/null || true
    sleep 3
    local waited=0
    while ss -tlnp 2>/dev/null | grep -qE ':(50060|8020|9010|8980) '; do
        echo "  [cleanup] Waiting for ports to release..."
        sleep 2
        waited=$((waited + 2))
        if [ $waited -ge 30 ]; then
            echo "  [cleanup] WARNING: ports still in use after 30s, force killing..."
            fuser -k 50060/tcp 2>/dev/null || true
            fuser -k 8020/tcp 2>/dev/null || true
            break
        fi
    done
    echo "  [cleanup] Done."
}

check_ubdiag_layer() {
    local cmake_log=$1
    local layer=$2
    local pattern=""
    case $layer in
        1) pattern="using submodule" ;;
        2) pattern="using system package" ;;
        3) pattern="using mock" ;;
    esac
    if grep -q "UbDiag: ${pattern}" "$cmake_log" 2>/dev/null; then
        echo "  [OK] Layer $layer hit: UbDiag: ${pattern}"
        return 0
    else
        echo "  [FAIL] Layer $layer NOT hit! Expected 'UbDiag: ${pattern}'"
        echo "  Actual cmake output:"
        grep 'UbDiag:' "$cmake_log" 2>/dev/null || echo "  (no UbDiag line found)"
        return 1
    fi
}

# link_build <build_dir>
# 创建 ./build → <build_dir> 符号链接，使 4 个脚本里的 ./build/... 路径生效
link_build() {
    local target=$1
    cd "$PROJECT_DIR"
    rm -f build
    ln -sfn "$target" build
    echo "  [link] build -> $target"
}

run_4scripts() {
    local log_dir=$1

    echo "  [run] Starting master..."
    bash "$SCRIPTS_DIR/run_mooncake_store_master.sh" >"$log_dir/01_master.log" 2>&1 &
    MASTER_PID=$!
    sleep 3

    if ! kill -0 $MASTER_PID 2>/dev/null; then
        echo "  [FAIL] Master failed to start (PID $MASTER_PID)"
        echo "  --- master log ---"
        tail -30 "$log_dir/01_master.log"
        return 1
    fi
    echo "  [run] Master PID=$MASTER_PID"

    echo "  [run] Starting client..."
    bash "$SCRIPTS_DIR/run_mooncake_store_client.sh" >"$log_dir/02_client.log" 2>&1 &
    CLIENT_PID=$!
    sleep 3

    if ! kill -0 $CLIENT_PID 2>/dev/null; then
        echo "  [FAIL] Client failed to start (PID $CLIENT_PID)"
        echo "  --- client log ---"
        tail -30 "$log_dir/02_client.log"
        kill $MASTER_PID 2>/dev/null || true
        return 1
    fi
    echo "  [run] Client PID=$CLIENT_PID"

    echo "  [run] Running write.sh..."
    bash "$SCRIPTS_DIR/write.sh" >"$log_dir/03_write.log" 2>&1
    WRITE_EXIT=$?
    echo "  [run] write.sh exit=$WRITE_EXIT"
    grep -E '(STATUS|succeeded|failed|complete|Total ops|Throughput|Wall time)' "$log_dir/03_write.log" 2>/dev/null | head -10

    echo "  [run] Running read.sh..."
    bash "$SCRIPTS_DIR/read.sh" >"$log_dir/04_read.log" 2>&1
    READ_EXIT=$?
    echo "  [run] read.sh exit=$READ_EXIT"
    grep -E '(STATUS|Throughput|Ops/sec|Wall time|Total ops|P50|P99)' "$log_dir/04_read.log" 2>/dev/null | head -10

    # Write per-layer summary
    {
        echo "write_exit=$WRITE_EXIT"
        echo "read_exit=$READ_EXIT"
        echo ""
        echo "--- read metrics ---"
        grep -E '(Wall time|Total ops|Throughput|Ops/sec|STATUS|Mean|P50|P99)' "$log_dir/04_read.log" 2>/dev/null || echo "(no metrics)"
    } > "$log_dir/00_summary.txt"

    kill_mooncake
    return 0
}

# ---- 前置检查 ----
echo "============================================"
echo "  Mooncake UbDiag 三层分发全验证"
echo "  Start: $(date)"
echo "  Project: $PROJECT_DIR"
echo "  Results: $RESULTS_DIR"
echo "============================================"

if [ ! -f "$SCRIPTS_DIR/run_mooncake_store_master.sh" ]; then
    echo "FATAL: run_mooncake_store_master.sh not found in $SCRIPTS_DIR"
    echo "Make sure you are on the supercache_dev_withscript branch."
    exit 1
fi
if [ ! -f "$SCRIPTS_DIR/write.sh" ]; then
    echo "FATAL: write.sh not found in $SCRIPTS_DIR"
    exit 1
fi
if [ ! -f "$SCRIPTS_DIR/read.sh" ]; then
    echo "FATAL: read.sh not found in $SCRIPTS_DIR"
    exit 1
fi

cd "$PROJECT_DIR"
mkdir -p "$RESULTS_DIR"

# ========================================================================
# Layer 1: Submodule (extern/ubdiag)
# ========================================================================
{
    echo ""
    echo "############################################################"
    echo "#  LAYER 1: Submodule (extern/ubdiag)"
    echo "############################################################"

    L1_BUILD="$PROJECT_DIR/build_verify_l1"
    L1_LOG="$RESULTS_DIR/layer1_submodule"
    mkdir -p "$L1_LOG"
    rm -rf "$L1_BUILD"
    mkdir -p "$L1_BUILD"

    # Restore submodule if previously hidden
    if [ -d "$PROJECT_DIR/extern/ubdiag_bak" ]; then
        mv "$PROJECT_DIR/extern/ubdiag_bak" "$PROJECT_DIR/extern/ubdiag"
    fi

    echo "  [build] cmake configure..."
    cd "$L1_BUILD"
    cmake "$PROJECT_DIR" "${CMAKE_ARGS[@]}" > "$L1_LOG/cmake_output.log" 2>&1
    check_ubdiag_layer "$L1_LOG/cmake_output.log" 1

    echo "  [build] make -j$(nproc)..."
    make -j$(nproc) > "$L1_LOG/make_output.log" 2>&1
    echo "  [build] make exit=$?"

    link_build "$L1_BUILD"
    kill_mooncake
    run_4scripts "$L1_LOG" || echo "  [WARN] Layer 1 scripts had issues"
    echo "  Layer 1 DONE"
} 2>&1 | tee "$RESULTS_DIR/layer1.log"

# ========================================================================
# Layer 2: System Package
# ========================================================================
{
    echo ""
    echo "############################################################"
    echo "#  LAYER 2: System Package (find_package)"
    echo "############################################################"

    L2_BUILD="$PROJECT_DIR/build_verify_l2"
    L2_LOG="$RESULTS_DIR/layer2_system"
    mkdir -p "$L2_LOG"
    rm -rf "$L2_BUILD"
    mkdir -p "$L2_BUILD"

    # Step 1: Build & install UbDiag to local prefix (no sudo needed)
    if [ ! -d "$UBDIAG_SRC" ]; then
        if [ -d "$PROJECT_DIR/extern/ubdiag_bak" ]; then
            mv "$PROJECT_DIR/extern/ubdiag_bak" "$UBDIAG_SRC"
        fi
    fi

    if [ -d "$UBDIAG_SRC" ]; then
        echo "  [ubdiag] Building ubdiag for local install..."
        rm -rf "$UBDIAG_INSTALL_PREFIX"
        UBDIAG_BUILD="$PROJECT_DIR/build_ubdiag_layer2"
        rm -rf "$UBDIAG_BUILD"
        mkdir -p "$UBDIAG_BUILD"
        cd "$UBDIAG_BUILD"
        cmake "$UBDIAG_SRC" \
            -DCMAKE_INSTALL_PREFIX="$UBDIAG_INSTALL_PREFIX" \
            -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF \
            > "$L2_LOG/ubdiag_cmake.log" 2>&1
        make -j$(nproc) > "$L2_LOG/ubdiag_make.log" 2>&1
        make install > "$L2_LOG/ubdiag_install.log" 2>&1
        echo "  [ubdiag] Installed to $UBDIAG_INSTALL_PREFIX"

        # Step 2: Hide submodule so FindUbDiag falls through to Layer 2
        if [ -d "$PROJECT_DIR/extern/ubdiag" ]; then
            mv "$PROJECT_DIR/extern/ubdiag" "$PROJECT_DIR/extern/ubdiag_bak"
        fi

        # Step 3: Build Mooncake with CMAKE_PREFIX_PATH pointing to local install
        echo "  [build] cmake configure (CMAKE_PREFIX_PATH=$UBDIAG_INSTALL_PREFIX)..."
        cd "$L2_BUILD"
        cmake "$PROJECT_DIR" \
            "${CMAKE_ARGS[@]}" \
            -DCMAKE_PREFIX_PATH="$UBDIAG_INSTALL_PREFIX" \
            > "$L2_LOG/cmake_output.log" 2>&1
        check_ubdiag_layer "$L2_LOG/cmake_output.log" 2

        echo "  [build] make -j$(nproc)..."
        make -j$(nproc) > "$L2_LOG/make_output.log" 2>&1
        echo "  [build] make exit=$?"

        link_build "$L2_BUILD"
        kill_mooncake
        run_4scripts "$L2_LOG" || echo "  [WARN] Layer 2 scripts had issues"

        # Restore submodule for next layer
        if [ -d "$PROJECT_DIR/extern/ubdiag_bak" ]; then
            mv "$PROJECT_DIR/extern/ubdiag_bak" "$PROJECT_DIR/extern/ubdiag"
        fi
    else
        echo "  [SKIP] ubdiag source not found at $UBDIAG_SRC — cannot verify Layer 2"
    fi
    echo "  Layer 2 DONE"
} 2>&1 | tee "$RESULTS_DIR/layer2.log"

# ========================================================================
# Layer 3: Mock Fallback
# ========================================================================
{
    echo ""
    echo "############################################################"
    echo "#  LAYER 3: Mock Fallback (no-op PerfPoint)"
    echo "############################################################"

    L3_BUILD="$PROJECT_DIR/build_verify_l3"
    L3_LOG="$RESULTS_DIR/layer3_mock"
    mkdir -p "$L3_LOG"
    rm -rf "$L3_BUILD"
    mkdir -p "$L3_BUILD"

    # Hide submodule
    if [ -d "$PROJECT_DIR/extern/ubdiag" ]; then
        mv "$PROJECT_DIR/extern/ubdiag" "$PROJECT_DIR/extern/ubdiag_bak"
    fi

    # Also remove the local install from Layer 2 so find_package also fails
    rm -rf "$UBDIAG_INSTALL_PREFIX" 2>/dev/null || true

    echo "  [build] cmake configure (expecting mock)..."
    cd "$L3_BUILD"
    cmake "$PROJECT_DIR" "${CMAKE_ARGS[@]}" > "$L3_LOG/cmake_output.log" 2>&1
    check_ubdiag_layer "$L3_LOG/cmake_output.log" 3

    echo "  [build] make -j$(nproc)..."
    make -j$(nproc) > "$L3_LOG/make_output.log" 2>&1
    echo "  [build] make exit=$?"

    link_build "$L3_BUILD"
    kill_mooncake
    run_4scripts "$L3_LOG" || echo "  [WARN] Layer 3 scripts had issues"

    # Restore submodule
    if [ -d "$PROJECT_DIR/extern/ubdiag_bak" ]; then
        mv "$PROJECT_DIR/extern/ubdiag_bak" "$PROJECT_DIR/extern/ubdiag"
    fi
    echo "  Layer 3 DONE"
} 2>&1 | tee "$RESULTS_DIR/layer3.log"

# ========================================================================
# Final Summary
# ========================================================================
echo ""
echo "============================================"
echo "  ALL 3 LAYERS VERIFICATION COMPLETE"
echo "  End: $(date)"
echo "============================================"

for layer in 1 2 3; do
    case $layer in
        1) LNAME="Layer1 (submodule)"  ; LDIR="layer1_submodule" ;;
        2) LNAME="Layer2 (system pkg)" ; LDIR="layer2_system" ;;
        3) LNAME="Layer3 (mock)"       ; LDIR="layer3_mock" ;;
    esac
    echo ""
    echo "--- $LNAME ---"
    if [ -f "$RESULTS_DIR/$LDIR/00_summary.txt" ]; then
        cat "$RESULTS_DIR/$LDIR/00_summary.txt"
    else
        echo "  (no summary — layer may have been skipped or failed)"
    fi
done

# Restore build symlink to Layer 1's build (the default)
ln -sfn "$PROJECT_DIR/build_verify_l1" "$PROJECT_DIR/build" 2>/dev/null || true

echo ""
echo "Full logs: $RESULTS_DIR"
echo "============================================"
