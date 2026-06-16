#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:$LD_LIBRARY_PATH
export MC_LOG_ENABLE=on
export MC_LOG_LEVEL=INFO
export MC_LOG_DIR=/var/log/mooncake
export MC_LOG_DETAIL_ENABLE=off
export MC_LOG_MAX_SIZE=100
export MC_LOG_BUFFER_SECS=3
export MC_HIFREQ_LOG_SAMPLE_RATE=0.1
MOONCAKE_MASTER=./build/mooncake-store/src/mooncake_master
$MOONCAKE_MASTER --global_file_segment_size=9223372036854775807 \
        --enable_http_metadata_server=true \
        --http_metadata_server_host=141.61.84.245 \
        --http_metadata_server_port=8020 \
        --default_kv_lease_ttl=300000 \
        --enable_offload=false \
        --port=50060 \
        --metrics_port=9010
