#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib64:/usr/local/lib:$LD_LIBRARY_PATH
export MC_STORE_CLIENT_SETUP_RETRIES=3
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.17.0/24,141.61.84.0/24"
export MC_STORE_CLIENT_METRIC_BANDWIDTH=0
export MC_LOG_ENABLE=on
export MC_LOG_LEVEL=INFO
# export MC_LOG_DIR=/var/log/mooncake
export MC_LOG_DETAIL_ENABLE=off
export MC_LOG_MAX_SIZE=100
export MC_LOG_BUFFER_SECS=3
export MC_HIFREQ_LOG_SAMPLE_RATE=0
export MC_TCP_BIND_ADDRESS=141.61.84.245
export MC_URMA_TRANS_MODE=RM

MOONCAKE_CLIENT=/home/q00913006/project/mooncake-qyf/build/mooncake-store/src/mooncake_client

$MOONCAKE_CLIENT \
    --metadata_server='http://141.61.84.245:8020/metadata' \
    --master_server_address='141.61.84.245:50060' \
    --host=$MC_TCP_BIND_ADDRESS \
    --global_segment_size=21474836480 \
    --device_names=bonding_dev_0 \
    --threads=16 \
    --protocol=ub \
    --port=8980
