// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_OBMM_ENDPOINT_H
#define MOONCAKE_OBMM_ENDPOINT_H

#include <string>
#include <vector>
#include <memory>
#include <map>
#include "transport/kunpeng_transport/ub_endpoint.h"
#include "transport/kunpeng_transport/ub_context.h"

namespace mooncake {

// Simple shared memory definition
struct SHM {
    uint8_t *addr;
    size_t len;
    uint64_t memid;
    char name[48]; // SHM_MAX_NAME_BUFF_LEN
    int fd;
};

// Simple shared memory manager
class ShmMgr {
public:
    static bool Init() {
        return true;
    }
    
    static bool Fini() {
        return true;
    }
    
    static int LocalMmap(SHM *shm, int prot) {
        // Simple implementation for testing
        return 0;
    }
    
    static int RemoteMalloc(SHM *shm) {
        // Simple implementation for testing
        return 0;
    }
    
    static int Free(SHM *shm) {
        // Simple implementation for testing - do not free memory here
        // Memory will be freed by the caller
        return 0;
    }
};

class ObmmContext : public UbContext {
public:
    ObmmContext(UbTransport& engine, std::string device_name,
                int max_endpoints);
    ~ObmmContext() override;

    std::string toString() override;
    int getAsyncFd() override;
    int submitPostSend(const std::vector<Transport::Slice*>& slice_list) override;
    int construct(GlobalConfig& config) override;
    int deconstruct() override;
    void* seg(uint64_t addr);
    int buildLocalBufferDesc(uint64_t addr,
                            UbTransport::BufferDesc& buffer_desc) override;
    void* localSegWithIndex(unsigned value) override;
    int registerMemoryRegion(uint64_t va, size_t length) override;
    int unregisterMemoryRegion(uint64_t addr) override;
    std::string getEid() override;
    int doProcessContextEvents() override;
    void* retrieveRemoteSeg(const std::string& remoteSegmentStr) override;
    int openDevice(const std::string& device_name, uint8_t port,
                  int& eid_index) override;
    std::string eid() const;
    const std::vector<void*>& local_tseg_list() const;
    int poll(int num_entries, Transport::Slice** slices, int jfc_index) override;
    volatile int* outstandingCount(int jfc_index) override;
    int jfcCount() override;
    static bool uninit();
    static bool init();
    std::shared_ptr<UbEndPoint> makeEndpoint() override;

private:
    std::vector<void*> local_tseg_list_;
    std::vector<std::pair<void*, size_t>> seg_region_list_;
    std::vector<void*> imported_seg_list_;
    std::vector<void*> remote_seg_list_;
    std::map<std::string, void*> import_tseg_map;
    RWSpinlock seg_region_lock_;
};

class ObmmEndpoint : public UbEndPoint {
public:
    explicit ObmmEndpoint(ObmmContext* context);
    ~ObmmEndpoint();

    int construct(GlobalConfig& config) override;
    int deconstruct() override;
    void setPeerNicPath(const std::string& peer_nic_path) override;
    const std::string toString() const override;
    int setupConnectionsByActive() override;
    void disconnectUnlocked() override;
    int setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                 HandShakeDesc& local_desc) override;
    bool hasOutstandingSlice() const override;
    int submitPostSend(std::vector<Transport::Slice*>& slice_list,
                      std::vector<Transport::Slice*>& failed_slice_list) override;
    std::vector<uint32_t> JettyNum() const;

private:
    int doSetupConnection(const std::string& peer_eid,
                         std::vector<uint32_t> peer_jetty_num_list,
                         std::string* reply_msg = nullptr);
    int doSetupConnection(int jetty_index, const std::string& peer_eid,
                         uint32_t peer_jetty_num,
                         std::string* reply_msg = nullptr);
private:
    ObmmContext* context_;
    std::vector<void*> jetty_list_;
    std::map<void*, void*> imported_jetty_map_;
    int max_wr_depth_;
    volatile int* wr_depth_list_;
};

} // namespace mooncake

#endif // MOONCAKE_OBMM_ENDPOINT_H
