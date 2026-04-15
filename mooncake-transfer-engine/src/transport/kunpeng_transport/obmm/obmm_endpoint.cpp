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

#include <glog/logging.h>
#include <cassert>
#include <cstddef>
#include "config.h"
#include "transport/kunpeng_transport/obmm/obmm_endpoint.h"
#include "transport/kunpeng_transport/ub_endpoint.h"

namespace mooncake {

ObmmContext::ObmmContext(UbTransport& engine, std::string device_name,
                         int max_endpoints)
    : UbContext(engine, std::move(device_name), max_endpoints) {
}

ObmmContext::~ObmmContext() {
    auto thisString = toString();
    worker_pool_.reset();
    LOG(INFO) << "destroy worker pool done.";
    endpoint_store_->destroy();
    LOG(INFO) << "destroy endpoint store done.";
    // Do not call deconstruct() here as it may cause double free
    // Memory is already freed by unregisterMemoryRegion() called from TearDown()
    LOG(WARNING) << "finished destroy context : " << thisString;
}

std::string ObmmContext::toString() {
    std::ostringstream ss;
    ss << "ObmmContext:{device_name : " << deviceName()
       << " ,max_endpoints : " << max_endpoints_
       << " ,async_fd : " << getAsyncFd() << " }";
    return ss.str();
}

int ObmmContext::getAsyncFd() {
    return -1;
}

int ObmmContext::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
    return worker_pool_->submitPostSend(slice_list);
}

int ObmmContext::construct(GlobalConfig& config) {
    int eid_index = 0;
    if (openDevice(deviceName(), 0, eid_index)) {
        LOG(ERROR) << "Failed to open device : " << deviceName();
        return ERR_CONTEXT;
    }

    worker_pool_ = std::make_shared<UbWorkerPool>(*this, socketId());
    LOG(INFO) << "create workerpool done";
    return 0;
}

int ObmmContext::deconstruct() {
    // Clear local_tseg_list_ first to avoid dangling pointers
    local_tseg_list_.clear();

    // Use lock to protect seg_region_list_ access
    std::vector<std::pair<void*, size_t>> seg_region_copy;
    {
        RWSpinlock::WriteGuard guard(seg_region_lock_);
        // Create a copy of seg_region_list_ to avoid modification during iteration
        seg_region_copy = seg_region_list_;
        seg_region_list_.clear();
    }
    
    // Release memory outside the lock
    for (auto& entry : seg_region_copy) {
        SHM* shm = static_cast<SHM*>(entry.first);
        ShmMgr::Free(shm);
        delete shm;
    }

    // Process imported_seg_list_
    auto imported_seg_copy = imported_seg_list_;
    imported_seg_list_.clear();
    for (auto& seg : imported_seg_copy) {
        SHM* shm = static_cast<SHM*>(seg);
        ShmMgr::Free(shm);
        delete shm;
    }

    // Process remote_seg_list_
    auto remote_seg_copy = remote_seg_list_;
    remote_seg_list_.clear();
    for (auto& seg : remote_seg_copy) {
        free(seg);
    }

    import_tseg_map.clear();

    return 0;
}

void* ObmmContext::seg(uint64_t addr) {
    RWSpinlock::ReadGuard guard(seg_region_lock_);
    for (auto iter = seg_region_list_.begin(); iter != seg_region_list_.end();
         ++iter) {
        SHM* shm = static_cast<SHM*>(iter->first);
        if (reinterpret_cast<uint64_t>(shm->addr) <= addr &&
            addr < reinterpret_cast<uint64_t>(shm->addr) + shm->len) {
            return shm;
        }
    }

    LOG(ERROR) << "Address " << addr << " seg not found for " << deviceName();
    return nullptr;
}

int ObmmContext::buildLocalBufferDesc(uint64_t addr,
                                     UbTransport::BufferDesc& buffer_desc) {
    SHM* shm = static_cast<SHM*>(seg(addr));
    if (!shm) {
        return ERR_MEMORY;
    }
    auto str = serializeBinaryData(shm, sizeof(SHM));
    auto index = local_tseg_list().size() - 1;
    buffer_desc.tseg.push_back(str);
    buffer_desc.l_seg_index.push_back(index);
    return 0;
}

void* ObmmContext::localSegWithIndex(unsigned value) {
    return local_tseg_list_.at(value);
}

int ObmmContext::registerMemoryRegion(uint64_t va, size_t length) {
    if (length > (size_t)globalConfig().max_seg_size) {
        PLOG(WARNING) << "The buffer length exceeds device max_seg_size, "
                      << "shrink it to " << globalConfig().max_seg_size;
        length = (size_t)globalConfig().max_seg_size;
    }
    LOG(INFO) << "Register memory region " << va << " length " << length;

    SHM* shm = new SHM;
    shm->addr = reinterpret_cast<uint8_t*>(va);
    shm->len = length;
    shm->memid = 0;
    shm->fd = -1;
    memset(shm->name, 0, sizeof(shm->name));

    if (ShmMgr::LocalMmap(shm, PROT_READ | PROT_WRITE) != 0) {
        LOG(ERROR) << "Failed to mmap shared memory";
        delete shm;
        return ERR_MEMORY;
    }

    local_tseg_list_.push_back(shm);

    RWSpinlock::WriteGuard guard(seg_region_lock_);
    seg_region_list_.emplace_back(shm, length);
    return 0;
}

int ObmmContext::unregisterMemoryRegion(uint64_t addr) {
    RWSpinlock::WriteGuard guard(seg_region_lock_);
    bool has_removed;
    do {
        has_removed = false;
        for (auto iter = seg_region_list_.begin();
             iter != seg_region_list_.end(); ++iter) {
            SHM* shm = static_cast<SHM*>(iter->first);
            if (reinterpret_cast<uint64_t>(shm->addr) <= addr &&
                addr < reinterpret_cast<uint64_t>(shm->addr) + shm->len) {
                // First remove from local_tseg_list_
                for (auto tseg_iter = local_tseg_list_.begin(); tseg_iter != local_tseg_list_.end(); ++tseg_iter) {
                    if (*tseg_iter == shm) {
                        local_tseg_list_.erase(tseg_iter);
                        break;
                    }
                }
                ShmMgr::Free(shm);
                delete shm;
                seg_region_list_.erase(iter);
                has_removed = true;
                break;
            }
        }
    } while (has_removed);
    return 0;
}

std::string ObmmContext::getEid() {
    return eid();
}

int ObmmContext::doProcessContextEvents() {
    return 0;
}

void* ObmmContext::retrieveRemoteSeg(const std::string& remoteSegmentStr) {
    auto ret = import_tseg_map.find(remoteSegmentStr);
    if (ret != import_tseg_map.end()) {
        return ret->second;
    }
    std::vector<unsigned char> output_buffer;
    deserializeBinaryData(remoteSegmentStr, output_buffer);
    SHM* shm = static_cast<SHM*>(malloc(sizeof(SHM)));
    memcpy(shm, output_buffer.data(), sizeof(SHM));
    
    SHM* import_shm = new SHM;
    memcpy(import_shm, shm, sizeof(SHM));
    if (ShmMgr::RemoteMalloc(import_shm) != 0) {
        LOG(ERROR) << "Failed to allocate remote shared memory";
        delete import_shm;
        free(shm);
        return nullptr;
    }
    remote_seg_list_.push_back(shm);
    imported_seg_list_.push_back(import_shm);
    import_tseg_map[remoteSegmentStr] = import_shm;
    return import_shm;
}

int ObmmContext::openDevice(const std::string& device_name, uint8_t port,
                           int& eid_index) {
    eid_index = 0;
    return 0;
}

std::string ObmmContext::eid() const {
    return "00:00:00:00:00:00:00:01";
}

const std::vector<void*>& ObmmContext::local_tseg_list() const {
    return local_tseg_list_;
}

int ObmmContext::poll(int num_entries, Transport::Slice** slices, int jfc_index) {
    return 0;
}

volatile int* ObmmContext::outstandingCount(int jfc_index) {
    return nullptr;
}

int ObmmContext::jfcCount() {
    return 1;
}

bool ObmmContext::uninit() {
    ShmMgr::Fini();
    return true;
}

bool ObmmContext::init() {
    if (!ShmMgr::Init()) {
        LOG(ERROR) << "OBMM module init failed";
        return false;
    }
    LOG(INFO) << "OBMM module init success";
    return true;
}

std::shared_ptr<UbEndPoint> ObmmContext::makeEndpoint() {
    return std::make_shared<ObmmEndpoint>(this);
}

ObmmEndpoint::ObmmEndpoint(ObmmContext* context)
    : UbEndPoint(), context_(context), max_wr_depth_(0), wr_depth_list_(nullptr) {}

ObmmEndpoint::~ObmmEndpoint() {
    deconstruct();
}

int ObmmEndpoint::construct(GlobalConfig& config) {
    size_t num_jetty_list = config.num_jetty_per_ep;
    size_t max_wr_depth = config.max_wr;
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    jetty_list_.resize(num_jetty_list);

    max_wr_depth_ = (int)max_wr_depth;
    wr_depth_list_ = new volatile int[num_jetty_list];
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }

    for (size_t i = 0; i < num_jetty_list; ++i) {
        wr_depth_list_[i] = 0;
        void* jetty = malloc(16); // Simple mock jetty
        memset(jetty, 0, 16);
        jetty_list_[i] = jetty;
        LOG(INFO) << "Create jetty success";
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int ObmmEndpoint::deconstruct() {
    for (size_t i = 0; i < jetty_list_.size(); ++i) {
        auto imported_it = imported_jetty_map_.find(jetty_list_[i]);
        auto imported_jetty = (imported_it != imported_jetty_map_.end())
                                  ? imported_it->second
                                  : nullptr;
        if (imported_jetty != nullptr) {
            free(imported_jetty);
        }
        free(jetty_list_[i]);
    }
    jetty_list_.clear();
    if (wr_depth_list_ != nullptr) {
        delete[] wr_depth_list_;
        wr_depth_list_ = nullptr;
    }
    imported_jetty_map_.clear();
    return 0;
}

void ObmmEndpoint::setPeerNicPath(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Previous connection will be discarded";
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

const std::string ObmmEndpoint::toString() const {
    auto status = status_.load(std::memory_order_relaxed);
    if (status == CONNECTED) {
        return "EndPoint: local " + context_->nicPath() + ", peer " +
               peer_nic_path_;
    } else {
        return "EndPoint: local " + context_->nicPath() + " (unconnected)";
    }
}

int ObmmEndpoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(INFO) << "Connection has been established";
        return 0;
    }

    if (context_->nicPath() == peer_nic_path_) {
        auto segment_desc =
            context_->engine().meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (segment_desc) {
            for (auto& nic : segment_desc->devices) {
                if (nic.name == context_->deviceName()) {
                    return doSetupConnection(nic.eid, JettyNum());
                }
            }
        }
        LOG(ERROR) << "Peer NIC " << context_->deviceName()
                   << " not found in localhost";
        return ERR_DEVICE_NOT_FOUND;
    }

    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_->nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.jetty_num = JettyNum();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_->engine().sendHandshake(peer_server_name, local_desc,
                                              peer_desc);
    if (rc) return rc;
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Reject the handshake request by peer "
                   << local_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.local_nic_path != peer_nic_path_ ||
        peer_desc.peer_nic_path != local_desc.local_nic_path) {
        LOG(ERROR) << "Invalid argument: received packet mismatch"
                   << ", local.local_nic_path: " << local_desc.local_nic_path
                   << ", local.peer_nic_path: " << local_desc.peer_nic_path
                   << ", peer.local_nic_path: " << peer_desc.local_nic_path
                   << ", peer.peer_nic_path: " << peer_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    auto segment_desc =
        context_->engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto& nic : segment_desc->devices) {
            if (nic.name == peer_nic_name) {
                return doSetupConnection(nic.eid, peer_desc.jetty_num);
            }
        }
    }
    LOG(ERROR) << "Peer NIC " << peer_nic_name << " not found in "
               << peer_server_name;
    return ERR_DEVICE_NOT_FOUND;
}

void ObmmEndpoint::disconnectUnlocked() {
    for (size_t i = 0; i < jetty_list_.size(); ++i) {
        auto imported_jetty = imported_jetty_map_[jetty_list_[i]];
        if (imported_jetty != nullptr) {
            free(imported_jetty);
        }
    }
    status_.store(UNCONNECTED, std::memory_order_release);
}

int ObmmEndpoint::setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                           HandShakeDesc& local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Re-establish connection: " << toString();
        disconnectUnlocked();
    }

    if (peer_desc.peer_nic_path != context_->nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg =
            "Invalid argument: peer nic path inconsistency, expect " +
            context_->nicPath() + " + " + peer_nic_path_ + ", while got " +
            peer_desc.peer_nic_path + " + " + peer_desc.local_nic_path;

        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path = context_->nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.jetty_num = JettyNum();

    auto segment_desc =
        context_->engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto& nic : segment_desc->devices) {
            if (nic.name == peer_nic_name) {
                return doSetupConnection(nic.eid, peer_desc.jetty_num,
                                         &local_desc.reply_msg);
            }
        }
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

bool ObmmEndpoint::hasOutstandingSlice() const {
    if (active_) return true;
    for (size_t i = 0; i < jetty_list_.size(); i++) {
        if (wr_depth_list_[i] != 0) return true;
    }
    return false;
}

int ObmmEndpoint::submitPostSend(
    std::vector<Transport::Slice*>& slice_list,
    std::vector<Transport::Slice*>& failed_slice_list) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!active_) return 0;

    // For OBMM, we'll use shared memory direct access
    for (auto slice : slice_list) {
        if (slice->opcode == Transport::TransferRequest::WRITE) {
            // Write operation: copy data from source to destination
            memcpy(reinterpret_cast<void*>(slice->ub.dest_addr),
                   slice->source_addr, slice->length);
        } else if (slice->opcode == Transport::TransferRequest::READ) {
            // Read operation: copy data from source to destination
            memcpy(slice->source_addr,
                   reinterpret_cast<void*>(slice->ub.dest_addr), slice->length);
        }
        slice->markSuccess();
    }

    slice_list.clear();
    return 0;
}

std::vector<uint32_t> ObmmEndpoint::JettyNum() const {
    std::vector<uint32_t> ret;
    for (int jetty_index = 0; jetty_index < (int)jetty_list_.size();
         ++jetty_index) {
        ret.push_back(1); // Simple mock jetty ID
    }
    return ret;
}

int ObmmEndpoint::doSetupConnection(const std::string& peer_eid,
                                   std::vector<uint32_t> peer_jetty_num_list,
                                   std::string* reply_msg) {
    if (jetty_list_.size() != peer_jetty_num_list.size()) {
        std::string message =
            "jetty count mismatch in peer and local endpoints, check "
            "MC_MAX_EP_PER_CTX";
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    for (int jetty_index = 0; jetty_index < (int)jetty_list_.size();
         ++jetty_index) {
        int ret = doSetupConnection(
            jetty_index, peer_eid, peer_jetty_num_list[jetty_index], reply_msg);
        if (ret) return ret;
    }

    status_.store(CONNECTED, std::memory_order_relaxed);
    return 0;
}

int ObmmEndpoint::doSetupConnection(int jetty_index,
                                   const std::string& peer_eid,
                                   uint32_t peer_jetty_num,
                                   std::string* reply_msg) {
    if (jetty_index < 0 || jetty_index >= (int)jetty_list_.size()) {
        return ERR_INVALID_ARGUMENT;
    }
    auto& jetty = jetty_list_[jetty_index];

    // For OBMM, we just create a dummy target jetty
    void* imported_jetty = malloc(16); // Simple mock imported jetty
    memset(imported_jetty, 0, 16);
    imported_jetty_map_[jetty] = imported_jetty;
    LOG(INFO) << "Bind jetty success, remote jetty id:" << peer_jetty_num;

    return 0;
}

} // namespace mooncake
