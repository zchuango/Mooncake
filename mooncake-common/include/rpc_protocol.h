#pragma once

#include <cstdlib>
#include <string>
#include <string_view>

namespace mooncake {

enum class RpcProtocol {
    Tcp,
    Rdma,
    Urma,
};

inline std::string_view RpcProtocolEnvValue() {
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    return value ? std::string_view(value) : std::string_view();
}

inline RpcProtocol GetRpcProtocolFromEnv() {
    auto value = RpcProtocolEnvValue();
    if (value == "urma") {
        return RpcProtocol::Urma;
    }
    if (value == "rdma" || value == "ibv") {
        return RpcProtocol::Rdma;
    }
    return RpcProtocol::Tcp;
}

inline const char* RpcProtocolName(RpcProtocol protocol) {
    switch (protocol) {
        case RpcProtocol::Urma:
            return "urma";
        case RpcProtocol::Rdma:
            return "rdma";
        case RpcProtocol::Tcp:
        default:
            return "tcp";
    }
}

}  // namespace mooncake
