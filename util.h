//
// Created by Sam Gwydir on 11/17/26.
//

#ifndef S3_BITTORRENT_CPP_UTIL_H
#define S3_BITTORRENT_CPP_UTIL_H

#include <cstdint>
#include <format>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <openssl/evp.h>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"

// Chunks are 4 MiB, which is exactly gRPC's default 4 MiB receive limit, so a
// GetChunksReply (chunk + other fields) would be rejected. Allow up to 16 MiB.
inline constexpr int kMaxMessageBytes = 16 * 1024 * 1024;

inline std::shared_ptr<grpc::Channel> makeChannel(const std::string &addr) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(kMaxMessageBytes);
    args.SetMaxSendMessageSize(kMaxMessageBytes);
    return grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args);
}

inline std::string sha256(const std::string &input) {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    const EVP_MD *md = EVP_sha256();
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int lengthOfHash = 0;

    EVP_DigestInit_ex(context, md, nullptr);
    EVP_DigestUpdate(context, input.c_str(), input.size());
    EVP_DigestFinal_ex(context, hash, &lengthOfHash);
    EVP_MD_CTX_free(context);

    std::stringstream ss;
    for (unsigned int i = 0; i < lengthOfHash; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
}

struct Manifest {
    friend std::istream &operator>>(std::istream &is, Manifest &m);

    friend std::ostream &operator<<(std::ostream &os, const Manifest &m);

    std::string filename;
    std::vector<std::pair<int, std::string> > chunks; // size, hash
    std::size_t totalBytes;
};

inline std::istream &operator>>(std::istream &is, Manifest &m) {
    std::string filename;
    std::size_t numChunks;
    std::vector<std::pair<int, std::string> > chunks; // size, hash
    std::size_t totalBytes;

    is >> filename;
    is >> totalBytes;
    is >> numChunks;
    chunks.resize(numChunks);
    for (std::size_t i = 0; i < numChunks; ++i) {
        int size;
        is >> size;
        std::string hash;
        is >> hash;
        chunks[i] = std::pair(size, hash);
    }
    m = Manifest{filename, chunks, totalBytes};
    return is;
}

inline std::ostream &operator<<(std::ostream &os, const Manifest &m) {
    os << m.filename << std::endl;
    os << m.totalBytes << std::endl;
    os << m.chunks.size() << std::endl;
    for (const auto &chunk: m.chunks) {
        auto &[size, hash] = chunk;
        os << size << " " << hash << std::endl;
    }
    return os;
}

using WorkerID = uint32_t;
using ChunkID = uint32_t;

enum class ChunkState {
    Missing = 0,
    InProgress,
    Done
};

bittorrent::ChunkState ToProto(ChunkState s) {
    switch (s) {
        case ChunkState::Missing:  return bittorrent::ChunkState::Missing;
        case ChunkState::InProgress:  return bittorrent::ChunkState::InProgress;
        case ChunkState::Done:  return bittorrent::ChunkState::Done;
    }
    return bittorrent::Missing;   // unreachable
}

std::optional<ChunkState> FromProto(bittorrent::ChunkState s) {
    switch (s) {
        case bittorrent::ChunkState::Missing:  return ChunkState::Missing;
        case bittorrent::ChunkState::InProgress:  return ChunkState::InProgress;
        case bittorrent::ChunkState::Done:  return ChunkState::Done;
        default: return std::nullopt;   // UNSPECIFIED, or a value from a newer client
    }
}


using ChunkData = std::string;

struct Chunk {
    Chunk(): id(-1), state(ChunkState::Missing) {}
    explicit Chunk(const bittorrent::Chunk& c)
        : id(c.chunkid()),
          hash(c.chunkhash()),                       // after changing the proto field to string
          state(*FromProto(c.chunkstate())),
          data(c.chunk()) {}

    explicit Chunk(ChunkID id, std::string hash, ChunkState state, ChunkData data) : id(id), hash(hash), state(state), data(data) {}
    bool Verify(const std::string& desired_hash) {
        return sha256(data) == desired_hash;
    }

    ChunkID id;
    std::string hash;
    ChunkState state;
    ChunkData data;
};

bittorrent::Chunk ToProto(Chunk s) {
    bittorrent::Chunk chunk;
    chunk.set_chunkid(s.id);
    chunk.set_chunkhash(s.hash);
    chunk.set_chunkstate(ToProto(s.state));
    chunk.set_chunk(s.data);
    return chunk;
}

std::optional<Chunk> FromProto(bittorrent::Chunk s) {
    Chunk chunk;
    chunk.id = s.chunkid();
    chunk.hash = s.chunkhash();
    chunk.state = *FromProto(s.chunkstate());
    chunk.data = s.chunk();
    return chunk;
}

struct PeerAddr {
    PeerAddr() {
    }

    PeerAddr(const std::string &ip, const std::string &port) : ip(ip), port(port) {
    }

    PeerAddr(const std::string &ip, const int port) : ip(ip), port(std::to_string(port)) {
    }

    PeerAddr(const std::string &peer_uri) {
        size_t port_sep = peer_uri.rfind(':');
        if (port_sep != std::string::npos) {
            ip = peer_uri.substr(0, port_sep);
            port = peer_uri.substr(port_sep + 1);
        }
    }

    bool operator==(const PeerAddr &other) const = default;

    std::string to_string() const {
        return std::format("{}:{}", ip, port);
    }

    std::string ip;
    std::string port;
};

namespace std {
    template <>
    struct hash<PeerAddr> {
        std::size_t operator()(const PeerAddr& p) const noexcept {
            std::size_t h1 = std::hash<std::string>{}(p.ip);
            std::size_t h2 = std::hash<std::string>{}(p.port);
            return h1 ^ (h2 << 1);
        }
    };
}


struct WorkerState {
    std::string ip;
    std::vector<ChunkID> chunks;
};


#endif //S3_BITTORRENT_CPP_UTIL_H
