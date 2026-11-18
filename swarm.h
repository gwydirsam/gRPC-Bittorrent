//
// Shared by the visualizers: fetches the peer list from the tracker and each
// peer's chunk states over gRPC, and keeps the latest result per peer plus a
// message log. Fetching and applying are separate so a front end can do the
// network calls without holding its lock.
//

#ifndef S3_BITTORRENT_CPP_SWARM_H
#define S3_BITTORRENT_CPP_SWARM_H

#include <algorithm>
#include <chrono>
#include <deque>
#include <format>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"

constexpr std::size_t kSwarmLogLines = 14;
constexpr auto kSwarmRpcTimeout = std::chrono::seconds(2);

enum class LogLevel { Info, Ok, Error };

struct LogEntry {
    LogLevel level;
    std::string text;
};

struct PeerView {
    std::vector<bittorrent::ChunkState> chunks;
    std::vector<std::chrono::steady_clock::time_point> filledAt;  // when each chunk was first seen done
    std::chrono::steady_clock::time_point polledAt;               // when this view was last updated
    std::string error;

    // True if the chunk turned done in the most recent poll.
    bool justFilled(std::size_t i) const {
        return i < filledAt.size() && filledAt[i] == polledAt;
    }
};

struct Swarm {
    std::map<std::string, PeerView> peers;
    std::vector<std::string> addrs;   // peers from the latest tracker reply
    std::deque<LogEntry> log;

    void logLine(LogLevel level, std::string text) {
        log.push_back({level, std::move(text)});
        if (log.size() > kSwarmLogLines) log.pop_front();
    }
};

struct TrackerReply {
    std::string filename;
    bool ok = false;
    std::string error;
    std::vector<std::string> addrs;
};

struct PeerReply {
    std::string addr;
    bool ok = false;
    std::string error;
    std::vector<bittorrent::ChunkState> chunks;
};

// Peer stubs are cached per address. Only the polling thread touches them.
using PeerStubs = std::map<std::string, std::unique_ptr<bittorrent::Peer::Stub> >;

inline std::chrono::system_clock::time_point rpcDeadline() {
    return std::chrono::system_clock::now() + kSwarmRpcTimeout;
}

inline TrackerReply fetchPeers(bittorrent::Tracker::Stub &tracker, const std::string &filename) {
    bittorrent::GetWorkersRequest request;
    request.set_filename(filename);
    bittorrent::GetWorkersReply reply;
    grpc::ClientContext context;
    context.set_deadline(rpcDeadline());
    const auto status = tracker.GetWorkers(&context, request, &reply);

    TrackerReply result{.filename = filename};
    if (!status.ok()) {
        result.error = status.error_message();
        return result;
    }
    result.ok = true;
    for (const auto &w: reply.workers()) {
        result.addrs.push_back(std::format("{}:{}", w.ip(), w.port()));
    }
    return result;
}

inline void applyPeers(Swarm &swarm, const TrackerReply &reply) {
    swarm.logLine(LogLevel::Info, std::format("→ tracker GetWorkers({})", reply.filename));
    if (!reply.ok) {
        swarm.logLine(LogLevel::Error, std::format("✗ tracker: {}", reply.error));
        return;
    }
    swarm.logLine(LogLevel::Info, std::format("← tracker {} peer(s)", reply.addrs.size()));
    swarm.addrs = reply.addrs;
    std::erase_if(swarm.peers, [&](const auto &entry) {
        return std::ranges::find(swarm.addrs, entry.first) == swarm.addrs.end();
    });
}

inline PeerReply fetchPeerChunks(PeerStubs &stubs, const std::string &addr, const std::string &filename) {
    auto &stub = stubs[addr];
    if (!stub) {
        stub = bittorrent::Peer::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }
    bittorrent::GetAvailableChunksRequest request;
    request.set_filename(filename);
    bittorrent::GetAvailableChunksReply reply;
    grpc::ClientContext context;
    context.set_deadline(rpcDeadline());
    const auto status = stub->GetAvailableChunks(&context, request, &reply);

    PeerReply result{.addr = addr};
    if (!status.ok()) {
        result.error = status.error_message();
        return result;
    }
    result.ok = true;
    result.chunks.assign(reply.chunkcount(), bittorrent::ChunkState::Missing);
    for (const auto &c: reply.chunks()) {
        if (c.chunkid() >= 0 && static_cast<std::size_t>(c.chunkid()) < result.chunks.size()) {
            result.chunks[c.chunkid()] = c.chunkstate();
        }
    }
    return result;
}

inline void applyPeerChunks(Swarm &swarm, const PeerReply &reply, std::chrono::steady_clock::time_point now) {
    swarm.logLine(LogLevel::Info, std::format("→ {} GetAvailableChunks", reply.addr));
    auto &view = swarm.peers[reply.addr];
    if (!reply.ok) {
        view.error = reply.error;
        swarm.logLine(LogLevel::Error, std::format("✗ {}: {}", reply.addr, reply.error));
        return;
    }
    view.error.clear();

    auto filledAt = view.filledAt;
    filledAt.resize(reply.chunks.size());
    int done = 0;
    int inProgress = 0;
    std::vector<int> newlyDone;
    for (std::size_t i = 0; i < reply.chunks.size(); ++i) {
        const bool wasDone = i < view.chunks.size() && view.chunks[i] == bittorrent::ChunkState::Done;
        if (reply.chunks[i] == bittorrent::ChunkState::Done) {
            ++done;
            if (!wasDone) {
                filledAt[i] = now;
                newlyDone.push_back(static_cast<int>(i));
            }
        } else if (reply.chunks[i] == bittorrent::ChunkState::InProgress) {
            ++inProgress;
        }
    }
    view.chunks = reply.chunks;
    view.filledAt = std::move(filledAt);
    view.polledAt = now;

    swarm.logLine(LogLevel::Info, std::format("← {} {}/{} done, {} in progress", reply.addr, done,
                                              view.chunks.size(), inProgress));
    if (!newlyDone.empty()) {
        std::string ids;
        for (std::size_t i = 0; i < newlyDone.size() && i < 8; ++i) {
            ids += std::format("{} ", newlyDone[i]);
        }
        if (newlyDone.size() > 8) ids += "…";
        swarm.logLine(LogLevel::Ok, std::format("✓ {} chunk(s) now done at {}: {}", newlyDone.size(), reply.addr, ids));
    }
}

#endif //S3_BITTORRENT_CPP_SWARM_H
