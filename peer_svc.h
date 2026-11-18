//
// Created by Sam Gwydir on 11/17/26.
//

#ifndef S3_BITTORRENT_CPP_PEER_SVC_H
#define S3_BITTORRENT_CPP_PEER_SVC_H

#include <format>
#include <string>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"
#include "worker.h"

// Answers Ping with the name of the role serving it ("tracker" or "worker").
class PeerServiceImpl final : public bittorrent::Peer::Service {
public:
    explicit PeerServiceImpl(Worker &w) : _w(w) {
    };

    grpc::Status Ping(grpc::ServerContext *ctx, const bittorrent::PingRequest *request,
                      bittorrent::PingReply *reply) override {
        auto worker = request->worker();
        auto ip = worker.ip();
        auto port = worker.port();
        auto str = std::format("{}:{}", ip, port);
        reply->set_message(std::format("Ping from {}", str));
        return grpc::Status::OK;
    }

    grpc::Status GetAvailableChunks(grpc::ServerContext *ctx, const bittorrent::GetAvailableChunksRequest *request,
        bittorrent::GetAvailableChunksReply *reply) override {
        reply->set_filename(_w.getFileName());
        reply->set_chunkcount(_w.getChunkCount());
        auto chunks = _w.getAvailableChunks();
        for (int i = 0; i < chunks.size(); i++) {
            auto chunk = reply->add_chunks();
            chunk->set_chunkid(i);
            chunk->set_chunkstate(ToProto(chunks[i]));
        }
        return grpc::Status::OK;
    }

    grpc::Status GetChunks(grpc::ServerContext *ctx, const bittorrent::GetChunksRequest *request,
        bittorrent::GetChunksReply *reply) override {
        const int id = request->chunkid();
        if (id < 0 || static_cast<std::size_t>(id) >= _w.getChunkCount()) {
            return {grpc::StatusCode::OUT_OF_RANGE, std::format("no chunk {}", id)};
        }
        if (_w.getChunkState(static_cast<ChunkID>(id)) != ChunkState::Done) {
            return {grpc::StatusCode::NOT_FOUND, std::format("chunk {} not available", id)};
        }
        auto chunk = _w.getChunk(id);

        reply->set_filename(_w.getFileName());
        reply->mutable_chunk()->CopyFrom(ToProto(chunk));   // creates the sub-message
        return grpc::Status::OK;
    }

private:
    Worker &_w;
};

#endif //S3_BITTORRENT_CPP_PEER_SVC_H
