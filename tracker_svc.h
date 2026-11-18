//
// Created by Sam Gwydir on 11/17/26.
//

#ifndef S3_BITTORRENT_CPP_TRACKER_SVC_H
#define S3_BITTORRENT_CPP_TRACKER_SVC_H

#include <format>
#include <string>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"
#include "tracker.h"


class TrackerServiceImpl final : public bittorrent::Tracker::Service {
public:
    explicit TrackerServiceImpl(Tracker &t) : _t(t) {
    }

    grpc::Status Ping(grpc::ServerContext *ctx, const bittorrent::PingRequest *request,
                      bittorrent::PingReply *reply) override {
        auto worker = request->worker();
        auto ip = worker.ip();
        auto port = worker.port();
        auto str = std::format("{}:{}", ip, port);
        _t.peerConnect(str);
        reply->set_message(std::format("Ping from {}", str));
        return grpc::Status::OK;
    }

    grpc::Status Disconnect(grpc::ServerContext *ctx, const bittorrent::DisconnectRequest *request,
                            bittorrent::DisconnectReply *reply) override {
        auto worker = request->worker();
        auto str = std::format("{}:{}", worker.ip(), worker.port());
        _t.peerDisconnect(str);
        reply->set_message(std::format("Disconnected {}", str));
        return grpc::Status::OK;
    }

    grpc::Status GetWorkers(grpc::ServerContext *context, const bittorrent::GetWorkersRequest *request,
                            bittorrent::GetWorkersReply *response) override {
        auto workers = _t.getPeers();
        for (const auto &w: workers) {
            auto worker = response->add_workers();
            const auto pos = w.rfind(':');
            worker->set_ip(w.substr(0, pos));
            worker->set_port(w.substr(pos + 1));
        }
        return grpc::Status::OK;
    }

private:
    Tracker &_t;
};

#endif //S3_BITTORRENT_CPP_TRACKER_SVC_H
