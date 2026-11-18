#include <atomic>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <format>
#include <iostream>
#include <memory>
#include <print>
#include <stdexcept>
#include <string>
#include <grpcpp/grpcpp.h>

#include "util.h"
#include "tracker.h"
#include "worker.h"
#include "peer_svc.h"
#include "tracker_svc.h"




std::unique_ptr<grpc::Server> startServer(const std::string &addr, grpc::Service &service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}


std::unique_ptr<grpc::Server> startPeerServer(const std::string &addr, grpc::Service &service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}


std::atomic<bool> stopRequested{false};

void onStopSignal(int) {
    stopRequested = true;
}

int main(int argc, char *argv[]) {
    if (std::string(argv[1]) == "run") {
        const std::filesystem::path manifestPath(argv[2]);
        //Tracker(manifestPath);
        const std::filesystem::path outPath1("out1");
        //Worker(&t, manifestPath, outPath1);
    } else if (std::string(argv[1]) == "tracker") {
        // tracker <addr>, e.g. 0.0.0.0:50051
        Tracker tracker;
        TrackerServiceImpl svc(tracker);
        //tracker.printManifest();
        auto server = grpc::ServerBuilder()
                .AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials())
                .RegisterService(&svc)
                .BuildAndStart();
        server->Wait();
    } else if (std::string(argv[1]) == "worker") {
        const std::filesystem::path manifestPath(argv[2]);
        int id{std::stoi(argv[3])};
        std::ifstream file(manifestPath, std::ios::binary);
        if (!file) {
            std::cerr << "Error: Could not open file.\n";
            return 1;
        }
        Manifest m{};
        file >> m;
        file.close();
        // worker <tracker_addr>, e.g. 127.0.0.1:50051
        const std::string trackerAddr = "0.0.0.0:50051";
        ChunkFile cf(m, argv[4]);
        Worker worker(id, trackerAddr, m, cf);
        //worker.setupFile();
        PeerServiceImpl svc(worker);
        //worker.start("0.0.0.0:50051");
        //std::println("worker listening on 0.0.0.0:50051");
        std::string ip = "0.0.0.0";
        int server_port = 0;
        auto server = grpc::ServerBuilder()
                .SetMaxReceiveMessageSize(kMaxMessageBytes)
                .SetMaxSendMessageSize(kMaxMessageBytes)
                .AddListeningPort(std::format("{}:{}", ip, server_port), grpc::InsecureServerCredentials(),
                                  &server_port)
                .RegisterService(&svc)
                .BuildAndStart();
        worker.setServerAddr(ip, server_port);
        std::signal(SIGINT, onStopSignal);
        std::signal(SIGTERM, onStopSignal);
        while (!stopRequested) {
            worker.getStatus();
            try {
                std::println("{}", worker.pingTracker());
            } catch (const std::exception &e) {
                std::cerr << "Ping failed: " << e.what() << "\n";
                return 1;
            }
            try {
                for (const auto &w: worker.getPeers()) {
                    std::println("{}", w.to_string());
                }
            } catch (const std::exception &e) {
                std::cerr << "GetWorkers failed: " << e.what() << "\n";
                return 1;
            }
            try {
                for (const auto &p: worker.getPeers()) {
                    try {
                        std::println("{}", worker.pingPeer(p));
                        worker.getPeerChunks(p);
                        worker.getNextChunk(p);
                    } catch (const std::exception &e) {
                        std::cerr << std::format("Peer {} failed: {}\n", p.to_string(), e.what());
                    }
                }
            } catch (const std::exception &e) {
                std::cerr << "GetWorkers failed: " << e.what() << "\n";
                return 1;
            }
            sleep(1);
        }
        try {
            std::println("{}", worker.disconnectTracker());
        } catch (const std::exception &e) {
            std::cerr << "Disconnect failed: " << e.what() << "\n";
            return 1;
        }
        //server->Wait();
        //worker.wait();
    } else if (std::string(argv[1]) == "generate" && argc == 4) {
        // generate manifest
        const std::filesystem::path path(argv[2]);
        const std::filesystem::path manifestPath(argv[3]);
        const std::size_t chunkSize = 4194304; // 4 MB chunks

        std::ifstream file(path, std::ios::binary);
        if (!file) {
            std::cerr << "Error: Could not open file.\n";
            return 1;
        }

        std::vector<char> buffer(chunkSize);
        std::vector<std::pair<int, std::string> > chunks; // size, hash
        std::size_t totalBytesRead = 0;

        std::ofstream ofile(manifestPath);
        // Always verify the file opened successfully
        if (!ofile.is_open()) {
            std::cerr << "Error opening file.\n";
            return 1;
        }
        while (file.read(buffer.data(), buffer.size()) || file.gcount() > 0) {
            std::streamsize bytesRead = file.gcount();

            // Process buffer[0] through buffer[bytesRead - 1]
            auto hash = sha256(std::string(buffer.begin(), buffer.begin() + bytesRead));
            chunks.push_back(std::make_pair(bytesRead, hash));
            std::println("{}: {}", bytesRead, hash);
            totalBytesRead += bytesRead;
        }
        Manifest m{path, chunks, totalBytesRead};
        ofile << m;
        ofile.close();
        file.close();
    }
    return 0;
}
