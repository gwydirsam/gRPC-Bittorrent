//
// Created by Sam Gwydir on 11/17/26.
//

#ifndef S3_BITTORRENT_CPP_WORKER_H
#define S3_BITTORRENT_CPP_WORKER_H

#include <chrono>
#include <format>
#include <fstream>
#include <memory>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"
#include "util.h"

class ChunkFile {
public:
    ChunkFile(const Manifest& m, const std::string& outFile) : _fileName(outFile), _manifest(m) {
        openOrCreateFile();
    }

    bool setupFile() {
        auto sizeInBytes = _manifest.totalBytes;
        if (sizeInBytes == 0) {
            std::ofstream file(_fileName, std::ios::binary | std::ios::trunc);
            return file.is_open();
        }

        std::ofstream file(_fileName, std::ios::binary | std::ios::trunc);
        if (!file) {
            return false;
        }

        // Move the write pointer to the last byte position
        file.seekp(sizeInBytes - 1);

        // Write a single null byte to set the file size
        file.write("", 1);

        return file.good();
    }

    void openOrCreateFile() {
        _chunks.clear();
        for (std::size_t i = 0; i < _manifest.chunks.size(); ++i) {
            _chunks.emplace_back(static_cast<ChunkID>(i), _manifest.chunks[i].second, ChunkState::Missing, ChunkData{});
        }
        std::filesystem::path path(_fileName);
        if (!std::filesystem::exists(path)) {
            setupFile();
            return;
        }
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            std::cerr << "Error: Could not open file.\n";
            throw std::runtime_error("Could not open file.");
        }

        std::vector<char> buffer(_chunkSize);
        std::size_t totalBytesRead = 0;
        ChunkID id = 0;

        while (file.read(buffer.data(), buffer.size()) || file.gcount() > 0) {
            std::streamsize bytesRead = file.gcount();

            // Process buffer[0] through buffer[bytesRead - 1]
            auto hash = sha256(std::string(buffer.begin(), buffer.begin() + bytesRead));
            if (id >= _manifest.chunks.size()) {
                throw std::runtime_error("File has more chunks than the manifest.");
            }
            std::string data(buffer.data(), bytesRead);
            Chunk chunk = (_manifest.chunks[id].first == bytesRead && _manifest.chunks[id].second == hash)
                              ? Chunk(id, hash, ChunkState::Done, data)
                              : Chunk(id, hash, ChunkState::Missing, "");
            _chunks[id] = chunk;
            ++id;
        }
        assert(id == _chunks.size());
        file.close();
    }

    void WriteChunk(const Chunk &c) {
        if (c.id >= _manifest.chunks.size()) {
            throw std::runtime_error("Chunk id out of range");
        }
        const auto &[size, hash] = _manifest.chunks[c.id];
        if (c.hash != hash || c.data.size() != static_cast<std::size_t>(size)) {
            throw std::runtime_error("Chunk does not match manifest");
        }

        std::fstream file(_fileName, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) {
            throw std::runtime_error("Could not open file.");
        }
        file.seekp(static_cast<std::streamoff>(c.id * _chunkSize), std::ios::beg);
        file.write(c.data.data(), static_cast<std::streamsize>(c.data.size()));
        if (!file) {
            throw std::runtime_error("Failed to write chunk");
        }
        _chunks[c.id] = c;
    }

    Chunk ReadChunk(const ChunkID id) {
        if (id >= _chunks.size()) {
            throw std::runtime_error("Chunk id out of range");
        }
        std::ifstream file(_fileName, std::ios::binary);
        if (!file) {
            std::cerr << "Error: Could not open file.\n";
            throw std::runtime_error("Could not open file.");
        }

        file.seekg(static_cast<std::streamoff>(id * _chunkSize), std::ios::beg);
        if (!file) {
            throw std::runtime_error("Failed to seek to offset");
        }

        // Read up to _chunkSize bytes; the last chunk may be shorter at end of file.
        std::string data(_chunkSize, '\0');
        file.read(data.data(), _chunkSize);
        data.resize(file.gcount());
        if (data.empty()) {
            throw std::runtime_error("No data at chunk offset");
        }
        return Chunk(id, sha256(data), ChunkState::Done, data);
    }

    std::vector<Chunk> GetChunks() {
        return _chunks;
    }

    std::string getFileName() {
        return _fileName;
    }

private:
    Manifest _manifest;
    std::vector<Chunk> _chunks;
    std::string _fileName;
    const std::size_t _chunkSize = 4194304; // 4 MB chunks
};

class Worker {
public:
    Worker(WorkerID id, std::string trackerAddr, Manifest &m, ChunkFile& cf):
                                                                _chunkFile(cf),
                                                                _id(id), _trackerAddr(std::move(trackerAddr)),
                                                                _m(std::move(m)) {
        _chunks = cf.GetChunks();
    };

    void getStatus() {
        int missing_count = 0;
        int done_count = 0;
        int in_progress_count = 0;
        for (const auto &c: _chunks) {
            switch (c.state) {
                case ChunkState::Missing:
                    ++missing_count;
                    break;
                case ChunkState::InProgress:
                    ++in_progress_count;
                    break;
                case ChunkState::Done:
                    ++done_count;
                    break;
            }
        }
        std::println("Missing: {}, In Progress: {}, Done: {}", missing_count, in_progress_count, done_count);
        printThroughput();
    }

    void printPeerChunks(const PeerAddr &peer) {
        std::vector<ChunkState> chunks;
        {
            std::lock_guard<std::mutex> lock(_available_chunk_mtx);
            chunks = _AvailableChunkMap[peer];
        }
        int missing_count = 0;
        int done_count = 0;
        int in_progress_count = 0;
        for (const auto &c: chunks) {
            switch (c) {
                case ChunkState::Missing:
                    ++missing_count;
                    break;
                case ChunkState::InProgress:
                    ++in_progress_count;
                    break;
                case ChunkState::Done:
                    ++done_count;
                    break;
            }
        }
        std::println("{}: Missing: {}, In Progress: {}, Done: {}", peer.to_string(), missing_count,
                     in_progress_count, done_count);
    }

    std::string pingTracker() {
        auto stub = bittorrent::Tracker::NewStub(grpc::CreateChannel(_trackerAddr, grpc::InsecureChannelCredentials()));
        bittorrent::PingRequest request;
        bittorrent::Worker *worker = request.mutable_worker();
        worker->set_ip(_peerAddr.ip);
        worker->set_port(_peerAddr.port);
        bittorrent::PingReply reply;
        grpc::ClientContext context;
        grpc::Status status = stub->Ping(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        return reply.message();
    }

    std::string disconnectTracker() {
        auto stub = bittorrent::Tracker::NewStub(grpc::CreateChannel(_trackerAddr, grpc::InsecureChannelCredentials()));
        bittorrent::DisconnectRequest request;
        bittorrent::Worker *worker = request.mutable_worker();
        worker->set_ip(_peerAddr.ip);
        worker->set_port(_peerAddr.port);
        bittorrent::DisconnectReply reply;
        grpc::ClientContext context;
        grpc::Status status = stub->Disconnect(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        return reply.message();
    }

    std::string pingPeer(const PeerAddr &peer) {
        auto stub = bittorrent::Peer::NewStub(makeChannel(peer.to_string()));
        bittorrent::PingRequest request;
        bittorrent::Worker *worker = request.mutable_worker();
        worker->set_ip(peer.ip);
        worker->set_port(peer.port);
        bittorrent::PingReply reply;
        grpc::ClientContext context;
        grpc::Status status = stub->Ping(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        return reply.message();
    }

    std::vector<PeerAddr> getPeers() {
        auto stub = bittorrent::Tracker::NewStub(grpc::CreateChannel(_trackerAddr, grpc::InsecureChannelCredentials()));
        bittorrent::GetWorkersRequest request;
        request.set_filename(_chunkFile.getFileName());
        bittorrent::GetWorkersReply reply;
        grpc::ClientContext context;
        grpc::Status status = stub->GetWorkers(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        std::vector<PeerAddr> peers;
        for (const auto &w: reply.workers()) {
            PeerAddr p(w.ip(), w.port());
            if (p == _peerAddr) continue;
            peers.push_back(p);
        }
        _peers = peers;
        return peers;
    }

    std::vector<ChunkState> getPeerChunks(const PeerAddr &peer) {
        auto stub = peerStub(peer);
        bittorrent::GetAvailableChunksRequest request;
        request.set_filename(_chunkFile.getFileName());
        bittorrent::GetAvailableChunksReply reply;
        grpc::ClientContext context;
        grpc::Status status = stub.GetAvailableChunks(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        std::vector<ChunkState> chunks;
        chunks.resize(reply.chunkcount());
        for (const auto &c: reply.chunks()) {
            chunks[c.chunkid()] = *FromProto(c.chunkstate());
        }
        setAvailableChunks(peer, chunks);
        printPeerChunks(peer);
        return chunks;
    }

    void getChunk(const PeerAddr &peer, ChunkID chunkid) {
        auto stub = peerStub(peer);
        bittorrent::GetChunksRequest request;
        request.set_filename(_chunkFile.getFileName());
        request.set_chunkid(chunkid);
        bittorrent::GetChunksReply reply;
        grpc::ClientContext context;
        const auto start = std::chrono::steady_clock::now();
        grpc::Status status = stub.GetChunks(&context, request, &reply);
        if (!status.ok()) throw std::runtime_error(status.error_message());
        Chunk c(reply.chunk());
        addChunk(c);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        if (!_downloadStarted) {
            _downloadStart = start;
            _downloadStarted = true;
        }
        _bytesDownloaded += c.data.size();
        std::println("chunk {}: {} bytes in {:.1f} ms", c.id, c.data.size(),
                     std::chrono::duration<double, std::milli>(elapsed).count());
        if (!_downloadFinished && isDone()) {
            _downloadEnd = start + elapsed;
            _downloadFinished = true;
            printThroughput();
        }
    }

    void printThroughput() {
        if (!_downloadStarted) return;
        const auto end = _downloadFinished ? _downloadEnd : std::chrono::steady_clock::now();
        const double secs = std::chrono::duration<double>(end - _downloadStart).count();
        if (secs <= 0) return;
        const double mb = static_cast<double>(_bytesDownloaded) / (1024.0 * 1024.0);
        std::println("Downloaded {:.2f} MB in {:.1f} s ({:.2f} MB/s)", mb, secs, mb / secs);
    }

    void getNextChunk(const PeerAddr &peer) {
        std::vector<ChunkState> peerChunks;
        {
            std::lock_guard<std::mutex> lock(_available_chunk_mtx);
            peerChunks = _AvailableChunkMap[peer];
        }
        for (ChunkID i = 0; i < peerChunks.size(); ++i) {
            if (peerChunks[i] == ChunkState::Done && getChunkState(i) != ChunkState::Done) {
                getChunk(peer, i);
                //break;
            }
        }
    }

    std::vector<ChunkID> checkChunks() {
        return {};
    }

    void setServerAddr(const std::string &ip, const int port) {
        _peerAddr = PeerAddr(ip, port);
    }

    bool isDone() {
        for (const auto &c: _chunks) {
            if (c.state != ChunkState::Done) {
                return false;
            }
        }
        return true;
    }

    bittorrent::Peer::Stub &peerStub(const PeerAddr &peer) {
        std::lock_guard<std::mutex> lock(_mtx);
        auto &s = _stubs[peer];
        if (!s) {
            _stubs[peer] = bittorrent::Peer::NewStub(makeChannel(peer.to_string()));
        }
        return *s;
    }

    std::vector<ChunkState> getAvailableChunks() {
        std::lock_guard<std::mutex> lock(_mtx);
        std::vector<ChunkState> states;
        states.reserve(_chunks.size());
        for (const auto &c: _chunks) {
            states.push_back(c.state);
        }
        return states;
    }

    void setAvailableChunks(const PeerAddr &p, std::vector<ChunkState> &available) {
        {
            std::lock_guard<std::mutex> lock(_available_chunk_mtx);
            _AvailableChunkMap[p] = available;
            for (int i = 0; i < available.size(); i++) {
                _AvailableChunkPeerMap[i].push_back(p);
            }
        }
    }

    void addChunk(Chunk chunk) {
        {
            std::lock_guard<std::mutex> lock(_mtx);
            _chunks[chunk.id] = chunk;
        }
        _chunkFile.WriteChunk(chunk);
    }

    Chunk getChunk(ChunkID id) {
            std::lock_guard<std::mutex> lock(_mtx);
        if (_chunks[id].state == ChunkState::Done) {
            return _chunkFile.ReadChunk(id);
        }
        throw std::runtime_error("Chunk not done!");
    }

    ChunkState getChunkState(ChunkID id) {
        std::lock_guard<std::mutex> lock(_mtx);
        return _chunks[id].state;
    }

    std::string getFileName() {
        return _chunkFile.getFileName();
    }

    std::size_t getChunkCount() {
        return _chunks.size();
    }

    void setHasAllChunks() {
        std::lock_guard<std::mutex> lock(_mtx);
        for (auto &c: _chunks) {
            c.state = ChunkState::Done;
        }
    }

private:
    ChunkFile &_chunkFile;

    bool _downloadStarted = false;
    bool _downloadFinished = false;
    std::chrono::steady_clock::time_point _downloadStart;
    std::chrono::steady_clock::time_point _downloadEnd;
    std::size_t _bytesDownloaded = 0;

    WorkerID _id;
    PeerAddr _peerAddr;
    std::string _trackerAddr;

    std::mutex _mtx;
    std::vector<Chunk> _chunks;

    std::mutex _available_chunk_mtx;
    std::unordered_map<PeerAddr, std::vector<ChunkState> > _AvailableChunkMap;
    std::unordered_map<ChunkID, std::vector<PeerAddr> > _AvailableChunkPeerMap;

    std::unordered_map<PeerAddr, std::unique_ptr<bittorrent::Peer::Stub> > _stubs;

    Manifest _m;
    std::unique_ptr<grpc::Server> _server;
    std::vector<PeerAddr> _peers;
};

#endif //S3_BITTORRENT_CPP_WORKER_H
