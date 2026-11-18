//
// Live terminal view of a swarm. Polls the tracker for peers, asks each peer
// which chunks it has, and redraws a chunk map per peer so missing chunks are
// seen gradually turning into done ones. A message log shows every request and
// reply the visualizer makes.
//
// Usage: visualizer <tracker_addr> <filename> [interval_ms]
//   e.g. visualizer 127.0.0.1:50051 out1 500

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <format>
#include <iostream>
#include <print>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "bittorrent.grpc.pb.h"
#include "swarm.h"

namespace {

constexpr int kCellsPerRow = 64;

const char *kReset = "\033[0m";
const char *kBold = "\033[1m";
const char *kDim = "\033[90m";
const char *kGreen = "\033[32m";
const char *kYellow = "\033[33m";
const char *kBrightYellow = "\033[1;93m";
const char *kRed = "\033[31m";
const char *kEraseLine = "\033[K";     // clear from cursor to end of line
const char *kEraseBelow = "\033[J";    // clear from cursor to end of screen

std::atomic<bool> stopRequested{false};

void onStopSignal(int) {
    stopRequested = true;
}

const char *colorFor(LogLevel level) {
    switch (level) {
        case LogLevel::Ok: return kGreen;
        case LogLevel::Error: return kRed;
        default: return "";
    }
}

// Builds one frame into a buffer and writes it in a single pass. The cursor is
// moved home instead of clearing the screen, and every line erases its own
// leftovers, so the view updates in place rather than scrolling.
void render(const Swarm &swarm, const std::string &filename, std::chrono::milliseconds interval) {
    std::ostringstream out;
    out << "\033[H";
    std::println(out, "{}swarm: {}{}   refresh {} ms   ctrl-c to quit{}", kBold, filename, kReset,
                 interval.count(), kEraseLine);
    std::println(out, "legend: {}█{} done  {}█{} just filled  {}◐{} in progress  {}·{} missing{}\n",
                 kGreen, kReset, kBrightYellow, kReset, kYellow, kReset, kDim, kReset, kEraseLine);

    for (const auto &[addr, view]: swarm.peers) {
        if (!view.error.empty()) {
            std::println(out, "{}{}  unreachable: {}{}{}\n", kRed, addr, view.error, kReset, kEraseLine);
            continue;
        }
        int done = 0;
        for (const auto s: view.chunks) {
            if (s == bittorrent::ChunkState::Done) ++done;
        }
        std::println(out, "{}{}{}  {}/{} done{}", kBold, addr, kReset, done, view.chunks.size(), kEraseLine);

        for (std::size_t row = 0; row < view.chunks.size(); row += kCellsPerRow) {
            out << "  ";
            const std::size_t end = std::min(view.chunks.size(), row + kCellsPerRow);
            for (std::size_t i = row; i < end; ++i) {
                switch (view.chunks[i]) {
                    case bittorrent::ChunkState::Done:
                        out << std::format("{}█{}", view.justFilled(i) ? kBrightYellow : kGreen, kReset);
                        break;
                    case bittorrent::ChunkState::InProgress:
                        out << std::format("{}◐{}", kYellow, kReset);
                        break;
                    default:
                        out << std::format("{}·{}", kDim, kReset);
                        break;
                }
            }
            out << kEraseLine << '\n';
        }
        out << kEraseLine << '\n';
    }

    std::println(out, "{}messages{}{}", kBold, kReset, kEraseLine);
    for (const auto &entry: swarm.log) {
        std::println(out, "  {}{}{}{}", colorFor(entry.level), entry.text, kReset, kEraseLine);
    }
    out << kEraseBelow;
    std::cout << out.str() << std::flush;
}

} // namespace

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::println(stderr, "usage: visualizer <tracker_addr> <filename> [interval_ms]");
        return 1;
    }
    const std::string trackerAddr = argv[1];
    const std::string filename = argv[2];
    const auto interval = std::chrono::milliseconds(argc > 3 ? std::atoi(argv[3]) : 500);

    std::signal(SIGINT, onStopSignal);
    std::signal(SIGTERM, onStopSignal);

    auto tracker = bittorrent::Tracker::NewStub(grpc::CreateChannel(trackerAddr, grpc::InsecureChannelCredentials()));
    PeerStubs stubs;
    Swarm swarm;

    std::print("\033[2J\033[?25l"); // clear once, then hide cursor while drawing
    while (!stopRequested) {
        applyPeers(swarm, fetchPeers(*tracker, filename));
        for (const auto &addr: swarm.addrs) {
            applyPeerChunks(swarm, fetchPeerChunks(stubs, addr, filename), std::chrono::steady_clock::now());
        }
        render(swarm, filename, interval);
        std::this_thread::sleep_for(interval);
    }
    std::print("\033[?25h{}\n", kReset); // restore cursor
    return 0;
}
