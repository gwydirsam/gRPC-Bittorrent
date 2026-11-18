//
// Created by Sam Gwydir on 11/17/26.
//

#ifndef S3_BITTORRENT_CPP_TRACKER_H
#define S3_BITTORRENT_CPP_TRACKER_H

#include <format>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

class Tracker {
public:
    Tracker() = default;

    void peerConnect(std::string &addr) {
        bool ok = true;
        {
            std::lock_guard<std::mutex> _(_mtx);
            std::unordered_set<std::string>::iterator it;
            ok = _peers.insert(addr).second;
        }
        if (ok) std::cout << std::format("{} connected", addr) << std::endl;
    }

    void peerDisconnect(std::string &addr) {
        {
            std::lock_guard<std::mutex> _(_mtx);
            _peers.erase(addr);
        }
        std::cout << std::format("{} disconnected", addr) << std::endl;
    }

    std::vector<std::string> getPeers() {
        return std::vector<std::string>(_peers.begin(), _peers.end());
    }

private:
    std::mutex _mtx;
    std::unordered_set<std::string> _peers;
};

#endif //S3_BITTORRENT_CPP_TRACKER_H
