//
// Graphical view of a swarm, the GLFW/Dear ImGui counterpart of visualizer.cpp.
// A background thread polls the tracker and peers; the window draws each peer's
// chunk map, with chunks fading from yellow to green as they finish, and a log
// of every request and reply.
//
// Usage: visualizer_gui <tracker_addr> <filename> [interval_ms]
//   e.g. visualizer_gui 127.0.0.1:50051 out1 500

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <print>
#include <string>
#include <thread>
#include <vector>
#include <GLFW/glfw3.h>
#ifdef __APPLE__
#include <OpenGL/gl.h>
#else
#include <GL/gl.h>
#endif
#include "imgui.h"
#include "imgui_impl_glfw.h"
#include "imgui_impl_opengl3.h"
#include "bittorrent.grpc.pb.h"
#include "swarm.h"

namespace {

constexpr auto kFadeDuration = std::chrono::milliseconds(1500);
constexpr float kLogHeight = 220.0f;

std::mutex gSwarmMutex;   // guards gSwarm, which the poller writes and the window reads
Swarm gSwarm;
std::atomic<bool> gStop{false};

// Runs on its own thread so a slow or dead peer never stalls the window.
void pollLoop(std::string trackerAddr, std::string filename, std::chrono::milliseconds interval) {
    auto tracker = bittorrent::Tracker::NewStub(grpc::CreateChannel(trackerAddr, grpc::InsecureChannelCredentials()));
    PeerStubs stubs;
    while (!gStop) {
        // Network calls happen without the lock; only the results are applied under it.
        const auto trackerReply = fetchPeers(*tracker, filename);
        std::vector<PeerReply> replies;
        if (trackerReply.ok) {
            for (const auto &addr: trackerReply.addrs) {
                replies.push_back(fetchPeerChunks(stubs, addr, filename));
            }
        }
        {
            std::lock_guard lock(gSwarmMutex);
            applyPeers(gSwarm, trackerReply);
            const auto now = std::chrono::steady_clock::now();
            for (const auto &reply: replies) {
                applyPeerChunks(gSwarm, reply, now);
            }
        }
        std::this_thread::sleep_for(interval);
    }
}

std::uint8_t lerpChannel(float from, float to, float t) {
    return static_cast<std::uint8_t>(from + (to - from) * t);
}

ImU32 cellColor(const PeerView &view, std::size_t i, std::chrono::steady_clock::time_point now) {
    switch (view.chunks[i]) {
        case bittorrent::ChunkState::Done: {
            const float age = std::chrono::duration<float>(now - view.filledAt[i]).count();
            const float fade = std::chrono::duration<float>(kFadeDuration).count();
            const float t = std::clamp(age / fade, 0.0f, 1.0f);   // 0 = just filled, 1 = settled
            return IM_COL32(lerpChannel(255, 80, t), lerpChannel(220, 200, t), lerpChannel(80, 110, t), 255);
        }
        case bittorrent::ChunkState::InProgress:
            return IM_COL32(200, 150, 40, 255);
        default:
            return IM_COL32(60, 60, 66, 255);
    }
}

void drawGrid(const PeerView &view, std::chrono::steady_clock::time_point now) {
    constexpr float kCell = 14.0f;
    constexpr float kGap = 3.0f;
    constexpr float kPitch = kCell + kGap;

    const float width = ImGui::GetContentRegionAvail().x;
    const std::size_t cols = std::max(1.0f, (width + kGap) / kPitch);
    const ImVec2 origin = ImGui::GetCursorScreenPos();
    ImDrawList *drawList = ImGui::GetWindowDrawList();
    for (std::size_t i = 0; i < view.chunks.size(); ++i) {
        const float x = origin.x + static_cast<float>(i % cols) * kPitch;
        const float y = origin.y + static_cast<float>(i / cols) * kPitch;
        drawList->AddRectFilled({x, y}, {x + kCell, y + kCell}, cellColor(view, i, now), 2.0f);
    }
    const std::size_t rows = (view.chunks.size() + cols - 1) / cols;
    ImGui::Dummy({width, static_cast<float>(rows) * kPitch});
}

ImVec4 logColor(LogLevel level) {
    switch (level) {
        case LogLevel::Ok: return {0.35f, 0.85f, 0.45f, 1.0f};
        case LogLevel::Error: return {0.95f, 0.35f, 0.35f, 1.0f};
        default: return {0.8f, 0.8f, 0.8f, 1.0f};
    }
}

void drawWindow(const std::string &filename, std::chrono::milliseconds interval) {
    const ImGuiViewport *viewport = ImGui::GetMainViewport();
    ImGui::SetNextWindowPos(viewport->WorkPos);
    ImGui::SetNextWindowSize(viewport->WorkSize);
    constexpr ImGuiWindowFlags kFlags = ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                                        ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse;
    ImGui::Begin("swarm", nullptr, kFlags);

    ImGui::Text("swarm: %s   refresh %lld ms", filename.c_str(), static_cast<long long>(interval.count()));
    ImGui::TextColored({0.35f, 0.85f, 0.45f, 1.0f}, "done");
    ImGui::SameLine();
    ImGui::TextColored({1.0f, 0.86f, 0.31f, 1.0f}, "just filled");
    ImGui::SameLine();
    ImGui::TextColored({0.78f, 0.59f, 0.16f, 1.0f}, "in progress");
    ImGui::SameLine();
    ImGui::TextColored({0.24f, 0.24f, 0.26f, 1.0f}, "missing");
    ImGui::Separator();

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(gSwarmMutex);
        ImGui::BeginChild("peers", ImVec2(0, -kLogHeight));
        for (const auto &[addr, view]: gSwarm.peers) {
            if (!view.error.empty()) {
                ImGui::TextColored(logColor(LogLevel::Error), "%s  unreachable: %s", addr.c_str(), view.error.c_str());
                ImGui::Spacing();
                continue;
            }
            const auto done = std::ranges::count(view.chunks, bittorrent::ChunkState::Done);
            ImGui::Text("%s  %lld/%zu done", addr.c_str(), static_cast<long long>(done), view.chunks.size());
            drawGrid(view, now);
            ImGui::Spacing();
        }
        ImGui::EndChild();

        ImGui::Separator();
        ImGui::Text("messages");
        ImGui::BeginChild("log", ImVec2(0, 0), ImGuiChildFlags_Borders);
        const bool atBottom = ImGui::GetScrollY() >= ImGui::GetScrollMaxY();
        for (const auto &entry: gSwarm.log) {
            ImGui::TextColored(logColor(entry.level), "%s", entry.text.c_str());
        }
        if (atBottom) ImGui::SetScrollHereY(1.0f);
        ImGui::EndChild();
    }

    ImGui::End();
}

} // namespace

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::println(stderr, "usage: visualizer_gui <tracker_addr> <filename> [interval_ms]");
        return 1;
    }
    const std::string trackerAddr = argv[1];
    const std::string filename = argv[2];
    const auto interval = std::chrono::milliseconds(argc > 3 ? std::atoi(argv[3]) : 500);

    if (!glfwInit()) {
        std::println(stderr, "failed to initialise GLFW");
        return 1;
    }
    glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, 3);
    glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, 2);
    glfwWindowHint(GLFW_OPENGL_FORWARD_COMPAT, GLFW_TRUE);
    glfwWindowHint(GLFW_OPENGL_PROFILE, GLFW_OPENGL_CORE_PROFILE);
    GLFWwindow *window = glfwCreateWindow(1000, 760, "swarm", nullptr, nullptr);
    if (!window) {
        std::println(stderr, "failed to create window");
        glfwTerminate();
        return 1;
    }
    glfwMakeContextCurrent(window);
    glfwSwapInterval(1);

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGui::StyleColorsDark();
    ImGui_ImplGlfw_InitForOpenGL(window, true);
    ImGui_ImplOpenGL3_Init("#version 150");

    std::thread poller(pollLoop, trackerAddr, filename, interval);

    while (!glfwWindowShouldClose(window)) {
        glfwPollEvents();
        ImGui_ImplOpenGL3_NewFrame();
        ImGui_ImplGlfw_NewFrame();
        ImGui::NewFrame();
        drawWindow(filename, interval);
        ImGui::Render();

        int width = 0;
        int height = 0;
        glfwGetFramebufferSize(window, &width, &height);
        glViewport(0, 0, width, height);
        glClearColor(0.10f, 0.10f, 0.12f, 1.0f);
        glClear(GL_COLOR_BUFFER_BIT);
        ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());
        glfwSwapBuffers(window);
    }

    gStop = true;
    poller.join();

    ImGui_ImplOpenGL3_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();
    glfwDestroyWindow(window);
    glfwTerminate();
    return 0;
}
