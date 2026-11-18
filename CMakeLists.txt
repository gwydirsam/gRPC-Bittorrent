cmake_minimum_required(VERSION 4.3)
project(s3_bittorrent_cpp)

set(CMAKE_CXX_STANDARD 26)

find_package(OpenSSL REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Generate C++ protobuf and gRPC sources from proto/*.proto into the build tree.
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_FILE ${PROTO_DIR}/bittorrent.proto)
set(PROTO_SRCS ${PROTO_GEN_DIR}/bittorrent.pb.cc)
set(PROTO_HDRS ${PROTO_GEN_DIR}/bittorrent.pb.h)
set(GRPC_SRCS ${PROTO_GEN_DIR}/bittorrent.grpc.pb.cc)
set(GRPC_HDRS ${PROTO_GEN_DIR}/bittorrent.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS --cpp_out=${PROTO_GEN_DIR}
       --grpc_out=${PROTO_GEN_DIR}
       --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
       -I ${PROTO_DIR}
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE} protobuf::protoc gRPC::grpc_cpp_plugin
  COMMENT "Generating gRPC sources from ${PROTO_FILE}"
  VERBATIM)

add_library(s3_proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(s3_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(s3_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_executable(s3_bittorrent_cpp main.cpp
        util.h
        tracker.h
        worker.h
        tracker_svc.h
        peer_svc.h)
target_link_libraries(s3_bittorrent_cpp PRIVATE OpenSSL::Crypto s3_proto)

# Live terminal view of a swarm's chunk progress; talks to the tracker and peers over gRPC.
add_executable(visualizer visualizer.cpp swarm.h)
target_link_libraries(visualizer PRIVATE s3_proto)

# Graphical view of the same swarm, drawn with GLFW and Dear ImGui.
find_package(glfw3 REQUIRED)
find_package(OpenGL REQUIRED)

include(FetchContent)
FetchContent_Declare(imgui
        GIT_REPOSITORY https://github.com/ocornut/imgui.git
        GIT_TAG v1.91.9
        GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(imgui)

# Dear ImGui ships without a CMakeLists, so build its core and the GLFW/OpenGL3 backends directly.
add_library(imgui_glfw STATIC
        ${imgui_SOURCE_DIR}/imgui.cpp
        ${imgui_SOURCE_DIR}/imgui_draw.cpp
        ${imgui_SOURCE_DIR}/imgui_tables.cpp
        ${imgui_SOURCE_DIR}/imgui_widgets.cpp
        ${imgui_SOURCE_DIR}/backends/imgui_impl_glfw.cpp
        ${imgui_SOURCE_DIR}/backends/imgui_impl_opengl3.cpp)
target_include_directories(imgui_glfw PUBLIC ${imgui_SOURCE_DIR} ${imgui_SOURCE_DIR}/backends)
target_compile_definitions(imgui_glfw PUBLIC GL_SILENCE_DEPRECATION)
target_link_libraries(imgui_glfw PUBLIC glfw OpenGL::GL)

add_executable(visualizer_gui visualizer_gui.cpp swarm.h)
target_link_libraries(visualizer_gui PRIVATE s3_proto imgui_glfw)
