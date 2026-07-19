cmake_minimum_required(VERSION 3.20)
project(veltrixdb_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler flags ─────────────────────────────────────────────────────────────
add_compile_options(
    -O3
    -march=native
    -pthread
    -msse4.2
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter
    -fno-rtti
)

# ── liburing ───────────────────────────────────────────────────────────────────
find_library(URING_LIB uring REQUIRED)
find_path(URING_INCLUDE_DIR liburing.h
    PATHS /usr/include /usr/local/include
    REQUIRED)

# ── veltrixdb_engine static library ───────────────────────────────────────────
add_library(veltrixdb_engine STATIC
    src/lirs_cache.cpp
    src/shard.cpp
    src/write_path.cpp
    src/defragmenter.cpp
    src/art.cpp              # ART index + Janitor compaction
    src/scheduler.cpp        # Priority io_uring scheduler
    src/storage_bridge.cpp   # 8-ring SQPOLL batch-write bridge
)

target_include_directories(veltrixdb_engine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${URING_INCLUDE_DIR}
)

target_link_libraries(veltrixdb_engine
    PUBLIC
        ${URING_LIB}
        pthread
)

target_compile_features(veltrixdb_engine PUBLIC cxx_std_20)

# ── Link-time optimisation (release builds) ───────────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET veltrixdb_engine PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# ── Tests (optional, requires GTest) ──────────────────────────────────────────
option(VELTRIX_BUILD_TESTS "Build unit tests" OFF)

if(VELTRIX_BUILD_TESTS)
    find_package(GTest QUIET)
    if(GTest_FOUND)
        enable_testing()
        add_subdirectory(tests)
    else()
        message(WARNING "GTest not found — skipping tests")
    endif()
endif()
