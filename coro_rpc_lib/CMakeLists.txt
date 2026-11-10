cmake_minimum_required(VERSION 3.10.0)

project(coro_rpc_lib
        DESCRIPTION "coro_rpc_lib"
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

option(ENABLE_TSAN "Enable thread sanitizer" OFF)
option(ENABLE_ASAN "Enable address sanitizer" OFF)
if (ENABLE_TSAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
else()
    if(ENABLE_ASAN)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
endif()

include_directories(../deps/yalantinglibs/include)
include_directories(../deps/yalantinglibs/include/ylt/thirdparty)

add_library(coro_rpc coro_rpc_lib/coro_rpc.cpp)

option(ENABLE_RDMA "Enable RDMA support" OFF)
if (ENABLE_RDMA)
    message(STATUS "Enable RDMA support")
    add_compile_definitions("YLT_ENABLE_IBV")
    target_link_libraries(coro_rpc -libverbs)
endif()

option(ENABLE_TEST "Enable test executable" OFF)
if (ENABLE_TEST)
    add_executable(test_rpc_lib tests/test_rpc_lib.cpp)
    # doctest is a single header already vendored by the ylt submodule
    target_include_directories(test_rpc_lib PRIVATE ../deps/yalantinglibs/src/include)
    link_directories(build)
    target_link_libraries(test_rpc_lib coro_rpc pthread)
    add_test(NAME test_rpc_lib COMMAND test_rpc_lib)
endif()

add_compile_definitions("STRUCT_PACK_DISABLE_OUTER_BRACE")
