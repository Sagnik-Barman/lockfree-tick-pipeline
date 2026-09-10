cmake_minimum_required(VERSION 3.16)
project(lockfree_tick_pipeline CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

find_package(Threads REQUIRED)
include_directories(include)

add_executable(tick_pipeline src/main.cpp)
target_link_libraries(tick_pipeline PRIVATE Threads::Threads)
