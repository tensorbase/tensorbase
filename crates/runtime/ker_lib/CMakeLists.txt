cmake_minimum_required(VERSION 3.15)
project(ker_runner C)

set(CMAKE_C_STANDARD 11)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_C_FLAGS "-Wall -Wextra")
set(CMAKE_C_FLAGS_DEBUG "-g")
set(CMAKE_C_FLAGS_RELEASE "-O2 -march=native")

add_executable(ker_runner ker_lib.c ker_runner.c)
TARGET_LINK_LIBRARIES(ker_runner pthread)
