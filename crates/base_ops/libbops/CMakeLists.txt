# Standard CMake prelude - nothing special
cmake_minimum_required(VERSION 3.0)
project(libbops C)

file(GLOB libbops_src "*.h" "*.c")

add_library(libbops STATIC ${libbops_src}) 

# for cmake
install(TARGETS libbops DESTINATION .)


