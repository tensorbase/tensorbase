cmake_minimum_required(VERSION 2.8.7)
project(slt)

set(CMAKE_CXX_STANDARD 11)

include_directories(/jin/tensorbase/repo/duckdb/src/include)
link_directories(/jin/tensorbase/repo/duckdb/build/release/src)
link_directories(/jin/tensorbase/repo/duckdb/build/release/third_party/re2)
link_directories(/jin/tensorbase/repo/duckdb/build/release/third_party/miniz)
link_directories(/jin/tensorbase/repo/duckdb/build/release/third_party/libpg_query)

add_executable(slt slt.cpp)
target_link_libraries(slt libduckdb_static.a libre2.a libminiz.a pthread libpg_query.a)