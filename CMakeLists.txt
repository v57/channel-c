cmake_minimum_required(VERSION 3.20)
project(channel_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
option(CHANNEL_C_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

if(CHANNEL_C_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined)
  add_link_options(-fsanitize=address,undefined)
endif()

if(ENABLE_COVERAGE)
  add_compile_options(-O0 -g --coverage)
  add_link_options(--coverage)
endif()

find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
pkg_check_modules(LIBWEBSOCKETS REQUIRED IMPORTED_TARGET libwebsockets)
pkg_check_modules(YYJSON REQUIRED IMPORTED_TARGET yyjson)
pkg_check_modules(CMOCKA REQUIRED IMPORTED_TARGET cmocka)

add_library(channel_c STATIC
  src/core/channel.c
  src/client/client.c
  src/server/server.c
  src/process/process.c
)

target_include_directories(channel_c
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/core
)

target_link_libraries(channel_c
  PUBLIC
    PkgConfig::LIBWEBSOCKETS
    PkgConfig::YYJSON
    OpenSSL::SSL
    OpenSSL::Crypto
)

enable_testing()

add_executable(test_process tests/unit/test_process.c)
target_link_libraries(test_process PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME unit_process COMMAND test_process)

add_executable(test_internal tests/unit/test_internal.c)
target_link_libraries(test_internal PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME unit_internal COMMAND test_internal)

add_executable(test_client_server_errors tests/unit/test_client_server_errors.c)
target_link_libraries(test_client_server_errors PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME unit_client_server_errors COMMAND test_client_server_errors)

add_executable(test_failpoints tests/unit/test_failpoints.c)
target_link_libraries(test_failpoints PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME unit_failpoints COMMAND test_failpoints)

add_executable(test_ts_server tests/integration/test_ts_server.c)
target_link_libraries(test_ts_server PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME integration_ts_server COMMAND test_ts_server)

add_executable(test_c_server tests/integration/test_c_server.c)
target_link_libraries(test_c_server PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME integration_c_server COMMAND test_c_server)

add_executable(test_c_robustness tests/integration/test_c_robustness.c)
target_link_libraries(test_c_robustness PRIVATE channel_c PkgConfig::CMOCKA)
add_test(NAME integration_c_robustness COMMAND test_c_robustness)

add_executable(channel_benchmark bench/benchmark.c)
target_link_libraries(channel_benchmark PRIVATE channel_c)
