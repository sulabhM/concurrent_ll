cmake_minimum_required(VERSION 3.14)

project(concurrent_ll
    VERSION 1.0.0
    DESCRIPTION "Lock-free concurrent linked list with MVCC semantics"
    LANGUAGES C CXX
)

# Options
option(BUILD_TESTS "Build the test suite" ON)
option(BUILD_SHARED_LIBS "Build shared library" OFF)

# C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# C++ standard (for tests)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler warnings
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Thread support
find_package(Threads REQUIRED)

# ==============================================================================
# Library target
# ==============================================================================

add_library(concurrent_ll
    list.c
)

target_include_directories(concurrent_ll
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(concurrent_ll
    PUBLIC
        Threads::Threads
)

# Set library properties
set_target_properties(concurrent_ll PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "list.h;list_cxx.h"
)

# ==============================================================================
# Tests
# ==============================================================================

if(BUILD_TESTS)
    # Fetch Catch2
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v2.13.10
    )
    FetchContent_MakeAvailable(Catch2)

    enable_testing()

    add_executable(concurrent_ll_tests
        tests/main.cpp
        tests/test_concurrent_ll.cpp
    )

    target_link_libraries(concurrent_ll_tests
        PRIVATE
            concurrent_ll
            Catch2::Catch2
    )

    # Include path for standalone build
    target_include_directories(concurrent_ll_tests
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
    )

    add_test(NAME concurrent_ll_tests COMMAND concurrent_ll_tests)
endif()

# ==============================================================================
# Installation
# ==============================================================================

include(GNUInstallDirs)

install(TARGETS concurrent_ll
    EXPORT concurrent_ll-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/concurrent_ll
)

install(EXPORT concurrent_ll-targets
    FILE concurrent_ll-targets.cmake
    NAMESPACE concurrent_ll::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/concurrent_ll
)

# Create package config file
include(CMakePackageConfigHelpers)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/concurrent_ll-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/concurrent_ll-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/concurrent_ll-config.cmake
    @ONLY
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/concurrent_ll-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/concurrent_ll-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/concurrent_ll
)
