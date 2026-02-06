cmake_minimum_required(VERSION 3.19)
project(pg_lab
    VERSION 0.0.3
    DESCRIPTION ""
    LANGUAGES CXX
)

include(FetchContent)

set(CMAKE_GENERATOR "Unix Makefiles")
set(CXX_STANDARD_REQUIRED 20)
set(CMAKE_CXX_STANDARD 20)
add_compile_options(-fPIC -g -Wall -Wextra)

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE "Debug")
    add_compile_options(-O0)
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0)
endif()

execute_process(
    COMMAND which pg_config
    OUTPUT_VARIABLE PG_CONFIG_EXECUTABLE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (PG_CONFIG_EXECUTABLE)
    message(STATUS "Using pg_config executable at: ${PG_CONFIG_EXECUTABLE}")
    execute_process(
        COMMAND ${PG_CONFIG_EXECUTABLE} --includedir
        OUTPUT_VARIABLE PG_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND ${PG_CONFIG_EXECUTABLE} --libdir
        OUTPUT_VARIABLE PG_LIB_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else()
    message(FATAL_ERROR "pg_config not found. Please specify the PostgreSQL server installation directory using PG_INSTALL_DIR.")
endif()

message(STATUS "PostgreSQL include directory: ${PG_INCLUDE_DIR}")
message(STATUS "PostgreSQL library directory: ${PG_LIB_DIR}")


set(PGLAB_TRACE OFF CACHE BOOL "Enable tracing of the hinting extension")
if (PGLAB_TRACE)
    message(STATUS "Enabling tracing")
    add_compile_definitions(PGLAB_TRACE)
endif()

#
# Antlr4 config
#
# See https://github.com/antlr/antlr4/discussions/4418 for reference
#

set(ANTLR4_WITH_STATIC_CRT OFF) # using /MD flag for antlr4_runtime (for Visual C++ compilers only)
set(ANTLR_BUILD_SHARED OFF)     # in my case I built the static version
set(ANTLR_TAG 4.13.1)           # to specify the GitHub tag for the jar too
set(DISABLE_WARNINGS ON)        # Antlr gives a few deprecation warnings for me
set(ANTLR_BUILD_CPP_TESTS OFF)  # to avoid creating dist folder
set(ANTLR_BUILD_CPP_EXAMPLES OFF)

set(ANTLR_EXECUTABLE "${CMAKE_CURRENT_BINARY_DIR}/antlr-${ANTLR_TAG}-complete.jar") # a local path for antlr.jar
if(NOT EXISTS "${ANTLR_EXECUTABLE}")
    message(STATUS "Downloading antlr-${ANTLR_TAG}-complete.jar to ${ANTLR_EXECUTABLE}")
    file(
        DOWNLOAD
        "https://www.antlr.org/download/antlr-${ANTLR_TAG}-complete.jar"
        "${ANTLR_EXECUTABLE}"
    )
endif()
FetchContent_Declare(
    antlr
    GIT_REPOSITORY https://github.com/antlr/antlr4
    GIT_TAG ${ANTLR_TAG}
    SOURCE_SUBDIR "runtime/Cpp"
)
FetchContent_MakeAvailable(antlr) # populates antlr_SOURCE_DIR
include(${antlr_SOURCE_DIR}/runtime/Cpp/cmake/FindANTLR.cmake) # populates ANTLR_TARGET

antlr_target(HintBlockGrammar HintBlock.g4 BOTH LISTENER PACKAGE pg_lab)

#
# Project config
#

if(APPLE)
    set_target_properties(pg_lab PROPERTIES
        LINK_FLAGS "-Wl,-undefined,dynamic_lookup -Wl,-flat_namespace"
    )
endif()

add_library(pg_lab MODULE
    src/pg_lab.cc
    src/hints.cc
    src/hint_parser.cc
    ${ANTLR_HintBlockGrammar_CXX_OUTPUTS}
)

target_include_directories(pg_lab PRIVATE
    "include"
    "${PG_INCLUDE_DIR}"
    "${PG_INCLUDE_DIR}/server"
    "${PG_INCLUDE_DIR}/postgresql/server"
    "${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src"
    "${ANTLR_HintBlockGrammar_OUTPUT_DIR}"
)

target_link_libraries(pg_lab PRIVATE antlr4_static)

add_custom_command(TARGET pg_lab POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:pg_lab>
        "${PG_LIB_DIR}/postgresql/pg_lab.$<IF:$<PLATFORM_ID:Darwin>,dylib,so>"
    COMMENT "Copying pg_lab to PostgreSQL server extension directory at ${PG_LIB_DIR}/postgresql/"
)
