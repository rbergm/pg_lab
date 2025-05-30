cmake_minimum_required(VERSION 3.19)
project(pg_lab
    VERSION 0.0.3
    DESCRIPTION ""
    LANGUAGES CXX)

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

set(PG_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../pg-build/pg-17" CACHE PATH "PostgreSQL server installation directory")
message(STATUS "Using PostgreSQL server path: ${PG_INSTALL_DIR}")

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

add_library(pg_lab MODULE
    src/pg_lab.cc
    src/hints.cc
    src/hint_parser.cc
    src/path_gen.cc
    ${ANTLR_HintBlockGrammar_CXX_OUTPUTS})

if(APPLE)
    set_target_properties(pg_lab PROPERTIES
        LINK_FLAGS "-Wl,-undefined,dynamic_lookup -Wl,-flat_namespace"
    )
endif()

target_link_libraries(pg_lab PRIVATE antlr4_static)

target_include_directories(pg_lab PRIVATE include)
target_include_directories(pg_lab PRIVATE
    "${PG_INSTALL_DIR}/include/postgresql/server")
target_include_directories(pg_lab PRIVATE
    "${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src"
    "${ANTLR_HintBlockGrammar_OUTPUT_DIR}")

add_custom_command(TARGET pg_lab POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:pg_lab>
        "${PG_INSTALL_DIR}/lib/postgresql/pg_lab.$<IF:$<PLATFORM_ID:Darwin>,dylib,so>"
    COMMENT "Copying pg_lab to PostgreSQL server extension directory at ${PG_INSTALL_DIR}/lib/postgresql/"
)
