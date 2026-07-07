# FindUbDiag.cmake Resolves UbDiag::ubdiag_lib with a three-layer fallback: 1.
# Submodule (extern/ubdiag) — add_subdirectory if present 2. System package —
# find_package(UbDiag QUIET) 3. Mock header  — mooncake-common/ubdiag-mock/
# (no-op PerfPoint)
#
# Usage: include(${CMAKE_SOURCE_DIR}/mooncake-common/FindUbDiag.cmake)
# target_link_libraries(your_target PRIVATE UbDiag::ubdiag_lib)

if(TARGET UbDiag::ubdiag_lib)
  return()
endif()

option(MOONCAKE_UBDIAG_BUILD_CLI
  "Build the vendored UbDiag CLI when extern/ubdiag is used" ON)
option(MOONCAKE_UBDIAG_L1_SHARED
  "Build vendored UbDiag as libubdiag.so so Mooncake and the CLI use one SDK" ON)
option(MOONCAKE_UBDIAG_ENABLE_PERCENTILE
  "Enable vendored UbDiag P99/P999/P9999 percentile calculation for Mooncake PerfPoint" ON)
option(MOONCAKE_UBDIAG_ENABLE_PERFLOG
  "Enable vendored UbDiag PerfLog timestamp logging for Mooncake PerfPoint" ON)
option(MOONCAKE_UBDIAG_PERFPOINT_ONLY
  "Disable vendored UbDiag OB/MemPoint/CachePoint extensions; keep Mooncake PerfPoint/P99/PerfLog/CSV" ON)
option(MOONCAKE_UBDIAG_DISABLE_SYSTEM
  "Skip Layer 2 system-package lookup, used only for forced mock verification" OFF)

# Layer 1: Submodule (same pattern as extern/pybind11). UbDiag's generic
# BUILD_TESTS / BUILD_EXAMPLES default to ON and collide with Mooncake option
# names, so temporarily narrow them only while adding the submodule.
if(EXISTS "${CMAKE_SOURCE_DIR}/extern/ubdiag/CMakeLists.txt")
  set(_MOONCAKE_UBDIAG_SAVED_BUILD_EXAMPLES "${BUILD_EXAMPLES}")
  set(_MOONCAKE_UBDIAG_HAD_BUILD_EXAMPLES_CACHE FALSE)
  if(DEFINED CACHE{BUILD_EXAMPLES})
    set(_MOONCAKE_UBDIAG_HAD_BUILD_EXAMPLES_CACHE TRUE)
    get_property(_MOONCAKE_UBDIAG_BUILD_EXAMPLES_HELP CACHE BUILD_EXAMPLES PROPERTY HELPSTRING)
  endif()
  set(_MOONCAKE_UBDIAG_SAVED_BUILD_TESTS "${BUILD_TESTS}")
  set(_MOONCAKE_UBDIAG_HAD_BUILD_TESTS_CACHE FALSE)
  if(DEFINED CACHE{BUILD_TESTS})
    set(_MOONCAKE_UBDIAG_HAD_BUILD_TESTS_CACHE TRUE)
    get_property(_MOONCAKE_UBDIAG_BUILD_TESTS_HELP CACHE BUILD_TESTS PROPERTY HELPSTRING)
  endif()
  set(BUILD_EXAMPLES OFF)
  set(BUILD_EXAMPLES OFF CACHE BOOL "Disable UbDiag examples when vendored by Mooncake" FORCE)
  set(BUILD_TESTS OFF)
  set(BUILD_TESTS OFF CACHE BOOL "Disable UbDiag tests when vendored by Mooncake" FORCE)
  if(MOONCAKE_UBDIAG_L1_SHARED)
    set(UBDIAG_BUILD_SHARED ON CACHE BOOL "Build vendored UbDiag as a shared library" FORCE)
  endif()
  if(MOONCAKE_UBDIAG_ENABLE_PERCENTILE)
    set(ENABLE_PERCENTILE ON CACHE BOOL "Enable vendored UbDiag percentile calculation" FORCE)
  endif()
  if(MOONCAKE_UBDIAG_ENABLE_PERFLOG)
    set(ENABLE_PERFLOG ON CACHE BOOL "Enable vendored UbDiag PerfLog support" FORCE)
  endif()
  if(MOONCAKE_UBDIAG_PERFPOINT_ONLY)
    set(ENABLE_OB_MEMORY OFF CACHE BOOL "Disable vendored UbDiag eBPF memory observation" FORCE)
    set(ENABLE_OB_CACHE OFF CACHE BOOL "Disable vendored UbDiag cache observation" FORCE)
    set(ENABLE_MEMPOINT OFF CACHE BOOL "Disable vendored UbDiag MemPoint observation" FORCE)
    set(UBDIAG_ENABLE_CACHEPOINT OFF CACHE BOOL "Disable vendored UbDiag CachePoint observation" FORCE)
  endif()

  add_subdirectory(${CMAKE_SOURCE_DIR}/extern/ubdiag
                   ${CMAKE_BINARY_DIR}/extern/ubdiag_build EXCLUDE_FROM_ALL)

  if(_MOONCAKE_UBDIAG_HAD_BUILD_EXAMPLES_CACHE)
    set(BUILD_EXAMPLES "${_MOONCAKE_UBDIAG_SAVED_BUILD_EXAMPLES}"
        CACHE BOOL "${_MOONCAKE_UBDIAG_BUILD_EXAMPLES_HELP}" FORCE)
  else()
    unset(BUILD_EXAMPLES CACHE)
  endif()
  set(BUILD_EXAMPLES "${_MOONCAKE_UBDIAG_SAVED_BUILD_EXAMPLES}")
  if(_MOONCAKE_UBDIAG_HAD_BUILD_TESTS_CACHE)
    set(BUILD_TESTS "${_MOONCAKE_UBDIAG_SAVED_BUILD_TESTS}"
        CACHE BOOL "${_MOONCAKE_UBDIAG_BUILD_TESTS_HELP}" FORCE)
  else()
    unset(BUILD_TESTS CACHE)
  endif()
  set(BUILD_TESTS "${_MOONCAKE_UBDIAG_SAVED_BUILD_TESTS}")

  if(TARGET ubdiag_lib)
    set(_MOONCAKE_UBDIAG_SOURCE_DIR "${CMAKE_SOURCE_DIR}/extern/ubdiag")
    # ubdiag's CMake uses CMAKE_SOURCE_DIR instead of CMAKE_CURRENT_SOURCE_DIR
    # for its include paths. When consumed via add_subdirectory from Mooncake,
    # CMAKE_SOURCE_DIR points to Mooncake's root, not ubdiag's. Fix it here.
    target_include_directories(ubdiag_lib PUBLIC
      $<BUILD_INTERFACE:${_MOONCAKE_UBDIAG_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>)

    foreach(_MOONCAKE_UBDIAG_LIB_TARGET
            ubdiag_manager_lib ubdiag_runtime_lib ubdiag_bpf_loader)
      if(TARGET ${_MOONCAKE_UBDIAG_LIB_TARGET})
        target_include_directories(${_MOONCAKE_UBDIAG_LIB_TARGET} PUBLIC
          $<BUILD_INTERFACE:${_MOONCAKE_UBDIAG_SOURCE_DIR}/include>
          $<BUILD_INTERFACE:${_MOONCAKE_UBDIAG_SOURCE_DIR}/src>)
      endif()
    endforeach()

    if(TARGET ubdiag)
      target_include_directories(ubdiag PRIVATE
        ${_MOONCAKE_UBDIAG_SOURCE_DIR}/include
        ${_MOONCAKE_UBDIAG_SOURCE_DIR}/src
        ${_MOONCAKE_UBDIAG_SOURCE_DIR}/src/cli)
      if(MOONCAKE_UBDIAG_BUILD_CLI AND NOT TARGET mooncake_ubdiag_cli)
        add_custom_target(mooncake_ubdiag_cli ALL DEPENDS ubdiag)
      endif()
    elseif(MOONCAKE_UBDIAG_BUILD_CLI)
      message(WARNING "UbDiag: extern/ubdiag does not define the ubdiag CLI target")
    endif()

    add_library(UbDiag::ubdiag_lib ALIAS ubdiag_lib)
    message(STATUS "UbDiag: using submodule (extern/ubdiag, CLI=${MOONCAKE_UBDIAG_BUILD_CLI})")
    return()
  endif()
endif()

# Layer 2: System package — only search standard system library paths
# Using NO_DEFAULT_PATH + explicit PATHS to prevent cmake from recursively
# searching CMAKE_SYSTEM_PREFIX_PATH subdirectories (e.g., UbDiag_bak, build artifacts)
if(NOT MOONCAKE_UBDIAG_DISABLE_SYSTEM)
  find_package(UbDiag QUIET
      NO_DEFAULT_PATH
      PATHS /usr/lib64/cmake /usr/local/lib64/cmake)
  if(TARGET UbDiag::ubdiag_lib)
    message(STATUS "UbDiag: using system package")
    return()
  endif()
endif()

# Layer 3: Mock fallback (no-op PerfPoint, guarantees compilation)
add_library(ubdiag_mock INTERFACE)
target_include_directories(
  ubdiag_mock INTERFACE ${CMAKE_SOURCE_DIR}/mooncake-common/ubdiag-mock)
add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)
message(STATUS "UbDiag: using mock (no-op PerfPoint)")
