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

# Layer 1: Submodule (same pattern as extern/pybind11) ubdiag's BUILD_TESTS /
# BUILD_EXAMPLES default to ON; EXCLUDE_FROM_ALL keeps them out of the default
# build so mooncake users don't compile ubdiag's test suite unless explicitly
# requested.
if(EXISTS "${CMAKE_SOURCE_DIR}/extern/ubdiag/CMakeLists.txt")
  add_subdirectory(${CMAKE_SOURCE_DIR}/extern/ubdiag
                   ${CMAKE_BINARY_DIR}/extern/ubdiag_build EXCLUDE_FROM_ALL)
  if(TARGET ubdiag_lib)
    # ubdiag's CMake uses CMAKE_SOURCE_DIR instead of CMAKE_CURRENT_SOURCE_DIR
    # for its include paths. When consumed via add_subdirectory from Mooncake,
    # CMAKE_SOURCE_DIR points to Mooncake's root, not ubdiag's. Fix it here.
    target_include_directories(ubdiag_lib PUBLIC
      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/extern/ubdiag/include>
      $<INSTALL_INTERFACE:include>)
    add_library(UbDiag::ubdiag_lib ALIAS ubdiag_lib)
    message(STATUS "UbDiag: using submodule (extern/ubdiag)")
    return()
  endif()
endif()

# Layer 2: System package
find_package(UbDiag QUIET)
if(TARGET UbDiag::ubdiag_lib)
  message(STATUS "UbDiag: using system package")
  return()
endif()

# Layer 3: Mock fallback (no-op PerfPoint, guarantees compilation)
add_library(ubdiag_mock INTERFACE)
target_include_directories(
  ubdiag_mock INTERFACE ${CMAKE_SOURCE_DIR}/mooncake-common/ubdiag-mock)
add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)
message(STATUS "UbDiag: using mock (no-op PerfPoint)")
