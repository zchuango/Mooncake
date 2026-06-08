# Custom Findspdlog for submodule - creates INTERFACE library with correct includes
set(SPDLOG_SUBMODULE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/extern/spdlog)
set(SPDLOG_SUBMODULE_INCLUDE ${SPDLOG_SUBMODULE_ROOT}/include)

if(EXISTS ${SPDLOG_SUBMODULE_INCLUDE}/spdlog/spdlog.h)
    message(STATUS "Using spdlog from submodule: ${SPDLOG_SUBMODULE_ROOT}")
    set(SPDLOG_FOUND TRUE)
    set(spdlog_FOUND TRUE)
    
    # Create an INTERFACE library that other targets can link against
    # This ensures correct include order
    if(NOT TARGET spdlog_submodule)
        add_library(spdlog_submodule INTERFACE)
        target_include_directories(spdlog_submodule INTERFACE 
            ${SPDLOG_SUBMODULE_INCLUDE}
            ${SPDLOG_SUBMODULE_INCLUDE}/spdlog/fmt/bundled)
    endif()
    
    set(SPDLOG_TARGET spdlog_submodule)
    set(spdlog_TARGET spdlog_submodule)
else()
    message(FATAL_ERROR "spdlog submodule not found at ${SPDLOG_SUBMODULE_ROOT}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(spdlog DEFAULT_MSG spdlog_TARGET)
