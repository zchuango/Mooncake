file(GLOB UB_SOURCES "*.cpp")

if (URMA_MOCK)
    list(APPEND UB_SOURCES "urma/mock_urma_api.cpp")
    add_library(ub_transport OBJECT ${UB_SOURCES})
else ()
    add_library(ub_transport OBJECT ${UB_SOURCES})
    target_link_libraries(ub_transport
            PUBLIC
            /usr/lib64/liburma.so
    )
endif ()
target_include_directories(ub_transport
        PUBLIC
        ${urma_INCLUDE_DIR}
)
target_link_libraries(ub_transport
        PRIVATE
        JsonCpp::JsonCpp
        glog::glog
        pthread
)