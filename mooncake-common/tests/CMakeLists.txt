add_executable(logging_test logging_test.cpp)
target_link_libraries(logging_test PUBLIC
    mooncake_common
    gtest
    pthread
)
add_test(NAME logging_test COMMAND logging_test)

add_executable(default_config_test default_config_test.cpp)
target_link_libraries(default_config_test PUBLIC
    mooncake_common 
    gtest
    pthread PUBLIC
)
add_test(NAME default_config_test COMMAND default_config_test)
