cmake_minimum_required(VERSION 3.0.0)
project (executorapp)

find_package(Boost COMPONENTS thread REQUIRED)
include_directories("${Boost_INCLUDE_DIR}")

set(CMAKE_CXX_STANDARD 20)

aux_source_directory(. SRC_LIST)

#add_library(${PROJECT_NAME} SHARED ${SRC_LIST})
add_executable(${PROJECT_NAME} ${SRC_LIST})
target_link_libraries(
    ${PROJECT_NAME}
    Boost::thread
  )