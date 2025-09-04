cmake_minimum_required(VERSION 3.10)  # CMake version check
project(hc)               # Create project "simple_example"
set(CMAKE_CXX_STANDARD 14)            # Enable c++14 standard

# Add main.cpp file of project root directory as source file
set(HC_SOURCE_FILES
        Main.cpp
        Shared/AlgorithmDSManager.cpp
        HierarchicalClustering.cpp
        Node.cpp Shared/Utility.cpp
        Shared/CommandLineParser.cpp
        Shared/GreedySplit.cpp
        Shared/GreedySplit.hpp
        Calculator/Calculator.cpp
        Calculator/VolumeCalcInfo.cpp
        Calculator/Utils.cpp
        Calculator/Lock.cpp
        Calculator/Cache.cpp
        Shared/MigrationPlan.cpp
        Shared/MigrationPlan.hpp
        )

include_directories(hc Calculator Shared)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(hc ${HC_SOURCE_FILES})
TARGET_LINK_LIBRARIES(hc sqlite3)
