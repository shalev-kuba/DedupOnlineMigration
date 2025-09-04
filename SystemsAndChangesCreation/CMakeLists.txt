cmake_minimum_required(VERSION 3.20)
project(UBC)

set(CMAKE_CXX_STANDARD 14)

add_executable(trace_parser
        TraceParser/TraceParser.cpp
        TraceParser/TraceFile.cpp
        TraceParser/TraceFile.hpp
        Common/Utility.cpp
        Common/Utility.hpp
        Common/Structs.hpp
        TraceParser/Config.hpp
        TraceParser/Config.cpp)

add_executable(union_fs_files
        UnionFSFiles/UnionFsFiles.cpp
        UnionFSFiles/FsFile.cpp
        UnionFSFiles/FSFile.hpp
        Common/Utility.cpp
        Common/Utility.hpp
        Common/Structs.hpp
        Common/FileLinesIterator.cpp
        Common/FileLinesIterator.hpp
        UnionFSFiles/FSUnioner.cpp
        UnionFSFiles/FSUnioner.hpp
        Common/Config.cpp
        Common/Config.hpp)

add_executable(create_system
        Common/FileLinesIterator.cpp
        Common/FileLinesIterator.hpp
        SystemCreation/SystemCreation.cpp
        SystemCreation/VolumeFile.cpp
        SystemCreation/VolumeFile.hpp
        Common/Config.cpp
        Common/Utility.cpp
        Common/Utility.hpp
        Common/Config.hpp
        Common/FileLinesIterator.cpp
        )

add_executable(summary_system
        Common/FileLinesIterator.cpp
        Common/FileLinesIterator.hpp
        SystemCreation/SystemCreationSummary.cpp
        SystemCreation/VolumeFile.cpp
        SystemCreation/VolumeFile.hpp
        Common/Config.cpp
        Common/Utility.cpp
        Common/Utility.hpp
        Common/Config.hpp
        Common/FileLinesIterator.cpp
)