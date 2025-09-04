#pragma once
#include <set>
#include <vector>
#include <string>
#include <memory>

typedef struct{
    std::vector<std::string> raw_headers;
    std::string input_file;
    uint64_t username;
    uint64_t host_name;
    std::string snapshot_date;
    std::string windows_version;
    uint64_t logical_size;
    uint64_t num_logical_blocks;
    uint64_t num_physical_blocks;
    uint64_t num_logical_files;
    uint64_t num_files;
    uint64_t gen_level;
} FSMetaData;

typedef struct{
    std::set<std::string> input_files;
    std::set<uint64_t> users;
    std::set<uint64_t> hosts;
    uint64_t logical_size;
    uint64_t num_logical_blocks;
    uint64_t physical_size;
    uint64_t num_physical_blocks;
    uint64_t sum_of_separated_physical_blocks;
} UnionFilesMetadata;

typedef struct{
    uint64_t local_sn;
    int32_t size;
} LocalBlockInfo;

typedef struct{
    uint64_t global_sn;
    int32_t size;
    std::vector<uint64_t> files_sn;
} GlobalBlockInfo;

typedef struct{
    FSMetaData fs_metadata;
    uint64_t global_file_sn;
    std::shared_ptr<std::vector<uint64_t>> blocks_global_sn;
} FSSummary;
