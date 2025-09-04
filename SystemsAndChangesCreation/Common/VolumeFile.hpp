#pragma once
#include "Structs.hpp"

#include <string>
#include <unordered_map>
#include <fstream>
#include <memory>

class VolumeFile final{
public:
    explicit VolumeFile(const std::string& file_path,
                        std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>>& global_fp_to_info,
                        uint64_t global_file_sn);

    ~VolumeFile();

public:
    std::shared_ptr<FSSummary> get_fs_summary();

private:
    static std::string get_header_value(const std::string& raw_header);
    static FSMetaData consume_header(std::ifstream& input_stream);

private:
    void load_blocks();
    void parse_file();

private:
    std::ifstream _input_stream;
    FSMetaData _fs_headers;
    std::unordered_map<uint64_t,std::string> _block_local_sn_to_fp;
    std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>> _fp_to_block_info;
    std::shared_ptr<std::vector<uint64_t>> _fs_global_blocks_sn_order;
    uint64_t _global_file_sn;

public:
    VolumeFile(VolumeFile&) = delete;
    VolumeFile(VolumeFile&&) = delete;
    VolumeFile& operator=(VolumeFile&) = delete;
    VolumeFile& operator=(VolumeFile&&) = delete;
};
