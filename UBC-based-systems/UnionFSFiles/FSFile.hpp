#pragma once
#include "../Common/Structs.hpp"

#include <string>
#include <unordered_map>
#include <fstream>
#include <memory>

class FSFile final{
public:
    explicit FSFile(const std::string& file_path,
                    std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>>& global_fp_to_info,
                    uint64_t global_file_sn,
                    bool filter_blocks = false);

    ~FSFile();

public:
    std::shared_ptr<FSSummary> get_fs_summary();
//    void print_to_file(const std::string& out_path);

private:
    static std::string get_header_value(const std::string& raw_header);
    static FSMetaData consume_header(std::ifstream& input_stream);
    static bool is_block_filtered(const std::string& fp);

private:
    void load_blocks();
    void parse_file();

private:
    const bool _filter_blocks;
    std::ifstream _input_stream;
    FSMetaData _fs_headers;
    std::unordered_map<uint64_t,std::string> _block_local_sn_to_fp;
    std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>> _fp_to_block_info;
    std::shared_ptr<std::vector<uint64_t>> _fs_global_blocks_sn_order;
    std::vector<std::string> _raw_header_lines;
    uint64_t _global_file_sn;

public:
    FSFile(FSFile&) = delete;
    FSFile(FSFile&&) = delete;
    FSFile& operator=(FSFile&) = delete;
    FSFile& operator=(FSFile&&) = delete;
};
