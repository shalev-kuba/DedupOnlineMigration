#pragma once
#include "../Common/Structs.hpp"
#include "FSFile.hpp"

#include <string>
#include <fstream>
#include <memory>
#include <unordered_map>

class FSUnioner final {
public:
    explicit FSUnioner(const std::string& , const bool is_filter_union);
    ~FSUnioner();

public:
    void add_fs(const std::string& fs_path);
    void print_to_file();

private:
    void summary_results();
    void print_header_to_file();
    void print_blocks_to_file();
    void print_files_to_file();

private:
    const bool _is_filter_union;
    std::ofstream _output_stream;
    uint64_t _global_file_sn;
    std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>> _global_fp_to_info;
    std::shared_ptr<std::unordered_map<uint64_t , std::string>> _global_block_sn_to_fp;
    std::vector<std::shared_ptr<FSSummary>> _fs_summaries;
    UnionFilesMetadata _metadata;

public:
    FSUnioner(FSUnioner&) = delete;
    FSUnioner(FSUnioner&&) = delete;
    FSUnioner& operator=(FSUnioner&) = delete;
    FSUnioner& operator=(FSUnioner&&) = delete;
};

