#pragma once

#include "../Common/Structs.hpp"

#include <fstream>
#include <string>
#include <unordered_map>

class TraceFile final {
public:
    explicit TraceFile(const std::string& file_path);
    ~TraceFile();

private:
    static FSMetaData consume_headers(std::ifstream& input_stream, const std::string& file_path);
    static bool is_trace_eof(const std::string& line);
    static bool is_block_line(const std::string& line);

public:
    void print_to_file(const std::string& out_dir);

private:
    bool consume_file();
    void update_with_block(const std::string& block_line);
    void consume_file_blocks();
    void print_fs_header(std::ofstream& out_file) const;
    void print_blocks(std::ofstream& out_file);
    void print_fs_file(std::ofstream& out_file);
    std::string get_file_name() const;


private:
    std::ifstream _input_stream;
    FSMetaData _fs_headers;
    std::unordered_map<std::string,LocalBlockInfo> _blocks;

public:
    TraceFile(TraceFile&) = delete;
    TraceFile(TraceFile&&) = delete;
    TraceFile& operator=(TraceFile&) = delete;
    TraceFile& operator=(TraceFile&&) = delete;
};
