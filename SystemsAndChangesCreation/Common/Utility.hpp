#pragma once

#include <string>
#include <vector>

namespace Utility {
    void createDir(const std::string &path);
    std::vector<std::string> split_str(std::string str, const std::string& delimiter, const int64_t expected_size=-1);
    std::string get_comma_token(std::ifstream& input_stream, const bool skip_space= true);
    std::string get_file_name(const std::string& file_path);
    std::ifstream get_input_file_stream(const std::string& file_path);
    std::ofstream get_output_file_stream(const std::string& file_path);
    std::string get_trace_header_line(std::ifstream & file_stream);
    std::string get_line(std::ifstream & file_stream, const bool throw_on_eof= true);
    std::string get_date(const uint64_t timestamp);
};
