#pragma once
#include <fstream>

class FileLinesIterator final {
public:
    explicit FileLinesIterator(const std::string& path);
    ~FileLinesIterator();

public:
    std::string get_current_line() const;
    bool is_done() const;
    void next();

private:
    std::ifstream _input_stream;
    std::string _current_line;

public:
    FileLinesIterator(FileLinesIterator&) = delete;
    FileLinesIterator(FileLinesIterator&&) = delete;
    FileLinesIterator& operator=(FileLinesIterator&&) = delete;
    FileLinesIterator& operator=(FileLinesIterator&) = delete;
};
