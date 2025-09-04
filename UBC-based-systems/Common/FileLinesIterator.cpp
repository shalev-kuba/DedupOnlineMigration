#include "FileLinesIterator.hpp"
#include "Utility.hpp"

FileLinesIterator::FileLinesIterator(const std::string &path) :
_input_stream(Utility::get_input_file_stream(path)),
_current_line(Utility::get_line(_input_stream, false))
{
}

std::string FileLinesIterator::get_current_line() const {
    if(is_done())
    {
        throw std::runtime_error("FileLinesIterator::get_current_line called after is_done()=true");
    }

    return _current_line;
}

void FileLinesIterator::next() {
    if(is_done())
    {
        throw std::runtime_error("FileLinesIterator::next called after is_done()=true");
    }

    static constexpr bool DONT_THROW_ON_EOF = false;
    _current_line = Utility::get_line(_input_stream, DONT_THROW_ON_EOF);
}

bool FileLinesIterator::is_done() const {
    return _current_line.empty();
}

FileLinesIterator::~FileLinesIterator() {
    try{
        _input_stream.close();
    }
    catch(...)
    {
    }
}



