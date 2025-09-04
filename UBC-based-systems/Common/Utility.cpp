#include "Utility.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept>
#include <fstream>
#include <ctime>
#include <sstream>
#include <iomanip>

void Utility::createDir(const std::string &path) {
    struct stat info{};

    if( stat( path.c_str(), &info ) == 0 && !S_ISDIR(info.st_mode))
        throw std::runtime_error("could not create dir " + path + ". a file is already exist there.");

    if(S_ISDIR(info.st_mode))
        return;

    if (mkdir(path.c_str(), 0777) != 0 && errno != EEXIST)
        throw std::runtime_error("could not create dir " + path);
}

std::vector<std::string> Utility::split_str(std::string str, const std::string &delimiter, const int64_t expected_size) {

    std::vector<std::string> res;
    if(expected_size > 0)
    {
        res.reserve(expected_size);
    }

    size_t pos = 0;
    std::string token;
    while ((pos = str.find(delimiter)) != std::string::npos) {
        res.emplace_back(str.substr(0, pos));
        str.erase(0, pos + delimiter.length());
    }

    if(str.length() > 0){
        res.emplace_back(str);
    }

    return res;
}

std::string Utility::get_comma_token(std::ifstream& input_stream, const bool skip_space){
    std::string res;
    if(!std::getline(input_stream, res, ','))
    {
        return "";
    }

    if(skip_space)
    {
        return res.substr(1); //skip space
    }

    return res;
}

std::string Utility::get_file_name(const std::string &file_path) {
    std::string file_name = file_path;
    static constexpr char DIR_SEPARATOR = '/';
    if(file_path.find_last_of(DIR_SEPARATOR) != std::string::npos)
    {
        file_name = file_path.substr(file_path.find_last_of(DIR_SEPARATOR)+1);
    }

    return file_name;
}

std::ifstream Utility::get_input_file_stream(const std::string &file_path) {
    std::ifstream file_stream(file_path);

    if(!file_stream.is_open()){
        throw std::runtime_error("Could not open the given trace file");
    }

    return file_stream;
}

std::ofstream Utility::get_output_file_stream(const std::string& file_path){
    std::ofstream file_stream(file_path);

    if(!file_stream.is_open()){
        throw std::runtime_error("Could not output open the given trace file");
    }

    return file_stream;
}

std::string Utility::get_trace_header_line(std::ifstream &file_stream) {
    std::string line = Utility::get_line(file_stream);

    if(line.empty())
        throw std::runtime_error("Unexpected empty line while parsing trace header");

    return line;
}

std::string Utility::get_line(std::ifstream &file_stream, const bool throw_on_eof) {
    std::string line;

    if(!std::getline(file_stream, line))
    {
        if(throw_on_eof){
            throw std::runtime_error("Unexpected EOF while parsing trace header");
        }

        return line;
    }

    static constexpr char CR_CHAR = '\r';
    if(!line.empty() && line[line.size()-1] == CR_CHAR)
    {
        line.erase(line.size()-1); // need dos2unix. this is a fix for removing '\r'
    }

    return line;
}

std::string Utility::get_date(const uint64_t timestamp) {
    auto datetime = static_cast<time_t>(timestamp);

    tm newtime = {};
    if(localtime_r(&datetime,&newtime) == NULL)
    {
        throw std::runtime_error("Could not convert timestamp to date");
    }

    static const char* const DATE_FORMAT = "%d_%m_%Y_%H_%M";
    std::ostringstream str_stream;
    str_stream << std::put_time(&newtime, DATE_FORMAT);

    return str_stream.str();
}
