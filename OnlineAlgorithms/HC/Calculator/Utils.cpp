#include "Utils.hpp"

#include <sstream>

std::string Utils::getSetString(const std::set<int> &s) {
    std::string output;
    int i=1;
    for (const int val : s) {
        output += std::to_string(val) + (i++==s.size()? "" : " ");
    }

    return std::move(output);
}

std::string Utils::toString(const size_t x) {
    std::stringstream ss;
    ss << x;
    return std::move(ss.str());
}
