#pragma once

#include <set>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>

namespace Utils {
    /**
     * flattens a set of ints to a string seperated by spaces
     * @param s - int ordered set
     * @return - the resulted string
     */
    std::string getSetString(const std::set<int> &s);

    /**
     * converts size_t to string
     * @param x - size_t value
     * @return corresponding string
     */
    std::string toString(const size_t x);
};
