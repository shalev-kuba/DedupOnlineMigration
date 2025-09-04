#include "CommandLineParser.hpp"

#include <iostream>
#include <algorithm>

CommandLineParser::CommandLineParser(int argc, char **argv){
    for (int i = 1; i < argc; ++i) {
        const std::string current_arg = argv[i];
        if (current_arg.front() == '-') {
            std::vector<std::string> current_tag_value;
            while (i < argc - 1 && argv[i+1][0] != '-')
                current_tag_value.emplace_back(argv[++i]);

            m_args.insert(make_pair(current_arg, current_tag_value));
        }
    }
}

void CommandLineParser::validateConstraintsHold() const{
    for (const ConstraintTuple& constraint : m_constraints)
        validateConstraintHolds(constraint);
}

void CommandLineParser::validateConstraintHolds(const ConstraintTuple& constraint) const{
    if (!constraint.is_optional && !isTagExist(constraint.tag))
        throw std::invalid_argument("ERROR: The param " + constraint.tag + " is missing");

    if(isTagExist(constraint.tag))
    {
        const std::vector<std::string>& tag_values = getTag(constraint.tag);
        if(tag_values.size() != constraint.occurrences && constraint.occurrences != VARIABLE_NUM_OF_OCCURRENCES)
            throw std::invalid_argument("ERROR: The param " + constraint.tag + " Need to receive " +
                std::to_string(constraint.occurrences) + " parameters");

        validateConstraintValueType(constraint, tag_values);
    }
}

const std::vector<std::string>& CommandLineParser::getTag(const std::string& option) const{
    const auto found = m_args.find(option);
    if (found != m_args.end())
        return found->second;

    throw std::invalid_argument("Tag " + option + " does not exist");
}

void CommandLineParser::validateConstraintValueType(const ConstraintTuple& constraint,
                                                    const std::vector<std::string>& values){
    switch (constraint.type) {
        case ArgumentType::INT:
            for (const auto& value : values){
                if (!isNumber(value))
                    throw std::invalid_argument("ERROR: Need to receive int for " + constraint.tag);
            }
            break;
        case ArgumentType::DOUBLE:
            for (const auto &value : values) {
                try {
                    stod(value);
                }
                catch (const std::exception &e){
                    throw std::invalid_argument("ERROR: Need to receive double for " + constraint.tag);
                }
            }
            break;

        default:
            break;
    }
}

bool CommandLineParser::isNumber(const std::string& s) {
    return !s.empty() && std::find_if(s.cbegin(),s.cend(),[](unsigned char c) { return !std::isdigit(c); }) == s.cend();
}

bool CommandLineParser::isTagExist(const std::string &option) const {
    return m_args.find(option) != m_args.cend();
}

void CommandLineParser::addConstraint(const std::string &tag, ArgumentType type, int occurrences, bool optional,
                                      const std::string & description) {
    m_constraints.emplace_back(tag, type, occurrences, optional, description);
}

void CommandLineParser::printUsageAndDescription() const{
    std::cout << "[USAGE]:"<<std::endl<<"./hc ";

    for (const auto& constraint : m_constraints) {
        std::string constraint_str = constraint.tag;
        if(constraint.type != ArgumentType::BOOL)
        {
            const std::string type_str = ArgumentTypeToString(constraint.type) +
                    (constraint.occurrences == CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES ?
                    " - VARIABLE LENGTH LIST" : "*" + std::to_string(constraint.occurrences));

            constraint_str +=  "(TYPE=" + type_str +")";
        }

        std::cout << (constraint.is_optional? "[" + constraint_str + "] " : constraint_str + " ");
    }

    std::cout <<std::endl << std::endl;

    printDescription();
}

std::string CommandLineParser::ArgumentTypeToString(const ArgumentType arg_type){
    switch (arg_type) {
        case ArgumentType::INT:
            return "INT";
        case ArgumentType::STRING:
            return "STRING";
        case ArgumentType::DOUBLE:
            return "DOUBLE";
        default:
            return "";
    }
}

void CommandLineParser::printDescription() const{
    std::cout <<"PARAMS DESCRIPTION:" << std::endl;

    for (const auto& constraint : m_constraints)
        std::cout<< "\t" << constraint.tag << ": " << constraint.description << std::endl;

    std::cout <<std::endl;
}
