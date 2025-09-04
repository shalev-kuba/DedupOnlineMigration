#pragma once

#include <map>
#include <vector>
#include <string>

class CommandLineParser final
{
public:
    static constexpr int VARIABLE_NUM_OF_OCCURRENCES = -1;
    enum class ArgumentType{
        INT=0,
        STRING,
        DOUBLE,
        BOOL
    };

    struct ConstraintTuple{
        ConstraintTuple(std::string tag, const ArgumentType type, const int occurrences, const bool optional,
                        std::string description):
                        tag(std::move(tag)),
                        type(type),
                        occurrences(occurrences),
                        is_optional(optional),
                        description(std::move(description))
                        {
                        }

        const std::string tag;
        const std::string description;
        const ArgumentType type;
        const int occurrences;
        const bool is_optional;
    };

    using TagValuesMap = std::map<std::string, std::vector<std::string>>;

public:
    /**
     * parses argv
     * @param argc - program's argc
     * @param argv - program's argv
     */
    explicit CommandLineParser(int argc, char **argv);

    /**
     * adds a constraint for command line
     * @param tag - in form of '-' and something, such as -tag
     * @param type - INT/STRING/DOUBLE/BOOL
     * @param occurrences - number of occurrences after the tag (-1 for any size)
     * @param optional - is this tag optional or no
     * @param description - string describes the param, default is empty string
     */
    void addConstraint(const std::string& tag, const ArgumentType type, const int occurrences = 0,
                       const bool optional = true, const std::string & description="");

    /**
     * prints the USAGE and description
     */
    void printUsageAndDescription() const;

    /**
     * prints params description
     */
    void printDescription() const;

    /**
     * enforce those constrains
     * @throw std::invalid_argument if some constraint is not valid
     */
    void validateConstraintsHold() const;

    /**
     *
     * @param tag - command line tag
     * @return the corresponding arguments
     */
    const std::vector<std::string>& getTag(const std::string &tag) const;

    /**
     *
     * @param tag - command line tag
     * @return whether the tag exists
     */
    bool isTagExist(const std::string& tag) const;

private:
    /**
     * @param constraint - a given constraint
     * @throw std::invalid_argument if constraint is not valid
     */
    void validateConstraintHolds(const ConstraintTuple& constraint) const;

private:
    /**
     * @param constraint - a given constraint
     * @param values - a params list given for the constraint
     * @throw std::invalid_argument if the params list is not valid for the constraint
     */
    static void validateConstraintValueType(const ConstraintTuple& constraint, const std::vector<std::string>& values);

    /**
     * @param arg_type - a ArgumentType enum value
     * @return string represent the enum value
     */
    static std::string ArgumentTypeToString(const ArgumentType arg_type);

    /**
     * @param s - a string
     * @return whether the string represents a number
     */
    static bool isNumber(const std::string& s);

private:
    TagValuesMap m_args;
    std::vector<ConstraintTuple> m_constraints;
};
