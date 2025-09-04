#include "Node.hpp"

Node::Node(const int file, const double size) :
    m_current_files({file}),
    m_is_activated(true),
    m_size(size),
    m_orig_size(size),
    m_orig_file(file)
{
}

void Node::disableNode() {
    m_is_activated = false;
}

bool Node::isActivated() const{
    return m_is_activated;
}

const std::set<int>& Node::getCurrentFiles() const {
    return m_current_files;
}

void Node::addFiles(const std::set<int> &new_files) {
    m_current_files.insert(new_files.cbegin(), new_files.cend());
}

void Node::setSize(const double new_size) {
    m_size = new_size;
}

double Node::getSize() const{
    return m_size;
}

void Node::reset() {
    m_current_files = {m_orig_file};
    m_size = m_orig_size;
    m_is_activated = true;
}
