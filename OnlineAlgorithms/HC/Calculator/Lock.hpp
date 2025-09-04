#pragma once

#include <string>

class Lock final {
public:
    explicit Lock(const std::string& lock_path);
    ~Lock();

    bool lock();

    void unlock();

private:
    static int openFile(const std::string& lock_path);

private:
    const int m_fd;
    const std::string m_path;
    bool m_holds_lock;
};
