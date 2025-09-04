#include "Lock.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <unistd.h>

Lock::Lock(const std::string& lock_path):
m_path(lock_path), m_fd(openFile(lock_path)), m_holds_lock(false)
{
}

int Lock::openFile(const std::string& lock_path){
    const int fd = open(lock_path.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        throw std::runtime_error("Cannot open file lock");
    }

    return fd;
}

bool Lock::lock() {
    if (m_holds_lock) {
        throw std::runtime_error("lock is already been held by this process");
    }

    static constexpr int NUM_LOCK_TRIES=40;
    for(int num_try=0; num_try < NUM_LOCK_TRIES; ++num_try){
        if(flock(m_fd, LOCK_EX | LOCK_NB) == 0){
            m_holds_lock = true;
            break;
        }

        static const std::chrono::milliseconds SLEEP_BETWEEN_LOCK_TRY(500);
        std::this_thread::sleep_for(SLEEP_BETWEEN_LOCK_TRY);
    }

    return m_holds_lock;
}

void Lock::unlock() {
    if (!m_holds_lock) {
        throw std::runtime_error("lock is not held by this process");
    }

    if(flock(m_fd, LOCK_UN) < 0)
        throw std::runtime_error("Failed to release file lock");

    m_holds_lock = false;
}

Lock::~Lock() {
    try {
        if(m_holds_lock)
            unlock();
    }catch (...){
    }

    close(m_fd);
}
