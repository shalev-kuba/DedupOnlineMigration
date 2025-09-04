#pragma once
#include <functional>

using std::function;

class ScopeGuard final {
public:
    template< class Func >
    explicit ScopeGuard(const Func& cleanup_func):
        m_cleanup( cleanup_func ),
        m_cleaned(false)
    {
    }

    ~ScopeGuard() {
        try {
            if(!m_cleaned)
                m_cleanup();
        }catch (...){}
    }

    void disableCleanup() {m_cleaned=true;}

private:
    const function<void()> m_cleanup;
    bool m_cleaned;
};