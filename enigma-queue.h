#ifndef HPHP_ENIGMA_QUEUE_H
#define HPHP_ENIGMA_QUEUE_H

#include "hphp/runtime/ext/extension.h"
#include "enigma-query.h"

namespace HPHP {


struct EnigmaPlanInfo {
    enum class ParameterType {
        /*
         * Query uses the numbered prepared parameter placeholder.
         * (eg. "where id = ?")
         */
        Numbered,
        /*
         * Query uses the named prepared parameter placeholder.
         * (eg. "where id = :id")
         */
        Named
    };

    EnigmaPlanInfo(std::string const & cmd);

    Array mapParameters(Array const & params);

    std::string command;
    std::string rewrittenCommand;
    ParameterType type;
    std::vector<std::string> parameterNameMap;
    unsigned parameterCount;

private:
    Array mapNamedParameters(Array const & params);
    Array mapNumberedParameters(Array const & params);

    void determineParameterType();
    std::tuple<std::string, unsigned> parseNumberedParameters() const;
    std::tuple<std::string, std::vector<std::string> > parseNamedParameters() const;
    bool isValidPlaceholder(std::size_t pos) const;
    std::size_t namedPlaceholderLength(std::size_t pos) const;
};

class EnigmaPlanCache {
public:
    struct CachedPlan {
        CachedPlan(std::string const & cmd);

        std::string statementName;
        EnigmaPlanInfo planInfo;
    };

    typedef std::unique_ptr<CachedPlan> p_CachedPlan;

    const unsigned DefaultPlanCacheSize = 30;

    EnigmaPlanCache();
    ~EnigmaPlanCache();

    EnigmaPlanCache(EnigmaPlanCache const &) = delete;
    EnigmaPlanCache & operator = (EnigmaPlanCache const &) = delete;

    CachedPlan const * lookupPlan(std::string const & query) const;
    void storePlan(std::string const & query, std::string const & statementName);

private:
    unsigned planCacheSize_{ DefaultPlanCacheSize };
    std::unordered_map<std::string, p_CachedPlan> plans_;
};


class EnigmaPool {
public:
    const unsigned DefaultQueueSize = 50;
    const unsigned DefaultPoolSize = 1;

    EnigmaPool(Array const & options);
    ~EnigmaPool();

    EnigmaPool(EnigmaPool const &) = delete;
    EnigmaPool & operator = (EnigmaPool const &) = delete;

    EnigmaQueryAwait * enqueue(p_EnigmaQuery query);

private:
    unsigned maxQueueSize_{ DefaultQueueSize };
    // Number of connections we'll keep alive (even if they're idle)
    unsigned poolSize_{ DefaultPoolSize };
    unsigned nextConnectionIndex_{ 0 };
    std::queue<EnigmaQueryAwait *> queue_;
    std::vector<unsigned> idleConnections_;
    std::unordered_map<unsigned, sp_EnigmaConnection> connectionMap_;


    unsigned assignConnectionId();
    void addConnection(Array const & options);
    void executeNext();
    void queryCompleted(unsigned connectionId);
};


class EnigmaPoolInterface {
public:
    static Object newInstance(EnigmaPool * p);

    EnigmaPool * pool;
};

void registerEnigmaQueueClasses();

}

#endif //HPHP_ENIGMA_QUEUE_H
