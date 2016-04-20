#ifndef HPHP_ENIGMA_QUEUE_H
#define HPHP_ENIGMA_QUEUE_H

#include "hphp/runtime/ext/extension.h"
#include "enigma-common.h"
#include "enigma-query.h"

namespace HPHP {
namespace Enigma {


struct PlanInfo {
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

    PlanInfo(std::string const & cmd);

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

class PlanCache {
public:
    struct CachedPlan {
        CachedPlan(std::string const & cmd);

        std::string statementName;
        PlanInfo planInfo;
    };

    typedef std::unique_ptr<CachedPlan> p_CachedPlan;

    const unsigned DefaultPlanCacheSize = 30;

    PlanCache();
    ~PlanCache();

    PlanCache(PlanCache const &) = delete;
    PlanCache & operator = (PlanCache const &) = delete;

    CachedPlan const * lookupPlan(std::string const & query) const;
    void storePlan(std::string const & query, std::string const & statementName);

private:
    unsigned planCacheSize_{ DefaultPlanCacheSize };
    std::unordered_map<std::string, p_CachedPlan> plans_;
};


class Pool {
public:
    const unsigned DefaultQueueSize = 50;
    const unsigned DefaultPoolSize = 1;

    Pool(Array const & options);
    ~Pool();

    Pool(Pool const &) = delete;
    Pool & operator = (Pool const &) = delete;

    QueryAwait * enqueue(p_Query query);

private:
    unsigned maxQueueSize_{ DefaultQueueSize };
    // Number of connections we'll keep alive (even if they're idle)
    unsigned poolSize_{ DefaultPoolSize };
    unsigned nextConnectionIndex_{ 0 };
    std::queue<QueryAwait *> queue_;
    std::vector<unsigned> idleConnections_;
    std::unordered_map<unsigned, sp_Connection> connectionMap_;


    unsigned assignConnectionId();
    void addConnection(Array const & options);
    void executeNext();
    void queryCompleted(unsigned connectionId);
};


class PoolInterface {
public:
    static Object newInstance(Pool * p);

    Pool * pool;
};

void registerQueueClasses();

}
}

#endif //HPHP_ENIGMA_QUEUE_H
