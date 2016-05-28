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
    bool isValidNamedPlaceholder(std::size_t pos) const;
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

    PlanCache() = default;

    PlanCache(PlanCache const &) = delete;
    PlanCache & operator = (PlanCache const &) = delete;

    CachedPlan const * lookupPlan(std::string const & query) const;
    CachedPlan const * assignPlan(std::string const & query);

private:
    static constexpr char * PlanNamePrefix = "EnigmaPlan_";
    const unsigned DefaultPlanCacheSize = 30;

    unsigned planCacheSize_{ DefaultPlanCacheSize };
    unsigned nextPlanId_{0};
    std::unordered_map<std::string, p_CachedPlan> plans_;

    CachedPlan const * storePlan(std::string const & query, std::string const & statementName);
    std::string generatePlanName();
};

typedef std::unique_ptr<PlanCache> p_PlanCache;


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
    // Queries waiting for execution
    std::queue<QueryAwait *> queue_;
    std::vector<unsigned> idleConnections_;
    std::unordered_map<unsigned, sp_Connection> connectionMap_;
    std::unordered_map<unsigned, p_PlanCache> planCaches_;
    // Statements we're currently preparing
    std::unordered_map<unsigned, QueryAwait *> preparing_;
    // Queries to execute after the statement was prepared
    std::unordered_map<unsigned, p_Query> pendingPrepare_;

    Mutex mutex_;

    unsigned assignConnectionId();
    void addConnection(Array const & options);
    void removeConnection(unsigned connectionId);
    void executeNext();
    void queryCompleted(unsigned connectionId);
};

typedef std::shared_ptr<Pool> sp_Pool;

class PoolInterface {
public:
    static Object newInstance(sp_Pool p);

    ~PoolInterface();

    sp_Pool pool;

private:
    void init(sp_Pool p);
    void sweep();
};

class QueryInterface {
public:
    void init(String const & command, Array const & params);

    inline String const & command() {
        return command_;
    }

    inline Array const & params() {
        return params_;
    }

    inline void setFlags(unsigned flags) {
        flags_ = flags;
    }

    inline unsigned flags() const {
        return flags_;
    }

private:
    String command_;
    Array params_;
    unsigned flags_{0};
};

void registerQueueClasses();

}
}

#endif //HPHP_ENIGMA_QUEUE_H
