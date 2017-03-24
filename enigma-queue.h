#ifndef HPHP_ENIGMA_QUEUE_H
#define HPHP_ENIGMA_QUEUE_H

#include "hphp/runtime/ext/extension.h"
#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/EvictingCacheMap.h>
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
    static const unsigned DefaultPlanCacheSize = 30;
    static const unsigned MaxPlanCacheSize = 1000;

    PlanCache(unsigned size = DefaultPlanCacheSize);

    PlanCache(PlanCache const &) = delete;
    PlanCache & operator = (PlanCache const &) = delete;

    CachedPlan const * lookupPlan(std::string const & query);
    CachedPlan const * assignPlan(std::string const & query);

private:
    static constexpr char const * PlanNamePrefix = "EnigmaPlan_";

    unsigned nextPlanId_{0};
    folly::EvictingCacheMap<std::string, p_CachedPlan> plans_;

    CachedPlan const * storePlan(std::string const & query, std::string const & statementName);
    std::string generatePlanName();
};

typedef std::unique_ptr<PlanCache> p_PlanCache;

typedef unsigned ConnectionId;

class PoolHandle;

class AssignmentManager {
public:
    virtual ~AssignmentManager();

    // Tries to enqueue a query. Returns true if queuing was handled by the assignment
    // manager, false otherwise.
    virtual bool enqueue(QueryAwait * event, PoolHandle * handle) = 0;
    // Assigns an idle connection to the requested pool handle
    virtual ConnectionId assignConnection(PoolHandle * handle) = 0;
    // Assigns an enqueued query to the requested connection
    virtual QueryAwait * assignQuery(ConnectionId cid) = 0;
    // Notifies the assignment manager that the specified connection is no longer assigned to a pool handle
    // Returns whether the connection can be managed by the connection pool
    virtual bool notifyFinishAssignment(PoolHandle * handle, ConnectionId cid) = 0;

    // Notifies the assignment manager that a pool handle was created to this pool
    virtual void notifyHandleCreated(PoolHandle * handle) = 0;
    // Notifies the assignment manager that a pool handle was destroyed for this pool
    virtual void notifyHandleReleased(PoolHandle * handle) = 0;

    // Indicates that a new connection was added to the pool
    virtual void notifyConnectionAdded(ConnectionId cid) = 0;
    // Indicates that a connection was removed from the pool
    virtual void notifyConnectionRemoved(ConnectionId cid) = 0;
};

typedef std::unique_ptr<AssignmentManager> p_AssignmentManager;

class Pool {
public:
    const static unsigned DefaultQueueSize = 50;
    const static unsigned MaxQueueSize = 1000;
    const static unsigned DefaultPoolSize = 1;
    const static unsigned MaxPoolSize = 100;
    const static ConnectionId InvalidConnectionId = std::numeric_limits<ConnectionId>::max();

    Pool(Array const & connectionOpts, Array const & poolOpts);
    ~Pool();

    Pool(Pool const &) = delete;
    Pool & operator = (Pool const &) = delete;

    QueryAwait * enqueue(p_Query query, PoolHandle * handle);
    void enqueue(QueryAwait * query, PoolHandle * handle);
    void execute(ConnectionId connectionId, QueryAwait * query, PoolHandle * handle);

    ConnectionId assignConnectionId(PoolHandle * handle);
    void releaseConnection(ConnectionId connectionId);
    sp_Connection connection(ConnectionId connectionId);

    void createHandle(PoolHandle * handle);
    void releaseHandle(PoolHandle * handle);

private:
    struct QueueItem {
        QueryAwait * query;
        PoolHandle * handle;
    };

    unsigned maxQueueSize_{ DefaultQueueSize };
    // Number of connections we'll keep alive (even if they're idle)
    unsigned poolSize_{ DefaultPoolSize };
    // Number of prepared statements to keep per connection
    unsigned planCacheSize_{ PlanCache::DefaultPlanCacheSize };
    ConnectionId nextConnectionIndex_{ 0 };
    // Queries waiting for execution
    folly::MPMCQueue<QueueItem> queue_;
    folly::MPMCQueue<ConnectionId> idleConnections_;
    std::unordered_map<ConnectionId, sp_Connection> connectionMap_;
    std::unordered_map<ConnectionId, p_PlanCache> planCaches_;
    // Statements we're currently preparing
    std::unordered_map<ConnectionId, QueueItem> preparing_;
    // Queries to execute after the statement was prepared
    std::unordered_map<ConnectionId, p_Query> pendingPrepare_;
    p_AssignmentManager transactionLifetimeManager_;
    // TODO: p_AssignmentManager assignmentManager_;

    void addConnection(Array const & options);
    void removeConnection(ConnectionId connectionId);
    void tryExecuteNext();
    void queryCompleted(ConnectionId connectionId, PoolHandle * handle);
};

typedef std::shared_ptr<Pool> sp_Pool;

class PersistentPoolStorage {
public:
    sp_Pool make(Array const & connectionOpts, Array const & poolOpts);
    sp_Pool add(Array const & connectionOpts, Array const & poolOpts);
    void remove(Array const & connectionOpts);

private:
    ReadWriteMutex lock_;
    std::unordered_map<std::string, sp_Pool> pools_;

    static std::string makeKey(Array const & connectionOpts);
};

struct TransactionState {
    static const unsigned MaxPendingQueries = 10;

    TransactionState();

    TransactionState(TransactionState const &) = delete;
    TransactionState & operator = (TransactionState const &) = delete;

    ConnectionId connectionId{Pool::InvalidConnectionId};
    bool executing {false};
    folly::ProducerConsumerQueue<QueryAwait *> pendingQueries;
};

class PoolHandle {
public:
    static Object newInstance(sp_Pool p);

    ~PoolHandle();

    void sweep();

    sp_Pool pool;
    std::atomic<unsigned> runningQueries {0};
    TransactionState transaction;

private:
    void init(sp_Pool p);
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
