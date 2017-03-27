#ifndef HPHP_ENIGMA_QUEUE_H
#define HPHP_ENIGMA_QUEUE_H

#include "hphp/runtime/ext/extension.h"
#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/EvictingCacheMap.h>
#include "enigma-common.h"
#include "enigma-query.h"
#include "enigma-async.h"
#include "enigma-plan.h"

namespace HPHP {
namespace Enigma {

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

class PoolConnectionHandle {
public:
    PoolConnectionHandle(sp_Pool pool);
    PoolConnectionHandle(PoolConnectionHandle const &) = delete;
    PoolConnectionHandle & operator = (PoolConnectionHandle const &) = delete;
    ~PoolConnectionHandle();

    sp_Connection getConnection() const;

private:
    sp_Pool pool_;
    unsigned connectionId_;
};

class PoolHandle {
public:
    PoolHandle(sp_Pool pool);
    PoolHandle(PoolHandle const &) = delete;
    PoolHandle & operator = (PoolHandle const &) = delete;
    ~PoolHandle();

    Pgsql::p_ResultResource query(String const & command, Array const & params, unsigned flags);
    QueryAwait * asyncQuery(String const & command, Array const & params, unsigned flags);

    inline sp_Pool pool() const {
        return pool_;
    }

    inline TransactionState & transaction() {
        return transaction_;
    }

private:
    sp_Pool pool_;
    TransactionState transaction_;
};

class HHPoolHandle {
public:
    static Object newInstance(sp_Pool p);
    ~HHPoolHandle();
    void sweep();

    std::unique_ptr<PoolHandle> handle;

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
