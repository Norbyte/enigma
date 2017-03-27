#ifndef HPHP_ENIGMA_ASSIGNMENT_H
#define HPHP_ENIGMA_ASSIGNMENT_H

#include "hphp/runtime/ext/extension.h"
#include "enigma-common.h"
#include "enigma-query.h"
#include "enigma-async.h"
#include "enigma-queue.h"

namespace HPHP {
namespace Enigma {


class TransactionLifetimeManager : public AssignmentManager {
public:
    TransactionLifetimeManager();
    ~TransactionLifetimeManager();

    TransactionLifetimeManager(TransactionLifetimeManager const &) = delete;
    TransactionLifetimeManager & operator = (TransactionLifetimeManager const &) = delete;

    // Tries to enqueue a query. Returns true if queuing was handled by the assignment
    // manager, false otherwise.
    virtual bool enqueue(QueryAwait * event, PoolHandle * handle) override;
    // Assigns an idle connection to the requested pool handle
    virtual ConnectionId assignConnection(PoolHandle * handle) override;
    // Assigns an enqueued query to the requested connection
    virtual QueryAwait * assignQuery(ConnectionId cid) override;
    // Notifies the assignment manager that the specified connection is no longer assigned to a pool handle
    // Returns whether the connection can be managed by the connection pool
    virtual bool notifyFinishAssignment(PoolHandle * handle, ConnectionId cid) override;

    // Notifies the assignment manager that a pool handle was created to this pool
    virtual void notifyHandleCreated(PoolHandle * handle) override;
    // Notifies the assignment manager that a pool handle was destroyed for this pool
    virtual void notifyHandleReleased(PoolHandle * handle) override;

    // Indicates that a new connection was added to the pool
    virtual void notifyConnectionAdded(ConnectionId cid) override;
    // Indicates that a connection was removed from the pool
    virtual void notifyConnectionRemoved(ConnectionId cid) override;

private:
    struct ConnectionState {
        PoolHandle * handle {nullptr};
        bool rollingBack {false};
    };

    std::map<ConnectionId, ConnectionState> connections_;

    void beginTransaction(ConnectionId cid, PoolHandle * handle);
    void finishTransaction(ConnectionId cid, PoolHandle * handle);
    void rollback(ConnectionId cid, sp_Connection connection, sp_Pool pool);
    void rollbackCompleted(ConnectionId cid, sp_Connection connection, sp_Pool pool, bool succeeded);
};

}
}

#endif //HPHP_ENIGMA_ASSIGNMENT_H
