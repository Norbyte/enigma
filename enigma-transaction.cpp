#include "enigma-transaction.h"
#include "enigma-queue.h"

namespace HPHP {
namespace Enigma {


TransactionLifetimeManager::TransactionLifetimeManager() {}


TransactionLifetimeManager::~TransactionLifetimeManager() {}

bool TransactionLifetimeManager::enqueue(QueryAwait * event, PoolHandle * handle) {
    auto & txn = handle->transaction;
    if (txn.connectionId != Pool::InvalidConnectionId) {
        if (!txn.executing) {
            ENIG_DEBUG("TLM::enqueue(): Begin executing query");
            txn.executing = true;
            handle->pool->execute(txn.connectionId, event, handle);
        } else {
            ENIG_DEBUG("TLM::enqueue(): Add query to local queue");
            bool written = txn.pendingQueries.write(event);
            if (!written) {
                throwEnigmaException("Transactional queue size exceeded");
            }
        }

        return true;
    } else {
        return false;
    }
}

ConnectionId TransactionLifetimeManager::assignConnection(PoolHandle * handle) {
    return handle->transaction.connectionId;
}


QueryAwait * TransactionLifetimeManager::assignQuery(ConnectionId cid) {
    auto handle = connectionsInTransaction_[cid];
    if (handle) {
        always_assert(!handle->transaction.executing);
        always_assert(handle->transaction.connectionId == cid);
        QueryAwait * query;
        if (handle->transaction.pendingQueries.read(query)) {
            ENIG_DEBUG("TLM::assignQuery(): Assign query from queue");
            return query;
        }
    }

    return nullptr;
}


bool TransactionLifetimeManager::notifyFinishAssignment(PoolHandle * handle, ConnectionId cid) {
    auto connection = handle->pool->connection(cid);
    bool assigned = (handle->transaction.connectionId != Pool::InvalidConnectionId);
    bool inTransaction = connection->inTransaction();
    handle->transaction.executing = false;

    if (inTransaction && !assigned) {
        ENIG_DEBUG("TLM::notifyFinishAssignment(): Connection assigned to handle");
        beginTransaction(cid, handle);
    } else if (!inTransaction && assigned) {
        ENIG_DEBUG("TLM::notifyFinishAssignment(): Connection added to idle pool");
        finishTransaction(cid, handle);
    }

    return !inTransaction;
}


void TransactionLifetimeManager::notifyHandleCreated(PoolHandle * handle) {
}


void TransactionLifetimeManager::notifyHandleReleased(PoolHandle * handle) {
    auto cid = handle->transaction.connectionId;
    if (cid != Pool::InvalidConnectionId) {
        ENIG_DEBUG("TLM::notifyHandleReleased(): Drop transaction");
        finishTransaction(cid, handle);
        handle->pool->releaseConnection(cid);
    }
}


void TransactionLifetimeManager::notifyConnectionAdded(ConnectionId cid) {
    connectionsInTransaction_.insert(std::make_pair(cid, nullptr));
}


void TransactionLifetimeManager::notifyConnectionRemoved(ConnectionId cid) {
    connectionsInTransaction_.erase(connectionsInTransaction_.find(cid));
}


void TransactionLifetimeManager::beginTransaction(ConnectionId cid, PoolHandle * handle) {
    always_assert(handle->transaction.connectionId == Pool::InvalidConnectionId);
    connectionsInTransaction_[cid] = handle;
    handle->transaction.connectionId = cid;
}


void TransactionLifetimeManager::finishTransaction(ConnectionId cid, PoolHandle * handle) {
    connectionsInTransaction_[cid] = nullptr;
    handle->transaction.connectionId = Pool::InvalidConnectionId;

    // Move queries that were queued after COMMIT/ROLLBACK to the shared queue
    QueryAwait * query;
    while (handle->transaction.pendingQueries.read(query)) {
        handle->pool->enqueue(query, handle);
    }
}


}
}
