#include "enigma-transaction.h"
#include "enigma-queue.h"

namespace HPHP {
namespace Enigma {


TransactionLifetimeManager::TransactionLifetimeManager() {}


TransactionLifetimeManager::~TransactionLifetimeManager() {}

bool TransactionLifetimeManager::enqueue(QueryAwait * event, PoolHandle * handle) {
    auto & txn = handle->transaction();
    if (txn.connectionId != Pool::InvalidConnectionId) {
        if (!txn.executing) {
            ENIG_DEBUG("TLM::enqueue(): Begin executing query");
            txn.executing = true;
            handle->pool()->execute(txn.connectionId, event, handle);
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
    return handle->transaction().connectionId;
}


QueryAwait * TransactionLifetimeManager::assignQuery(ConnectionId cid) {
    auto handle = connections_[cid].handle;
    if (handle) {
        auto & txn = handle->transaction();
        always_assert(!txn.executing);
        always_assert(txn.connectionId == cid);
        QueryAwait * query;
        if (txn.pendingQueries.read(query)) {
            ENIG_DEBUG("TLM::assignQuery(): Assign query from queue");
            return query;
        }
    }

    return nullptr;
}


bool TransactionLifetimeManager::notifyFinishAssignment(PoolHandle * handle, ConnectionId cid) {
    auto connection = handle->pool()->connection(cid);
    auto & txn = handle->transaction();
    bool assigned = (txn.connectionId != Pool::InvalidConnectionId);
    bool inTransaction = connection->inTransaction();
    txn.executing = false;

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
    auto cid = handle->transaction().connectionId;
    if (cid != Pool::InvalidConnectionId) {
        ENIG_DEBUG("TLM::notifyHandleReleased(): Drop transaction");
        finishTransaction(cid, handle);
        auto connection = handle->pool()->connection(cid);
        if (!connection->inTransaction()) {
            handle->pool()->releaseConnection(cid);
        }
    }
}


void TransactionLifetimeManager::notifyConnectionAdded(ConnectionId cid) {
    connections_.insert(std::make_pair(cid, ConnectionState()));
}


void TransactionLifetimeManager::notifyConnectionRemoved(ConnectionId cid) {
    connections_.erase(connections_.find(cid));
}


void TransactionLifetimeManager::beginTransaction(ConnectionId cid, PoolHandle * handle) {
    auto & txn = handle->transaction();
    always_assert(txn.connectionId == Pool::InvalidConnectionId);
    connections_[cid].handle = handle;
    txn.connectionId = cid;
}


void TransactionLifetimeManager::finishTransaction(ConnectionId cid, PoolHandle * handle) {
    auto & txn = handle->transaction();
    connections_[cid].handle = nullptr;
    txn.connectionId = Pool::InvalidConnectionId;

    // Move queries that were queued after COMMIT/ROLLBACK/sweep to the shared queue
    QueryAwait * event;
    while (txn.pendingQueries.read(event)) {
        handle->pool()->enqueue(event, handle);
    }

    auto connection = handle->pool()->connection(cid);
    if (connection->inTransaction()) {
        if (txn.executing) {
            connections_[cid].rollingBack = true;
        } else {
            rollback(cid, connection, handle->pool());
        }
    }
}


void TransactionLifetimeManager::rollback(ConnectionId cid, sp_Connection connection, sp_Pool pool) {
    ENIG_DEBUG("TLM::rollback(): Rolling back active transaction");
    auto query = new Query(Query::RawInit{}, "rollback");
    auto event = new QueryAwait(p_Query(query));
    event->assign(connection);

    auto callback = [=] () {
        if (!event->succeeded()) {
            ENIG_DEBUG("TLM::rollback(): Failed: " << event->lastError().c_str());
        }
        this->rollbackCompleted(cid, connection, pool, event->succeeded());
    };
    event->begin(callback);
}


void TransactionLifetimeManager::rollbackCompleted(ConnectionId cid, sp_Connection connection, sp_Pool pool, bool succeeded) {
    if (!succeeded) {
        ENIG_DEBUG("TLM::rollbackCompleted(): Resetting connection");
        connection->beginReset();
    }

    pool->releaseConnection(cid);
}


}
}
