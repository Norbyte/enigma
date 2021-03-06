#include <hphp/util/conv-10.h>
#include "enigma-queue.h"
#include "enigma-transaction.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {
namespace Enigma {


AssignmentManager::~AssignmentManager() {}

const StaticString
    s_PoolSize("pool_size"),
    s_QueueSize("queue_size"),
    s_PlanCacheSize("plan_cache_size");

Pool::Pool(Array const & connectionOpts, Array const & poolOpts)
    : queue_(MaxQueueSize),
      idleConnections_(MaxPoolSize),
      transactionLifetimeManager_(new TransactionLifetimeManager()) {
    if (poolOpts.exists(s_PoolSize)) {
        auto size = (unsigned)poolOpts[s_PoolSize].toInt32();
        if (size < 1 || size > MaxPoolSize) {
            throwEnigmaException("Invalid pool size specified");
        }

        poolSize_ = size;
    }

    if (poolOpts.exists(s_QueueSize)) {
        auto size = (unsigned)poolOpts[s_QueueSize].toInt32();
        if (size < 1 || size > MaxQueueSize) {
            throwEnigmaException("Invalid queue size specified");
        }

        maxQueueSize_ = size;
    }

    if (poolOpts.exists(s_PlanCacheSize)) {
        auto size = (unsigned)poolOpts[s_PlanCacheSize].toInt32();
        if (size < 1 || size > PlanCache::MaxPlanCacheSize) {
            throwEnigmaException("Invalid plan cache size specified");
        }

        planCacheSize_ = size;
    }

    Pgsql::ConnectionOptions pgsqlOpts;
    for (ArrayIter iter(connectionOpts); iter; ++iter) {
        pgsqlOpts.insert(std::make_pair(
                iter.first().toString().toCppString(), iter.second().toString().toCppString()));
    }

    for (unsigned i = 0; i < poolSize_; i++) {
        addConnection(pgsqlOpts);
    }
}

Pool::~Pool() {
}

void Pool::addConnection(Pgsql::ConnectionOptions const & options) {
    auto connection = std::make_shared<Connection>(options, planCacheSize_);
    auto connectionId = nextConnectionIndex_++;
    connectionMap_.insert(std::make_pair(connectionId, connection));
    idleConnections_.blockingWrite(connectionId);
    transactionLifetimeManager_->notifyConnectionAdded(connectionId);
}

void Pool::removeConnection(ConnectionId connectionId) {
    transactionLifetimeManager_->notifyConnectionRemoved(connectionId);
    auto prepareIt = preparing_.find(connectionId);
    if (prepareIt != preparing_.end()) {
        preparing_.erase(prepareIt);
    }

    auto pendingPrepareIt = pendingPrepare_.find(connectionId);
    if (pendingPrepareIt != pendingPrepare_.end()) {
        pendingPrepare_.erase(pendingPrepareIt);
    }

    connectionMap_.erase(connectionId);
}

QueryAwait * Pool::enqueue(p_Query query, PoolHandle * handle) {
    if (queue_.size() >= maxQueueSize_) {
        // TODO improve error reporting
        throw Exception("Enigma queue size exceeded");
    }

    ENIG_DEBUG("Pool::enqueue(): create QueryAwait");
    auto event = new QueryAwait(std::move(query));
    enqueue(event, handle);
    return event;
}

void Pool::enqueue(QueryAwait * event, PoolHandle * handle) {
    if (!transactionLifetimeManager_->enqueue(event, handle)) {
        if (!queue_.writeIfNotFull(QueueItem{event, handle})) {
            throw Exception("Enigma queue size exceeded");
        }
    }

    tryExecuteNext();
}

void Pool::releaseConnection(ConnectionId connectionId) {
    idleConnections_.blockingWrite(connectionId);
}

sp_Connection Pool::connection(ConnectionId connectionId) {
    auto it = connectionMap_.find(connectionId);
    always_assert(it != connectionMap_.end());
    return it->second;
}

void Pool::createHandle(PoolHandle * handle) {
    transactionLifetimeManager_->notifyHandleCreated(handle);
}

void Pool::releaseHandle(PoolHandle * handle) {
    transactionLifetimeManager_->notifyHandleReleased(handle);
}

ConnectionId Pool::assignConnectionId(PoolHandle * handle) {
    ConnectionId connectionId = InvalidConnectionId;
    if (handle != nullptr) {
        connectionId = transactionLifetimeManager_->assignConnection(handle);
    }

    if (connectionId != InvalidConnectionId) {
        return connectionId;
    }

    // todo: random, RR selection
    for (;;) {
        idleConnections_.blockingRead(connectionId);
        // Handle case where the connection ID is still in the idle queue,
        // but the connection was already closed.
        if (connectionMap_.find(connectionId) != connectionMap_.end()) {
            break;
        }
    }

    return connectionId;
}

void Pool::tryExecuteNext() {
    QueueItem query;
    if (idleConnections_.isEmpty()) {
        return;
    }

    if (!queue_.read(query)) {
        return;
    }

    auto connectionId = assignConnectionId(query.handle);
    execute(connectionId, query.query, query.handle);
}

void Pool::execute(ConnectionId connectionId, QueryAwait * query, PoolHandle * handle) {
    ENIG_DEBUG("Pool::execute");

    auto connection = connectionMap_[connectionId];
    auto const & q = query->query();

    /*
     * Check if the query is a candidate for automatic prepared statement generation
     * and if planning has already taken place for this query.
     */
    if (q.flags() & Query::kCachePlan && q.type() == Query::Type::Parameterized) {
        auto plan = connection->planCache().lookupPlan(q.command().c_str());
        if (plan) {
            /*
             * Query was already prepared on this connection, use the
             * auto assigned statement handle.
             */
            ENIG_DEBUG("Begin executing cached prepared stmt");
            p_Query execQuery(new Query(
                    Query::PreparedInit{}, plan->statementName, q.params()));
            query->swapQuery(std::move(execQuery));
        } else {
            /*
             * Begin preparing the query and store the original query params
             * for later execution.
             */
            ENIG_DEBUG("Begin preparing");
            plan = connection->planCache().assignPlan(q.command().c_str());
            p_Query planQuery(new Query(
                    Query::PrepareInit{}, plan->statementName, plan->planInfo.rewrittenCommand,
                    plan->planInfo.parameterCount));
            auto originalQuery = query->swapQuery(std::move(planQuery));
            preparing_.insert(std::make_pair(connectionId, QueueItem{query, handle}));
            pendingPrepare_.insert(std::make_pair(connectionId, std::move(originalQuery)));
        }
    } else {
        ENIG_DEBUG("Begin executing query");
    }

    auto callback = [this, connectionId, handle] {
        this->queryCompleted(connectionId, handle);
    };
    query->assign(connection);
    query->begin(callback);
}

void Pool::queryCompleted(ConnectionId connectionId, PoolHandle * handle) {
    if (transactionLifetimeManager_->notifyFinishAssignment(handle, connectionId)) {
        releaseConnection(connectionId);
    } else {
        auto query = transactionLifetimeManager_->assignQuery(connectionId);
        if (query) {
            execute(connectionId, query, handle);
        }
    }

    tryExecuteNext();
}

sp_Pool PersistentPoolStorage::make(Array const & connectionOpts, Array const & poolOpts) {
    {
        ReadLock l(lock_);
        auto connectionKey = makeKey(connectionOpts);
        auto it = pools_.find(connectionKey);
        if (it != pools_.end()) {
            ENIG_DEBUG("PersistentPoolStorage::make() reuse existing connection");
            return it->second;
        }
    }

    return add(connectionOpts, poolOpts);
}

sp_Pool PersistentPoolStorage::add(Array const & connectionOpts, Array const & poolOpts) {
    WriteLock l(lock_);

    ENIG_DEBUG("PersistentPoolStorage::add()");
    auto connectionKey = makeKey(connectionOpts);
    auto pool = std::make_shared<Enigma::Pool>(connectionOpts, poolOpts);
    pools_.insert(std::make_pair(connectionKey, pool));
    return pool;
}

void PersistentPoolStorage::remove(Array const & connectionOpts) {
    WriteLock l(lock_);

    ENIG_DEBUG("PersistentPoolStorage::remove()");
    auto connectionKey = makeKey(connectionOpts);
    auto it = pools_.find(connectionKey);
    if (it != pools_.end()) {
        pools_.erase(it);
    }
}

std::string PersistentPoolStorage::makeKey(Array const & connectionOpts) {
    String key;
    for (ArrayIter param(connectionOpts); param; ++param) {
        key += param.first().toString();
        key += "=";
        key += param.second().toString();
        key += ";";
    }

    return key.toCppString();
}


TransactionState::TransactionState()
    : pendingQueries(MaxPendingQueries)
{}


PoolConnectionHandle::PoolConnectionHandle(sp_Pool pool)
    : pool_(pool), connectionId_(pool->assignConnectionId(nullptr)) {
}

PoolConnectionHandle::~PoolConnectionHandle() {
    pool_->releaseConnection(connectionId_);
}

sp_Connection PoolConnectionHandle::getConnection() const {
    return pool_->connection(connectionId_);
}



PoolHandle::PoolHandle(sp_Pool p)
        : pool_(p) {
    p->createHandle(this);
}

PoolHandle::~PoolHandle() {
}

void PoolHandle::bindConnection() {
    if (!connection_) {
        connection_.reset(new PoolConnectionHandle(pool_));
        connection_->getConnection()->ensureConnected();
    }
}

Pgsql::p_ResultResource PoolHandle::query(String const & command, Array const & params, unsigned flags) {
    if (connection_) {
        return query(connection_->getConnection(), command, params, flags);
    } else {
        PoolConnectionHandle ch(pool_);
        auto connection = ch.getConnection();
        connection->ensureConnected();
        return query(connection, command, params, flags);
    }
}

Pgsql::p_ResultResource PoolHandle::query(sp_Connection connection, String const & command, Array const & params, unsigned flags) {
    std::string sql = command.c_str();
    Pgsql::p_ResultResource result;
    if (flags & Query::kCachePlan) {
        auto plan = connection->planCache().lookupPlan(sql);
        if (plan == nullptr) {
            plan = connection->planCache().assignPlan(sql);
            if (plan != nullptr) {
                try {
                    Query query(Query::PrepareInit {}, plan->statementName, plan->planInfo.rewrittenCommand,
                            plan->planInfo.parameterCount);
                    query.exec(connection->connection());
                } catch (...) {
                    connection->planCache().forgetPlan(sql);
                    throw;
                }
            }
        }

        if (plan != nullptr) {
            auto bindableParams = plan->planInfo.mapParameters(params);
            Query query(Query::PreparedInit{}, plan->statementName, bindableParams);
            query.setFlags(flags);
            result = query.exec(connection->connection());
        }
    }

    if (!result) {
        PlanInfo planInfo(sql);
        auto bindableParams = planInfo.mapParameters(params);
        Query query(Query::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
        query.setFlags(flags);
        result = query.exec(connection->connection());
    };

    std::string lastError;
    if (!connection->isQuerySuccessful(*result.get(), lastError)) {
        throwEnigmaException(lastError);
    }

    return result;
}

QueryAwait * PoolHandle::asyncQuery(String const & command, Array const & params, unsigned flags) {
    PlanInfo planInfo(command.c_str());
    auto bindableParams = planInfo.mapParameters(params);
    auto query = new Query(Query::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
    query->setFlags(flags);
    return pool_->enqueue(p_Query(query), this);
}



const StaticString s_PoolHandle("PoolHandle"),
        s_PoolHandleNS("Enigma\\Pool"),
        s_QueryInterface("QueryInterface"),
        s_QueryInterfaceNS("Enigma\\Query");

Object HHPoolHandle::newInstance(sp_Pool p) {
    Object instance{Unit::lookupClass(s_PoolHandleNS.get())};
    Native::data<HHPoolHandle>(instance)
            ->init(p);
    return instance;
}

HHPoolHandle::~HHPoolHandle() {
    sweep();
}

void HHPoolHandle::init(sp_Pool p) {
    handle.reset(new PoolHandle(p));
}

void HHPoolHandle::sweep() {
    ENIG_DEBUG("HHPoolHandle::sweep()");
    handle.reset();
}

Object HHVM_METHOD(HHPoolHandle, syncQuery, Object const & queryObj) {
    auto poolHandle = Native::data<HHPoolHandle>(this_);
    if (!poolHandle->handle) {
        throwEnigmaException(
                "Pool::syncQuery(): Cannot execute a query after the pool handle was released");
    }

    auto queryClass = Unit::lookupClass(s_QueryInterfaceNS.get());
    if (!queryObj.instanceof(queryClass)) {
        SystemLib::throwInvalidArgumentExceptionObject(
                "Pool::syncQuery() expects a Query object as its parameter");
    }

    auto queryData = Native::data<QueryInterface>(queryObj);

    try {
        auto result = poolHandle->handle->query(queryData->command(), queryData->params(), queryData->flags());
        return Object{QueryResult::newInstance(std::move(result))};
    } catch (std::exception & e) {
        throwEnigmaException(e.what());
    }
}

Object HHVM_METHOD(HHPoolHandle, asyncQuery, Object const & queryObj) {
    auto poolHandle = Native::data<HHPoolHandle>(this_);
    if (!poolHandle->handle) {
        throwEnigmaException(
                "Pool::asyncQuery(): Cannot execute a query after the pool handle was released");
    }

    auto queryClass = Unit::lookupClass(s_QueryInterfaceNS.get());
    if (!queryObj.instanceof(queryClass)) {
        SystemLib::throwInvalidArgumentExceptionObject(
                "Pool::asyncQuery() expects a Query object as its parameter");
    }

    auto queryData = Native::data<QueryInterface>(queryObj);

    try {
        auto waitEvent = poolHandle->handle->asyncQuery(queryData->command(), queryData->params(), queryData->flags());
        return Object{waitEvent->getWaitHandle()};
    } catch (std::exception & e) {
        throwEnigmaException(e.what());
    }
}


void HHVM_METHOD(HHPoolHandle, bindConnection) {
    auto poolHandle = Native::data<HHPoolHandle>(this_);
    if (!poolHandle->handle) {
        throwEnigmaException(
                "Pool::bindConnection(): Cannot bind after the pool handle was released");
    }

    poolHandle->handle->bindConnection();
}


void HHVM_METHOD(HHPoolHandle, release) {
    auto poolHandle = Native::data<HHPoolHandle>(this_);
    if (!poolHandle->handle) {
        throwEnigmaException(
                "Pool::release(): Pool handle already released");
    }

    poolHandle->sweep();
}


void QueryInterface::init(String const & command, Array const & params) {
    command_ = command;
    params_ = params;
}


void HHVM_METHOD(QueryInterface, __construct, String const & command, Array const & params) {
    auto query = Native::data<QueryInterface>(this_);
    query->init(command, params);
}


void HHVM_METHOD(QueryInterface, enablePlanCache, bool enabled) {
    auto query = Native::data<QueryInterface>(this_);
    auto flags = query->flags();
    if (enabled) {
        query->setFlags(flags | Query::kCachePlan);
    } else {
        query->setFlags(flags & ~Query::kCachePlan);
    }
}


void HHVM_METHOD(QueryInterface, setBinary, bool enabled) {
    auto query = Native::data<QueryInterface>(this_);
    auto flags = query->flags();
    if (enabled) {
        query->setFlags(flags | Query::kBinary);
    } else {
        query->setFlags(flags & ~Query::kBinary);
    }
}


void registerQueueClasses() {
    ENIGMA_NAMED_ME(HHPoolHandle, Pool, asyncQuery);
    ENIGMA_NAMED_ME(HHPoolHandle, Pool, syncQuery);
    ENIGMA_NAMED_ME(HHPoolHandle, Pool, bindConnection);
    ENIGMA_NAMED_ME(HHPoolHandle, Pool, release);
    Native::registerNativeDataInfo<HHPoolHandle>(s_PoolHandle.get());

    ENIGMA_NAMED_ME(QueryInterface, Query, __construct);
    ENIGMA_NAMED_ME(QueryInterface, Query, enablePlanCache);
    ENIGMA_NAMED_ME(QueryInterface, Query, setBinary);
    HHVM_RCC_INT(QueryInterfaceNS, CACHE_PLAN, Query::kCachePlan);
    HHVM_RCC_INT(QueryInterfaceNS, BINARY, Query::kBinary);
    Native::registerNativeDataInfo<QueryInterface>(s_QueryInterface.get());
}

}
}
