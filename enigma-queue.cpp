#include <hphp/util/conv-10.h>
#include "enigma-queue.h"
#include "enigma-transaction.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {
namespace Enigma {


PlanInfo::PlanInfo(std::string const & cmd)
        : command(cmd) {
    determineParameterType();
}

Array PlanInfo::mapParameters(Array const & params) {
    if (type == ParameterType::Named) {
        return mapNamedParameters(params);
    } else {
        return mapNumberedParameters(params);
    }
}

Array PlanInfo::mapNamedParameters(Array const & params) {
    if (parameterNameMap.size() != params.size()) {
        throw Exception(std::string("Parameter count mismatch; expected ") + std::to_string(parameterNameMap.size())
            + " named parameters, got " + std::to_string(params.size()));
    }

    Array mapped{Array::Create()};
    for (unsigned i = 0; i < parameterNameMap.size(); i++) {
        auto key = String(parameterNameMap[i]);
        auto value = params->nvGet(key.get());
        if (value == nullptr) {
            throw EnigmaException(std::string("Missing bound parameter: ") + key.c_str());
        }

        mapped.append(*reinterpret_cast<Variant const *>(value));
    }

    return mapped;
}

Array PlanInfo::mapNumberedParameters(Array const & params) {
    if (parameterCount != params.size()) {
        throw Exception(std::string("Parameter count mismatch; expected ") + std::to_string(parameterCount)
            + " parameters, got " + std::to_string(params.size()));
    }

    Array mapped{Array::attach(PackedArray::MakeReserve(parameterCount))};
    for (unsigned i = 0; i < parameterCount; i++) {
        auto value = params->nvGet(i);
        if (value == nullptr) {
            throw Exception(std::string("Missing bound parameter: ") + std::to_string(i));
        }

        mapped.append(*reinterpret_cast<Variant const *>(value));
    }

    return mapped;
}

void PlanInfo::determineParameterType() {
    auto numbered = parseNumberedParameters();
    auto named = parseNamedParameters();

    if (std::get<1>(named).size() > 0 && std::get<1>(numbered) > 0) {
        throw Exception("Query contains both named and numbered parameters");
    }

    if (std::get<1>(named).size() > 0) {
        type = ParameterType::Named;
        rewrittenCommand = std::move(std::get<0>(named));
        parameterNameMap = std::move(std::get<1>(named));
    } else {
        type = ParameterType::Numbered;
        rewrittenCommand = std::move(std::get<0>(numbered));
        parameterCount = std::get<1>(numbered);
    }
}

inline bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

inline bool is_alnum(char c) {
    return (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z');
}

inline bool is_placeholder_char(char c) {
    return is_alnum(c) || c == '_';
}

inline bool PlanInfo::isValidPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [0-9a-zA-Z (\r\n\t]
    if (pos != 0) {
        char prev = command[pos - 1];
        if (
               !is_space(prev)
               && !is_alnum(prev)
               && prev != '('
               && prev != ']'
               && prev != ','
            ) {
            return false;
        }
    }

    // Check if the following byte is in [0-9a-zA-Z:) \r\n\t]
    // Allow ":", as parameter typecasting is fairly common
    if (pos < command.length() - 1) {
        char next = command[pos + 1];
        if (
                !is_space(next)
                && !is_alnum(next)
                && next != ':'
                && next != ')'
                && next != ']'
                && next != ','
                ) {
            return false;
        }
    }

    return true;
}

inline bool PlanInfo::isValidNamedPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [ \r\n\t]
    if (pos != 0) {
        char prev = command[pos - 1];
        if (
                !is_space(prev)
                && prev != '('
                && prev != '['
                && prev != ','
                ) {
            return false;
        }
    }

    // Check if the following byte is in [0-9a-zA-Z_]
    if (
            pos < command.length() - 1
            && !is_placeholder_char(command[pos + 1])
            ) {
        return false;
    }

    return true;
}

inline std::size_t PlanInfo::namedPlaceholderLength(std::size_t pos) const {
    if (!isValidNamedPlaceholder(pos)) {
        return 0;
    }

    auto i = pos + 1;
    for (; i < command.length() && (is_alnum(command[i]) || command[i] == '_'); i++);

    return i - pos - 1;
}

std::tuple<std::string, unsigned> __attribute__ ((noinline)) PlanInfo::parseNumberedParameters() const {
    unsigned numParams{0};
    std::string rewritten;
    rewritten.reserve(command.size() + (command.size() >> 1));

    // Check if the placeholder is valid
    // (should only be preceded and followed by [0-9a-z] and whitespace)
    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find('?', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.append(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        if (isValidPlaceholder(pos++)) {
            char paramNo[21];
            paramNo[0] = '\0';
            auto paramNoStr = conv_10(++numParams, &paramNo[20]);
            rewritten.append(paramNoStr.start(), paramNoStr.size());
        } else {
            rewritten.push_back('?');
        }
    }

    rewritten.append(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten, numParams);
}

std::tuple<std::string, std::vector<std::string> > PlanInfo::parseNamedParameters() const {
    std::vector<std::string> params;
    std::unordered_map<std::string, unsigned> paramMap;
    std::string rewritten;
    rewritten.reserve(command.size() + (command.size() >> 1));

    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find(':', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.append(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        auto placeholderLength = namedPlaceholderLength(pos++);
        if (placeholderLength > 0) {
            std::string param = command.substr(pos, placeholderLength);
            auto paramIt = paramMap.find(param);
            if (paramIt != paramMap.end()) {
                char paramNo[21];
                paramNo[0] = '\0';
                auto paramNoStr = conv_10(paramIt->second, &paramNo[20]);
                rewritten.append(paramNoStr.start(), paramNoStr.size());
            } else {
                params.push_back(param);
                paramMap.insert(std::make_pair(param, params.size()));

                char paramNo[21];
                paramNo[0] = '\0';
                auto paramNoStr = conv_10(params.size(), &paramNo[20]);
                rewritten.append(paramNoStr.start(), paramNoStr.size());
            }

            pos += placeholderLength;
            lastWrittenPos += placeholderLength;
        } else {
            rewritten.push_back(':');
        }
    }

    rewritten.append(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten, std::move(params));
}

PlanCache::PlanCache(unsigned size)
    : plans_(size)
{}

PlanCache::CachedPlan::CachedPlan(std::string const & cmd)
        : planInfo(cmd)
{}

PlanCache::CachedPlan const * PlanCache::lookupPlan(std::string const & query) {
    auto it = plans_.find(query);
    if (it != plans_.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

PlanCache::CachedPlan const * PlanCache::assignPlan(std::string const & query) {
    auto name = generatePlanName();
    return storePlan(query, name);
}

PlanCache::CachedPlan const * PlanCache::storePlan(std::string const & query, std::string const & statementName) {
    auto plan = p_CachedPlan(new CachedPlan(query));
    auto planPtr = plan.get();
    plan->statementName = statementName;
    plans_.set(query, std::move(plan));
    return planPtr;
}

std::string PlanCache::generatePlanName() {
    return PlanNamePrefix + std::to_string(nextPlanId_++);
}

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

    for (unsigned i = 0; i < poolSize_; i++) {
        addConnection(connectionOpts);
    }
}

Pool::~Pool() {
}

void Pool::addConnection(Array const & options) {
    auto connection = std::make_shared<Connection>(options);
    auto connectionId = nextConnectionIndex_++;
    connectionMap_.insert(std::make_pair(connectionId, connection));
    auto planCache = p_PlanCache(new PlanCache(planCacheSize_));
    planCaches_.insert(std::make_pair(connectionId, std::move(planCache)));
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

    planCaches_.erase(connectionId);
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
    ConnectionId connectionId = transactionLifetimeManager_->assignConnection(handle);
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
        auto plan = planCaches_[connectionId]->lookupPlan(q.command().c_str());
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
            plan = planCaches_[connectionId]->assignPlan(q.command().c_str());
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


const StaticString s_PoolHandle("PoolHandle"),
        s_PoolHandleNS("Enigma\\Pool"),
        s_QueryInterface("QueryInterface"),
        s_QueryInterfaceNS("Enigma\\Query");

Object PoolHandle::newInstance(sp_Pool p) {
    Object instance{Unit::lookupClass(s_PoolHandleNS.get())};
    Native::data<PoolHandle>(instance)
            ->init(p);
    return instance;
}

PoolHandle::~PoolHandle() {
    sweep();
}

void PoolHandle::init(sp_Pool p) {
    pool = p;
    p->createHandle(this);
}

void PoolHandle::sweep() {
    ENIG_DEBUG("PoolHandle::sweep()");
    if (pool) {
        pool->releaseHandle(this);
    }

    always_assert(runningQueries == 0);
    pool.reset();
}


Object HHVM_METHOD(PoolHandle, query, Object const & queryObj) {
    auto poolHandle = Native::data<PoolHandle>(this_);
    if (!poolHandle->pool) {
        throwEnigmaException(
                "Pool::query(): Cannot execute a query after the pool handle was released");
    }

    auto queryClass = Unit::lookupClass(s_QueryInterfaceNS.get());
    if (!queryObj.instanceof(queryClass)) {
        SystemLib::throwInvalidArgumentExceptionObject(
                "Pool::query() expects a Query object as its parameter");
    }

    auto queryData = Native::data<QueryInterface>(queryObj);

    try {
        PlanInfo planInfo(queryData->command().c_str());
        auto bindableParams = planInfo.mapParameters(queryData->params());
        auto query = new Query(Query::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
        query->setFlags(queryData->flags());
        auto waitEvent = poolHandle->pool->enqueue(p_Query(query), poolHandle);

        return Object{waitEvent->getWaitHandle()};
    } catch (std::exception & e) {
        throwEnigmaException(e.what());
    }
}


Object HHVM_METHOD(PoolHandle, syncQuery, Object const & queryObj) {
    auto poolHandle = Native::data<PoolHandle>(this_);
    if (!poolHandle->pool) {
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
        PlanInfo planInfo(queryData->command().c_str());
        auto bindableParams = planInfo.mapParameters(queryData->params());
        Query query(Query::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
        query.setFlags(queryData->flags());
        auto connectionId = poolHandle->pool->assignConnectionId(poolHandle);
        auto connection = poolHandle->pool->connection(connectionId);
        connection->ensureConnected();
        auto result = query.exec(connection->connection());
        poolHandle->pool->releaseConnection(connectionId);
        return Object{QueryResult::newInstance(std::move(result))};
    } catch (std::exception & e) {
        // TODO: release connection ID on exception! --> ConnectionHandle?
        throwEnigmaException(e.what());
    }
}


void HHVM_METHOD(PoolHandle, release) {
    auto poolHandle = Native::data<PoolHandle>(this_);
    if (!poolHandle->pool) {
        throwEnigmaException(
                "Pool::release(): Cannot release a pool handle multiple times");
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
    ENIGMA_NAMED_ME(PoolHandle, Pool, query);
    ENIGMA_NAMED_ME(PoolHandle, Pool, syncQuery);
    ENIGMA_NAMED_ME(PoolHandle, Pool, release);
    Native::registerNativeDataInfo<PoolHandle>(s_PoolHandle.get());

    ENIGMA_NAMED_ME(QueryInterface, Query, __construct);
    ENIGMA_NAMED_ME(QueryInterface, Query, enablePlanCache);
    ENIGMA_NAMED_ME(QueryInterface, Query, setBinary);
    HHVM_RCC_INT(QueryInterfaceNS, CACHE_PLAN, Query::kCachePlan);
    HHVM_RCC_INT(QueryInterfaceNS, BINARY, Query::kBinary);
    Native::registerNativeDataInfo<QueryInterface>(s_QueryInterface.get());
}

}
}
