#include "enigma-queue.h"
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

    Array mapped{Array::Create()};
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

bool PlanInfo::isValidPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [0-9a-zA-Z (\r\n\t]
    if (
            pos != 0
            && !isspace(command[pos - 1])
            && !isalnum(command[pos - 1])
            && command[pos - 1] != '('
            && command[pos - 1] != ']'
            && command[pos - 1] != ','
            ) {
        return false;
    }

    // Check if the following byte is in [0-9a-zA-Z:) \r\n\t]
    // Allow ":", as parameter typecasting is fairly common
    if (
            pos < command.length() - 1
            && !isspace(command[pos + 1])
            && !isalnum(command[pos + 1])
            && command[pos + 1] != ':'
            && command[pos + 1] != ')'
            && command[pos + 1] != ']'
            && command[pos + 1] != ','
            ) {
        return false;
    }

    return true;
}

bool PlanInfo::isValidNamedPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [ \r\n\t]
    if (
            pos != 0
            && !isspace(command[pos - 1])
            && command[pos - 1] != '('
            && command[pos - 1] != '['
            && command[pos - 1] != ','
            ) {
        return false;
    }

    // Check if the following byte is in [0-9a-zA-Z_]
    if (
            pos < command.length() - 1
            && !isalnum(command[pos + 1])
            && command[pos + 1] != '_'
            ) {
        return false;
    }

    return true;
}

std::size_t PlanInfo::namedPlaceholderLength(std::size_t pos) const {
    if (!isValidNamedPlaceholder(pos)) {
        return 0;
    }

    auto i = pos + 1;
    for (; i < command.length() && (isalnum(command[i]) || command[i] == '_'); i++);

    return i - pos - 1;
}

std::tuple<std::string, unsigned> PlanInfo::parseNumberedParameters() const {
    unsigned numParams{0};
    std::ostringstream rewritten;

    // Check if the placeholder is valid
    // (should only be preceded and followed by [0-9a-z] and whitespace)
    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find('?', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.write(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        if (isValidPlaceholder(pos++)) {
            rewritten << '$';
            rewritten << (++numParams);
        } else {
            rewritten << '?';
        }
    }

    rewritten.write(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten.str(), numParams);
}

std::tuple<std::string, std::vector<std::string> > PlanInfo::parseNamedParameters() const {
    std::vector<std::string> params;
    std::unordered_map<std::string, unsigned> paramMap;
    std::ostringstream rewritten;

    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find(':', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.write(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        auto placeholderLength = namedPlaceholderLength(pos++);
        if (placeholderLength > 0) {
            std::string param = command.substr(pos, placeholderLength);
            auto paramIt = paramMap.find(param);
            if (paramIt != paramMap.end()) {
                rewritten << '$';
                rewritten << paramIt->second;
            } else {
                params.push_back(param);
                paramMap.insert(std::make_pair(param, params.size()));

                rewritten << '$';
                rewritten << params.size();
            }

            pos += placeholderLength;
            lastWrittenPos += placeholderLength;
        } else {
            rewritten << ':';
        }
    }

    rewritten.write(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten.str(), std::move(params));
}


PlanCache::CachedPlan::CachedPlan(std::string const & cmd)
        : planInfo(cmd)
{}

PlanCache::CachedPlan const * PlanCache::lookupPlan(std::string const & query) const {
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
    plans_.insert(std::make_pair(query, std::move(plan)));
    return planPtr;
}

std::string PlanCache::generatePlanName() {
    return PlanNamePrefix + std::to_string(nextPlanId_++);
}

const StaticString
    s_PoolSize("pool_size"),
    s_QueueSize("queue_size");

Pool::Pool(Array const & connectionOpts, Array const & poolOpts)
    : queue_(MaxQueueSize),
      boundQueue_(MaxQueueSize),
      idleConnections_(MaxPoolSize) {
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
    auto planCache = p_PlanCache(new PlanCache());
    planCaches_.insert(std::make_pair(connectionId, std::move(planCache)));
    idleConnections_.blockingWrite(connectionId);
}

void Pool::removeConnection(unsigned connectionId) {
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

QueryAwait * Pool::enqueue(p_Query query, PoolInterface * interface) {
    if (queue_.size() + boundQueue_.size() >= maxQueueSize_) {
        // TODO improve error reporting
        throw Exception("Enigma queue size exceeded");
    }

    ENIG_DEBUG("Pool::enqueue(): create QueryAwait");
    auto event = new QueryAwait(std::move(query));
    if (interface->connectionId == InvalidConnectionId) {
        queue_.blockingWrite(QueueItem{event, interface});
    } else {
        boundQueue_.blockingWrite(QueueItem{event, interface});
    }

    tryExecuteNext();
    return event;
}

void Pool::releaseConnection(unsigned connectionId) {
    idleConnections_.blockingWrite(connectionId);
}

unsigned Pool::assignConnectionId(PoolInterface * interface) {
    if (interface->connectionId != InvalidConnectionId) {
        return interface->connectionId;
    }

    // todo: random, RR selection
    unsigned connectionId;
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
    /*
     * First check the queries assigned to running transactions, as they use
     * a dedicated connection and don't consume connections from the shared pool.
     */
    if (!boundQueue_.read(query)) {
        if (idleConnections_.isEmpty()) {
            return;
        }

        if (!queue_.read(query)) {
            return;
        }
    }

    auto connectionId = assignConnectionId(query.interface);
    execute(connectionId, query.query, query.interface);
}

void Pool::execute(unsigned connectionId, QueryAwait * query, PoolInterface * interface) {
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
            std::unique_ptr<Query> execQuery(new Query(
                    Query::PreparedInit{}, plan->statementName, q.params()));
            query->swapQuery(std::move(execQuery));
        } else {
            /*
             * Begin preparing the query and store the original query params
             * for later execution.
             */
            ENIG_DEBUG("Begin preparing");
            plan = planCaches_[connectionId]->assignPlan(q.command().c_str());
            std::unique_ptr<Query> planQuery(new Query(
                    Query::PrepareInit{}, plan->statementName, plan->planInfo.rewrittenCommand,
                    plan->planInfo.parameterCount));
            auto originalQuery = query->swapQuery(std::move(planQuery));
            preparing_.insert(std::make_pair(connectionId, QueueItem{query, interface}));
            pendingPrepare_.insert(std::make_pair(connectionId, std::move(originalQuery)));
        }
    } else {
        ENIG_DEBUG("Begin executing query");
    }

    auto callback = [this, connectionId, interface] {
        this->queryCompleted(connectionId, interface);
    };
    query->assign(connection);
    query->begin(callback);
}

void Pool::queryCompleted(unsigned connectionId, PoolInterface * interface) {
    auto connection = connectionMap_[connectionId];
    if (connection->inTransaction()) {
        /*
         * If a transaction is open, bind the connection to the pool making the query.
         */
        interface->connectionId = connectionId;
        ENIG_DEBUG("Pool::queryCompleted: In transaction, not releasing connection to pool");
    } else {
        interface->connectionId = InvalidConnectionId;
        idleConnections_.blockingWrite(connectionId);
        ENIG_DEBUG("Pool::queryCompleted: Connection added to idle pool");
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


const StaticString s_PoolInterface("PoolInterface"),
        s_PoolInterfaceNS("Enigma\\Pool"),
        s_QueryInterface("QueryInterface"),
        s_QueryInterfaceNS("Enigma\\Query");

Object PoolInterface::newInstance(sp_Pool p) {
    Object instance{Unit::lookupClass(s_PoolInterfaceNS.get())};
    Native::data<PoolInterface>(instance)
            ->init(p);
    return instance;
}

PoolInterface::~PoolInterface() {
    sweep();
}

void PoolInterface::init(sp_Pool p) {
    pool = p;
}

void PoolInterface::sweep() {
    if (connectionId != Pool::InvalidConnectionId) {
        pool->releaseConnection(connectionId);
    }

    pool.reset();
}


Object HHVM_METHOD(PoolInterface, query, Object const & queryObj) {
    auto poolInterface = Native::data<PoolInterface>(this_);

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
        auto waitEvent = poolInterface->pool->enqueue(std::unique_ptr<Query>(query), poolInterface);

        return Object{waitEvent->getWaitHandle()};
    } catch (std::exception & e) {
        throwEnigmaException(e.what());
    }
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
    ENIGMA_NAMED_ME(PoolInterface, Pool, query);
    Native::registerNativeDataInfo<PoolInterface>(s_PoolInterface.get());

    ENIGMA_NAMED_ME(QueryInterface, Query, __construct);
    ENIGMA_NAMED_ME(QueryInterface, Query, enablePlanCache);
    ENIGMA_NAMED_ME(QueryInterface, Query, setBinary);
    HHVM_RCC_INT(QueryInterfaceNS, CACHE_PLAN, Query::kCachePlan);
    HHVM_RCC_INT(QueryInterfaceNS, BINARY, Query::kBinary);
    Native::registerNativeDataInfo<QueryInterface>(s_QueryInterface.get());
}

}
}
