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

    Array mapped;
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

    Array mapped;
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
    // Check if the preceding byte is in [0-9a-zA-Z \r\n\t]
    if (
            pos != 0
            && !isspace(command[pos - 1])
            && !isalnum(command[pos - 1])
            ) {
        return false;
    }

    // Check if the following byte is in [0-9a-zA-Z: \r\n\t]
    // Allow ":", as parameter typecasting is fairly common
    if (
            pos < command.length() - 1
            && !isspace(command[pos + 1])
            && !isalnum(command[pos + 1])
            && command[pos + 1] != ':'
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


Pool::Pool(Array const & options) {
    for (unsigned i = 0; i < poolSize_; i++) {
        addConnection(options);
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
    idleConnections_.push_back(connectionId);
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
    auto idleIt = std::find(idleConnections_.begin(), idleConnections_.end(), connectionId);
    if (idleIt != idleConnections_.end()) {
        idleConnections_.erase(idleIt);
    }
}

QueryAwait * Pool::enqueue(p_Query query) {
    if (queue_.size() >= maxQueueSize_) {
        // TODO improve error reporting
        throw Exception("Enigma queue size exceeded");
    }

    Lock lock(mutex_);
    bool shouldExecute = queue_.empty() && !idleConnections_.empty();
    ENIG_DEBUG("Pool::enqueue(): create QueryAwait");
    auto event = new QueryAwait(std::move(query));
    queue_.push(event);

    if (shouldExecute) {
        executeNext();
    }

    return event;
}

unsigned Pool::assignConnectionId() {
    always_assert(!idleConnections_.empty());
    // todo: random, RR selection
    auto index = rand() % idleConnections_.size();
    unsigned connectionId = idleConnections_[index];
    if (index < idleConnections_.size() - 1) {
        idleConnections_[index] = *idleConnections_.rbegin();
    }

    idleConnections_.pop_back();
    return connectionId;
}

void Pool::executeNext() {
    ENIG_DEBUG("Pool::executeNext");
    always_assert(!queue_.empty());

    auto connectionId = assignConnectionId();
    auto connection = connectionMap_[connectionId];

    auto query = queue_.front();
    queue_.pop();

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
            preparing_.insert(std::make_pair(connectionId, query));
            pendingPrepare_.insert(std::make_pair(connectionId, std::move(originalQuery)));
        }
    } else {
        ENIG_DEBUG("Begin executing query");
    }

    auto callback = [this, connectionId] {
        Lock lock(mutex_);
        this->queryCompleted(connectionId);
    };
    query->assign(connection);
    query->begin(callback);
}

void Pool::queryCompleted(unsigned connectionId) {
    ENIG_DEBUG("Pool::queryCompleted");
    idleConnections_.push_back(connectionId);
    if (!queue_.empty()) {
        executeNext();
    }
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
        auto waitEvent = poolInterface->pool->enqueue(std::unique_ptr<Query>(query));

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


void registerQueueClasses() {
    ENIGMA_NAMED_ME(PoolInterface, Pool, query);
    Native::registerNativeDataInfo<PoolInterface>(s_PoolInterface.get());

    ENIGMA_NAMED_ME(QueryInterface, Query, __construct);
    ENIGMA_NAMED_ME(QueryInterface, Query, enablePlanCache);
    HHVM_RCC_INT(QueryInterfaceNS, CACHE_PLAN, Query::kCachePlan);
    Native::registerNativeDataInfo<QueryInterface>(s_QueryInterface.get());
}

}
}