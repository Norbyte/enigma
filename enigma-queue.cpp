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

    // Check if the following byte is in [0-9a-zA-Z \r\n\t]
    if (
            pos < command.length() - 1
            && !isspace(command[pos + 1])
            && !isalnum(command[pos + 1])
            ) {
        return false;
    }

    return true;
}

std::size_t PlanInfo::namedPlaceholderLength(std::size_t pos) const {
    if (!isValidPlaceholder(pos)) {
        return 0;
    }

    auto i = pos;
    for (; i < command.length() && (isalnum(command[i]) || command[i] == '_'); i++);

    return i - pos;
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
    ENIG_DEBUG(rewritten.str());
    return std::make_tuple(rewritten.str(), numParams);
}

std::tuple<std::string, std::vector<std::string> > PlanInfo::parseNamedParameters() const {
    unsigned numParams{0};
    std::vector<std::string> params;
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
            rewritten << '$';
            rewritten << (++numParams);
            params.push_back(command.substr(pos, placeholderLength));
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

void PlanCache::storePlan(std::string const & query, std::string const & statementName) {
    auto plan = p_CachedPlan(new CachedPlan(query));
    plan->statementName = statementName;
    plans_.insert(std::make_pair(query, std::move(plan)));
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
    idleConnections_.push_back(connectionId);
}

QueryAwait * Pool::enqueue(p_Query query) {
    if (queue_.size() >= maxQueueSize_) {
        // TODO improve error reporting
        throw Exception("Enigma queue size exceeded");
    }

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
    auto callback = [this, connectionId] { this->queryCompleted(connectionId); };
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

    PlanInfo planInfo(queryData->command().c_str());
    auto bindableParams = planInfo.mapParameters(queryData->params());
    auto query = new Query(Query::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
    query->setFlags(queryData->flags());
    auto waitEvent = poolInterface->pool->enqueue(std::unique_ptr<Query>(query));
    return Object{waitEvent->getWaitHandle()};
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