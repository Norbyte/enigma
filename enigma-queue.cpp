#include "enigma-queue.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {


EnigmaPlanInfo::EnigmaPlanInfo(std::string const & cmd)
        : command(cmd) {
    determineParameterType();
}

Array EnigmaPlanInfo::mapParameters(Array const & params) {
    if (type == ParameterType::Named) {
        return mapNamedParameters(params);
    } else {
        return mapNumberedParameters(params);
    }
}

Array EnigmaPlanInfo::mapNamedParameters(Array const & params) {
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

Array EnigmaPlanInfo::mapNumberedParameters(Array const & params) {
    Array mapped;
    for (unsigned i = 0; i < parameterCount; i++) {
        auto value = params->nvGet(i);
        if (value == nullptr) {
            throw EnigmaException(std::string("Missing bound parameter: ") + std::to_string(i));
        }

        mapped.append(*reinterpret_cast<Variant const *>(value));
    }

    return mapped;
}

void EnigmaPlanInfo::determineParameterType() {
    auto numbered = parseNumberedParameters();
    auto named = parseNamedParameters();

    if (std::get<1>(named).size() > 0 && std::get<1>(numbered) > 0) {
        throw EnigmaException("Query contains both named and numbered parameters");
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

bool EnigmaPlanInfo::isValidPlaceholder(std::size_t pos) const {
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

std::size_t EnigmaPlanInfo::namedPlaceholderLength(std::size_t pos) const {
    if (!isValidPlaceholder(pos)) {
        return 0;
    }

    auto i = pos;
    for (; i < command.length() && (isalnum(command[i]) || command[i] == '_'); i++);

    return i - pos;
}

std::tuple<std::string, unsigned> EnigmaPlanInfo::parseNumberedParameters() const {
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
    LOG(INFO) << rewritten.str();
    return std::make_tuple(rewritten.str(), numParams);
}

std::tuple<std::string, std::vector<std::string> > EnigmaPlanInfo::parseNamedParameters() const {
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


EnigmaPlanCache::CachedPlan::CachedPlan(std::string const & cmd)
        : planInfo(cmd)
{}

EnigmaPlanCache::CachedPlan const * EnigmaPlanCache::lookupPlan(std::string const & query) const {
    auto it = plans_.find(query);
    if (it != plans_.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

void EnigmaPlanCache::storePlan(std::string const & query, std::string const & statementName) {
    auto plan = p_CachedPlan(new CachedPlan(query));
    plan->statementName = statementName;
    plans_.insert(std::make_pair(query, std::move(plan)));
}


EnigmaPool::EnigmaPool(Array const & options) {
    for (unsigned i = 0; i < poolSize_; i++) {
        addConnection(options);
    }
}

EnigmaPool::~EnigmaPool() {
}

void EnigmaPool::addConnection(Array const & options) {
    auto connection = std::make_shared<EnigmaConnection>(options);
    auto connectionId = nextConnectionIndex_++;
    connectionMap_.insert(std::make_pair(connectionId, connection));
    idleConnections_.push_back(connectionId);
}

EnigmaQueryAwait * EnigmaPool::enqueue(p_EnigmaQuery query) {
    if (queue_.size() >= maxQueueSize_) {
        // TODO improve error reporting
        throw EnigmaException("Enigma queue size exceeded");
    }

    bool shouldExecute = queue_.empty() && !idleConnections_.empty();
    LOG(INFO) << "create EnigmaQueryAwait";
    auto event = new EnigmaQueryAwait(std::move(query));
    queue_.push(event);

    if (shouldExecute) {
        executeNext();
    }

    return event;
}

unsigned EnigmaPool::assignConnectionId() {
    always_assert(!idleConnections_.empty());
    auto index = rand() % idleConnections_.size();
    unsigned connectionId = idleConnections_[index];
    if (index < idleConnections_.size() - 1) {
        idleConnections_[index] = *idleConnections_.rbegin();
    }

    idleConnections_.pop_back();
    return connectionId;
}

void EnigmaPool::executeNext() {
    LOG(INFO) << "EnigmaPool::executeNext";
    always_assert(!queue_.empty());

    auto connectionId = assignConnectionId();
    auto connection = connectionMap_[connectionId];

    auto query = queue_.front();
    queue_.pop();
    auto callback = [this, connectionId] { this->queryCompleted(connectionId); };
    query->assign(connection);
    query->begin(callback);
}

void EnigmaPool::queryCompleted(unsigned connectionId) {
    LOG(INFO) << "EnigmaPool::queryCompleted";
    idleConnections_.push_back(connectionId);
    if (!queue_.empty()) {
        executeNext();
    }
}


const StaticString s_EnigmaPoolInterface("EnigmaPoolInterface");

Object EnigmaPoolInterface::newInstance(EnigmaPool * p) {
    Object instance{Unit::lookupClass(s_EnigmaPoolInterface.get())};
    Native::data<EnigmaPoolInterface>(instance)
            ->pool = p;
    return instance;
}


Object HHVM_METHOD(EnigmaPoolInterface, query, String const & command, Array const & params) {
    auto poolInterface = Native::data<EnigmaPoolInterface>(this_);

    EnigmaPlanInfo planInfo(command.c_str());
    auto bindableParams = planInfo.mapParameters(params);
    auto query = new EnigmaQuery(EnigmaQuery::ParameterizedInit{}, planInfo.rewrittenCommand, bindableParams);
    auto waitEvent = poolInterface->pool->enqueue(std::unique_ptr<EnigmaQuery>(query));
    return Object{waitEvent->getWaitHandle()};
}

void registerEnigmaQueueClasses() {
    HHVM_ME(EnigmaPoolInterface, query);
    Native::registerNativeDataInfo<EnigmaPoolInterface>(s_EnigmaPoolInterface.get());
}

}