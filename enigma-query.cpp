#include "enigma-query.h"
#include "hphp/runtime/vm/native-data.h"
#include "hphp/runtime/base/execution-context.h"

namespace HPHP {
namespace Enigma {

void throwEnigmaException(std::string const & message) {
    auto error = ErrorResult::newInstance(message);
    throw error;
}

SocketIoHandler::SocketIoHandler(AsioEventBase* base, int fd, QueryAwait * event)
        : AsioEventHandler(base, fd), pgsqlEvent_(event)
{ }


void SocketIoHandler::handlerReady(uint16_t events) noexcept {
    pgsqlEvent_->socketReady(
            (events & AsioEventHandler::READ) == AsioEventHandler::READ,
            (events & AsioEventHandler::WRITE) == AsioEventHandler::WRITE
    );
}



QueryAwait::QueryAwait(p_Query query)
        : query_(std::move(query))
{}

QueryAwait::~QueryAwait() {
    detachSocketIoHandler();
}

void QueryAwait::assign(sp_Connection connection) {
    always_assert(!connection_);
    connection_ = connection;
}

void QueryAwait::begin(CompletionCallback callback) {
    ENIG_DEBUG("QueryAwait::begin()");
    always_assert(connection_);

    callback_ = callback;
    auto queryCallback = [this] (bool succeeded, Pgsql::ResultResource * results, std::string errorInfo) {
        this->queryCompleted(succeeded, std::unique_ptr<Pgsql::ResultResource>(results), errorInfo);
    };
    connection_->executeQuery(std::move(query_), queryCallback);
    attachSocketIoHandler();
}

void QueryAwait::cancelQuery() {
    if (query_) {
        /*
         * Passing a failure result to the callback is sufficient if we haven't
         * sent the query to the pgsql server yet.
         */
        queryCompleted(false, nullptr, "Query canceled");
    } else {
        /*
         * Cancel the running query.
         * (The query may still complete successfully in certain edge cases)
         */
        connection_->cancelQuery();
    }
}

p_Query QueryAwait::swapQuery(p_Query query) {
    auto q = std::move(query_);
    query_ = std::move(query);
    return std::move(q);
}

void QueryAwait::socketReady(bool read, bool write) {
    if (completed_) {
        return;
    }

    connection_->socketReady(read, write);

    if (completed_) {
        // Notify the client that the async operation completed
        markAsFinished();
        return;
    }

    /*
     * Ignore socket state changes if the IO handler was unregistered
     */
    if (!socketIoHandler_) {
        return;
    }

    if (connection_->isConnecting() && socket_ != connection_->socket()) {
        ENIG_DEBUG("QueryAwait::socketReady(): pgsql socket num changed");
        /*
         * When the connection failed, libpq may create a new socket and
         * retry the connection with different options (eg. a non-SSL connection after
         * SSL connection was rejected), so we need to resubscribe if the socket changed.
         */
        fdChanged();
    } else if (connection_->isWriting() != writing_) {
        /*
         * We should only subscribe to WRITE events if libpq is waiting for a write
         * operation to complete, because ASIO triggers the write ready event on
         * sockets even if the send buffer is empty (thus creating an infinite
         * amount of write notifications).
         */
        writing_ = connection_->isWriting();
        auto event = writing_ ? AsioEventHandler::READ_WRITE : AsioEventHandler::READ;
        socketIoHandler_->unregisterHandler();
        socketIoHandler_->registerHandler(event | AsioEventHandler::PERSIST);
    }
}

void QueryAwait::queryCompleted(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result,
                                std::string const & errorInfo) {
    ENIG_DEBUG("QueryAwait::queryCompleted()");
    /*
     * There is no need to keep the handler running after the query has
     * completed, as the pgsql connection works strictly in a request -> response
     * fashion, ie. the server cannot initiate any requests on its own and
     * should not send us any traffic after our query was answered.
     *
     * Also, this is necessary to make sure that a subsequent query using a
     * different QueryAwait object will not trigger our socket handlers.
     */
    detachSocketIoHandler();

    succeeded_ = succeeded;
    result_ = std::move(result);
    lastError_ = errorInfo;
    callback_();
    completed_ = true;
}

void QueryAwait::unserialize(Cell & result) {
    if (succeeded_) {
        ENIG_DEBUG("QueryAwait::unserialize() OK");
        auto queryResult = QueryResult::newInstance(std::move(result_));
        cellCopy(make_tv<KindOfObject>(queryResult.detach()), result);
    } else {
        ENIG_DEBUG("QueryAwait::unserialize() caught error");
        result.m_type = DataType::KindOfNull;
        throwEnigmaException(lastError_);
    }
}

void QueryAwait::attachSocketIoHandler() {
    auto socket = connection_->socket();
    ENIG_DEBUG("QueryAwait::attachSocketIoHandler() " << socket);
    always_assert(socket > 0);
    auto eventBase = getSingleton<AsioEventBase>();
    assert(!eventBase->isInEventBaseThread());
    socketIoHandler_ = std::make_shared<SocketIoHandler>(eventBase.get(), socket, this);
    writing_ = true;
    socket_ = socket;
    eventBase->runInEventBaseThread([this] {
        this->socketIoHandler_->registerHandler(AsioEventHandler::READ_WRITE | AsioEventHandler::PERSIST);
    });
}

void QueryAwait::detachSocketIoHandler() {
    assert(getSingleton<AsioEventBase>()->isInEventBaseThread());
    if (socketIoHandler_) {
        ENIG_DEBUG("QueryAwait::detachSocketIoHandler() " << socket_);
        socketIoHandler_->unregisterHandler();
        socketIoHandler_.reset();
    }
}

void QueryAwait::fdChanged() {
    assert(getSingleton<AsioEventBase>()->isInEventBaseThread());
    auto socket = connection_->socket();
    ENIG_DEBUG("QueryAwait::fdChanged() " << socket);
    socketIoHandler_->unregisterHandler();
    socketIoHandler_->changeHandlerFD(socket);
    socketIoHandler_->registerHandler(AsioEventHandler::READ_WRITE | AsioEventHandler::PERSIST);
    writing_ = true;
    socket_ = socket;
}




Query::Query(RawInit, String const & command)
        : type_(Type::Raw), command_(command)
{}

Query::Query(ParameterizedInit, String const & command, Array const & params)
        : type_(Type::Parameterized), command_(command), params_(params)
{}

Query::Query(ParameterizedInit, String const & command, Pgsql::PreparedParameters const & params)
        : type_(Type::Parameterized), command_(command), params_(params)
{}

Query::Query(PrepareInit, String const & stmtName, String const & command, unsigned numParams)
        : type_(Type::Prepare), command_(command), statement_(stmtName), numParams_(numParams)
{}

Query::Query(PreparedInit, String const & stmtName, Array const & params)
        : type_(Type::Prepared), statement_(stmtName), params_(params)
{}

Query::Query(PreparedInit, String const & stmtName, Pgsql::PreparedParameters const & params)
        : type_(Type::Prepared), statement_(stmtName), params_(params)
{}



const StaticString s_ErrorResult("ErrorResult"),
        s_ErrorResultNS("Enigma\\ErrorResult");

Object ErrorResult::newInstance(std::string const & message) {
    Object instance{Unit::lookupClass(s_ErrorResultNS.get())};
    Native::data<ErrorResult>(instance)
            ->postConstruct(message);
    return instance;
}

void ErrorResult::postConstruct(std::string const & message) {
    message_ = message;
}

String HHVM_METHOD(ErrorResult, getMessage) {
    auto data = Native::data<ErrorResult>(this_);
    return data->getMessage();
}

const StaticString
    s_QueryResult("QueryResult"),
    s_QueryResultNS("Enigma\\QueryResult");

Object QueryResult::newInstance(std::unique_ptr<Pgsql::ResultResource> results) {
    Object instance{Unit::lookupClass(s_QueryResultNS.get())};
    Native::data<QueryResult>(instance)
            ->postConstruct(std::move(results));
    return instance;
}


QueryResult::QueryResult() {}

QueryResult & QueryResult::operator = (QueryResult const & src) {
    throw Object(SystemLib::AllocExceptionObject(
            "Cloning QueryResult is not allowed"
    ));
}

void QueryResult::postConstruct(std::unique_ptr<Pgsql::ResultResource> results) {
    // TODO Result::create();
    results_ = std::move(results);
}


Array HHVM_METHOD(QueryResult, fetchArrays, int64_t flags) {
    auto data = Native::data<QueryResult>(this_);
    Pgsql::ResultResource const & resource = data->resource();

    Array results{Array::Create()};
    auto rows = resource.numTuples(),
         cols = resource.numFields();

    req::vector<String> colNames((ssize_t)cols);
    req::vector<Oid> colTypes((ssize_t)cols);
    for (auto col = 0; col < cols; col++) {
        colNames[col] = resource.columnName(col);
        colTypes[col] = resource.columnType(col);
    }

    uint32_t valueFlags = (uint32_t)(flags & QueryResult::kResultResourceMask);
    bool numbered = (bool)(flags & QueryResult::kNumbered);
    for (auto row = 0; row < rows; row++) {
        Array rowArr{Array::Create()};
        if (numbered)
        {
            // Fetch arrays with 0, 1, ..., n as keys
            for (auto col = 0; col < cols; col++) {
                rowArr.set(col, resource.typedValue(row, col, colTypes[col], valueFlags));
            }
        }
        else
        {
            // Fetch arrays with column names as keys
            for (auto col = 0; col < cols; col++) {
                rowArr.set(colNames[col], resource.typedValue(row, col, colTypes[col], valueFlags));
            }
        }

        results.append(rowArr);
    }

    return results;
}


Array HHVM_METHOD(QueryResult, fetchObjects, String const & cls, int64_t flags, Array const & args) {
    auto rowClass = Unit::getClass(cls.get(), true);
    if (rowClass == nullptr) {
        throwEnigmaException(std::string("Could not find result row class: ") + cls.c_str());
    }

    auto ctor = rowClass->getCtor();
    auto data = Native::data<QueryResult>(this_);
    Pgsql::ResultResource const & resource = data->resource();
    Array results{Array::Create()};
    auto rows = resource.numTuples(),
         cols = resource.numFields();

    req::vector<Oid> colTypes((ssize_t)cols);
    for (auto col = 0; col < cols; col++) {
        colTypes[col] = resource.columnType(col);
    }

    uint32_t valueFlags = (uint32_t)(flags & QueryResult::kResultResourceMask);
    bool callCtor = !(flags & QueryResult::kDontCallCtor);
    bool constructBeforeBind = (bool)(flags & QueryResult::kConstructBeforeBinding);
    /*
     * When binding to properties, we'll set class properties directly in
     * the property vector, thus bypassing __set and dynamic properties.
     */
    if (flags & QueryResult::kBindToProperties) {

        bool allowInvalidSlot = (bool)(flags & (QueryResult::kIgnoreUndeclared | QueryResult::kAllowUndeclared)),
             useSetter = (bool)(flags & QueryResult::kAllowUndeclared);

        /*
         * The kIgnoreUndeclared and kAllowUndeclared flags control the way fetching works
         * when the property we're updating is not a declared property of the class.
         *
         * 0 (no flags set):  Throw an exception when a column in the result set is not
         *                    a declared property of the class
         * kIgnoreUndeclared: Ignore undeclared properties (don't set those properties)
         * kAllowUndeclared:  Add undeclared properties as dynamic properties (using __set)
         */
        req::vector<String> propNames((ssize_t)cols);
        req::vector<Slot> propSlots((ssize_t)cols);
        for (auto col = 0; col < cols; col++) {
            auto colName = resource.columnName(col);
            propNames[col] = colName;
            auto prop = rowClass->lookupDeclProp(colName.get());
            if (prop == kInvalidSlot && !allowInvalidSlot) {
                throwEnigmaException(std::string("Result row class ") + cls.c_str() + " has no property: " + colName.c_str());
            }

            propSlots[col] = prop;
        }

        for (auto row = 0; row < rows; row++) {
            Object rowObj{rowClass};
            if (constructBeforeBind && callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, args, rowObj.get());
            }

            auto props = rowObj->propVec();
            for (auto col = 0; col < cols; col++) {
                auto slot = propSlots[col];
                auto value = resource.typedValue(row, col, colTypes[col], valueFlags);
                if (UNLIKELY(slot == kInvalidSlot)) {
                    if (useSetter) {
                        rowObj->o_set(propNames[col], value);
                    }
                } else {
                    auto prop = &props[propSlots[col]];
                    *reinterpret_cast<Variant *>(prop) = value;
                }
            }

            if (!constructBeforeBind && callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, args, rowObj.get());
            }

            results.append(rowObj);
        }
    } else {
        req::vector<String> colNames((ssize_t)cols);
        for (auto col = 0; col < cols; col++) {
            colNames[col] = resource.columnName(col);
        }

        for (auto row = 0; row < rows; row++) {
            // Construct a new row object and call the setter on each property
            Object rowObj{rowClass};
            if (constructBeforeBind && callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, args, rowObj.get());
            }

            for (auto col = 0; col < cols; col++) {
                rowObj->o_set(colNames[col], resource.typedValue(row, col, colTypes[col], valueFlags));
            }

            if (!constructBeforeBind && callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, args, rowObj.get());
            }

            results.append(rowObj);
        }
    }

    return results;
}



Connection::Connection(Array const & options)
        : options_(options)
{}

void Connection::beginConnect() {
    if (state_ != State::Dead) {
        throw EnigmaException("Already connected");
    }

    writing_ = true;
    if (!resource_) {
        ENIG_DEBUG("Connection::beginConnect()");
        resource_ = std::unique_ptr<Pgsql::ConnectionResource>(
                new Pgsql::ConnectionResource(options_));
        state_ = State::Connecting;
    } else {
        resource_->resetStart();
        state_ = State::Resetting;
    }
}

void Connection::beginReset() {
    writing_ = true;
    ENIG_DEBUG("Connection::beginReset()");
    resource_->resetStart();
    state_ = State::Resetting;
}

void Connection::executeQuery(p_Query query, QueryCompletionCallback callback) {
    if (hasQueuedQuery_) {
        throw EnigmaException("A query is already queued on this connection");
    }

    hasQueuedQuery_ = true;
    nextQuery_ = std::move(query);
    queryCallback_ = std::move(callback);
    lastError_.clear();

    switch (state_) {
        case State::Dead:
            beginConnect();
            break;

        case State::Idle:
            beginQuery();
            break;

        case State::Connecting:
        case State::Resetting:
            break;

        default:
            always_assert(false);
    }
}

void Connection::cancelQuery() {
    if (state_ == State::Executing) {
        resource_->cancel();
    }
}

void Connection::setStateChangeCallback(StateChangeCallback callback) {
    stateChangeCallback_ = callback;
}

void Connection::beginQuery() {
    ENIG_DEBUG("Connection::beginQuery()");
    bool binary = (nextQuery_->flags() & Query::kBinary) == Query::kBinary;
    switch (nextQuery_->type()) {
        case Query::Type::Raw:
            resource_->sendQuery(nextQuery_->command());
            break;

        case Query::Type::Parameterized:
            resource_->sendQueryParams(nextQuery_->command(), nextQuery_->params(), binary);
            break;

        case Query::Type::Prepare:
            resource_->sendPrepare(nextQuery_->statement(), nextQuery_->command(), nextQuery_->numParams());
            break;

        case Query::Type::Prepared:
            resource_->sendQueryPrepared(nextQuery_->statement(), nextQuery_->params(), binary);
            break;
    }

    lastError_.clear();
    writing_ = true;
    state_ = State::Executing;
}

void Connection::finishQuery(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result) {
    if (!hasQueuedQuery_) {
        return;
    }

    hasQueuedQuery_ = false;
    nextQuery_.reset(nullptr);
    auto callback = std::move(queryCallback_);
    queryCallback_ = QueryCompletionCallback{};
    callback(succeeded, result.release(), lastError_);
}

void Connection::socketReady(bool read, bool write) {
    switch (state_) {
        case State::Idle:
        case State::Dead:
            LOG(ERROR) << "Socket ready event triggered on pgsql connection when no request is in progress";
            break;

        case State::Connecting:
            processPollingStatus(resource_->pollConnection());
            break;

        case State::Resetting:
            processPollingStatus(resource_->pollReset());
            break;

        case State::Executing:
        {
            if (write) {
                if (resource_->flush()) {
                    writing_ = false;
                }
            }

            if (read) {
                if (resource_->consumeInput()) {
                    queryCompleted();
                }
            }

            break;
        }
    }
}

void Connection::queryCompleted() {
    ENIG_DEBUG("Connection::queryCompleted()");
    state_ = State::Idle;
    auto result = resource_->getResult();
    if (!result) {
        lastError_ = resource_->errorMessage();
        finishQuery(false, nullptr);
    } else {
        bool succeeded;
        switch (result->status()) {
            case Pgsql::ResultResource::Status::CommandOk:
            case Pgsql::ResultResource::Status::TuplesOk:
                succeeded = true;
                break;

            case Pgsql::ResultResource::Status::CopyIn:
            case Pgsql::ResultResource::Status::CopyOut:
                lastError_ = "Row COPY not supported";
                // Kill off the connection, as we cannot cancel a COPY
                // command any other way :(
                markAsDead(lastError_);
                succeeded = false;
                break;

            case Pgsql::ResultResource::Status::EmptyQuery:
                lastError_ = "Empty query";
                succeeded = false;
                break;

            case Pgsql::ResultResource::Status::FatalError:
            case Pgsql::ResultResource::Status::BadResponse:
                lastError_ = result->errorMessage();
                if (lastError_.empty()) {
                    lastError_ = resource_->errorMessage();
                }
                succeeded = false;
                break;

            default:
                lastError_ = "ResultResource::status() returned unexpected value";
                succeeded = false;
                break;
        }

        finishQuery(succeeded, std::move(result));
    }
}

void Connection::processPollingStatus(Pgsql::ConnectionResource::PollingStatus status) {
    switch (status) {
        case Pgsql::ConnectionResource::PollingStatus::Ok:
            connectionOk();
            break;

        case Pgsql::ConnectionResource::PollingStatus::Failed:
            connectionDied();
            break;

        case Pgsql::ConnectionResource::PollingStatus::Reading:
            writing_ = false;
            break;

        case Pgsql::ConnectionResource::PollingStatus::Writing:
            writing_ = true;
            break;
    }
}

void Connection::connectionOk() {
    ENIG_DEBUG("Connection::connectionOk()");
    state_ = State::Idle;
    if (stateChangeCallback_) {
        stateChangeCallback_(*this, state_);
    }

    if (hasQueuedQuery_) {
        beginQuery();
    }
}

void Connection::connectionDied() {
    ENIG_DEBUG("Connection::connectionDied(): " << resource_->errorMessage().c_str());
    markAsDead(resource_->errorMessage());
    finishQuery(false, nullptr);
}

void Connection::markAsDead(std::string const & reason) {
    ENIG_DEBUG("Connection::connectionDied(): " << reason.c_str());
    state_ = State::Dead;
    lastError_ = reason;
    if (stateChangeCallback_) {
        stateChangeCallback_(*this, state_);
    }
}


void registerClasses() {
    ENIGMA_ME(ErrorResult, getMessage);
    Native::registerNativeDataInfo<ErrorResult>(s_ErrorResult.get());

    ENIGMA_ME(QueryResult, fetchArrays);
    ENIGMA_ME(QueryResult, fetchObjects);
    Native::registerNativeDataInfo<QueryResult>(s_QueryResult.get());
    HHVM_RCC_INT(QueryResultNS, NATIVE_JSON, Pgsql::ResultResource::kNativeJson);
    HHVM_RCC_INT(QueryResultNS, NATIVE_ARRAYS, Pgsql::ResultResource::kNativeArrays);
    HHVM_RCC_INT(QueryResultNS, NATIVE_DATETIME, Pgsql::ResultResource::kNativeDateTime);
    HHVM_RCC_INT(QueryResultNS, NATIVE, Pgsql::ResultResource::kAllNative);
    HHVM_RCC_INT(QueryResultNS, NUMERIC_FLOAT, Pgsql::ResultResource::kNumericAsFloat);

    HHVM_RCC_INT(QueryResultNS, BIND_TO_PROPERTIES, QueryResult::kBindToProperties);
    HHVM_RCC_INT(QueryResultNS, IGNORE_UNDECLARED, QueryResult::kIgnoreUndeclared);
    HHVM_RCC_INT(QueryResultNS, ALLOW_UNDECLARED, QueryResult::kAllowUndeclared);
    HHVM_RCC_INT(QueryResultNS, DONT_CALL_CTOR, QueryResult::kDontCallCtor);
    HHVM_RCC_INT(QueryResultNS, NUMBERED, QueryResult::kNumbered);
    HHVM_RCC_INT(QueryResultNS, CONSTRUCT_BEFORE_BINDING, QueryResult::kConstructBeforeBinding);
}

}
}
