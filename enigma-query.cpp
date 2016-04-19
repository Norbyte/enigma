#include "enigma-query.h"
#include "hphp/runtime/vm/native-data.h"
#include "hphp/runtime/base/execution-context.h"

namespace HPHP {

void throwEnigmaException(std::string const & message) {
    auto error = EnigmaErrorResult::newInstance(message);
    throw error;
    // SystemLib::throwExceptionObject()
    // throw_object(exception_type, params, true /* init */);
}

EnigmaSocketIoHandler::EnigmaSocketIoHandler(AsioEventBase* base, int fd, EnigmaQueryAwait * event)
        : AsioEventHandler(base, fd), pgsqlEvent_(event)
{ }


void EnigmaSocketIoHandler::handlerReady(uint16_t events) noexcept {
    pgsqlEvent_->socketReady(
            (events & AsioEventHandler::READ) == AsioEventHandler::READ,
            (events & AsioEventHandler::WRITE) == AsioEventHandler::WRITE
    );
}



EnigmaQueryAwait::EnigmaQueryAwait(p_EnigmaQuery query)
        : query_(std::move(query))
{}

EnigmaQueryAwait::~EnigmaQueryAwait() {
    detachSocketIoHandler();
}

void EnigmaQueryAwait::assign(sp_EnigmaConnection connection) {
    always_assert(!connection_);
    connection_ = connection;
}

void EnigmaQueryAwait::begin(CompletionCallback callback) {
    LOG(INFO) << "EnigmaQueryAwait::begin()";
    always_assert(connection_);

    callback_ = callback;
    auto queryCallback = [this] (bool succeeded, Pgsql::ResultResource * results) {
        this->queryCompleted(succeeded, std::unique_ptr<Pgsql::ResultResource>(results));
    };
    connection_->executeQuery(std::move(query_), queryCallback);
    attachSocketIoHandler();
}

void EnigmaQueryAwait::cancel() {
    if (query_) {
        // TODO: add error message!
        /*
         * Passing a failure result to the callback is sufficient if we haven't
         * sent the query to the pgsql server yet.
         */
        queryCompleted(false, nullptr);
    } else {
        /*
         * Cancel the running query.
         * (The query may still complete successfully in certain edge cases)
         */
        connection_->cancelQuery();
    }
}

void EnigmaQueryAwait::socketReady(bool read, bool write) {
    /*if (read && write) LOG(INFO) << "socket ready RW";
    else if (read) LOG(INFO) << "socket ready R";
    else LOG(INFO) << "socket ready W";*/
    connection_->socketReady(read, write);

    /*
     * Ignore socket state changes if the IO handler was unregistered
     */
    if (!socketIoHandler_) {
        return;
    }

    if (connection_->isConnecting() && socket_ != connection_->socket()) {
        LOG(INFO) << "pgsql socket num changed";
        /*
         * When the connection failed, libpq may create a new socket and
         * retry the connection with different options (eg. a non-SSL connection after
         * SSL connection was rejected), so we need to resubscribe if the socket changed.
         */
        detachSocketIoHandler();
        attachSocketIoHandler();
    } else if (connection_->isWriting() != writing_) {
        /*
         * We should only subscribe to WRITE events if libpq is waiting for a write
         * operation to complete, because ASIO triggers the write ready event on
         * sockets even if the send buffer is empty (thus creating an infinite
         * amount of write notifications).
         */
        // LOG(INFO) << "pgsql R/W state changed";
        writing_ = connection_->isWriting();
        auto event = writing_ ? AsioEventHandler::READ_WRITE : AsioEventHandler::READ;
        socketIoHandler_->unregisterHandler();
        socketIoHandler_->registerHandler(event | AsioEventHandler::PERSIST);
    }
}

void EnigmaQueryAwait::queryCompleted(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result) {
    LOG(INFO) << "queryCompleted()";
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
    callback_();
    // Notify the client that the async operation completed
    markAsFinished();
}

void EnigmaQueryAwait::unserialize(Cell & result) {
    LOG(INFO) << "Unserialize!";
    if (succeeded_) {
        LOG(INFO) << "got results!";
        auto queryResult = EnigmaQueryResult::newInstance(std::move(result_));
        cellCopy(make_tv<KindOfObject>(queryResult.detach()), result);
    } else {
        LOG(INFO) << "got exc!";
        result.m_type = DataType::KindOfNull;
        if (result_) {
            throwEnigmaException(result_->errorMessage().toString().c_str());
        } else {
            throwEnigmaException(connection_->lastError());
        }
    }
}

void EnigmaQueryAwait::attachSocketIoHandler() {
    LOG(INFO) << "attaching ASIO handler";
    auto eventBase = getSingleton<AsioEventBase>();
    auto handler = std::make_shared<EnigmaSocketIoHandler>(eventBase.get(), connection_->socket(), this);
    handler->registerHandler(AsioEventHandler::READ_WRITE | AsioEventHandler::PERSIST);
    writing_ = true;
    socketIoHandler_ = handler;
    socket_ = connection_->socket();
}

void EnigmaQueryAwait::detachSocketIoHandler() {
    if (socketIoHandler_) {
        LOG(INFO) << "detaching ASIO handler";
        socketIoHandler_->unregisterHandler();
        socketIoHandler_.reset();
    }
}




EnigmaQuery::EnigmaQuery(RawInit, String const & command)
        : type_(Type::Raw), command_(command)
{}

EnigmaQuery::EnigmaQuery(ParameterizedInit, String const & command, Array const & params)
        : type_(Type::Parameterized), command_(command), params_(params)
{}

EnigmaQuery::EnigmaQuery(PrepareInit, String const & stmtName, String const & command, unsigned numParams)
        : type_(Type::Prepare), command_(command), statement_(stmtName), numParams_(numParams)
{}

EnigmaQuery::EnigmaQuery(PreparedInit, String const & stmtName, Array const & params)
        : type_(Type::Prepared), statement_(stmtName), params_(params)
{}



const StaticString s_EnigmaErrorResult("EnigmaErrorResult");

Object EnigmaErrorResult::newInstance(std::string const & message) {
    Object instance{Unit::lookupClass(s_EnigmaErrorResult.get())};
    Native::data<EnigmaErrorResult>(instance)
            ->postConstruct(message);
    return instance;
}

void EnigmaErrorResult::postConstruct(std::string const & message) {
    message_ = message;
}

String HHVM_METHOD(EnigmaErrorResult, getMessage) {
    auto data = Native::data<EnigmaErrorResult>(this_);
    return data->getMessage();
}

const StaticString
    s_EnigmaQueryResult("EnigmaQueryResult"),
    s_BindToProperties("BIND_TO_PROPERTIES"),
    s_IgnoreUndeclared("IGNORE_UNDECLARED"),
    s_AllowUndeclared("ALLOW_UNDECLARED"),
    s_DontCallCtor("DONT_CALL_CTOR");

Object EnigmaQueryResult::newInstance(std::unique_ptr<Pgsql::ResultResource> results) {
    Object instance{Unit::lookupClass(s_EnigmaQueryResult.get())};
    Native::data<EnigmaQueryResult>(instance)
            ->postConstruct(std::move(results));
    return instance;
}


EnigmaQueryResult::EnigmaQueryResult() {}

EnigmaQueryResult & EnigmaQueryResult::operator = (EnigmaQueryResult const & src) {
    throw Object(SystemLib::AllocExceptionObject(
            "Cloning EnigmaQueryResult is not allowed"
    ));
}

void EnigmaQueryResult::postConstruct(std::unique_ptr<Pgsql::ResultResource> results) {
    // TODO EnigmaResult::create();
    results_ = std::move(results);
}


Array HHVM_METHOD(EnigmaQueryResult, fetchArrays) {
    auto data = Native::data<EnigmaQueryResult>(this_);
    Pgsql::ResultResource const & resource = data->resource();

    Array results;
    auto rows = resource.numTuples(),
         cols = resource.numFields();

    req::vector<String> colNames((ssize_t)cols);
    req::vector<Oid> colTypes((ssize_t)cols);
    for (auto col = 0; col < cols; col++) {
        colNames[col] = resource.columnName(col);
        colTypes[col] = resource.columnType(col);
    }

    for (auto row = 0; row < rows; row++) {
        Array rowArr;
        for (auto col = 0; col < cols; col++) {
            rowArr.set(colNames[col], resource.typedValue(row, col, colTypes[col]));
        }

        results.append(rowArr);
    }

    return results;
}


Array HHVM_METHOD(EnigmaQueryResult, fetchObjects, String const & cls, int64_t flags) {
    auto rowClass = Unit::lookupClass(cls.get());
    if (rowClass == nullptr) {
        throwEnigmaException(std::string("Could not find result row class: ") + cls.c_str());
    }

    auto ctor = rowClass->getCtor();
    auto data = Native::data<EnigmaQueryResult>(this_);
    Pgsql::ResultResource const & resource = data->resource();
    Array results;
    auto rows = resource.numTuples(),
         cols = resource.numFields();

    req::vector<Oid> colTypes((ssize_t)cols);
    for (auto col = 0; col < cols; col++) {
        colTypes[col] = resource.columnType(col);
    }

    bool callCtor = !(flags & EnigmaQueryResult::kDontCallCtor);
    /*
     * When binding to properties, we'll set class properties directly in
     * the property vector, thus bypassing __set and dynamic properties.
     */
    if (flags & EnigmaQueryResult::kBindToProperties) {

        bool allowInvalidSlot = (bool)(flags & (EnigmaQueryResult::kIgnoreUndeclared | EnigmaQueryResult::kAllowUndeclared)),
             useSetter = (bool)(flags & EnigmaQueryResult::kAllowUndeclared);

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
            if (callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, Array(), rowObj.get());
            }

            auto props = rowObj->propVec();
            for (auto col = 0; col < cols; col++) {
                auto slot = propSlots[col];
                auto value = resource.typedValue(row, col, colTypes[col]);
                if (UNLIKELY(slot == kInvalidSlot)) {
                    if (useSetter) {
                        rowObj->o_set(propNames[col], value);
                    }
                } else {
                    auto prop = &props[propSlots[col]];
                    *reinterpret_cast<Variant *>(prop) = value;
                }
            }

            results.append(rowObj);
        }
    } else {
        req::vector<String> colNames((ssize_t)cols);
        for (auto col = 0; col < cols; col++) {
            colNames[col] = resource.columnName(col);
        }

        for (auto row = 0; row < rows; row++) {
            Object rowObj{rowClass};
            if (callCtor) {
                TypedValue ret;
                g_context->invokeFunc(&ret, ctor, Array(), rowObj.get());
            }

            for (auto col = 0; col < cols; col++) {
                rowObj->o_set(colNames[col], resource.typedValue(row, col, colTypes[col]));
            }

            results.append(rowObj);
        }
    }

    return results;
}



EnigmaConnection::EnigmaConnection(Array const & options)
        : options_(options)
{}

void EnigmaConnection::beginConnect() {
    if (state_ != State::Dead) {
        throw EnigmaException("Already connected");
    }

    writing_ = true;
    if (!resource_) {
        LOG(INFO) << "starting connection";
        resource_ = std::unique_ptr<Pgsql::ConnectionResource>(
                new Pgsql::ConnectionResource(options_));
        state_ = State::Connecting;
    } else {
        resource_->resetStart();
        state_ = State::Resetting;
    }
}

void EnigmaConnection::executeQuery(p_EnigmaQuery query, QueryCompletionCallback callback) {
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

void EnigmaConnection::cancelQuery() {
    if (state_ == State::Executing) {
        resource_->cancel();
    }
}

void EnigmaConnection::beginQuery() {
    LOG(INFO) << "starting query";
    switch (nextQuery_->type()) {
        case EnigmaQuery::Type::Raw:
            resource_->sendQuery(nextQuery_->command());
            break;

        case EnigmaQuery::Type::Parameterized:
            resource_->sendQueryParams(nextQuery_->command(), nextQuery_->params());
            break;

        case EnigmaQuery::Type::Prepare:
            resource_->sendPrepare(nextQuery_->command(), nextQuery_->statement(), nextQuery_->numParams());
            break;

        case EnigmaQuery::Type::Prepared:
            resource_->sendQueryPrepared(nextQuery_->statement(), nextQuery_->params());
            break;

        // TODO: auto-prepare, plan cache, ... ... (higher level?)
    }

    writing_ = true;
    state_ = State::Executing;
}

void EnigmaConnection::finishQuery(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result) {
    if (!hasQueuedQuery_) {
        return;
    }

    hasQueuedQuery_ = false;
    nextQuery_.reset(nullptr);
    auto callback = std::move(queryCallback_);
    queryCallback_ = QueryCompletionCallback{};
    callback(succeeded, result.release());
}

void EnigmaConnection::socketReady(bool read, bool write) {
    switch (state_) {
        case State::Idle:
        case State::Dead:
            LOG(ERROR) << "Socket ready event triggered on pgsql connection when no request is in progress";
            break;

        case State::Connecting:
            // LOG(INFO) << "pollConnection()";
            processPollingStatus(resource_->pollConnection());
            break;

        case State::Resetting:
            LOG(INFO) << "pollReset()";
            processPollingStatus(resource_->pollReset());
            break;

        case State::Executing:
        {
            // LOG(INFO) << "exec poll";
            if (write) {
                if (resource_->flush()) {
                    LOG(INFO) << "flush() completed";
                    writing_ = false;
                }
            }

            if (read) {
                if (resource_->consumeInput()) {
                    LOG(INFO) << "consumeInput() completed";
                    queryCompleted();
                }
            }

            break;
        }
    }
}

void EnigmaConnection::queryCompleted() {
    LOG(INFO) << "exec ok!";
    state_ = State::Idle;
    auto result = resource_->getResult();
    if (!result) {
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
                succeeded = false;
                break;

            case Pgsql::ResultResource::Status::EmptyQuery:
            case Pgsql::ResultResource::Status::FatalError:
            case Pgsql::ResultResource::Status::BadResponse:
                // TODO: use result errormsg or connection errormsg?
                lastError_ = resource_->errorMessage();
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

void EnigmaConnection::processPollingStatus(Pgsql::ConnectionResource::PollingStatus status) {
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

void EnigmaConnection::connectionOk() {
    LOG(INFO) << "pgsql connected";
    state_ = State::Idle;
    if (hasQueuedQuery_) {
        beginQuery();
    }
}

void EnigmaConnection::connectionDied() {
    LOG(INFO) << "pgsql connection failed: " << resource_->errorMessage().c_str();
    state_ = State::Dead;
    finishQuery(false, nullptr);
}


void registerEnigmaClasses() {
    HHVM_ME(EnigmaErrorResult, getMessage);
    Native::registerNativeDataInfo<EnigmaErrorResult>(s_EnigmaErrorResult.get());

    HHVM_ME(EnigmaQueryResult, fetchArrays);
    HHVM_ME(EnigmaQueryResult, fetchObjects);
    Native::registerNativeDataInfo<EnigmaQueryResult>(s_EnigmaQueryResult.get());
    Native::registerClassConstant<KindOfInt64>(s_EnigmaQueryResult.get(),
        s_BindToProperties.get(), EnigmaQueryResult::kBindToProperties);
    Native::registerClassConstant<KindOfInt64>(s_EnigmaQueryResult.get(),
        s_IgnoreUndeclared.get(), EnigmaQueryResult::kIgnoreUndeclared);
    Native::registerClassConstant<KindOfInt64>(s_EnigmaQueryResult.get(),
        s_AllowUndeclared.get(), EnigmaQueryResult::kAllowUndeclared);
    Native::registerClassConstant<KindOfInt64>(s_EnigmaQueryResult.get(),
        s_DontCallCtor.get(), EnigmaQueryResult::kDontCallCtor);
}

}
