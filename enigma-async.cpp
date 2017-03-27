#include "enigma-async.h"
#include "hphp/runtime/vm/native-data.h"
#include "hphp/runtime/base/execution-context.h"
#include "hphp/util/compatibility.h"

namespace HPHP {
namespace Enigma {

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



Connection::Connection(Array const & options, unsigned planCacheSize)
        : options_(options), planCache_(planCacheSize)
{}

void Connection::ensureConnected() {
    if (state_ == State::Dead) {
        connect();
    }
}

void Connection::connect() {
    if (state_ != State::Dead) {
        throw EnigmaException("Already connected");
    }

    planCache_.clear();
    if (!resource_) {
        ENIG_DEBUG("Connection::connect()");
        resource_ = std::unique_ptr<Pgsql::ConnectionResource>(
                new Pgsql::ConnectionResource(options_, Pgsql::ConnectionInit::InitSync));
    } else {
        resource_->reset();
    }

    state_ = State::Idle;
}

void Connection::reset() {
    ENIG_DEBUG("Connection::reset()");
    planCache_.clear();
    resource_->reset();
    state_ = State::Idle;
}

void Connection::beginConnect() {
    if (state_ != State::Dead) {
        throw EnigmaException("Already connected");
    }

    planCache_.clear();
    writing_ = true;
    if (!resource_) {
        ENIG_DEBUG("Connection::beginConnect()");
        resource_ = std::unique_ptr<Pgsql::ConnectionResource>(
                new Pgsql::ConnectionResource(options_, Pgsql::ConnectionInit::InitAsync));
        state_ = State::Connecting;
    } else {
        resource_->resetStart();
        state_ = State::Resetting;
    }
}

void Connection::beginReset() {
    writing_ = true;
    planCache_.clear();
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
    nextQuery_->send(*resource_.get());

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
        bool succeeded = isQuerySuccessful(*result.get(), lastError_);
        finishQuery(succeeded, std::move(result));
    }
}

bool Connection::isQuerySuccessful(Pgsql::ResultResource & result, std::string & lastError) {
    switch (result.status()) {
        case Pgsql::ResultResource::Status::CommandOk:
        case Pgsql::ResultResource::Status::TuplesOk:
            return true;

        case Pgsql::ResultResource::Status::CopyIn:
        case Pgsql::ResultResource::Status::CopyOut:
            lastError = "Row COPY not supported";
            // Kill off the connection, as we cannot cancel a COPY
            // command any other way :(
            markAsDead(lastError);
            return false;

        case Pgsql::ResultResource::Status::EmptyQuery:
            lastError = "Empty query";
            return false;

        case Pgsql::ResultResource::Status::FatalError:
        case Pgsql::ResultResource::Status::BadResponse:
            lastError = result.errorMessage();
            if (lastError.empty()) {
                lastError = resource_->errorMessage();
            }
            return false;

        default:
            lastError = "ResultResource::status() returned unexpected value";
            return false;
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
    ENIG_DEBUG("Connection::markAsDead(): " << reason.c_str());
    state_ = State::Dead;
    lastError_ = reason;
    if (stateChangeCallback_) {
        stateChangeCallback_(*this, state_);
    }
}

}
}
