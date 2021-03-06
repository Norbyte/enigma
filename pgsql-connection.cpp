#include "pgsql-connection.h"
#include "pgsql-result.h"
#include "hphp/runtime/base/array-iterator.h"

namespace HPHP {
namespace Pgsql {

PreparedParameters::PreparedParameters() {}

PreparedParameters::PreparedParameters(PreparedParameters const & prepared)
        : paramBuffer_(prepared.paramBuffer_), params_(prepared.params_.size()) {
    for (unsigned i = 0; i < prepared.params_.size(); i++) {
        auto ptr = prepared.params_[i];
        if (ptr == nullptr) {
            params_[i] = nullptr;
        } else {
            params_[i] = ptr - prepared.paramBuffer_.data() + paramBuffer_.data();
        }
    }
}

PreparedParameters::PreparedParameters(Array const & params)
        : params_((ssize_t)params.size()) {
    unsigned i = 0;
    std::vector<ssize_t> positions((ssize_t)params.size());
    for (ArrayIter param(params); param; ++param, ++i) {
        if (param.second().isNull()) {
            positions[i] = std::string::npos;
        } else {
            auto str = param.second().toString();
            auto currentBufferPos = paramBuffer_.size();
            std::copy(str.c_str(), str.c_str() + str.length() + 1, std::back_inserter(paramBuffer_));
            positions[i] = currentBufferPos;
        }
    }

    for (unsigned i = 0; i < positions.size(); i++) {
        if (positions[i] == std::string::npos) {
            params_[i] = nullptr;
        } else {
            params_[i] = paramBuffer_.data() + positions[i];
        }
    }
}


ConnectionResource::ConnectionResource(ConnectionOptions const & params, ConnectionInit initType) {
    beginConnection(params, initType);
}

ConnectionResource::~ConnectionResource() {
    if (connection_ != nullptr) {
        PQfinish(connection_);
    }
}

/**
 * Poll libpq so that it can proceed with the connection sequence.
 */
ConnectionResource::PollingStatus ConnectionResource::pollConnection() {
    switch (PQconnectPoll(connection_)) {
        case PGRES_POLLING_FAILED:  return PollingStatus::Failed;
        case PGRES_POLLING_READING: return PollingStatus::Reading;
        case PGRES_POLLING_WRITING: return PollingStatus::Writing;
        case PGRES_POLLING_OK:      return PollingStatus::Ok;
        default:
            throw EnigmaException("Unknown value returned from PQconnectPoll()");
    }
}

/**
 * Reset the communication channel to the server.
 */
void ConnectionResource::reset() {
    ENIG_DEBUG("PQreset()");
    PQreset(connection_);
}

/**
 * Reset the communication channel to the server, in a nonblocking manner.
 */
void ConnectionResource::resetStart() {
    ENIG_DEBUG("PQresetStart()");
    if (PQresetStart(connection_) != 1) {
        throw EnigmaException(std::string("Failed to reset connection: ") + errorMessage());
    }
}

/**
 * Poll libpq so that it can proceed with the reset sequence.
 */
ConnectionResource::PollingStatus ConnectionResource::pollReset() {
    ENIG_DEBUG("PQresetPoll()");
    switch (PQresetPoll(connection_)) {
        case PGRES_POLLING_FAILED:  return PollingStatus::Failed;
        case PGRES_POLLING_READING: return PollingStatus::Reading;
        case PGRES_POLLING_WRITING: return PollingStatus::Writing;
        case PGRES_POLLING_OK:      return PollingStatus::Ok;
        default:
            throw EnigmaException("Unknown value returned from PQresetPoll()");
    }
}

/**
 * Returns the status of the connection.
 */
ConnectionResource::Status ConnectionResource::status() const {
    ENIG_DEBUG("PQstatus()");
    switch (PQstatus(connection_)) {
        case CONNECTION_OK:  return Status::Ok;
        case CONNECTION_BAD: return Status::Bad;
            // The rest of the CONNECTION_ statuses are only used during asynchronous connection
        default:
            return Status::Pending;
    }
}

/**
 * Returns the current in-transaction status of the server.
 */
ConnectionResource::TransactionStatus ConnectionResource::transactionStatus() const {
    ENIG_DEBUG("PQtransactionStatus()");
    switch (PQtransactionStatus(connection_)) {
        case PQTRANS_IDLE:    return TransactionStatus::Idle;
        case PQTRANS_ACTIVE:  return TransactionStatus::Active;
        case PQTRANS_INTRANS: return TransactionStatus::InTransaction;
        case PQTRANS_INERROR: return TransactionStatus::InError;
        case PQTRANS_UNKNOWN: return TransactionStatus::Unknown;
        default:
            throw EnigmaException("Unknown value returned from PQtransactionStatus()");
    }
}

bool ConnectionResource::inTransaction() const {
    auto state = transactionStatus();
    return state == TransactionStatus::InTransaction ||
            state == TransactionStatus::InError;
}

/**
 * Looks up a current parameter setting of the server.
 */
Variant ConnectionResource::parameterStatus(String const & param) const {
    auto value = PQparameterStatus(connection_, param.c_str());
    if (value == nullptr) {
        return Variant(Variant::NullInit{});
    } else {
        return String(value);
    }
}

/**
 * Returns the error message most recently generated by an operation on the connection.
 */
std::string ConnectionResource::errorMessage() const {
    auto message = PQerrorMessage(connection_);
    if (message == nullptr) {
        return std::string();
    } else {
        return std::string(message);
    }
}

/**
 * Obtains the file descriptor number of the connection socket to the server.
 */
int ConnectionResource::socket() const {
    return PQsocket(connection_);
}

/**
 * Returns the process ID (PID) of the backend process handling this connection.
 */
int ConnectionResource::backendPid() const {
    return PQbackendPID(connection_);
}

/**
 * Submits a command to the server waits for the result.
 */
p_ResultResource ConnectionResource::query(String const & command) {
    ENIG_DEBUG("PQexec()");
    auto result = PQexec(connection_, command.c_str());
    if (result == nullptr) {
        throw EnigmaException(std::string("Failed to execute query: ") + errorMessage());
    }

    return p_ResultResource(new ResultResource(result));
}

/**
 * Submits a command to the server and waits for the result, with the ability to pass parameters separately from the SQL command text.
 */
p_ResultResource ConnectionResource::queryParams(String const & command, PreparedParameters const & params, bool binary) {
    ENIG_DEBUG("PQsendQueryParams()");
    auto result = PQexecParams(connection_, command.c_str(), params.count(), nullptr, params.buffer(), nullptr, nullptr, binary ? 1 : 0);
    if (result == nullptr) {
        throw EnigmaException(std::string("Failed to execute query: ") + errorMessage());
    }

    return p_ResultResource(new ResultResource(result));
}

/**
 * Submits a request to create a prepared statement with the given parameters, and waits for completion.
 */
p_ResultResource ConnectionResource::prepare(String const & stmtName, String const & command, int numParams) {
    ENIG_DEBUG("PQsendPrepare()");
    auto result = PQprepare(connection_, stmtName.c_str(), command.c_str(), numParams, nullptr);
    if (result == nullptr) {
        throw EnigmaException(std::string("Failed to prepare statement: ") + errorMessage());
    }

    return p_ResultResource(new ResultResource(result));
}

/**
 * Sends a request to execute a prepared statement with given parameters, and waits for the result.
 */
p_ResultResource ConnectionResource::queryPrepared(String const & stmtName, PreparedParameters const & params, bool binary) {
    ENIG_DEBUG("PQsendQueryPrepared()");
    auto result = PQexecPrepared(connection_, stmtName.c_str(), params.count(), params.buffer(), nullptr, nullptr, binary ? 1 : 0);
    if (result == nullptr) {
        throw EnigmaException(std::string("Failed to execute prepared query: ") + errorMessage());
    }

    return p_ResultResource(new ResultResource(result));
}

/**
 * Submits a request to obtain information about the specified prepared statement, and waits for completion.
 */
p_ResultResource ConnectionResource::describePrepared(String const & stmtName) {
    ENIG_DEBUG("PQsendDescribePrepared()");
    auto result = PQdescribePrepared(connection_, stmtName.c_str());
    if (result == nullptr) {
        throw EnigmaException(std::string("Failed to describe prepared statement: ") + errorMessage());
    }

    return p_ResultResource(new ResultResource(result));
}

/**
 * Submits a command to the server without waiting for the result(s).
 */
void ConnectionResource::sendQuery(String const & command) {
    ENIG_DEBUG("PQsendQuery()");
    if (PQsendQuery(connection_, command.c_str()) != 1) {
        throw EnigmaException(std::string("Failed to send query: ") + errorMessage());
    }
}

/**
 * Submits a command and separate parameters to the server without waiting for the result(s).
 */
void ConnectionResource::sendQueryParams(String const & command, PreparedParameters const & params, bool binary) {
    ENIG_DEBUG("PQsendQueryParams()");
    if (PQsendQueryParams(connection_, command.c_str(), params.count(), nullptr, params.buffer(), nullptr, nullptr, binary ? 1 : 0) != 1) {
        throw EnigmaException(std::string("Failed to send query: ") + errorMessage());
    }
}

/**
 * Sends a request to create a prepared statement with the given parameters, without waiting for completion.
 */
void ConnectionResource::sendPrepare(String const & stmtName, String const & command, int numParams) {
    ENIG_DEBUG("PQsendPrepare()");
    if (PQsendPrepare(connection_, stmtName.c_str(), command.c_str(), numParams, nullptr) != 1) {
        throw EnigmaException(std::string("Failed to prepare statement: ") + errorMessage());
    }
}

/**
 * Sends a request to execute a prepared statement with given parameters, without waiting for the result(s).
 */
void ConnectionResource::sendQueryPrepared(String const & stmtName, PreparedParameters const & params, bool binary) {
    ENIG_DEBUG("PQsendQueryPrepared()");
    if (PQsendQueryPrepared(connection_, stmtName.c_str(), params.count(), params.buffer(), nullptr, nullptr, binary ? 1 : 0) != 1) {
        throw EnigmaException(std::string("Failed to send prepared query: ") + errorMessage());
    }
}

/**
 * Submits a request to obtain information about the specified prepared statement, without waiting for completion.
 */
void ConnectionResource::sendDescribePrepared(String const & stmtName) {
    ENIG_DEBUG("PQsendDescribePrepared()");
    if (PQsendDescribePrepared(connection_, stmtName.c_str()) != 1) {
        throw EnigmaException(std::string("Failed to describe prepared statement: ") + errorMessage());
    }
}

/**
 * Waits for the next result from a prior sendQuery, sendQueryParams, sendPrepare, or
 * sendQueryPrepared call, and returns it.
 * A null pointer is returned when the command is complete and there will be no more results.
 */
std::unique_ptr<ResultResource> ConnectionResource::getResult() {
    ENIG_DEBUG("PQgetResult()");
    auto result = PQgetResult(connection_);
    if (result == nullptr) {
        return std::unique_ptr<ResultResource>();
    } else {
        /*
         * Only fetch one result when copying in/out, as the fetch will block
         * indefinitely until all rows are transferred using PQputCopy/PQgetCopy.
         */
        auto status = PQresultStatus(result);

        if (status != PGRES_COPY_IN &&
            status != PGRES_COPY_OUT &&
            status != PGRES_COPY_BOTH) {
            /*
             * We don't support multiple result sets, so discard all
             * subsequent PQresult-s
             */
            while (auto discarded = PQgetResult(connection_)) {
                PQclear(discarded);
            }
        }

        return std::unique_ptr<ResultResource>(
                new ResultResource(result));
    }
}

/**
 * If input is available from the server, consume it.
 *
 * Returns false if a command is busy, that is, getResult() would block waiting for input
 */
bool ConnectionResource::consumeInput() {
    if (PQconsumeInput(connection_) != 1) {
        throw EnigmaException(std::string("Failed to process server response: ") + errorMessage());
    }

    return PQisBusy(connection_) != 1;
}

/**
 * Attempts to flush any queued output data to the server.
 *
 * Returns true if successful, or false if it was unable to send all the data in the send queue yet.
 */
bool ConnectionResource::flush() {
    ENIG_DEBUG("flush()");
    switch (PQflush(connection_)) {
        case 0:
            return true;
        case 1:
            return false;
        default:
            throw EnigmaException(std::string("Failed to flush connection: ") + errorMessage());
    }
}

void ConnectionResource::cancel() {
    char errbuf[256];
    errbuf[0] = '\0';
    auto cancel = PQgetCancel(connection_);
    bool canceled = false;
    if (cancel != nullptr) {
        ENIG_DEBUG("PQcancel()");
        canceled = (PQcancel(cancel, errbuf, sizeof(errbuf)) == 1);
        PQfreeCancel(cancel);
    }

    if (!canceled) {
        throw EnigmaException(std::string("Failed to cancel query: ") + errbuf);
    }
}

/**
 * Convert an array to a list of raw (const char *) strings.
 * The string vectors must be preallocated to hold at least [values.length()] elements.
 */
void ConnectionResource::arrayToStringList(Array const & values, req::vector<String> & strings,
                                           req::vector<const char *> & ptrs, bool allowNulls) {
    unsigned i = 0;
    for (ArrayIter param(values); param; ++param, ++i) {
        if (allowNulls && param.second().isNull()) {
            ptrs[i] = nullptr;
        } else {
            strings[i] = param.second().toString();
            ptrs[i] = strings[i].c_str();
        }
    }
}

void ConnectionResource::beginConnection(ConnectionOptions const & params, ConnectionInit initType) {
    ssize_t n_params = params.size();
    req::vector<std::string> keys(n_params), values(n_params);
    req::vector<const char *> pg_keys(n_params + 1), pg_values(n_params + 1);

    unsigned i = 0;
    for (auto it = params.begin(); it != params.end(); ++it, ++i) {
        keys[i] = it->first;
        pg_keys[i] = keys[i].c_str();

        values[i] = it->second;
        pg_values[i] = values[i].c_str();
    }

    pg_keys[i] = nullptr;
    pg_values[i] = nullptr;

    ENIG_DEBUG("PQconnectStartParams()");
    if (initType == ConnectionInit::InitAsync) {
        connection_ = PQconnectStartParams(pg_keys.data(), pg_values.data(), 0);
    } else {
        connection_ = PQconnectdbParams(pg_keys.data(), pg_values.data(), 0);
    }

    if (connection_ == nullptr) {
        throw EnigmaException("Failed to initialize pgsql connection");
    }

    if (status() == Status::Bad) {
        throw EnigmaException(std::string("Failed to initialize pgsql connection: ")
                              + errorMessage());
    }

    if (PQsetnonblocking(connection_, 1) != 0) {
        throw EnigmaException("Failed to set nonblocking mode on connection");
    }
}

} // namespace Pgsql
} // namespace HPHP
