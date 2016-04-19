#ifndef HPHP_ENIGMA_QUERY_H
#define HPHP_ENIGMA_QUERY_H

#include "hphp/runtime/ext/extension.h"
#include "hphp/runtime/ext/asio/socket-event.h"
#include "hphp/runtime/ext/asio/asio-external-thread-event.h"
#include "pgsql-connection.h"
#include "pgsql-result.h"

namespace HPHP {


class EnigmaQuery {
public:
    // Execute an SQL command, without any parameters
    enum class RawInit {};
    // Execute an ad hoc SQL command, with parameters
    enum class ParameterizedInit {};
    // Execute a prepared statement, with parameters
    enum class PreparedInit {};
    // Prepare a query for later execution
    enum class PrepareInit {};

    enum class Type {
        Raw,
        Parameterized,
        Prepare,
        Prepared
    };

    EnigmaQuery(RawInit, String const & command);
    EnigmaQuery(ParameterizedInit, String const & command, Array const & params);
    EnigmaQuery(PrepareInit, String const & stmtName, String const & command, unsigned numParams);
    EnigmaQuery(PreparedInit, String const & stmtName, Array const & params);

    EnigmaQuery(const EnigmaQuery &) = delete;
    EnigmaQuery & operator = (const EnigmaQuery &) = delete;

    inline Type type() const {
        return type_;
    }

    inline String const & command() const {
        return command_;
    }

    inline String const & statement() const {
        return statement_;
    }

    inline unsigned const & numParams() const {
        return numParams_;
    }

    inline Pgsql::PreparedParameters const & params() const {
        return params_;
    }

private:
    Type type_;
    String command_;
    String statement_;
    unsigned numParams_;
    Pgsql::PreparedParameters params_;
};

typedef std::unique_ptr<EnigmaQuery> p_EnigmaQuery;


class EnigmaResult {
    // TODO
};



class EnigmaErrorResult : public EnigmaResult {
public:
    static Object newInstance(std::string const & message);

    inline std::string const & getMessage() {
        return message_;
    }

private:
    std::string message_;

    void postConstruct(std::string const & message);
};

class EnigmaQueryResult : public EnigmaResult {
public:
    static Object newInstance(std::unique_ptr<Pgsql::ResultResource> results);

    enum FetchOptions {
        kBindToProperties = 0x01,
        kIgnoreUndeclared = 0x02,
        kAllowUndeclared = 0x04,
        kDontCallCtor = 0x08
    };

    EnigmaQueryResult();
    EnigmaQueryResult(EnigmaQueryResult const &) = delete;
    EnigmaQueryResult & operator =(EnigmaQueryResult const & src);

    inline Pgsql::ResultResource const & resource() const {
        return *results_.get();
    }

private:
    std::unique_ptr<Pgsql::ResultResource> results_;

    void postConstruct(std::unique_ptr<Pgsql::ResultResource> results);
};

class EnigmaConnection {
public:
    enum class State {
        Idle,       // connection is idle
        Connecting, // connection is being set up
        Resetting,  // reconnecting to server
        Executing,  // waiting for server to finish executing query
        Dead        // not connected yet, or connection was lost
    };

    typedef std::function<void(bool, Pgsql::ResultResource *)> QueryCompletionCallback;

    EnigmaConnection(Array const & options);

    void executeQuery(p_EnigmaQuery query, QueryCompletionCallback callback);
    void cancelQuery();

    inline bool inTransaction() const {
        return resource_->inTransaction();
    }

    inline bool isConnecting() const {
        return state_ == State::Connecting || state_ == State::Resetting;
    }

    inline bool isWriting() const {
        return writing_;
    }

    inline int socket() const {
        if (resource_) {
            return resource_->socket();
        } else {
            return -1;
        }
    }

    inline std::string const & lastError() const {
        return lastError_;
    }

protected:
    friend struct EnigmaQueryAwait;

    void socketReady(bool read, bool write);

private:
    Array options_;
    std::unique_ptr<Pgsql::ConnectionResource> resource_{ nullptr };
    State state_{ State::Dead };
    bool writing_{ true };

    bool hasQueuedQuery_ { false };
    p_EnigmaQuery nextQuery_;
    QueryCompletionCallback queryCallback_;
    std::string lastError_;

    void beginConnect();
    void beginQuery();
    void finishQuery(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result);
    void queryCompleted();
    void processPollingStatus(Pgsql::ConnectionResource::PollingStatus status);
    void connectionOk();
    void connectionDied();
};

typedef std::shared_ptr<EnigmaConnection> sp_EnigmaConnection;



struct EnigmaQueryAwait;

/**
 * Asynchronous socket read/write handler for libpq sockets
 */
struct EnigmaSocketIoHandler : AsioEventHandler {
public:
    EnigmaSocketIoHandler(AsioEventBase * base, int fd, EnigmaQueryAwait * event);

    virtual void handlerReady(uint16_t events) noexcept override;

private:
    EnigmaQueryAwait * pgsqlEvent_;
};


struct EnigmaQueryAwait : public AsioExternalThreadEvent {
public:
    typedef std::function<void ()> CompletionCallback;

    EnigmaQueryAwait(p_EnigmaQuery query);
    ~EnigmaQueryAwait();

    virtual void unserialize(Cell & result) override;
    void assign(sp_EnigmaConnection connection);
    void begin(CompletionCallback callback);
    void cancel();

protected:
    sp_EnigmaConnection connection_;

    friend class EnigmaSocketIoHandler;

    void socketReady(bool read, bool write);
    void queryCompleted(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result);
    void attachSocketIoHandler();
    void detachSocketIoHandler();

private:
    std::shared_ptr<EnigmaSocketIoHandler> socketIoHandler_;
    int socket_{ -1 };
    bool succeeded_;
    bool writing_{ true };
    std::unique_ptr<Pgsql::ResultResource> result_;
    p_EnigmaQuery query_{ nullptr };
    CompletionCallback callback_;
};

void registerEnigmaClasses();

}

#endif //HPHP_ENIGMA_QUERY_H
