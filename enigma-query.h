#ifndef HPHP_ENIGMA_QUERY_H
#define HPHP_ENIGMA_QUERY_H

#include "hphp/runtime/ext/extension.h"
#include "hphp/runtime/ext/asio/socket-event.h"
#include "hphp/runtime/ext/asio/asio-external-thread-event.h"
#include "enigma-common.h"
#include "pgsql-connection.h"
#include "pgsql-result.h"

namespace HPHP {
namespace Enigma {

class Query {
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

    enum Flags {
        kCachePlan = 0x01
    };

    Query(RawInit, String const & command);
    Query(ParameterizedInit, String const & command, Array const & params);
    Query(PrepareInit, String const & stmtName, String const & command, unsigned numParams);
    Query(PreparedInit, String const & stmtName, Array const & params);

    Query(const Query &) = delete;
    Query & operator = (const Query &) = delete;

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

    inline void setFlags(unsigned flags) {
        flags_ = flags;
    }

    inline unsigned flags() const {
        return flags_;
    }

private:
    Type type_;
    String command_;
    String statement_;
    unsigned numParams_;
    Pgsql::PreparedParameters params_;
    unsigned flags_{0};
};

typedef std::unique_ptr<Query> p_Query;


class Result {
    // TODO
};



class ErrorResult : public Result {
public:
    static Object newInstance(std::string const & message);

    inline std::string const & getMessage() {
        return message_;
    }

private:
    std::string message_;

    void postConstruct(std::string const & message);
};

class QueryResult : public Result {
public:
    static Object newInstance(std::unique_ptr<Pgsql::ResultResource> results);

    enum FetchOptions {
        kBindToProperties = 0x01,
        kIgnoreUndeclared = 0x02,
        kAllowUndeclared = 0x04,
        kDontCallCtor = 0x08
    };

    QueryResult();
    QueryResult(QueryResult const &) = delete;
    QueryResult & operator =(QueryResult const & src);

    inline Pgsql::ResultResource const & resource() const {
        return *results_.get();
    }

private:
    std::unique_ptr<Pgsql::ResultResource> results_;

    void postConstruct(std::unique_ptr<Pgsql::ResultResource> results);
};

class Connection {
public:
    enum class State {
        Idle,       // connection is idle
        Connecting, // connection is being set up
        Resetting,  // reconnecting to server
        Executing,  // waiting for server to finish executing query
        Dead        // not connected yet, or connection was lost
    };

    typedef std::function<void(bool, Pgsql::ResultResource *, std::string)> QueryCompletionCallback;

    Connection(Array const & options);

    void executeQuery(p_Query query, QueryCompletionCallback callback);
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
    friend struct QueryAwait;

    void socketReady(bool read, bool write);

private:
    Array options_;
    std::unique_ptr<Pgsql::ConnectionResource> resource_{ nullptr };
    State state_{ State::Dead };
    bool writing_{ true };

    bool hasQueuedQuery_ { false };
    p_Query nextQuery_;
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

typedef std::shared_ptr<Connection> sp_Connection;



struct QueryAwait;

/**
 * Asynchronous socket read/write handler for libpq sockets
 */
struct SocketIoHandler : AsioEventHandler {
public:
    SocketIoHandler(AsioEventBase * base, int fd, QueryAwait * event);

    virtual void handlerReady(uint16_t events) noexcept override;

private:
    QueryAwait * pgsqlEvent_;
};


struct QueryAwait : public AsioExternalThreadEvent {
public:
    typedef std::function<void ()> CompletionCallback;

    QueryAwait(p_Query query);
    ~QueryAwait();

    virtual void unserialize(Cell & result) override;
    void assign(sp_Connection connection);
    void begin(CompletionCallback callback);
    void cancel();

protected:
    sp_Connection connection_;

    friend class SocketIoHandler;

    void socketReady(bool read, bool write);
    void queryCompleted(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result,
                        std::string const & errorInfo);
    void attachSocketIoHandler();
    void detachSocketIoHandler();

private:
    std::shared_ptr<SocketIoHandler> socketIoHandler_;
    int socket_{ -1 };
    bool succeeded_;
    bool writing_{ true };
    std::unique_ptr<Pgsql::ResultResource> result_;
    std::string lastError_;
    p_Query query_{ nullptr };
    CompletionCallback callback_;
};

void registerClasses();

}
}

#endif //HPHP_ENIGMA_QUERY_H
