#ifndef HPHP_ENIGMA_ASYNC_H
#define HPHP_ENIGMA_ASYNC_H

#include "hphp/runtime/ext/extension.h"
#include "hphp/runtime/ext/asio/socket-event.h"
#include "hphp/runtime/ext/asio/asio-external-thread-event.h"
#include "enigma-common.h"
#include "enigma-plan.h"
#include "enigma-query.h"
#include "pgsql-connection.h"
#include "pgsql-result.h"

namespace HPHP {
namespace Enigma {

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
    typedef std::function<void(Connection &, State)> StateChangeCallback;

    Connection(Pgsql::ConnectionOptions const & options, unsigned planCacheSize);

    void ensureConnected();
    void beginReset();
    void executeQuery(p_Query query, QueryCompletionCallback callback);
    void cancelQuery();

    void setStateChangeCallback(StateChangeCallback callback);
    bool isQuerySuccessful(Pgsql::ResultResource & result, std::string & lastError);

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

    inline Pgsql::ConnectionResource & connection() const {
        return *resource_.get();
    }

    inline PlanCache & planCache() {
        return planCache_;
    }

protected:
    friend struct QueryAwait;

    void socketReady(bool read, bool write);

private:
    Pgsql::ConnectionOptions options_;
    std::unique_ptr<Pgsql::ConnectionResource> resource_{ nullptr };
    State state_{ State::Dead };
    bool writing_{ true };

    bool hasQueuedQuery_ { false };
    p_Query nextQuery_;
    PlanCache planCache_;
    QueryCompletionCallback queryCallback_;
    std::string lastError_;
    StateChangeCallback stateChangeCallback_;

    void connect();
    void reset();
    void beginConnect();
    void beginQuery();
    void finishQuery(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result);
    void queryCompleted();
    void processPollingStatus(Pgsql::ConnectionResource::PollingStatus status);
    void connectionOk();
    void markAsDead(std::string const & reason);
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
    void cancelQuery();

    inline bool succeeded() const {
        return succeeded_;
    }

    inline std::string const & lastError() const {
        return lastError_;
    }

    inline Query const & query() const {
        always_assert(query_);
        return *query_;
    }

    p_Query swapQuery(p_Query query);

protected:
    sp_Connection connection_;

    friend class SocketIoHandler;

    void socketReady(bool read, bool write);
    void queryCompleted(bool succeeded, std::unique_ptr<Pgsql::ResultResource> result,
                        std::string const & errorInfo);
    void attachSocketIoHandler();
    void detachSocketIoHandler();
    void fdChanged();

private:
    std::shared_ptr<SocketIoHandler> socketIoHandler_;
    int socket_{ -1 };
    bool succeeded_;
    bool writing_{ true };
    bool completed_{ false };
    std::unique_ptr<Pgsql::ResultResource> result_;
    std::string lastError_;
    p_Query query_{ nullptr };
    CompletionCallback callback_;
};

}
}

#endif //HPHP_ENIGMA_ASYNC_H
