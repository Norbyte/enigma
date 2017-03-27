#ifndef HPHP_ENIGMA_QUERY_H
#define HPHP_ENIGMA_QUERY_H

#include "hphp/runtime/ext/extension.h"
#include "hphp/runtime/ext/asio/socket-event.h"
#include "hphp/runtime/ext/asio/asio-external-thread-event.h"
#include "enigma-common.h"
#include "enigma-plan.h"
#include "pgsql-connection.h"
#include "pgsql-result.h"

namespace HPHP {
namespace Enigma {

[[ noreturn ]] void throwEnigmaException(std::string const & message);

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
        kCachePlan = 0x01,
        kBinary = 0x02
    };

    Query(RawInit, String const & command);
    Query(ParameterizedInit, String const & command, Array const & params);
    Query(ParameterizedInit, String const & command, Pgsql::PreparedParameters const & params);
    Query(PrepareInit, String const & stmtName, String const & command, unsigned numParams);
    Query(PreparedInit, String const & stmtName, Array const & params);
    Query(PreparedInit, String const & stmtName, Pgsql::PreparedParameters const & params);

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

    void send(Pgsql::ConnectionResource & connection);
    Pgsql::p_ResultResource exec(Pgsql::ConnectionResource & connection);

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
        // Lower 8 bits reserved for ResultResource flags
        kResultResourceMask = 0xff,
        kBindToProperties = 0x0100,
        kIgnoreUndeclared = 0x0200,
        kAllowUndeclared = 0x0400,
        kDontCallCtor = 0x0800,
        kNumbered = 0x1000,
        kConstructBeforeBinding = 0x2000
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

void registerClasses();

}
}

#endif //HPHP_ENIGMA_QUERY_H
