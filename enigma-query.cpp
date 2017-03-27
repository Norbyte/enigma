#include "enigma-query.h"
#include "hphp/runtime/vm/native-data.h"
#include "hphp/runtime/base/execution-context.h"

namespace HPHP {
namespace Enigma {

void throwEnigmaException(std::string const & message) {
    auto error = ErrorResult::newInstance(message);
    throw error;
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

void Query::send(Pgsql::ConnectionResource & connection) {
    bool binary = (flags() & kBinary) == kBinary;
    switch (type()) {
        case Query::Type::Raw:
            connection.sendQuery(command());
            break;

        case Query::Type::Parameterized:
            if (params().count() == 0) {
                connection.sendQuery(command());
            } else {
                connection.sendQueryParams(command(), params(), binary);
            }

            break;

        case Query::Type::Prepare:
            connection.sendPrepare(statement(), command(), numParams());
            break;

        case Query::Type::Prepared:
            connection.sendQueryPrepared(statement(), params(), binary);
            break;
    }
}

Pgsql::p_ResultResource Query::exec(Pgsql::ConnectionResource & connection) {
    bool binary = (flags() & kBinary) == kBinary;
    switch (type()) {
        case Query::Type::Raw:
            return connection.query(command());

        case Query::Type::Parameterized:
            if (params().count() == 0 && !binary) {
                return connection.query(command());
            } else {
                return connection.queryParams(command(), params(), binary);
            }

        case Query::Type::Prepare:
            return connection.prepare(statement(), command(), numParams());

        case Query::Type::Prepared:
            return connection.queryPrepared(statement(), params(), binary);

        default:
            throw std::runtime_error("Invalid query type");
    }
}


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
    try {
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
            if (numbered) {
                // Fetch arrays with 0, 1, ..., n as keys
                for (auto col = 0; col < cols; col++) {
                    rowArr.set(col, resource.typedValue(row, col, colTypes[col], valueFlags));
                }
            } else {
                // Fetch arrays with column names as keys
                for (auto col = 0; col < cols; col++) {
                    rowArr.set(colNames[col], resource.typedValue(row, col, colTypes[col], valueFlags));
                }
            }

            results.append(rowArr);
        }

        return results;
    } catch (EnigmaException & e) {
        throwEnigmaException(e.what());
    }
}


Array HHVM_METHOD(QueryResult, fetchObjects, String const & cls, int64_t flags, Array const & args) {
    auto rowClass = Unit::getClass(cls.get(), true);
    if (rowClass == nullptr) {
        throwEnigmaException(std::string("Could not find result row class: ") + cls.c_str());
    }

    try {
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
                    g_context->invokeFunc(ctor, args, rowObj.get());
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
                    g_context->invokeFunc(ctor, args, rowObj.get());
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
                    g_context->invokeFunc(ctor, args, rowObj.get());
                }

                for (auto col = 0; col < cols; col++) {
                    rowObj->o_set(colNames[col], resource.typedValue(row, col, colTypes[col], valueFlags));
                }

                if (!constructBeforeBind && callCtor) {
                    g_context->invokeFunc(ctor, args, rowObj.get());
                }

                results.append(rowObj);
            }
        }

        return results;
    } catch (EnigmaException & e) {
        throwEnigmaException(e.what());
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
