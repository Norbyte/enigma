#include "pgsql-result.h"
#include "pgsql-connection.h"
#include "hphp/runtime/base/array-iterator.h"
#include "pgsql-parse.h"

namespace HPHP {
namespace Pgsql {


ResultResource::ResultResource(PGresult *result)
        : result_{result} { }

ResultResource::~ResultResource() {
    if (result_) {
        PQclear(result_);
    }
}

/**
 * Returns the result status of the command.
 */
ResultResource::Status ResultResource::status() const {
    auto status = PQresultStatus(result_);
    switch (status) {
        case PGRES_EMPTY_QUERY:    return Status::EmptyQuery;
        case PGRES_COMMAND_OK:     return Status::CommandOk;
        case PGRES_TUPLES_OK:      return Status::TuplesOk;
        case PGRES_COPY_IN:        return Status::CopyIn;
        case PGRES_COPY_OUT:       return Status::CopyOut;
        case PGRES_BAD_RESPONSE:   return Status::BadResponse;
        case PGRES_NONFATAL_ERROR: return Status::NonfatalError;
        case PGRES_FATAL_ERROR:    return Status::FatalError;
        case PGRES_COPY_BOTH:      return Status::CopyBoth;
        default:
            throw EnigmaException(std::string("Unknown result status returned: ") + PQresStatus(status));
    }
}

/**
 * Returns the error message associated with the command, or an empty string if there was no error.
 */
std::string ResultResource::errorMessage() const {
    auto message = PQresultErrorMessage(result_);
    if (message == nullptr) {
        return std::string();
    } else {
        return message;
    }
}

/**
 * Returns the error message associated with the command, or an empty string if there was no error.
 */
Variant ResultResource::errorField(DiagField field) const {
    auto value = PQresultErrorField(result_, (int) field);
    if (value == nullptr) {
        return Variant(Variant::NullInit{});
    } else {
        return String(value);
    }
}

/**
 * Returns the number of rows (tuples) in the query result
 */
int ResultResource::numTuples() const {
    return PQntuples(result_);
}

/**
 * Returns the number of columns (fields) in each row of the query result.
 */
int ResultResource::numFields() const {
    return PQnfields(result_);
}

/**
 * Returns the column name associated with the given column number. Column numbers start at 0.
 */
String ResultResource::columnName(int column) const {
    auto name = PQfname(result_, column);
    if (name == nullptr) {
        throw EnigmaException("Column name requested for out-of-range column number");
    }

    return String(name);
}

/**
 * Returns the column number associated with the given column name.
 * -1 is returned if the given name does not match any column.
 * The given name is treated like an identifier in an SQL command, that is, it is downcased unless double-quoted.
 */
int ResultResource::columnNumber(String const & name) const {
    return PQfnumber(result_, name.c_str());
}

/**
 * Returns whether format of the given column is binary.
 * Column numbers start at 0.
 */
bool ResultResource::columnBinary(int column) const {
    return PQfformat(result_, column) == 1;
}

/**
 * Returns the data type associated with the given column number.
 * The integer returned is the internal OID number of the type. Column numbers start at 0.
 */
Oid ResultResource::columnType(int column) const {
    return PQftype(result_, column);
}

/**
 * Returns the OID of the table from which the given column was fetched. Column numbers start at 0.
 * InvalidOid is returned if the column number is out of range, or
 * if the specified column is not a simple reference to a table column.
 */
Oid ResultResource::columnTable(int column) const {
    return PQftable(result_, column);
}

/**
 * Returns a single field value of one row of the result. Row and column numbers start at 0.
 */
Variant ResultResource::value(int row, int column) const {
    if (PQgetisnull(result_, row, column) == 1) {
        return Variant(Variant::NullInit{});
    } else {
        auto value = PQgetvalue(result_, row, column);
        auto length = PQgetlength(result_, row, column);
        return String(value, (size_t) length, CopyStringMode{});
    }
}

/**
 * Returns a single field value of one row of the result. Row and column numbers start at 0.
 */
Variant ResultResource::typedValue(int row, int column, Oid oid, uint32_t flags) const {
    if (PQgetisnull(result_, row, column) == 1) {
        return Variant(Variant::NullInit{});
    } else {
        auto value = PQgetvalue(result_, row, column);
        auto length = PQgetlength(result_, row, column);

        if (columnBinary(column)) {
            return parseBinaryValueOid(value, length, oid, flags);
        } else {
            return parseTextValueOid(value, length, oid, flags);
        }
    }
}

/**
 * Returns the number of parameters of a prepared statement.
 */
int ResultResource::numParams() const {
    return PQnparams(result_);
}

/**
 * Returns the data type of the indicated statement parameter. Parameter numbers start at 0.
 */
Oid ResultResource::paramType(int param) const {
    return PQparamtype(result_, param);
}

int ResultResource::affectedRows() const {
    auto tuples = PQcmdTuples(result_);
    if (tuples != nullptr && tuples[0]) {
        return atoi(tuples);
    } else {
        return 0;
    }
}


}
}
