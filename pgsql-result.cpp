#include "pgsql-result.h"
#include "pgsql-connection.h"
#include "hphp/runtime/base/array-iterator.h"
#include "hphp/runtime/ext/json/ext_json.h"
#include "hphp/runtime/ext/datetime/ext_datetime.h"

namespace HPHP {
namespace Pgsql {

/*
 * PostgreSQL OID values from server/catalog/pg_type.h
 */
enum TypeOid {
    kOidBool = 16,
    kOidBytea = 17,
    kOidChar = 18,
    kOidInt8 = 20,
    kOidInt2 = 21,
    kOidInt4 = 23,
    kOidText = 25,
    kOidOid = 26,
    kOidXid = 28,
    kOidCid = 29,
    kOidJson = 114,
    kOidXml = 142,
    kOidFloat4 = 700,
    kOidFloat8 = 701,
    kOidUnknown = 705,
    kOidInt2Array = 1005,
    kOidInt4Array = 1007,
    kOidFloat4Array = 1021,
    kOidFloat8Array = 1022,
    kOidBpchar = 1042,
    kOidVarchar = 1043,
    kOidDate = 1082,
    kOidTime = 1083,
    kOidTimestamp = 1114,
    kOidTimestamptz = 1184,
    kOidInterval = 1186,
    kOidNumeric = 1700,
    kOidUuid = 2950
};

long fast_atol(const char *str, int len) {
    long value = 0;
    long sign = 1;
    if (str[0] == '-') {
        sign = -1;
        ++str;
        --len;
    }

    switch (len) {
        case 19: value += (str[len-19] - '0') * 1000000000000000000l;
        case 18: value += (str[len-18] - '0') * 100000000000000000l;
        case 17: value += (str[len-17] - '0') * 10000000000000000l;
        case 16: value += (str[len-16] - '0') * 1000000000000000l;
        case 15: value += (str[len-15] - '0') * 100000000000000l;
        case 14: value += (str[len-14] - '0') * 10000000000000l;
        case 13: value += (str[len-13] - '0') * 1000000000000l;
        case 12: value += (str[len-12] - '0') * 100000000000l;
        case 11: value += (str[len-11] - '0') * 10000000000l;
        case 10: value += (str[len-10] - '0') * 1000000000l;
        case  9: value += (str[len- 9] - '0') * 100000000l;
        case  8: value += (str[len- 8] - '0') * 10000000l;
        case  7: value += (str[len- 7] - '0') * 1000000l;
        case  6: value += (str[len- 6] - '0') * 100000l;
        case  5: value += (str[len- 5] - '0') * 10000l;
        case  4: value += (str[len- 4] - '0') * 1000l;
        case  3: value += (str[len- 3] - '0') * 100l;
        case  2: value += (str[len- 2] - '0') * 10l;
        case  1: value += (str[len- 1] - '0');
        default: break;
    }

    return value * sign;
}


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
String ResultResource::value(int row, int column) const {
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
    } else if (columnBinary(column)) {
        return binaryValue(row, column, oid, flags);
    } else {
        return textValue(row, column, oid, flags);
    }
}

/**
 * Returns a single binary-formatted field value of one row of the result.
 * Row and column numbers start at 0.
 */
Variant ResultResource::binaryValue(int row, int column, Oid oid, uint32_t flags) const {
    auto value = PQgetvalue(result_, row, column);
    auto length = PQgetlength(result_, row, column);

    switch (oid) {
        case kOidBool:
            return Variant(value[0] == 1);

        case kOidInt2: {
            uint16_t i = __builtin_bswap16(*reinterpret_cast<uint16_t *>(value));
            return Variant(*reinterpret_cast<int16_t *>(&i));
        }

        case kOidInt4:
        case kOidOid:
        case kOidXid:
        case kOidCid: {
            uint32_t i = __builtin_bswap32(*reinterpret_cast<uint32_t *>(value));
            return Variant(*reinterpret_cast<int32_t *>(&i));
        }

        case kOidInt8: {
            uint64_t i = ((uint64_t)__builtin_bswap32(*reinterpret_cast<uint32_t *>(value)) << 32) |
                         __builtin_bswap32(*reinterpret_cast<uint32_t *>(value + 4));
            return Variant(*reinterpret_cast<int64_t *>(&i));
        }

        case kOidFloat4: {
            uint32_t i = __builtin_bswap32(*reinterpret_cast<uint32_t *>(value));
            return Variant(*reinterpret_cast<float *>(&i));
        }

        case kOidFloat8: {
            uint64_t i = ((uint64_t)__builtin_bswap32(*reinterpret_cast<uint32_t *>(value)) << 32) |
                         __builtin_bswap32(*reinterpret_cast<uint32_t *>(value + 4));
            return Variant(*reinterpret_cast<double *>(&i));
        }

        case kOidBytea:
        case kOidChar:
        case kOidText:
        case kOidXml:
        case kOidUnknown:
        case kOidBpchar:
        case kOidVarchar:
            return String(value, (size_t) length, CopyStringMode{});

        case kOidJson:
            if (flags & kNativeJson)
            {
                String json(value, (size_t) length, CopyStringMode{});
                return f_json_decode(json);
            }
            else
                return String(value, (size_t) length, CopyStringMode{});

        case kOidInt2Array:
        case kOidInt4Array:
        case kOidFloat4Array:
        case kOidFloat8Array:
        case kOidDate:
        case kOidTime:
        case kOidTimestamp:
        case kOidTimestamptz:
        case kOidInterval:
        case kOidNumeric: // TODO !!!
        default:
            throw EnigmaException(std::string("Cannot receive type using binary protocol: OID ")
                + std::to_string(oid));
    }
}

/**
 * Returns a single text-formatted field value of one row of the result.
 * Row and column numbers start at 0.
 */
Variant ResultResource::textValue(int row, int column, Oid oid, uint32_t flags) const {
    auto value = PQgetvalue(result_, row, column);
    auto length = PQgetlength(result_, row, column);

    switch (oid) {
        case kOidBool:
            return Variant(value[0] == 't');

        case kOidInt2:
        case kOidInt4:
        case kOidInt8:
        case kOidOid:
        case kOidXid:
        case kOidCid:
            return Variant(fast_atol(value, length));

        case kOidFloat4:
            return Variant(fast_atof<float>(value));

        case kOidFloat8:
            return Variant(fast_atof<double>(value));

        case kOidNumeric:
            if (flags & kNumericAsFloat)
                return Variant(fast_atof<double>(value));
            else
                return String(value, (size_t) length, CopyStringMode{});

        case kOidJson:
            if (flags & kNativeJson)
            {
                String json(value, (size_t) length, CopyStringMode{});
                return f_json_decode(json);
            }
            else
                return String(value, (size_t) length, CopyStringMode{});

        case kOidDate:
        case kOidTimestamp:
        case kOidTimestamptz:
            if (flags & kNativeDateTime)
            {
                String dt(value, (size_t) length, CopyStringMode{});
                return f_date_create(dt);
            }
            else
                return String(value, (size_t) length, CopyStringMode{});

        case kOidInt2Array:
        case kOidInt4Array:
        case kOidFloat4Array:
        case kOidFloat8Array:
        case kOidTime:
        case kOidInterval:
        default:
            return String(value, (size_t) length, CopyStringMode{});
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
