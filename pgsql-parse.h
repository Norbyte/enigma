#ifndef HPHP_PGSQL_PARSE_H
#define HPHP_PGSQL_PARSE_H

#include "hphp/runtime/ext/json/ext_json.h"
#include "hphp/runtime/ext/datetime/ext_datetime.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {
namespace Pgsql {


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
    kOidXmlArray = 143,
    kOidJsonArray = 199,
    kOidFloat4 = 700,
    kOidFloat8 = 701,
    kOidUnknown = 705,
    kOidBoolArray = 1000,
    kOidCharArray = 1002,
    kOidInt2Array = 1005,
    kOidInt4Array = 1007,
    kOidTextArray = 1009,
    kOidBpcharArray = 1014,
    kOidVarcharArray = 1015,
    kOidInt8Array = 1016,
    kOidFloat4Array = 1021,
    kOidFloat8Array = 1022,
    kOidBpchar = 1042,
    kOidVarchar = 1043,
    kOidDate = 1082,
    kOidTime = 1083,
    kOidTimestamp = 1114,
    kOidTimestampArray = 1115,
    kOidDateArray = 1182,
    kOidTimestamptz = 1184,
    kOidTimestamptzArray = 1185,
    kOidInterval = 1186,
    kOidNumericArray = 1231,
    kOidNumeric = 1700,
    kOidUuid = 2950
};

inline int16_t parseInt16(const char * value)
{
    uint16_t i = __builtin_bswap16(*reinterpret_cast<uint16_t const *>(value));
    return *reinterpret_cast<int16_t *>(&i);
}

inline int32_t parseInt32(const char * value)
{
    uint32_t i = __builtin_bswap32(*reinterpret_cast<uint32_t const *>(value));
    return *reinterpret_cast<int32_t *>(&i);
}

inline int64_t parseInt64(const char * value)
{
    uint64_t i = ((uint64_t)__builtin_bswap32(*reinterpret_cast<uint32_t const *>(value)) << 32) |
                 __builtin_bswap32(*reinterpret_cast<uint32_t const *>(value + 4));
    return *reinterpret_cast<int64_t *>(&i);
}

inline float parseFloat32(const char * value)
{
    uint32_t i = __builtin_bswap32(*reinterpret_cast<uint32_t const *>(value));
    return *reinterpret_cast<float *>(&i);
}

inline double parseFloat64(const char * value)
{
    uint64_t i = ((uint64_t)__builtin_bswap32(*reinterpret_cast<uint32_t const *>(value)) << 32) |
                 __builtin_bswap32(*reinterpret_cast<uint32_t const *>(value + 4));
    return *reinterpret_cast<double *>(&i);
}

template<Oid Ty, bool Binary>
Variant parseValue(const char * value, int length, uint32_t flags) = delete;

#define PG_BINARY_PARSER(ty) template<> \
    inline Variant parseValue<kOid##ty, true>(const char * value, int length, uint32_t flags)

#define PG_TEXT_PARSER(ty) template<> \
    inline Variant parseValue<kOid##ty, false>(const char * value, int length, uint32_t flags)

#define PG_PARSE_STRING { return String(value, (size_t) length, CopyStringMode{}); }


/******************************************************************
 *
 *                   BINARY PROTOCOL PARSERS
 *
 ******************************************************************/


PG_BINARY_PARSER(Bool)
{
    return Variant(value[0] == 1);
}

PG_BINARY_PARSER(Int2)
{
    return Variant(parseInt16(value));
}

PG_BINARY_PARSER(Int4)
{
    return Variant(parseInt32(value));
}

PG_BINARY_PARSER(Oid)
{
    return Variant(parseInt32(value));
}

PG_BINARY_PARSER(Xid)
{
    return Variant(parseInt32(value));
}

PG_BINARY_PARSER(Cid)
{
    return Variant(parseInt32(value));
}

PG_BINARY_PARSER(Int8)
{
    return Variant(parseInt64(value));
}

PG_BINARY_PARSER(Float4)
{
    return Variant(parseFloat32(value));
}

PG_BINARY_PARSER(Float8)
{
    return Variant(parseFloat64(value));
}

const StaticString
    s_DateTimeFormat("U.u"),
    s_DateFormat("Y-m-d H:i:s");

inline Variant parseBinaryTimestamp(const char * value, uint32_t flags)
{
    int64_t time = parseInt64(value);
    // Number of microseconds between 1970-01-01 and 2000-01-01
    time += 946684800000000l;

    // Convert to "epoch.microseconds" format
    char ts[32];
    sprintf(ts, "%ld.%ld", time / 1000000, time % 1000000);
    String tss(ts, CopyStringMode{});
    auto cls = DateTimeData::getClass();
    Variant dt = HHVM_STATIC_MN(DateTime, createFromFormat)(
            cls, s_DateTimeFormat, tss, init_null_variant);
    if (!dt.isObject()) {
        return init_null_variant;
    }

    Object datetime(dt.toObject());
    if (flags & ResultResource::kNativeDateTime) {
        return datetime;
    } else {
        return HHVM_MN(DateTime, format)(datetime.get(), s_DateFormat);
    }
}


void
j2date(int jd, int *year, int *month, int *day)
{
    unsigned int julian;
    unsigned int quad;
    unsigned int extra;
    int         y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;

    return;
}   /* j2date() */

const unsigned POSTGRES_EPOCH_JDATE = 2451545;

PG_BINARY_PARSER(Date)
{
    int32_t date = parseInt32(value);
    int year, month, day;
    j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

    // Convert to "epoch.microseconds" format
    char ts[64];
    sprintf(ts, "%d-%02d-%02d", year, month, day);
    String tss(ts, CopyStringMode{});

    if (flags & ResultResource::kNativeDateTime) {
        auto cls = DateTimeData::getClass();
        return HHVM_STATIC_MN(DateTime, createFromFormat)(
                cls, s_DateTimeFormat, tss, init_null_variant);
    } else {
        return tss;
    }
}

PG_BINARY_PARSER(Timestamp)
{
    return parseBinaryTimestamp(value, flags);
}

PG_BINARY_PARSER(Timestamptz)
{
    // TODO: timezone information is lost when receiving TIMESTAMPTZ
    // using the binary protocol
    return parseBinaryTimestamp(value, flags);
}

PG_BINARY_PARSER(Bytea) PG_PARSE_STRING
PG_BINARY_PARSER(Char) PG_PARSE_STRING
PG_BINARY_PARSER(Text) PG_PARSE_STRING
PG_BINARY_PARSER(Xml) PG_PARSE_STRING
PG_BINARY_PARSER(Unknown) PG_PARSE_STRING
PG_BINARY_PARSER(Bpchar) PG_PARSE_STRING
PG_BINARY_PARSER(Varchar) PG_PARSE_STRING

PG_BINARY_PARSER(Json)
{
    if (flags & ResultResource::kNativeJson) {
        String json(value, (size_t) length, CopyStringMode{});
        return f_json_decode(json);
    } else
        PG_PARSE_STRING;
}




/******************************************************************
 *
 *                    TEXT PROTOCOL PARSERS
 *
 ******************************************************************/

PG_TEXT_PARSER(Bool)
{
    return Variant(value[0] == 't');
}

PG_TEXT_PARSER(Int2)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Int4)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Oid)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Xid)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Cid)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Int8)
{
    return Variant(fast_atol(value, length));
}

PG_TEXT_PARSER(Float4)
{
    return Variant(atof(value));
}

PG_TEXT_PARSER(Float8)
{
    return Variant(atof(value));
}

PG_TEXT_PARSER(Numeric)
{
    if (flags & ResultResource::kNumericAsFloat)
        return Variant(atof(value));
    else
        PG_PARSE_STRING
}

PG_TEXT_PARSER(Json)
{
    if (flags & ResultResource::kNativeJson) {
        String json(value, (size_t) length, CopyStringMode{});
        return f_json_decode(json);
    } else
        PG_PARSE_STRING
}

PG_TEXT_PARSER(Date)
{
    if (flags & ResultResource::kNativeDateTime) {
        String dt(value, (size_t) length, CopyStringMode{});
        return HHVM_FN(date_create)(dt);
    } else
        PG_PARSE_STRING
}

PG_TEXT_PARSER(Timestamp)
{
    if (flags & ResultResource::kNativeDateTime) {
        String dt(value, (size_t) length, CopyStringMode{});
        return HHVM_FN(date_create)(dt);
    } else
        PG_PARSE_STRING
}

PG_TEXT_PARSER(Timestamptz)
{
    if (flags & ResultResource::kNativeDateTime) {
        String dt(value, (size_t) length, CopyStringMode{});
        return f_date_create(dt);
    } else
        PG_PARSE_STRING
}

#undef PG_BINARY_PARSER
#undef PG_TEXT_PARSER

inline Variant parseBinaryValueOid(const char * value, int length, Oid oid, uint32_t flags);

inline Variant parseBinaryArray(const char * value, int length, uint32_t flags)
{
    if (length < 12) {
        throw EnigmaException("Not enough bytes for headers");
    }

    int32_t dimensions = parseInt32(value + 0);
    /*int32_t hasNull =*/ parseInt32(value + 4);
    int32_t elementOid = parseInt32(value + 8);
    value += 12;
    length -= 12;

    if (dimensions == 0) {
        return Array::Create();
    } else if (dimensions > 1) {
        throw EnigmaException("Only 1-dimensional arrays are supported");
    }

    if (length < 8) {
        throw EnigmaException("Not enough bytes for dimension information");
    }

    int32_t count = parseInt32(value + 0);
    int32_t leftBound = parseInt32(value + 4);
    // pgsql array numbering starts from 1
    leftBound--;
    value += 8;
    length -= 8;

    Array arr{Array::Create()};
    for (int i = 0; i < count; i++) {
        if (length < 4) {
            throw EnigmaException("Not enough bytes for element length");
        }

        int32_t elementLength = parseInt32(value);
        value += 4;
        length -= 4;
        if (elementLength == -1) {
            // Null value
            arr.set(i + leftBound, init_null());
        } else if (elementLength < 0) {
            throw EnigmaException("Invalid element length");
        } else if (length < elementLength) {
            throw EnigmaException("Not enough bytes for element data");
        } else {
            arr.set(i + leftBound, parseBinaryValueOid(value, elementLength, elementOid, flags));
            value += elementLength;
            length -= elementLength;
        }
    }

    if (length) {
        throw EnigmaException("Stray data at end of array");
    }

    return arr;
}

#define HANDLE_ARRAY(ty) case kOid##ty##Array:  \
    if (flags & ResultResource::kNativeArrays) { \
        return parseBinaryArray(value, length, flags); \
    } else { \
        throw EnigmaException(std::string("Cannot fetch array type as string when using binary protocol: OID ") \
                              + std::to_string(oid)); \
    }

#define HANDLE_TYPE(ty) case kOid##ty: return parseValue<kOid##ty, true>(value, length, flags);
inline Variant parseBinaryValueOid(const char * value, int length, Oid oid, uint32_t flags)
{
    switch (oid) {
        HANDLE_TYPE(Bool)
        HANDLE_TYPE(Int2)
        HANDLE_TYPE(Int4)
        HANDLE_TYPE(Oid)
        HANDLE_TYPE(Xid)
        HANDLE_TYPE(Cid)
        HANDLE_TYPE(Int8)
        HANDLE_TYPE(Float4)
        HANDLE_TYPE(Float8)
        HANDLE_TYPE(Date)
        HANDLE_TYPE(Timestamp)
        HANDLE_TYPE(Timestamptz)

        HANDLE_TYPE(Bytea)
        HANDLE_TYPE(Char)
        HANDLE_TYPE(Text)
        HANDLE_TYPE(Xml)
        HANDLE_TYPE(Unknown)
        HANDLE_TYPE(Bpchar)
        HANDLE_TYPE(Varchar)
        HANDLE_TYPE(Json)

        HANDLE_ARRAY(Bool)
        HANDLE_ARRAY(Int2)
        HANDLE_ARRAY(Int4)
        HANDLE_ARRAY(Int8)
        HANDLE_ARRAY(Float4)
        HANDLE_ARRAY(Float8)
        HANDLE_ARRAY(Numeric)
        HANDLE_ARRAY(Json)
        HANDLE_ARRAY(Date)
        HANDLE_ARRAY(Timestamp)
        HANDLE_ARRAY(Timestamptz)

        HANDLE_ARRAY(Xml)
        HANDLE_ARRAY(Char)
        HANDLE_ARRAY(Text)
        HANDLE_ARRAY(Bpchar)
        HANDLE_ARRAY(Varchar)

        default:
            throw EnigmaException(std::string("Cannot receive type using binary protocol: OID ")
                                  + std::to_string(oid));
    }
}
#undef HANDLE_TYPE
#undef HANDLE_ARRAY

inline Variant parseTextValueOid(const char * value, int length, Oid oid, uint32_t flags);

inline Variant parseTextArray(const char * value, int length, Oid elementOid, uint32_t flags)
{
    auto valueStart = value;
    if (length < 2) {
        throw EnigmaException(std::string("Array literal has illegal length: ") + valueStart);
    }

    if (*value != '{') {
        throw EnigmaException(std::string("Array literal must begin with '{': ") + valueStart);
    }

    value++;
    length--;

    Array arr{Array::Create()};
    while (*value != '}') {
        if (*value == '"') {
            value++;
            length--;

            // TODO: will not be efficient for large arrays!
            String lit(length, ReserveStringMode{});
            int litLength = 0;
            char * litMut = lit.mutableData();

            while (*value != '"') {
                if (*value == '\\') {
                    litMut[litLength++] = value[1];
                    value += 2;
                    length -= 2;
                } else {
                    litMut[litLength++] = *value;
                    value++;
                    length--;
                }
            }

            arr.append(parseTextValueOid(lit.c_str(), litLength, elementOid, flags));

            value++;
            length--;
        } else {
            auto start = value;
            while (*value != ',' && *value != '}') {
                value++;
                length--;
            }

            if (start != value) {
                if (memcmp(start, "NULL", 4) == 0) {
                    arr.append(Variant(Variant::NullInit{}));
                } else {
                    arr.append(parseTextValueOid(start, value - start, elementOid, flags));
                }
            } else {
                throw EnigmaException(std::string("Unexpected zero length element in array: ") + valueStart);
            }
        }

        if (*value == ',') {
            value++;
            length--;
        } else if (*value != '}') {
            throw EnigmaException(std::string("Expected comma after end of element: ") + valueStart);
        }
    }

    if (length != 1) {
        throw EnigmaException(std::string("Stray data at end of array: ") + valueStart);
    }

    return arr;
}

#define HANDLE_ARRAY(ty) case kOid##ty##Array: \
    if (flags & ResultResource::kNativeArrays) { \
        return parseTextArray(value, length, kOid##ty, flags); \
    } else { \
        return String(value, (size_t) length, CopyStringMode{}); \
    }

#define HANDLE_TYPE(ty) case kOid##ty: return parseValue<kOid##ty, false>(value, length, flags);
inline Variant parseTextValueOid(const char * value, int length, Oid oid, uint32_t flags)
{
    switch (oid) {
        HANDLE_TYPE(Bool)
        HANDLE_TYPE(Int2)
        HANDLE_TYPE(Int4)
        HANDLE_TYPE(Oid)
        HANDLE_TYPE(Xid)
        HANDLE_TYPE(Cid)
        HANDLE_TYPE(Int8)
        HANDLE_TYPE(Float4)
        HANDLE_TYPE(Float8)
        HANDLE_TYPE(Numeric)
        HANDLE_TYPE(Json)
        HANDLE_TYPE(Date)
        HANDLE_TYPE(Timestamp)
        HANDLE_TYPE(Timestamptz)

        HANDLE_ARRAY(Bool)
        HANDLE_ARRAY(Int2)
        HANDLE_ARRAY(Int4)
        HANDLE_ARRAY(Int8)
        HANDLE_ARRAY(Float4)
        HANDLE_ARRAY(Float8)
        HANDLE_ARRAY(Numeric)
        HANDLE_ARRAY(Json)
        HANDLE_ARRAY(Date)
        HANDLE_ARRAY(Timestamp)
        HANDLE_ARRAY(Timestamptz)

        HANDLE_ARRAY(Xml)
        HANDLE_ARRAY(Char)
        HANDLE_ARRAY(Text)
        HANDLE_ARRAY(Bpchar)
        HANDLE_ARRAY(Varchar)

        default: return String(value, (size_t) length, CopyStringMode{});
    }
}
#undef HANDLE_TYPE

#undef PG_PARSE_STRING

}
}

#endif
