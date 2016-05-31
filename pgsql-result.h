#ifndef HPHP_PGSQL_RESOURCE_H
#define HPHP_PGSQL_RESOURCE_H

#include "hphp/runtime/ext/extension.h"
#include <libpq-fe.h>
#include "enigma-common.h"

namespace HPHP {
namespace Pgsql {

class ResultResource {
public:
    enum class Status {
        EmptyQuery,     // The string sent to the server was empty
        CommandOk,      // Successful completion of a command returning no data
        TuplesOk,       // Successful completion of a command returning data
        CopyOut,        // Copy Out (from server) data transfer started
        CopyIn,         // Copy In (to server) data transfer started
        BadResponse,    // The server's response was not understood
        NonfatalError,  // A nonfatal error (a notice or warning) occurred
        FatalError,     // A fatal error occurred
        CopyBoth        // Copy In/Out (to and from server) data transfer started
    };

    enum class DiagField : int {
        // The severity; the field contents are ERROR, FATAL, or PANIC (in an error message),
        // or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message)
        Severity = PG_DIAG_SEVERITY,
        // The SQLSTATE code for the error
        SQLState = PG_DIAG_SQLSTATE,
        // The primary human-readable error message (typically one line).
        PrimaryMessage = PG_DIAG_MESSAGE_PRIMARY,
        // Detail: an optional secondary error message carrying more detail about the problem. Might run to multiple lines
        DetailMessage = PG_DIAG_MESSAGE_DETAIL,
        // Hint: an optional suggestion what to do about the problem
        HintMessage = PG_DIAG_MESSAGE_HINT,
        // A string containing a decimal integer indicating an error cursor position as an index into the original statement string.
        // The first character has index 1, and positions are measured in characters not bytes.
        StatementPosition = PG_DIAG_STATEMENT_POSITION,
        // Used when the cursor position refers to an internally generated command rather than the one submitted by the client
        InternalPosition = PG_DIAG_INTERNAL_POSITION,
        // The text of a failed internally-generated command.
        // This could be, for example, a SQL query issued by a PL/pgSQL function.
        InternalQuery = PG_DIAG_INTERNAL_QUERY,
        // An indication of the context in which the error occurred.
        // Presently this includes a call stack traceback of active procedural language functions and
        // internally-generated queries. The trace is one entry per line, most recent first.
        Context = PG_DIAG_CONTEXT,
        // The file name of the source-code location where the error was reported.
        SourceFile = PG_DIAG_SOURCE_FILE,
        // The line number of the source-code location where the error was reported.
        SourceLine = PG_DIAG_SOURCE_LINE,
        // The name of the source-code function reporting the error
        SourceFunction = PG_DIAG_SOURCE_FUNCTION
    };

    enum TypedValueOptions {
        kNativeJson = 0x01,
        kNativeArrays = 0x02,
        kNativeDateTime = 0x04,
        kAllNative = kNativeJson | kNativeArrays | kNativeDateTime,
        kNumericAsFloat = 0x08
    };

    ResultResource(PGresult *result);

    ~ResultResource();

    /**
     * Returns the result status of the command.
     */
    Status status() const;

    /**
     * Returns the error message associated with the command, or an empty string if there was no error.
     */
    std::string errorMessage() const;

    /**
     * Returns the error message associated with the command, or an empty string if there was no error.
     */
    Variant errorField(DiagField field) const;

    /**
     * Returns the number of rows (tuples) in the query result
     */
    int numTuples() const;

    /**
     * Returns the number of columns (fields) in each row of the query result.
     */
    int numFields() const;

    /**
     * Returns the column name associated with the given column number. Column numbers start at 0.
     */
    String columnName(int column) const;

    /**
     * Returns the column number associated with the given column name.
     * -1 is returned if the given name does not match any column.
     * The given name is treated like an identifier in an SQL command, that is, it is downcased unless double-quoted.
     */
    int columnNumber(String const & name) const;

    /**
     * Returns whether format of the given column is binary.
     * Column numbers start at 0.
     */
    bool columnBinary(int column) const;

    /**
     * Returns the data type associated with the given column number.
     * The integer returned is the internal OID number of the type. Column numbers start at 0.
     */
    Oid columnType(int column) const;

    /**
     * Returns the OID of the table from which the given column was fetched. Column numbers start at 0.
     * InvalidOid is returned if the column number is out of range, or
     * if the specified column is not a simple reference to a table column.
     */
    Oid columnTable(int column) const;

    /**
     * Returns a single field value of one row of the result. Row and column numbers start at 0.
     */
    String value(int row, int column) const;

    /**
     * Returns a single field value of one row of the result. Row and column numbers start at 0.
     */
    Variant typedValue(int row, int column, Oid type, unsigned flags) const;

    /**
     * Returns the number of parameters of a prepared statement.
     */
    int numParams() const;

    /**
     * Returns the data type of the indicated statement parameter. Parameter numbers start at 0.
     */
    Oid paramType(int param) const;

    int affectedRows() const;

private:
    PGresult * result_;
};

}
}

#endif //HPHP_PGSQL_RESOURCE_H
