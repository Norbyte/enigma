FIND_PATH(PGSQL_INCLUDE_DIR NAMES libpq-fe.h
        PATHS /usr/include /usr/include/postgresql /usr/local/include /usr/local/include/postgresql /usr/include/pgsql)

FIND_LIBRARY(PGSQL_LIBRARY NAMES pq PATHS /lib /usr/lib /usr/local/lib)

IF (PGSQL_INCLUDE_DIR AND PGSQL_LIBRARY)
    MESSAGE(STATUS "Found PostgreSQL: ${PGSQL_INCLUDE_DIR} ${PGSQL_LIBRARY}")
ELSE()
    MESSAGE(FATAL_ERROR "Could NOT find PostgreSQL library")
ENDIF()

include_directories(${PGSQL_INCLUDE_DIR})

# add_definitions(-DENIGMA_DEBUG)

HHVM_EXTENSION(enigma ext_enigma.cpp pgsql-connection.cpp pgsql-result.cpp enigma-plan.cpp enigma-query.cpp enigma-queue.cpp enigma-transaction.cpp)
HHVM_SYSTEMLIB(enigma ext_enigma.php)

target_link_libraries(enigma ${PGSQL_LIBRARY})
