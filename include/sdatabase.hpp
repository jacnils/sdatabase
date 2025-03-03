/* sdatabase - Simple database abstraction for SQLite3 and PostgreSQL
 * Licensed under the MIT license
 * Copyright (c) 2024-2025 Jacob Nilsson
 */

#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <stdexcept>
#include <cstdint>

#ifndef SDB_SQLITE3
#ifndef SDB_POSTGRESQL
#define SDB_SQLITE3
#endif
#endif
#ifndef SDB_ENABLE_ICONV
#define SDB_ENABLE_ICONV
#endif

#ifdef SDB_SQLITE3
#include <sqlite3.h>
#endif
#ifdef SDB_POSTGRESQL
#include <libpq-fe.h>
#endif

#ifdef SDB_ENABLE_ICONV
#include <iconv.h>
#endif

/**
 * @brief Namespace for database related functions and classes.
 */
namespace sdatabase {
#ifdef SDB_SQLITE3
    /**
     * @brief Temporary storage for data. Do not use this directly.
     */
    inline std::vector<std::unordered_map<std::string, std::string>> tmp{};
    /**
     * @brief Callback function for sqlite3_exec.
     *
     * @param data Pointer to data.
     * @param argc Number of columns.
     * @param argv Column values.
     * @param name Column names.
     * @return int 0.
     */
    int callback(void* data, int argc, char** argv, char** name);
#endif
#ifdef SDB_SQLITE3
    /**
     * @brief Class for database operations.
     */
    class SQLite3Database {
        sqlite3* sqlite3_db{};
        std::string database{};
        bool is_good{false};

        template<typename T, typename... Args>
        void bind_parameters(sqlite3_stmt* stmt, int index, T value, Args... args) {
            if constexpr (std::is_same_v<T, int>) {
                bind_parameter(stmt, index, value);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                bind_parameter(stmt, index, value);
            } else if constexpr (std::is_same_v<T, double>) {
                bind_parameter(stmt, index, value);
            } else if constexpr (std::is_same_v<T, std::string>) {
                bind_parameter(stmt, index, value);
            } else if constexpr (std::is_same_v<T, const char*>) {
                bind_parameter(stmt, index, value);
            }
            bind_parameters(stmt, index + 1, args...);
        }

        std::string remove_non_utf8(const std::string& input) {
#ifdef SDB_ENABLE_ICONV
            iconv_t cd = iconv_open("UTF-8//IGNORE", "UTF-8");
            if (cd == (iconv_t)-1) {
                throw std::runtime_error("iconv_open failed");
            }

            std::vector<char> output(input.size() * 2);
            char* inbuf = const_cast<char*>(input.data());
            size_t inbytesleft = input.size();
            char* outbuf = output.data();
            size_t outbytesleft = output.size();

            while (inbytesleft > 0) {
                size_t result = iconv(cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
                if (result == (size_t)-1) {
                    if (errno == EILSEQ || errno == EINVAL) {
                        ++inbuf;
                        --inbytesleft;
                    } else if (errno == E2BIG) {
                        size_t used = output.size() - outbytesleft;
                        output.resize(output.size() * 2);
                        outbuf = output.data() + used;
                        outbytesleft = output.size() - used;
                    } else {
                        iconv_close(cd);
                        return "";
                    }
                }
            }

            iconv_close(cd);
            return std::string(output.data(), output.size() - outbytesleft);
#else
            return input;
#endif
        }

        void bind_parameters(sqlite3_stmt* stmt, int index) {}

        void bind_parameter(sqlite3_stmt* stmt, int index, int value) {
#ifdef SDB_ENABLE_PRINTDEBUG
            std::cerr << "Binding int: " << value << " to index: " << index << "\n";
#endif
            sqlite3_bind_int(stmt, index, value);
        }

        void bind_parameter(sqlite3_stmt* stmt, int index, int64_t value) {
#ifdef SDB_ENABLE_PRINTDEBUG
            std::cerr << "Binding int: " << value << " to index: " << index << "\n";
#endif
            sqlite3_bind_int64(stmt, index, value);
        }

        void bind_parameter(sqlite3_stmt* stmt, int index, double value) {
#ifdef SDB_ENABLE_PRINTDEBUG
            std::cerr << "Binding double: " << value << " to index: " << index << "\n";
#endif
            sqlite3_bind_double(stmt, index, value);
        }

        void bind_parameter(sqlite3_stmt* stmt, int index, const std::string& value) {
#ifdef SDB_ENABLE_PRINTDEBUG
            std::cerr << "Binding string: " << value << " to index: " << index << "\n";
#endif
            sqlite3_bind_text(stmt, index, remove_non_utf8(value).c_str(), -1, SQLITE_TRANSIENT);
        }

        void bind_parameter(sqlite3_stmt* stmt, int index, const char* value) {
#ifdef SDB_ENABLE_PRINTDEBUG
            std::cerr << "Binding string: " << value << " to index: " << index << "\n";
#endif
            sqlite3_bind_text(stmt, index, remove_non_utf8(value).c_str(), -1, SQLITE_TRANSIENT);
        }

        public:
            template <typename... Args>
            bool exec(const std::string& query, Args... args) {
                static_assert(sizeof...(args) > 0, "exec() requires more parameters");
                sqlite3_stmt* stmt;

                std::string nq{};
                for (size_t i = 0; i < query.size(); ++i) {
                    if (query[i] == '$' && i + 1 < query.size() && isdigit(query[i + 1])) {
                        nq += '?';
                        while (i + 1 < query.size() && isdigit(query[i + 1])) {
                            ++i;
                        }
                    } else {
                        nq += query[i];
                    }
                }

                if (sqlite3_prepare_v2(sqlite3_db, nq.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
                    std::cerr << "Failed to prepare statement\n";
                    return false;
                }

                bind_parameters(stmt, 1, args...);

                if (sqlite3_step(stmt) != SQLITE_DONE) {
                    std::cerr << "Failed to step statement" << sqlite3_errmsg(sqlite3_db) << "\n";
                    return false;
                }

                sqlite3_finalize(stmt);

                return true;
            }
            template <typename... Args>
            std::vector<std::unordered_map<std::string, std::string>> query(const std::string& query, Args... args) {
                static_assert(sizeof...(args) > 0, "query() requires more parameters");
                if (!this->is_good) {
                    return {};
                }

                std::string nq{};
                for (size_t i = 0; i < query.size(); ++i) {
                    if (query[i] == '$' && i + 1 < query.size() && isdigit(query[i + 1])) {
                        nq += '?';
                        while (i + 1 < query.size() && isdigit(query[i + 1])) {
                            ++i;
                        }
                    } else {
                        nq += query[i];
                    }
                }

                sqlite3_stmt* stmt;
                if (sqlite3_prepare_v2(sqlite3_db, nq.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
                    return {};
                }

                bind_parameters(stmt, 1, args...);

                std::vector<std::unordered_map<std::string, std::string>> result;
                int status;
                while ((status = sqlite3_step(stmt)) == SQLITE_ROW) {
                    std::unordered_map<std::string, std::string> row;
                    for (int i = 0; i < sqlite3_column_count(stmt); ++i) {
                        row[sqlite3_column_name(stmt, i)] = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                    }
                    result.push_back(std::move(row));
                }

                sqlite3_finalize(stmt);
                return result;
            }
            /**
             * @brief Query the database, returning data.
             * @param query Query to execute.
             * @return std::vector<std::unordered_map<std::string, std::string>> Data.
             */
            std::vector<std::unordered_map<std::string, std::string>> query(const std::string& query);
            /**
             * @brief Execute an SQL command.
             * @param query Query to execute.
             * @return bool True if successful.
             */
            bool exec(const std::string& query);
            /**
             * @brief Check if the database is good.
             * @return bool True if good.
             */
            bool good();
            /**
             * @brief Check if the database is open.
             * @return bool True if open.
             */
            bool is_open();
            /**
             * @brief Open a database from file.
             * @param database Database to open.
             */
            void open(const std::string& database);
            /**
             * @brief Close the open database.
             */
            void close();
            /**
             * @brief Check if the database is empty.
             * @return bool
             */
            bool empty();
            /**
             * @brief Validate an SQL statement.
             * @param query Query to validate.
             * @return bool True if valid.
             */
            bool validate(const std::string& query);
            /**
             * @brief Get the last insertion.
             * @return std::int64_t Last insertion.
             */
            std::int64_t get_last_insertion();
            /**
             * @brief Constructor.
             */
            SQLite3Database();
            /**
             * @brief Constructor.
             * @param database Database to open.
             */
            SQLite3Database(const std::string& database);
            /**
             * @brief Destructor.
             */
            ~SQLite3Database();
    };
#endif

#ifdef SDB_POSTGRESQL
    class PostgreSQLDatabase {
            PGconn* pg_conn{};
            std::string host{};
            std::string user{};
            std::string password{};
            std::string database{};
            bool is_good{false};
            int port{5432};

            template <typename T>
            std::string to_string(const T& value) {
                if constexpr (std::is_same_v<T, std::string>) {
                    return value;
                } else if constexpr (std::is_same_v<T, const char*>) {
                    return std::string(value);
                } else {
                    return std::to_string(value);
                }
            }

        std::string remove_non_utf8(const std::string& input) {
#ifdef SDB_ENABLE_ICONV
                iconv_t cd = iconv_open("UTF-8//IGNORE", "UTF-8");
                if (cd == (iconv_t)-1) {
                    throw std::runtime_error("iconv_open failed");
                }

                std::vector<char> output(input.size() * 2);
                char* inbuf = const_cast<char*>(input.data());
                size_t inbytesleft = input.size();
                char* outbuf = output.data();
                size_t outbytesleft = output.size();

                while (inbytesleft > 0) {
                    size_t result = iconv(cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
                    if (result == (size_t)-1) {
                        if (errno == EILSEQ || errno == EINVAL) {
                            ++inbuf;
                            --inbytesleft;
                        } else if (errno == E2BIG) {
                            size_t used = output.size() - outbytesleft;
                            output.resize(output.size() * 2);
                            outbuf = output.data() + used;
                            outbytesleft = output.size() - used;
                        } else {
                            iconv_close(cd);
                            return "";
                        }
                    }
                }

                iconv_close(cd);
                return std::string(output.data(), output.size() - outbytesleft);
#else
                return input;
#endif
            }
        public:
            template <typename... Args>
            bool exec(const std::string& query, Args... args) {
                static_assert(sizeof...(args) > 0, "exec() requires more parameters");
                if (!this->is_good) {
                    return false;
                }

                int i{1};
                std::string nq{};
                for (char ch : query) {
                    if (ch == '?') {
                        nq += "$" + std::to_string(i++);
                    } else {
                        nq += ch;
                    }
                }

                std::vector<std::string> str{remove_non_utf8(to_string(args))...};
                std::vector<const char*> param_v{};
                for (const std::string& s : str) {
#ifdef SDB_ENABLE_PRINTDEBUG
                    std::cerr << "Binding string: " << s << "\n";
#endif
                    param_v.push_back(s.c_str());
                }

                PGresult* res = PQexecParams(pg_conn, nq.c_str(), param_v.size(), nullptr, param_v.data(), nullptr, nullptr, 0);

                if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                    PQclear(res);
                    return false;
                }

                PQclear(res);
                return true;
            }

            template <typename... Args>
            std::vector<std::unordered_map<std::string, std::string>> query(const std::string& query, Args... args) {
                static_assert(sizeof...(args) > 0, "query() requires more parameters");
                if (!this->is_good) {
                    return {};
                }

                int n{1};
                std::string nq{};
                for (char ch : query) {
                    if (ch == '?') {
                        nq += "$" + std::to_string(n++);
                    } else {
                        nq += ch;
                    }
                }

                std::vector<std::string> str{to_string(args)...};
                std::vector<const char*> param_v{};
                for (const std::string& s : str) {
                    param_v.push_back(s.c_str());
                }

                PGresult* res = PQexecParams(pg_conn, nq.c_str(), param_v.size(), nullptr, param_v.data(), nullptr, nullptr, 0);

                if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                    PQclear(res);
                    return {};
                }

                std::vector<std::unordered_map<std::string, std::string>> result;
                int nrows = PQntuples(res);
                int nfields = PQnfields(res);

                for (int i = 0; i < nrows; ++i) {
                    std::unordered_map<std::string, std::string> row;
                    for (int j = 0; j < nfields; ++j) {
                        row[PQfname(res, j)] = PQgetvalue(res, i, j);
                    }
                    result.push_back(std::move(row));
                }

                PQclear(res);
                return result;
            }
            std::vector<std::unordered_map<std::string, std::string>> query(const std::string& query);
            bool exec(const std::string& query);
            bool good();
            bool is_open();
            void open(const std::string& host, const std::string& user, const std::string& password, const std::string& database, int port=5432);
            void close();
            bool empty();
            bool validate(const std::string& query);
            std::int64_t get_last_insertion();
            PostgreSQLDatabase() = default;
            PostgreSQLDatabase(const std::string& host, const std::string& user, const std::string& password, const std::string& database, int port=5432);
            ~PostgreSQLDatabase();
    };
#endif
}

#ifdef SDB_SQLITE3
inline int sdatabase::callback(void* data, int argc, char** argv, char** name) {
    (void)data;

    std::unordered_map<std::string, std::string> map{};
    for (int i{0}; i < argc; i++) {
        map[name[i]] = argv[i] ? argv[i] : "";
    }

    tmp.push_back(std::move(map));

    return 0;
}

inline sdatabase::SQLite3Database::SQLite3Database(const std::string& database) {
    if (sqlite3_open(database.c_str(), &this->sqlite3_db)) {
        return;
    }

    this->database = database;
    this->is_good = true;
}

inline sdatabase::SQLite3Database::SQLite3Database() {
    this->is_good = false;
}

inline void sdatabase::SQLite3Database::open(const std::string& database) {
    if (this->is_good) {
        return;
    }

    if (sqlite3_open(database.c_str(), &this->sqlite3_db)) {
        return;
    }

    this->is_good = true;
}

inline bool sdatabase::SQLite3Database::exec(const std::string& query) {
    if (!this->is_good) {
        return false;
    }

    if (!this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement in database file '" + this->database + "': " + query + "\n"};
    }

    char* err{};

    int ret = sqlite3_exec(sqlite3_db, query.c_str(), nullptr, nullptr, &err);

    if (ret != SQLITE_OK) {
        sqlite3_free(err);
        return false;
    }

    return true;
}

inline bool sdatabase::SQLite3Database::validate(const std::string& query) {
    if (!this->is_good) {
        return false;
    }

    sqlite3_stmt* stmt;

    int ret = sqlite3_prepare_v2(sqlite3_db, query.c_str(), -1, &stmt, nullptr);

    if (ret != SQLITE_OK) {
        return false;
    }

    sqlite3_finalize(stmt);

    return true;
}

inline std::vector<std::unordered_map<std::string, std::string>> sdatabase::SQLite3Database::query(const std::string& query) {
    if (!this->is_good) {
        return {};
    }

    if (!this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement: " + query + "\n"};
    }

    char* err{};

    int status = sqlite3_exec(sqlite3_db, query.c_str(), sdatabase::callback, nullptr, &err);

    if (status != SQLITE_OK) {
        sqlite3_free(err);
        return {};
    }

    return std::move(tmp);
}

inline bool sdatabase::SQLite3Database::good() {
    return this->is_good;
}

inline bool sdatabase::SQLite3Database::is_open() {
    return this->good();
}

inline void sdatabase::SQLite3Database::close() {
    if (this->is_good) {
        sqlite3_close(this->sqlite3_db);
        this->is_good = false;
    }
}

inline bool sdatabase::SQLite3Database::empty() {
    return std::ifstream(this->database).peek() == std::ifstream::traits_type::eof();
}

inline sdatabase::SQLite3Database::~SQLite3Database() {
    if (this->is_good) {
        this->close();
    }
}

inline std::int64_t sdatabase::SQLite3Database::get_last_insertion() {
    if (!this->is_good) {
        return -1;
    }

    return sqlite3_last_insert_rowid(this->sqlite3_db);
}
#endif
#ifdef SDB_POSTGRESQL
inline sdatabase::PostgreSQLDatabase::PostgreSQLDatabase(const std::string& host,
    const std::string& user, const std::string& password, const std::string& database, int port) {

    this->open(host, user, password, database, port);
}

inline sdatabase::PostgreSQLDatabase::~PostgreSQLDatabase() {
    if (this->is_good) {
        this->close();
    }
}

inline void sdatabase::PostgreSQLDatabase::open(const std::string& host,
        const std::string& user, const std::string& password, const std::string& database, int port) {
    if (this->is_good) {
        return;
    }

    this->host = host;
    this->user = user;
    this->password = password;
    this->database = database;
    this->port = port;

    const std::string conninfo = ("host=" + host + " user=" + user + " password=" + password + " dbname=" + database + " port=" + std::to_string(port));
    this->pg_conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(pg_conn) != CONNECTION_OK || !pg_conn) {
        PQfinish(pg_conn);
        return;
    }

    PQsetNoticeProcessor(pg_conn, [](void*, const char* message) {
    }, nullptr);

    this->is_good = true;
}

inline bool sdatabase::PostgreSQLDatabase::exec(const std::string& query) {
    if (!this->is_good) {
        return false;
    }

    if (PQstatus(pg_conn) != CONNECTION_OK) {
        throw std::runtime_error{"Connection to database failed: " + std::string(PQerrorMessage(pg_conn))};
    }

    if (!this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement in database '" + this->database + "': " + query + "\n"};
    }

    PGresult* res = PQexec(pg_conn, query.c_str());

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

inline bool sdatabase::PostgreSQLDatabase::validate(const std::string& query) {
    if (!this->is_good) {
        return false;
    }

    PGresult* res = PQprepare(pg_conn, "", query.c_str(), 0, nullptr);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

inline std::vector<std::unordered_map<std::string, std::string>> sdatabase::PostgreSQLDatabase::query(const std::string& query) {
    if (!this->is_good) {
        return {};
    }

    if (!this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement: " + query + "\n"};
    }

    PGresult* res = PQexec(pg_conn, query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> result;
    int nrows = PQntuples(res);
    int nfields = PQnfields(res);

    for (int i = 0; i < nrows; ++i) {
        std::unordered_map<std::string, std::string> row;
        for (int j = 0; j < nfields; ++j) {
            row[PQfname(res, j)] = PQgetvalue(res, i, j);
        }
        result.push_back(std::move(row));
    }

    PQclear(res);
    return result;
}

inline bool sdatabase::PostgreSQLDatabase::good() {
    return this->is_good;
}

inline bool sdatabase::PostgreSQLDatabase::is_open() {
    return this->good();
}

inline void sdatabase::PostgreSQLDatabase::close() {
    if (this->is_good) {
        PQfinish(this->pg_conn);
        this->is_good = false;
    }
}

inline bool sdatabase::PostgreSQLDatabase::empty() {
    if (!this->is_good) {
        return true;
    }

    const char* query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';";
    PGresult* res = PQexec(pg_conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return true;
    }

    bool is_empty = std::stoi(PQgetvalue(res, 0, 0)) == 0;
    PQclear(res);
    return is_empty;
}

inline std::int64_t sdatabase::PostgreSQLDatabase::get_last_insertion() {
    if (!this->is_good) {
        return -1;
    }

    const char* query = "SELECT LASTVAL();";
    PGresult* res = PQexec(pg_conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return -1;
    }

    std::int64_t last_insertion = std::stoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return last_insertion;
}
#endif
