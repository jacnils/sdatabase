/* sdatabase - Simple database abstraction for SQLite3 (and potential future databases)
 * Licensed under the MIT license
 * Copyright (c) 2024-2025 Jacob Nilsson
 */

#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <stdexcept>
#include <sqlite3.h>

/**
 * @brief Namespace for database related functions and classes.
 */
namespace sdatabase {
    /**
     * @brief Class for database operations.
     */
    class Database {
        private:
            sqlite3* db{};
            std::string database{};
            bool is_good{false};
        public:
            /**
             * @brief Query the database, returning data.
             * @param query Query to execute.
             * @param validate Validate the query.
             * @return std::vector<std::unordered_map<std::string, std::string>> Data.
             */
            std::vector<std::unordered_map<std::string, std::string>> query(const std::string& query, const bool validate=true);
            /**
             * @brief Execute an SQL command.
             * @param query Query to execute.
             * @param validate Validate the query.
             * @return bool True if successful.
             */
            bool exec(const std::string& query, const bool validate=true);
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
            Database();
            /**
             * @brief Constructor.
             * @param database Database to open.
             */
            Database(const std::string& database);
            /**
             * @brief Destructor.
             */
            ~Database();
    };

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
}

inline int sdatabase::callback(void* data, int argc, char** argv, char** name) {
    (void)data;

    std::unordered_map<std::string, std::string> map{};
    for (int i{0}; i < argc; i++) {
        map[name[i]] = argv[i] ? argv[i] : "";
    }

    tmp.push_back(std::move(map));

    return 0;
}

inline sdatabase::Database::Database(const std::string& database) {
    if (sqlite3_open(database.c_str(), &this->db)) {
        return;
    }

    this->database = database;
    this->is_good = true;
}

inline sdatabase::Database::Database() {
    this->is_good = false;
}

inline void sdatabase::Database::open(const std::string& database) {
    if (this->is_good) {
        return;
    }

    if (sqlite3_open(database.c_str(), &this->db)) {
        return;
    }

    this->is_good = true;
}

inline bool sdatabase::Database::exec(const std::string& query, const bool validate) {
    if (!this->is_good) {
        return false;
    }

    if (validate && !this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement in database file '" + this->database + "': " + query + "\n"};
    }

    char* err{};

    int ret = sqlite3_exec(db, query.c_str(), nullptr, nullptr, &err);

    if (ret != SQLITE_OK) {
        sqlite3_free(err);
        return false;
    }

    return true;
}

inline bool sdatabase::Database::validate(const std::string& query) {
    if (!this->is_good) {
        return false;
    }

    sqlite3_stmt* stmt;

    int ret = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);

    if (ret != SQLITE_OK) {
        return false;
    }

    sqlite3_finalize(stmt);

    return true;
}

inline std::vector<std::unordered_map<std::string, std::string>> sdatabase::Database::query(const std::string& query, const bool validate) {
    if (!this->is_good) {
        return {};
    }

    if (validate && !this->validate(query)) {
        throw std::runtime_error{"Invalid SQL statement: " + query + "\n"};
    }

    char* err{};

    int status = sqlite3_exec(db, query.c_str(), sdatabase::callback, nullptr, &err);

    if (status != SQLITE_OK) {
        sqlite3_free(err);
        return {};
    }

    return std::move(tmp);
}

inline bool sdatabase::Database::good() {
    return this->is_good;
}

inline bool sdatabase::Database::is_open() {
    return this->good();
}

inline void sdatabase::Database::close() {
    if (this->is_good) {
        sqlite3_close(this->db);
        this->is_good = false;
    }
}

inline bool sdatabase::Database::empty() {
    return std::ifstream(this->database).peek() == std::ifstream::traits_type::eof();
}

inline sdatabase::Database::~Database() {
    if (this->is_good) {
        this->close();
    }
}

inline std::int64_t sdatabase::Database::get_last_insertion() {
    if (!this->is_good) {
        return -1;
    }

    return sqlite3_last_insert_rowid(this->db);
}
