#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include <string>
#include <sys/types.h>

// include the sql parser
#include "SQLParser.h"
// contains printing utilities
#include "sqlhelper.h"

#define SELECT hsql::StatementType::kStmtSelect
#define CREATE hsql::StatementType::kStmtCreate

std::string getHomeDir();

void handleSQLStatement(std::string query);

void printStatementInfo(const hsql::CreateStatement *statement);

void printStatementInfo(const hsql::SelectStatement *statement);


const char *EXIT = "quit";
const unsigned int BLOCK_SZ = 4096; // block size
const char *HOME = "cpsc5300/5300-Fossa/data"; // the db dir
const char *EXAMPLE = "example.db"; // name of the db

int main(int argc, char** argv)
{
    std::string home;

    if (argc != 2) // needs one arguement to save our db ./sql dir/to/write
    {
        // needs a directory dedicated for the db
        std::cout << "Have you created a dir: ~/" << HOME  << "? (y/n) " << std::endl;
        std::string ans;
        std::cin >> ans;
        if( ans[0] != 'y') {
            std::cout << " Usage: " << argv[0] << " [path to a writable directory]" << std::endl;
            return EXIT_FAILURE;
        }
        home = getHomeDir();
    }
    else {
        home = argv[1];
    }

    std::cout << "(sql5300: running with database environment at " << home << ")" << std::endl;

    // creates db environment using DB_CREATE flag
    DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(home.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(NULL, EXAMPLE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

	char block[BLOCK_SZ];
	Dbt data(block, sizeof(block));
	int block_number;
	Dbt key(&block_number, sizeof(block_number));
	block_number = 1;
	strcpy(block, "hello!");
	db.put(NULL, &key, &data, 0);  // write block #1 to the database

	Dbt rdata;
	db.get(NULL, &key, &rdata, 0); // read block #1 from the database
	std::cout << "Read (block #" << block_number << "): '" << (char *)rdata.get_data() << "'";
	std::cout << " (expect 'hello!')" << std::endl;

    // Parse the SQL strings
    std::string input; // input string

    // if input is "quit", terminate the program
    while(true) {
        std::cout << "SQL> "; // prompt for the first input
        std::getline(std::cin, input);
        if (input == EXIT) { // EXIT condition
            std::cout << "Terminating the program" << std::endl;
            break;
        }
        handleSQLStatement(input);
    }

    return EXIT_SUCCESS;
}

std::string getHomeDir() {
    const char *home = std::getenv("HOME"); // get parent dir of HOME
	std::string envdir = std::string(home) + "/" + HOME; // absolute dir of HOME in the environment

    return envdir;
}

void handleSQLStatement(std::string query) {
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query); 

    if (result->isValid()) { // valid SQL
        for (uint i = 0; i < result->size(); ++i) {
            const hsql::SQLStatement *statement = result->getStatement(i);
            switch(statement->type()) {
                case SELECT:
                    printStatementInfo((hsql::SelectStatement*)statement);
                    break;
                case CREATE:
                    printStatementInfo((hsql::CreateStatement*)statement);
                    break;
                default:
                    hsql::printStatementInfo(statement);
                    break;
            }
        }
        delete result;
    } else { // invalid SQL
        fprintf(stderr, "Given string is not a valid SQL query.\n");
        fprintf(stderr, "%s (L%d:%d)\n",
                result->errorMsg(),
                result->errorLine(),
                result->errorColumn());
        delete result;
    }
}

// TODO: Add support for create table
void printStatementInfo(const hsql::CreateStatement *statement) {
    std::cout << "create statement" << std::endl;
    hsql::printStatementInfo(statement);
}

// TODO: add support to select from
// TODO: add support to join
// TODO: add support to where clause
// TODO: add support to alias
void printStatementInfo(const hsql::SelectStatement *statement) {
    std::cout << "select statement" << std::endl;
    hsql::printStatementInfo(statement);
}