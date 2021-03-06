/**
 * @file sql5300.cpp - SQL shell
 * @author Ana Carolina de Souza Mendes, MSCS
 * @author Fangsheng Xu, MSCS
 * @see "Seattle University, CPSC5300, Spring 2022"
 * This is free and unencumbered software released into the public domain.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include <string>
#include <sys/types.h>
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

#define SELECT hsql::StatementType::kStmtSelect
#define CREATE hsql::StatementType::kStmtCreate
#define OPTYPE hsql::Expr::OperatorType

/**
 * Global db environment used in storage engine
 */
DbEnv *_DB_ENV;

/**
 * Parses the query inputed by user.
 * implementation is close to execute() in other repos
 * @param query to be parsed
 */
void handleSQLStatement(std::string query);

/**
 * Parses SQL statement and prints its query.
 * @param statement to be parsed
 */
void printStatementInfo(const hsql::CreateStatement *statement);

/**
 * Parses SQL statement and prints its query.
 * @param statement to be parsed
 */
void printStatementInfo(const hsql::SelectStatement *statement);

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
std::string columnDefinitionToString(const hsql::ColumnDefinition *col);

/**
 * Builds a formatted string with the select statement expressions
 * @param selectList list of expressions in select statement
 * @return a string with the select statement
 */
std::string getSelectList(std::vector<hsql::Expr*>* selectList);

/**
 * Builds a formatted string with the table references
 * @param fromTable reference for a select statement
 * @return a string with the table reference
 */
std::string getFromTable(hsql::TableRef* fromTable);

/**
 * Builds a formatted string with the from clause JOIN type
 * @param type of join
 * @return a string with the join type
 */
std::string getJoinType(hsql::JoinType type);

/**
 * Builds a formatted string for the expression type
 * @param expr expression
 * @return a string with the query expression
 */
std::string getExpression(hsql::Expr* expr);

/**
 * Builds a formatted string with the where clause components
 * @param whereClause for a select statement
 * @return a string with the where clause
 */
std::string getWhereClause(hsql::Expr* whereClause);

/**
 * Builds a formatted string with the group by clause components
 * @param groupBy clause for a select statement
 * @return a string with the group by clause
 */
std::string getGroupBy(hsql::GroupByDescription* groupBy);

const char *EXIT = "quit"; // command to quit the sql shell program
const unsigned int BLOCK_SZ = 4096; // block size
const char *HOME = "cpsc5300/data"; // the relative db dir
const char *EXAMPLE = "example.db"; // name of the db

int main(int argc, char** argv) {
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
        const char *dir = std::getenv("HOME"); // get parent dir of HOME
	    home = std::string(dir) + "/" + HOME; // absolute dir of HOME in the environment
    }
    else {
        home = argv[1];
    }

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    try {
        // env.open(home, DB_CREATE | DB_INIT_MPOOL, 0);
        env.open(home.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
        std::cout << "(sql5300: running with database environment at " << home << ")" << std::endl;
    } catch (DbException &exc) {
        std::cerr << "(sql5300: " << exc.what() << ")";
        env.close(0);
        exit(1);
    }
    _DB_ENV = &env;

    // Parse the SQL strings
    std::string input; // input string

    // if input is "quit", terminate the program
    while(true) {
        std::cout << "SQL> "; // prompt for the next input
        std::getline(std::cin, input);
        if (input == "") {
            continue;
        }
        if (input == EXIT) { // EXIT condition
            std::cout << "Terminating the program" << std::endl;
            break;
        }
        // Naive Test
        if (input == "test") {
            std::cout << "test_heap_storage: " << (test_heap_storage() ? "\nTests Passed" : "\nTests Failed") << std::endl;
            continue;
        }

        handleSQLStatement(input);
    }

    return EXIT_SUCCESS;
}

void handleSQLStatement(std::string query) {
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);
    if (!result->isValid()) { // invalid SQL
        std::cout << "Invalid SQL: " << query << std::endl;
        fprintf(stderr, "%s (L%d:%d)\n",
                result->errorMsg(),
                result->errorLine(),
                result->errorColumn());
        delete result;
        return;
    }
    // process Valid SQL, equivalent to execute() in other repos
    for (uint i = 0; i < result->size(); ++i) {
        const hsql::SQLStatement *statement = result->getStatement(i);
        switch(statement->type()) {
            case SELECT:
                printStatementInfo((const hsql::SelectStatement*)statement);
                break;
            case CREATE:
                printStatementInfo((const hsql::CreateStatement*)statement);
                break;
            default:
                hsql::printStatementInfo(statement);
                break;
        }
    }
    delete result;
}

void printStatementInfo(const hsql::CreateStatement *statement) {
    std::cout << "CREATE ";
    // Handle each type of CREATE
    switch(statement->type) {
        case hsql::CreateStatement::kTable: // Create Table
            std::cout << "TABLE ";
            break;
        case hsql::CreateStatement::kTableFromTbl: // Hyrise file format
            std::cout << "TABLE FROM Hyrise File Format ";
            break;
        case hsql::CreateStatement::kView: // Create View
            std::cout << "VIEW ";
            break;
        case hsql::CreateStatement::kIndex: // Create Index
            std::cout << "INDEX ";
            break;
        default:
            std::cout << "UN-SUPPORTED CREATE TYPE ";
            break;
    }
    // Table Name
    std::cout << statement->tableName << " ";

    // handle each column
    std::cout << "(";
    uint column_size = statement->columns->size();
    for (uint i = 0; i < column_size; ++i) {
        if (i != 0) {
            std::cout << ", "; // append ','
        }
        std::cout << columnDefinitionToString(statement->columns->at(i));
    }
    std::cout << ")" << std::endl;
}

void printStatementInfo(const hsql::SelectStatement *statement) {
    std::string selectStatement = "";

    selectStatement.append(getSelectList(statement->selectList));
    selectStatement.append("FROM ");
    selectStatement.append(getFromTable(statement->fromTable));
    selectStatement.append(getWhereClause(statement->whereClause));
    selectStatement.append(getGroupBy(statement->groupBy));

    std::cout << selectStatement << std::endl;
}

std::string columnDefinitionToString(const hsql::ColumnDefinition *col) {
    std::string ret;
    ret += col->name;
    switch(col->type) {
        case hsql::ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case hsql::ColumnDefinition::INT:
            ret += " INT";
            break;
        case hsql::ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

std::string getSelectList(std::vector<hsql::Expr*>* selectList) {
    std::string output = "SELECT ";

    // i's data type changed from int to long unsigned int bc previous warning from the compiler
    for(long unsigned int i = 0; i < selectList->size(); i++) {
        output.append(getExpression(selectList->at(i)));
        if(i < (selectList->size() - 1)) {
            output.append(", ");
        }
    }
    output.append(" ");

    return output;
}

std::string getFromTable(hsql::TableRef* fromTable) {
    std::string output = "";

    switch(fromTable->type) {
        case hsql::TableRefType::kTableName:
            if(fromTable->schema) {
                output.append(fromTable->schema);
                output.append(".");
            }
            output.append(fromTable->name);
            break;
        case hsql::TableRefType::kTableJoin:
            // name of table on the left
            output.append(getFromTable(fromTable->join->left));
            // name of table on the right
            output.append(getJoinType(fromTable->join->type));
            output.append(getFromTable(fromTable->join->right));
            output.append(" ON ");
            output.append(getExpression(fromTable->join->condition));
            break;
        case hsql::TableRefType::kTableCrossProduct:
            for(int i = (fromTable->list->size() - 1); i >= 0; i--) {
                output.append(getFromTable(fromTable->list->at(i)));
                if(i > 0) {
                    output.append(", ");
                }
            }
            break;
        default:
            std::cerr << "Table type " << fromTable->type << " not found." << std::endl;
            return output;
    }

    if(fromTable->alias != nullptr) {
        output.append(" AS ");
        output.append(fromTable->alias);
    }

    return output;
}

std::string getJoinType(hsql::JoinType type) {
    if(!type) return " JOIN ";

    std::string output = "";

    switch(type) {
        case hsql::JoinType::kJoinInner:
            output.append(" INNER JOIN ");
            break;
        case hsql::JoinType::kJoinOuter:
            output.append(" OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinLeft:
            output.append(" LEFT JOIN ");
            break;
        case hsql::JoinType::kJoinRight:
            output.append(" RIGHT JOIN ");
            break;
        case hsql::JoinType::kJoinLeftOuter:
            output.append(" LEFT OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinRightOuter:
            output.append(" RIGHT OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinCross:
            output.append(" CROSS JOIN ");
            break;
        case hsql::JoinType::kJoinNatural:
            output.append(" NATURAL JOIN ");
            break;
        default:
            std::cerr << "Join type " << type << " not found." << std::endl;
            return output;
    }

    return output;
};

std::string getExpression(hsql::Expr* expr) {
    if(!expr) return "";

    std::string output = "";

    switch(expr->type) {
        case hsql::ExprType::kExprStar:
            output.append("*");
            break;
        case hsql::ExprType::kExprLiteralInt:
            output.append(std::to_string(expr->ival));
            break;
        case hsql::ExprType::kExprColumnRef:
            if(expr->table) {
                output.append(expr->table);
                output.append(".");
            }
            output.append(expr->name);
            break;
        case hsql::ExprType::kExprOperator:
            if(expr == nullptr) {
                output.append("null ");
            }
            else {
                output.append(getExpression(expr->expr));
                output.append(" ");
                output += expr->opChar;
                output.append(" ");
                if(expr->expr2 != nullptr) {
                    output.append(getExpression(expr->expr2));
                }
                else if(expr->exprList != nullptr) {
                    for(hsql::Expr* e : *expr->exprList) {
                        output.append(getExpression(e));
                    }
                }
            }
            break;
        case hsql::ExprType::kExprSelect:
            output.append(getSelectList(expr->select->selectList));
            break;
        default:
            std::cerr << "Expression type " << expr->type << " not found." << std::endl;
            return output;
    }

    if(expr->alias != nullptr) {
        output.append(expr->alias);
    }

    return output;
}

std::string getWhereClause(hsql::Expr* whereClause) {
    if(!whereClause) return "";

    std::string output = " WHERE ";
    output.append(getExpression(whereClause));

    return output;
}

std::string getGroupBy(hsql::GroupByDescription* groupBy) {
    if(!groupBy) return "";

    return "Group by not implemented";
}