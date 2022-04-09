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
#define OPTYPE hsql::Expr::OperatorType

// All types of create statement
#define CREATE_TABLE hsql::ColumnDefinition::kCreateTable;
// #define CREATE_TABLE_FROM_TABLE hsql::ColumnDefinition::kCreateTableFromTbl;
// #define CREATE_VIEW hsql::ColumnDefinition::kCreateView;
// #define CREATE_INDEX hsql::ColumnDefinition::kCreateIndex;

// Possible data types for each column in create statement:
// unknown, text, int, double
// #define UNKNOWN hsql::DataType::UNKNOWN;
// #define TEXT hsql::DataType::TEXT;
// #define INT hsql::DataType::INT;
// #define DOUBLE hsql::DataType::DOUBLE;

std::string getHomeDir();

void handleSQLStatement(std::string query);

void printStatementInfo(const hsql::CreateStatement *statement);

void printStatementInfo(const hsql::SelectStatement *statement);

std::string columnDefinitionToString(const hsql::ColumnDefinition *col);

std::string getSelectList(std::vector<hsql::Expr*>* selectList);

std::string getFromTable(hsql::TableRef* fromTable);

std::string getJoinType(hsql::JoinType type);

std::string getExpression(hsql::Expr* expr);

std::string getOperator(OPTYPE opType);

std::string getWhereClause(hsql::Expr* whereClause);

std::string getGroupBy(hsql::GroupByDescription* groupBy);

const char *EXIT = "quit";
const unsigned int BLOCK_SZ = 4096; // block size
const char *HOME = "cpsc5300/data"; // the db dir
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
    std::cout << "SQL> "; // prompt for the first input

    // if input is "quit", terminate the program
    while(true) {
        std::getline(std::cin, input);
        if (input == "") {
            std::cout << "SQL> ";
            continue;
        }
        if (input == EXIT) { // EXIT condition
            std::cout << "Terminating the program" << std::endl;
            break;
        }
        handleSQLStatement(input);
        std::cout << "SQL> "; // prompt for the next input
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
    // TODO Use fast fail pattern here
    if (result->isValid()) { // valid SQL
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
    } else { // invalid SQL
        // fprintf(stderr, "Given string is not a valid SQL query.\n");
        std::cout << "Invalid SQL: " << query << std::endl;
        fprintf(stderr, "%s (L%d:%d)\n",
                result->errorMsg(),
                result->errorLine(),
                result->errorColumn());
        delete result;
    }
}

// TODO: Add support for create table
// Test SQL:
// create table foo (a text, b integer, c double)
void printStatementInfo(const hsql::CreateStatement *statement) {
    std::cout << "CREATE ";
    // Type of CREATE
    switch(statement->type) {
    // case CREATE_TABLE:
    case 0: // KTable, create table
        std::cout << "TABLE ";
        break;
    default: // TODO Add support for other create types later(next milestone or next sprint)
        std::cout << "UN-SUPPORTED TYPE ";
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

// TODO: add support to join
// TODO: add support to where clause
// TODO: add support to alias
void printStatementInfo(const hsql::SelectStatement *statement) {
    std::string selectStatement = "";

    selectStatement.append(getSelectList(statement->selectList));
    selectStatement.append("FROM ");
    selectStatement.append(getFromTable(statement->fromTable));
    selectStatement.append(getWhereClause(statement->whereClause));
    selectStatement.append(getGroupBy(statement->groupBy));

    std::cout << selectStatement << std::endl;
}

/**
**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
 // create table foo (a text, b integer, c double)
std::string columnDefinitionToString(const hsql::ColumnDefinition *col) {
    // std::string ret(col->name);
    std::string ret;
    ret += col->name;
    switch(col->type) {
    // case DOUBLE:
    case 3:
        ret += " DOUBLE";
        break;
    // case INT:
    case 2:
        ret += " INT";
        break;
    // case TEXT:
    case 1:
        ret += " TEXT";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
    // return "INVALID_FUNCTION";
}

std::string getSelectList(std::vector<hsql::Expr*>* selectList) {
    std::string output = "SELECT ";

    for(int i = 0; i < selectList->size(); i++) {
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
            output.append("ON ");
            output.append(getExpression(fromTable->join->condition));
            break;
        case hsql::TableRefType::kTableCrossProduct:
            for (hsql::TableRef* table : *fromTable->list) {
                output.append(getFromTable(table));
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

    output.append(" ");

    return output;
}

std::string getJoinType(hsql::JoinType type) {
    if(!type) return "";

    std::string output = "";

    switch(type) {
        case hsql::JoinType::kJoinInner:
            output.append("INNER JOIN ");
            break;
        case hsql::JoinType::kJoinOuter:
            output.append("OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinLeft:
            output.append("LEFT JOIN ");
            break;
        case hsql::JoinType::kJoinRight:
            output.append("RIGHT JOIN ");
            break;
        case hsql::JoinType::kJoinLeftOuter:
            output.append("LEFT OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinRightOuter:
            output.append("RIGHT OUTER JOIN ");
            break;
        case hsql::JoinType::kJoinCross:
            output.append("CROSS JOIN ");
            break;
        case hsql::JoinType::kJoinNatural:
            output.append("NATURAL JOIN ");
            break;
        default:
            output.append("JOIN ");
            break;
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
                output.append(getOperator(expr->opType));
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

std::string getOperator(OPTYPE opType) {
    if(!opType) return "";

    std::string output = "";

    switch(opType) {
        case OPTYPE::NONE:
            std::cout << "NONE" << std::endl;
            break;
        case OPTYPE::BETWEEN:
            std::cout << "BETWEEN" << std::endl;
            break;
        case OPTYPE::CASE:
            std::cout << "CASE" << std::endl;
            break;
        case OPTYPE::SIMPLE_OP:
            output.append(" =");
            break;
        case OPTYPE::NOT_EQUALS:
            std::cout << "NOT_EQUALS" << std::endl;
            break;
        case OPTYPE::LESS_EQ:
            std::cout << "LESS_EQ" << std::endl;
            break;
        case OPTYPE::GREATER_EQ:
            std::cout << "GREATER_EQ" << std::endl;
            break;
        case OPTYPE::LIKE:
            std::cout << "LIKE" << std::endl;
            break;
        case OPTYPE::NOT_LIKE:
            std::cout << "NOT_LIKE" << std::endl;
            break;
        case OPTYPE::AND:
            std::cout << "AND" << std::endl;
            break;
        case OPTYPE::OR:
            std::cout << "OR" << std::endl;
            break;
        case OPTYPE::IN:
            std::cout << "IN" << std::endl;
            break;
        case OPTYPE::NOT:
            std::cout << "NOT" << std::endl;
            break;
        case OPTYPE::UMINUS:
            std::cout << "UMINUS" << std::endl;
            break;
        case OPTYPE::ISNULL:
            std::cout << "ISNULL" << std::endl;
            break;
        case OPTYPE::EXISTS:
            std::cout << "EXISTS" << std::endl;
            break;
        default:
            std::cerr << "Operator type " << opType << " not found." << std::endl;
            return output;
    }

    return output;
};

std::string getWhereClause(hsql::Expr* whereClause) {
    return "where clause";
}

std::string getGroupBy(hsql::GroupByDescription* groupBy) {
    return "group by";
}