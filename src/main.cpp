#include <iostream>
#include "QueryPlanner.h"

using namespace std;

extern "C" {
int yysqlparse(void);
}

// the aggregate function (NULL if no agg)
extern struct FuncOperator *finalFunction;

// the list of tables and aliases in the query
extern struct TableList *tables;

// the predicate in the WHERE clause
extern struct AndList *boolean;

// grouping atts (NULL if no grouping)
extern struct NameList *groupingAtts;

// the set of attributes in the SELECT (NULL if no such atts)
extern struct NameList *attsToSelect;

// data associated with creating a Table
extern struct CreateTable* createData; 

// 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctAtts; 

// 1 if there is a DISTINCT in an aggregate query
extern int distinctFunc;

// Says whether it is a create table, insert, drop table, set output, or select
extern int command;

// The type of the output
extern int outType;

// a referenced file
extern char *refFile;

// a referenced table
extern char *refTable; 

void doSelect() {
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                        attsToSelect, distinctAtts, distinctFunc, refFile);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Execute(outType);
}

int main() {
    bool quit = false;
    refFile = NULL;
    outType = SET_NONE;
    while (!quit) {
        command = 0;
        yysqlparse();
        switch (command) {
            case CREATE: {

            } break;
            case INSERT_INTO: {

            } break;
            case DROP: {

            } break;
            case OUTPUT_SET: {
                cout << "\nOutput mode has been set!\n";
            } break;
            case SELECT_TABLE: {
                doSelect();
            } break;
            case QUIT_SQL: {
                quit = true;
            } break;
        }
    }
}