#include <iostream>
#include "QueryPlanner.h"
#include "Sequel.h"
#include "cpptoml.h"

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
extern struct CreateTable *createData;

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

int main() {
    refFile = NULL;
    outType = SET_NONE;
    bool quit = false;
    Sequel sequel;
    while (!quit) {
        command = 0;
        cout << "\nSQL> ";
        yysqlparse();
        switch (command) {
            case CREATE: {
                sequel.doCreate(createData, refTable);
            } break;
            case INSERT_INTO: {
                sequel.doInsert(refTable, refFile);
            } break;
            case DROP: {
                sequel.doDrop(refTable);
            } break;
            case OUTPUT_SET: {
                cout << "\nOutput mode has been set!\n";
            } break;
            case SELECT_TABLE: {
                QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                            attsToSelect, distinctAtts, distinctFunc, refFile);
                sequel.doSelect(qt, outType);
            } break;
            case QUIT_SQL: {
                quit = true;
            } break;
        }
    }
}
