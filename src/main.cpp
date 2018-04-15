#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
int yysqlparse(void);  // defined in y.tab.c
}

extern struct FuncOperator
    *finalFunction;  // the aggregate function (NULL if no agg)

extern struct TableList *tables;  // the list of tables and aliases in the query
extern struct AndList *boolean;   // the predicate in the WHERE clause
extern struct NameList *groupingAtts;  // grouping atts (NULL if no grouping)
extern struct NameList *
    attsToSelect;  // the set of attributes in the SELECT (NULL if no such atts)
extern struct NameList *
    attsToCreate;  // the set of attributes in the SELECT (NULL if no such atts)
extern struct CreateTable *createData;  // data associated with creating a Table

int main() { yysqlparse(); }
