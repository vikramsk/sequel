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

// the set of attributes in the SELECT (NULL if no such atts)
extern struct NameList *attsToCreate;

// 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctAtts; 

// 1 if there is a DISTINCT in an aggregate query
extern int distinctFunc;

int main() {
    while (true) {
        yysqlparse();
        QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                       attsToSelect, distinctAtts, distinctFunc);
        QueryPlanner qp(qt);
        qp.Create();
    }
}
