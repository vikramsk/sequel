#include <iostream>
#include "gtest/gtest.h"
#include "testsuite.h"
#include "QueryPlanner.h"
#include "ParseTree.h"

TEST(QueryPlanTest, Query3) {
    char *query = "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                    attsToSelect, distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}

TEST(QueryPlanTest, Query6) {
    char *query = "SELECT l.l_orderkey FROM lineitem AS l WHERE (l.l_quantity > 30)";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                    attsToSelect, distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}

TEST(QueryPlanTest, Query10) {
    char *query = "SELECT SUM (l.l_extendedprice * l.l_discount) FROM lineitem AS l WHERE (l.l_discount<0.07) AND (l.l_quantity < 24)";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts,
                    attsToSelect, distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}