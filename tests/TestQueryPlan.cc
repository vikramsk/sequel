#include <iostream>
#include "ParseTree.h"
#include "QueryPlanner.h"
#include "gtest/gtest.h"
#include "testsuite.h"

// TEST(QueryPlanTest, Query1) {
//    char *query =
//        "SELECT SUM (ps.ps_supplycost), s.s_suppkey FROM part AS p, supplier "
//        "AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND "
//        "(s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) GROUP BY "
//        "s.s_suppkey";
//    cout << endl << query << endl;
//    init_sql_parser(query);
//    yysqlparse();
//    close_sql_parser();
//    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                   distinctAtts, distinctFunc);
//    QueryPlanner qp(qt);
//    qp.Create();
//    qp.Print();
//}

// TEST(QueryPlanTest, Query2) {
//     char *query =
//         "SELECT SUM (c.c_acctbal), c.c_name FROM customer AS c, orders AS o "
//         "WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000.0) "
//         "GROUP BY c.c_name";
//     cout << endl << query << endl;
//     init_sql_parser(query);
//     yysqlparse();
//     close_sql_parser();
//     QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                    distinctAtts, distinctFunc);
//     QueryPlanner qp(qt);
//     qp.Create();
//     qp.Print();
// }

TEST(QueryPlanTest, Query3) {
    char *query =
        "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l "
        "WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode "
        "= 'MAIL')";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
                   distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}

TEST(QueryPlanTest, Query4) {
    char *query =
        "SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal FROM customer "
        "AS c1, customer AS c2 WHERE (c1.c_nationkey = c2.c_nationkey) AND "
        "(c1.c_name ='Customer#000070919')";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
                   distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}

// TEST(QueryPlanTest, Query5) {
//     char *query =
//         "SELECT SUM(l.l_discount) FROM customer AS c, orders AS o, lineitem AS "
//         "l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) "
//         "AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30) AND "
//         "(l.l_discount < 0.03)";
//     cout << endl << query << endl;
//     init_sql_parser(query);
//     yysqlparse();
//     close_sql_parser();
//     QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                    distinctAtts, distinctFunc);
//     QueryPlanner qp(qt);
//     qp.Create();
//     qp.Print();
// }

TEST(QueryPlanTest, Query6) {
    char *query =
        "SELECT l.l_orderkey FROM lineitem AS l WHERE (l.l_quantity > 30)";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
                   distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}

// TEST(QueryPlanTest, Query7) {
//     char *query =
//         "SELECT DISTINCT c.c_name FROM lineitem AS l, orders AS o, customer AS "
//         "c, nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) "
//         "AND(o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) "
//         "AND (n.n_regionkey = r.r_regionkey)";
//     cout << endl << query << endl;
//     init_sql_parser(query);
//     yysqlparse();
//     close_sql_parser();
//     QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                    distinctAtts, distinctFunc);
//     QueryPlanner qp(qt);
//     qp.Create();
//     qp.Print();
// }

// TEST(QueryPlanTest, Query8) {
//     char *query =
//         "SELECT l.l_discount FROM lineitem AS l, orders AS o, customer AS c, "
//         "nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) AND "
//         "(o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND "
//         "(n.n_regionkey = r.r_regionkey) AND (r.r_regionkey = 1) AND "
//         "(o.o_orderkey < 10000)";
//     cout << endl << query << endl;
//     init_sql_parser(query);
//     yysqlparse();
//     close_sql_parser();
//     QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                    distinctAtts, distinctFunc);
//     QueryPlanner qp(qt);
//     qp.Create();
//     qp.Print();
// }

// TEST(QueryPlanTest, Query9) {
//     char *query =
//         "SELECT SUM (l.l_discount) FROM customer AS c, orders AS o, lineitem "
//         "AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = "
//         "l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity "
//         "> 30) AND (l.l_discount < 0.03)";
//     cout << endl << query << endl;
//     init_sql_parser(query);
//     yysqlparse();
//     close_sql_parser();
//     QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
//                    distinctAtts, distinctFunc);
//     QueryPlanner qp(qt);
//     qp.Create();
//     qp.Print();
// }

TEST(QueryPlanTest, Query10) {
    char *query =
        "SELECT SUM (l.l_extendedprice * l.l_discount) FROM lineitem AS l "
        "WHERE (l.l_discount<0.07) AND (l.l_quantity < 24)";
    cout << endl << query << endl;
    init_sql_parser(query);
    yysqlparse();
    close_sql_parser();
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
                   distinctAtts, distinctFunc);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Print();
}
