#include <math.h>
#include <stdlib.h>
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "testsuite.h"
#include "gtest/gtest.h"

TEST(StatisticalEstimationTest, MultiplePredicates) {
    Statistics s_1;
    char *relName[] = {"orders", "customer", "nation"};

    s_1.AddRel(relName[0], 1500000);
    s_1.AddAtt(relName[0], "o_custkey", 150000);

    s_1.AddRel(relName[1], 150000);
    s_1.AddAtt(relName[1], "c_custkey", 150000);
    s_1.AddAtt(relName[1], "c_nationkey", 25);

    s_1.AddRel(relName[2], 25);
    s_1.AddAtt(relName[2], "n_nationkey", 25);

    char *cnf = "(o_custkey = 10) AND (c_custkey = o_custkey)";
    yy_scan_string(cnf);
    yyparse();

    // Join the first two relations in relName
    s_1.Apply(final, relName, 2);

    cnf = "(c_nationkey = n_nationkey) AND (n_nationkey < 10)";
    yy_scan_string(cnf);
    yyparse();

    double result_1 = s_1.Estimate(final, relName, 3);
    
    Statistics s_2;

    s_2.AddRel(relName[0], 1500000);
    s_2.AddAtt(relName[0], "o_custkey", 150000);

    s_2.AddRel(relName[1], 150000);
    s_2.AddAtt(relName[1], "c_custkey", 150000);
    s_2.AddAtt(relName[1], "c_nationkey", 25);

    s_2.AddRel(relName[2], 25);
    s_2.AddAtt(relName[2], "n_nationkey", 25);

    cnf = "(c_custkey = o_custkey) AND (n_nationkey < 10) AND (c_nationkey = n_nationkey) AND (o_custkey = 10)";
    yy_scan_string(cnf);
    yyparse();

    ASSERT_EQ(s_2.Estimate(final, relName, 3),result_1);
}

TEST(StatisticalEstimationTest, ORingMultiplePredicates) {
    Statistics s_1;
    char *relName[] = {"orders", "customer", "nation"};

    s_1.AddRel(relName[0], 1500000);
    s_1.AddAtt(relName[0], "o_custkey", 150000);

    s_1.AddRel(relName[1], 150000);
    s_1.AddAtt(relName[1], "c_custkey", 150000);
    s_1.AddAtt(relName[1], "c_nationkey", 25);

    s_1.AddRel(relName[2], 25);
    s_1.AddAtt(relName[2], "n_nationkey", 25);

    char *cnf = "(o_custkey = 10 OR c_custkey = 20)";
    yy_scan_string(cnf);
    yyparse();

    // Join the first two relations in relName
    s_1.Apply(final, relName, 2);

    cnf = "(c_nationkey = n_nationkey) AND (n_nationkey < 10 OR n_nationkey = 15 OR n_nationkey = 20)";
    yy_scan_string(cnf);
    yyparse();

    double result_1 = s_1.Estimate(final, relName, 3);
    
    Statistics s_2;

    s_2.AddRel(relName[0], 1500000);
    s_2.AddAtt(relName[0], "o_custkey", 150000);

    s_2.AddRel(relName[1], 150000);
    s_2.AddAtt(relName[1], "c_custkey", 150000);
    s_2.AddAtt(relName[1], "c_nationkey", 25);

    s_2.AddRel(relName[2], 25);
    s_2.AddAtt(relName[2], "n_nationkey", 25);

    cnf = "(n_nationkey < 10 OR n_nationkey = 15 OR n_nationkey = 20) AND (c_nationkey = n_nationkey) AND (o_custkey = 10 OR c_custkey = 20)";
    yy_scan_string(cnf);
    yyparse();

    double result_2 = s_2.Estimate(final, relName, 3);
    ASSERT_LE(fabs(result_2 - result_1),0.1);
}

TEST(StatisticalEstimationTest, ORPlusJoinPredicates) {
    Statistics s_1;
    char *relName[] = {"orders", "customer", "nation"};

    s_1.AddRel(relName[0], 1500000);
    s_1.AddAtt(relName[0], "o_custkey", 150000);

    s_1.AddRel(relName[1], 150000);
    s_1.AddAtt(relName[1], "c_custkey", 150000);
    s_1.AddAtt(relName[1], "c_nationkey", 25);

    s_1.AddRel(relName[2], 25);
    s_1.AddAtt(relName[2], "n_nationkey", 25);

    char *cnf = "(o_custkey = 10 OR c_custkey = 20) AND (c_custkey = o_custkey)";
    yy_scan_string(cnf);
    yyparse();

    // Join the first two relations in relName
    s_1.Apply(final, relName, 2);

    cnf = "(c_nationkey = n_nationkey) AND (n_nationkey < 10 OR n_nationkey = 15)";
    yy_scan_string(cnf);
    yyparse();

    double result_1 = s_1.Estimate(final, relName, 3);
    
    Statistics s_2;

    s_2.AddRel(relName[0], 1500000);
    s_2.AddAtt(relName[0], "o_custkey", 150000);

    s_2.AddRel(relName[1], 150000);
    s_2.AddAtt(relName[1], "c_custkey", 150000);
    s_2.AddAtt(relName[1], "c_nationkey", 25);

    s_2.AddRel(relName[2], 25);
    s_2.AddAtt(relName[2], "n_nationkey", 25);

    cnf = "(n_nationkey < 10 OR n_nationkey = 15) AND (c_nationkey = n_nationkey) AND (c_custkey = o_custkey) AND (o_custkey = 10 OR c_custkey = 20)";
    yy_scan_string(cnf);
    yyparse();

    double result_2 = s_2.Estimate(final, relName, 3);
    ASSERT_EQ(result_1,result_2);
}