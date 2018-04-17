#include <math.h>
#include <stdlib.h>
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "testsuite.h"
#include "gtest/gtest.h"

TEST(StatisticalEstimationTest, ResultWithMultiplePredicates) {
    Statistics s;
    char *relName[] = {"orders", "customer", "nation"};

    s.AddRel(relName[0], 1500000);
    s.AddAtt(relName[0], "o_custkey", 150000);

    s.AddRel(relName[1], 150000);
    s.AddAtt(relName[1], "c_custkey", 150000);
    s.AddAtt(relName[1], "c_nationkey", 25);

    s.AddRel(relName[2], 25);
    s.AddAtt(relName[2], "n_nationkey", 25);

    char *cnf = "(c_custkey = o_custkey)";
    yy_scan_string(cnf);
    yyparse();

    // Join the first two relations in relName
    s.Apply(final, relName, 2);

    cnf = " (c_nationkey = n_nationkey)";
    yy_scan_string(cnf);
    yyparse();

    double result = s.Estimate(final, relName, 3);
    ASSERT_LE(fabs(result - 1500000),0.1);
    s.Apply(final, relName, 3);
}