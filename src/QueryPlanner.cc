#include "QueryPlanner.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"

using namespace std;

QueryPlanner::~QueryPlanner() {}

void QueryPlanner::Create() {
    initializeStats();

    if (tokens.andList) processAndList();

    if (tokens.aggFunction) processAggFuncs();
}

void QueryPlanner::Print() {}

// reads the table list and loads the stats
// for the relations given in the table list.
void QueryPlanner::initializeStats() {}

void QueryPlanner::processAndList() {}

void QueryPlanner::processAggFuncs() {

    if (tokens.distinctNoAgg) {
        if (tokens.groupingAtts) {
            cerr << "group by without aggregate function is not supported by this database (yet)" << endl;
            exit(1);
        } else {
            createProjectNode();
            createDupRemovalNode();
        }
    } else {
        if (tokens.distinctWithAgg) {
            if (tokens.groupingAtts) {
                cerr << "group by with distinct sum function is not supported by this database (yet)" << endl;
                exit(1);
            } else {
                createProjectNode();
                createDupRemovalNode();
                createSumNode();
            }
        } else {
            if (tokens.groupingAtts) {
                if (tokens.aggFunction) createGroupByNode();
                else {
                    cerr << "group by without aggregate function is not supported by this database (yet)" << endl;
                    exit(1);
                }
            }
            createProjectNode();
        }
    }
}
