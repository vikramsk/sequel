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

void QueryPlanner::processAggFuncs() {}
