#include "QueryPlanner.h"
#include <iostream>
#include <string>
#include "ParseTree.h"

void QueryPlanner::createProjectNode() {
    Node newRoot;
    newRoot.operation = PROJECT;
    newRoot.inPipeL = root->outPipe;
    newRoot.inPipeR = NULL;
    newRoot.numAttsIn = root->outSchema->GetNumAtts();
    //newRoot.numAttsOut = 3;
    //int keepMe[] = {0, 1, 7};
    newRoot.keepMe = getAttributesList();
    root = &newRoot;
}

int *QueryPlanner::getAttributesList() {
    if (!tokens.attsToSelect) {
        cerr << "attributes to be selected is not specified" << endl;
        exit(1);
    }
    struct NameList *selAttribute = tokens.attsToSelect;
    while (selAttribute) {
        selAttribute = selAttribute->next;
    }
}

void QueryPlanner::createGroupByNode() {}

void QueryPlanner::createSumNode() {}

void QueryPlanner::createDupRemovalNode() {}