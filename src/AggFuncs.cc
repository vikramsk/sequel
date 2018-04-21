#include <iostream>
#include <string>
#include "ParseTree.h"
#include "QueryPlanner.h"

void QueryPlanner::createProjectNode() {
    Node newRoot(PROJECT);
    newRoot.inPipeL = root->outPipe;
    newRoot.inPipeR = NULL;
    newRoot.numAttsIn = root->outSchema->GetNumAtts();
    newRoot.keepMe = getAttributesList(newRoot.numAttsOut);
    if (newRoot.numAttsOut == 0) {
        cerr << "output attributes are not specified in the query" << endl;
        exit(1);
    }
    root = &newRoot;
}

int *QueryPlanner::getAttributesList(int &numAttsOut) {
    numAttsOut = 0;
    vector<int> finalAtts;
    struct NameList *selAttribute = tokens.attsToSelect;

    while (selAttribute) {
        int pos = root->outSchema->Find(selAttribute->name);
        if (pos == -1) {
            cerr << selAttribute->name << " is an invalid output attribute"
                 << endl;
            exit(1);
        }
        numAttsOut++;
        finalAtts.push_back(pos);
        selAttribute = selAttribute->next;
    }

    return finalAtts.data();
}

void QueryPlanner::createGroupByNode() {}

void QueryPlanner::createSumNode() {}

void QueryPlanner::createDupRemovalNode() {}
