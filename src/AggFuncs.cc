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
    newRoot.keepMe = getAttributesList(newRoot.numAttsOut);

    root = &newRoot;
}

int *QueryPlanner::getAttributesList(int &numAttsOut) {
    if (!tokens.attsToSelect) {
        cerr << "output attributes are not specified in the query" << endl;
        exit(1);
    }

    vector<int> finalAtts;
    numAttsOut = 0;
    struct NameList *selAttribute = tokens.attsToSelect;
    
    while (selAttribute) {
        int pos = root->outSchema->Find(selAttribute->name);
        if (pos == -1) {
            cerr << selAttribute->name << " is an invalid output attribute" << endl;
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