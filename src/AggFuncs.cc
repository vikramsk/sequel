#include <iostream>
#include <algorithm>
#include "ParseTree.h"
#include "QueryPlanner.h"
#include "Schema.h"
#include "string.h"

void QueryPlanner::createProjectNode() {
    Node newRoot(PROJECT);
    newRoot.inPipeL = root->outPipe;
    newRoot.numAttsIn = root->outSchema->GetNumAtts();
    setAttributesList(newRoot.numAttsOut, newRoot.attsToKeep, newRoot.outSchema);
    if (newRoot.numAttsOut == 0) {
        cerr << "output attributes are not specified in the query" << endl;
        exit(1);
    }
    root = &newRoot;
}

void QueryPlanner::setAttributesList(int &numAttsOut, int *attsToKeep, Schema *newSchema) {
    struct AttDetails {
        int pos;
        Attribute details;
    };
    
    numAttsOut = 0;
    vector<AttDetails> finalAtts;
    struct NameList *selAttribute = tokens.attsToSelect;

    while (selAttribute) {
        int position = root->outSchema->Find(selAttribute->name);
        if (position == -1) {
            cerr << selAttribute->name << " is an invalid output attribute"
                 << endl;
            exit(1);
        }
        numAttsOut++;
        AttDetails att;
        att.pos = position;
        att.details.name = strdup(selAttribute->name);
        att.details.myType = root->outSchema->FindType(selAttribute->name);
        finalAtts.push_back(att);
        selAttribute = selAttribute->next;
    }

    sort(finalAtts.begin(), finalAtts.end(),
        [](AttDetails att1, AttDetails att2) 
        { return att1.pos - att2.pos; });
    attsToKeep = new int[numAttsOut];
    Attribute *attsList = new Attribute[numAttsOut];
    for (int i = 0; i < numAttsOut; i++) {
        attsToKeep[i] = finalAtts[i].pos;
        attsList[i] = finalAtts[i].details;
    }
    newSchema = new Schema("out_schema",numAttsOut,attsList);    
}

void QueryPlanner::createGroupByNode() {
    Node newRoot(GROUPBY);
    newRoot.inPipeL = root->outPipe;
    newRoot.func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    setupGroupOrder(newRoot.outSchema);
    newRoot.groupOrder = new OrderMaker(newRoot.outSchema);
    root = &newRoot;
}

void QueryPlanner::setupGroupOrder(Schema *newSchema) {
    struct AttDetails {
        int pos;
        Attribute details;
    };
    
    int numAttsOut = 0;
    vector<AttDetails> finalAtts;
    struct NameList *groupAttribute = tokens.groupingAtts;

    while (groupAttribute) {
        int position = root->outSchema->Find(groupAttribute->name);
        if (position == -1) {
            cerr << groupAttribute->name << " is an invalid attribute for grouping"
                 << endl;
            exit(1);
        }
        numAttsOut++;
        AttDetails att;
        att.pos = position;
        att.details.name = strdup(groupAttribute->name);
        att.details.myType = root->outSchema->FindType(groupAttribute->name);
        finalAtts.push_back(att);
        groupAttribute = groupAttribute->next;
    }

    sort(finalAtts.begin(), finalAtts.end(),
        [](AttDetails att1, AttDetails att2) 
        { return att1.pos - att2.pos; });
    Attribute attsList[numAttsOut+1];
    attsList[0] = {"GroupSum", Double};
    for (int i = 0; i < numAttsOut; i++) {
        attsList[i+1] = finalAtts[i].details;
    }
    newSchema = new Schema("out_schema",numAttsOut+1,attsList);    
}

void QueryPlanner::createSumNode() {
    Node newRoot(SUM);
    newRoot.inPipeL = root->outPipe;
    newRoot.func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    Attribute attsList[1] = {{"Sum", Double}};
    newRoot.outSchema = new Schema("out_schema",1,attsList);
    root = &newRoot;
}

void QueryPlanner::createDupRemovalNode() {

}
