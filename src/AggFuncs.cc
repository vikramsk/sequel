#include <iostream>
#include <algorithm>
#include "ParseTree.h"
#include "QueryPlanner.h"
#include "Schema.h"
#include "string.h"

void QueryPlanner::createProjectNode() {
    Node *newRoot = new Node(PROJECT);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->numAttsIn = root->outSchema->GetNumAtts();
    newRoot->attsToKeep = setAttributesList(newRoot->numAttsOut, newRoot->outSchema);
    if (newRoot->numAttsOut == 0) {
        cerr << "output attributes are not specified in the query" << endl;
        exit(1);
    }
    root = newRoot;
}

int *QueryPlanner::setAttributesList(int &numAttsOut, Schema *&newSchema) { 
    numAttsOut = 0;
    vector<AttDetails> finalAtts;
    NameList *selAttribute = tokens.attsToSelect;
    addAttsToList(finalAtts,numAttsOut,selAttribute);
    if (tokens.aggFunction) {
        selAttribute = extractAttsFromFunc(tokens.aggFunction, NULL);
        addAttsToList(finalAtts,numAttsOut,selAttribute);
    }
    if (tokens.groupingAtts) {
        selAttribute = tokens.groupingAtts;
        addAttsToList(finalAtts,numAttsOut,selAttribute);
    }

    sort(finalAtts.begin(), finalAtts.end(),
        [](AttDetails att1, AttDetails att2) 
        { return att1.pos - att2.pos; });
    int *attsToKeep = new int[numAttsOut];
    Attribute *attsList = new Attribute[numAttsOut];
    for (int i = 0; i < numAttsOut; i++) {
        attsToKeep[i] = finalAtts[i].pos;
        attsList[i] = finalAtts[i].details;
    }
    newSchema = new Schema("out_schema",numAttsOut,attsList);
    return attsToKeep;
}

void QueryPlanner::addAttsToList(vector<AttDetails> &finalAtts, int &numAttsOut, NameList *selAttribute) {
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
}

NameList *QueryPlanner::extractAttsFromFunc(FuncOperator *root, NameList *rest) {
    if (!root) return rest;
    rest = extractAttsFromFunc(root->leftOperator, rest);
    if (root->leftOperand->code == NAME) {
        NameList *head = new NameList();
        *head = {root->leftOperand->value, rest};
        rest = head;
    }
    return extractAttsFromFunc(root->right, rest);
}


void QueryPlanner::createGroupByNode() {
    Node *newRoot = new Node(GROUPBY);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    setupGroupOrder(newRoot->outSchema);
    newRoot->groupOrder = new OrderMaker(newRoot->outSchema);
    root = newRoot;
}

void QueryPlanner::setupGroupOrder(Schema *&newSchema) {
    int numAttsOut = 0;
    vector<AttDetails> finalAtts;
    struct NameList *groupAttribute = tokens.groupingAtts;
    addAttsToList(finalAtts,numAttsOut,groupAttribute);

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
    Node *newRoot = new Node(SUM);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    Attribute attsList[1] = {{"Sum", Double}};
    newRoot->outSchema = new Schema("out_schema",1,attsList);
    root = newRoot;
}

void QueryPlanner::createDupRemovalNode() {
    Node *newRoot = new Node(DISTINCT);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->outSchema = root->outSchema;
    root = newRoot;
}
