#include <iostream>
#include <map>
#include "ParseTree.h"
#include "QueryPlanner.h"
#include "Schema.h"
#include "string.h"

void QueryPlanner::createProjectNode() {
    Node *newRoot = new Node(PROJECT);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->outPipe = new Pipe(tokens.pipeSize);
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
    map<int, Attribute> finalAtts;
    NameList *selAttribute = tokens.attsToSelect;
    addAttsToList(finalAtts,numAttsOut,selAttribute);
    if (tokens.aggFunction) {
        selAttribute = extractAttsFromFunc(tokens.aggFunction, NULL);
        addAttsToList(finalAtts,numAttsOut,selAttribute);
    }

    int i = 0;
    int *attsToKeep = new int[numAttsOut];
    Attribute *attsList = new Attribute[numAttsOut];
    
    for (auto pair : finalAtts) {
        attsToKeep[i] = pair.first;
        attsList[i++] = pair.second;
    }
    newSchema = new Schema("out_schema",numAttsOut,attsList);
    return attsToKeep;
}

void QueryPlanner::addAttsToList(map<int, Attribute> &finalAtts, int &numAttsOut, NameList *selAttribute) {
    while (selAttribute) {
        int position = root->outSchema->Find(selAttribute->name);
        if (position == -1) {
            cerr << selAttribute->name << " is an invalid output attribute"
                 << endl;
            exit(1);
        }
        numAttsOut++;
        Attribute att;
        att.name = strdup(selAttribute->name);
        att.myType = root->outSchema->FindType(selAttribute->name);
        finalAtts[position] = att;
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
    newRoot->outPipe = new Pipe(tokens.pipeSize);
    newRoot->func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    Schema *grpSchema = setupGroupOrder(newRoot->outSchema);
    newRoot->groupOrder = new OrderMaker(grpSchema);
    root = newRoot;
}

Schema *QueryPlanner::setupGroupOrder(Schema *&newSchema) {
    int numAttsOut = 0;
    map<int, Attribute> finalAtts;
    struct NameList *groupAttribute = tokens.groupingAtts;
    
    addAttsToList(finalAtts,numAttsOut,groupAttribute);
    
    int i = 0;
    Attribute groupAttsList[numAttsOut];
    Attribute finalAttsList[numAttsOut+1];
    finalAttsList[i] = {"GroupSum", Double};

    for (auto pair : finalAtts) {
        groupAttsList[i] = pair.second;
        finalAttsList[i+1] = pair.second;
        i++;
    }
    newSchema = new Schema("out_schema",numAttsOut+1,finalAttsList);
    return new Schema("grp_schema",numAttsOut,groupAttsList);
}

void QueryPlanner::createSumNode() {
    Node *newRoot = new Node(SUM);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->outPipe = new Pipe(tokens.pipeSize);
    newRoot->func.GrowFromParseTree(tokens.aggFunction,*(root->outSchema));
    Attribute attsList[1] = {{"Sum", Double}};
    newRoot->outSchema = new Schema("out_schema",1,attsList);
    root = newRoot;
}

void QueryPlanner::createDupRemovalNode() {
    Node *newRoot = new Node(DISTINCT);
    newRoot->leftLink = root;
    newRoot->inPipeL = root->outPipe;
    newRoot->outPipe = new Pipe(tokens.pipeSize);
    newRoot->outSchema = root->outSchema;
    root = newRoot;
}
