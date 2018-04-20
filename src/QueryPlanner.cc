#include "QueryPlanner.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"
#include "cpptoml.h"

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
void QueryPlanner::initializeStats() {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("stats");
    char *statsPath = new char[entry->length() + 1];
    strcpy(statsPath, entry->c_str());

    stats.Read(statsPath);

    TableList *tblPtr = tokens.tables;
    unordered_set<string> relList;
    while (tblPtr) {
        stats.CopyRel(tblPtr->tableName, tblPtr->aliasAs);
        stats.DeleteRel(tblPtr->tableName);
        tblPtr = tblPtr->next;
    }

    // TODO: Delete all extra relations using the catalog.
}

void QueryPlanner::processAndList() { tokens.createRelOrPairs(); }

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

vector<string> getRelations(struct ComparisonOp *cmpOp) {
    vector<string> relations;
    if (cmpOp->left && cmpOp->left->code == NAME) {
        string attName(cmpOp->left->value);
    }
}

RelOrPair *parseOrPair(struct OrList *orList) {
    RelOrPair *rlp = new RelOrPair();
    rlp->orList = orList;
    OrList *orPtr = orList;
    while (orPtr) {
        vector<string> rels = getRelations(orPtr->left);
        rlp->relations.insert(rels.begin(), rels.end());
        orPtr = orPtr->rightOr;
    }
    return rlp;
}

void QueryTokens::createRelOrPairs() {
    AndList *andPtr = andList;
    while (andPtr) {
        RelOrPair *relOrPair = parseOrPair(andPtr->left);
        relOrPairs.push_back(relOrPair);
        for (auto rel : relOrPair->relations) {
            relClauses[rel].push_back(relOrPair);
        }
        andPtr = andPtr->rightAnd;
    }
}
