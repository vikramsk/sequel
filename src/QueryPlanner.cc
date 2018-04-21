#include "QueryPlanner.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ComparisonEngine.h"
#include "ParseTree.h"
#include "Schema.h"
#include "cpptoml.h"

using namespace std;

vector<char *> convertToCStrings(vector<string> strs) {
    vector<char *> cstrings{};

    for (auto &str : strs) {
        cstrings.push_back(&str.front());
    }
    return cstrings;
}

char *getCString(string str) {
    char *val = new char[str.length() + 1];
    strcpy(val, str.c_str());
    return val;
}

QueryPlanner::~QueryPlanner() {}

void QueryPlanner::Create() {
    auto relAliasMap = initializeStats();

    if (tokens.andList) processAndList(relAliasMap);

    if (tokens.aggFunction) processAggFuncs();
}

void QueryPlanner::Print() {}

// reads the table list and loads the stats
// for the relations given in the table list.
unordered_map<string, string> QueryPlanner::initializeStats() {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("stats");
    char *statsPath = new char[entry->length() + 1];
    strcpy(statsPath, entry->c_str());

    stats.Read(statsPath);

    TableList *tblPtr = tokens.tables;
    unordered_set<string> relList;
    unordered_map<string, string> relAliasMap;
    while (tblPtr) {
        stats.CopyRel(tblPtr->tableName, tblPtr->aliasAs);
        string tblAlias(tblPtr->aliasAs);
        relAliasMap[tblAlias] = string(tblPtr->tableName);

        // this loads the key in the map.
        tokens.relClauses[tblAlias];
        stats.DeleteRel(tblPtr->tableName);
        tblPtr = tblPtr->next;
    }

    // TODO: Delete all extra relations using the catalog.
}

void QueryPlanner::processAndList(unordered_map<string, string> relAliasMap) {
    tokens.createRelOrPairs();
    createSelectionNodes(relAliasMap);
}

AndList *createAndList(vector<OrList *> orList) {
    if (!orList.size()) return NULL;

    AndList *aList = new AndList();
    return aList;
}

void QueryPlanner::createSelectionNodes(
    unordered_map<string, string> relAliasMap) {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("catalog");
    char *catalogPath = new char[entry->length() + 1];
    strcpy(catalogPath, entry->c_str());

    for (auto &rc : tokens.relClauses) {
        vector<OrList *> orList;
        for (auto &rp : rc.second) {
            if (rp->relations.size() == 1) {
                orList.push_back(rp->orList);
            }
        }

        AndList *andList = createAndList(orList);
        vector<string> relName{rc.first};
        vector<char *> relNames = convertToCStrings(relName);

        relationNode[rc.first] = new Node(SELFILE);
        relationNode[rc.first]->relations.insert(rc.first);
        relationNode[rc.first]->outSchema =
            new Schema(catalogPath, getCString(relAliasMap[rc.first]));

        if (andList) {
            stats.Apply(andList, relNames.data(), 1);
            relationNode[rc.first]->cnf.GrowFromParseTree(
                andList, relationNode[rc.first]->outSchema,
                relationNode[rc.first]->literal);
        }
    }
}

void QueryPlanner::processAggFuncs() {
    if (tokens.distinctNoAgg) {
        if (tokens.groupingAtts) {
            cerr << "group by without aggregate function is not supported by "
                    "this database (yet)"
                 << endl;
            exit(1);
        } else {
            createProjectNode();
            createDupRemovalNode();
        }
    } else {
        if (tokens.distinctWithAgg) {
            if (tokens.groupingAtts) {
                cerr << "group by with distinct sum function is not supported "
                        "by this database (yet)"
                     << endl;
                exit(1);
            } else {
                createProjectNode();  // TODO: Figure how to use attributes
                                      // inside the function
                createDupRemovalNode();
                createSumNode();
            }
        } else {
            if (tokens.groupingAtts) {
                if (tokens.aggFunction)
                    createGroupByNode();
                else {
                    cerr << "group by without aggregate function is not "
                            "supported by this database (yet)"
                         << endl;
                    exit(1);
                }
            } else if (tokens.aggFunction)
                createSumNode();
            else
                createProjectNode();
        }
    }
}

unordered_set<string> getRelations(struct ComparisonOp *cmpOp) {
    unordered_set<string> relations;
    if (cmpOp->left && cmpOp->left->code == NAME) {
        string attName(cmpOp->left->value);
        string relName = attName.substr(0, attName.find("."));
        relations.insert(relName);
    }
    if (cmpOp->right && cmpOp->right->code == NAME) {
        string attName(cmpOp->right->value);
        string relName = attName.substr(0, attName.find("."));
        relations.insert(relName);
    }
    return relations;
}

RelOrPair *parseOrPair(struct OrList *orList) {
    RelOrPair *rlp = new RelOrPair();
    rlp->orList = orList;
    OrList *orPtr = orList;
    while (orPtr) {
        unordered_set<string> rels = getRelations(orPtr->left);
        if (rels.size() > 0) rlp->relations.insert(rels.begin(), rels.end());
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
