#include "QueryPlanner.h"
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"
#include "cpptoml.h"

using namespace std;

char *getCString(string str) {
    char *val = new char[str.length() + 1];
    strcpy(val, str.c_str());
    return val;
}

char **convertToCStrings(vector<string> strs) {
    char **cStrings = new char *[strs.size()];

    for (int i = 0; i < strs.size(); i++) {
        cStrings[i] = getCString(strs[i]);
    }
    return cStrings;
}

QueryPlanner::~QueryPlanner() {}

void QueryPlanner::Create() {
    unordered_map<string, string> relAliasMap = initializeStats();

    if (tokens.andList) processAndList(relAliasMap);

    processAggFuncs();
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
                createProjectNode();  // Projects attributes within the function
                createDupRemovalNode();
                createSumNode();
            }
        } else {
            if (tokens.groupingAtts) {
                if (tokens.aggFunction) {
                    createGroupByNode();
                } else {
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

struct orListComparator {
    inline bool operator()(const OrList *or1, const OrList *or2) {
        if (or1->rightOr) {
            return false;
        }
        return true;
    }
};

AndList *createAndList(vector<OrList *> orList) {
    if (!orList.size()) return NULL;
    sort(orList.begin(), orList.end(), orListComparator());
    AndList *aList = new AndList();
    AndList *aListPtr = aList;
    int size = orList.size();
    for (int i = 0; i < size; i++) {
        aListPtr->left = orList[i];
        if (i < size - 1) {
            aListPtr->rightAnd = new AndList();
            aListPtr = aListPtr->rightAnd;
        }
    }
    return aList;
}

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

        tblPtr = tblPtr->next;
    }

    // TODO: Delete all extra relations using the catalog.
    return relAliasMap;
}

void QueryPlanner::processAndList(unordered_map<string, string> relAliasMap) {
    tokens.createRelOrPairs();
    createSelectionNodes(relAliasMap);
    createJoinOrder();
    root = relationNode.begin()->second;
}

void QueryPlanner::createJoinOrder() {
    while (tokens.relOrPairs.size()) {
        mergeCheapestRelations();
    }

    // merges all relations into one node if
    // it's not already done.
    performCrossJoins();
}

void QueryPlanner::performCrossJoins() {
    unordered_set<Node *> uniqueNodes;
    for (auto &r : relationNode) {
        uniqueNodes.insert(r.second);
    }
    if (uniqueNodes.size() == 1) return;

    double minEstimate = numeric_limits<double>::max();
    pair<Node *, Node *> minNodes;
    for (auto &n1 : uniqueNodes) {
        for (auto &n2 : uniqueNodes) {
            if (n1 == n2) continue;
            vector<string> mergeRelations;
            mergeRelations.insert(mergeRelations.end(), n1->relations.begin(),
                                  n1->relations.end());
            mergeRelations.insert(mergeRelations.end(), n2->relations.begin(),
                                  n2->relations.end());
            double est = stats.Estimate(NULL, convertToCStrings(mergeRelations),
                                        mergeRelations.size());
            if (est < minEstimate) {
                minEstimate = est;
                minNodes = make_pair(n1, n2);
            }
        }
    }

    Node *joinNode = new Node(JOIN);
    joinNode->outPipe = new Pipe(100);
    joinNode->inPipeL = minNodes.first->outPipe;
    joinNode->leftLink = minNodes.first;
    joinNode->inPipeR = minNodes.second->outPipe;
    joinNode->rightLink = minNodes.second;
    joinNode->relations.insert(minNodes.first->relations.begin(),
                               minNodes.first->relations.end());
    joinNode->relations.insert(minNodes.second->relations.begin(),
                               minNodes.second->relations.end());
    joinNode->outSchema = new Schema();
    joinNode->outSchema->Merge(minNodes.first->outSchema,
                               minNodes.second->outSchema);
    joinNode->cnf.GrowFromParseTree(NULL, minNodes.first->outSchema,
                                    minNodes.second->outSchema,
                                    joinNode->literal);
    for (auto &r : joinNode->relations) {
        relationNode[r] = joinNode;
    }

    performCrossJoins();
}

void QueryPlanner::mergeCheapestRelations() {
    double minEstimate = numeric_limits<double>::max();
    vector<RelOrPair *> minRelOrPairs;
    AndList *queryAndList;
    for (auto &rop : tokens.relOrPairs) {
        unordered_set<string> rels;
        for (auto &r : rop->relations) {
            rels.insert(relationNode[r]->relations.begin(),
                        relationNode[r]->relations.end());
        }
        vector<RelOrPair *> relOrPairs = {rop};

        rels.insert(rop->relations.begin(), rop->relations.end());
        if (!areNodesMergeable(rels)) continue;

        for (auto &rop2 : tokens.relOrPairs) {
            if (rop == rop2) continue;

            unordered_set<string> additionalRels;
            for (auto &r : rop2->relations) {
                additionalRels.insert(relationNode[r]->relations.begin(),
                                      relationNode[r]->relations.end());
            }
            additionalRels.insert(rels.begin(), rels.end());
            additionalRels.insert(rop2->relations.begin(),
                                  rop2->relations.end());
            if (areNodesMergeable(additionalRels)) {
                rels.insert(rop2->relations.begin(), rop2->relations.end());
                relOrPairs.push_back(rop2);
            }
        }

        double est = estimate(relOrPairs, false);
        if (est < minEstimate) {
            minEstimate = est;
            minRelOrPairs = relOrPairs;
        }
    }
    Node *joinNode = createJoinNode(minRelOrPairs);

    for (int i = 0; i < minRelOrPairs.size(); i++) {
        tokens.relOrPairs.remove(minRelOrPairs[i]);
        delete minRelOrPairs[i];
    }

    for (auto &r : joinNode->relations) {
        relationNode[r] = joinNode;
    }
}

void updateRelNodes(vector<RelOrPair *> &relOrPairs, Node *joinNode) {
    unordered_set<string> uniqueRels;
}

Node *QueryPlanner::createJoinNode(vector<RelOrPair *> &relOrPairs) {
    Node *node = new Node(JOIN);
    node->outPipe = new Pipe(100);

    estimate(relOrPairs, true);

    Node *nodeL = relationNode[*relOrPairs[0]->relations.begin()];
    Node *nodeR;
    bool nodesSet = false;
    vector<OrList *> orList;
    for (int i = 0; i < relOrPairs.size(); i++) {
        orList.push_back(relOrPairs[i]->orList);
        if (nodesSet) continue;

        for (auto &r : relOrPairs[i]->relations) {
            if (nodeL == relationNode[r]) continue;
            nodeR = relationNode[r];
            nodesSet = true;
            break;
        }
    }

    AndList *andList = createAndList(orList);

    node->outSchema = new Schema();
    node->outSchema->Merge(nodeL->outSchema, nodeR->outSchema);
    node->cnf.GrowFromParseTree(andList, nodeL->outSchema, nodeR->outSchema,
                                node->literal);
    node->leftLink = nodeL;
    node->inPipeL = nodeL->outPipe;
    node->rightLink = nodeR;
    node->inPipeR = nodeR->outPipe;
    node->relations.insert(nodeL->relations.begin(), nodeL->relations.end());
    node->relations.insert(nodeR->relations.begin(), nodeR->relations.end());
    return node;
}

double QueryPlanner::estimate(vector<RelOrPair *> relOrPairs,
                              bool applyEstimate) {
    vector<OrList *> orList;
    unordered_set<string> rels;
    for (auto &rop : relOrPairs) {
        orList.push_back(rop->orList);
        for (auto &r : rop->relations) {
            rels.insert(r);
            rels.insert(relationNode[r]->relations.begin(),
                        relationNode[r]->relations.end());
        }
    }
    vector<string> relList;
    relList.insert(relList.end(), rels.begin(), rels.end());

    AndList *andList = createAndList(orList);
    double estimate =
        stats.Estimate(andList, convertToCStrings(relList), rels.size());

    if (applyEstimate)
        stats.Apply(andList, convertToCStrings(relList), rels.size());

    return estimate;
}

bool QueryPlanner::areNodesMergeable(unordered_set<string> rels) {
    unordered_set<Node *> uniqueNodes;
    for (auto r : rels) {
        uniqueNodes.insert(relationNode[r]);
    }
    return uniqueNodes.size() == 2;
}

Schema *getTransformedSchema(string relName, string relAlias) {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("catalog");
    char *catalogPath = new char[entry->length() + 1];
    strcpy(catalogPath, entry->c_str());

    Schema *schema = new Schema(catalogPath, getCString(relName));

    Attribute *atts = schema->GetAtts();
    for (int i = 0; i < schema->GetNumAtts(); i++) {
        size_t len = relAlias.size() + strlen(atts[i].name) + 2;
        char *name = (char *)malloc(len + 2);
        strcpy(name, getCString(relAlias));
        strcat(name, ".");
        strcat(name, atts[i].name);
        atts[i].name = name;
    }
    return schema;
}

void QueryPlanner::createSelectionNodes(
    unordered_map<string, string> relAliasMap) {
    for (auto &rc : tokens.relClauses) {
        vector<OrList *> orList;
        for (auto &rp : rc.second) {
            if (rp->relations.size() == 1) {
                orList.push_back(rp->orList);
                tokens.relOrPairs.remove(rp);
            }
        }

        AndList *andList = createAndList(orList);
        vector<string> relName{rc.first};
        char **relNames = convertToCStrings(relName);

        relationNode[rc.first] = new Node(SELFILE);
        relationNode[rc.first]->fileName = relAliasMap[rc.first] + ".bin";
        relationNode[rc.first]->inPipeL = NULL;
        relationNode[rc.first]->inPipeR = NULL;
        relationNode[rc.first]->outPipe = new Pipe(100);

        relationNode[rc.first]->relations.insert(rc.first);
        relationNode[rc.first]->outSchema =
            getTransformedSchema(relAliasMap[rc.first], rc.first);

        if (andList) stats.Apply(andList, relNames, 1);
        relationNode[rc.first]->cnf.GrowFromParseTree(
            andList, relationNode[rc.first]->outSchema,
            relationNode[rc.first]->literal);
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
