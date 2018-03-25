#ifndef COMPARISON_H
#define COMPARISON_H

#include <fstream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"

using namespace std;
// This stores an individual comparison that is part of a CNF
class Comparison {
    friend class ComparisonEngine;
    friend class CNF;

    Target operand1;
    int whichAtt1;
    Target operand2;
    int whichAtt2;

    Type attType;

    CompOperator op;

   public:
    Comparison();

    // copy constructor
    Comparison(const Comparison &copyMe);

    // print to the screen
    void Print();
};

class Schema;

// This structure encapsulates a sort order for records
class OrderMaker {
    friend class ComparisonEngine;
    friend class CNF;

    int numAtts;

    int whichAtts[MAX_ANDS];
    Type whichTypes[MAX_ANDS];

   public:
    // writes out an ordermaker
    friend std::ostream &operator<<(std::ostream &os, const OrderMaker &om) {
        os << om.numAtts << endl;

        for (int i = 0; i < om.numAtts; i++) {
            os << om.whichAtts[i] << " ";
            os << om.whichTypes[i] << endl;
        }
        return os;
    };
    // reads into an ordermaker
    friend std::istream &operator>>(std::istream &is, OrderMaker &om) {
        is >> om.numAtts;

        for (int i = 0; i < om.numAtts; i++) {
            is >> om.whichAtts[i];
            int t;
            is >> t;
            om.whichTypes[i] = (Type)t;
        }
        return is;
    };
    // creates an empty OrdermMaker
    OrderMaker();

    // create an OrderMaker that can be used to sort records
    // based upon ALL of their attributes
    OrderMaker(Schema *schema);

    // returns the number of attributes
    int getNumAttributes();

    // gets the list of attributes in the OrderMaker instance
    int *getAttributes();

    // print to the screen
    void Print();
};

class Record;

// This structure stores a CNF expression that is to be evaluated
// during query execution

class CNF {
    friend class ComparisonEngine;

    Comparison orList[MAX_ANDS][MAX_ORS];

    int orLens[MAX_ANDS];
    int numAnds;

   public:
    // this returns an instance of the OrderMaker class that
    // allows the CNF to be implemented using a sort-based
    // algorithm such as a sort-merge join.  Returns a 0 if and
    // only if it is impossible to determine an acceptable ordering
    // for the given comparison
    int GetSortOrders(OrderMaker &left, OrderMaker &right);

    // this method uses the sort order of the sorted file and builds
    // query orders from the current cnf instance. Returns a 0 if
    // the cnf instance has no sorting attributes that can be used
    // to optimize search
    int GetQueryOrder(OrderMaker &sortOrder, OrderMaker &queryOrder,
                      OrderMaker &queryLiteralOrder);

    // print the comparison structure to the screen
    void Print();

    // this takes a parse tree for a CNF and converts it into a 2-D
    // matrix storing the same CNF expression.  This function is applicable
    // specifically to the case where there are two relations involved
    void GrowFromParseTree(struct AndList *parseTree, Schema *leftSchema,
                           Schema *rightSchema, Record &literal);

    // version of the same function, except that it is used in the case of
    // a relational selection over a single relation so only one schema is used
    void GrowFromParseTree(struct AndList *parseTree, Schema *mySchema,
                           Record &literal);
};

#endif
