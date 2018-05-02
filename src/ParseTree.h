#ifndef PARSE_TREE_H
#define PARSE_TREE_H

// these are the different types of operators that can appear
// in a CNF expression
#define LESS_THAN 1
#define GREATER_THAN 2
#define EQUALS 3

// these are the types of operands that can appear in a CNF expression
static const int DOUBLE = 1;
static const int INT = 2;
static const int STRING = 3;
static const int NAME = 4;

// Commands
#define CREATE 1
#define INSERT_INTO 2
#define DROP 3
#define OUTPUT_SET 4
#define SELECT_TABLE 5
#define QUIT_SQL 6
#define UPDATE 7

// Table types
#define HEAP_DB 1
#define SORTED_DB 2

// Output types
#define SET_STDOUT 1
#define SET_FILE 2
#define SET_NONE 3

// Used by CREATE TABLE
struct CreateTable {
    // The type of database used to hold the table (HEAP or SORTED)
    int type;

    // The list of attributes
    struct AttDesc *atts;

    // The list of attributes that will be sorted on (in reverse order)
    struct NameList *sort;
};

// The description of an attribute
struct AttDesc {
    // Int, Double, or String
    int type;

    // The attributes name
    char *name;

    struct AttDesc *next;
};

struct TableList {
    // this is the original table name
    char *tableName;

    // this is the value it is aliased to
    char *aliasAs;

    // and this the next alias
    struct TableList *next;
};

struct NameList {
    // this is the name
    char *name;

    // and this is the next name in the list
    struct NameList *next;
};

struct Operand {
    // this tells us the type of the operand: FLOAT, INT, STRING...
    int code;

    // this is the actual operand
    char *value;
};

struct ComparisonOp {
    // this corresponds to one of the codes describing what type
    // of literal value we have in this nodes: LESS_THAN, EQUALS...
    int code;

    // these are the operands on the left and on the right
    struct Operand *left;
    struct Operand *right;
};

struct OrList {
    // this is the comparison to the left of the OR
    struct ComparisonOp *left;

    // this is the OrList to the right of the OR; again,
    // this might be NULL if the right is a simple comparison
    struct OrList *rightOr;
};

struct AndList {
    // this is the disjunction to the left of the AND
    struct OrList *left;

    // this is the AndList to the right of the AND
    // note that this can be NULL if the right is a disjunction
    struct AndList *rightAnd;
};

struct FuncOperand {
    // this tells us the type of the operand: FLOAT, INT, STRING...
    int code;

    // this is the actual operand
    char *value;
};

struct FuncOperator {
    // this tells us which operator to use: '+', '-', ...
    int code;

    // these are the operators on the left and on the right
    struct FuncOperator *leftOperator;
    struct FuncOperand *leftOperand;
    struct FuncOperator *right;
};

#endif
