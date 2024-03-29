#ifndef DEFS_H
#define DEFS_H

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072

enum Target { Left, Right, Literal };
enum CompOperator { LessThan, GreaterThan, Equals };
enum Type { Int, Double, String };

typedef enum {
    JOIN,
    SELFILE,
    SELPIPE,
    PROJECT,
    SUM,
    GROUPBY,
    DISTINCT,
    WRITEOUT
} opType;

unsigned int Random_Generate();

#endif
