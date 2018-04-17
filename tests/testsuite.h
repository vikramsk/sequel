#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "DBFile.h"
#include "Function.h"
#include "Pipe.h"
#include "Record.h"
using namespace std;

extern "C" {
int yyparse(void);                 // defined in y.tab.c
int yyfuncparse(void);             // defined in yyfunc.tab.c
void init_lexical_parser(char *);  // defined in lex.yy.c (from Lexer.l)
void close_lexical_parser();       // defined in lex.yy.c
void init_lexical_parser_func(
    char *);                       // defined in lex.yyfunc.c (from Lexerfunc.l)
void close_lexical_parser_func();  // defined in lex.yyfunc.c
struct YY_BUFFER_STATE *yy_scan_string(const char *);
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;

typedef struct {
    Pipe *pipe;
    OrderMaker *order;
    bool print;
    bool write;
} testutil;

class relation {
   private:
    const char *rname;
    const char *prefix;
    char rpath[100];
    Schema *rschema;

   public:
    static char *catalog_path;
    static char *dbfile_dir;
    static char *tpch_dir;

    relation(const char *_name, Schema *_schema, const char *_prefix)
        : rname(_name), rschema(_schema), prefix(_prefix) {
        sprintf(rpath, "%s%s.bin", prefix, rname);
    }
    relation(char *relName, char *dbf) {
        rname = relName;
        rschema = new Schema(catalog_path, relName);
        prefix = dbf;
        sprintf(rpath, "%s%s.bin", prefix, rname);
    }
    const char *name() { return rname; }
    const char *path() { return rpath; }
    Schema *schema() { return rschema; }
    void info() {
        cout << " relation info\n";
        cout << "\t name: " << name() << endl;
        cout << "\t path: " << path() << endl;
    }

    void get_cnf(char *input, CNF &cnf_pred, Record &literal) {
        init_lexical_parser(input);
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF.\n";
            exit(1);
        }
        cnf_pred.GrowFromParseTree(final, schema(),
                                   literal);  // constructs CNF predicate
        close_lexical_parser();
    }

    void get_cnf(char *input, Function &fn_pred) {
        init_lexical_parser_func(input);
        if (yyfuncparse() != 0) {
            cout << " Error: can't parse your CNF.\n";
            exit(1);
        }
        fn_pred.GrowFromParseTree(finalfunc,
                                  *(schema()));  // constructs CNF predicate
        close_lexical_parser_func();
    }

    void get_cnf(CNF &cnf_pred, Record &literal) {
        if (yyparse() != 0) {
            std::cout << "Can't parse your CNF.\n";
            exit(1);
        }
        cnf_pred.GrowFromParseTree(final, schema(),
                                   literal);  // constructs CNF predicate
    }

    void get_file_cnf(const char *fpath, CNF &cnf_pred, Record &literal) {
        yyin = fopen(fpath, "r");
        if (yyin == NULL) {
            cout << " Error: can't open file " << fpath << " for parsing \n";
            exit(1);
        }
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF.\n";
            exit(1);
        }
        cnf_pred.GrowFromParseTree(final, schema(),
                                   literal);  // constructs CNF predicate
    }

    void get_sort_order(OrderMaker &sortorder) {
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF.\n";
            exit(1);
        }
        Record literal;
        CNF sort_pred;
        sort_pred.GrowFromParseTree(final, schema(),
                                    literal);  // constructs CNF predicate
        OrderMaker dummy;
        sort_pred.GetSortOrders(sortorder, dummy);
    }

    static void get_cnf(char *input, Schema *left, CNF &cnf_pred,
                        Record &literal) {
        init_lexical_parser(input);
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF " << input << endl;
            exit(1);
        }
        cnf_pred.GrowFromParseTree(final, left,
                                   literal);  // constructs CNF predicate
        close_lexical_parser();
    }

    static void get_cnf(char *input, Schema *left, Schema *right, CNF &cnf_pred,
                        Record &literal) {
        init_lexical_parser(input);
        if (yyparse() != 0) {
            cout << " Error: can't parse your CNF " << input << endl;
            exit(1);
        }
        cnf_pred.GrowFromParseTree(final, left, right,
                                   literal);  // constructs CNF predicate
        close_lexical_parser();
    }

    static void get_cnf(char *input, Schema *left, Function &fn_pred) {
        init_lexical_parser_func(input);
        if (yyfuncparse() != 0) {
            cout << " Error: can't parse your arithmetic expr. " << input
                 << endl;
            exit(1);
        }
        fn_pred.GrowFromParseTree(finalfunc,
                                  *left);  // constructs CNF predicate
        close_lexical_parser_func();
    }
};

static constexpr const char *supplier = "supplier";
static constexpr const char *part = "part";
static constexpr const char *partsupp = "partsupp";
static constexpr const char *nation = "nation";
static constexpr const char *customer = "customer";
static constexpr const char *orders = "orders";
static constexpr const char *region = "region";
static constexpr const char *lineitem = "lineitem";
// extern relation *rel;
// extern relation *s, *p, *ps, *n, *li, *r, *o, *c;
