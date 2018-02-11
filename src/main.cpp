#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "Record.h"
#include "cpptoml.h"

using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

extern struct AndList *final;

int main() {
    auto config = cpptoml::parse_file("config.toml");
    auto filePath = config->get_as<string>("data");
    int nameLength = filePath->length();

    char fileName[nameLength + 20];
    strcpy(fileName, filePath->c_str());
    strcat(fileName, "lineitem.tbl");
    cout << fileName << endl;
    // try to parse the CNF
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
        cout << "Can't parse your CNF.\n";
        exit(1);
    }

    // suck up the schema from the file
    Schema lineitem("data/catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    myComparison.GrowFromParseTree(final, &lineitem, literal);

    // print out the comparison to the screen
    myComparison.Print();

    // now open up the text file and start processing it
    FILE *tableFile = fopen(fileName, "r");

    Record temp;
    Schema mySchema("data/catalog", "lineitem");

    // char *bits = literal.GetBits ();
    // cout << " numbytes in rec " << ((int *) bits)[0] << endl;
    // literal.Print (&supplier);

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    ComparisonEngine comp;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        if (counter % 10000 == 0) {
            cerr << counter << "\n";
        }

        if (comp.Compare(&temp, &literal, &myComparison)) temp.Print(&mySchema);
    }
}
