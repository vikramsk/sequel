#include "Catalog.h"
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include "cpptoml.h"

using namespace std;

Catalog::Catalog() {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("catalog");
    fileName = new char[entry->length() + 1];
    strcpy(fileName, entry->c_str());
    parseCatalog();
}

void Catalog::parseCatalog() {
    ifstream catalog(fileName);
    if (!catalog.is_open()) {
        cerr << "could not load the catalog" << endl;
        exit(1);
    }

    string line;

    while (catalog >> line) {
        if (!line.compare("BEGIN")) {
            string relName, relFile;
            catalog >> relName >> relFile;
            while (catalog >> line) {
                if (!line.compare("END")) break;

                CatAttribute *att = new CatAttribute();
                att->name = line;
                catalog >> att->type;
                relAttributes[relName].push_back(att);
            }
        }
    }
}

void Catalog::Write() {
    ofstream catalog(fileName);
    if (!catalog.is_open()) {
        cerr << "could not load the catalog" << endl;
        exit(1);
    }

    for (auto &rel : relAttributes) {
        catalog << "BEGIN" << endl;
        catalog << rel.first << endl;
        catalog << rel.first << ".tbl" << endl;
        for (auto &att : rel.second) {
            catalog << att->name << " " << att->type << endl;
        }
        catalog << "END" << endl << endl;
    }
}
