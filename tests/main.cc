#include <cpptoml.h>
#include "gtest/gtest.h"
#include "testsuite.h"

char *relation::tpch_dir = "";
char *relation::catalog_path = "";
char *relation::dbfile_dir = "";

void parseConfig() {
    auto config = cpptoml::parse_file("config.toml");

    auto entry = config->get_as<string>("data");

    relation::tpch_dir = new char[entry->length() + 1];
    strcpy(relation::tpch_dir, entry->c_str());

    entry = config->get_as<string>("catalog");
    relation::catalog_path = new char[entry->length() + 1];
    strcpy(relation::catalog_path, entry->c_str());

    entry = config->get_as<string>("dbfiles");
    relation::dbfile_dir = new char[entry->length() + 1];
    strcpy(relation::dbfile_dir, entry->c_str());
}

int main(int argc, char **argv) {
    parseConfig();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
