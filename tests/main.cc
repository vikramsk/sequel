#include <cpptoml.h>
#include "gtest/gtest.h"

class Config {
    const struct Constants {
        const char* ConfigFile = "config.toml";
        const char* Catalog = "test.path";
        const char* Data = "test.data";
        const char* Build = "test.build";
    } constants;

   public:
    Config();
};

int main(int argc, char** argv) {
    Config conf;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

Config::Config() {
    auto config = cpptoml::parse_file(constants.ConfigFile);
    auto filePath = config->get_as<std::string>(constants.Catalog);
    int nameLength = filePath->length();

    char fileName[nameLength + 1];
    strcpy(fileName, filePath->c_str());
}
